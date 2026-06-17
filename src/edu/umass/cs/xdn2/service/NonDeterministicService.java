package edu.umass.cs.xdn2.service;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.xdn2.request.XdnHttpRequest;
import edu.umass.cs.xdn2.request.XdnHttpRequestBatch;
import edu.umass.cs.xdn2.request.XdnStopRequest;
import edu.umass.cs.xdn2.XdnHttpForwarderClient;
import edu.umass.cs.xdn2.recorder.AbstractStateDiffRecorder;
import edu.umass.cs.xdn2.sandbox.SandboxManager;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * NonDeterministicService handles all non-deterministic services in XDN.
 *
 * Non-deterministic services use primary-backup replication:
 *   - Only the primary executes requests against its Docker container.
 *   - After execution, the recorder captures the filesystem state diff.
 *   - PrimaryBackupManager proposes the diff via Paxos to all replicas.
 *   - Backup replicas apply the diff via applyStatediff().
 *
 * State transfer during reconfiguration uses the getFinalState() → restore()
 * path (rsync + tar), bypassing GigaPaxos's checkpoint() mechanism entirely.
 * checkpoint() returns a stub — it is intentionally unused for this service type.
 *
 * Lifecycle (restore() cases):
 *   xdn:init:…             → createServiceInstance() only (no container yet)
 *                            PBM handles actual container startup via
 *                            nondeter:start: and nondeter:startbackup: prefixes
 *   nondeter:create:…      → same as xdn:init:, legacy PBM prefix
 *   nondeter:start:        → initContainerizedService2() on primary
 *   nondeter:startbackup:  → postInitialization() on backup (recorder apply-side)
 *   xdn:final:…            → reviveContainerizedService() on reconfiguration
 *   null                   → wipe state (epoch end)
 */
public class NonDeterministicService {

    /**
     * Returned by checkpoint() for non-deterministic services.
     * GigaPaxos uses checkpoint() only for Paxos log truncation and
     * single-node crash recovery. For non-deterministic services,
     * reconfiguration state transfer uses getFinalState() instead,
     * so a stub here is intentional.
     *
     * Note: single-node crash recovery is a known gap — see paxos-reconfiguration.md.
     */
    public static final String CHECKPOINT_STUB = "xdn:nondeter:checkpoint:stub";

    private final String myNodeId;

    // serviceName → current placement epoch
    private final Map<String, Integer> servicePlacementEpoch = new ConcurrentHashMap<>();

    // serviceName → ServiceInstance
    private final Map<String, ServiceInstance> serviceInstances = new ConcurrentHashMap<>();

    // Shared HTTP request cache
    private final Map<Long, Request> requestCache;

    // HTTP forwarder: sends requests to the local container
    private final XdnHttpForwarderClient httpForwarderClient;

    // State diff recorder: captures and applies filesystem changes
    // Initialized lazily per service via SandboxManager
    private final AbstractStateDiffRecorder stateDiffRecorder;

    private final SandboxManager sandboxManager;

    private static final String DBG_HDR_NO_OP_EXECUTION = "___DNO";
    public static final boolean TIMING_HEADERS_ENABLED =
            Boolean.getBoolean("XDN_TIMING_HEADERS");

    private final Logger logger = Logger.getLogger(NonDeterministicService.class.getName());

    public NonDeterministicService(String myNodeId, Map<Long, Request> requestCache,
                                   SandboxManager sandboxManager,
                                   AbstractStateDiffRecorder stateDiffRecorder) {
        this.myNodeId = myNodeId;
        this.requestCache = requestCache;
        this.sandboxManager = sandboxManager;
        this.stateDiffRecorder = stateDiffRecorder;
        this.httpForwarderClient = new XdnHttpForwarderClient();
    }

    // -------------------------------------------------------------------------
    // execute()
    // -------------------------------------------------------------------------

    /**
     * Called by GigaPaxos (via PBM) on the primary after it wins the right
     * to execute the request. Forwards to the local container.
     *
     * Backup replicas do not call execute() for HTTP requests — they receive
     * state diffs via applyStatediff() instead.
     */
    public boolean execute(Request request) {
        String serviceName = request.getServiceName();

        if (request instanceof XdnHttpRequest xdnHttpRequest) {
            if (xdnHttpRequest.getHttpRequest().headers().contains(DBG_HDR_NO_OP_EXECUTION)) {
                xdnHttpRequest.setHttpResponse(buildNoOpResponse());
                return true;
            }

            forwardToContainer(xdnHttpRequest);
            requestCache.remove(xdnHttpRequest.getRequestID());
            return true;
        }

        if (request instanceof XdnHttpRequestBatch batch) {
            forwardBatchToContainer(batch);
            requestCache.remove(batch.getRequestID());
            return true;
        }

        if (request instanceof XdnStopRequest stopRequest) {
            int epoch = stopRequest.getEpochNumber();
            // Capture final state before stopping. This is consumed by
            // getFinalState() during the reconfiguration START phase.
            captureFinalState(serviceName, epoch);
            return stopContainerInstance(serviceName, epoch);
        }

        logger.log(Level.WARNING,
                "{0}:NonDeterministicService unknown request type={1}",
                new Object[]{myNodeId, request.getClass().getSimpleName()});
        return false;
    }

    // -------------------------------------------------------------------------
    // checkpoint() — intentional stub
    // -------------------------------------------------------------------------

    /**
     * Not used for state transfer. Returns a stub so GigaPaxos does not
     * crash on nil checkpoints. See CHECKPOINT_STUB javadoc.
     */
    public String checkpoint(String name) {
        return CHECKPOINT_STUB;
    }

    // -------------------------------------------------------------------------
    // restore()
    // -------------------------------------------------------------------------

    /**
     * Routes based on state prefix:
     *
     *   null                   → wipe state
     *   xdn:init:…             → createServiceInstance() only, no container
     *   nondeter:create:…      → same as above (legacy PBM prefix)
     *   nondeter:start:        → start container on primary
     *   nondeter:startbackup:  → start recorder apply-side on backup
     *   xdn:final:…            → revive container from previous epoch state
     */
    public boolean restore(String name, String state) {
        if (state == null) {
            Integer epoch = servicePlacementEpoch.get(name);
            if (epoch == null) return true;
            return deleteContainerInstance(name, epoch);
        }

        // Strip legacy nondeter:create: prefix (added by PBM before passing to Paxos)
        if (state.startsWith(ServiceProperty.NON_DETERMINISTIC_CREATE_PREFIX)) {
            state = state.substring(ServiceProperty.NON_DETERMINISTIC_CREATE_PREFIX.length());
        }

        if (state.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)) {
            // Metadata setup only — PBM calls nondeter:start: separately to
            // start the actual container after primary election completes.
            return createServiceInstance(name, state);
        }

        if (state.startsWith(ServiceProperty.NON_DETERMINISTIC_START_PREFIX)
                && !state.startsWith(ServiceProperty.NON_DETERMINISTIC_START_BACKUP_PREFIX)) {
            // Primary won the election — start the container
            return startContainerAsPrimary(name);
        }

        if (state.startsWith(ServiceProperty.NON_DETERMINISTIC_START_BACKUP_PREFIX)) {
            // This node is a backup — start the recorder apply-side
            return startRecorderAsBackup(name);
        }

        if (state.startsWith(ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX)) {
            // Reconfiguration: restore from previous epoch's final state
            return reviveFromFinalState(name, state);
        }

        logger.log(Level.WARNING,
                "{0}:NonDeterministicService restore() unknown prefix for {1}: {2}",
                new Object[]{myNodeId, name, state});
        return false;
    }

    // -------------------------------------------------------------------------
    // BackupableApplication methods
    // -------------------------------------------------------------------------

    /**
     * Called by PBM's capture thread after a batch of requests complete.
     * Atomically gets and clears all filesystem changes since the last capture.
     */
    public byte[] captureStatediff(String serviceName) {
        Integer epoch = servicePlacementEpoch.get(serviceName);
        if (epoch == null) {
            logger.log(Level.WARNING,
                    "{0}:NonDeterministicService captureStatediff() unknown service={1}",
                    new Object[]{myNodeId, serviceName});
            return null;
        }
        return stateDiffRecorder.captureStateDiff(serviceName, epoch);
    }

    /**
     * Called by PBM on each backup after Paxos commits an ApplyStateDiffPacket.
     * Applied on a single-threaded executor to preserve ordering.
     */
    public boolean applyStatediff(String serviceName, byte[] statediff) {
        Integer epoch = servicePlacementEpoch.get(serviceName);
        if (epoch == null) {
            logger.log(Level.WARNING,
                    "{0}:NonDeterministicService applyStatediff() unknown service={1}",
                    new Object[]{myNodeId, serviceName});
            return false;
        }
        return stateDiffRecorder.applyStateDiff(serviceName, epoch, statediff);
    }

    // -------------------------------------------------------------------------
    // Reconfigurable helpers
    // -------------------------------------------------------------------------

    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        return new XdnStopRequest(name, epoch);
    }

    /**
     * Called by AR.handleRequestEpochFinalState() during reconfiguration.
     * Tars the container's bind-mounted state directory and returns it as a
     * Base64 string (or a URL for large states).
     *
     * Format: xdn:final:<epoch>::<servicePropertyJSON>::<base64tar>
     *                                                or url:<url>
     */
    public String getFinalState(String name, int epoch) {
        ServiceInstance instance = serviceInstances.get(name);
        if (instance == null) {
            logger.log(Level.WARNING,
                    "{0}:NonDeterministicService getFinalState() unknown service={1}",
                    new Object[]{myNodeId, name});
            return null;
        }

        // Ensure final state is captured (may already exist from XdnStopRequest)
        String finalStatePath = String.format(
                "/tmp/xdn2/final/%s/%s/%d/final_state.tar",
                myNodeId, name, epoch);
        if (!new java.io.File(finalStatePath).exists()) {
            boolean captured = captureFinalState(name, epoch);
            if (!captured) return null;
        }

        String finalStatePrefix = String.format("%s%d:",
                ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX, epoch);

        java.io.File archive = new java.io.File(finalStatePath);
        int maxInlineBytes = 62500; // ~500 KB

        if (archive.length() <= maxInlineBytes) {
            try {
                byte[] bytes = Files.readAllBytes(Paths.get(finalStatePath));
                return String.format("%s::%s::%s",
                        finalStatePrefix,
                        instance.property.toJsonString(),
                        Base64.getEncoder().encodeToString(bytes));
            } catch (IOException e) {
                logger.log(Level.WARNING,
                        "{0}:NonDeterministicService getFinalState() I/O error: {1}",
                        new Object[]{myNodeId, e.getMessage()});
                return null;
            }
        } else {
            // Large state: pass by URL instead of inline Base64
            return ((edu.umass.cs.xdn2.sandbox.DockerSandboxManager) sandboxManager)
                    .buildFinalStateString(name, epoch, finalStatePath,
                            instance.property.toJsonString());
        }
    }

    public boolean deleteFinalState(String name, int epoch) {
        return deleteContainerInstance(name, epoch);
    }

    public Integer getEpoch(String name) {
        return servicePlacementEpoch.get(name);
    }

    // -------------------------------------------------------------------------
    // Public helpers
    // -------------------------------------------------------------------------

    public boolean hostsService(String serviceName) {
        return serviceInstances.containsKey(serviceName);
    }

    public ServiceInstance getServiceInstance(String serviceName) {
        return serviceInstances.get(serviceName);
    }

    public boolean isPrimary(String serviceName) {
        // Delegated from XdnApp; actual primary tracking is in PBM.
        // NonDeterministicService itself does not track role — PBM does.
        // This method exists so XdnApp can expose it without knowing PBM directly.
        // XdnReplicaCoordinator queries PBM via PBRC.isPrimary() instead.
        return false; // placeholder — XRC queries PBRC directly
    }

    // -------------------------------------------------------------------------
    // Private: service lifecycle
    // -------------------------------------------------------------------------

    /**
     * Parses and stores the ServiceInstance metadata.
     * Does NOT start the container — PBM drives that separately via
     * nondeter:start: (primary) or nondeter:startbackup: (backup).
     */
    private boolean createServiceInstance(String name, String state) {
        String encoded = state.substring(ServiceProperty.XDN_INITIAL_STATE_PREFIX.length());
        ServiceProperty property;
        try {
            property = ServiceProperty.createFromJsonString(encoded);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    "{0}:NonDeterministicService failed to parse ServiceProperty: {1}",
                    new Object[]{myNodeId, e.getMessage()});
            return false;
        }

        int epoch = 0;
        int allocatedPort = sandboxManager.allocatePort();
        String networkName = String.format("net::%s:%s", myNodeId, name);
        List<String> containerNames = buildContainerNames(name, epoch, property);
        ServiceInstance instance = new ServiceInstance(
                property, name, networkName, allocatedPort, containerNames);
        sandboxManager.prepareStateDirectory(name, epoch);
        stateDiffRecorder.preInitialization(name, epoch);

        serviceInstances.put(name, instance);
        servicePlacementEpoch.put(name, epoch);

        logger.log(Level.INFO,
                "{0}:NonDeterministicService created instance for {1} (no container yet)",
                new Object[]{myNodeId, name});
        return true;
    }

    /**
     * Starts the Docker container on the primary.
     * Called from restore("nondeter:start:") after PBM confirms this node is primary.
     */
    private boolean startContainerAsPrimary(String name) {
        ServiceInstance instance = serviceInstances.get(name);
        if (instance == null) {
            logger.log(Level.SEVERE,
                    "{0}:NonDeterministicService startContainerAsPrimary() " +
                            "called before createServiceInstance() for {1}",
                    new Object[]{myNodeId, name});
            return false;
        }

        Integer epoch = servicePlacementEpoch.get(name);
        if (epoch == null) return false;

        sandboxManager.createNetwork(name);
        boolean started = sandboxManager.startService(instance, epoch);
        if (!started) return false;

        boolean postInit = stateDiffRecorder.postInitialization(name, epoch);
        if (!postInit) return false;

        logger.log(Level.INFO,
                "{0}:NonDeterministicService container started as primary for {1}",
                new Object[]{myNodeId, name});
        return true;
    }

    /**
     * Starts the recorder apply-side on a backup node.
     * Called from restore("nondeter:startbackup:") after PBM sends InitBackupPacket.
     * No container is started — backups only apply state diffs.
     */
    private boolean startRecorderAsBackup(String name) {
        Integer epoch = servicePlacementEpoch.get(name);
        if (epoch == null) {
            logger.log(Level.WARNING,
                    "{0}:NonDeterministicService startRecorderAsBackup() " +
                            "called before createServiceInstance() for {1}",
                    new Object[]{myNodeId, name});
            return false;
        }

        boolean started = stateDiffRecorder.postInitialization(name, epoch);

        logger.log(Level.INFO,
                "{0}:NonDeterministicService recorder apply-side started for backup {1}: {2}",
                new Object[]{myNodeId, name, started});
        return started;
    }

    /**
     * Restores a service from a previous epoch's final state during reconfiguration.
     * Unpacks the tar into the new epoch's state directory, then starts the container.
     *
     * Format: xdn:final:<epoch>::<servicePropertyJSON>::<base64tar|url:...>
     */
    private boolean reviveFromFinalState(String name, String state) {
        String[] parts = state.split("::");
        if (parts.length < 3) {
            logger.log(Level.SEVERE,
                    "{0}:NonDeterministicService malformed xdn:final state for {1}",
                    new Object[]{myNodeId, name});
            return false;
        }

        String prefixPart = parts[0];
        String encodedProperty = parts[1];
        String encodedState = parts[2];

        int prevEpoch;
        try {
            String[] tokens = prefixPart.split(":");
            prevEpoch = Integer.parseInt(tokens[tokens.length - 1]);
        } catch (NumberFormatException e) {
            logger.log(Level.SEVERE,
                    "{0}:NonDeterministicService could not parse epoch from: {1}",
                    new Object[]{myNodeId, prefixPart});
            return false;
        }
        int newEpoch = prevEpoch + 1;

        ServiceProperty property;
        try {
            property = ServiceProperty.createFromJsonString(encodedProperty);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    "{0}:NonDeterministicService failed to parse ServiceProperty: {1}",
                    new Object[]{myNodeId, e.getMessage()});
            return false;
        }

        // Pre-initialize the recorder for the new epoch
        stateDiffRecorder.preInitialization(name, newEpoch);
        stateDiffRecorder.removeServiceRecorder(name, newEpoch);

        // Restore the state directory from Base64 tar or URL
        boolean stateRestored = sandboxManager.restoreStateFromSnapshot(
                name, newEpoch, encodedState);
        if (!stateRestored) return false;

        int allocatedPort = sandboxManager.allocatePort();
        String networkName = String.format("net::%s:%s", myNodeId, name);
        List<String> containerNames = buildContainerNames(name, newEpoch, property);
        ServiceInstance instance = new ServiceInstance(
                property, name, networkName, allocatedPort, containerNames);

        sandboxManager.createNetwork(name);
        boolean started = sandboxManager.startService(instance, newEpoch);
        if (!started) return false;

        stateDiffRecorder.postInitialization(name, newEpoch);

        serviceInstances.put(name, instance);
        servicePlacementEpoch.put(name, newEpoch);
        return true;
    }

    private boolean stopContainerInstance(String name, int epoch) {
        ServiceInstance instance = serviceInstances.get(name);
        if (instance == null) return true;
        return sandboxManager.stopService(instance);
    }

    private boolean deleteContainerInstance(String name, int epoch) {
        ServiceInstance instance = serviceInstances.get(name);
        if (instance == null) return true;
        boolean deleted = sandboxManager.deleteService(instance);
        if (deleted) {
            serviceInstances.remove(name);
            servicePlacementEpoch.remove(name);
            stateDiffRecorder.removeServiceRecorder(name, epoch);
        }
        return deleted;
    }

    /**
     * Captures the current state directory into a tar archive.
     * Called from execute(XdnStopRequest) and getFinalState().
     */
    private boolean captureFinalState(String name, int epoch) {
        ServiceInstance instance = serviceInstances.get(name);
        if (instance == null) return false;
        String capturedPath = sandboxManager.captureStateSnapshot(name, epoch);
        return capturedPath != null;
    }

    // -------------------------------------------------------------------------
    // Private: request forwarding
    // -------------------------------------------------------------------------

    private void forwardToContainer(XdnHttpRequest xdnRequest) {
        String serviceName = xdnRequest.getServiceName();
        ServiceInstance instance = serviceInstances.get(serviceName);
        if (instance == null) {
            logger.log(Level.WARNING,
                    "{0}:NonDeterministicService no instance for service={1}",
                    new Object[]{myNodeId, serviceName});
            return;
        }
        try {
            FullHttpResponse response = httpForwarderClient.execute("127.0.0.1",
                    instance.allocatedHttpPort, copyHttpRequest(xdnRequest));
            xdnRequest.setHttpResponse(response);

            if (TIMING_HEADERS_ENABLED && response != null) {
                response.headers().set("X-E-EXC-TS-" + myNodeId,
                        String.valueOf(System.nanoTime()));
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    "{0}:NonDeterministicService forward failed for {1}: {2}",
                    new Object[]{myNodeId, serviceName, e.getMessage()});
        }
    }

    private void forwardBatchToContainer(XdnHttpRequestBatch batch) {
        String serviceName = batch.getServiceName();
        ServiceInstance instance = serviceInstances.get(serviceName);
        if (instance == null) return;
        try {
            java.util.List<FullHttpRequest> requests = new java.util.ArrayList<>();
            for (XdnHttpRequest r : batch.getRequestList()) {
                requests.add(copyHttpRequest(r));
            }
            java.util.List<FullHttpResponse> responses = httpForwarderClient.executePipelined(
                    "127.0.0.1", instance.allocatedHttpPort, requests);
            java.util.List<XdnHttpRequest> batchRequests = batch.getRequestList();
            for (int i = 0; i < batchRequests.size() && i < responses.size(); i++) {
                batchRequests.get(i).setHttpResponse(responses.get(i));
            }
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    "{0}:NonDeterministicService batch forward failed for {1}: {2}",
                    new Object[]{myNodeId, serviceName, e.getMessage()});
        }
    }

    private FullHttpRequest copyHttpRequest(XdnHttpRequest xdnRequest) {
        HttpRequest original = xdnRequest.getHttpRequest();
        HttpContent content = xdnRequest.getHttpRequestContent();
        FullHttpRequest copy = new DefaultFullHttpRequest(
                original.protocolVersion(),
                original.method(),
                original.uri(),
                content.content().copy());
        copy.headers().setAll(original.headers());
        return copy;
    }

    private FullHttpResponse buildNoOpResponse() {
        io.netty.buffer.ByteBuf content =
                Unpooled.copiedBuffer("NoOp".getBytes());
        FullHttpResponse r = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
        r.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
        return r;
    }

    private List<String> buildContainerNames(String serviceName, int epoch,
                                             ServiceProperty property) {
        List<String> names = new java.util.ArrayList<>();
        for (int i = 0; i < property.getComponents().size(); i++) {
            names.add(String.format("c%d.e%d.%s.%s.xdn.io",
                    i, epoch, serviceName, myNodeId));
        }
        return names;
    }
}