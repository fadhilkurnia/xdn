package edu.umass.cs.xdn2.service;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.xdn2.XdnHttpForwarderClient;
import edu.umass.cs.xdn2.request.XdnHttpRequest;
import edu.umass.cs.xdn2.request.XdnHttpRequestBatch;
import edu.umass.cs.xdn2.request.XdnStopRequest;
import edu.umass.cs.xdn2.sandbox.SandboxManager;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * DeterministicService handles all deterministic services in XDN.
 *
 * Deterministic services use active replication: GigaPaxos coordinates the
 * request through Paxos and calls execute() on every replica. Each replica
 * forwards the request to its local Docker container and they all converge to
 * the same state because the application is deterministic.
 *
 * State transfer uses the standard GigaPaxos checkpoint() → restore() path.
 * getFinalState() is not needed — PaxosReplicaCoordinator handles it internally.
 *
 * Lifecycle:
 *   restore("xdn:init:…")      → createServiceInstance() + start container
 *   restore("xdn:final:…")     → revive container from previous epoch state
 *   restore(checkpointStr)      → reinstall from Paxos checkpoint
 *   restore(null)               → wipe state (end of epoch)
 *   execute(XdnHttpRequest)     → forward to local container, return response
 *   execute(XdnStopRequest)     → capture final state + stop container
 *   checkpoint(name)            → serialize current service state
 */
public class DeterministicService {

    private final String myNodeId;

    // serviceName → current placement epoch
    private final Map<String, Integer> servicePlacementEpoch = new ConcurrentHashMap<>();

    // serviceName → ServiceInstance (contains property, port, container names, etc.)
    private final Map<String, ServiceInstance> serviceInstances = new ConcurrentHashMap<>();

    // Shared HTTP request cache (keyed by requestID, shared with XdnApp)
    private final Map<Long, Request> requestCache;

    // HTTP forwarder: sends requests to the local container
    private final edu.umass.cs.xdn2.XdnHttpForwarderClient httpForwarderClient;

    // Backdoor header: skip real execution and return a NoOp response.
    // Used to measure coordination overhead in isolation.
    private static final String DBG_HDR_NO_OP_EXECUTION = "___DNO";

    public static final boolean TIMING_HEADERS_ENABLED =
            Boolean.getBoolean("XDN_TIMING_HEADERS");

    private final Logger logger = Logger.getLogger(DeterministicService.class.getName());

    private final SandboxManager sandboxManager;

    public DeterministicService(String myNodeId, Map<Long, Request> requestCache,
                                SandboxManager sandboxManager) {
        this.myNodeId = myNodeId;
        this.requestCache = requestCache;
        this.sandboxManager = sandboxManager;
        this.httpForwarderClient = new XdnHttpForwarderClient();
    }

    // -------------------------------------------------------------------------
    // execute()
    // -------------------------------------------------------------------------

    /**
     * Called by GigaPaxos on every replica after Paxos commits the request.
     * Forwards the HTTP request to the local container and stores the response.
     * Only the entry replica sends the response back to the client.
     */
    public boolean execute(Request request) {
        String serviceName = request.getServiceName();

        if (request instanceof XdnHttpRequest xdnHttpRequest) {
            // NoOp shortcut for coordination overhead benchmarking
            if (xdnHttpRequest.getHttpRequest().headers().contains(DBG_HDR_NO_OP_EXECUTION)) {
                xdnHttpRequest.setHttpResponse(buildNoOpResponse());
                return true;
            }

            forwardToContainer(xdnHttpRequest);

            // Non-entry replicas discard the response: they executed to update
            // state, not to serve the client.
            if (xdnHttpRequest.getHttpResponse() != null
                    && xdnHttpRequest.isCreatedFromString()) {
                ReferenceCountUtil.release(xdnHttpRequest.getHttpResponse());
            }

            requestCache.remove(xdnHttpRequest.getRequestID());
            return true;
        }

        if (request instanceof XdnHttpRequestBatch batch) {
            forwardBatchToContainer(batch);
            if (batch.isCreatedFromBytes()) {
                for (XdnHttpRequest r : batch.getRequestList()) {
                    if (r.getHttpResponse() != null) {
                        ReferenceCountUtil.release(r.getHttpResponse());
                    }
                }
            }
            requestCache.remove(batch.getRequestID());
            return true;
        }

        if (request instanceof XdnStopRequest stopRequest) {
            // For deterministic services the stop request only needs to stop
            // the container. Final state capture uses checkpoint(), not rsync.
            return stopContainerInstance(serviceName,
                    stopRequest.getEpochNumber());
        }

        logger.log(Level.WARNING,
                "{0}:DeterministicService unknown request type={1}",
                new Object[]{myNodeId, request.getClass().getSimpleName()});
        return false;
    }

    // -------------------------------------------------------------------------
    // checkpoint() — real implementation for deterministic services
    // -------------------------------------------------------------------------

    /**
     * Returns a serialized snapshot of the service state.
     * GigaPaxos uses this for Paxos log truncation and crash recovery.
     *
     * For deterministic services the state is fully reproducible by re-executing
     * requests, so the checkpoint only needs to capture enough to avoid replaying
     * from the beginning of the log. The service's bind-mounted state directory
     * is tarred and Base64-encoded, mirroring the reconfiguration path.
     *
     * TODO: for stateless deterministic services this can return a lightweight
     * version-stamp instead of a full snapshot.
     */
    public String checkpoint(String name) {
        ServiceInstance instance = serviceInstances.get(name);
        if (instance == null) {
            logger.log(Level.WARNING,
                    "{0}:DeterministicService checkpoint() for unknown service={1}",
                    new Object[]{myNodeId, name});
            return null;
        }
        Integer epoch = servicePlacementEpoch.get(name);
        if (epoch == null) return null;

        // Capture the current state directory into a tar snapshot
        String snapshotPath = captureStateSnapshot(name, epoch);
        if (snapshotPath == null) return null;

        // Return as: xdn:checkpoint:<epoch>::<servicePropertyJSON>::<base64>
        try {
            byte[] bytes = java.nio.file.Files.readAllBytes(
                    java.nio.file.Paths.get(snapshotPath));
            String base64 = java.util.Base64.getEncoder().encodeToString(bytes);
            return String.format("%s%d::%s::%s",
                    ServiceProperty.XDN_CHECKPOINT_PREFIX,
                    epoch,
                    instance.property.toJsonString(),
                    base64);
        } catch (java.io.IOException e) {
            logger.log(Level.WARNING,
                    "{0}:DeterministicService checkpoint() I/O error for {1}: {2}",
                    new Object[]{myNodeId, name, e.getMessage()});
            return null;
        }
    }

    // -------------------------------------------------------------------------
    // restore()
    // -------------------------------------------------------------------------

    /**
     * Called by GigaPaxos to initialize or restore a service.
     *
     * Handled cases:
     *   null          → wipe state (epoch end)
     *   xdn:init:…    → fresh creation
     *   xdn:final:…   → reconfiguration from previous epoch's final state
     *   xdn:checkpoint:… → recovery from Paxos checkpoint
     */
    public boolean restore(String name, String state) {
        if (state == null) {
            // Epoch ended — clean up the container
            Integer epoch = servicePlacementEpoch.get(name);
            if (epoch == null) return true;
            return deleteContainerInstance(name, epoch);
        }

        if (state.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)) {
            return createAndStart(name, state);
        }

        if (state.startsWith(ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX)) {
            return reviveFromFinalState(name, state);
        }

        if (state.startsWith(ServiceProperty.XDN_CHECKPOINT_PREFIX)) {
            return restoreFromCheckpoint(name, state);
        }

        logger.log(Level.WARNING,
                "{0}:DeterministicService restore() unknown state prefix for {1}: {2}",
                new Object[]{myNodeId, name, state});
        return false;
    }

    // -------------------------------------------------------------------------
    // Reconfigurable helpers
    // -------------------------------------------------------------------------

    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        return new XdnStopRequest(name, epoch);
    }

    /**
     * For deterministic services, getFinalState() should never be called because
     * PaxosReplicaCoordinator handles state transfer internally.
     * This is a safety fallback that delegates to checkpoint().
     */
    public String getFinalState(String name, int epoch) {
        logger.log(Level.WARNING,
                "{0}:DeterministicService getFinalState() called — " +
                        "this is unexpected for deterministic services. " +
                        "Falling back to checkpoint().",
                new Object[]{myNodeId});
        return checkpoint(name);
    }

    public boolean deleteFinalState(String name, int epoch) {
        return deleteContainerInstance(name, epoch);
    }

    public Integer getEpoch(String name) {
        return servicePlacementEpoch.get(name);
    }

    // -------------------------------------------------------------------------
    // Public helpers used by XdnApp
    // -------------------------------------------------------------------------

    public boolean hostsService(String serviceName) {
        return serviceInstances.containsKey(serviceName);
    }

    public ServiceInstance getServiceInstance(String serviceName) {
        return serviceInstances.get(serviceName);
    }

    // -------------------------------------------------------------------------
    // Private: service lifecycle
    // -------------------------------------------------------------------------

    /**
     * Parses the ServiceProperty, allocates container names and port,
     * then starts the Docker container.
     */
    private boolean createAndStart(String name, String state) {
        String encoded = state.substring(ServiceProperty.XDN_INITIAL_STATE_PREFIX.length());
        ServiceProperty property;
        try {
            property = ServiceProperty.createFromJsonString(encoded);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    "{0}:DeterministicService failed to parse ServiceProperty: {1}",
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
        sandboxManager.createNetwork(name);
        boolean started = sandboxManager.startService(instance, epoch);
        if (!started) return false;

        serviceInstances.put(name, instance);
        servicePlacementEpoch.put(name, epoch);
        return true;
    }

    /**
     * Restores a service from a previous epoch's final state.
     * Used during reconfiguration when this node joins a new epoch.
     *
     * Format: xdn:final:<epoch>::<servicePropertyJSON>::<base64tar>
     */
    private boolean reviveFromFinalState(String name, String state) {
        String[] parts = state.split("::");
        if (parts.length < 3) {
            logger.log(Level.SEVERE,
                    "{0}:DeterministicService malformed xdn:final state for {1}",
                    new Object[]{myNodeId, name});
            return false;
        }

        String prefixPart = parts[0]; // "xdn:final:<epoch>"
        String encodedProperty = parts[1];
        String encodedState = parts[2];

        int prevEpoch;
        try {
            String[] prefixTokens = prefixPart.split(":");
            prevEpoch = Integer.parseInt(prefixTokens[prefixTokens.length - 1]);
        } catch (NumberFormatException e) {
            logger.log(Level.SEVERE,
                    "{0}:DeterministicService could not parse epoch from: {1}",
                    new Object[]{myNodeId, prefixPart});
            return false;
        }
        int newEpoch = prevEpoch + 1;

        ServiceProperty property;
        try {
            property = ServiceProperty.createFromJsonString(encodedProperty);
        } catch (Exception e) {
            logger.log(Level.SEVERE,
                    "{0}:DeterministicService failed to parse ServiceProperty: {1}",
                    new Object[]{myNodeId, e.getMessage()});
            return false;
        }

        // Restore the state directory from the tar
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

        serviceInstances.put(name, instance);
        servicePlacementEpoch.put(name, newEpoch);
        return true;
    }

    /**
     * Restores a service from a Paxos checkpoint string.
     * Used for crash recovery on a single node.
     *
     * Format: xdn:checkpoint:<epoch>::<servicePropertyJSON>::<base64tar>
     */
    private boolean restoreFromCheckpoint(String name, String state) {
        // Same structure as xdn:final, just a different prefix
        String withoutPrefix = state.substring(ServiceProperty.XDN_CHECKPOINT_PREFIX.length());
        return reviveFromFinalState(name,
                ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX + withoutPrefix);
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
        }
        return deleted;
    }

    private String captureStateSnapshot(String name, int epoch) {
        ServiceInstance instance = serviceInstances.get(name);
        if (instance == null) return null;
        return sandboxManager.captureStateSnapshot(name, epoch);
    }

    // -------------------------------------------------------------------------
    // Private: request forwarding
    // -------------------------------------------------------------------------

    private void forwardToContainer(XdnHttpRequest xdnRequest) {
        String serviceName = xdnRequest.getServiceName();
        ServiceInstance instance = serviceInstances.get(serviceName);
        if (instance == null) {
            logger.log(Level.WARNING,
                    "{0}:DeterministicService no instance for service={1}",
                    new Object[]{myNodeId, serviceName});
            return;
        }
        try {
            FullHttpResponse response = httpForwarderClient.execute(
                    "127.0.0.1", instance.allocatedHttpPort,
                    copyHttpRequest(xdnRequest));
            xdnRequest.setHttpResponse(response);
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    "{0}:DeterministicService forward failed for {1}: {2}",
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
                    "{0}:DeterministicService batch forward failed for {1}: {2}",
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
        List<String> names = new ArrayList<>();
        for (int i = 0; i < property.getComponents().size(); i++) {
            names.add(String.format("c%d.e%d.%s.%s.xdn.io",
                    i, epoch, serviceName, myNodeId));
        }
        return names;
    }
}
