package edu.umass.cs.xdn2;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.interfaces.InitialStateValidator;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.xdn2.recorder.AbstractStateDiffRecorder;
import edu.umass.cs.xdn2.request.XdnHttpRequest;
import edu.umass.cs.xdn2.request.XdnHttpRequestBatch;
import edu.umass.cs.xdn2.request.XdnRequestType;
import edu.umass.cs.xdn2.request.XdnStopRequest;
import edu.umass.cs.xdn2.sandbox.SandboxManager;
import edu.umass.cs.xdn2.service.DeterministicService;
import edu.umass.cs.xdn2.service.NonDeterministicService;
import edu.umass.cs.xdn2.service.ServiceProperty;
import edu.umass.cs.xdn2.utils.Shell;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * XdnApp is the top-level GigaPaxos APPLICATION class for XDN.
 *
 * It replaces XdnGigapaxosApp with a clean separation between deterministic
 * and non-deterministic services. It owns:
 *   - a serviceRegistry mapping serviceName → ServiceType
 *   - a DeterministicService instance handling all deterministic services
 *   - a NonDeterministicService instance handling all non-deterministic services
 *   - a shared HTTP request cache (shared between both service types)
 *
 * GigaPaxos sees exactly one APPLICATION class and calls execute(),
 * checkpoint(), and restore() on this class. XdnApp routes each call to the
 * correct service based on the registry, which is populated on the first
 * restore() call per service.
 */
public class XdnApp
        implements Replicable, Reconfigurable, BackupableApplication,
        InitialStateValidator {

    public enum ServiceType {
        DETERMINISTIC,
        NON_DETERMINISTIC
    }

    private final Set<String> internalGpGroups;

    private final String myNodeId;

    // Registry: populated on first restore() per service
    private final Map<String, ServiceType> serviceRegistry = new ConcurrentHashMap<>();

    // The two service handlers
    private final DeterministicService deterministicService;
    private final NonDeterministicService nonDeterministicService;

    private final SandboxManager sandboxManager;

    // Shared HTTP request cache — avoids re-deserializing requests on
    // the entry replica when GigaPaxos passes them back through getRequest()
    private static final int REQUEST_CACHE_CAPACITY = 4096;
    private final Map<Long, Request> requestCache =
            Collections.synchronizedMap(
                    new LinkedHashMap<>(REQUEST_CACHE_CAPACITY, 0.75f, true) {
                        @Override
                        protected boolean removeEldestEntry(
                                Map.Entry<Long, Request> eldest) {
                            return size() > REQUEST_CACHE_CAPACITY;
                        }
                    });

    // Request types this app handles
    private final Set<IntegerPacketType> packetTypes;

    private final Logger logger = Logger.getLogger(XdnApp.class.getName());

    public XdnApp(String[] args) {
        assert args.length > 0 : "XdnApp requires at least one arg: the nodeId";
        this.myNodeId = args[args.length - 1].toLowerCase();

        logger.log(Level.INFO, "{0}:XdnApp initializing with args: {1}",
                new Object[]{myNodeId, Arrays.toString(args)});

        // Verify Docker is available before doing anything else
        checkDockerAvailable();

        // GigaPaxos internal group names that must not be routed to either service
        this.internalGpGroups = Set.of(
                PaxosConfig.getDefaultServiceName(),
                AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString(),
                AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString()
        );

        // Load XDN config and create shared infrastructure
        XdnConfig config = new XdnConfig();
        this.sandboxManager = SandboxManager.create(config, myNodeId);
        AbstractStateDiffRecorder recorder = AbstractStateDiffRecorder.create(config, myNodeId);

        this.deterministicService = new DeterministicService(
                myNodeId, requestCache, sandboxManager);
        this.nonDeterministicService = new NonDeterministicService(
                myNodeId, requestCache, sandboxManager, recorder);

        this.packetTypes = new HashSet<>();
        this.packetTypes.add(XdnRequestType.XDN_SERVICE_HTTP_REQUEST);
        this.packetTypes.add(XdnRequestType.XDN_HTTP_REQUEST_BATCH);
        this.packetTypes.add(XdnRequestType.XDN_STOP_REQUEST);
    }

    // -------------------------------------------------------------------------
    // Replicable interface
    // -------------------------------------------------------------------------

    @Override
    public boolean execute(Request request) {
        String serviceName = request.getServiceName();

        if (isInternalGroup(serviceName)) return true;

        ServiceType type = serviceRegistry.get(serviceName);
        if (type == null) {
            logger.log(Level.WARNING,
                    "{0}:XdnApp execute() for unknown service={1}",
                    new Object[]{myNodeId, serviceName});
            return false;
        }

        return switch (type) {
            case DETERMINISTIC     -> deterministicService.execute(request);
            case NON_DETERMINISTIC -> nonDeterministicService.execute(request);
        };
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
        return execute(request);
    }

    @Override
    public String checkpoint(String name) {
        if (isInternalGroup(name)) return null;

        ServiceType type = serviceRegistry.get(name);
        if (type == null) {
            logger.log(Level.WARNING,
                    "{0}:XdnApp checkpoint() for unknown service={1}",
                    new Object[]{myNodeId, name});
            return null;
        }

        return switch (type) {
            case DETERMINISTIC     -> deterministicService.checkpoint(name);
            case NON_DETERMINISTIC -> NonDeterministicService.CHECKPOINT_STUB;
        };
    }

    @Override
    public boolean restore(String name, String state) {
        if (isInternalGroup(name)) return true;

        // null state: GigaPaxos wipes the service (epoch end)
        if (state == null) {
            ServiceType type = serviceRegistry.get(name);
            if (type == null) return true;
            boolean result = switch (type) {
                case DETERMINISTIC     -> deterministicService.restore(name, null);
                case NON_DETERMINISTIC -> nonDeterministicService.restore(name, null);
            };
            if (result) serviceRegistry.remove(name);
            return result;
        }

        // Populate registry on first restore() for this service
        serviceRegistry.computeIfAbsent(name, n -> detectServiceType(state));

        ServiceType type = serviceRegistry.get(name);
        if (type == null) {
            logger.log(Level.WARNING,
                    "{0}:XdnApp restore() could not determine type for {1}",
                    new Object[]{myNodeId, name});
            return false;
        }

        return switch (type) {
            case DETERMINISTIC     -> deterministicService.restore(name, state);
            case NON_DETERMINISTIC -> nonDeterministicService.restore(name, state);
        };
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return packetTypes;
    }

    @Override
    public Request getRequest(String stringified) throws
            edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException {
        edu.umass.cs.xdn2.request.XdnRequestType packetType =
                edu.umass.cs.xdn2.request.XdnRequest
                        .getQuickPacketTypeFromEncodedPacket(stringified);
        if (packetType == null) {
            throw new edu.umass.cs.reconfiguration.reconfigurationutils
                    .RequestParseException(new Exception(
                    "Unknown XDN packet: " + stringified));
        }

        if (packetType.equals(XdnRequestType.XDN_SERVICE_HTTP_REQUEST)) {
            Long cachedId = XdnHttpRequest.parseRequestIdQuickly(stringified);
            if (cachedId != null) {
                Request cached = requestCache.get(cachedId);
                if (cached instanceof XdnHttpRequest xdnReq) {
                    if (xdnReq.getHttpResponse() == null
                            && XdnHttpRequest.doesHasResponse(stringified)) {
                        io.netty.handler.codec.http.HttpResponse parsed =
                                XdnHttpRequest.parseHttpResponse(stringified);
                        if (parsed != null) xdnReq.setHttpResponse(parsed);
                    }
                    return cached;
                }
            }
            XdnHttpRequest req = XdnHttpRequest.createFromString(stringified);
            return configureRequestMatchers(req);
        }

        if (packetType.equals(XdnRequestType.XDN_HTTP_REQUEST_BATCH)) {
            Long cachedId = XdnHttpRequestBatch.parseRequestIdQuickly(stringified);
            if (cachedId != null) {
                Request cached = requestCache.get(cachedId);
                if (cached instanceof XdnHttpRequestBatch) return cached;
            }
            XdnHttpRequestBatch batch = XdnHttpRequestBatch.createFromBytes(
                    stringified.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
            for (XdnHttpRequest r : batch.getRequestList()) {
                configureRequestMatchers(r);
            }
            return batch;
        }

        if (packetType.equals(XdnRequestType.XDN_STOP_REQUEST)) {
            return XdnStopRequest.createFromString(stringified);
        }

        throw new edu.umass.cs.reconfiguration.reconfigurationutils
                .RequestParseException(new Exception(
                "Unknown XDN packet type: " + packetType));
    }

    // -------------------------------------------------------------------------
    // Reconfigurable interface
    // -------------------------------------------------------------------------

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        ServiceType type = serviceRegistry.get(name);
        if (type == null) return null;
        return switch (type) {
            case DETERMINISTIC     -> deterministicService.getStopRequest(name, epoch);
            case NON_DETERMINISTIC -> nonDeterministicService.getStopRequest(name, epoch);
        };
    }

    @Override
    public String getFinalState(String name, int epoch) {
        ServiceType type = serviceRegistry.get(name);
        if (type == null) {
            logger.log(Level.WARNING,
                    "{0}:XdnApp getFinalState() for unknown service={1}",
                    new Object[]{myNodeId, name});
            return null;
        }
        return switch (type) {
            // Deterministic: PaxosReplicaCoordinator handles state transfer;
            // this is a safety fallback only
            case DETERMINISTIC     -> deterministicService.checkpoint(name);
            case NON_DETERMINISTIC -> nonDeterministicService.getFinalState(name, epoch);
        };
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {
        throw new RuntimeException("XdnApp.putInitialState is unimplemented " +
                "for name=" + name + " epoch=" + epoch);
    }

    @Override
    public boolean deleteFinalState(String name, int epoch) {
        ServiceType type = serviceRegistry.get(name);
        if (type == null) return true;
        return switch (type) {
            case DETERMINISTIC     -> deterministicService.deleteFinalState(name, epoch);
            case NON_DETERMINISTIC -> nonDeterministicService.deleteFinalState(name, epoch);
        };
    }

    @Override
    public Integer getEpoch(String name) {
        ServiceType type = serviceRegistry.get(name);
        if (type == null) return null;
        return switch (type) {
            case DETERMINISTIC     -> deterministicService.getEpoch(name);
            case NON_DETERMINISTIC -> nonDeterministicService.getEpoch(name);
        };
    }

    // -------------------------------------------------------------------------
    // BackupableApplication interface (non-deterministic path only)
    // -------------------------------------------------------------------------

    @Override
    public byte[] captureStatediff(String serviceName) {
        ServiceType type = serviceRegistry.get(serviceName);
        if (type != ServiceType.NON_DETERMINISTIC) {
            logger.log(Level.SEVERE,
                    "{0}:XdnApp captureStatediff() called for non-PB service={1}",
                    new Object[]{myNodeId, serviceName});
            return null;
        }
        return nonDeterministicService.captureStatediff(serviceName);
    }

    @Override
    public boolean applyStatediff(String serviceName, byte[] statediff) {
        ServiceType type = serviceRegistry.get(serviceName);
        if (type != ServiceType.NON_DETERMINISTIC) {
            logger.log(Level.SEVERE,
                    "{0}:XdnApp applyStatediff() called for non-PB service={1}",
                    new Object[]{myNodeId, serviceName});
            return false;
        }
        return nonDeterministicService.applyStatediff(serviceName, statediff);
    }

    // -------------------------------------------------------------------------
    // InitialStateValidator interface
    // -------------------------------------------------------------------------

    @Override
    public void validateInitialState(String initialState)
            throws InvalidInitialStateException {
        if (!initialState.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)) {
            throw new InvalidInitialStateException(
                    "Invalid initial state prefix, expecting " +
                            ServiceProperty.XDN_INITIAL_STATE_PREFIX);
        }
        String encoded = initialState.substring(
                ServiceProperty.XDN_INITIAL_STATE_PREFIX.length());
        ServiceProperty property;
        try {
            property = ServiceProperty.createFromJsonString(encoded);
        } catch (Exception e) {
            throw new InvalidInitialStateException(
                    "Invalid ServiceProperty JSON: " + e.getMessage());
        }
        for (var component : property.getComponents()) {
            String imageName = component.getImageName();
            int exitCode = Shell.runCommand("docker pull " + imageName, true);
            if (exitCode != 0) {
                throw new InvalidInitialStateException(
                        "Unknown container image '" + imageName + "'. " +
                                "Ensure the image is accessible at Docker Hub or your registry.");
            }
        }
    }

    // -------------------------------------------------------------------------
    // Public helpers used by XdnReplicaCoordinator and HttpActiveReplica
    // -------------------------------------------------------------------------

    public List<String> getContainerIds(String serviceName) {
        var instance = getServiceInstance(serviceName);
        if (instance == null) return null;
        return sandboxManager.getContainerIds(instance.containerNames);
    }

    public List<String> getContainerCreatedAtInfo(String serviceName) {
        var instance = getServiceInstance(serviceName);
        if (instance == null) return null;
        return sandboxManager.getContainerCreatedAt(instance.containerNames);
    }

    public List<String> getContainerStatus(String serviceName) {
        var instance = getServiceInstance(serviceName);
        if (instance == null) return null;
        // SandboxManager.getContainerStatus takes a single container name,
        // so we collect status for each container in the service
        List<String> statuses = new java.util.ArrayList<>();
        for (String containerName : instance.containerNames) {
            String status = sandboxManager.getContainerStatus(containerName);
            if (status != null) statuses.add(status);
        }
        return statuses;
    }

    /**
     * Waits until all containers for the given service are healthy.
     * Iterates containerNames and components in order (they correspond 1-to-1).
     *
     * TODO: add an overload to allow waiting for a specific container only.
     */
    public boolean waitUntilReady(String serviceName) {
        var instance = getServiceInstance(serviceName);
        if (instance == null) return false;

        List<edu.umass.cs.xdn2.service.ServiceComponent> components =
                instance.property.getComponents();

        for (int i = 0; i < instance.containerNames.size(); i++) {
            String containerName = instance.containerNames.get(i);
            String healthcheckCmd = (i < components.size())
                    ? components.get(i).getHealthcheckCommand()
                    : null;
            boolean ready = sandboxManager.waitUntilReady(containerName, healthcheckCmd);
            if (!ready) {
                logger.log(Level.WARNING,
                        "{0}:XdnApp waitUntilReady() container {1} not ready for {2}",
                        new Object[]{myNodeId, containerName, serviceName});
                return false;
            }
        }
        return true;
    }

    public void stop() {
        for (Map.Entry<String, ServiceType> entry : serviceRegistry.entrySet()) {
            String serviceName = entry.getKey();
            var instance = getServiceInstance(serviceName);
            if (instance != null) {
                sandboxManager.deleteService(instance);
            }
        }
        serviceRegistry.clear();
    }

    /**
     * Caches a deserialized request so getRequest() can return it without
     * re-deserializing when GigaPaxos calls it on the same node.
     */
    public void cacheRequest(Request request) {
        if (request instanceof XdnHttpRequest r) {
            requestCache.put(r.getRequestID(), r);
        } else if (request instanceof XdnHttpRequestBatch b) {
            requestCache.put(b.getRequestID(), b);
        }
    }

    public ServiceType getServiceType(String serviceName) {
        return serviceRegistry.get(serviceName);
    }

    public boolean hostsService(String serviceName) {
        ServiceType type = serviceRegistry.get(serviceName);
        if (type == null) return false;
        return switch (type) {
            case DETERMINISTIC     -> deterministicService.hostsService(serviceName);
            case NON_DETERMINISTIC -> nonDeterministicService.hostsService(serviceName);
        };
    }

    public edu.umass.cs.xdn2.service.ServiceInstance getServiceInstance(
            String serviceName) {
        ServiceType type = serviceRegistry.get(serviceName);
        if (type == null) return null;
        return switch (type) {
            case DETERMINISTIC     ->
                    deterministicService.getServiceInstance(serviceName);
            case NON_DETERMINISTIC ->
                    nonDeterministicService.getServiceInstance(serviceName);
        };
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    /**
     * Determines service type from the state string on the first restore() call.
     * Falls back to NON_DETERMINISTIC if parsing fails — primary-backup is always
     * safe, just slower.
     */
    private ServiceType detectServiceType(String state) {
        try {
            ServiceProperty property = parseServiceProperty(state);
            if (property == null) {
                logger.log(Level.WARNING,
                        "{0}:XdnApp could not parse ServiceProperty; " +
                                "defaulting to NON_DETERMINISTIC",
                        new Object[]{myNodeId});
                return ServiceType.NON_DETERMINISTIC;
            }
            return property.isDeterministic()
                    ? ServiceType.DETERMINISTIC
                    : ServiceType.NON_DETERMINISTIC;
        } catch (Exception e) {
            logger.log(Level.WARNING,
                    "{0}:XdnApp detectServiceType() failed: {1}; " +
                            "defaulting to NON_DETERMINISTIC",
                    new Object[]{myNodeId, e.getMessage()});
            return ServiceType.NON_DETERMINISTIC;
        }
    }

    /**
     * Parses a ServiceProperty from any known state string format:
     *   xdn:init:<servicePropertyJSON>
     *   xdn:final:<epoch>::<servicePropertyJSON>::<state>
     *   non-deter:load:xdn:init:<servicePropertyJSON>
     */
    public static ServiceProperty parseServiceProperty(String state) {
        if (state == null) return null;

        // Strip legacy non-deter:load: prefix
        if (state.startsWith(ServiceProperty.NON_DETERMINISTIC_LOAD_PREFIX)) {
            state = state.substring(
                    ServiceProperty.NON_DETERMINISTIC_LOAD_PREFIX.length());
        }

        if (state.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)) {
            String encoded = state.substring(
                    ServiceProperty.XDN_INITIAL_STATE_PREFIX.length());
            try {
                return ServiceProperty.createFromJsonString(encoded);
            } catch (Exception e) {
                return null;
            }
        }

        if (state.startsWith(ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX)) {
            String[] parts = state.split("::");
            if (parts.length >= 2) {
                try {
                    return ServiceProperty.createFromJsonString(parts[1]);
                } catch (Exception e) {
                    return null;
                }
            }
        }

        return null;
    }

    /**
     * Configures request matchers on a deserialized XdnHttpRequest based on
     * the service's ServiceProperty. Falls back gracefully if the service is
     * not yet registered.
     */
    private XdnHttpRequest configureRequestMatchers(XdnHttpRequest request) {
        if (request == null) return null;
        String serviceName = request.getServiceName();
        var instance = getServiceInstance(serviceName);
        if (instance != null) {
            request.setRequestMatchers(instance.property.getRequestMatchers());
        }
        return request;
    }

    private boolean isInternalGroup(String name) {
        return internalGpGroups.contains(name);
    }

    private static void checkDockerAvailable() {
        int exitCode = Shell.runCommand("docker version", true);
        if (exitCode != 0) {
            throw new RuntimeException(
                    "Docker is unavailable. Common reasons:\n" +
                            "(1) Docker is not installed\n" +
                            "(2) Docker requires sudo\n" +
                            "(3) Docker daemon is not running");
        }
    }
}