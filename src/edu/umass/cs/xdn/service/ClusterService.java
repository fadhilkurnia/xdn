package edu.umass.cs.xdn.service;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.XdnBandwidthProfiler;
import edu.umass.cs.xdn.XdnHttpForwarderClient;
import edu.umass.cs.xdn.cluster.ClusterTopology;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import edu.umass.cs.xdn.request.XdnStopRequest;
import edu.umass.cs.xdn.sandbox.SandboxManager;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * ClusterService handles all cluster-managed services in XDN (deploymentMode="cluster" in the
 * service spec -- e.g. etcd).
 *
 * <p>Cluster-managed services replicate their own state via their own internal protocol (e.g.
 * etcd's Raft). XDN does not capture, diff, or replicate their state at all -- it only launches one
 * container per replica, puts them on a shared network so they can find each other by name, and
 * tells each container its own identity/peers/join-phase via XDN_CLUSTER_* environment variables so
 * the image (or a thin entrypoint wrapper) can wire itself up. This mirrors XdnGigapaxosApp's
 * isClusterManaged() branch, which does the same thing and skips its state-diff recorder entirely
 * for the same reason.
 *
 * <p>Unlike NonDeterministicService, container start is NOT gated behind any role confirmation --
 * there is no "primary" or "backup" here, every replica starts its container the same way, as soon
 * as it's told to.
 *
 * <p>Currently scoped to single-component cluster services (the one component is both the entry
 * point and the cluster member). Multi-component clusters (a separate stateful cluster member plus
 * sidecars sharing its network namespace, as XdnGigapaxosApp supports) are not yet handled.
 *
 * <p>Lifecycle: restore("xdn:init:…") → createAndStart() -- immediate container start, no waiting
 * restore(null) → stop and remove the container (epoch end) execute(XdnHttpRequest) → forward to
 * local container, return response execute(XdnStopRequest) → stop container checkpoint(name) → stub
 * only, see CHECKPOINT_STUB
 */
public class ClusterService {

  /**
   * Returned by checkpoint(). GigaPaxos uses checkpoint() only for Paxos log truncation and
   * single-node crash recovery. Cluster-managed services replicate themselves, so there is no
   * XDN-level state to checkpoint -- a stub is intentional here, same reasoning and same known gap
   * as NonDeterministicService.CHECKPOINT_STUB.
   */
  public static final String CHECKPOINT_STUB = "xdn:cluster:checkpoint:stub";

  private final String myNodeId;

  // serviceName → current placement epoch
  private final Map<String, Integer> servicePlacementEpoch = new ConcurrentHashMap<>();

  // serviceName → ServiceInstance (contains property, port, container names, etc.)
  private final Map<String, ServiceInstance> serviceInstances = new ConcurrentHashMap<>();

  // One lock per service name. GigaPaxos re-sends START_EPOCH every 16 seconds until it is
  // acked, and a cluster start can take longer than that, so restore() can be entered again
  // for the same service while the first call is still running. The lock makes the repeat
  // wait, and restore() then returns without touching Docker.
  private final Map<String, Object> initLocks = new ConcurrentHashMap<>();

  // Shared HTTP request cache (keyed by requestID, shared with XdnApp)
  private final Map<Long, Request> requestCache;

  // serviceName → topology pushed by StatefulClusterReplicaCoordinator (shared with XdnApp,
  // populated via XdnApp.setClusterTopology() before restore() is called)
  private final Map<String, ClusterTopology> clusterTopologies;

  // HTTP forwarder: sends requests to the local container
  private final XdnHttpForwarderClient httpForwarderClient;

  private final Logger logger = Logger.getLogger(ClusterService.class.getName());

  private final SandboxManager sandboxManager;

  private final XdnBandwidthProfiler bandwidthProfiler;

  // serviceName -> probe container name, needed to remove it on service deletion
  private final Map<String, String> probeContainerNames = new ConcurrentHashMap<>();

  public ClusterService(
      String myNodeId,
      Map<Long, Request> requestCache,
      SandboxManager sandboxManager,
      Map<String, ClusterTopology> clusterTopologies) {
    this.myNodeId = myNodeId;
    this.requestCache = requestCache;
    this.sandboxManager = sandboxManager;
    this.clusterTopologies = clusterTopologies;
    this.httpForwarderClient = new XdnHttpForwarderClient();
    this.bandwidthProfiler = new XdnBandwidthProfiler(myNodeId);
  }

  /**
   * Returns this replica's bandwidth edge snapshot for a profiled service, or null when not
   * profiled (tracer disabled, probe failed, or service not hosted here).
   */
  public org.json.JSONObject getBandwidthSnapshot(String serviceName) {
    return this.bandwidthProfiler.snapshot(serviceName);
  }

  // -------------------------------------------------------------------------
  // execute()
  // -------------------------------------------------------------------------

  /**
   * Called by GigaPaxos on every replica after Paxos commits the request. Forwards the HTTP request
   * to the local container and stores the response. Only the entry replica sends the response back
   * to the client.
   */
  public boolean execute(Request request) {
    String serviceName = request.getServiceName();

    if (request instanceof XdnHttpRequest xdnHttpRequest) {
      forwardToContainer(xdnHttpRequest);

      // Non-entry replicas discard the response: they executed to update
      // state, not to serve the client.
      if (xdnHttpRequest.getHttpResponse() != null && xdnHttpRequest.isCreatedFromString()) {
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
      return stopContainerInstance(serviceName, stopRequest.getEpochNumber());
    }

    logger.log(
        Level.WARNING,
        "{0}:ClusterService unknown request type={1}",
        new Object[] {myNodeId, request.getClass().getSimpleName()});
    return false;
  }

  // -------------------------------------------------------------------------
  // checkpoint() — stub only, see CHECKPOINT_STUB javadoc
  // -------------------------------------------------------------------------

  public String checkpoint(String name) {
    return CHECKPOINT_STUB;
  }

  // -------------------------------------------------------------------------
  // restore()
  // -------------------------------------------------------------------------

  /**
   * Called by GigaPaxos to initialize or clean up a service.
   *
   * <p>Handled cases: null → stop and remove the container (epoch end) xdn:init:… → start the
   * container immediately, no role confirmation needed
   *
   * <p>Anything else (final-state revival, checkpoint restoration) is not supported: cluster-
   * managed services never produce a final state (see XdnApp's getFinalState handling) and
   * checkpoint() only ever returns CHECKPOINT_STUB, so GigaPaxos should never actually call
   * restore() with either of those formats for a cluster-managed service.
   */
  public boolean restore(String name, String state) {
    if (state == null) {
      Integer epoch = servicePlacementEpoch.get(name);
      if (epoch == null) return true;
      return deleteContainerInstance(name, epoch);
    }

    if (state.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)) {
      // A repeated init must not run createAndStart again. It wipes the state directory and
      // force-removes the containers, which would destroy the first call's work in progress.
      synchronized (initLocks.computeIfAbsent(name, k -> new Object())) {
        if (serviceInstances.containsKey(name)) {
          logger.log(
              Level.INFO,
              "{0}:ClusterService restore() {1} already started, ignoring duplicate init",
              new Object[] {myNodeId, name});
          return true;
        }
        return createAndStart(name, state);
      }
    }

    logger.log(
        Level.SEVERE,
        "{0}:ClusterService restore() unsupported state prefix for {1}: {2}",
        new Object[] {myNodeId, name, state});
    return false;
  }

  // -------------------------------------------------------------------------
  // getStopRequest()
  // -------------------------------------------------------------------------

  public ReconfigurableRequest getStopRequest(String name, int epoch) {
    return new XdnStopRequest(name, epoch);
  }

  // -------------------------------------------------------------------------
  // deleteFinalState()
  // -------------------------------------------------------------------------

  /** Same cleanup as an epoch ending -- there is no separate final-state artifact to remove. */
  public boolean deleteFinalState(String name, int epoch) {
    return deleteContainerInstance(name, epoch);
  }

  // -------------------------------------------------------------------------
  // getEpoch() / hostsService() / getServiceInstance()
  // -------------------------------------------------------------------------

  public Integer getEpoch(String name) {
    return servicePlacementEpoch.get(name);
  }

  public boolean hostsService(String serviceName) {
    return serviceInstances.containsKey(serviceName);
  }

  public ServiceInstance getServiceInstance(String serviceName) {
    return serviceInstances.get(serviceName);
  }

  // -------------------------------------------------------------------------
  // Private: container lifecycle
  // -------------------------------------------------------------------------

  /**
   * Creates a fresh service instance and starts its container immediately, on the shared cluster
   * network, with a network alias and XDN_CLUSTER_* env vars derived from this replica's topology
   * (pushed earlier by StatefulClusterReplicaCoordinator via XdnApp.setClusterTopology()).
   *
   * <p>Format: xdn:init:<servicePropertyJSON>
   */
  private boolean createAndStart(String name, String state) {
    String encoded = state.substring(ServiceProperty.XDN_INITIAL_STATE_PREFIX.length());
    ServiceProperty property;
    try {
      property = ServiceProperty.createFromJsonString(encoded);
    } catch (Exception e) {
      logger.log(
          Level.SEVERE,
          "{0}:ClusterService failed to parse ServiceProperty: {1}",
          new Object[] {myNodeId, e.getMessage()});
      return false;
    }

    ClusterTopology topology = clusterTopologies.get(name);
    if (topology == null) {
      logger.log(
          Level.SEVERE,
          "{0}:ClusterService createAndStart() no topology pushed for {1} -- "
              + "coordinator must call setClusterTopology() before restore()",
          new Object[] {myNodeId, name});
      return false;
    }

    int epoch = 0;
    int allocatedPort = sandboxManager.allocatePort();
    String networkName = "xdn-cluster-" + name;
    List<String> containerNames = buildContainerNames(name, epoch, property);
    ServiceInstance instance =
        new ServiceInstance(property, name, networkName, allocatedPort, containerNames);

    instance.networkAlias = "replica-" + topology.myOrdinal();
    instance.extraEnv = buildClusterEnv(property, topology);

    sandboxManager.prepareStateDirectory(name, epoch);
    boolean networkReady = sandboxManager.createClusterNetwork(name);
    if (!networkReady) return false;

    boolean started = sandboxManager.startService(instance, epoch);
    if (!started) return false;

    serviceInstances.put(name, instance);
    servicePlacementEpoch.put(name, epoch);

    // Attach the bandwidth probe sidecar and start profiling this replica's traffic. Any
    // failure here only disables profiling; the service itself is unaffected.
    if (Config.getGlobalBoolean(ReconfigurationConfig.RC.XDN_CLUSTER_BW_TRACER_ENABLED)) {
      String probeImage =
          Config.getGlobalString(ReconfigurationConfig.RC.XDN_CLUSTER_BW_PROBE_IMAGE);
      String clusterContainerName = containerNames.get(0);
      String probeName = "bwprobe." + clusterContainerName;
      if (sandboxManager.startSidecarContainer(probeImage, probeName, clusterContainerName, null)) {
        probeContainerNames.put(name, probeName);
        bandwidthProfiler.register(name, probeName, topology.clusterSize(), allocatedPort);
      } else {
        logger.log(
            Level.WARNING,
            "{0}:ClusterService bandwidth probe failed to start; profiling disabled for {1}",
            new Object[] {myNodeId, name});
      }
    }

    return true;
  }

  /**
   * Builds the XDN_CLUSTER_* environment variables the container reads to discover its own identity
   * and peers. Currently scoped to single-component services -- the one component is always the
   * cluster member, so there's no stateful-vs-entry selection to do (unlike XdnGigapaxosApp's
   * multi-component-aware version).
   */
  private Map<String, String> buildClusterEnv(ServiceProperty property, ClusterTopology topology) {
    Map<String, String> env = new LinkedHashMap<>();

    StringBuilder peers = new StringBuilder();
    for (int i = 0; i < topology.clusterSize(); i++) {
      if (i > 0) peers.append(',');
      peers.append("replica-").append(i);
    }

    env.put("XDN_CLUSTER_ORDINAL", String.valueOf(topology.myOrdinal()));
    env.put("XDN_CLUSTER_SIZE", String.valueOf(topology.clusterSize()));
    env.put("XDN_CLUSTER_SELF", "replica-" + topology.myOrdinal());
    env.put("XDN_CLUSTER_PEERS", peers.toString());
    if (property.getPeerPort() != null) {
      env.put("XDN_CLUSTER_PEER_PORT", String.valueOf(property.getPeerPort()));
    }
    env.put(
        "XDN_CLUSTER_PHASE", topology.phase() == ClusterTopology.Phase.JOIN ? "join" : "bootstrap");

    return env;
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
      clusterTopologies.remove(name);
      sandboxManager.deleteClusterNetwork(name);

      bandwidthProfiler.deregister(name);
      String probeName = probeContainerNames.remove(name);
      if (probeName != null) {
        sandboxManager.stopContainer(probeName);
      }
    }
    return deleted;
  }

  // -------------------------------------------------------------------------
  // Private: request forwarding
  // -------------------------------------------------------------------------

  private void forwardToContainer(XdnHttpRequest xdnRequest) {
    String serviceName = xdnRequest.getServiceName();
    ServiceInstance instance = serviceInstances.get(serviceName);
    if (instance == null) {
      logger.log(
          Level.WARNING,
          "{0}:ClusterService no instance for service={1}",
          new Object[] {myNodeId, serviceName});
      return;
    }
    try {
      FullHttpResponse response =
          httpForwarderClient.execute(
              "127.0.0.1", instance.allocatedHttpPort, copyHttpRequest(xdnRequest));
      xdnRequest.setHttpResponse(response);
    } catch (Exception e) {
      logger.log(
          Level.WARNING,
          "{0}:ClusterService forward failed for {1}: {2}",
          new Object[] {myNodeId, serviceName, e.getMessage()});
    }
  }

  private void forwardBatchToContainer(XdnHttpRequestBatch batch) {
    String serviceName = batch.getServiceName();
    ServiceInstance instance = serviceInstances.get(serviceName);
    if (instance == null) return;
    try {
      List<FullHttpRequest> requests = new ArrayList<>();
      for (XdnHttpRequest r : batch.getRequestList()) {
        requests.add(copyHttpRequest(r));
      }
      List<FullHttpResponse> responses =
          httpForwarderClient.executePipelined("127.0.0.1", instance.allocatedHttpPort, requests);
      List<XdnHttpRequest> batchRequests = batch.getRequestList();
      for (int i = 0; i < batchRequests.size() && i < responses.size(); i++) {
        batchRequests.get(i).setHttpResponse(responses.get(i));
      }
    } catch (Exception e) {
      logger.log(
          Level.WARNING,
          "{0}:ClusterService batch forward failed for {1}: {2}",
          new Object[] {myNodeId, serviceName, e.getMessage()});
    }
  }

  private FullHttpRequest copyHttpRequest(XdnHttpRequest xdnRequest) {
    HttpRequest original = xdnRequest.getHttpRequest();
    HttpContent content = xdnRequest.getHttpRequestContent();
    FullHttpRequest copy =
        new DefaultFullHttpRequest(
            original.protocolVersion(),
            original.method(),
            original.uri(),
            content.content().copy());
    copy.headers().setAll(original.headers());
    return copy;
  }

  private List<String> buildContainerNames(
      String serviceName, int epoch, ServiceProperty property) {
    List<String> names = new ArrayList<>();
    for (int i = 0; i < property.getComponents().size(); i++) {
      names.add(String.format("c%d.e%d.%s.%s.xdn.io", i, epoch, serviceName, myNodeId));
    }
    return names;
  }
}
