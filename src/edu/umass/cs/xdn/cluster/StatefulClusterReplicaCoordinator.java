package edu.umass.cs.xdn.cluster;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONObject;

/**
 * Pass-through coordinator for self-clustering services (the {@code xdn cluster launch} command).
 *
 * <p>Unlike every other coordinator in XDN, this one does <strong>not</strong> replicate requests
 * across replicas. The containerized service handles its own coordination (e.g. etcd, CockroachDB,
 * a Postgres replication cluster, an etcd ensemble). XDN's job here is only:
 *
 * <ul>
 *   <li>place {@code N} replica containers on {@code N} nodes,
 *   <li>assign each a stable ordinal identity ({@code 0..N-1}, deterministic from the sorted node
 *       set),
 *   <li>publish a {@link ClusterTopology} to the application layer so the container is started on
 *       the right overlay network with the right {@code XDN_CLUSTER_*} environment variables,
 *   <li>execute incoming requests on the local replica's container — no consensus, no broadcast.
 * </ul>
 */
public class StatefulClusterReplicaCoordinator<NodeIDType>
    extends AbstractReplicaCoordinator<NodeIDType> {

  private final NodeIDType myNodeId;
  private final Replicable app;
  private final Set<IntegerPacketType> packetTypes;

  private record ClusterReplicaInstance<NodeIDType>(
      String serviceName,
      int currEpoch,
      String initStateSnapshot,
      List<NodeIDType> orderedNodes,
      int myOrdinal) {}

  private final ConcurrentMap<String, ClusterReplicaInstance<NodeIDType>> currentInstances;

  private final Logger logger =
      Logger.getLogger(StatefulClusterReplicaCoordinator.class.getSimpleName());

  public StatefulClusterReplicaCoordinator(
      Replicable app,
      NodeIDType myId,
      Stringifiable<NodeIDType> nodeIdDeserializer,
      Messenger<NodeIDType, JSONObject> messenger) {
    super(app, messenger);
    this.myNodeId = myId;
    this.app = app;

    assert messenger.getMyID().equals(myId) : "Invalid node ID given in the messenger";
    assert nodeIdDeserializer.valueOf(this.myNodeId.toString()).equals(this.myNodeId)
        : "Invalid node ID deserializer given";

    // Pass-through: this coordinator has no protocol packets of its own. The XDN service/HTTP
    // request types are registered by XdnReplicaCoordinator itself, so the empty set is correct.
    this.packetTypes = new HashSet<>();
    this.currentInstances = new ConcurrentHashMap<>();
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return this.packetTypes;
  }

  @Override
  public boolean coordinateRequest(Request request, ExecutedCallback callback)
      throws IOException, RequestParseException {
    if (logger.isLoggable(Level.FINE)) {
      logger.log(
          Level.FINE,
          ">> {0} StatefulClusterReplicaCoordinator -- receiving request {1}",
          new Object[] {myNodeId, request.getClass().getSimpleName()});
    }

    String serviceName = request.getServiceName();
    if (!this.currentInstances.containsKey(serviceName)) {
      logger.log(Level.WARNING, "Ignoring request for unknown cluster service={0}", serviceName);
      return true;
    }

    Request unwrapped = request;
    if (unwrapped instanceof ReplicableClientRequest rcr) {
      unwrapped = rcr.getRequest();
    }

    // stop-epoch: tear down the local replica via app.restore(name, null)
    if (unwrapped instanceof ReconfigurableRequest rc && rc.isStop()) {
      boolean isSuccess = this.app.restore(serviceName, null);
      callback.executed(rc, isSuccess);
      return true;
    }

    // any other client request: execute locally and return — the cluster handles its own
    // replication, so there is no inter-replica coordination to perform here.
    if (unwrapped instanceof ClientRequest clientRequest) {
      boolean isExecSuccess = this.app.execute(clientRequest);
      callback.executed(clientRequest, isExecSuccess);
      return true;
    }

    throw new IllegalStateException(
        "Unexpected request type for cluster service: " + request.getRequestType());
  }

  @Override
  public boolean createReplicaGroup(
      String serviceName,
      int epoch,
      String state,
      Set<NodeIDType> nodes,
      String placementMetadata) {
    if (logger.isLoggable(Level.INFO)) {
      logger.log(
          Level.INFO,
          ">> {0}:StatefulClusterReplicaCoordinator -- createReplicaGroup name={1} nodes={2} "
              + "epoch={3}",
          new Object[] {myNodeId, serviceName, nodes, epoch});
    }

    // Deterministic ordinal assignment: every node sorts the same set the same way, so every
    // node computes the same nodeId -> ordinal map without any handshake. Reconfiguration
    // (Component 6) will replace this with a persisted ordinal map carried in ServiceProperty.
    List<NodeIDType> orderedNodes = new ArrayList<>(nodes);
    orderedNodes.sort(Comparator.comparing(Object::toString));
    int myOrdinal = orderedNodes.indexOf(myNodeId);
    if (myOrdinal < 0) {
      throw new IllegalStateException(
          "createReplicaGroup called on node " + myNodeId + " not in the replica set " + nodes);
    }

    ClusterReplicaInstance<NodeIDType> instance =
        new ClusterReplicaInstance<>(
            serviceName, epoch, state, Collections.unmodifiableList(orderedNodes), myOrdinal);
    this.currentInstances.put(serviceName, instance);

    // Push the topology to the app BEFORE restore(): initContainerizedService2's cluster branch
    // reads it to build the XDN_CLUSTER_* env and pick the right --network-alias.
    if (this.app instanceof ClusterTopologyAware aware) {
      aware.setClusterTopology(
          serviceName,
          new ClusterTopology(myOrdinal, orderedNodes.size(), ClusterTopology.Phase.BOOTSTRAP));
    }

    return this.app.restore(serviceName, state);
  }

  @Override
  public boolean deleteReplicaGroup(String serviceName, int epoch) {
    if (!this.currentInstances.containsKey(serviceName)) {
      return true;
    }
    ClusterReplicaInstance<NodeIDType> instance = this.currentInstances.get(serviceName);
    if (instance.currEpoch != epoch) {
      return true;
    }
    this.currentInstances.remove(serviceName);
    if (this.app instanceof ClusterTopologyAware aware) {
      aware.clearClusterTopology(serviceName);
    }
    return this.app.restore(serviceName, null);
  }

  @Override
  public Set<NodeIDType> getReplicaGroup(String serviceName) {
    ClusterReplicaInstance<NodeIDType> instance = this.currentInstances.get(serviceName);
    return instance != null ? new HashSet<>(instance.orderedNodes()) : null;
  }
}
