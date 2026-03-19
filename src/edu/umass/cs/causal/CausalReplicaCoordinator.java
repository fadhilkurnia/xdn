package edu.umass.cs.causal;

import edu.umass.cs.causal.dag.DirectedAcyclicGraph;
import edu.umass.cs.causal.dag.GraphVertex;
import edu.umass.cs.causal.dag.VectorTimestamp;
import edu.umass.cs.causal.packets.CausalPacket;
import edu.umass.cs.causal.packets.CausalPacketType;
import edu.umass.cs.causal.packets.CausalWriteAckPacket;
import edu.umass.cs.causal.packets.CausalWriteForwardPacket;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The protocol implemented here is based on Causal Memory by Ahamad, et al. (Georgia Tech report of
 * GIT-CC-93/55) from 1993, which provide Causal Consistency guarantee.
 * Some modifications from that base protocol includes:
 * - the use of directed acyclic graph (DAG), instead of queue, for the InQueue in each node
 * <p>
 * TODO:
 *  - Implement background sub-graph pruning by removing vertices that are already acknowledged
 *    by *all* the nodes. This requires recording the acknowledgements.
 *  - Implement optimization by keep track of the peer's timestamp (matrix clock) to send the needed
 *    dependencies for each peer, preventing the peer to wait (or ask) for the dependencies.
 *  - Implement checkpoint by recording (1) the current state, and (2) combined graph from the
 *    majority.
 *  - Implement recovery by re-instantiating state based on (1) the checkpoint state, which include
 *    a majority graph, and (2) the combined *current* graph from the majority.
 *  - Implement batching by having multiple write requests in a single vertex in the DAG.
 *  - Implement client-centric causal consistency that allow client to move to another replica
 *    while keep observing causal consistency. Implement this by passing metadata to client.
 *
 * @param <NodeIDType>
 */
public class CausalReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private record QuorumRecord(Set<String> confirmingNodeIds) {
        static QuorumRecord withInitialNode(String nodeId) {
            Set<String> set = ConcurrentHashMap.newKeySet();
            set.add(nodeId);
            return new QuorumRecord(set);
        }
    }

    private record ClientRequestAndCallback(ClientRequest request, ExecutedCallback callback) {
    }

    private record ReplicaInstance
            <NodeIDType>(String serviceName,
                         int currentEpoch,
                         String initStateSnapshot,
                         Set<NodeIDType> nodes,
                         int writeQuorumSize,
                         int readQuorumSize,
                         DirectedAcyclicGraph dag,

                         // quorum tracker to prune vertex in our DAG
                         ConcurrentHashMap<VectorTimestamp, QuorumRecord> graphNodeQuorumRecord,

                         // Tracks which vertices have completed app.execute(), not just DAG
                         // insertion. Forwarded writes check this — not idToVertexMapper — so they
                         // never execute before their causal dependencies have actually been applied
                         // to app state. Written after execute() returns; read under the dag lock
                         // in dependency checks, so no additional synchronization is needed.
                         Set<VectorTimestamp> executedTimestamps,

                         // buffered write-fwd, waiting for missing dependencies
                         Map<VectorTimestamp, CausalWriteForwardPacket> pendingForwardPackets) {
    }

    private final NodeIDType myNodeID;
    private final Stringifiable<NodeIDType> nodeIdDeserializer;

    private final Set<IntegerPacketType> requestTypes;
    private final Map<String, ReplicaInstance<NodeIDType>> instances;

    private final Logger logger = Logger.getLogger(CausalReplicaCoordinator.class.getName());

    public CausalReplicaCoordinator(Replicable app,
                                    NodeIDType myID,
                                    Stringifiable<NodeIDType> nodeIdDeserializer,
                                    Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        this.myNodeID = myID;
        this.instances = new ConcurrentHashMap<>();

        // Validate the nodeIdDeserializer
        this.nodeIdDeserializer = nodeIdDeserializer;
        assert this.nodeIdDeserializer.valueOf(myNodeID.toString()).equals(myNodeID) :
                "Invalid NodeIDType deserializer given";
        assert messenger.getMyID().equals(myNodeID) : "Invalid NodeID given in the messenger";

        // Initialize all the request types handled
        Set<IntegerPacketType> types = new HashSet<>(app.getRequestTypes());
        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        types.add(CausalPacketType.CAUSAL_PACKET);
        this.requestTypes = types;

        // Add packet de-multiplexer for CausalPacket that will invoke coordinateRequest() method
        CausalPacketDemultiplexer packetDemultiplexer =
                new CausalPacketDemultiplexer(this, app);
        this.messenger.precedePacketDemultiplexer(packetDemultiplexer);

        this.logger.log(Level.INFO, "Initialized at node " + this.myNodeID);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return this.requestTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        this.logger.log(Level.INFO, "node=" + this.myNodeID + " coordinating request " +
                request.getClass().getSimpleName());

        if (request instanceof ReplicableClientRequest r) {
            return this.handleClientRequest(r, callback);
        }

        if (request instanceof CausalPacket cp) {
            return this.handleCoordinationPacket(cp);
        }

        throw new RuntimeException("Unknown request handled by CausalReplicaCoordinator " +
                "which can only handle ReplicableClientRequest or CausalPacket.");
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes, String placementMetadata) {
        assert serviceName != null : "service name cannot be null";
        assert nodes != null && !nodes.isEmpty() : "nodes cannot be empty";
        assert epoch >= 0 : "epoch must not be a negative number";

        // get the write and quorum size based on the replica-group size
        int writeQuorumSize = nodes.size() / 2 + 1;
        int readQuorumSize = nodes.size() % 2 == 0
                ? nodes.size() / 2
                : nodes.size() / 2 + 1;
        assert writeQuorumSize + readQuorumSize == nodes.size() + 1 :
                "read and write quorum must intersect at one replica";

        // Initialize service instance for the given service name, having a root node
        // of zero in the DAG.
        List<String> nodesIds = new ArrayList<>();
        for (NodeIDType n : nodes) nodesIds.add(n.toString());
        GraphVertex zeroRootNode =
                new GraphVertex(new VectorTimestamp(nodesIds), new ArrayList<>());

        // The zero root vertex is considered already "executed" — it represents the
        // initial state, not a real write — so forwarded writes that list it as a
        // dependency are immediately unblocked.
        Set<VectorTimestamp> executedTimestamps = ConcurrentHashMap.newKeySet();
        executedTimestamps.add(zeroRootNode.getTimestamp());

        ReplicaInstance<NodeIDType> instance =
                new ReplicaInstance<>(
                        /*serviceName=*/serviceName,
                        /*currentEpoch=*/epoch,
                        /*initStateSnapshot=*/state,
                        /*nodes=*/nodes,
                        /*writeQuorumSize=*/writeQuorumSize,
                        /*readQuorumSize=*/readQuorumSize,
                        /*dag=*/new DirectedAcyclicGraph(zeroRootNode),
                        /*graphNodeQuorumRecord=*/new ConcurrentHashMap<>(),
                        /*executedTimestamps=*/executedTimestamps,
                        /*pendingForwardPackets=*/new ConcurrentHashMap<>());
        this.instances.put(serviceName, instance);

        // Start the app using the restore method with the passed initial state.
        boolean isRestoreSuccess = this.app.restore(serviceName, state);
        if (!isRestoreSuccess) {
            System.out.println(">>> " + this.myNodeID +
                    ":CausalReplicaCoordinator - failed to restore :(");
        }

        return isRestoreSuccess;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        assert serviceName != null : "service name cannot be null";
        assert epoch >= 0 : "epoch must not be a negative number";

        ReplicaInstance<NodeIDType> instance = this.instances.get(serviceName);
        if (instance == null) {
            return true;
        }

        // Terminate the app using the restore method with null state.
        return this.app.restore(serviceName, null);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        ReplicaInstance<NodeIDType> targetInstance = this.instances.get(serviceName);
        if (targetInstance == null) return null;
        return targetInstance.nodes;
    }

    private boolean handleClientRequest(ReplicableClientRequest clientReplicableRequest,
                                        ExecutedCallback callback) {
        assert clientReplicableRequest != null : "client request cannot be null";

        // Stop packet is sometimes wrapped in ClientRequest by ActiveReplica
        if (clientReplicableRequest.getRequest() instanceof ReconfigurableRequest rcRequest &&
                rcRequest.isStop()) {
            System.out.println(">> CausalReplicaCoordinator -- stopping a service in epoch=" +
                    rcRequest.getEpochNumber());
            boolean isSuccess = this.app.restore(clientReplicableRequest.getServiceName(), null);
            callback.executed(rcRequest, isSuccess);
            return isSuccess;
        }

        // Validates the client request
        Request clientRequest = clientReplicableRequest.getRequest();
        if (!(clientRequest instanceof ClientRequest)) {
            throw new RuntimeException("CausalReplicaCoordinator can only handle ClientRequest, " +
                    "but " + clientRequest.getClass().getSimpleName() + " is received at " +
                    this.myNodeID);
        }
        if (!(clientRequest instanceof BehavioralRequest behavioralRequest)) {
            throw new RuntimeException("CausalReplicaCoordinator can only handle " +
                    "BehavioralRequest");
        }
        if (!behavioralRequest.isReadOnlyRequest() &&
                !behavioralRequest.isWriteOnlyRequest()) {
            throw new RuntimeException("CausalReplicaCoordinator can only handle " +
                    "ReadOnlyRequest and WriteOnlyRequest.");
        }

        // Gather the service's instance metadata
        String serviceName = clientRequest.getServiceName();
        String myNodeIdStr = this.myNodeID.toString();
        ReplicaInstance<NodeIDType> serviceInstance = this.instances.get(serviceName);
        if (serviceInstance == null) {
            this.logger.log(Level.WARNING, "Unknown replica instance with name=" +
                    serviceName);
            return false;
        }
        List<String> nodeIds = new ArrayList<>();
        for (NodeIDType n : serviceInstance.nodes) {
            nodeIds.add(n.toString());
        }

        // Handle write-only request
        if (behavioralRequest.isWriteOnlyRequest()) {
            // Phase 1 (under lock): assign a timestamp and insert the vertex into the DAG.
            // This is the only part that must be atomic — it prevents two concurrent writes
            // from computing the same dominant timestamp from the same leaf set.
            // app.execute() is NOT called here; it is the slow part (container round-trip)
            // and must not hold the lock.
            final VectorTimestamp dominantTimestamp;
            final List<VectorTimestamp> leafTimestamp;
            synchronized (serviceInstance.dag) {
                List<GraphVertex> leafOpNodes = serviceInstance.dag.getLeafVertices();
                leafTimestamp = new ArrayList<>();
                for (GraphVertex leafNode : leafOpNodes) {
                    leafTimestamp.add(leafNode.getTimestamp());
                }

                VectorTimestamp maxTimestamp = !leafTimestamp.isEmpty()
                        ? VectorTimestamp.createMaxTimestamp(leafTimestamp)
                        : new VectorTimestamp(nodeIds);
                dominantTimestamp = maxTimestamp.increaseNodeTimestamp(myNodeIdStr);
                GraphVertex newOpNode = new GraphVertex(dominantTimestamp, List.of(clientRequest));
                serviceInstance.dag.addChildOf(leafOpNodes, newOpNode);

                serviceInstance.graphNodeQuorumRecord.put(
                        dominantTimestamp,
                        QuorumRecord.withInitialNode(myNodeIdStr));
            }

            // Phase 2 (outside lock): execute against the container. This is the slow I/O
            // step (~ms per request). Releasing the lock here allows concurrent forwarded
            // writes whose dependencies are already satisfied to proceed in parallel.
            boolean isExecSuccess = this.app.execute(clientRequest);
            assert isExecSuccess : "failed to execute request " + clientRequest;

            // Mark this vertex as fully executed so dependent forwarded writes can unblock.
            serviceInstance.executedTimestamps.add(dominantTimestamp);

            // Reply to client and drain any pending forwarded writes now unblocked.
            callback.executed(clientRequest, true);
            drainPendingForwardedWrites(serviceInstance);

            // Phase 3 (outside lock): broadcast to peers.
            CausalWriteForwardPacket wfp = new CausalWriteForwardPacket(
                    serviceName,
                    this.myNodeID.toString(),
                    leafTimestamp,
                    dominantTimestamp,
                    (ClientRequest) clientRequest);
            List<NodeIDType> myPeers = new ArrayList<>();
            for (NodeIDType n : serviceInstance.nodes) {
                if (n.equals(this.myNodeID)) continue;
                myPeers.add(n);
            }
            GenericMessagingTask<NodeIDType, CausalPacket> m =
                    new GenericMessagingTask<>(myPeers.toArray(), wfp);
            try {
                this.messenger.send(m);
            } catch (IOException | JSONException e) {
                throw new RuntimeException(e);
            }

            return true;
        }

        // Handle read-only request by executing it locally
        if (behavioralRequest.isReadOnlyRequest()) {
            boolean isExecSuccess = this.app.execute(clientRequest);
            assert isExecSuccess : "failed to execute request " + clientRequest;
            callback.executed(clientRequest, true);
            return true;
        }

        throw new RuntimeException("Unknown client request=" + clientRequest +
                "  behaviors=" + behavioralRequest.getBehaviors());
    }

    protected boolean handleCoordinationPacket(CausalPacket packet) {

        if (packet instanceof CausalWriteForwardPacket writeForwardPacket) {
            this.handleWriteForwardPacket(writeForwardPacket);
            return true;
        }

        if (packet instanceof CausalWriteAckPacket ackPacket) {
            this.handleWriteAckPacket(ackPacket);
            return true;
        }

        throw new RuntimeException("Unimplemented handler of packet " + packet.getRequestType());
    }

    private void handleWriteForwardPacket(CausalWriteForwardPacket packet) {
        String serviceName = packet.getServiceName();
        ReplicaInstance<NodeIDType> serviceInstance = this.instances.get(serviceName);
        assert serviceInstance != null : "Unknown service with name=" + serviceName;

        List<VectorTimestamp> dependencies = packet.getDependencies();

        // Phase 1 (under lock): check dependencies and insert into DAG.
        //
        // The dependency check and DAG insert must be atomic with respect to concurrent
        // local writes (handleClientRequest phase 1). Without the lock, a local write
        // could insert a vertex between our executedTimestamps check and our
        // pendingForwardPackets.put, causing us to buffer a packet whose dependencies
        // are already satisfied — stranding it until the next drain.
        //
        // We check executedTimestamps, not idToVertexMapper. A vertex being in the DAG
        // is not sufficient — app.execute() must have completed so that app state actually
        // reflects the causal predecessor before we execute the dependent write.
        synchronized (serviceInstance.dag) {
            boolean isDependenciesSatisfied =
                    serviceInstance.executedTimestamps.containsAll(dependencies);

            if (!isDependenciesSatisfied) {
                serviceInstance.pendingForwardPackets.put(
                        packet.getRequestTimestamp(), packet);
                return;
            }

            // Insert the vertex into the DAG under the lock so concurrent writes see it
            // immediately when computing their own leaf sets and timestamps.
            List<GraphVertex> parentGraphVertices =
                    serviceInstance.dag.getVerticesByTimestamps(dependencies);
            GraphVertex newGraphVertex = new GraphVertex(
                    packet.getRequestTimestamp(),
                    List.of(packet.getClientWriteOnlyRequest()));
            serviceInstance.dag.addChildOf(parentGraphVertices, newGraphVertex);
        }

        // Phase 2 (outside lock): execute against the container.
        ClientRequest clientRequest = packet.getClientWriteOnlyRequest();
        assert clientRequest instanceof BehavioralRequest behavioralRequest &&
                behavioralRequest.isWriteOnlyRequest() :
                "Expecting WriteOnlyRequest but got " + clientRequest.getClass().getSimpleName();
        boolean isExecSuccess = this.app.execute(clientRequest);
        assert isExecSuccess;

        // Mark executed, then drain any pending writes now unblocked by this one.
        serviceInstance.executedTimestamps.add(packet.getRequestTimestamp());
        drainPendingForwardedWrites(serviceInstance);

        // Phase 3 (outside lock): send ack with current leaf set.
        final List<VectorTimestamp> graphLeafNodeIds;
        synchronized (serviceInstance.dag) {
            List<GraphVertex> graphLeafNodes = serviceInstance.dag.getLeafVertices();
            graphLeafNodeIds = new ArrayList<>();
            for (GraphVertex n : graphLeafNodes) {
                graphLeafNodeIds.add(n.getTimestamp());
            }
        }
        String senderId = packet.getSenderId();
        NodeIDType senderNodeId = this.nodeIdDeserializer.valueOf(senderId);
        CausalWriteAckPacket ackPacket = new CausalWriteAckPacket(
                serviceName,
                this.myNodeID.toString(),
                packet.getRequestTimestamp(),
                graphLeafNodeIds);
        GenericMessagingTask<NodeIDType, CausalPacket> m =
                new GenericMessagingTask<>(senderNodeId, ackPacket);
        try {
            this.messenger.send(m);
        } catch (IOException | JSONException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Drains pending forwarded writes whose causal dependencies are now fully executed.
     * Called after any write completes execution on both the local and forwarded paths.
     *
     * Structure per iteration:
     *   - Acquire lock: scan pending map, collect all packets whose executedTimestamps
     *     covers their dependencies, insert each into the DAG, remove from pending.
     *   - Release lock: execute each collected packet against the container (~ms each).
     *   - Add to executedTimestamps, then repeat if anything was applied.
     *
     * The outer loop is necessary because applying packet A may satisfy the dependencies
     * of packet B, which was skipped in the same pass.
     */
    private void drainPendingForwardedWrites(ReplicaInstance<NodeIDType> serviceInstance) {
        assert serviceInstance != null : "Unexpected null ServiceInstance";

        boolean anyApplied;
        do {
            anyApplied = false;

            // Collect all unblocked packets and insert them into the DAG under the lock.
            List<CausalWriteForwardPacket> readyPackets = new ArrayList<>();
            synchronized (serviceInstance.dag) {
                Iterator<Map.Entry<VectorTimestamp, CausalWriteForwardPacket>> iterator =
                        serviceInstance.pendingForwardPackets.entrySet().iterator();
                while (iterator.hasNext()) {
                    Map.Entry<VectorTimestamp, CausalWriteForwardPacket> entry = iterator.next();
                    CausalWriteForwardPacket currPacket = entry.getValue();
                    List<VectorTimestamp> currDependencies = currPacket.getDependencies();

                    if (!serviceInstance.executedTimestamps.containsAll(currDependencies)) {
                        continue;
                    }

                    // Insert into DAG under the lock before releasing it for execute().
                    List<GraphVertex> parentGraphVertices =
                            serviceInstance.dag.getVerticesByTimestamps(currDependencies);
                    GraphVertex newGraphVertex = new GraphVertex(
                            currPacket.getRequestTimestamp(),
                            List.of(currPacket.getClientWriteOnlyRequest()));
                    serviceInstance.dag.addChildOf(parentGraphVertices, newGraphVertex);

                    iterator.remove();
                    readyPackets.add(currPacket);
                }
            }

            // Execute all ready packets outside the lock.
            for (CausalWriteForwardPacket currPacket : readyPackets) {
                ClientRequest clientRequest = currPacket.getClientWriteOnlyRequest();
                assert clientRequest instanceof BehavioralRequest br && br.isWriteOnlyRequest() :
                        "Expecting WriteOnlyRequest but got " +
                                clientRequest.getClass().getSimpleName();
                boolean isExecSuccess = this.app.execute(clientRequest);
                assert isExecSuccess : "Failed to execute the pending write request";

                serviceInstance.executedTimestamps.add(currPacket.getRequestTimestamp());
                anyApplied = true;

                // TODO: optionally send Ack to the sender
            }

            // If we applied anything, loop — newly executed packets may unblock more.
        } while (anyApplied && !serviceInstance.pendingForwardPackets.isEmpty());
    }

    private void handleWriteAckPacket(CausalWriteAckPacket packet) {
        String serviceName = packet.getServiceName();
        ReplicaInstance<NodeIDType> serviceInstance = this.instances.get(serviceName);
        if (serviceInstance == null) {
            throw new RuntimeException("Unknown service instance with name=" + serviceName);
        }

        VectorTimestamp ts = packet.getConfirmedGraphNodeId();
        QuorumRecord qr = serviceInstance.graphNodeQuorumRecord.get(ts);
        if (qr == null) {
            this.logger.log(Level.INFO, "Ignoring non-existent vertex in our DAG.");
            return;
        }

        // Record the acknowledgement. confirmingNodeIds is a concurrent set so the add
        // is safe without a lock. The size check + remove is a benign double-remove race:
        // ConcurrentHashMap.remove() is idempotent, and the pruning TODO below will need
        // to be made properly atomic (e.g. computeIfPresent) when implemented.
        qr.confirmingNodeIds().add(packet.getSenderId());
        if (qr.confirmingNodeIds().size() == serviceInstance.nodes().size()) {
            // TODO: send pruning packet to all nodes (can be done asynchronously),
            //  then remove the record.
            serviceInstance.graphNodeQuorumRecord().remove(ts);
        }
    }

}