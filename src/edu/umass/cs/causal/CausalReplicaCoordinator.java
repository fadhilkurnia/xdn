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
 * - the use of directed acyclic graph (DAG), instead of queue, for the InQueue in each node,
 * - the wait of a majority quorum in the write path, ensuring the written value survive when up to
 * half of the replicas are unavailable.
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

                         // buffered write-request, waiting for write quorum
                         Map<VectorTimestamp, List<ClientRequestAndCallback>> pendingRequests,

                         Map<VectorTimestamp, QuorumRecord> graphNodeQuorumRecord,

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

        // Add packet demultiplexer for CausalPacket that will invoke the coordinateRequest() method
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
                request);

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
        GraphVertex zeroRootNode = new GraphVertex(new VectorTimestamp(nodesIds), new ArrayList<>());
        ReplicaInstance<NodeIDType> instance =
                new ReplicaInstance<>(
                        /*serviceName=*/serviceName,
                        /*currentEpoch=*/epoch,
                        /*initStateSnapshot=*/state,
                        /*nodes=*/nodes,
                        /*writeQuorumSize=*/writeQuorumSize,
                        /*readQuorumSize=*/readQuorumSize,
                        /*dag=*/new DirectedAcyclicGraph(zeroRootNode),
                        /*pendingRequests=*/new HashMap<>(),
                        /*pendingForwardPackets=*/new HashMap<>(),
                        /*writeQuorumRecord=*/new HashMap<>());
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

        // FIXME: stop packet should not be wrapped as client request
        if (clientReplicableRequest.getRequest() instanceof ReconfigurableRequest rcRequest &&
                rcRequest.isStop()) {
            System.out.println(">> CausalReplicaCoordinator -- stopping a service in epoch=" +
                    rcRequest.getEpochNumber());
            return this.app.restore(clientReplicableRequest.getServiceName(), null);
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

        // handle write-only request by forwarding it to the write-quorum
        if (behavioralRequest.isWriteOnlyRequest()) {
            // get the leaf timestamps
            List<GraphVertex> leafOpNodes = serviceInstance.dag.getLeafVertices();
            List<VectorTimestamp> leafTimestamp = new ArrayList<>();
            for (GraphVertex leafNode : leafOpNodes) {
                leafTimestamp.add(leafNode.getTimestamp());
            }

            // Create a new GraphNode with a dominant timestamp, by increasing our component
            // in the timestamp.
            VectorTimestamp maxTimestamp = !leafTimestamp.isEmpty()
                    ? VectorTimestamp.createMaxTimestamp(leafTimestamp)
                    : new VectorTimestamp(nodeIds);
            VectorTimestamp dominantTimestamp = maxTimestamp.increaseNodeTimestamp(myNodeIdStr);
            GraphVertex newOpNode = new GraphVertex(dominantTimestamp, List.of(clientRequest));
            serviceInstance.dag.addChildOf(leafOpNodes, newOpNode);

            serviceInstance.pendingRequests.putIfAbsent(
                    dominantTimestamp,
                    List.of(new ClientRequestAndCallback((ClientRequest) clientRequest, callback)));

            // execute the request
            boolean isExecSuccess = this.app.execute(clientRequest);
            assert isExecSuccess : "failed to execute request " + clientRequest;

            // Broadcast the write request to all, then this node need to wait for
            // the write quorum.
            serviceInstance.graphNodeQuorumRecord.put(
                    dominantTimestamp,
                    new QuorumRecord(
                            new HashSet<>(Collections.singletonList(myNodeIdStr))));
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

            // Handle an edge-case when write quorum is one (i.e., myself is enough)
            QuorumRecord qr = serviceInstance.graphNodeQuorumRecord.get(dominantTimestamp);
            boolean isQuorumAchieved =
                    qr.confirmingNodeIds.size() >= serviceInstance.writeQuorumSize;
            if (isQuorumAchieved) {
                serviceInstance.graphNodeQuorumRecord.remove(dominantTimestamp);
                List<ClientRequestAndCallback> callbacks =
                        serviceInstance.pendingRequests.get(dominantTimestamp);
                for (ClientRequestAndCallback cb : callbacks) {
                    Request request = cb.request();
                    cb.callback.executed(request, true);
                }
            }

            return true;
        }

        // handle read-only request by executing it locally
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
            return this.handleWriteForwardPacket(writeForwardPacket);
        }

        if (packet instanceof CausalWriteAckPacket ackPacket) {
            return this.handleWriteAckPacket(ackPacket);
        }

        throw new RuntimeException("Unimplemented handler of packet " + packet.getRequestType());
    }

    private boolean handleWriteForwardPacket(CausalWriteForwardPacket packet) {
        String serviceName = packet.getServiceName();
        ReplicaInstance<NodeIDType> serviceInstance = this.instances.get(serviceName);
        assert serviceInstance != null : "unknown service with name=" + serviceName;

        // Validate that we have all the parents node (i.e., dependencies)
        List<VectorTimestamp> dependencies = packet.getDependencies();
        boolean isDependenciesSatisfied = serviceInstance.dag.isContainAll(dependencies);

        // If we don't have all the dependencies, then buffer the forwarded write operation.
        if (!isDependenciesSatisfied) {
            serviceInstance.pendingForwardPackets.put(
                    packet.getRequestTimestamp(), packet);
            // TODO: request the missing dependencies from other nodes.
            throw new RuntimeException("Unimplemented: missing dependencies, " +
                    "need to sync with our peer.");
        }

        // Execute the request
        ClientRequest clientRequest = packet.getClientWriteOnlyRequest();
        // TODO: assert the client request type is write only
        boolean isExecSuccess = this.app.execute(clientRequest);
        assert isExecSuccess;

        // Update my local causal graph
        List<GraphVertex> parentGraphVertices =
                serviceInstance.dag.getVerticesByTimestamps(dependencies);
        GraphVertex newGraphVertex = new GraphVertex(
                packet.getRequestTimestamp(),
                List.of(packet.getClientWriteOnlyRequest()));
        serviceInstance.dag.addChildOf(parentGraphVertices, newGraphVertex);

        // Get the current graph leaf nodes' ID
        List<GraphVertex> graphLeafNodes = serviceInstance.dag.getLeafVertices();
        List<VectorTimestamp> graphLeafNodeIds = new ArrayList<>();
        for (GraphVertex n : graphLeafNodes) {
            graphLeafNodeIds.add(n.getTimestamp());
        }

        // Send acknowledgment to the sender
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

        return true;
    }

    private boolean handleWriteAckPacket(CausalWriteAckPacket packet) {
        String serviceName = packet.getServiceName();
        ReplicaInstance<NodeIDType> serviceInstance = this.instances.get(serviceName);
        if (serviceInstance == null) {
            throw new RuntimeException("Unknown service instance with name=" + serviceName);
        }

        VectorTimestamp ts = packet.getConfirmedGraphNodeId();
        QuorumRecord qr = serviceInstance.graphNodeQuorumRecord.get(ts);
        if (qr == null) {
            this.logger.log(Level.INFO, "Ignoring non-existent vertex.");
            return true;
        }

        // records the acknowledgement
        qr.confirmingNodeIds().add(packet.getSenderId());
        if (qr.confirmingNodeIds().size() == serviceInstance.nodes().size()) {
            // TODO: send prunning packet to all nodes (can be done asynchronously),
            //  then remove the record.
            serviceInstance.graphNodeQuorumRecord().remove(ts);
            return true;
        }

        // checks quorum requirement
        boolean isQuorumAchieved = qr.confirmingNodeIds.size() >= serviceInstance.writeQuorumSize;
        if (!isQuorumAchieved) {
            return true;
        }

        // quorum is achieved, return the response back to client (if we haven't).
        List<ClientRequestAndCallback> callbacks = serviceInstance.pendingRequests.get(ts);
        if (callbacks == null) {
            // we have returned response previously for this vertex
            return true;
        }

        // actually send response back to client, because we haven't yet.
        for (ClientRequestAndCallback cb : callbacks) {
            Request request = cb.request();
            cb.callback.executed(request, true);
        }
        serviceInstance.pendingRequests.remove(ts);

        return true;
    }

}
