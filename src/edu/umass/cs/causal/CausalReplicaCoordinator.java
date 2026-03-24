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
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
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

                         // buffered write-fwd, waiting for missing dependencies
                         Map<VectorTimestamp, CausalWriteForwardPacket> pendingForwardPackets) {
    }

    private final NodeIDType myNodeID;
    private final Stringifiable<NodeIDType> nodeIdDeserializer;

    private final Set<IntegerPacketType> requestTypes;
    private final Map<String, ReplicaInstance<NodeIDType>> instances;

    private final Logger logger = Logger.getLogger(CausalReplicaCoordinator.class.getName());

    // Dedicated executor for all messenger.send() calls so they never block the
    // calling thread (which may be a Netty EventLoop or GigaPaxos NIO thread).
    // Both the write-forward broadcast and the ack send are submitted here.
    private final ExecutorService replicationExecutor = Executors.newFixedThreadPool(
            Math.max(2, Runtime.getRuntime().availableProcessors() / 2),
            r -> {
                Thread t = new Thread(r, "xdn-causal-replication");
                t.setDaemon(true);
                return t;
            });


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
        ReplicaInstance<NodeIDType> instance =
                new ReplicaInstance<>(
                        /*serviceName=*/serviceName,
                        /*currentEpoch=*/epoch,
                        /*initStateSnapshot=*/state,
                        /*nodes=*/nodes,
                        /*writeQuorumSize=*/writeQuorumSize,
                        /*readQuorumSize=*/readQuorumSize,
                        /*dag=*/new DirectedAcyclicGraph(zeroRootNode),
                        /*pendingForwardPackets=*/new ConcurrentHashMap<>(),
                        /*writeQuorumRecord=*/new ConcurrentHashMap<>());
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

            // Monotonic fast path: no causal dependencies between independent clients.
            // Bypass the DAG entirely — execute locally, respond to client immediately,
            // then broadcast to peers off-thread. Mirrors LazyReplicaCoordinator's write path.
            if (behavioralRequest.isMonotonicRequest()) {
                boolean isExecSuccess = this.app.execute(clientRequest);
                if (isExecSuccess) {
                    stampAll(clientRequest, XdnHttpRequest.TS_CALLBACK);
                    callback.executed(clientRequest, true);

                    // Broadcast to peers with an empty dependency list — peers will detect
                    // isEmpty() and also bypass their DAG on receipt.
                    final Set<NodeIDType> myPeers = new HashSet<>(serviceInstance.nodes());
                    myPeers.remove(myNodeID);
                    if (!myPeers.isEmpty()) {
                        final CausalWriteForwardPacket wfp = new CausalWriteForwardPacket(
                                serviceName,
                                this.myNodeID.toString(),
                                Collections.emptyList(),          // no dependencies — monotonic
                                new VectorTimestamp(List.of()),   // sentinel — not used by peer
                                (ClientRequest) clientRequest);
                        replicationExecutor.submit(() -> {
                            try {
                                messenger.send(new GenericMessagingTask<>(
                                        myPeers.toArray(), wfp));
                            } catch (IOException | JSONException e) {
                                logger.log(Level.WARNING,
                                        "Failed to send monotonic forward packet: " + e.getMessage(), e);
                            }
                        });
                    }
                }
                return isExecSuccess;
            }

            // Non-monotonic slow path: full DAG ordering required.
            // The entire read-modify-write on the DAG must be atomic.
            // Two concurrent writes arriving on different threads (Netty EventLoop vs NIO
            // messenger) would otherwise both read the same leaf set, compute the same
            // dominant timestamp, and produce a duplicate vertex — or race on ArrayList
            // internals inside GraphVertex.children causing the NPE seen at high throughput.
            final CausalWriteForwardPacket wfp;
            synchronized (serviceInstance.dag) {
                // Get the current leaf vector timestamps
                List<GraphVertex> leafOpNodes = serviceInstance.dag.getLeafVertices();
                List<VectorTimestamp> leafTimestamp = new ArrayList<>();
                for (GraphVertex leafNode : leafOpNodes) {
                    leafTimestamp.add(leafNode.getTimestamp());
                }

                // Create a new GraphNode with a dominant timestamp, by increasing this replica's
                // component in the vector timestamp.
                VectorTimestamp maxTimestamp = !leafTimestamp.isEmpty()
                        ? VectorTimestamp.createMaxTimestamp(leafTimestamp)
                        : new VectorTimestamp(nodeIds);
                VectorTimestamp dominantTimestamp = maxTimestamp.increaseNodeTimestamp(myNodeIdStr);
                GraphVertex newOpNode = new GraphVertex(dominantTimestamp, List.of(clientRequest));
                serviceInstance.dag.addChildOf(leafOpNodes, newOpNode);

                // Record that we (the originating node) have already applied this write.
                serviceInstance.graphNodeQuorumRecord.put(
                        dominantTimestamp,
                        QuorumRecord.withInitialNode(myNodeIdStr));

                wfp = new CausalWriteForwardPacket(
                        serviceName,
                        this.myNodeID.toString(),
                        leafTimestamp,
                        dominantTimestamp,
                        (ClientRequest) clientRequest);
            }

            // Execute and respond outside the lock — DAG ordering is already fixed above.
            boolean isExecSuccess = this.app.execute(clientRequest);
            if (isExecSuccess) {
                stampAll(clientRequest, XdnHttpRequest.TS_CALLBACK);
                callback.executed(clientRequest, true);
            }

            // Broadcast outside the lock and off the calling thread — I/O must not
            // block the Netty EventLoop or GigaPaxos NIO thread.
            List<NodeIDType> myPeers = new ArrayList<>();
            for (NodeIDType n : serviceInstance.nodes) {
                if (n.equals(this.myNodeID)) continue;
                myPeers.add(n);
            }
            if (!myPeers.isEmpty()) {
                final GenericMessagingTask<NodeIDType, CausalPacket> m =
                        new GenericMessagingTask<>(myPeers.toArray(), wfp);
                replicationExecutor.submit(() -> {
                    try {
                        this.messenger.send(m);
                    } catch (IOException | JSONException e) {
                        logger.log(Level.WARNING,
                                "Failed to send CausalWriteForwardPacket: " + e.getMessage(), e);
                    }
                });
            }

            return true;
        }

        // Handle read-only request by executing it locally
        if (behavioralRequest.isReadOnlyRequest()) {
            boolean isExecSuccess = this.app.execute(clientRequest);
            assert isExecSuccess : "failed to execute request " + clientRequest;
            stampAll(clientRequest, XdnHttpRequest.TS_CALLBACK);
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

        // Monotonic fast path: empty dependency list means the originating node sent this
        // via the monotonic fast path. No DAG interaction needed — execute directly on the
        // calling NIO thread and return. No ack is sent; the originating node already
        // responded to its client before broadcasting.
        if (dependencies.isEmpty()) {
            ClientRequest clientRequest = packet.getClientWriteOnlyRequest();
            if (clientRequest instanceof XdnHttpRequest xhr) {
                xhr.clearHttpResponse();
            } else if (clientRequest instanceof XdnHttpRequestBatch batch) {
                for (XdnHttpRequest xhr : batch.getRequestList()) xhr.clearHttpResponse();
            }
            this.app.execute(clientRequest, true);
            return;
        }

        // Non-monotonic slow path: dependency check and DAG insert must be atomic.
        final List<ClientRequest> toExecute = new ArrayList<>();
        final CausalWriteAckPacket ackPacket;
        synchronized (serviceInstance.dag) {
            // Validate that we have all the parents node (i.e., dependencies).
            // Must be checked under the lock so the dependency check and the DAG insert
            // are atomic — otherwise a concurrent insert could satisfy a dependency between
            // our check and our buffering, leaving the packet stranded in pending forever.
            boolean isDependenciesSatisfied = serviceInstance.dag.isContainAll(dependencies);

            // If we don't have all the dependencies, then buffer the forwarded write operation so
            // that we can execute the operation later, once the dependencies are satisfied.
            if (!isDependenciesSatisfied) {
                serviceInstance.pendingForwardPackets.put(
                        packet.getRequestTimestamp(), packet);
                return;
            }

            // Resolve DAG position for this write and any newly unblocked pending writes.
            ClientRequest clientRequest = packet.getClientWriteOnlyRequest();
            assert clientRequest instanceof BehavioralRequest behavioralRequest &&
                    behavioralRequest.isWriteOnlyRequest() :
                    "Expecting WriteOnlyRequest but got " + clientRequest.getClass().getSimpleName();

            if (clientRequest instanceof XdnHttpRequest xhr) {
                xhr.clearHttpResponse();
            } else if (clientRequest instanceof XdnHttpRequestBatch batch) {
                for (XdnHttpRequest xhr : batch.getRequestList()) {
                    xhr.clearHttpResponse();
                }
            }

            // Update my local causal directly-acyclic graph
            List<GraphVertex> parentGraphVertices =
                    serviceInstance.dag.getVerticesByTimestamps(dependencies);
            GraphVertex newGraphVertex = new GraphVertex(
                    packet.getRequestTimestamp(),
                    List.of(packet.getClientWriteOnlyRequest()));
            serviceInstance.dag.addChildOf(parentGraphVertices, newGraphVertex);

            toExecute.add(clientRequest);

            // Get the current graph leaf nodes' ID for the ack
            List<GraphVertex> graphLeafNodes = serviceInstance.dag.getLeafVertices();
            List<VectorTimestamp> graphLeafNodeIds = new ArrayList<>();
            for (GraphVertex n : graphLeafNodes) {
                graphLeafNodeIds.add(n.getTimestamp());
            }
            ackPacket = new CausalWriteAckPacket(
                    serviceName,
                    this.myNodeID.toString(),
                    packet.getRequestTimestamp(),
                    graphLeafNodeIds);

            // Handle all pending forwarded requests while still holding the lock,
            // so their dependency checks and DAG inserts are also atomic.
            handlePendingForwardedWriteAfterPackets(serviceInstance, toExecute);
        }

        // Execute all resolved writes outside the lock, on the calling thread.
        for (ClientRequest cr : toExecute) {
            this.app.execute(cr, true);
        }

        // Send acknowledgment outside the lock and off the calling thread.
        String senderId = packet.getSenderId();
        NodeIDType senderNodeId = this.nodeIdDeserializer.valueOf(senderId);
        final GenericMessagingTask<NodeIDType, CausalPacket> m =
                new GenericMessagingTask<>(senderNodeId, ackPacket);
        replicationExecutor.submit(() -> {
            try {
                this.messenger.send(m);
            } catch (IOException | JSONException e) {
                logger.log(Level.WARNING,
                        "Failed to send CausalWriteAckPacket: " + e.getMessage(), e);
            }
        });
    }

    /**
     * Must be called while holding {@code synchronized(serviceInstance.dag)}.
     * Iterates pending packets repeatedly until no more can be unblocked, since
     * applying one pending packet may satisfy the dependencies of another.
     * Resolved requests are appended to {@code toExecute} for execution outside the lock.
     */
    private void handlePendingForwardedWriteAfterPackets(
            ReplicaInstance<NodeIDType> serviceInstance,
            List<ClientRequest> toExecute) {
        assert serviceInstance != null : "Unexpected null ServiceInstance";
        if (serviceInstance.pendingForwardPackets.isEmpty()) {
            return;
        }

        // Repeat until a full pass finds nothing new to apply, because applying one
        // pending packet can satisfy the dependencies of another pending packet.
        boolean anyApplied;
        do {
            anyApplied = false;
            Iterator<Map.Entry<VectorTimestamp, CausalWriteForwardPacket>> iterator =
                    serviceInstance.pendingForwardPackets.entrySet().iterator();
            while (iterator.hasNext()) {
                Map.Entry<VectorTimestamp, CausalWriteForwardPacket> entry = iterator.next();
                CausalWriteForwardPacket currPacket = entry.getValue();
                List<VectorTimestamp> currDependencies = currPacket.getDependencies();
                boolean isDepSatisfied = serviceInstance.dag.isContainAll(currDependencies);

                // Skip if the dependencies are still not satisfied
                if (!isDepSatisfied) continue;

                // Resolve DAG position for this pending write
                ClientRequest clientRequest = currPacket.getClientWriteOnlyRequest();
                assert clientRequest instanceof BehavioralRequest behavioralRequest &&
                        behavioralRequest.isWriteOnlyRequest() :
                        "Expecting WriteOnlyRequest but got " + clientRequest.getClass().getSimpleName();

                // Update my local causal directly-acyclic graph
                List<GraphVertex> parentGraphVertices =
                        serviceInstance.dag.getVerticesByTimestamps(currDependencies);
                GraphVertex newGraphVertex = new GraphVertex(
                        currPacket.getRequestTimestamp(),
                        List.of(currPacket.getClientWriteOnlyRequest()));
                serviceInstance.dag.addChildOf(parentGraphVertices, newGraphVertex);

                toExecute.add(clientRequest);

                // remove the pending write packet from the map
                iterator.remove();
                anyApplied = true;

                // TODO: optionally send Ack to the sender
            }
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

        // records the acknowledgement
        qr.confirmingNodeIds().add(packet.getSenderId());
        if (qr.confirmingNodeIds().size() == serviceInstance.nodes().size()) {
            // TODO: send pruning packet to all nodes (can be done asynchronously),
            //  then remove the record.
            serviceInstance.graphNodeQuorumRecord().remove(ts);
            return;
        }
    }

    private void stampAll(Request request, int stage) {
        if (!XdnHttpRequest.ENABLE_LATENCY_TRACING) return;
        if (request instanceof XdnHttpRequestBatch batch) {
            for (XdnHttpRequest xhr : batch.getRequestList()) xhr.stamp(stage);
        } else if (request instanceof XdnHttpRequest xhr) {
            xhr.stamp(stage);
        }
    }
}