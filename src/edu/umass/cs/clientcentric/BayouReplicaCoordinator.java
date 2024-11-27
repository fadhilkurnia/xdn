package edu.umass.cs.clientcentric;

import edu.umass.cs.clientcentric.packets.ClientCentricPacketType;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

// TODO: allowing client to specify the requested consistency-model per-request (per session)
//  not only per-service that we are currently have.

// TODO: Provide validation that restrict this coordinator being used only for application
//  whose all of its operations are monotonic.

// TODO: implement prunning (or GC) mechanism to cut the prefix of the executedRequests list.
//  One approach is to use the peerTime to get the minimum sequenceNumber of write requests
//  executed by this replica. Then we also need to store the startingSequenceNumber for this
//  replica.

// TODO: make executedRequests persistent by storing it on disk.

public class BayouReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    public static final ConsistencyModel DEFAULT_CLIENT_CENTRIC_CONSISTENCY_MODEL =
            ConsistencyModel.MONOTONIC_READS;

    public record ReplicaInstance<NodeIDType>
            (String name,                                    // the service name
             ConsistencyModel consistencyModel,              // the requested consistency model
             int currEpoch,                                  // current placement epoch
             String lastSnapshot,                            // last checkpoint snapshot
             Set<NodeIDType> nodeIDs,                        // set of replicas for this service
             VectorTimestamp currTimestamp,                  // the service's current timestamp
             Map<Long, RequestAndCallback> pendingRequests,  // buffered request, waiting for sync
             List<byte[]> executedRequests,                  // ordered executed write requests
             Map<NodeIDType, VectorTimestamp> peerTimestamp
            ) {
    }

    private final NodeIDType myNodeID;
    private final Stringifiable<NodeIDType> nodeIdDeserializer;
    private final Replicable app;
    private final Logger logger = Logger.getGlobal();
    private final Set<IntegerPacketType> packetTypes;
    private final Map<String, ReplicaInstance<NodeIDType>> instances;

    public BayouReplicaCoordinator(Replicable app,
                                   NodeIDType myNodeID,
                                   Stringifiable<NodeIDType> nodeIdDeserializer,
                                   Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        this.myNodeID = myNodeID;
        this.nodeIdDeserializer = nodeIdDeserializer;
        this.app = app;
        this.instances = new HashMap<>();

        // validate the nodeIdDeserializer
        assert this.nodeIdDeserializer.valueOf(myNodeID.toString()).equals(myNodeID) :
                "Invalid NodeIDType deserializer given";
        assert messenger.getMyID().equals(myNodeID) : "Invalid NodeID given in the messenger";

        // initialize all the supported packet types
        this.packetTypes = new HashSet<>();
        packetTypes.addAll(List.of(ClientCentricPacketType.values()));

        // add packet demultiplexer for ClientCentricPacket that will invoke
        // the coordinateRequest() method
        ClientCentricPacketDemultiplexer packetDemultiplexer =
                new ClientCentricPacketDemultiplexer(this, app);
        this.messenger.precedePacketDemultiplexer(packetDemultiplexer);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return this.packetTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        logger.log(Level.INFO, "Coordinating request " + request);

        String serviceName = request.getServiceName();
        ReplicaInstance<NodeIDType> serviceInstance = instances.get(serviceName);
        if (serviceInstance == null) {
            logger.log(Level.WARNING, "Coordinating request for unknown service name="
                    + serviceName);
            return true;
        }

        if (serviceInstance.consistencyModel.equals(ConsistencyModel.MONOTONIC_READS)) {
            return MonotonicReadsHandler.coordinateRequest(
                    request, callback, serviceInstance, this.app, this.nodeIdDeserializer,
                    this.messenger);
        }

        if (serviceInstance.consistencyModel.equals(ConsistencyModel.MONOTONIC_WRITES)) {
            return MonotonicWritesHandler.coordinateRequest(
                    request, callback, serviceInstance, this.app, this.nodeIdDeserializer,
                    this.messenger);
        }

        if (serviceInstance.consistencyModel.equals(ConsistencyModel.READ_YOUR_WRITES)) {
            return ReadYourWritesHandler.coordinateRequest(
                    request, callback, serviceInstance, this.app, this.nodeIdDeserializer,
                    this.messenger);
        }

        if (serviceInstance.consistencyModel.equals(ConsistencyModel.WRITES_FOLLOW_READS)) {
            return WritesFollowReadsHandler.coordinateRequest(
                    request, callback, serviceInstance, this.app, this.nodeIdDeserializer,
                    this.messenger);
        }

        throw new RuntimeException("Unknown client-centric consistency model " +
                serviceInstance.consistencyModel.name() + " for service with name=" + serviceName);
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes, String placementMetadata) {
        return this.createReplicaGroup(
                /*consistencyModel=*/DEFAULT_CLIENT_CENTRIC_CONSISTENCY_MODEL,
                /*serviceName=*/serviceName,
                /*epoch=*/epoch,
                /*state=*/state,
                /*nodes=*/nodes);
    }

    public boolean createReplicaGroup(ConsistencyModel consistencyModel, String serviceName,
                                      int epoch, String state, Set<NodeIDType> nodes) {
        // Initialize an empty vector timestamp.
        List<String> nodeIDs = new ArrayList<>();
        for (NodeIDType nodeID : nodes) {
            nodeIDs.add(nodeID.toString());
        }
        VectorTimestamp timestamp = new VectorTimestamp(nodeIDs);

        // Initialize empty vector timestamp for each of the peer.
        Map<NodeIDType, VectorTimestamp> peerTimestamp = new HashMap<>();
        for (NodeIDType nodeID : nodes) {
            if (nodeID.equals(myNodeID)) continue;
            peerTimestamp.put(nodeID, new VectorTimestamp(nodeIDs));
        }

        // Register the created instance.
        this.instances.put(serviceName,
                new ReplicaInstance<>(
                        /*name=*/serviceName,
                        /*consistencyModel=*/consistencyModel,
                        /*currEpoch=*/epoch,
                        /*lastSnapshot=*/state,
                        /*nodeIDs=*/nodes,
                        /*currTimestamp=*/timestamp,
                        /*pendingRequests=*/new ConcurrentHashMap<>(),
                        /*executedRequests=*/new ArrayList<>(),
                        /*peerTimestamp=*/peerTimestamp));

        // Start the app using the restore method with the passed initial state.
        return this.app.restore(serviceName, state);

        // TODO: start periodic synchronization based on serviceInstance.peerTimestamp.
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        ReplicaInstance<NodeIDType> removedInstance = this.instances.remove(serviceName);
        if (removedInstance == null) {
            return true;
        }
        return this.app.restore(serviceName, null);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        ReplicaInstance<NodeIDType> targetInstance = this.instances.get(serviceName);
        if (targetInstance == null) return null;
        return targetInstance.nodeIDs;
    }
}
