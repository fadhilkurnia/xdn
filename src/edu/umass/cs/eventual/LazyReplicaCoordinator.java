package edu.umass.cs.eventual;

import edu.umass.cs.eventual.packets.LazyPacket;
import edu.umass.cs.eventual.packets.LazyPacketType;
import edu.umass.cs.eventual.packets.LazyWriteAfterPacket;
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
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class LazyReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private final NodeIDType myNodeId;
    private final Replicable app;
    private final Set<IntegerPacketType> packetTypes;
    private final Messenger<NodeIDType, JSONObject> messenger;

    private record LazyReplicaInstance<NodeIDType>(String serviceName,
                                                   int currEpoch,
                                                   String initStateSnapshot,
                                                   Set<NodeIDType> nodes) {
    }

    private final ConcurrentMap<String, LazyReplicaInstance<NodeIDType>> currentInstances;

    private final Logger logger = Logger.getLogger(LazyReplicaCoordinator.class.getSimpleName());

    public LazyReplicaCoordinator(Replicable app,
                                  NodeIDType myId,
                                  Stringifiable<NodeIDType> nodeIdDeserializer,
                                  Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        this.myNodeId = myId;
        this.messenger = messenger;
        this.app = app;

        // validate the nodeIdDeserializer
        assert messenger.getMyID().equals(myId) : "Invalid node ID given in the messenger";
        assert nodeIdDeserializer.valueOf(this.myNodeId.toString()).equals(this.myNodeId)
                : "Invalid node ID deserializer given";

        // initialize all the supported packet types
        this.packetTypes = new HashSet<>();
        this.packetTypes.addAll(List.of(LazyPacketType.values()));

        this.currentInstances = new ConcurrentHashMap<>();

        // add packet demultiplexer for LaztPacket that will invoke
        // the coordinateRequest() method.
        LazyPacketDemultiplexer packetDemultiplexer =
                new LazyPacketDemultiplexer(this, app);
        this.messenger.precedePacketDemultiplexer(packetDemultiplexer);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return this.packetTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        System.out.println(">> " + myNodeId + " LazyReplicaCoordinator -- receiving request " +
                request.getClass().getSimpleName());
        if (!(request instanceof ReplicableClientRequest) && !(request instanceof LazyPacket)) {
            throw new RuntimeException("Unknown request/packet handled by LazyReplicaCoordinator");
        }

        // validate that service exists
        String serviceName = request.getServiceName();
        if (!this.currentInstances.containsKey(serviceName)) {
            logger.log(Level.WARNING, "Ignoring request for unknown service=" + serviceName);
            return true;
        }
        LazyReplicaInstance<NodeIDType> currInstance = this.currentInstances.get(serviceName);

        // unwrap the request if needed
        Request currRequestOrPacket = request;
        if (currRequestOrPacket instanceof ReplicableClientRequest rcr) {
            currRequestOrPacket = rcr.getRequest();
        }

        // handle StopEpoch packet
        if (currRequestOrPacket instanceof ReconfigurableRequest rcRequest &&
                rcRequest.isStop()) {
            boolean isSuccess = this.app.restore(serviceName, null);
            callback.executed(rcRequest, isSuccess);
            return true;
        }

        // handle client-initiated request
        if (currRequestOrPacket instanceof ClientRequest clientRequest) {
            // validates that request is either ReadOnly or (WriteOnly and Monotonic).
            if (!(clientRequest instanceof BehavioralRequest br)) {
                throw new RuntimeException("Expecting BehavioralRequest for LazyReplicaCoordinator");
            }
            boolean isReadOnly = br.isReadOnlyRequest();
            boolean isMonotonic = br.isMonotonicRequest();
            boolean isWriteOnly = br.isWriteOnlyRequest();
            if (!(isReadOnly || (isMonotonic && isWriteOnly))) {
                throw new RuntimeException(
                        "Expecting ReadOnly request or (Monotonic and WriteOnly) request " +
                                "for LazyReplicaCoordinator");
            }

            boolean isExecSuccess = this.app.execute(clientRequest);
            if (isExecSuccess) {
                callback.executed(clientRequest, true);
            }

            // for write request, send WRITE_AFTER packets to all replicas but myself
            // Response are removed in receiver side before execution.
            if (isWriteOnly && isMonotonic) {
                LazyPacket writeAfterPacket = new LazyWriteAfterPacket(
                        myNodeId.toString(), clientRequest);
                Set<NodeIDType> myPeers = currInstance.nodes();
                myPeers.remove(myNodeId);
                GenericMessagingTask<NodeIDType, LazyPacket> m =
                        new GenericMessagingTask<>(myPeers.toArray(), writeAfterPacket);
                try {
                    logger.log(Level.INFO,  "Sending WRITE_AFTER packet ...");
                    messenger.send(m);
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }
            }

            return isExecSuccess;
        }

        // handle peer-initiated packet (i.e., write-after)
        if (currRequestOrPacket instanceof LazyPacket lp &&
                lp instanceof LazyWriteAfterPacket writeAfterPacket) {
            Request clientRequest = writeAfterPacket.getClientWriteOnlyRequest();
            boolean isWriteOnly = (clientRequest instanceof BehavioralRequest br) &&
                    br.isWriteOnlyRequest();
            boolean isMonotonic = (clientRequest instanceof BehavioralRequest br) &&
                    br.isMonotonicRequest();
            if (!isWriteOnly || !isMonotonic) {
                throw new RuntimeException(
                        "Expecting (Monotonic and WriteOnly) request for LazyReplicaCoordinator");
            }

            if (clientRequest instanceof XdnHttpRequest xhr) {
                xhr.clearHttpResponse();
            } else if (clientRequest instanceof XdnHttpRequestBatch batch) {
                for (XdnHttpRequest xhr: batch.getRequestList()) {
                    xhr.clearHttpResponse();
                }
            }

            return this.app.execute(clientRequest, true);
        }

        throw new IllegalStateException("Unexpected LazyPacket/Request: " + request.getRequestType());
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes, String placementMetadata) {
        System.out.printf(">> %s:LazyReplicaCoordinator -- " +
                        "createReplicaGroup name=%s nodes=%s epoch=%d state=%s\n",
                myNodeId, serviceName, nodes, epoch, state);
        LazyReplicaInstance<NodeIDType> replicaInstance =
                new LazyReplicaInstance<>(serviceName, epoch, state, nodes);
        this.currentInstances.put(serviceName, replicaInstance);

        // Creating a replica group is a special case for reconfiguration where we reconfigure
        // from nothing to something. In that case, we call app.restore(.) with initialState.
        return this.app.restore(serviceName, state);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        if (!this.currentInstances.containsKey(serviceName)) return true;
        LazyReplicaInstance<NodeIDType> replicaInstance = this.currentInstances.get(serviceName);
        if (replicaInstance.currEpoch != epoch) return true;
        this.currentInstances.remove(serviceName);

        // Deleting a replica group is a special case for reconfiguration where we reconfigure
        // from something into nothing. In that case, we call app.restore(.) with null state.
        return this.app.restore(serviceName, null);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        LazyReplicaInstance<NodeIDType> replicaInstance = this.currentInstances.get(serviceName);
        return replicaInstance != null ? replicaInstance.nodes() : null;
    }
}
