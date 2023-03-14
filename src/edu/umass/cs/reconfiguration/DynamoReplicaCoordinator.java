package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.dynamo.DynamoPacket;
import edu.umass.cs.reconfiguration.examples.dynamo.DynamoRequest;
import edu.umass.cs.reconfiguration.examples.noop.NoopAppRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @param <NodeIDType>
 * @author Fadhil
 */
public class DynamoReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private Set<NodeIDType> nodeReplicas = null;
    private final NodeIDType myID;
    private final int myIntID;
    // a map of NodeIDType objects to integers
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    private Integer currentEpoch = null;
    private String checkpointState = "";

    // mapping between client request ID to the counter
    private HashMap<Long, Integer> quorumCounter;

    // get the app request by ID
    private HashMap<Long, AppRequest> appRequestByID;
    private HashMap<Long, ExecutedCallback> callbackByRequestID;

    protected static final Logger log = (ReconfigurationConfig.getLogger());
    // variables for messaging purposes

    public DynamoReplicaCoordinator(Replicable app,
                                    NodeIDType id,
                                    Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        this.myIntID = this.integerMap.put(id);
        this.myID = id;
        System.out.println("constructor-" + this.myID);

        // TODO: create quorumCounter per service name
        // currently we assume a single service name
        quorumCounter = new HashMap<Long, Integer>();
        appRequestByID = new HashMap<Long, AppRequest>();
        callbackByRequestID = new HashMap<Long, ExecutedCallback>();
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> packetTypes = new HashSet<IntegerPacketType>();
        packetTypes.add(AppRequest.PacketType.DYNAMO_FORWARD_REQUEST);
        packetTypes.add(AppRequest.PacketType.DYNAMO_FORWARD_ACK);
        return packetTypes;
    }

    /* all requests need to be coordinated, therefore we always return
     * true here */
    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback) throws IOException, RequestParseException {
        System.out.println(">>>>>> [" + this.myID + "] coordinate request ... " + (request == null ? "null" : request));

        // handle client request
        if (request instanceof AppRequest ar) {

            // AppRequest with type == DEFAULT_APP_REQUEST is handled by the entry replica
            // We broadcast the request to other replicas but using type == ANOTHER_APP_REQUEST
            System.out.println(">>>>>> [" + this.myID + "] app-request-type " + ar.getRequestType());

            if (ar.getRequestType() == AppRequest.PacketType.DEFAULT_APP_REQUEST) {
                // store the client's request
                appRequestByID.put(ar.getRequestID(), ar);
                callbackByRequestID.put(ar.getRequestID(), callback);

                // create a broadcast packet
                AppRequest broadcastPacket = new AppRequest(
                        ar.getServiceName(),
                        ar.getEpochNumber(),
                        ar.getRequestID(),
                        ar.getValue(),
                        AppRequest.PacketType.DYNAMO_FORWARD_REQUEST,
                        false
                );
                broadcastPacket.senderID = this.myIntID;

                // broadcast request to other active replicas
                for (NodeIDType nid : nodeReplicas) {
                    if (nid.equals(myID)) continue; // skip sending to ourself
                    System.out.printf(">>>>>> [" + this.myID + "] %s sending to %s: %s\n", this.myID, nid, ar);
                    GenericMessagingTask<NodeIDType, Object> messagingTask = new GenericMessagingTask<NodeIDType, Object>(
                            nid,
                            broadcastPacket);
                    try {
                        this.messenger.send(messagingTask);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    }
                }

                // execute the request locally
                this.execute(ar, false);
                quorumCounter.put(ar.getRequestID(), 1);

                // entry replica need to response to client
                // other replica which we broadcast do not need to
//                replyToClient = true;

                return true;
            }

            // execute and put the response for client (only for entry replica)
//            this.execute(ar, replyToClient);
//            callback.executed(ar, true);
//            System.out.println(">>>>>> [" + this.myID + "] response ... " + ar.getResponse());

            // handle coordination packet: broadcast
            if (ar.getRequestType() == AppRequest.PacketType.DYNAMO_FORWARD_REQUEST) {
                System.out.println(">>>>>> [" + this.myID + "] received broadcast from " + ar.senderID);
                System.out.println(">>>>>> [" + this.myID + "] received broadcast from " + ar.senderID + " " + integerMap.get(ar.senderID));

                // execute locally
                this.execute(new NoopAppRequest(
                        ar.getServiceName(),
                        ar.getEpochNumber(),
                        (int) ar.getRequestID(),
                        ar.getValue(),
                        AppRequest.PacketType.DEFAULT_APP_REQUEST,
                        false
                ), false);

                // prepare ack packet
                AppRequest ackPacket = new AppRequest(
                        ar.getServiceName(),
                        ar.getEpochNumber(),
                        ar.getRequestID(),
                        ar.getValue(),
                        AppRequest.PacketType.DYNAMO_FORWARD_ACK,
                        false
                );
                ackPacket.senderID = this.myIntID;
                GenericMessagingTask<NodeIDType, Object> ackPacketTask = new GenericMessagingTask<NodeIDType, Object>(
                        integerMap.get(ar.senderID),
                        ackPacket
                );

                // send ack back to the sender
                try {
                    this.messenger.send(ackPacketTask);
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }

                return true;
            }

            // handle coordination packet: broadcast ack
            if (ar.getRequestType() == AppRequest.PacketType.DYNAMO_FORWARD_ACK) {
                System.out.println(">>>>>> [" + this.myID + "] received broadcast ack " + integerMap.get(ar.senderID));

                if (!quorumCounter.containsKey(ar.getRequestID())) {
                    quorumCounter.put(ar.getRequestID(), 0);
                }

                // increase the ack counter
                quorumCounter.put(
                        ar.getRequestID(),
                        quorumCounter.get(ar.getRequestID()) + 1);

                // send the response back to client if we have 2 acks already
                if (quorumCounter.get(ar.getRequestID()) == 2) {
                    AppRequest appRequest = appRequestByID.get(ar.getRequestID());
                    ExecutedCallback c = callbackByRequestID.get(ar.getRequestID());
                    c.executed(appRequest, true);
                    System.out.println(">>>>>> [" + this.myID + "] response ... " + appRequest.getResponse());
                    // TODO: cleanup the counter after sending response
                    // TODO: use node.counter as the requestID
                }

                return true;
            }

            return true;
        }

        // handle coordination packet
        if (request instanceof DynamoPacket dp) {

            System.out.println(">>>>>> [" + this.myID + "] protocol-packet-type " + dp.getRequestType());

            if (dp.getRequestType() == DynamoPacket.PacketType.FORWARD_WRITE_REQUEST) {
                // execute the request locally
                this.execute(dp.getAppRequest(), false);

                // ack to the broadcaster
                DynamoPacket broadcastAck = new DynamoPacket(
                        this.myIntID,
                        DynamoPacket.PacketType.FORWARD_WRITE_RESPONSE,
                        dp.getAppRequest(),
                        dp.getEpochNumber());
                GenericMessagingTask<NodeIDType, Object> m = new GenericMessagingTask<NodeIDType, Object>(
                        (NodeIDType) integerMap.get(dp.getSenderID()), broadcastAck);

                System.out.printf(">>>>>> [" + this.myID + "] %s acks to %s: %s\n", this.myID, dp.getSenderID(), dp.getAppRequest());
                try {
                    this.messenger.send(m);
                } catch (JSONException e) {
                    throw new RuntimeException(e);
                }

                return true;
            }

            if (dp.getRequestType() == DynamoPacket.PacketType.FORWARD_WRITE_RESPONSE) {

                if (!quorumCounter.containsKey(dp.getRequestID())) {
                    quorumCounter.put(dp.getRequestID(), 0);
                }

                // increase the ack counter
                quorumCounter.put(
                        dp.getRequestID(),
                        quorumCounter.get(dp.getRequestID()) + 1);

                System.out.printf(">>>>>> [" + this.myID + "] %d acks: %d\n", this.myID, dp.getRequestID(), quorumCounter.get(dp.getRequestID()));

                // send the response back to client if we have 2 acks already
                if (quorumCounter.get(dp.getRequestID()) == 2) {
                    AppRequest ar = appRequestByID.get(dp.getRequestID());
                    ExecutedCallback c = callbackByRequestID.get(dp.getRequestID());
                    c.executed(ar, true);
                    // TODO: cleanup the counter after sending response
                    // TODO: use node.counter as the requestID
                }

                return true;
            }

            System.out.println(">>>>>> [" + this.myID + "] no execute since request is " + dp.getRequestType());
        }

        // else, not a client request not a dynamo coordination packet
        // so ignore it.
        System.out.println(">>>>>> [" + this.myID + "] no execute since request is " + request.getRequestType());

        return true;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        Set<Integer> set = integerMap.put(nodes);
        System.out.println(">>>>>> [" + this.myID + "] create replica group " + serviceName + " " + set.toString());
        nodeReplicas = nodes;
        this.restore(serviceName, state);
        return true;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        throw new RuntimeException("This method should not have been called");
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        System.out.println(">>>>>> [" + this.myID + "] get replica group " + serviceName);
        return nodeReplicas;
    }

    // the methods below are implementing the {@link Reconfigurable} interface

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        return null;
    }

    @Override
    public String getFinalState(String name, int epoch) {
        // snapshot the application and return as string
        this.checkpointState = this.checkpoint(name);
        return this.checkpointState;
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {
        System.out.println(">>>>>> [" + this.myID + "] put initial state: " + name + " " + state);
        this.currentEpoch = epoch;
        this.checkpointState = state;
        super.putInitialState(name, epoch, state);
    }

    @Override
    public boolean deleteFinalState(String name, int epoch) {
        return true;
    }

    @Override
    public Integer getEpoch(String name) {
        return this.currentEpoch;
    }
}
