package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;

/**
 * @param <NodeIDType>
 * @author Fadhil
 */
public class DynamoReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private Set<NodeIDType> nodeReplicas;
    private final NodeIDType myID;
    // a map of NodeIDType objects to integers
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    private int currentEpoch = 0;
    private String checkpointState = "";

    protected static final Logger log = (ReconfigurationConfig.getLogger());
    // variables for messaging purposes


    public DynamoReplicaCoordinator(Replicable app,
                                    NodeIDType id,
                                    Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        this.integerMap.put(id);
        this.myID = id;
        System.out.println("constructor-" + this.myID);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> packetTypes = new HashSet<IntegerPacketType>();
        packetTypes.add(AppRequest.PacketType.DEFAULT_APP_REQUEST);
        packetTypes.add(AppRequest.PacketType.ADMIN_APP_REQUEST);
        return packetTypes;
    }

    /* all requests need to be coordinated, therefore we always return
     * true here */
    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback) throws IOException, RequestParseException {
        System.out.println(">>>>>> [" + this.myID + "] coordinate request ... " + (request == null ? "null" : request));

        if (request instanceof AppRequest ar) {

            // AppRequest with type == DEFAULT_APP_REQUEST is handled by the entry replica
            // We broadcast the request to other replicas but using type == ADMIN_APP_REQUEST
            System.out.println(">>>>>> [" + this.myID + "] app-request-type " + ar.getRequestType());

            boolean replyToClient = false;
            if (ar.getRequestType() == AppRequest.PacketType.DEFAULT_APP_REQUEST) {
                AppRequest broadcastRequest = new AppRequest(
                        ar.getServiceName(),
                        ar.getValue(),
                        AppRequest.PacketType.ADMIN_APP_REQUEST,
                        false);

                // broadcast request to other active replicas
                for (NodeIDType nid : nodeReplicas) {
                    if (nid.equals(myID)) continue;
                    System.out.printf(">>>>>> [" + this.myID + "] %s sending to %s: %s\n", this.myID, nid, ar);
                    GenericMessagingTask<NodeIDType, ?> messagingTask = new GenericMessagingTask(
                            nid,
                            ReplicableClientRequest.wrap(broadcastRequest, true));

                    try {
                        this.messenger.send(messagingTask);
                    } catch (JSONException e) {
                        throw new RuntimeException(e);
                    }
                }

                // entry replica need to response to client
                // other replica which we broadcast do not need to
                replyToClient = true;
            }

            // execute and put the response for client (only for entry replica)
            this.execute(ar, replyToClient);
            callback.executed(ar, true);
            System.out.println(">>>>>> [" + this.myID + "] response ... " + ar.getResponse());

        } else {
            System.out.println(">>>>>> [" + this.myID + "] no execute since request is " + request.getRequestType());
        }
        return true;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        System.out.println(">>>>>> [" + this.myID + "] create replica group " + serviceName + " " + nodes.toString());
        nodeReplicas = nodes;
        return true;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        throw new RuntimeException("This method should not have been called");
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        System.out.println(">>>>>> [" + this.myID + "] get replica group " + serviceName);

        // TODO: confirm this hacky approach
        String temp = this.checkpoint(serviceName);
        temp = temp == null ? "" : temp;
        this.restore(serviceName, temp);
        return nodeReplicas;
    }

    // the methods below are implementing the {@link Reconfigurable} interface

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        return null;
    }

    @Override
    public String getFinalState(String name, int epoch) {
        this.checkpointState = this.checkpoint(name);
        return this.checkpointState;
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {
        System.out.println(">>>>>> [" + this.myID + "] put initial state: " + name + " " + state);
        this.currentEpoch++;
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
