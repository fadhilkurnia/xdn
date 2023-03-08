package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.IntegerMap;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * @param <NodeIDType>
 * @author Fadhil
 */
public class DynamoReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private Set<NodeIDType> nodeReplicas;
    private final int myID;
    // a map of NodeIDType objects to integers
    private final IntegerMap<NodeIDType> integerMap = new IntegerMap<NodeIDType>();
    private int currentEpoch = 0;
    private String checkpointState = "";

    // variables for messaging purposes


    public DynamoReplicaCoordinator(Replicable app,
                                    NodeIDType id,
                                    Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        this.myID = this.integerMap.put(id);
        System.out.println("constructor-" + this.myID);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> packetTypes = new HashSet<IntegerPacketType>();
        packetTypes.add(ReconfigurationPacket.PacketType.HELLO_REQUEST);
        packetTypes.add(ReconfigurationPacket.PacketType.NO_TYPE);
        packetTypes.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        return packetTypes;
    }

    /* all requests need to be coordinated, therefore we always return
     * true here */
    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback) throws IOException, RequestParseException {
        System.out.println("coordinate request ... " + request.toString());

        if (request instanceof ReplicableClientRequest rcr) {
            if (rcr.getRequest() instanceof AppRequest ar) {
                execute(rcr.getRequest(), false);
                System.out.println("response ... " + ar.getResponse());
            } else {
                System.out.println("no execute since request's class is " + rcr.getRequest().getClass().getSimpleName());
            }
        } else {
            System.out.println("no execute since class is " + request.getClass().getSimpleName());
        }
        return true;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        System.out.println("create replica group " + serviceName + nodes.toString());
        nodeReplicas = nodes;
        return true;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        nodeReplicas.clear();
        return true;
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        System.out.println("get replica group " + serviceName);
        return nodeReplicas;
    }

    // the methods below are implementing the {@link Reconfigurable} interface

    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        return null;
    }

    @Override
    public String getFinalState(String name, int epoch) {
        return this.checkpointState;
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {
        System.out.println(">>>> " + name + " " + state);
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
