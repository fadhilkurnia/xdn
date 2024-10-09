package edu.umass.cs.consistency.EventualConsistency;

import edu.umass.cs.gigapaxos.PaxosCoordinator;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class DynamoCoordinator<NodeIDType>
        extends PaxosReplicaCoordinator<NodeIDType> {
    private final DynamoManager<NodeIDType> dynamoManager;
    public DynamoCoordinator(Replicable app, NodeIDType myID,
                             Stringifiable<NodeIDType> unstringer,
                             Messenger<NodeIDType, ?> niot) {
        super(app, myID, unstringer, niot);
        assert (niot instanceof JSONMessenger);
        this.dynamoManager = new DynamoManager(myID, unstringer,
                (JSONMessenger<NodeIDType>) niot, this, "logs/DAGLogs.log",
                true);
        setReconfigurationServices();
    }
    private static Set<IntegerPacketType> requestTypes = null;
    private static final Set<String> reconfigurationServices = new HashSet<>();
    private void setReconfigurationServices(){
        reconfigurationServices.add("AR_AR_NODES");
        reconfigurationServices.add("AR_RC_NODES");
        reconfigurationServices.add("RC_RC_NODES");
        reconfigurationServices.add("RC_AR_NODES");
    }
    @Override
    public Set<IntegerPacketType> getRequestTypes() {

        if(requestTypes!=null) return requestTypes;
        // FIXME: get request types from a proper app
        Set<IntegerPacketType> types = this.app.getRequestTypes();

        if (types==null) types= new HashSet<IntegerPacketType>();

        for (IntegerPacketType type: DynamoRequestPacket.DynamoPacketType.values())
            types.add(type);

        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        return requestTypes = types;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback) throws IOException, RequestParseException {
        System.out.println(request.getRequestType());
        return reconfigurationServices.contains(request.getServiceName()) ? super.coordinateRequest(request, callback) :
                this.dynamoManager.propose(request.getServiceName(), request, callback)!= null;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        return reconfigurationServices.contains(serviceName) ? super.createReplicaGroup(serviceName, epoch, state, nodes)
                : this.dynamoManager.createReplicatedQuorumForcibly(
                serviceName, epoch, nodes, this, state);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        return reconfigurationServices.contains(serviceName) ? super.deleteReplicaGroup(serviceName, epoch) : this.dynamoManager.deleteReplicatedQuorum(serviceName, epoch);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return reconfigurationServices.contains(serviceName) ? super.getReplicaGroup(serviceName) : this.dynamoManager.getReplicaGroup(serviceName);
    }
    @Override
    public Integer getEpoch(String name) {
        return this.dynamoManager.getVersion(name);
    }


}
