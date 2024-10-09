package edu.umass.cs.consistency.ClientCentric;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class CCCoordinator<NodeIDType>
        extends AbstractReplicaCoordinator<NodeIDType> {
    private final CCManager<NodeIDType> CCManager;

    public CCCoordinator(Replicable app, NodeIDType myID,
                         Stringifiable<NodeIDType> unstringer,
                         Messenger<NodeIDType, ?> niot) {
        super(app, niot);
        assert (niot instanceof JSONMessenger);
        this.CCManager = new CCManager(myID, unstringer,
                (JSONMessenger<NodeIDType>) niot, this, null,
                true);
    }
    private static Set<IntegerPacketType> requestTypes = null;
    @Override
    public Set<IntegerPacketType> getRequestTypes() {

        if(requestTypes!=null) return requestTypes;
        // FIXME: get request types from a proper app
        Set<IntegerPacketType> types = this.app.getRequestTypes();

        if (types==null) types= new HashSet<IntegerPacketType>();

        for (IntegerPacketType type: CCRequestPacket.CCPacketType.values())
            types.add(type);

        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        return requestTypes = types;
    }
    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback) throws IOException, RequestParseException {
//        System.out.println();
        return this.CCManager.propose(request.getServiceName(), request, callback)!= null;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes) {
        System.out.println(">>>>> Creating replica group of servicename: "+serviceName+", on "+this.getMyID());
        return this.CCManager.createReplicatedQuorumForcibly(
                serviceName, epoch, nodes, this, state);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        return this.CCManager.deleteReplicatedQuorum(serviceName, epoch);
    }
    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return this.CCManager.getReplicaGroup(serviceName);
    }
    @Override
    public Integer getEpoch(String serviceName) {
        return this.CCManager.getVersion(serviceName);
    }
}
