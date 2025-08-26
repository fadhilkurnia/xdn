package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.Set;

public class SetReplicaPlacementRequest extends ClientReconfigurationPacket {

    private final Set<String> newReplicaPlacement;
    private final String coordinatorNodeId;

    public SetReplicaPlacementRequest(InetSocketAddress initiator,
                                      String name,
                                      Set<String> placement) {
        super(initiator, PacketType.SET_REPLICA_PLACEMENT_REQUEST, name, 0);
        this.newReplicaPlacement = placement;
        this.coordinatorNodeId = null;
    }

    public SetReplicaPlacementRequest(InetSocketAddress initiator,
                                      String name,
                                      Set<String> placement,
                                      String coordinatorNodeId) {
        super(initiator, PacketType.SET_REPLICA_PLACEMENT_REQUEST, name, 0);
        this.newReplicaPlacement = placement;
        this.coordinatorNodeId = coordinatorNodeId;
    }

    public Set<String> getNewReplicaPlacement() {
        return newReplicaPlacement;
    }

    public String getCoordinatorNodeId() {
        return coordinatorNodeId;
    }

    @Override
    public JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject jsonObject = super.toJSONObjectImpl();
        jsonObject.put("PLACEMENT", newReplicaPlacement);
        jsonObject.put("COORDINATOR", coordinatorNodeId);
        return jsonObject;
    }
}
