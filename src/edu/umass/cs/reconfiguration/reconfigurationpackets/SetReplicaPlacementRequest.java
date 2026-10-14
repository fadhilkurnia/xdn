package edu.umass.cs.reconfiguration.reconfigurationpackets;

import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;
import java.util.Set;

public class SetReplicaPlacementRequest extends ClientReconfigurationPacket {

    private final Set<String> newReplicaPlacement;
    private final String coordinatorNodeId;

    /**
     * Optional replication-mode override for the new epoch: "active" or "primary-backup".
     * Rides the placement metadata so a protocol switch is just an in-place placement update.
     */
    private final String replicationMode;

    public SetReplicaPlacementRequest(InetSocketAddress initiator,
                                      String name,
                                      Set<String> placement) {
        this(initiator, name, placement, null, null);
    }

    public SetReplicaPlacementRequest(InetSocketAddress initiator,
                                      String name,
                                      Set<String> placement,
                                      String coordinatorNodeId) {
        this(initiator, name, placement, coordinatorNodeId, null);
    }

    public SetReplicaPlacementRequest(InetSocketAddress initiator,
                                      String name,
                                      Set<String> placement,
                                      String coordinatorNodeId,
                                      String replicationMode) {
        super(initiator, PacketType.SET_REPLICA_PLACEMENT_REQUEST, name, 0);
        this.newReplicaPlacement = placement;
        this.coordinatorNodeId = coordinatorNodeId;
        this.replicationMode = replicationMode;
    }

    public Set<String> getNewReplicaPlacement() {
        return newReplicaPlacement;
    }

    public String getCoordinatorNodeId() {
        return coordinatorNodeId;
    }

    public String getReplicationMode() {
        return replicationMode;
    }

    @Override
    public JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject jsonObject = super.toJSONObjectImpl();
        jsonObject.put("PLACEMENT", newReplicaPlacement);
        jsonObject.put("COORDINATOR", coordinatorNodeId);
        jsonObject.put("REPLICATION", replicationMode);
        return jsonObject;
    }
}
