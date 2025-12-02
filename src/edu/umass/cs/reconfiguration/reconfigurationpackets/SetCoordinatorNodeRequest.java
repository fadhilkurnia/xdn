package edu.umass.cs.reconfiguration.reconfigurationpackets;

import edu.umass.cs.nio.interfaces.Stringifiable;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetSocketAddress;

public class SetCoordinatorNodeRequest<NodeIDType> extends ClientReconfigurationPacket {

    private final NodeIDType initiatorNodeId;
    private final NodeIDType newCoordinatorNodeId;

    private String crpKey;

    public SetCoordinatorNodeRequest(InetSocketAddress initiator,
                                     String name,
                                     NodeIDType initiatorNodeId,
                                     NodeIDType newCoordinatorNodeId) {
        super(initiator, PacketType.SET_COORDINATOR_NODE_REQUEST, name, 0);
        this.initiatorNodeId = initiatorNodeId;
        this.newCoordinatorNodeId = newCoordinatorNodeId;
    }

    public SetCoordinatorNodeRequest(JSONObject json, Stringifiable<NodeIDType> unstringer) throws JSONException {
        super(json, unstringer);
        this.initiatorNodeId = unstringer.valueOf(json.getString("initiatorNodeId"));
        this.newCoordinatorNodeId = unstringer.valueOf(json.getString("newCoordinatorNodeId"));
        this.crpKey = json.getString("crpKey");
    }

    public NodeIDType getInitiatorNodeId() {
        return initiatorNodeId;
    }

    public String getNewCoordinatorNodeId() {
        return newCoordinatorNodeId.toString();
    }

    public String getCrpKey() {
        return crpKey;
    }

    public void setCrpKey(String crpKey) {
        this.crpKey = crpKey;
    }

    @Override
    public JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject jsonObject = super.toJSONObjectImpl();
        jsonObject.put("initiatorNodeId", initiatorNodeId.toString());
        jsonObject.put("newCoordinatorNodeId", newCoordinatorNodeId.toString());
        jsonObject.put("crpKey", crpKey);
        return jsonObject;
    }
}
