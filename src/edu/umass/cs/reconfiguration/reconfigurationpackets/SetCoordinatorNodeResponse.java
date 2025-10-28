package edu.umass.cs.reconfiguration.reconfigurationpackets;

import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

public class SetCoordinatorNodeResponse<NodeIDType>
        extends BasicReconfigurationPacket<NodeIDType> implements ReplicableRequest {

    private final String requestId; // must match with the request
    private final Integer httpErrorCode;
    private final String responseMessage;

    public SetCoordinatorNodeResponse(String requestId,
                                      NodeIDType initiator,
                                      String name,
                                      int epochNumber,
                                      Integer httpErrorCode,
                                      String responseMessage) {
        super(initiator, PacketType.SET_COORDINATOR_NODE_RESPONSE, name, epochNumber);
        this.requestId = requestId;
        this.httpErrorCode = httpErrorCode;
        this.responseMessage = responseMessage;
        this.setKey(requestId);
    }

    @Override
    public long getRequestID() {
        return requestId.hashCode();
    }

    @Override
    public boolean needsCoordination() {
        return false;
    }

    @Override
    public JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject jsonObject = super.toJSONObjectImpl();
        jsonObject.put("requestId", this.requestId);
        jsonObject.put("httpErrorCode", this.httpErrorCode == null
                ? 200 : this.httpErrorCode);
        jsonObject.put("responseMessage", this.responseMessage);
        return jsonObject;
    }

    public SetCoordinatorNodeResponse(JSONObject json, Stringifiable<NodeIDType> unstringer)
            throws JSONException {
        super(json, unstringer);
        this.requestId = json.getString("requestId");
        this.httpErrorCode = json.getInt("httpErrorCode");
        this.responseMessage = json.getString("responseMessage");
    }

    public Integer getHttpErrorCode() {
        return httpErrorCode;
    }

    public String getResponseMessage() {
        return responseMessage;
    }
}
