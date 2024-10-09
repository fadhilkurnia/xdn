package edu.umass.cs.causal.packets;

import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import org.json.JSONException;
import org.json.JSONObject;

@Deprecated
public class DeprecatedCausalPacket extends JSONPacket implements ReplicableRequest {

    // general fields for all packet
    private final String serviceName;
    private final long requestID;

    // fields for client request packet
    private final boolean isClientRequest;
    private ReplicableClientRequest clientRequest;

    // TODO: fields for put_after packet

    public DeprecatedCausalPacket(ReplicableClientRequest clientRequest) {
        super(CausalPacketType.CAUSAL_PACKET);
        this.clientRequest = clientRequest;
        this.isClientRequest = true;
        this.serviceName = clientRequest.getServiceName();
        this.requestID = clientRequest.getRequestID();
    }

    private DeprecatedCausalPacket(String serviceName) {
        super(CausalPacketType.CAUSAL_PACKET);
        this.isClientRequest = false;
        this.serviceName = serviceName;
        this.requestID = System.currentTimeMillis();
    }

    private DeprecatedCausalPacket(String serviceName, long requestID) {
        super(CausalPacketType.CAUSAL_PACKET);
        this.isClientRequest = false;
        this.serviceName = serviceName;
        this.requestID = requestID;
    }

    public static DeprecatedCausalPacket createPutAfterPacket(String serviceName) {
        return new DeprecatedCausalPacket(serviceName);
    }

    public boolean isClientRequest() {
        return isClientRequest;
    }

    public ReplicableClientRequest getClientRequest() {
        return clientRequest;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("serviceName", this.serviceName);
        object.put("requestID", this.requestID);
        object.put("clientRequest",
                this.clientRequest != null ?
                this.clientRequest.toString() : "");

        return object;
    }

    public static DeprecatedCausalPacket createFromString(String encoded) {
        try {
            JSONObject json = new JSONObject(encoded);
            String serviceName = json.getString("serviceName");
            long requestID = json.getLong("requestID");

            return new DeprecatedCausalPacket(serviceName, requestID);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public IntegerPacketType getRequestType() {
        return CausalPacketType.CAUSAL_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.requestID;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }
}
