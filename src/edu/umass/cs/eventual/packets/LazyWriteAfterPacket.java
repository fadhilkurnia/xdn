package edu.umass.cs.eventual.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class LazyWriteAfterPacket extends LazyPacket {

    private final long packetId;
    private final String senderId;
    private final ClientRequest clientWriteOnlyRequest;

    public LazyWriteAfterPacket(String senderId, ClientRequest clientWriteOnlyRequest) {
        this(UUID.randomUUID().getLeastSignificantBits(), senderId, clientWriteOnlyRequest);
    }

    private LazyWriteAfterPacket(long packetId, String senderId, ClientRequest clientWriteOnlyRequest) {
        super(LazyPacketType.LAZY_WRITE_AFTER);
        assert packetId != 0 : "A likely invalid packetID given";
        assert senderId != null : "The sender cannot be null";
        assert clientWriteOnlyRequest != null : "The provided ClientRequest cannot be null";
        assert (clientWriteOnlyRequest instanceof BehavioralRequest br && br.isWriteOnlyRequest()) :
                "The provided ClientRequest must be a WriteOnlyRequest";
        this.packetId = packetId;
        this.senderId = senderId;
        this.clientWriteOnlyRequest = clientWriteOnlyRequest;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return LazyPacketType.LAZY_WRITE_AFTER;
    }

    @Override
    public String getServiceName() {
        return this.clientWriteOnlyRequest.getServiceName();
    }

    @Override
    public long getRequestID() {
        return this.packetId;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("id", this.packetId);
        object.put("sid", this.senderId);
        object.put("req", this.clientWriteOnlyRequest.toString());
        return object;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public ClientRequest getClientWriteOnlyRequest() {
        return clientWriteOnlyRequest;
    }

    // please see the comment in ClientCentricWriteAfterPacket
    public static LazyWriteAfterPacket fromJsonObject(JSONObject jsonObject,
                                                      AppRequestParser appRequestParser) {
        assert jsonObject != null : "The provided JSONObject cannot be null";
        assert appRequestParser != null : "The provided appRequestParser can not be null";
        assert jsonObject.has("id") : "Unknown ID from the encoded packet";
        assert jsonObject.has("req") : "Unknown user request from the encoded packet";
        assert jsonObject.has("sid") : "Unknown sender ID from the encoded packet";

        try {
            long requestId = jsonObject.getLong("id");
            String senderId = jsonObject.getString("sid");
            String encodedClientRequest = jsonObject.getString("req");
            Request clientRequest = appRequestParser.getRequest(encodedClientRequest);
            assert (clientRequest instanceof ClientRequest) :
                    "The request inside ClientCentricWriteAfterPacket " +
                            "must implement ClientRequest interface";
            assert (clientRequest instanceof BehavioralRequest br && br.isWriteOnlyRequest()) :
                    "The client request inside ClientCentricWriteAfterPacket " +
                            "must be WriteOnlyRequest";
            return new LazyWriteAfterPacket(requestId, senderId, (ClientRequest) clientRequest);
        } catch (JSONException | RequestParseException e) {
            Logger.getGlobal().log(Level.SEVERE, "Receiving an invalid encoded " +
                    "LazyWriteAfterPacket");
            return null;
        }

    }
}
