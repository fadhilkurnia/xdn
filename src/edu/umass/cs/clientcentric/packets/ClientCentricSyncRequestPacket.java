package edu.umass.cs.clientcentric.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientCentricSyncRequestPacket extends ClientCentricPacket {

    private final long packetId;
    private final String senderId;
    private final String serviceName;
    private final Long fromSequenceNumber;

    public ClientCentricSyncRequestPacket(String senderId, String serviceName,
                                           Long fromSequenceNumber) {
        this(System.currentTimeMillis(), senderId, serviceName, fromSequenceNumber);
    }

    private ClientCentricSyncRequestPacket(Long packetId, String senderId, String serviceName,
                                           Long fromSequenceNumber) {
        super(ClientCentricPacketType.CLIENT_CENTRIC_SYNC_REQ_PACKET);
        assert packetId != null && packetId != 0 : "Invalid packet id";
        assert senderId != null && !senderId.isEmpty() : "Invalid sender id";
        assert serviceName != null && !serviceName.isEmpty() : "Invalid service name";
        assert fromSequenceNumber != null && fromSequenceNumber >= 0 :
                "Invalid sequence number, must be >= 0";
        this.packetId = packetId;
        this.senderId = senderId;
        this.serviceName = serviceName;
        this.fromSequenceNumber = fromSequenceNumber;
    }


    @Override
    public IntegerPacketType getRequestType() {
        return ClientCentricPacketType.CLIENT_CENTRIC_SYNC_REQ_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.packetId;
    }

    public String getSenderId() {
        return senderId;
    }

    public Long getStartingSequenceNumber() {
        return fromSequenceNumber;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("id", this.packetId);
        object.put("sn", this.serviceName);
        object.put("sid", this.senderId);
        object.put("seq", this.fromSequenceNumber);
        return object;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public static ClientCentricSyncRequestPacket fromJSONObject(JSONObject object) {
        assert object != null : "The provided json object can not be null";
        assert object.has("id") : "Unknown ID from the encoded packet";
        assert object.has("sn") : "Unknown service name from the encoded packet";
        assert object.has("sid") : "Unknown sender ID from the encoded packet";
        assert object.has("seq") : "Unknown Sequence Number from the encoded packet";

        try {
            long packetId = object.getLong("id");
            String serviceName = object.getString("sn");
            String senderId = object.getString("sid");
            long fromSeqNum = object.getLong("seq");
            return new ClientCentricSyncRequestPacket(packetId, senderId, serviceName, fromSeqNum);
        } catch (JSONException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "receiving an invalid encoded client-centric-sync packet");
            return null;
        }
    }
}
