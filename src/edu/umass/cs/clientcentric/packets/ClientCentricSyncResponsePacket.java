package edu.umass.cs.clientcentric.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ClientCentricSyncResponsePacket extends ClientCentricPacket {

    private final long packetId;
    private final String senderId;
    private final String serviceName;
    private final long fromSequenceNumber;
    private final List<byte[]> encodedRequests;

    public ClientCentricSyncResponsePacket(String senderId, String serviceName,
                                           long fromSequenceNumber,
                                           List<byte[]> encodedRequests) {
        this(System.currentTimeMillis(), senderId, serviceName,
                fromSequenceNumber, encodedRequests);
    }

    private ClientCentricSyncResponsePacket(long packetId, String senderId, String serviceName,
                                            long fromSequenceNumber,
                                            List<byte[]> encodedRequests) {
        super(ClientCentricPacketType.CLIENT_CENTRIC_SYNC_RES_PACKET);
        assert senderId != null && !senderId.isEmpty() : "Invalid sender id";
        assert serviceName != null && !serviceName.isEmpty() : "Invalid service name";
        assert fromSequenceNumber >= 0 : "Invalid sequence number, must be >= 0";
        this.packetId = packetId;
        this.senderId = senderId;
        this.serviceName = serviceName;
        this.fromSequenceNumber = fromSequenceNumber;
        this.encodedRequests = encodedRequests;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return ClientCentricPacketType.CLIENT_CENTRIC_SYNC_RES_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.packetId;
    }

    @Override
    protected JSONObject toJSONObjectImpl() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("id", this.packetId);
        object.put("sn", this.serviceName);
        object.put("sid", this.senderId);
        object.put("seq", this.fromSequenceNumber);
        JSONArray requestsJsonArr = new JSONArray();
        // FIXME: these are inefficient because we are converting byte[] to String,
        //  which later will be serialized and converted again into byte[].
        for (byte[] req : this.encodedRequests) {
            requestsJsonArr.put(new String(req, StandardCharsets.ISO_8859_1));
        }
        object.put("req", requestsJsonArr);
        return object;
    }

    public String getSenderId() {
        return senderId;
    }

    public long getStartingSequenceNumber() {
        return fromSequenceNumber;
    }

    public List<byte[]> getEncodedRequests() {
        return encodedRequests;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public static ClientCentricSyncResponsePacket fromJSONObject(JSONObject object) {
        assert object != null : "The provided json object can not be null";
        assert object.has("req") : "Unknown requests from the encoded packet";
        assert object.has("id") : "Unknown ID from the encoded packet";
        assert object.has("sn") : "Unknown service name from the encoded packet";
        assert object.has("sid") : "Unknown sender ID from the encoded packet";
        assert object.has("seq") : "Unknown Sequence Number from the encoded packet";

        try {
            long packetId = object.getLong("id");
            String serviceName = object.getString("sn");
            String senderId = object.getString("sid");
            long fromSeqNum = object.getLong("seq");
            JSONArray requestJsonArr = object.getJSONArray("req");
            List<byte[]> encodedRequests = new ArrayList<>();
            for (int i = 0; i < requestJsonArr.length(); ++i) {
                String raw = requestJsonArr.getString(i);
                encodedRequests.add(raw.getBytes(StandardCharsets.ISO_8859_1));
            }
            return new ClientCentricSyncResponsePacket(packetId, serviceName, senderId,
                    fromSeqNum, encodedRequests);
        } catch (JSONException e) {
            Logger.getGlobal().log(Level.SEVERE,
                    "receiving an invalid encoded client-centric-sync-resp packet");
            return null;
        }
    }
}
