package edu.umass.cs.primarybackup.packets;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.proto.PrimaryBackupPacketProto;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ResponsePacket extends PrimaryBackupPacket implements Byteable {

    @Deprecated
    public static final String SERIALIZED_PREFIX = "pb:res:";

    private final String serviceName;
    private final byte[] encodedResponse;
    private final long requestId; // ID of request whose response contained in this packet

    public ResponsePacket(String serviceName, long requestId, byte[] encodedResponse) {
        this.serviceName = serviceName;
        this.encodedResponse = encodedResponse;
        this.requestId = requestId;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_RESPONSE_PACKET;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        // Despite the name of getRequestID, this method intention is to get the packet ID.
        // We don't need to have packet ID for ResponsePacket because it is already
        // idempotent: it is safe for a replica to receive duplicate ResponsePacket.
        return -1;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    public byte[] getEncodedResponse() {
        return encodedResponse;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ResponsePacket that = (ResponsePacket) o;
        return requestId == that.requestId &&
                Objects.equals(serviceName, that.serviceName) &&
                Arrays.equals(encodedResponse, that.encodedResponse);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serviceName, requestId);
        result = 31 * result + Arrays.hashCode(encodedResponse);
        return result;
    }

    @Override
    public String toString() {
        return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
    }

    @Override
    public byte[] toBytes() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Serialize the packet type
        int packetType = this.getRequestType().getInt();
        byte[] encodedHeader = ByteBuffer.allocate(4).putInt(packetType).array();

        // Prepare the packet proto builder
        PrimaryBackupPacketProto.ResponsePacket.Builder protoBuilder =
                PrimaryBackupPacketProto.ResponsePacket.newBuilder()
                        .setServiceName(this.serviceName)
                        .setRequestId(this.requestId)
                        .setEncodedResponse(ByteString.copyFrom(this.encodedResponse));

        // Serialize the packetType in the header, followed by the protobuf
        output.writeBytes(encodedHeader);
        output.writeBytes(protoBuilder.build().toByteArray());
        return output.toByteArray();
    }

    public static ResponsePacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length > 0;

        // Decode the packet type
        assert encodedPacket.length >= 4;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        assert packetType == PrimaryBackupPacketType.PB_RESPONSE_PACKET.getInt()
                : "invalid packet header: " + packetType;
        byte[] encoded = encodedPacket;
        encoded = Arrays.copyOfRange(encoded, 4, encoded.length);

        // Decode the bytes into Proto
        PrimaryBackupPacketProto.ResponsePacket decodedProto = null;
        try {
            decodedProto = PrimaryBackupPacketProto.ResponsePacket.parseFrom(encoded);
        } catch (InvalidProtocolBufferException e) {
            Logger.getGlobal().log(Level.WARNING,
                    "Invalid protobuf bytes given: " + e.getMessage());
            return null;
        }

        return new ResponsePacket(
                decodedProto.getServiceName(),
                decodedProto.getRequestId(),
                decodedProto.getEncodedResponse().toByteArray());
    }

    @Deprecated
    public static ResponsePacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("serviceName");
            String responseStr = json.getString("response");
            long packetID = json.getLong("id");

            return new ResponsePacket(
                    serviceName,
                    packetID,
                    responseStr.getBytes(StandardCharsets.ISO_8859_1)
            );
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

}
