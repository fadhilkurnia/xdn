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
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ForwardedRequestPacket extends PrimaryBackupPacket implements Byteable {

    @Deprecated
    public static final String SERIALIZED_PREFIX = "pb:fwd:";

    private final long packetId;
    private final String serviceName;
    private final String entryNodeId;
    private final byte[] encodedForwardedRequest;

    public ForwardedRequestPacket(String serviceName, String entryNodeId,
                                  byte[] encodedForwardedRequest) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName,
                entryNodeId,
                encodedForwardedRequest);
    }

    private ForwardedRequestPacket(long packetId, String serviceName, String entryNodeId, byte[] encodedForwardedRequest) {
        this.serviceName = serviceName;
        this.entryNodeId = entryNodeId;
        this.encodedForwardedRequest = encodedForwardedRequest;
        this.packetId = packetId;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_FORWARDED_REQUEST_PACKET;
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
    public boolean needsCoordination() {
        return true;
    }

    public byte[] getEncodedForwardedRequest() {
        return encodedForwardedRequest;
    }

    public String getEntryNodeId() {
        return entryNodeId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ForwardedRequestPacket that = (ForwardedRequestPacket) o;
        return packetId == that.packetId &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(entryNodeId, that.entryNodeId) &&
                Arrays.equals(encodedForwardedRequest, that.encodedForwardedRequest);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(serviceName, packetId);
        result = 31 * result + Arrays.hashCode(encodedForwardedRequest);
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
        PrimaryBackupPacketProto.ForwardedRequestPacket.Builder protoBuilder =
                PrimaryBackupPacketProto.ForwardedRequestPacket.newBuilder()
                        .setPacketId(this.packetId)
                        .setServiceName(this.serviceName)
                        .setSenderNodeId(this.entryNodeId)
                        .setEncodedRequest(ByteString.copyFrom(this.encodedForwardedRequest));

        // Serialize the packetType in the header, followed by the protobuf
        output.writeBytes(encodedHeader);
        output.writeBytes(protoBuilder.build().toByteArray());
        return output.toByteArray();
    }

    public static ForwardedRequestPacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length > 0;

        // Decode the packet type
        assert encodedPacket.length >= 4;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        assert packetType == PrimaryBackupPacketType.PB_FORWARDED_REQUEST_PACKET.getInt()
                : "invalid packet header: " + packetType;
        byte[] encoded = encodedPacket;
        encoded = Arrays.copyOfRange(encoded, 4, encoded.length);

        // Decode the bytes into Proto
        PrimaryBackupPacketProto.ForwardedRequestPacket decodedProto = null;
        try {
            decodedProto = PrimaryBackupPacketProto.ForwardedRequestPacket.parseFrom(encoded);
        } catch (InvalidProtocolBufferException e) {
            Logger.getGlobal().log(Level.WARNING,
                    "Invalid protobuf bytes given: " + e.getMessage());
            return null;
        }

        return new ForwardedRequestPacket(
                decodedProto.getPacketId(),
                decodedProto.getServiceName(),
                decodedProto.getSenderNodeId(),
                decodedProto.getEncodedRequest().toByteArray());
    }

    @Deprecated
    public static ForwardedRequestPacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            long packetId = json.getLong("id");
            String serviceName = json.getString("serviceName");
            String entryNodeID = json.getString("entryID");
            String forwardedReqStr = json.getString("forwardedRequest");

            return new ForwardedRequestPacket(
                    packetId,
                    serviceName,
                    entryNodeID,
                    forwardedReqStr.getBytes(StandardCharsets.ISO_8859_1)
            );
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

}
