package edu.umass.cs.primarybackup.packets;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.PrimaryEpoch;
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

public class InitBackupPacket extends PrimaryBackupPacket implements Byteable {

    @Deprecated
    public static final String SERIALIZED_PREFIX = "pb:init:";

    private final long packetId;
    private final String serviceName;

    public InitBackupPacket(String serviceName) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName);
    }

    private InitBackupPacket(long packetId, String serviceName) {
        this.packetId = packetId;
        this.serviceName = serviceName;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_INIT_BACKUP_PACKET;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    @Override
    public long getRequestID() {
        return packetId;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InitBackupPacket that = (InitBackupPacket) o;
        return this.packetId == that.packetId &&
                Objects.equals(serviceName, that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName);
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
        PrimaryBackupPacketProto.InitBackupPacket.Builder protoBuilder =
                PrimaryBackupPacketProto.InitBackupPacket.newBuilder()
                        .setPacketId(this.packetId)
                        .setServiceName(this.serviceName);

        // Serialize the packetType in the header, followed by the protobuf
        output.writeBytes(encodedHeader);
        output.writeBytes(protoBuilder.build().toByteArray());
        return output.toByteArray();
    }

    public static InitBackupPacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length > 0;

        // Decode the packet type
        assert encodedPacket.length >= 4;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        assert packetType == PrimaryBackupPacketType.PB_INIT_BACKUP_PACKET.getInt()
                : "invalid packet header: " + packetType;
        byte[] encoded = encodedPacket;
        encoded = Arrays.copyOfRange(encoded, 4, encoded.length);

        // Decode the bytes into Proto
        PrimaryBackupPacketProto.InitBackupPacket decodedProto = null;
        try {
            decodedProto = PrimaryBackupPacketProto.InitBackupPacket.parseFrom(encoded);
        } catch (InvalidProtocolBufferException e) {
            Logger.getGlobal().log(Level.WARNING,
                    "Invalid protobuf bytes given: " + e.getMessage());
            return null;
        }

        return new InitBackupPacket(
                decodedProto.getPacketId(),
                decodedProto.getServiceName());
    }

    @Deprecated
    public static InitBackupPacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("serviceName");
            return new InitBackupPacket(serviceName);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
}
