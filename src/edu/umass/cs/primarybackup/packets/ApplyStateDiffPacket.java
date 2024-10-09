package edu.umass.cs.primarybackup.packets;

import com.google.protobuf.ByteString;
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

public class ApplyStateDiffPacket extends PrimaryBackupPacket implements Byteable {

    @Deprecated
    public static final String SERIALIZED_PREFIX = "pb:sd:";

    private final long packetId;
    private final String serviceName;
    private final PrimaryEpoch<?> primaryEpoch;
    private final String stateDiff;

    public ApplyStateDiffPacket(String serviceName,
                                PrimaryEpoch<?> primaryEpoch,
                                String stateDiff) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName,
                primaryEpoch,
                stateDiff);
        assert serviceName != null;
        assert primaryEpoch != null;
        assert stateDiff != null;
    }

    private ApplyStateDiffPacket(long packetId,
                                String serviceName,
                                 PrimaryEpoch<?> primaryEpoch,
                                 String stateDiff) {
        assert packetId > 0;
        assert serviceName != null;
        assert primaryEpoch != null;
        assert stateDiff != null;

        this.packetId = packetId;
        this.serviceName = serviceName;
        this.primaryEpoch = primaryEpoch;
        this.stateDiff = stateDiff;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_STATE_DIFF_PACKET;
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

    public String getPrimaryEpochString() {
        return primaryEpoch.toString();
    }

    public String getStateDiff() {
        return stateDiff;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ApplyStateDiffPacket that = (ApplyStateDiffPacket) o;
        return packetId == that.packetId &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(primaryEpoch, that.primaryEpoch) &&
                Objects.equals(stateDiff, that.stateDiff);
    }

    @Override
    public int hashCode() {
        return Objects.hash(packetId, serviceName, primaryEpoch, stateDiff);
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
        PrimaryBackupPacketProto.ApplyStateDiffPacket.Builder protoBuilder =
                PrimaryBackupPacketProto.ApplyStateDiffPacket.newBuilder()
                        .setPacketId(this.packetId)
                        .setServiceName(this.serviceName)
                        .setPrimaryNodeId(this.primaryEpoch.nodeID)
                        .setPrimaryEpoch(this.primaryEpoch.counter)
                        .setEncodedStateDiff(
                                ByteString.copyFrom(this.stateDiff, StandardCharsets.ISO_8859_1));

        // Serialize the packetType in the header, followed by the protobuf
        output.writeBytes(encodedHeader);
        output.writeBytes(protoBuilder.build().toByteArray());
        return output.toByteArray();
    }

    public static ApplyStateDiffPacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length > 0;

        // Decode the packet type
        assert encodedPacket.length >= 4;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        assert packetType == PrimaryBackupPacketType.PB_STATE_DIFF_PACKET.getInt()
                : "invalid packet header: " + packetType;
        byte[] encoded = encodedPacket;
        encoded = Arrays.copyOfRange(encoded, 4, encoded.length);

        // Decode the bytes into Proto
        PrimaryBackupPacketProto.ApplyStateDiffPacket decodedProto = null;
        try {
            decodedProto = PrimaryBackupPacketProto.ApplyStateDiffPacket.parseFrom(encoded);
        } catch (InvalidProtocolBufferException e) {
            Logger.getGlobal().log(Level.WARNING,
                    "Invalid protobuf bytes given: " + e.getMessage());
            return null;
        }

        return new ApplyStateDiffPacket(
                decodedProto.getPacketId(),
                decodedProto.getServiceName(),
                new PrimaryEpoch<>(
                        decodedProto.getPrimaryNodeId(),
                        decodedProto.getPrimaryEpoch()),
                decodedProto.getEncodedStateDiff().toString(StandardCharsets.ISO_8859_1));
    }

    @Deprecated
    public static ApplyStateDiffPacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("sn");
            String primaryEpochStr = json.getString("ep");
            String stateDiff = json.getString("sd");
            long requestID = json.getLong("id");

            return new ApplyStateDiffPacket(
                    requestID,
                    serviceName,
                    new PrimaryEpoch<String>(primaryEpochStr),
                    stateDiff);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

}
