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

public class StartEpochPacket extends PrimaryBackupPacket implements Byteable {

    @Deprecated
    public static final String SERIALIZED_PREFIX = "pb:se:";

    private final long packetId;
    private final String serviceName;
    private final PrimaryEpoch<?> startingEpoch;

    public StartEpochPacket(String serviceName, PrimaryEpoch<?> epoch) {
        this(Math.abs(UUID.randomUUID().getLeastSignificantBits()),
                serviceName,
                epoch);
    }

    private StartEpochPacket(long packetId, String serviceName, PrimaryEpoch<?> epoch) {
        this.packetId = packetId;
        this.serviceName = serviceName;
        this.startingEpoch = epoch;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return PrimaryBackupPacketType.PB_START_EPOCH_PACKET;
    }

    @Override
    public String getServiceName() {
        return serviceName;
    }

    public String getStartingEpochString() {
        return startingEpoch.toString();
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
        StartEpochPacket that = (StartEpochPacket) o;
        return  this.packetId == that.packetId &&
                Objects.equals(serviceName, that.serviceName) &&
                Objects.equals(startingEpoch.counter, that.startingEpoch.counter) &&
                Objects.equals(startingEpoch.nodeID, that.startingEpoch.nodeID);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, startingEpoch.counter, startingEpoch.nodeID);
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
        PrimaryBackupPacketProto.StartEpochPacket.Builder protoBuilder =
                PrimaryBackupPacketProto.StartEpochPacket.newBuilder()
                        .setPacketId(this.packetId)
                        .setServiceName(this.serviceName)
                        .setPrimaryNodeId(this.startingEpoch.nodeID)
                        .setPrimaryEpoch(this.startingEpoch.counter);

        // Serialize the packetType in the header, followed by the protobuf
        output.writeBytes(encodedHeader);
        output.writeBytes(protoBuilder.build().toByteArray());
        return output.toByteArray();
    }

    public static StartEpochPacket createFromBytes(byte[] encodedPacket) {
        assert encodedPacket != null && encodedPacket.length > 0;

        // Decode the packet type
        assert encodedPacket.length >= 4;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        assert packetType == PrimaryBackupPacketType.PB_START_EPOCH_PACKET.getInt()
                : "invalid packet header: " + packetType;
        byte[] encoded = encodedPacket;
        encoded = Arrays.copyOfRange(encoded, 4, encoded.length);

        // Decode the bytes into Proto
        PrimaryBackupPacketProto.StartEpochPacket decodedProto = null;
        try {
            decodedProto = PrimaryBackupPacketProto.StartEpochPacket.parseFrom(encoded);
        } catch (InvalidProtocolBufferException e) {
            Logger.getGlobal().log(Level.WARNING,
                    "Invalid protobuf bytes given: " + e.getMessage());
            return null;
        }

        return new StartEpochPacket(
                decodedProto.getPacketId(), 
                decodedProto.getServiceName(),
                new PrimaryEpoch<>(
                        decodedProto.getPrimaryNodeId(),
                        decodedProto.getPrimaryEpoch()));
    }

    @Deprecated
    public static StartEpochPacket createFromString(String encodedPacket) {
        assert encodedPacket != null;
        assert !encodedPacket.isEmpty();
        assert encodedPacket.startsWith(SERIALIZED_PREFIX);

        encodedPacket = encodedPacket.substring(SERIALIZED_PREFIX.length());

        try {
            JSONObject json = new JSONObject(encodedPacket);
            String serviceName = json.getString("serviceName");
            String epochNode = json.getString("epochNode");
            int epochCounter = json.getInt("epochCounter");

            return new StartEpochPacket(serviceName, new PrimaryEpoch(epochNode, epochCounter));
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

}
