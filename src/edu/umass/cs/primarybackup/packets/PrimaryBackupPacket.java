package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class PrimaryBackupPacket implements ReplicableRequest {

    public static final String SERIALIZED_PREFIX = "pb:";

    // Returns null if the encodedPacket is not a PrimaryBackupPacket
    public static PrimaryBackupPacketType getQuickPacketTypeFromEncodedPacket(
            byte[] encodedPacket) {
        if (encodedPacket.length < 4) return null;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        return PrimaryBackupPacketType.intToType.get(packetType);
    }

    public static PrimaryBackupPacketType getQuickPacketTypeFromEncodedPacket(
            String encodedPacket) {
        return PrimaryBackupPacket.getQuickPacketTypeFromEncodedPacket(
                encodedPacket.getBytes(StandardCharsets.ISO_8859_1));
    }

    public static PrimaryBackupPacket createFromBytes(byte[] encodedPacket) {
        PrimaryBackupPacketType packetType =
                PrimaryBackupPacket.getQuickPacketTypeFromEncodedPacket(encodedPacket);
        assert packetType != null : "Invalid encoded PrimaryBackupPacket";

        if (packetType.equals(PrimaryBackupPacketType.PB_START_EPOCH_PACKET)) {
            return StartEpochPacket.createFromBytes(encodedPacket);
        }

        if (packetType.equals(PrimaryBackupPacketType.PB_FORWARDED_REQUEST_PACKET)) {
            return ForwardedRequestPacket.createFromBytes(encodedPacket);
        }

        if (packetType.equals(PrimaryBackupPacketType.PB_RESPONSE_PACKET)) {
            return ResponsePacket.createFromBytes(encodedPacket);
        }

        if (packetType.equals(PrimaryBackupPacketType.PB_STATE_DIFF_PACKET)) {
            return ApplyStateDiffPacket.createFromBytes(encodedPacket);
        }

        if (packetType.equals(PrimaryBackupPacketType.PB_CHANGE_PRIMARY_PACKET)) {
            throw new RuntimeException("Unimplemented :(");
        }

        throw new RuntimeException(
                "Unimplemented deserializer handler for packet type of " + packetType);
    }

    public static PrimaryBackupPacket createFromString(String encodedPacket) {
        return PrimaryBackupPacket.createFromBytes(
                encodedPacket.getBytes(StandardCharsets.ISO_8859_1));
    }


}
