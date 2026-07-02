package edu.umass.cs.xdn2.primarybackup.packets;

import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

import java.nio.ByteBuffer;

/**
 * PrimaryBackupPacket is the abstract base class for all packets used by the
 * xdn2 primary-backup protocol. This is a from-scratch implementation,
 * independent of edu.umass.cs.primarybackup.packets.PrimaryBackupPacket,
 * per the decision to keep the new design fully self-contained.
 *
 * Encoding is plain Java (DataOutputStream/ByteBuffer based via Byteable),
 * not protobuf, for now.
 */
public abstract class PrimaryBackupPacket implements ReplicableRequest {

    /**
     * Reads the first 4 bytes of an encoded packet to determine its type,
     * without fully deserializing it. Returns null if the bytes don't look
     * like a known PrimaryBackupPacketType.
     *
     * @param encodedPacket the raw encoded packet bytes.
     * @return the packet type, or null if unrecognized.
     */
    public static PrimaryBackupPacketType getQuickPacketTypeFromEncodedPacket(
            byte[] encodedPacket) {
        if (encodedPacket == null || encodedPacket.length < 4) return null;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        return PrimaryBackupPacketType.intToType.get(packetType);
    }

    /**
     * Dispatches to the correct concrete packet's createFromBytes() based on
     * the type header.
     *
     * @param encodedPacket the raw encoded packet bytes.
     * @return the deserialized packet.
     * @throws RuntimeException if the packet type is unrecognized or
     *                          unimplemented.
     */
    public static PrimaryBackupPacket createFromBytes(byte[] encodedPacket) {
        PrimaryBackupPacketType packetType =
                PrimaryBackupPacket.getQuickPacketTypeFromEncodedPacket(encodedPacket);
        assert packetType != null : "Invalid encoded PrimaryBackupPacket";

        // TODO: add cases here as more packet types are implemented
        //  (e.g. ChangePrimaryPacket, ForwardedRequestPacket, ResponsePacket
        //  equivalents) once the steady-state write-forwarding design is
        //  finalized.
        if (packetType.equals(PrimaryBackupPacketType.PB2_START_EPOCH_PACKET)) {
            return StartEpochPacket.createFromBytes(encodedPacket);
        }

        if (packetType.equals(PrimaryBackupPacketType.PB2_APPLY_STATE_DIFF_PACKET)) {
            return ApplyStateDiffPacket.createFromBytes(encodedPacket);
        }

        if (packetType.equals(PrimaryBackupPacketType.PB2_FORWARDED_REQUEST_PACKET)) {
            return ForwardedRequestPacket.createFromBytes(encodedPacket);
        }

        if (packetType.equals(PrimaryBackupPacketType.PB2_RESPONSE_PACKET)) {
            return ResponsePacket.createFromBytes(encodedPacket);
        }

        throw new RuntimeException(
                "Unimplemented deserializer handler for packet type of " + packetType);
    }
}