package edu.umass.cs.causal.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

import java.nio.ByteBuffer;

public abstract class CausalPacket implements ReplicableRequest {

    public static CausalPacketType getQuickPacketTypeFromEncodedPacket(byte[] encodedPacket) {
        if (encodedPacket.length < 4) return null;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        return CausalPacketType.intToType.get(packetType);
    }

    public static CausalPacket createFromBytes(byte[] encodedPacket,
                                               AppRequestParser appRequestParser) {
        CausalPacketType packetType =
                CausalPacket.getQuickPacketTypeFromEncodedPacket(encodedPacket);
        assert packetType != null : "Invalid encoded CausalPacket";

        if (packetType.equals(CausalPacketType.CAUSAL_WRITE_FORWARD_PACKET)) {
            return CausalWriteForwardPacket.createFromBytes(encodedPacket, appRequestParser);
        }

        if (packetType.equals(CausalPacketType.CAUSAL_WRITE_ACK_PACKET)) {
            return CausalWriteAckPacket.createFromBytes(encodedPacket);
        }

        if (packetType.equals(CausalPacketType.CAUSAL_READ_FORWARD_PACKET)) {
            return CausalReadForwardPacket.createFromBytes(encodedPacket, appRequestParser);
        }

        if (packetType.equals(CausalPacketType.CAUSAL_READ_ACK_PACKET)) {
            return CausalReadAckPacket.createFromBytes(encodedPacket, appRequestParser);
        }

        throw new RuntimeException("Unimplemented deserializer handler for packet type of " +
                packetType);
    }

}
