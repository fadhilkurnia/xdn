package edu.umass.cs.pram.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public abstract class PramPacket implements ReplicableRequest {

    protected PramPacket() {
    }

    public static PramPacketType getQuickPacketTypeFromEncodedPacket(byte[] encodedPacket) {
        if (encodedPacket == null || encodedPacket.length < Integer.BYTES) {
            return null;
        }
        int packetType = ByteBuffer.wrap(encodedPacket, 0, Integer.BYTES).getInt();
        return PramPacketType.intToType.get(packetType);
    }

    public static PramPacket createFromBytes(byte[] encodedPacket,
                                             AppRequestParser appRequestParser) {
        PramPacketType packetType =
                PramPacket.getQuickPacketTypeFromEncodedPacket(encodedPacket);
        assert packetType != null : "Invalid encoded PramPacket";

        return switch (packetType) {
            case PRAM_READ_PACKET -> PramReadPacket.createFromBytes(encodedPacket, appRequestParser);
            case PRAM_WRITE_PACKET -> PramWritePacket.createFromBytes(encodedPacket, appRequestParser);
            case PRAM_WRITE_AFTER_PACKET ->
                    PramWriteAfterPacket.createFromBytes(encodedPacket, appRequestParser);
            default -> throw new RuntimeException(
                    "Unimplemented deserializer for packet type " + packetType);
        };
    }

    @Override
    public String toString() {
        return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
    }
}
