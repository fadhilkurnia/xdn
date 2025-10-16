package edu.umass.cs.eventual.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

import java.nio.ByteBuffer;

public abstract class LazyPacket implements ReplicableRequest {

    // returns null if the encodedPacket is not from a valid LazyPacket
    public static LazyPacketType getQuickPacketTypeFromEncodedPacket(byte[] encodedPacket) {
        if (encodedPacket.length < 4) return null;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
        int packetType = headerBuffer.getInt(0);
        return LazyPacketType.intToType.get(packetType);
    }

    public static LazyPacket createFromBytes(byte[] encodedPacket,
                                             AppRequestParser appRequestParser) {
        LazyPacketType packetType = LazyPacket.getQuickPacketTypeFromEncodedPacket(encodedPacket);
        assert packetType != null : "Invalid encoded LazyPacket";


        if (packetType.equals(LazyPacketType.LAZY_WRITE_AFTER)) {
            return LazyWriteAfterPacket.createFromBytes(encodedPacket, appRequestParser);
        }

        throw new RuntimeException("Unimplemented deserializer handler for packet type of " +
                packetType);
    }
}
