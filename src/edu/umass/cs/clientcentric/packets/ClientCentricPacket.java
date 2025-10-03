package edu.umass.cs.clientcentric.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;

import java.nio.ByteBuffer;

public abstract class ClientCentricPacket implements ReplicableRequest {

    protected ClientCentricPacket() {
    }

    public static ClientCentricPacketType getQuickPacketTypeFromEncodedPacket(byte[] encodedPacket) {
        if (encodedPacket == null || encodedPacket.length < Integer.BYTES) {
            return null;
        }
        int packetType = ByteBuffer.wrap(encodedPacket, 0, Integer.BYTES).getInt();
        return ClientCentricPacketType.intToType.get(packetType);
    }

    public static ClientCentricPacket createFromBytes(byte[] encodedPacket,
                                                      AppRequestParser appRequestParser) {
        ClientCentricPacketType packetType =
                ClientCentricPacket.getQuickPacketTypeFromEncodedPacket(encodedPacket);
        assert packetType != null : "Invalid encoded ClientCentricPacket";

        return switch (packetType) {
            case CLIENT_CENTRIC_WRITE_AFTER_PACKET ->
                    ClientCentricWriteAfterPacket.createFromBytes(encodedPacket, appRequestParser);
            case CLIENT_CENTRIC_SYNC_REQ_PACKET ->
                    ClientCentricSyncRequestPacket.createFromBytes(encodedPacket);
            case CLIENT_CENTRIC_SYNC_RES_PACKET ->
                    ClientCentricSyncResponsePacket.createFromBytes(encodedPacket);
            default -> throw new RuntimeException(
                    "Unimplemented deserializer for packet type " + packetType);
        };
    }
}
