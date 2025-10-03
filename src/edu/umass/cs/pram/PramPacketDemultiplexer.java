package edu.umass.cs.pram;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.pram.packets.PramPacket;
import edu.umass.cs.pram.packets.PramPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;

/**
 * PramPacketDemultiplexer requires AppRequestParser because the WriteAfterPacket
 * must encapsulate ClientRequest.
 * <p>
 * Deserialization order for each Demultiplexer:
 * 1. processHeader(messageBytes, header)  // given bytes, quickly return null or Packet
 * 2. getPacketType(message)
 * 3. matchesType(message)
 * 4. handleMessage(message)
 * Therefore,
 */
public class PramPacketDemultiplexer extends AbstractPacketDemultiplexer<PramPacket> {

    private final PramReplicaCoordinator<?> coordinator;
    private final AppRequestParser appRequestDeserializer;

    public PramPacketDemultiplexer(PramReplicaCoordinator<?> coordinator,
                                   AppRequestParser appRequestDeserializer) {
        this.coordinator = coordinator;
        this.appRequestDeserializer = appRequestDeserializer;

        this.register(PramPacketType.values());
    }

    @Override
    protected PramPacket processHeader(byte[] message, NIOHeader header) {
        PramPacketType packetType =
                PramPacket.getQuickPacketTypeFromEncodedPacket(message);
        if (packetType == null) {
            return null;
        }

        return PramPacket.createFromBytes(message, appRequestDeserializer);
    }

    @Override
    protected Integer getPacketType(PramPacket message) {
        return message.getRequestType().getInt();
    }

    @Override
    protected boolean matchesType(Object message) {
        return message instanceof PramPacket;
    }

    @Override
    public boolean handleMessage(PramPacket message, NIOHeader header) {
        if (message == null) return false;
        try {
            return coordinator.coordinateRequest(message, null);
        } catch (IOException | RequestParseException e) {
            throw new RuntimeException(e);
        }
    }
}
