package edu.umass.cs.eventual;

import edu.umass.cs.eventual.packets.LazyPacket;
import edu.umass.cs.eventual.packets.LazyPacketType;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;

public class LazyPacketDemultiplexer extends AbstractPacketDemultiplexer<LazyPacket> {

    private final LazyReplicaCoordinator<?> coordinator;
    private final AppRequestParser appRequestParser;

    public LazyPacketDemultiplexer(LazyReplicaCoordinator<?> coordinator,
                                   AppRequestParser appRequestParser) {
        this.coordinator = coordinator;
        this.appRequestParser = appRequestParser;
        this.register(LazyPacketType.values());
    }

    @Override
    protected Integer getPacketType(LazyPacket message) {
        return message.getRequestType().getInt();
    }

    @Override
    protected LazyPacket processHeader(byte[] message, NIOHeader header) {
        LazyPacketType packetType = LazyPacket.getQuickPacketTypeFromEncodedPacket(message);
        if (packetType == null) {
            return null;
        }

        return LazyPacket.createFromBytes(message, this.appRequestParser);
    }

    @Override
    protected boolean matchesType(Object message) {
        return message instanceof LazyPacket;
    }

    @Override
    public boolean handleMessage(LazyPacket message, NIOHeader header) {
        if (message == null) return false;
        try {
            return coordinator.coordinateRequest(message, null);
        } catch (IOException | RequestParseException e) {
            throw new RuntimeException(e);
        }
    }
}
