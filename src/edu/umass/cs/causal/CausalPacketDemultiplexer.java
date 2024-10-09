package edu.umass.cs.causal;

import edu.umass.cs.causal.packets.CausalPacket;
import edu.umass.cs.causal.packets.CausalPacketType;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;

public class CausalPacketDemultiplexer extends AbstractPacketDemultiplexer<CausalPacket> {

    private final CausalReplicaCoordinator<?> coordinator;
    private final AppRequestParser appRequestParser;

    public CausalPacketDemultiplexer(CausalReplicaCoordinator<?> coordinator,
                                     AppRequestParser appRequestParser) {
        this.coordinator = coordinator;
        this.appRequestParser = appRequestParser;
        this.register(CausalPacketType.values());
    }

    @Override
    protected Integer getPacketType(CausalPacket message) {
        return message.getRequestType().getInt();
    }

    @Override
    protected CausalPacket processHeader(byte[] message, NIOHeader header) {
        CausalPacketType packetType = CausalPacket.getQuickPacketTypeFromEncodedPacket(message);
        if (packetType == null) {
            return null;
        }

        return CausalPacket.createFromBytes(message, this.appRequestParser);
    }

    @Override
    protected boolean matchesType(Object message) {
        return message instanceof CausalPacket;
    }

    @Override
    public boolean handleMessage(CausalPacket message, NIOHeader header) {
        if (message == null) return false;
        try {
            return coordinator.coordinateRequest(message, null);
        } catch (IOException | RequestParseException e) {
            throw new RuntimeException(e);
        }
    }
}
