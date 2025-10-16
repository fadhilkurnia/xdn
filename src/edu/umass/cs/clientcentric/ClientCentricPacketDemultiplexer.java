package edu.umass.cs.clientcentric;

import edu.umass.cs.clientcentric.packets.ClientCentricPacket;
import edu.umass.cs.clientcentric.packets.ClientCentricPacketType;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;

public class ClientCentricPacketDemultiplexer
        extends AbstractPacketDemultiplexer<ClientCentricPacket> {

    private final BayouReplicaCoordinator<?> coordinator;
    private final AppRequestParser appRequestParser;

    public ClientCentricPacketDemultiplexer(BayouReplicaCoordinator<?> coordinator,
                                            AppRequestParser appRequestParser) {
        this.coordinator = coordinator;
        this.appRequestParser = appRequestParser;
        this.register(ClientCentricPacketType.values());
    }

    @Override
    protected Integer getPacketType(ClientCentricPacket message) {
        return message.getRequestType().getInt();
    }

    @Override
    protected ClientCentricPacket processHeader(byte[] message, NIOHeader header) {
        ClientCentricPacketType packetType =
                ClientCentricPacket.getQuickPacketTypeFromEncodedPacket(message);
        if (packetType == null) {
            return null;
        }

        return ClientCentricPacket.createFromBytes(message, this.appRequestParser);
    }

    @Override
    protected boolean matchesType(Object message) {
        return message instanceof ClientCentricPacket;
    }

    @Override
    public boolean handleMessage(ClientCentricPacket message, NIOHeader header) {
        if (message == null) return false;
        try {
            return coordinator.coordinateRequest(message, null);
        } catch (IOException | RequestParseException e) {
            throw new RuntimeException(e);
        }
    }
}
