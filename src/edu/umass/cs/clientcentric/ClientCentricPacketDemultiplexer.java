package edu.umass.cs.clientcentric;

import edu.umass.cs.clientcentric.packets.ClientCentricPacket;
import edu.umass.cs.clientcentric.packets.ClientCentricPacketType;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

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
        String rawMessage;

        // convert bytes message into String
        try {
            rawMessage = new String(message, NIOHeader.CHARSET);
        } catch (UnsupportedEncodingException e) {
            return null;
        }

        // find the packet type from the message
        JSONObject object;
        Integer packetType;
        try {
            object = new JSONObject(rawMessage);
            packetType = JSONPacket.getPacketType(object);
        } catch (JSONException e) {
            return null;
        }

        if (!ClientCentricPacketType.intToType.containsKey(packetType)) {
            return null;
        }

        return (ClientCentricPacket) ClientCentricPacket
                .createFromString(rawMessage, this.appRequestParser);
    }

    @Override
    protected boolean matchesType(Object message) {
        return message instanceof ClientCentricPacketType;
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
