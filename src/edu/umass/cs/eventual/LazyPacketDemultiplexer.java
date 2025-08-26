package edu.umass.cs.eventual;

import edu.umass.cs.clientcentric.packets.ClientCentricPacket;
import edu.umass.cs.clientcentric.packets.ClientCentricPacketType;
import edu.umass.cs.eventual.packets.LazyPacket;
import edu.umass.cs.eventual.packets.LazyPacketType;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

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

        if (!LazyPacketType.intToType.containsKey(packetType)) {
            return null;
        }

        return LazyPacket.createFromString(rawMessage, this.appRequestParser);
    }

    @Override
    protected boolean matchesType(Object message) {
        return message instanceof LazyPacketType;
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
