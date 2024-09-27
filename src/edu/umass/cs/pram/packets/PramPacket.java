package edu.umass.cs.pram.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class PramPacket extends JSONPacket implements ReplicableRequest {

    protected PramPacket(IntegerPacketType t) {
        super(t);
    }

    public static Request createFromString(String encodedPacket,
                                           AppRequestParser appRequestParser) {
        JSONObject object;
        Integer packetType;
        try {
            object = new JSONObject(encodedPacket);
            packetType = JSONPacket.getPacketType(object);
        } catch (JSONException e) {
            return null;
        }

        if (packetType == null) {
            Logger.getGlobal().log(Level.WARNING,
                    "Ignoring PramPacket as the type is null");
            return null;
        }

        PramPacketType pramPacketType = PramPacketType.intToType.get(packetType);
        assert pramPacketType != null :
                "Unknown corresponding PramPacketType with type=" + packetType;
        switch (pramPacketType) {
            case PramPacketType.PRAM_READ_PACKET -> {
                return PramReadPacket.fromJsonObject(object, appRequestParser);
            }
            case PramPacketType.PRAM_WRITE_PACKET -> {
                throw new RuntimeException("unimplemented :(");
            }
            case PramPacketType.PRAM_WRITE_AFTER_PACKET -> {
                return PramWriteAfterPacket.fromJsonObject(object, appRequestParser);
            }
            default -> {
                Logger.getGlobal().log(Level.SEVERE,
                        "Handling unknown PramPacketType " + pramPacketType);
            }
        }

        return null;
    }
}
