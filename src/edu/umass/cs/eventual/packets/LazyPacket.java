package edu.umass.cs.eventual.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

public abstract class LazyPacket extends JSONPacket implements ReplicableRequest {

    protected LazyPacket(LazyPacketType t) {
        super(t);
    }

    public static LazyPacket createFromString(String encodedPacket,
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
                    "Ignoring LazyPacket as the type is null");
            return null;
        }

        LazyPacketType lazyPacketType = LazyPacketType.intToType.get(packetType);
        assert lazyPacketType != null :
                "Unknown corresponding LazyPacketType with type=" + packetType;
        if (lazyPacketType == LazyPacketType.LAZY_WRITE_AFTER) {
            return LazyWriteAfterPacket.fromJsonObject(object, appRequestParser);
        }

        Logger.getGlobal().log(Level.SEVERE,
                "Handling unknown LazyPacketType " + lazyPacketType);
        return null;
    }
}
