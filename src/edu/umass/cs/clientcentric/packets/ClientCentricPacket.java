package edu.umass.cs.clientcentric.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.logging.Level;
import java.util.logging.Logger;

// TODO: use Protobuf serialization format.

public abstract class ClientCentricPacket extends JSONPacket implements ReplicableRequest {

    protected ClientCentricPacket(IntegerPacketType t) {
        super(t);
    }

    /**
     * Returns null if the encodedPacket is incorrect.
     */
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
                    "Ignoring ClientCentricPacket as the type is null");
            return null;
        }

        ClientCentricPacketType clientCentricPacketType =
                ClientCentricPacketType.intToType.get(packetType);
        assert clientCentricPacketType != null :
                "Unknown corresponding ClientCentricPacketType with type=" + packetType;
        switch (clientCentricPacketType) {
            case CLIENT_CENTRIC_WRITE_AFTER_PACKET -> {
                return ClientCentricWriteAfterPacket.fromJsonObject(object, appRequestParser);
            }
            case CLIENT_CENTRIC_SYNC_REQ_PACKET -> {
                return ClientCentricSyncRequestPacket.fromJSONObject(object);
            }
            case CLIENT_CENTRIC_SYNC_RES_PACKET -> {
                return ClientCentricSyncResponsePacket.fromJSONObject(object);
            }
        }

        Logger.getGlobal().log(Level.SEVERE,
                "Handling unknown ClientCentricPacketType " + clientCentricPacketType);
        return null;
    }
}
