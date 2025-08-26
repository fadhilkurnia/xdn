package edu.umass.cs.clientcentric.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.utils.IntegerPacketTypeMap;

import java.util.HashMap;
import java.util.Map;

public enum ClientCentricPacketType implements IntegerPacketType {

    // Wrapper packet for all ClientCentric packets
    CLIENT_CENTRIC_PACKET(41500),

    // Entry Replica -> All Replicas
    CLIENT_CENTRIC_WRITE_AFTER_PACKET(41501),

    // Entry Replica -> A more up-to-date replica
    CLIENT_CENTRIC_SYNC_REQ_PACKET(41502),

    // A more up-to-date replica -> Entry Replica that sends SYNC_REQ
    CLIENT_CENTRIC_SYNC_RES_PACKET(41503);

    private static final Map<Integer, ClientCentricPacketType> numbers = new HashMap<>();

    /* ************** BEGIN static code block to ensure correct initialization *********** */
    static {
        for (ClientCentricPacketType type : ClientCentricPacketType.values()) {
            if (!ClientCentricPacketType.numbers.containsKey(type.number)) {
                ClientCentricPacketType.numbers.put(type.number, type);
            } else {
                assert (false) : "Duplicate or inconsistent enum type";
                throw new RuntimeException(
                        "Duplicate or inconsistent enum type");
            }
        }
    }
    /* *************** END static code block to ensure correct initialization *********** */

    private final int number;

    ClientCentricPacketType(int number) {
        this.number = number;
    }

    @Override
    public int getInt() {
        return this.number;
    }

    public static final IntegerPacketTypeMap<ClientCentricPacketType> intToType =
            new IntegerPacketTypeMap<>(ClientCentricPacketType.values());
}
