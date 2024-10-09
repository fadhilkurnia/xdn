package edu.umass.cs.causal.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.utils.IntegerPacketTypeMap;

import java.util.HashMap;
import java.util.Map;

public enum CausalPacketType implements IntegerPacketType {

    CAUSAL_PACKET(41300),

    // Entry Replica -> All Replicas
    CAUSAL_WRITE_FORWARD_PACKET(41301),

    // Replicas -> Entry Replica
    CAUSAL_WRITE_ACK_PACKET(41302),

    // Entry Replica -> All Replicas
    CAUSAL_READ_FORWARD_PACKET(41303),

    // Replicas -> Entry Replica
    CAUSAL_READ_ACK_PACKET(41304);

    // Client -> Entry Replica
    // COPS_APP_REQUEST_PACKET(41302);

    private static final Map<Integer, CausalPacketType> numbers = new HashMap<>();

    /* ************** BEGIN static code block to ensure correct initialization *********** */
    static {
        for (CausalPacketType type : CausalPacketType.values()) {
            if (!CausalPacketType.numbers.containsKey(type.number)) {
                CausalPacketType.numbers.put(type.number, type);
            } else {
                assert (false) : "Duplicate or inconsistent enum type";
                throw new RuntimeException(
                        "Duplicate or inconsistent enum type");
            }
        }
    }
    /* *************** END static code block to ensure correct initialization *********** */

    private final int number;

    CausalPacketType(int number) {
        this.number = number;
    }

    @Override
    public int getInt() {
        return this.number;
    }

    public static final IntegerPacketTypeMap<CausalPacketType> intToType =
            new IntegerPacketTypeMap<>(CausalPacketType.values());
}
