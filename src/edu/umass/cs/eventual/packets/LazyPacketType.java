package edu.umass.cs.eventual.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.utils.IntegerPacketTypeMap;

import java.util.HashMap;
import java.util.Map;

public enum LazyPacketType implements IntegerPacketType {

    // Wrapper packet for all packet types for Lazy Replication
    LAZY_PACKET(55500),

    LAZY_WRITE_AFTER(55501),

    // Periodic anti-entropy heartbeat carrying the sender's vector clock and state digest.
    LAZY_SYNC(55502),

    // Frontier-to-laggard checkpoint transfer (inline bytes or out-of-band handle).
    LAZY_CHECKPOINT(55503);

    private static final Map<Integer, LazyPacketType> numbers = new HashMap<>();

    /* ************** BEGIN static code block to ensure correct initialization *********** */
    static {
        for (LazyPacketType type : LazyPacketType.values()) {
            if (!LazyPacketType.numbers.containsKey(type.number)) {
                LazyPacketType.numbers.put(type.number, type);
            } else {
                assert (false) : "Duplicate or inconsistent enum type";
                throw new RuntimeException(
                        "Duplicate or inconsistent enum type");
            }
        }
    }
    /* *************** END static code block to ensure correct initialization *********** */

    private final int number;

    LazyPacketType(int number) {
        this.number = number;
    }

    @Override
    public int getInt() {
        return this.number;
    }

    public static final IntegerPacketTypeMap<LazyPacketType> intToType =
            new IntegerPacketTypeMap<>(LazyPacketType.values());
}
