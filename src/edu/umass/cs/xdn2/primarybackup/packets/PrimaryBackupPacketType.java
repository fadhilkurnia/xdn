package edu.umass.cs.xdn2.primarybackup.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.utils.IntegerPacketTypeMap;

import java.util.HashMap;
import java.util.Map;

/**
 * PrimaryBackupPacketType enumerates the packet types used by the xdn2
 * primary-backup protocol. Integer range is deliberately disjoint from the
 * old edu.umass.cs.primarybackup.packets.PrimaryBackupPacketType range
 * (35400-35406) to avoid any accidental collision if both packages are ever
 * loaded in the same JVM.
 *
 * TODO: confirm this integer range does not collide with any other packet
 *  type registry in the system before this is used in a real deployment.
 */
public enum PrimaryBackupPacketType implements IntegerPacketType {

    // Primary -> All Replicas (proposed via Paxos)
    PB2_START_EPOCH_PACKET(35500),

    // Primary -> All Replicas (proposed via Paxos)
    PB2_APPLY_STATE_DIFF_PACKET(35501),

    PB2_FORWARDED_REQUEST_PACKET(35502),
    PB2_RESPONSE_PACKET(35503);

    private static final Map<Integer, PrimaryBackupPacketType> numbers = new HashMap<>();

    static {
        for (PrimaryBackupPacketType type : PrimaryBackupPacketType.values()) {
            if (!PrimaryBackupPacketType.numbers.containsKey(type.number)) {
                PrimaryBackupPacketType.numbers.put(type.number, type);
            } else {
                assert (false) : "Duplicate or inconsistent enum type";
                throw new RuntimeException("Duplicate or inconsistent enum type");
            }
        }
    }

    private final int number;

    PrimaryBackupPacketType(int number) {
        this.number = number;
    }

    @Override
    public int getInt() {
        return this.number;
    }

    public static final IntegerPacketTypeMap<PrimaryBackupPacketType> intToType =
            new IntegerPacketTypeMap<>(PrimaryBackupPacketType.values());
}