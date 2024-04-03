package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.nio.interfaces.IntegerPacketType;

import java.util.HashMap;

public enum PrimaryBackupPacketType implements IntegerPacketType {

    PB_START_EPOCH_PACKET(35400);

    private static final HashMap<Integer, PrimaryBackupPacketType> numbers = new HashMap<>();

    /* ************** BEGIN static code block to ensure correct initialization *********** */
    static {
        for (PrimaryBackupPacketType type : PrimaryBackupPacketType.values()) {
            if (!PrimaryBackupPacketType.numbers.containsKey(type.number)) {
                PrimaryBackupPacketType.numbers.put(type.number, type);
            } else {
                assert (false) : "Duplicate or inconsistent enum type";
                throw new RuntimeException(
                        "Duplicate or inconsistent enum type");
            }
        }
    }
    /* *************** END static code block to ensure correct initialization *********** */

    private final int number;

    PrimaryBackupPacketType(int number) {
        this.number = number;
    }

    @Override
    public int getInt() {
        return this.number;
    }
}
