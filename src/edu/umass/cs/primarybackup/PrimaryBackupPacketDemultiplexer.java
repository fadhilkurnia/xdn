package edu.umass.cs.primarybackup;

import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.primarybackup.packets.PrimaryBackupPacket;
import edu.umass.cs.primarybackup.packets.PrimaryBackupPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;

import java.io.IOException;

public class PrimaryBackupPacketDemultiplexer extends
        AbstractPacketDemultiplexer<PrimaryBackupPacket> {

    private final PrimaryBackupReplicaCoordinator<?> coordinator;

    public PrimaryBackupPacketDemultiplexer(PrimaryBackupReplicaCoordinator<?> coordinator) {
        assert coordinator != null;
        this.coordinator = coordinator;
        this.register(PrimaryBackupPacketType.values());
    }

    @Override
    protected Integer getPacketType(PrimaryBackupPacket message) {
        return message.getRequestType().getInt();
    }

    @Override
    protected PrimaryBackupPacket processHeader(byte[] message, NIOHeader header) {
        PrimaryBackupPacketType packetType =
                PrimaryBackupPacket.getQuickPacketTypeFromEncodedPacket(message);
        if (packetType == null) {
            return null;
        }
        return PrimaryBackupPacket.createFromBytes(message);
    }

    @Override
    protected boolean matchesType(Object message) {
        return message instanceof PrimaryBackupPacket;
    }

    @Override
    public boolean handleMessage(PrimaryBackupPacket message, NIOHeader header) {
        if (message == null) return false;
        try {
            return this.coordinator.coordinateRequest(message, null);
        } catch (IOException | RequestParseException e) {
            throw new RuntimeException(e);
        }
    }
}
