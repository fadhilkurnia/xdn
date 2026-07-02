package edu.umass.cs.xdn2.primarybackup;

import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn2.primarybackup.packets.PrimaryBackupPacket;
import edu.umass.cs.xdn2.primarybackup.packets.PrimaryBackupPacketType;

import java.io.IOException;

/**
 * PrimaryBackupPacketDemultiplexer intercepts direct node-to-node
 * PrimaryBackupPackets at the NIO layer (ForwardedRequestPacket,
 * ResponsePacket) and routes them to the coordinator's coordinateRequest().
 *
 * StartEpochPacket and ApplyStateDiffPacket travel via Paxos and are
 * delivered through XdnApp.execute() — they do NOT go through this
 * demultiplexer.
 */
public class PrimaryBackupPacketDemultiplexer
        extends AbstractPacketDemultiplexer<PrimaryBackupPacket> {

    private final PrimaryBackupCoordinator<?> coordinator;

    public PrimaryBackupPacketDemultiplexer(PrimaryBackupCoordinator<?> coordinator) {
        assert coordinator != null;
        this.coordinator = coordinator;
        // Only register direct node-to-node packet types here.
        // StartEpochPacket and ApplyStateDiffPacket go through Paxos/XdnApp.execute().
        this.register(PrimaryBackupPacketType.PB2_FORWARDED_REQUEST_PACKET);
        this.register(PrimaryBackupPacketType.PB2_RESPONSE_PACKET);
    }

    @Override
    protected Integer getPacketType(PrimaryBackupPacket message) {
        return message.getRequestType().getInt();
    }

    @Override
    protected PrimaryBackupPacket processHeader(byte[] message, NIOHeader header) {
        PrimaryBackupPacketType packetType =
                PrimaryBackupPacket.getQuickPacketTypeFromEncodedPacket(message);
        if (packetType == null) return null;
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