package edu.umass.cs.bluegreenprimarybackup;

import edu.umass.cs.nio.AbstractPacketDemultiplexer;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.bluegreenprimarybackup.packets.BlueGreenPrimaryBackupPacket;
import edu.umass.cs.bluegreenprimarybackup.packets.BlueGreenPrimaryBackupPacketType;
import java.io.IOException;

/**
 * BlueGreenPrimaryBackupPacketDemultiplexer intercepts direct node-to-node
 * BlueGreenPrimaryBackupPackets at the NIO layer (BlueGreenForwardedRequestPacket,
 * BlueGreenResponsePacket) and routes them to the coordinator's coordinateRequest().
 *
 * <p>BlueGreenStartEpochPacket and BlueGreenApplyStateDiffPacket travel via Paxos and are delivered
 * through XdnApp.execute() — they do NOT go through this demultiplexer.
 */
public class BlueGreenPrimaryBackupPacketDemultiplexer
    extends AbstractPacketDemultiplexer<BlueGreenPrimaryBackupPacket> {

  private final BlueGreenPrimaryBackupCoordinator<?> coordinator;

  public BlueGreenPrimaryBackupPacketDemultiplexer(
      BlueGreenPrimaryBackupCoordinator<?> coordinator) {
    assert coordinator != null;
    this.coordinator = coordinator;
    // Only register direct node-to-node packet types here.
    // BlueGreenStartEpochPacket and BlueGreenApplyStateDiffPacket go through
    // Paxos/XdnApp.execute().
    this.register(BlueGreenPrimaryBackupPacketType.PB2_FORWARDED_REQUEST_PACKET);
    this.register(BlueGreenPrimaryBackupPacketType.PB2_RESPONSE_PACKET);
  }

  @Override
  protected Integer getPacketType(BlueGreenPrimaryBackupPacket message) {
    return message.getRequestType().getInt();
  }

  @Override
  protected BlueGreenPrimaryBackupPacket processHeader(byte[] message, NIOHeader header) {
    BlueGreenPrimaryBackupPacketType packetType =
        BlueGreenPrimaryBackupPacket.getQuickPacketTypeFromEncodedPacket(message);
    if (packetType == null) return null;
    return BlueGreenPrimaryBackupPacket.createFromBytes(message);
  }

  @Override
  protected boolean matchesType(Object message) {
    return message instanceof BlueGreenPrimaryBackupPacket;
  }

  @Override
  public boolean handleMessage(BlueGreenPrimaryBackupPacket message, NIOHeader header) {
    if (message == null) return false;
    try {
      return this.coordinator.coordinateRequest(message, null);
    } catch (IOException | RequestParseException e) {
      throw new RuntimeException(e);
    }
  }
}
