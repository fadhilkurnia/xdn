package edu.umass.cs.bluegreenprimarybackup.packets;

import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.UUID;

/**
 * BlueGreenStartEpochPacket announces that a node has become (or is about to become) primary for a
 * given service, under a given placement.
 *
 * <p>Corresponds to the "StartPrimaryEpoch{nextPrimaryEpoch, nextPrimaryID, nextPlacement}" packet
 * in the event/action protocol pseudocode. Proposed via paxosManager.propose() and delivered to all
 * replicas via XdnApp.execute() on Paxos commit.
 *
 * <p>Wire format (plain Java, no protobuf): [4 bytes packetType][8 bytes packetId][4 bytes
 * serviceName length] [serviceName bytes][4 bytes nextPlacement][4 bytes nextPrimaryEpoch] [4 bytes
 * nextPrimaryID length][nextPrimaryID bytes]
 */
public class BlueGreenStartEpochPacket extends BlueGreenPrimaryBackupPacket implements Byteable {

  private final long packetId;
  private final String serviceName;
  private final int nextPlacement;
  private final int nextPrimaryEpoch;
  private final String nextPrimaryID;

  public BlueGreenStartEpochPacket(
      String serviceName, int nextPlacement, int nextPrimaryEpoch, String nextPrimaryID) {
    this(
        Math.abs(UUID.randomUUID().getLeastSignificantBits()),
        serviceName,
        nextPlacement,
        nextPrimaryEpoch,
        nextPrimaryID);
  }

  private BlueGreenStartEpochPacket(
      long packetId,
      String serviceName,
      int nextPlacement,
      int nextPrimaryEpoch,
      String nextPrimaryID) {
    assert packetId > 0;
    assert serviceName != null;
    assert nextPrimaryID != null;

    this.packetId = packetId;
    this.serviceName = serviceName;
    this.nextPlacement = nextPlacement;
    this.nextPrimaryEpoch = nextPrimaryEpoch;
    this.nextPrimaryID = nextPrimaryID;
  }

  @Override
  public IntegerPacketType getRequestType() {
    return BlueGreenPrimaryBackupPacketType.PB2_START_EPOCH_PACKET;
  }

  @Override
  public String getServiceName() {
    return this.serviceName;
  }

  @Override
  public long getRequestID() {
    return this.packetId;
  }

  @Override
  public boolean needsCoordination() {
    return true;
  }

  public int getNextPlacement() {
    return this.nextPlacement;
  }

  public int getNextPrimaryEpoch() {
    return this.nextPrimaryEpoch;
  }

  public String getNextPrimaryID() {
    return this.nextPrimaryID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    BlueGreenStartEpochPacket that = (BlueGreenStartEpochPacket) o;
    return packetId == that.packetId
        && nextPlacement == that.nextPlacement
        && nextPrimaryEpoch == that.nextPrimaryEpoch
        && Objects.equals(serviceName, that.serviceName)
        && Objects.equals(nextPrimaryID, that.nextPrimaryID);
  }

  @Override
  public int hashCode() {
    return Objects.hash(packetId, serviceName, nextPlacement, nextPrimaryEpoch, nextPrimaryID);
  }

  @Override
  public String toString() {
    return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
  }

  @Override
  public byte[] toBytes() {
    try {
      ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
      DataOutputStream out = new DataOutputStream(byteStream);

      out.writeInt(this.getRequestType().getInt());
      out.writeLong(this.packetId);

      byte[] serviceNameBytes = this.serviceName.getBytes(StandardCharsets.UTF_8);
      out.writeInt(serviceNameBytes.length);
      out.write(serviceNameBytes);

      out.writeInt(this.nextPlacement);
      out.writeInt(this.nextPrimaryEpoch);

      byte[] nextPrimaryIDBytes = this.nextPrimaryID.getBytes(StandardCharsets.UTF_8);
      out.writeInt(nextPrimaryIDBytes.length);
      out.write(nextPrimaryIDBytes);

      return byteStream.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static BlueGreenStartEpochPacket createFromBytes(byte[] encodedPacket) {
    assert encodedPacket != null && encodedPacket.length >= 4;

    ByteBuffer buffer = ByteBuffer.wrap(encodedPacket);
    int packetType = buffer.getInt();
    assert packetType == BlueGreenPrimaryBackupPacketType.PB2_START_EPOCH_PACKET.getInt()
        : "invalid packet header: " + packetType;

    long packetId = buffer.getLong();

    int serviceNameLen = buffer.getInt();
    byte[] serviceNameBytes = new byte[serviceNameLen];
    buffer.get(serviceNameBytes);
    String serviceName = new String(serviceNameBytes, StandardCharsets.UTF_8);

    int nextPlacement = buffer.getInt();
    int nextPrimaryEpoch = buffer.getInt();

    int nextPrimaryIDLen = buffer.getInt();
    byte[] nextPrimaryIDBytes = new byte[nextPrimaryIDLen];
    buffer.get(nextPrimaryIDBytes);
    String nextPrimaryID = new String(nextPrimaryIDBytes, StandardCharsets.UTF_8);

    return new BlueGreenStartEpochPacket(
        packetId, serviceName, nextPlacement, nextPrimaryEpoch, nextPrimaryID);
  }
}
