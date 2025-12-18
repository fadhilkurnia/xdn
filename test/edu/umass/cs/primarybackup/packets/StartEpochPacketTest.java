package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.primarybackup.PrimaryEpoch;
import java.nio.ByteBuffer;
import java.util.Objects;
import org.junit.Test;

public class StartEpochPacketTest {
  @Test
  public void TestStartEpochPacketSerializationDeserialization() {
    PrimaryEpoch<String> e = new PrimaryEpoch<>("ar0", 1);
    StartEpochPacket p1 = new StartEpochPacket("dummy-service-name", e);
    byte[] encodedPacket = p1.toBytes();

    // ensure the first four bytes represent the packet type
    ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
    int packetType = headerBuffer.getInt(0);
    assert packetType == PrimaryBackupPacketType.PB_START_EPOCH_PACKET.getInt();

    // ensure the decoded packet is equal to the original one
    StartEpochPacket p2 = StartEpochPacket.createFromBytes(encodedPacket);
    assert p2 != null;
    assert Objects.equals(p1, p2);
  }
}
