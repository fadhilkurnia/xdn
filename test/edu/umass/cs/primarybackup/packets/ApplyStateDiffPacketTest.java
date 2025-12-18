package edu.umass.cs.primarybackup.packets;

import edu.umass.cs.primarybackup.PrimaryEpoch;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Random;
import org.junit.Test;

public class ApplyStateDiffPacketTest {
  @Test
  public void TestApplyStateDiffPacketSerializationDeserialization() {
    byte[] stateDiff = new byte[10240];
    new Random().nextBytes(stateDiff);

    String serviceName = "dummyServiceName";
    PrimaryEpoch<String> zero = new PrimaryEpoch<>("0:0");
    String stateDiffString = new String(stateDiff, StandardCharsets.ISO_8859_1);

    ApplyStateDiffPacket p1 = new ApplyStateDiffPacket(serviceName, zero, stateDiffString);
    byte[] encodedPacket = p1.toBytes();

    // ensure the first four bytes represent the packet type
    ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
    int packetType = headerBuffer.getInt(0);
    assert packetType == PrimaryBackupPacketType.PB_STATE_DIFF_PACKET.getInt();

    ApplyStateDiffPacket p2 = ApplyStateDiffPacket.createFromBytes(encodedPacket);

    assert p2 != null;
    assert Objects.equals(p1, p2);
  }
}
