package edu.umass.cs.primarybackup.packets;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.junit.Test;

public class ForwardedRequestPacketTest {
  @Test
  public void TestForwardedRequestPacketSerializationDeserialization() {
    String serviceName = "dummy-service-name";
    String entryNodeID = "ar0";
    byte[] forwardedReq = "raw-request".getBytes(StandardCharsets.ISO_8859_1);

    ForwardedRequestPacket p1 = new ForwardedRequestPacket(serviceName, entryNodeID, forwardedReq);
    byte[] encodedPacket = p1.toBytes();

    // ensure the first four bytes represent the packet type
    ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
    int packetType = headerBuffer.getInt(0);
    assert packetType == PrimaryBackupPacketType.PB_FORWARDED_REQUEST_PACKET.getInt();

    // ensure the decoded packet is equal to the original one
    ForwardedRequestPacket p2 = ForwardedRequestPacket.createFromBytes(encodedPacket);

    assert p2 != null;
    assert Objects.equals(p1, p2);
  }
}
