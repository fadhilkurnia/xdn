package edu.umass.cs.primarybackup.packets;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import org.junit.Test;

public class ResponsePacketTest {
  @Test
  public void TestResponsePacketSerializationDeserialization() {
    String serviceName = "dummy-service-name";
    byte[] request = "raw-response".getBytes(StandardCharsets.ISO_8859_1);
    long requestId = 1000;
    ResponsePacket p1 = new ResponsePacket(serviceName, requestId, request);

    byte[] encodedPacket = p1.toBytes();

    // ensure the first four bytes represent the packet type
    ByteBuffer headerBuffer = ByteBuffer.wrap(encodedPacket);
    int packetType = headerBuffer.getInt(0);
    assert packetType == PrimaryBackupPacketType.PB_RESPONSE_PACKET.getInt();

    // ensure the decoded packet is equal to the original one
    ResponsePacket p2 = ResponsePacket.createFromBytes(encodedPacket);

    assert p2 != null;
    assert Objects.equals(p1, p2);
  }
}
