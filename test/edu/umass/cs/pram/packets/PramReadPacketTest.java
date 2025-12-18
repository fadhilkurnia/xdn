package edu.umass.cs.pram.packets;

import static org.junit.Assert.*;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.xdn.interfaces.behavior.ReadOnlyRequest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PramReadPacketTest {

  private static class DummyReadOnlyAppRequest extends ReadOnlyRequest implements ClientRequest {

    @Override
    public ClientRequest getResponse() {
      return this;
    }

    @Override
    public IntegerPacketType getRequestType() {
      return AppRequest.PacketType.DEFAULT_APP_REQUEST;
    }

    @Override
    public String getServiceName() {
      return "dummyService";
    }

    @Override
    public long getRequestID() {
      return 123;
    }

    @Override
    public String toString() {
      return String.format(
          "DummyReadOnlyAppRequest{id:%d,svc:%s}", this.getRequestID(), this.getServiceName());
    }
  }

  private static class DummyReadOnlyAppRequestParser implements AppRequestParser {

    @Override
    public Request getRequest(String stringified) {
      return new DummyReadOnlyAppRequest();
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
      return new HashSet<>(List.of(AppRequest.PacketType.DEFAULT_APP_REQUEST));
    }
  }

  @Test
  public void testInitialization() {
    ClientRequest dummyRequest = new DummyReadOnlyAppRequest();
    PramReadPacket packet = new PramReadPacket(dummyRequest);
    assertTrue(packet.getRequestID() > 0);
    assertTrue(packet.needsCoordination());
    assertSame(packet.getRequestType(), PramPacketType.PRAM_READ_PACKET);
  }

  @Test
  public void testToString() {
    ClientRequest dummyRequest = new DummyReadOnlyAppRequest();
    PramReadPacket packet = new PramReadPacket(dummyRequest);
    assertNotNull(packet.toString());
    assertTrue(packet.toBytes().length > 0);
  }

  @Test
  public void testSerializationDeserialization() {
    ClientRequest dummyRequest = new DummyReadOnlyAppRequest();
    PramReadPacket sourcePacket = new PramReadPacket(dummyRequest);
    PramPacket decoded =
        PramPacket.createFromBytes(sourcePacket.toBytes(), new DummyReadOnlyAppRequestParser());
    assertTrue(decoded instanceof PramReadPacket);
    PramReadPacket resultingPacket = (PramReadPacket) decoded;
    assertEquals(sourcePacket.getRequestID(), resultingPacket.getRequestID());
    assertEquals(
        sourcePacket.getClientReadRequest().getRequestType(),
        resultingPacket.getClientReadRequest().getRequestType());
  }
}
