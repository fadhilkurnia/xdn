package edu.umass.cs.pram.packets;

import static org.junit.Assert.*;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.xdn.interfaces.behavior.WriteOnlyRequest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

@RunWith(JUnit4.class)
public class PramWriteAfterPacketTest {

  private static class DummyWriteOnlyAppRequest extends WriteOnlyRequest implements ClientRequest {

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
      return 999;
    }

    @Override
    public String toString() {
      return String.format(
          "DummyWriteAfterRequest{id:%d,svc:%s}", this.getRequestID(), this.getServiceName());
    }
  }

  private static class DummyWriteAfterAppRequestParser implements AppRequestParser {

    @Override
    public Request getRequest(String stringified) {
      return new DummyWriteOnlyAppRequest();
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
      return new HashSet<>(List.of(AppRequest.PacketType.DEFAULT_APP_REQUEST));
    }
  }

  @Test
  public void testSerializationDeserialization() {
    ClientRequest dummyRequest = new DummyWriteOnlyAppRequest();
    PramWriteAfterPacket sourcePacket = new PramWriteAfterPacket("sender", dummyRequest);
    PramPacket decoded =
        PramPacket.createFromBytes(sourcePacket.toBytes(), new DummyWriteAfterAppRequestParser());
    assertTrue(decoded instanceof PramWriteAfterPacket);
    PramWriteAfterPacket result = (PramWriteAfterPacket) decoded;
    assertEquals(sourcePacket.getRequestID(), result.getRequestID());
    assertEquals(sourcePacket.getSenderID(), result.getSenderID());
    assertEquals(
        sourcePacket.getClientWriteRequest().getRequestType(),
        result.getClientWriteRequest().getRequestType());
  }
}
