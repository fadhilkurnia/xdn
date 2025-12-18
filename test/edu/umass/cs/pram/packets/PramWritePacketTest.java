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
public class PramWritePacketTest {

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
      return 321;
    }

    @Override
    public String toString() {
      return String.format(
          "DummyWriteOnlyAppRequest{id:%d,svc:%s}", this.getRequestID(), this.getServiceName());
    }
  }

  private static class DummyWriteOnlyAppRequestParser implements AppRequestParser {

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
    PramWritePacket sourcePacket = new PramWritePacket(dummyRequest);
    PramPacket decoded =
        PramPacket.createFromBytes(sourcePacket.toBytes(), new DummyWriteOnlyAppRequestParser());
    assertTrue(decoded instanceof PramWritePacket);
    PramWritePacket resultingPacket = (PramWritePacket) decoded;
    assertEquals(sourcePacket.getRequestID(), resultingPacket.getRequestID());
    assertEquals(
        sourcePacket.getClientWriteRequest().getRequestType(),
        resultingPacket.getClientWriteRequest().getRequestType());
  }
}
