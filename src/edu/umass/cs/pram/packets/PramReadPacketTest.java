package edu.umass.cs.pram.packets;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.interfaces.behavior.ReadOnlyRequest;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.*;

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
            return String.format("DummyReadOnlyAppRequest{id:%d,svc:%s}",
                    this.getRequestID(), this.getServiceName());
        }
    }

    private static class DummyReadOnlyAppRequestParser implements AppRequestParser {

        @Override
        public Request getRequest(String stringified) throws RequestParseException {
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
        assertThat(packet.toString(), containsString(dummyRequest.toString()));
        assertThat(packet.toString(),
                containsString(String.valueOf(AppRequest.PacketType.DEFAULT_APP_REQUEST.getInt())));
        System.out.println(packet.toString());
    }

    @Test
    public void testFromJsonObject() throws JSONException {
        JSONObject object = new JSONObject();
        object.put("id", 123);
        object.put("type", PramPacketType.PRAM_READ_PACKET.getInt());
        object.put("req", (new DummyReadOnlyAppRequest()).toString());
        PramReadPacket packet = PramReadPacket.fromJsonObject(object, new DummyReadOnlyAppRequestParser());
        assertNotNull(packet);
        System.out.println(packet.toString());
    }

    @Test
    public void testSerializationDeserialization() {
        ClientRequest dummyRequest = new DummyReadOnlyAppRequest();
        PramReadPacket sourcePacket = new PramReadPacket(dummyRequest);
        Request resultingPacket =
                PramPacket.createFromString(sourcePacket.toString(), new DummyReadOnlyAppRequestParser());
        assertTrue(resultingPacket instanceof PramReadPacket);
        assertEquals(sourcePacket.toString(), resultingPacket.toString());
    }

}
