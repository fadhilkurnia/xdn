package edu.umass.cs.causal.packets;

import edu.umass.cs.causal.dag.VectorTimestamp;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.xdn.XdnHttpRequestTest;
import edu.umass.cs.xdn.request.XdnRequestParser;
import org.junit.Test;

import java.util.List;

public class CausalWriteForwardPacketTest {

    @Test
    public void CausalWriteForwardPacketTest_SerializationDeserialization() {
        ClientRequest clientRequest = XdnHttpRequestTest.helpCreateDummyRequest();
        VectorTimestamp dependency = new VectorTimestamp(List.of("AR0", "AR1", "AR2"));
        VectorTimestamp timestamp = new VectorTimestamp(List.of("AR0", "AR1", "AR2"));
        timestamp.updateNodeTimestamp("AR0", 1);
        CausalWriteForwardPacket packet = new CausalWriteForwardPacket(
                clientRequest.getServiceName(),
                "AR0",
                List.of(dependency),
                timestamp,
                clientRequest);

        byte[] encoded = packet.toBytes();
        assert encoded.length > 0;

        CausalWriteForwardPacket decodedPacket =
                CausalWriteForwardPacket.createFromBytes(encoded, new XdnRequestParser());
        assert decodedPacket != null;
        assert decodedPacket.equals(packet);
    }

}
