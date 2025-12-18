package edu.umass.cs.xdn.request;

import java.util.Objects;
import org.junit.Test;

@Deprecated
public class TestXDNHttpForwardRequest {
  @Test
  public void TestXDNHttpForwardRequestSerializationDeserialization() {
    XdnJsonHttpRequest httpRequest = XdnJsonHttpRequest.TestXdnHttpRequest.createDummyTestRequest();
    String entryNodeID = "ar0";
    XDNHttpForwardRequest forwardRequest = new XDNHttpForwardRequest(httpRequest, entryNodeID);

    XDNHttpForwardRequest deserializedForwardRequest =
        XDNHttpForwardRequest.createFromString(forwardRequest.toString());

    System.out.println(forwardRequest.toString());
    System.out.println(deserializedForwardRequest.toString());

    assert deserializedForwardRequest != null : "null deserialized XDNHttpForwardRequest";
    assert Objects.equals(forwardRequest, deserializedForwardRequest)
        : "deserialized XDNHttpForwardRequest is different";
  }
}
