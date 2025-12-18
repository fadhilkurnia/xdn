package edu.umass.cs.xdn.request;

import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import io.netty.handler.codec.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * XDNRequest is an umbrella class that holds all Requests handled by XDN Application and Replica
 * Coordinator. All the Request's types are available in {@link XdnRequestType}.
 *
 * <p>Unlike Packet, a Request may or may not need a Response. For example, {@link XdnHttpRequest}
 * has an attribute to store a Response (via {@link XdnHttpRequest#setHttpResponse(HttpResponse)}),
 * but the Response itself can still be Null.
 *
 * <p>Currently, there are three kind of Requests supported. ┌────────────┐ ┌─────────────────►│
 * XDNRequest │◄───────────────┐ │ └────────────┘ │ │ │ │ │ ┌───────┴───────┐
 * ┌──────────────┴─────────┐ │ XdnHttpRequest│ │ XdnStopRequest │ └───────────────┘
 * └────────────────────────┘
 */
public abstract class XdnRequest implements ReplicableRequest {

  // SERIALIZED_PREFIX is used as prefix of the serialized (string) version of XDNRequest,
  // otherwise Gigapaxos will detect it as JSONPacket and handle it incorrectly.
  // TODO: deprecate this
  public static final String SERIALIZED_PREFIX = "xdn:";

  public static XdnRequestType getQuickPacketTypeFromEncodedPacket(String encodedPacket) {
    assert encodedPacket != null;
    byte[] encoded = encodedPacket.getBytes(StandardCharsets.ISO_8859_1);
    assert encoded.length >= 4;
    ByteBuffer headerBuffer = ByteBuffer.wrap(encoded);
    int packetType = headerBuffer.getInt(0);
    return XdnRequestType.intToType.get(packetType);
  }
}
