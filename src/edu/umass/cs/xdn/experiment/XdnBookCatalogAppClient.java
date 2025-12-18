package edu.umass.cs.xdn.experiment;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.ReconfigurableAppClientAsync;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnRequestType;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.UUID;

public class XdnBookCatalogAppClient extends ReconfigurableAppClientAsync<String> {

  public XdnBookCatalogAppClient(Set<InetSocketAddress> reconfigurators) throws IOException {
    super(reconfigurators);
  }

  @Override
  public Request getRequest(String stringified) throws RequestParseException {
    XdnRequestType requestType = XdnHttpRequest.getQuickPacketTypeFromEncodedPacket(stringified);
    assert requestType == XdnRequestType.XDN_SERVICE_HTTP_REQUEST;
    return XdnHttpRequest.createFromString(stringified);
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return Set.of(XdnRequestType.XDN_SERVICE_HTTP_REQUEST);
  }

  public static void main(String[] args) throws IOException {
    // Initialize connection to the control plane
    Set<InetSocketAddress> controlPlaneServers = Set.of(new InetSocketAddress("127.0.0.1", 3000));
    XdnBookCatalogAppClient client = new XdnBookCatalogAppClient(controlPlaneServers);

    // Ensure there is service name called "bookcatalog"!

    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add("XDN", "bookcatalog");
    headers.add(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    HttpRequest httpRequest =
        new DefaultHttpRequest(HttpVersion.HTTP_1_1, HttpMethod.POST, "/api/books", headers);
    String randomId = UUID.randomUUID().toString();
    String content =
        "{\"title\": \"Distributed Systems of " + randomId + "\", \"author\": \"Tanenbaum\"}";
    HttpContent httpContent =
        new DefaultHttpContent(
            Unpooled.copiedBuffer(content.getBytes(StandardCharsets.ISO_8859_1)));
    XdnHttpRequest xdnHttpRequest = new XdnHttpRequest(httpRequest, httpContent);

    // try to send the request
    client.sendRequest(
        xdnHttpRequest,
        response -> {
          System.out.println("response: " + response);
          return "success";
        });
  }
}
