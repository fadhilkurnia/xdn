package edu.umass.cs.xdn.request;

import static org.junit.jupiter.api.Assertions.*;

import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.junit.jupiter.api.Test;

public class XdnHttpRequestBatchTest {

  @Test
  public void testParseRequestIdQuickly() {
    XdnHttpRequest request1 = new XdnHttpRequest(createHttpRequest("/a"), createContent("body1"));
    XdnHttpRequest request2 = new XdnHttpRequest(createHttpRequest("/b"), createContent("body2"));

    XdnHttpRequestBatch batch = new XdnHttpRequestBatch(List.of(request1, request2));
    String encoded = new String(batch.toBytes(), StandardCharsets.ISO_8859_1);

    Long quickId = XdnHttpRequestBatch.parseRequestIdQuickly(encoded);
    assertNotNull(quickId);
    assertEquals(batch.getRequestID(), quickId);
  }

  @Test
  public void testParseRequestIdQuicklyMalformed() {
    assertNull(XdnHttpRequestBatch.parseRequestIdQuickly("bad"));
  }

  private static HttpRequest createHttpRequest(String uri) {
    return new DefaultHttpRequest(
        HttpVersion.HTTP_1_1,
        HttpMethod.GET,
        uri,
        new DefaultHttpHeaders().add("XDN", "dummyService"));
  }

  private static HttpContent createContent(String body) {
    return new DefaultHttpContent(Unpooled.copiedBuffer(body.getBytes(StandardCharsets.UTF_8)));
  }
}
