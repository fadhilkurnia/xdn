package edu.umass.cs.xdn;

import edu.umass.cs.clientcentric.VectorTimestamp;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnJsonHttpRequest;
import edu.umass.cs.xdn.request.XdnRequest;
import edu.umass.cs.xdn.request.XdnRequestType;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

public class XdnHttpRequestTest {

    @Test
    public void testHttpRequestId_Random() {
        HttpRequest request = XdnHttpRequestTest.helpCreateDummyHttpRequest();
        HttpContent content = XdnHttpRequestTest.helpCreateDummyHttpContent(128);
        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);
        assertNotEquals(httpRequest.getRequestID(), 0);
        assertNotNull(request.headers().get("XDN-Request-ID"));
        assertEquals(Long.parseLong(request.headers().get("XDN-Request-ID")),
                httpRequest.getRequestID());
    }

    @Test
    public void testHttpRequestId_InferHeaderETag() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(256);

        String specifiedId = "specified-etag-id";
        request.headers().add(HttpHeaderNames.ETAG, specifiedId);
        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);

        long expectedInferredId = Objects.hashCode(specifiedId);
        assertEquals(httpRequest.getRequestID(), expectedInferredId);
        assertNotNull(request.headers().get("XDN-Request-ID"));
        assertEquals(Long.parseLong(request.headers().get("XDN-Request-ID")), expectedInferredId);
    }

    @Test
    public void testHttpRequestId_InferHeaderXRequestId() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(256);

        String specifiedId = "specified-request-id";
        request.headers().add("X-Request-ID", specifiedId);
        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);

        long expectedInferredId = Objects.hashCode(specifiedId);
        assertEquals(httpRequest.getRequestID(), expectedInferredId);
        assertNotNull(request.headers().get("XDN-Request-ID"));
        assertEquals(Long.parseLong(request.headers().get("XDN-Request-ID")), expectedInferredId);
    }

    @Test
    public void testHttpRequestId_InferHeaderXdnRequestId() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(256);

        String specifiedId = "specified-request-id";
        long expectedInferredId = Objects.hashCode(specifiedId);
        request.headers().add("XDN-Request-ID", expectedInferredId);
        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);

        assertEquals(httpRequest.getRequestID(), expectedInferredId);
        assertNotNull(request.headers().get("XDN-Request-ID"));
        assertEquals(Long.parseLong(request.headers().get("XDN-Request-ID")), expectedInferredId);
    }

    @Test
    public void testHttpRequestServiceName_InferHeaderXdn() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(16);

        String specifiedServiceName = "dummyServiceName";
        request.headers().remove("XDN");
        request.headers().set("XDN", specifiedServiceName);
        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);

        assertEquals(httpRequest.getServiceName(), specifiedServiceName);
    }

    @Test
    public void testHttpRequestServiceName_InferHeaderHost() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(16);

        String specifiedServiceName = "dummyServiceName1";
        String hostName = String.format("%s.xdn.io", specifiedServiceName);
        request.headers().remove("XDN");
        request.headers().remove(HttpHeaderNames.HOST);
        request.headers().set(HttpHeaderNames.HOST, hostName);
        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);
        assertEquals(httpRequest.getServiceName(), specifiedServiceName);

        specifiedServiceName = "dummyServiceName2";
        hostName = String.format("%s.xdnapp.com", specifiedServiceName);
        request.headers().remove("XDN");
        request.headers().remove(HttpHeaderNames.HOST);
        request.headers().set(HttpHeaderNames.HOST, hostName);
        httpRequest = new XdnHttpRequest(request, content);
        assertEquals(httpRequest.getServiceName(), specifiedServiceName);
    }

    @Test
    public void testRequestSerialization() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(256);

        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);
        String encodedRequest = httpRequest.toString();
        assertNotNull(encodedRequest);
        assertEquals(encodedRequest,
                new String(httpRequest.toBytes(), StandardCharsets.ISO_8859_1));
        assertEquals(XdnRequest.getQuickPacketTypeFromEncodedPacket(encodedRequest),
                XdnRequestType.XDN_SERVICE_HTTP_REQUEST);
    }

    @Test
    public void testRequestSerializationDeserialization() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(256);

        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);
        String encoded = httpRequest.toString();

        XdnHttpRequest decodedHttpRequest = XdnHttpRequest.createFromString(encoded);
        assertNotNull(decodedHttpRequest);
        assertEquals(decodedHttpRequest, httpRequest);
        assertEquals(decodedHttpRequest.getRequestID(), httpRequest.getRequestID());
    }

    @Test
    public void testRequestSerializationDeserialization_AppRequestParser()
            throws RequestParseException {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(256);

        // serialize the request
        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);
        String encoded = httpRequest.toString();

        // prepare the app request parser
        String[] args = {"AR0"};
        Replicable app = new XdnGigapaxosApp(args);
        assertFalse(app.getRequestTypes().isEmpty());

        // parse/deserialize the request
        Request decodedRequest = app.getRequest(encoded);

        // check the equality
        assertInstanceOf(XdnHttpRequest.class, decodedRequest);
        assertInstanceOf(ClientRequest.class, decodedRequest);
        assertEquals(decodedRequest, httpRequest);
        assertEquals(decodedRequest.getRequestType(), httpRequest.getRequestType());
        assertEquals(((ClientRequest) decodedRequest).getRequestID(), httpRequest.getRequestID());
    }

    @Test
    public void testBenchmarkAgainstJsonEncoding_Serialization() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(1024);

        final int repetitions = 1000000;
        XdnHttpRequest protobufHttpRequest = new XdnHttpRequest(request, content);
        XdnJsonHttpRequest jsonHttpRequest = new XdnJsonHttpRequest(
                "dummyServiceName", request, content);

        int byteLenProtobuf = 0;
        long startTime = System.nanoTime();
        for (int i = 0; i < repetitions; i++) {
            String encodedProtobuf = protobufHttpRequest.toString();
            byteLenProtobuf = encodedProtobuf.length();
        }
        long estimatedTimeProtobuf = (System.nanoTime() - startTime) / repetitions;

        int byteLenJson = 0;
        startTime = System.nanoTime();
        for (int i = 0; i < repetitions; i++) {
            String encodedJson = jsonHttpRequest.toString();
            byteLenJson = encodedJson.length();
        }
        long estimatedTimeJson = (System.nanoTime() - startTime) / repetitions;

        System.out.println("-----------");
        System.out.println(">>> estimated time (protobuf) : " + estimatedTimeProtobuf + " ns");
        System.out.println(">>> estimated time (json)     : " + estimatedTimeJson + " ns");
        System.out.println("-----------");
        System.out.println(">>> encoded size (protobuf)   : " + byteLenProtobuf + " bytes");
        System.out.println(">>> encoded size (json)       : " + byteLenJson + " bytes");

        assertTrue(byteLenProtobuf <= byteLenJson);
        assertTrue(estimatedTimeProtobuf <= estimatedTimeJson);
    }

    @Test
    public void testBenchmarkAgainstJsonEncoding_Deserialization() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(1024);

        final int repetitions = 1000000;
        XdnHttpRequest protobufHttpRequest = new XdnHttpRequest(request, content);
        String encodedProtobuf = protobufHttpRequest.toString();

        XdnJsonHttpRequest jsonHttpRequest = new XdnJsonHttpRequest(
                "dummyServiceName", request, content);
        String encodedJson = jsonHttpRequest.toString();

        long startTime = System.nanoTime();
        boolean isNullProtobuf = false;
        for (int i = 0; i < repetitions; i++) {
            XdnHttpRequest decoded = XdnHttpRequest.createFromString(encodedProtobuf);
            isNullProtobuf = decoded == null;
        }
        long estimatedTimeProtobuf = (System.nanoTime() - startTime) / repetitions;

        startTime = System.nanoTime();
        boolean isNullJson = false;
        for (int i = 0; i < repetitions; i++) {
            XdnJsonHttpRequest decoded = XdnJsonHttpRequest.createFromString(encodedJson);
            isNullJson = decoded == null;
        }
        long estimatedTimeJson = (System.nanoTime() - startTime) / repetitions;

        System.out.println("-----------");
        System.out.println(">>> estimated time (protobuf) : " + estimatedTimeProtobuf + " ns");
        System.out.println(">>> estimated time (json)     : " + estimatedTimeJson + " ns");
        System.out.println("-----------");
        System.out.println(">>> is null? (protobuf)       : " + isNullProtobuf);
        System.out.println(">>> is null? (json)           : " + isNullJson);

        assertTrue(estimatedTimeProtobuf <= estimatedTimeJson);
        assertFalse(isNullProtobuf);
        assertFalse(isNullJson);
    }


    @Test
    public void testSetResponseTimestamp() {
        HttpRequest request = this.helpCreateDummyHttpRequest();
        HttpContent content = this.helpCreateDummyHttpContent(256);
        HttpResponse response = this.helpCreateDummyHttpResponse(128);
        XdnHttpRequest httpRequest = new XdnHttpRequest(request, content);
        httpRequest.setHttpResponse(response);

        // Test setting write timestamp
        String timestampType = "W";
        VectorTimestamp writeTimestamp = new VectorTimestamp(
                List.of(new String[]{"AR0", "AR1", "AR2"}));
        httpRequest.setLastTimestamp(timestampType, writeTimestamp);

        String observedCookie = httpRequest.getHttpResponse().headers().get("Set-Cookie");
        String expectedTimestampInsideCookieValue = String.format(
                "%s=%s",
                XdnHttpRequest.XDN_TIMESTAMP_COOKIE_PREFIX + timestampType, writeTimestamp);
        assertTrue(observedCookie.contains(expectedTimestampInsideCookieValue));

        // Test setting read timestamp
        timestampType = "R";
        VectorTimestamp readTimestamp = new VectorTimestamp(
                List.of(new String[]{"AR0", "AR1", "AR2"}));
        readTimestamp.updateNodeTimestamp("AR0", 100);
        httpRequest.setLastTimestamp(timestampType, readTimestamp);

        List<String> observedCookies =
                httpRequest.getHttpResponse().headers().getAll("Set-Cookie");
        expectedTimestampInsideCookieValue = String.format(
                "%s=%s",
                XdnHttpRequest.XDN_TIMESTAMP_COOKIE_PREFIX + timestampType, readTimestamp);
        boolean isExist = false;
        for (String s : observedCookies) {
            if (s.contains(expectedTimestampInsideCookieValue)) isExist = true;
        }
        assertTrue(isExist);
    }

    @Disabled("Disabled due to the ongoing development")
    @Test
    public void testGetRequestTimestamp() {
        throw new RuntimeException("unimplemented");
    }

    @Disabled("Disabled due to the ongoing development")
    @Test
    public void testBehavioralReadOnlyRequest() {
        throw new RuntimeException("unimplemented");
    }

    @Disabled("Disabled due to the ongoing development")
    @Test
    public void testBehavioralReadModifyWriteRequest() {
        throw new RuntimeException("unimplemented");
    }

    @Disabled("Disabled due to the ongoing development")
    @Test
    public void testBehavioralWriteOnlyRequest() {
        throw new RuntimeException("unimplemented");
    }

    private static HttpRequest helpCreateDummyHttpRequest() {
        String serviceName = "dummyServiceName";
        return new DefaultHttpRequest(
                HttpVersion.HTTP_1_1,
                HttpMethod.POST,
                "/?name=alice-book-catalog&qval=qwerty",
                new DefaultHttpHeaders()
                        .add("header-1", "value-1")
                        .add("header-1", "value-2")
                        .add("header-1", "value-3")
                        .add("header-a", "value-a")
                        .add("header-b", "value-b")
                        .add("Random-1", "a,b,c")
                        .add("Random-2", "a:b:c")
                        .add("XDN", serviceName)
                        .add("Random-Char", "=,;:\"'`")
                        .add("Content-Type", "multipart/mixed; boundary=gc0p4Jq0MYt08"));
    }

    private static HttpContent helpCreateDummyHttpContent(int contentLength) {
        String randomHttpBody = helpCreateRandomString(contentLength);
        return new DefaultHttpContent(
                Unpooled.copiedBuffer(randomHttpBody.getBytes(StandardCharsets.UTF_8)));
    }

    private HttpResponse helpCreateDummyHttpResponse(int contentLength) {
        String randomHttpBody = this.helpCreateRandomString(contentLength);
        return new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.OK,
                Unpooled.copiedBuffer(randomHttpBody.getBytes()));
    }

    private static String helpCreateRandomString(int length) {
        String AlphaNumericString = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvxyz0123456789";
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; ++i) {
            int ch = (int) (AlphaNumericString.length() * Math.random());
            sb.append(AlphaNumericString.charAt(ch));
        }
        return sb.toString();
    }

    public static XdnHttpRequest helpCreateDummyRequest() {
        HttpRequest request = helpCreateDummyHttpRequest();
        HttpContent content = helpCreateDummyHttpContent(128);
        return new XdnHttpRequest(request, content);
    }

}