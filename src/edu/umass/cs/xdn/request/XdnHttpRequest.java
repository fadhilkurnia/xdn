package edu.umass.cs.xdn.request;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.umass.cs.clientcentric.VectorTimestamp;
import edu.umass.cs.clientcentric.interfaces.TimestampedRequest;
import edu.umass.cs.clientcentric.interfaces.TimestampedResponse;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;
import edu.umass.cs.xdn.proto.XdnHttpRequestProto;
import edu.umass.cs.xdn.service.ServiceProperty;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.netty.handler.codec.http.HttpMethod.GET;

public class XdnHttpRequest extends XdnRequest implements ClientRequest,
        BehavioralRequest, TimestampedRequest, TimestampedResponse, Byteable {

    public static final String XDN_HTTP_REQUEST_ID_HEADER = "XDN-Request-ID";
    public static final String XDN_TIMESTAMP_COOKIE_PREFIX = "XDN-CC-TS-";

    private final long requestId;
    private final String serviceName;
    private final HttpRequest httpRequest;
    private final HttpContent httpRequestContent;
    private HttpResponse httpResponse;

    // The set for BehavioralRequest interface that requires returning
    // the behaviors of this HttpRequest.
    private final Set<RequestBehaviorType> behaviors;

    public XdnHttpRequest(HttpRequest request, HttpContent content) {
        assert request != null : "HttpRequest must be specified";
        assert content != null : "HttpContent must be specified";

        this.httpRequest = request;
        this.httpRequestContent = content;

        // Infers requestId from httpRequest, otherwise generates random ID.
        Long inferredRequestId = this.inferRequestId(request);
        this.requestId = inferredRequestId != null
                ? inferredRequestId
                : Math.abs(UUID.randomUUID().getLeastSignificantBits());
        this.httpRequest.headers().set(XDN_HTTP_REQUEST_ID_HEADER, this.requestId);

        // Infers service name from httpRequest.
        this.serviceName = this.inferServiceName(request);
        assert this.serviceName != null : "Failed to infer service name from the given HttpRequest";

        // Infers request behaviors based on httpRequest
        this.behaviors = this.inferRequestBehaviors(request, null);
    }

    // In general, we infer the HTTP request ID based on these headers:
    // (1) `ETag`, (2) `X-Request-ID`, or (3) `XDN-Request-ID`, in that order.
    // If the request does not contain those headers, null will be returned.
    private Long inferRequestId(HttpRequest httpRequest) {
        assert httpRequest != null : "Unspecified httpRequest";

        // case-1: encoded as Etag header
        String etag = httpRequest.headers().get(HttpHeaderNames.ETAG);
        if (etag != null) {
            return (long) Objects.hashCode(etag);
        }

        // case-2: encoded as X-Request-ID header
        String xRequestID = httpRequest.headers().get("X-Request-ID");
        if (xRequestID != null) {
            return (long) Objects.hashCode(xRequestID);
        }

        // case-3: encoded as XDN-Request-ID header
        String xdnReqID = httpRequest.headers().get("XDN-Request-ID");
        if (xdnReqID != null) {
            return Long.valueOf(xdnReqID);
        }

        return null;
    }

    // The service's name is encoded in the request header.
    // For example, the service name is 'hello' for these cases:
    // - request with "XDN: hello" in the header.
    // - request with "Host: hello.<domain>.<single-word-tld>:80" in the header,
    //   for example "Host: hello.xdnapp.com" or "Host: hello.xdn.io".
    // return null if service's name cannot be inferred
    public static String inferServiceName(HttpRequest httpRequest) {
        assert httpRequest != null : "Unspecified httpRequest";

        // Case-1: encoded in the XDN header (e.g., XDN: alice-book-catalog)
        final String headerKey = "XDN";
        String xdnHeader = httpRequest.headers().get(headerKey);
        if (xdnHeader != null && !xdnHeader.isEmpty()) {
            return xdnHeader;
        }

        // Case-2: encoded in the required Host header
        //   (e.g., Host: alice-book-catalog.xdnapp.com)
        String hostName = httpRequest.headers().get(HttpHeaderNames.HOST);
        if (hostName == null || hostName.isEmpty()) {
            return null;
        }
        String[] hostStringComponents = hostName.split("\\.");
        if (hostStringComponents.length < 3) {
            return null;
        }
        int lastThirdIdx = hostStringComponents.length - 3;
        String encodedServiceName = hostStringComponents[lastThirdIdx];
        if (!encodedServiceName.isEmpty()) {
            return encodedServiceName;
        }

        return null;
    }

    // TODO: complete this implementation, possibly need to infer from the service properties.
    //  Currently we assume all Http Get/Options/Head request is read-only, while others
    //  are write-only.
    private Set<RequestBehaviorType> inferRequestBehaviors(HttpRequest httpRequest,
                                                           ServiceProperty serviceProperty) {
        Set<RequestBehaviorType> types = new HashSet<>();
        HttpMethod httpMethod = httpRequest.method();
        if (httpMethod == GET || httpMethod == HttpMethod.OPTIONS ||
                httpMethod == HttpMethod.HEAD) {
            types.add(RequestBehaviorType.READ_ONLY);
        } else {
            types.add(RequestBehaviorType.WRITE_ONLY);
        }
        return types;
    }

    @Override
    public ClientRequest getResponse() {
        return this;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return XdnRequestType.XDN_SERVICE_HTTP_REQUEST;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public long getRequestID() {
        return this.requestId;
    }

    @Override
    public boolean needsCoordination() {
        return true;
    }

    @Override
    public Set<RequestBehaviorType> getBehaviors() {
        return this.behaviors;
    }

    @Override
    public VectorTimestamp getLastTimestamp(String timestampName) {
        assert timestampName.equals("R") || timestampName.equals("W");
        assert this.httpRequest != null;

        // Validate if we have cookie in the header
        String cookieRaw = this.httpRequest.headers() != null
                ? this.httpRequest.headers().get("Cookie")
                : null;
        if (cookieRaw == null) {
            return null;
        }

        // decode the cookie
        Set<io.netty.handler.codec.http.cookie.Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieRaw);
        if (cookies.isEmpty()) {
            return null;
        }

        // decode the timestamp in the cookie
        final String timestampCookieKey = XDN_TIMESTAMP_COOKIE_PREFIX + timestampName;
        String timestampString = null;
        for (Cookie cookie : cookies) {
            if (cookie.name().equals(timestampCookieKey)) {
                timestampString = cookie.value();
            }
        }
        if (timestampString == null) {
            return null;
        }

        return VectorTimestamp.createFromString(timestampString);
    }

    @Override
    public void setLastTimestamp(String timestampName, VectorTimestamp timestamp) {
        assert timestampName != null;
        assert timestampName.equals("R") || timestampName.equals("W");
        assert this.httpResponse != null : "response cannot be null";
        assert this.httpResponse.headers() != null;

        final String timestampCookieKey = XDN_TIMESTAMP_COOKIE_PREFIX + timestampName;
        Cookie cookie = new io.netty.handler.codec.http.cookie.DefaultCookie(timestampCookieKey, timestamp.toString());
        cookie.setPath("/");
        cookie.setHttpOnly(true);
        this.httpResponse.headers().add("Set-Cookie", ServerCookieEncoder.STRICT.encode(cookie));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XdnHttpRequest that = (XdnHttpRequest) o;
        return Objects.equals(serviceName, that.serviceName) && Objects.equals(httpRequest, that.httpRequest) && Objects.equals(httpRequestContent, that.httpRequestContent);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, httpRequest, httpRequestContent);
    }

    public HttpRequest getHttpRequest() {
        return this.httpRequest;
    }

    public HttpContent getHttpRequestContent() {
        return this.httpRequestContent;
    }

    public HttpResponse getHttpResponse() {
        return this.httpResponse;
    }

    public void setHttpResponse(HttpResponse httpResponse) {
        assert httpRequest != null;
        this.httpResponse = httpResponse;
    }

    public java.net.http.HttpRequest getJavaNetHttpRequest(boolean isUseLocalhost, int targetPort) {
        java.net.http.HttpRequest.Builder netHttpRequestBuilder =
                java.net.http.HttpRequest.newBuilder();

        // Convert URI
        URI uri;
        String url;
        if (isUseLocalhost) {
            url = String.format("http://127.0.0.1:%d%s",
                    targetPort, this.httpRequest.uri());
            try {
                uri = new URI(url);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        } else {
            throw new RuntimeException("unimplemented");
        }

        // Get method
        String method = this.httpRequest.method().toString();

        // Preparing the HTTP headers, if any.
        // note that the code need to be run with the following flag:
        // "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host",
        // otherwise setting those restricted headers here will later trigger
        // java.lang.IllegalArgumentException, such as: restricted header name: "Host".
        if (this.httpRequest.headers() != null) {
            Iterator<Map.Entry<String, String>> it = this.httpRequest.headers().iteratorAsString();
            while (it.hasNext()) {
                Map.Entry<String, String> entry = it.next();
                netHttpRequestBuilder.setHeader(entry.getKey(), entry.getValue());
            }
        }

        // Converts body, if any.
        java.net.http.HttpRequest.BodyPublisher bodyPublisher =
                java.net.http.HttpRequest.BodyPublishers.noBody();
        if (this.httpRequestContent.content() != null) {
            int lenBody = this.httpRequestContent.content().readableBytes();
            byte[] requestBody = new byte[lenBody];
            this.httpRequestContent.content().duplicate().readBytes(requestBody);
            bodyPublisher = java.net.http.HttpRequest.BodyPublishers.ofByteArray(requestBody);
        }

        return netHttpRequestBuilder
                .uri(uri)
                .method(method, bodyPublisher)
                .build();
    }

    @Override
    public byte[] toBytes() {
        ByteArrayOutputStream output = new ByteArrayOutputStream();

        // Serialize the packet type
        int packetType = this.getRequestType().getInt();
        byte[] encodedHeader = ByteBuffer.allocate(4).putInt(packetType).array();

        // Serialize the protocol version
        XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion version;
        if (this.httpRequest.protocolVersion().equals(HttpVersion.HTTP_1_0)) {
            version = XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion.HTTP_1_0;
        } else {
            version = XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion.HTTP_1_1;
        }

        // Serialize the http method
        XdnHttpRequestProto.XdnHttpRequest.HttpMethod method;
        switch (this.httpRequest.method().toString()) {
            case "GET": {
                method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.GET;
                break;
            }
            case "HEAD": {
                method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.HEAD;
                break;
            }
            case "POST": {
                method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.POST;
                break;
            }
            case "PUT": {
                method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.PUT;
                break;
            }
            case "DELETE": {
                method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.DELETE;
                break;
            }
            case "CONNECT": {
                method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.CONNECT;
                break;
            }
            case "OPTIONS": {
                method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.OPTIONS;
                break;
            }
            case "PATCH": {
                method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.PATCH;
                break;
            }
            default: {
                throw new RuntimeException("unsupported http method of "
                        + this.httpRequest.method());
            }
        }

        // Get the request uri
        String uri = this.httpRequest.uri();

        // Serialize the request content
        int lenBody = this.httpRequestContent.content().readableBytes();
        byte[] requestBody = new byte[lenBody];
        this.httpRequestContent.content().duplicate().readBytes(requestBody);
        ByteString requestBodyBytes = ByteString.copyFrom(requestBody);

        // Serialize the headers
        Iterator<Map.Entry<String, String>> it = this.httpRequest.headers().iteratorAsString();
        List<XdnHttpRequestProto.XdnHttpRequest.Header> headerList = new ArrayList<>();
        while (it.hasNext()) {
            Map.Entry<String, String> e = it.next();
            XdnHttpRequestProto.XdnHttpRequest.Header header =
                    XdnHttpRequestProto.XdnHttpRequest.Header.newBuilder()
                            .setName(e.getKey())
                            .setValue(e.getValue())
                            .build();
            headerList.add(header);
        }

        // Serialize all the fields above with Protobuf
        XdnHttpRequestProto.XdnHttpRequest.Builder protoBuilder =
                XdnHttpRequestProto.XdnHttpRequest
                        .newBuilder()
                        .setProtocolVersion(version)
                        .setRequestMethod(method)
                        .setRequestUri(uri)
                        .addAllRequestHeaders(headerList)
                        .setRequestBody(requestBodyBytes);

        // Serialize the packetType in the header, followed by the protobuf
        output.writeBytes(encodedHeader);
        output.writeBytes(protoBuilder.build().toByteArray());
        return output.toByteArray();
    }

    @Override
    public String toString() {
        return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
    }

    public static XdnHttpRequest createFromString(String encodedRequest) {
        assert encodedRequest != null;
        byte[] encoded = encodedRequest.getBytes(StandardCharsets.ISO_8859_1);

        // Decode the packet type
        assert encoded.length >= 4;
        ByteBuffer headerBuffer = ByteBuffer.wrap(encoded);
        int packetType = headerBuffer.getInt(0);
        assert packetType == XdnRequestType.XDN_SERVICE_HTTP_REQUEST.getInt()
                : "invalid packet header: " + packetType;
        encoded = Arrays.copyOfRange(encoded, 4, encoded.length);

        // Decode the bytes into Proto
        XdnHttpRequestProto.XdnHttpRequest decodedProto = null;
        try {
            decodedProto = XdnHttpRequestProto.XdnHttpRequest.parseFrom(encoded);
        } catch (InvalidProtocolBufferException e) {
            Logger.getGlobal().log(Level.WARNING,
                    "Invalid protobuf bytes given: " + e.getMessage());
            return null;
        }

        // Convert the protocol version
        HttpVersion version = getHttpVersionFromProto(decodedProto.getProtocolVersion());

        // Convert the Http method
        HttpMethod method = new HttpMethod(decodedProto.getRequestMethod().toString());

        // Get the URI
        String uri = decodedProto.getRequestUri();

        // Convert the headers
        HttpHeaders headers = new DefaultHttpHeaders();
        for (XdnHttpRequestProto.XdnHttpRequest.Header header :
                decodedProto.getRequestHeadersList()) {
            headers.add(header.getName(), header.getValue());
        }

        // Convert the content
        HttpContent content = new DefaultHttpContent(
                Unpooled.copiedBuffer(decodedProto.getRequestBody().toByteArray()));

        // Convert the proto into HttpRequest and HttpContent
        HttpRequest request = new DefaultHttpRequest(version, method, uri, headers);

        return new XdnHttpRequest(request, content);
    }

    private static HttpVersion getHttpVersionFromProto(
            XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion version) {
        if (Objects.requireNonNull(version) ==
                XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion.HTTP_1_0) {
            return HttpVersion.HTTP_1_0;
        }
        return HttpVersion.HTTP_1_1;
    }
}
