package edu.umass.cs.xdn.request;

import com.google.protobuf.ByteString;
import com.google.protobuf.CodedInputStream;
import com.google.protobuf.ExtensionRegistryLite;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.WireFormat;
import edu.umass.cs.clientcentric.VectorTimestamp;
import edu.umass.cs.clientcentric.interfaces.TimestampedRequest;
import edu.umass.cs.clientcentric.interfaces.TimestampedResponse;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;
import edu.umass.cs.xdn.proto.XdnHttpRequestProto;
import edu.umass.cs.xdn.service.RequestMatcher;
import edu.umass.cs.xdn.service.ServiceProperty;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XdnHttpRequest extends XdnRequest
    implements ClientRequest, BehavioralRequest, TimestampedRequest, TimestampedResponse, Byteable {

  public static final String XDN_HTTP_REQUEST_ID_HEADER = "XDN-Request-ID";
  public static final String XDN_TIMESTAMP_COOKIE_PREFIX = "XDN-CC-TS-";

  // URL query param used by XDN to name the target service without a dedicated
  // header or DNS subdomain, so a link like http://host:port/?_xdnsvc=foo works
  // from a browser. The leading underscore signals that it is an XDN-reserved
  // param; any param starting with XDN_RESERVED_QUERY_PREFIX is stripped from
  // the URI before the request is forwarded to the containerized service.
  public static final String XDN_SVC_QUERY_PARAM = "_xdnsvc";
  public static final String XDN_RESERVED_QUERY_PREFIX = "_xdn";

  // Cookie name used to carry the service name across browser navigation once
  // the server sets Set-Cookie: XDN=<service> in response to an _xdnsvc URL.
  public static final String XDN_SVC_COOKIE_NAME = "XDN";

  public static final List<RequestMatcher> defaultSingletonRequestMatchers =
      ServiceProperty.createDefaultMatchers();

  private static final Logger LOG = Logger.getLogger(XdnHttpRequest.class.getName());
  private static final ExtensionRegistryLite EMPTY_REGISTRY =
      ExtensionRegistryLite.getEmptyRegistry();

  private final long requestId;
  private final String serviceName;
  // Whether the service name was provided via the XDN_SVC_QUERY_PARAM URL param.
  // Used by maybeAddXdnCookieToResponse() to decide whether to set Set-Cookie.
  private final boolean serviceNameFromQueryParam;

  private final HttpRequest httpRequest;
  private final HttpContent httpRequestContent;

  private HttpResponse httpResponse;
  private ByteBuf httpResponseBody;

  // A helper flag that is true iff this instance is created via createFromString().
  // The flag is particularly useful to decide whether to release the reference-counted
  // httpResponse. In XDN, the instance is created via createFromString() in non entry-replica
  // node, and thus we can discard and release the httpResponse immediately.
  private final boolean isCreatedFromString;

  // The set for BehavioralRequest interface that requires returning
  // the behaviors of this HttpRequest. Note that requestMatcher must
  // be set to know the behaviors of this HttpRequest.
  private Set<RequestBehaviorType> behaviors;
  private List<RequestMatcher> requestMatchers;

  public XdnHttpRequest(HttpRequest request, HttpContent content) {
    this(null, request, content, null, false);
  }

  private XdnHttpRequest(
      Long providedRequestId,
      HttpRequest request,
      HttpContent content,
      List<RequestMatcher> requestMatchers,
      boolean isCreatedFromString) {
    assert request != null : "HttpRequest must be specified";
    assert content != null : "HttpContent must be specified";

    this.httpRequest = request;
    this.httpRequestContent = content;

    // Get requestId from the provided arg, otherwise infer requestId from httpRequest,
    // otherwise generates random ID.
    Long inferredRequestId = this.inferRequestId(request);
    this.requestId =
        providedRequestId != null
            ? providedRequestId
            : inferredRequestId != null
                ? inferredRequestId
                : ThreadLocalRandom.current().nextLong(Long.MAX_VALUE);
    this.httpRequest.headers().set(XDN_HTTP_REQUEST_ID_HEADER, this.requestId);

    // Infers service name from httpRequest. Also remember whether the name came from
    // the _xdnsvc query param so the response path knows to emit Set-Cookie: XDN=...
    this.serviceNameFromQueryParam = readXdnsvcQueryParam(request) != null;
    this.serviceName = inferServiceName(request);
    assert this.serviceName != null : "Failed to infer service name from the given HttpRequest";

    // Normalize the service name into the XDN header so it survives serialization
    // to Paxos follower replicas, which re-run inferServiceName on the deserialized
    // wire form. Without this, a request that named its service via _xdnsvc (which
    // we strip below) would lose the signal in transit and the follower would fail
    // to decode the batched request with "Mismatched service name".
    this.httpRequest.headers().set("XDN", this.serviceName);

    // Strip XDN-reserved query params (e.g., _xdnsvc) from the URI so the containerized
    // service never sees XDN routing internals. Must run after inferServiceName.
    this.httpRequest.setUri(stripXdnReservedQueryParams(this.httpRequest.uri()));

    // Use the default request matcher if not specified
    this.requestMatchers =
        (requestMatchers != null && !requestMatchers.isEmpty())
            ? requestMatchers
            : defaultSingletonRequestMatchers;

    this.isCreatedFromString = isCreatedFromString;
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

  // The service's name can be specified in multiple ways. Precedence, highest first:
  //   1. URL query param "_xdnsvc" (e.g., http://host/path?_xdnsvc=hello) — lets a
  //      user switch services by clicking a fresh link even if their client sends a
  //      stale XDN header or cookie.
  //   2. "XDN" header (e.g., XDN: hello) — the canonical programmatic form (CLI/curl).
  //   3. "XDN" cookie — set by the server after a _xdnsvc request, so that subsequent
  //      browser navigation (which drops query params on link clicks) keeps routing
  //      to the same service.
  //   4. Host subdomain (e.g., Host: hello.xdn.io) — legacy, requires DNS/hosts setup.
  // Returns null if service's name cannot be inferred.
  public static String inferServiceName(HttpRequest httpRequest) {
    assert httpRequest != null : "Unspecified httpRequest";

    // Case-1: URL query param _xdnsvc
    String queryParamName = readXdnsvcQueryParam(httpRequest);
    if (queryParamName != null && !queryParamName.isEmpty()) {
      return queryParamName;
    }

    // Case-2: XDN header
    final String headerKey = "XDN";
    String xdnHeader = httpRequest.headers().get(headerKey);
    if (xdnHeader != null && !xdnHeader.isEmpty()) {
      return xdnHeader;
    }

    // Case-3: XDN cookie
    String cookieName = readXdnCookie(httpRequest);
    if (cookieName != null && !cookieName.isEmpty()) {
      return cookieName;
    }

    // Case-4: Host subdomain (e.g., Host: alice-book-catalog.xdnapp.com)
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

  // Returns true if the request carries a browser-only XDN signal that the legacy
  // header/Host dispatcher would miss: the _xdnsvc URL query param or the XDN cookie.
  // Used by HttpActiveReplica's top-level channelRead0 dispatcher so that browser
  // clicks on http://host/?_xdnsvc=foo (and their follow-up cookie-bearing requests)
  // are routed to the XDN handler instead of the legacy GigaPaxos path.
  public static boolean hasXdnBrowserSignal(HttpRequest httpRequest) {
    if (httpRequest == null) {
      return false;
    }
    return readXdnsvcQueryParam(httpRequest) != null || readXdnCookie(httpRequest) != null;
  }

  // Returns the first non-empty value of the _xdnsvc query param, or null if absent.
  private static String readXdnsvcQueryParam(HttpRequest httpRequest) {
    String uri = httpRequest.uri();
    if (uri == null || uri.indexOf('?') < 0) {
      return null;
    }
    List<String> values = new QueryStringDecoder(uri).parameters().get(XDN_SVC_QUERY_PARAM);
    if (values == null) {
      return null;
    }
    for (String v : values) {
      if (v != null && !v.isEmpty()) {
        return v;
      }
    }
    return null;
  }

  // Returns the value of the XDN cookie, or null if absent. This is distinct from
  // the XDN-CC-TS-* cookies used for causal-consistency vector timestamps; both may
  // coexist in the same Cookie header.
  private static String readXdnCookie(HttpRequest httpRequest) {
    if (httpRequest.headers() == null) {
      return null;
    }
    String cookieRaw = httpRequest.headers().get("Cookie");
    if (cookieRaw == null || cookieRaw.isEmpty()) {
      return null;
    }
    Set<Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieRaw);
    for (Cookie cookie : cookies) {
      if (XDN_SVC_COOKIE_NAME.equals(cookie.name())) {
        return cookie.value();
      }
    }
    return null;
  }

  // Removes any query param whose key starts with XDN_RESERVED_QUERY_PREFIX from
  // the URI. Returns the URI unchanged when it has no query string or no reserved
  // params. When the only params are reserved, the resulting URI has no '?'.
  static String stripXdnReservedQueryParams(String uri) {
    if (uri == null || uri.indexOf('?') < 0) {
      return uri;
    }
    QueryStringDecoder decoder = new QueryStringDecoder(uri);
    Map<String, List<String>> params = decoder.parameters();
    boolean hasReserved = false;
    for (String key : params.keySet()) {
      if (key != null && key.startsWith(XDN_RESERVED_QUERY_PREFIX)) {
        hasReserved = true;
        break;
      }
    }
    if (!hasReserved) {
      return uri;
    }
    QueryStringEncoder encoder = new QueryStringEncoder(decoder.path());
    for (Map.Entry<String, List<String>> e : params.entrySet()) {
      String key = e.getKey();
      if (key == null || key.startsWith(XDN_RESERVED_QUERY_PREFIX)) {
        continue;
      }
      for (String value : e.getValue()) {
        encoder.addParam(key, value);
      }
    }
    return encoder.toString();
  }

  private Set<RequestBehaviorType> matchRequestBehaviors(List<RequestMatcher> svcReqMatchers) {
    assert svcReqMatchers != null : "request matcher must be set before calling this method";
    assert this.httpRequest.uri().startsWith("/")
        : "unexpected path in the http request: " + httpRequest.uri();
    Set<RequestBehaviorType> types = new HashSet<>();

    // TODO: we should handle the hierarchy of paths and behaviors.
    //  e.g., "POST /" is declared as READ_MODIFY_WRITE but "POST /api/books/123" is WRITE_ONLY,
    //  then WRITE_ONLY should take the priority since it is more "specific".
    for (RequestMatcher matcher : svcReqMatchers) {
      if (matcher.getHttpMethods().contains(this.httpRequest.method().name())
          && this.httpRequest.uri().startsWith(matcher.getPathPrefix())) {
        types.add(matcher.getBehavior());
      }
    }

    // default behavior with no matched behavior
    if (types.isEmpty()) {
      types.add(RequestBehaviorType.READ_MODIFY_WRITE);
      return types;
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
    if (this.behaviors == null) {
      this.behaviors = matchRequestBehaviors(this.requestMatchers);
    }
    return this.behaviors;
  }

  public void setRequestMatchers(List<RequestMatcher> requestMatchers) {
    this.requestMatchers = requestMatchers;

    // The cached behaviors are cleared so that the next call to getBehaviors()
    // will recompute them using the new matchers. This is needed because, in batching mode,
    // getBehaviors() may be called before setRequestMatchers() is called,
    // which causes behaviors from the default matchers to be cached.
    // Without clearing the cache, the old cached value would still be returned
    // even after the correct YAML matchers are set.
    this.behaviors = null;
  }

  @Override
  public VectorTimestamp getLastTimestamp(String timestampName) {
    assert timestampName.equals("R") || timestampName.equals("W");
    assert this.httpRequest != null;

    // Validate if we have cookie in the header
    String cookieRaw =
        this.httpRequest.headers() != null ? this.httpRequest.headers().get("Cookie") : null;
    if (cookieRaw == null) {
      return null;
    }

    // decode the cookie
    Set<io.netty.handler.codec.http.cookie.Cookie> cookies =
        ServerCookieDecoder.STRICT.decode(cookieRaw);
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
    Cookie cookie =
        new io.netty.handler.codec.http.cookie.DefaultCookie(
            timestampCookieKey, timestamp.toString());
    cookie.setPath("/");
    cookie.setHttpOnly(true);
    this.httpResponse.headers().add("Set-Cookie", ServerCookieEncoder.STRICT.encode(cookie));
  }

  // If this request named the service via the _xdnsvc URL query param, sets
  // Set-Cookie: XDN=<serviceName>; Path=/ on the given response so subsequent
  // browser requests (which drop query params on link clicks) still resolve.
  // No-op otherwise, so header- and Host-based clients don't get clobbered.
  public void maybeAddXdnCookieToResponse(HttpResponse response) {
    if (!this.serviceNameFromQueryParam) {
      return;
    }
    if (response == null || response.headers() == null) {
      return;
    }
    Cookie cookie =
        new io.netty.handler.codec.http.cookie.DefaultCookie(XDN_SVC_COOKIE_NAME, this.serviceName);
    cookie.setPath("/");
    response.headers().add("Set-Cookie", ServerCookieEncoder.STRICT.encode(cookie));
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    XdnHttpRequest that = (XdnHttpRequest) o;
    return Objects.equals(serviceName, that.serviceName)
        && Objects.equals(httpRequest, that.httpRequest)
        && Objects.equals(httpRequestContent, that.httpRequestContent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceName, httpRequest, httpRequestContent);
  }

  public HttpRequest getHttpRequest() {
    return this.httpRequest;
  }

  public HttpContent getHttpRequestContent() {
    return httpRequestContent;
  }

  public HttpResponse getHttpResponse() {
    return this.httpResponse;
  }

  public void setHttpResponse(HttpResponse httpResponse) {
    assert httpResponse != null : "Expecting non null httpResponse";
    assert httpResponse instanceof FullHttpResponse
        : "Expecting FullHttpResponse, but found " + httpResponse.getClass().getSimpleName();
    FullHttpResponse fullHttpResponse = (FullHttpResponse) httpResponse;
    this.httpResponse = fullHttpResponse;
    this.httpResponseBody = fullHttpResponse.content();
  }

  public void clearHttpResponse() {
    this.httpResponse = null;
    this.httpResponseBody = null;
  }

  @Override
  public byte[] toBytes() {
    final int packetType = this.getRequestType().getInt();

    XdnHttpRequestProto.XdnHttpRequest.Builder builder =
        XdnHttpRequestProto.XdnHttpRequest.newBuilder()
            .setRequestId(this.requestId)
            .setProtocolVersion(getHttpVersionProto(this.httpRequest.protocolVersion()))
            .setRequestMethod(getHttpMethodProto(this.httpRequest.method()))
            .setRequestUri(this.httpRequest.uri())
            .addAllRequestHeaders(getHeaderList(this.httpRequest.headers()))
            .setRequestBody(extractBodyByteString(this.httpRequestContent.content()));

    if (this.httpResponse != null) {
      builder.setResponse(buildResponseProto());
    }

    byte[] protoBytes = builder.build().toByteArray();
    byte[] serialized = new byte[Integer.BYTES + protoBytes.length];
    ByteBuffer.wrap(serialized).putInt(packetType).put(protoBytes);
    return serialized;
  }

  private XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion getHttpVersionProto(
      HttpVersion httpVersion) {
    XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion version;
    if (httpVersion.equals(HttpVersion.HTTP_1_0)) {
      version = XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion.HTTP_1_0;
    } else {
      version = XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion.HTTP_1_1;
    }
    return version;
  }

  private List<XdnHttpRequestProto.XdnHttpRequest.Header> getHeaderList(HttpHeaders headers) {
    Iterator<Map.Entry<String, String>> it = headers.iteratorAsString();
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
    return headerList;
  }

  private static XdnHttpRequestProto.XdnHttpRequest.HttpMethod getHttpMethodProto(
      HttpMethod httpMethod) {
    return switch (httpMethod.name()) {
      case "GET" -> XdnHttpRequestProto.XdnHttpRequest.HttpMethod.GET;
      case "HEAD" -> XdnHttpRequestProto.XdnHttpRequest.HttpMethod.HEAD;
      case "POST" -> XdnHttpRequestProto.XdnHttpRequest.HttpMethod.POST;
      case "PUT" -> XdnHttpRequestProto.XdnHttpRequest.HttpMethod.PUT;
      case "DELETE" -> XdnHttpRequestProto.XdnHttpRequest.HttpMethod.DELETE;
      case "CONNECT" -> XdnHttpRequestProto.XdnHttpRequest.HttpMethod.CONNECT;
      case "OPTIONS" -> XdnHttpRequestProto.XdnHttpRequest.HttpMethod.OPTIONS;
      case "PATCH" -> XdnHttpRequestProto.XdnHttpRequest.HttpMethod.PATCH;
      default -> throw new IllegalArgumentException(
          "Unsupported HTTP method: " + httpMethod.name());
    };
  }

  private static ByteString extractBodyByteString(ByteBuf buffer) {
    int readable = buffer.readableBytes();
    if (readable == 0) {
      return ByteString.EMPTY;
    }
    return ByteString.copyFrom(ByteBufUtil.getBytes(buffer, buffer.readerIndex(), readable, true));
  }

  private XdnHttpRequestProto.XdnHttpRequest.Response buildResponseProto() {
    ByteString responseBody =
        this.httpResponseBody != null
            ? extractBodyByteString(this.httpResponseBody)
            : ByteString.EMPTY;

    return XdnHttpRequestProto.XdnHttpRequest.Response.newBuilder()
        .setProtocolVersion(getHttpVersionProto(this.httpResponse.protocolVersion()))
        .setStatusCode(this.httpResponse.status().code())
        .addAllHeaders(getHeaderList(this.httpResponse.headers()))
        .setResponseBody(responseBody)
        .build();
  }

  public String getLogText() {
    Set<String> textContentTypes =
        Set.of(
            "text/javascript", "application/json", "application/text", "text/plain", "text/html");
    boolean isTextContent = false;
    StringBuilder headerStringListBuilder = new StringBuilder();
    Iterator<Map.Entry<String, String>> iter = this.httpRequest.headers().iteratorAsString();
    while (iter.hasNext()) {
      var it = iter.next();
      headerStringListBuilder.append(it.getKey());
      headerStringListBuilder.append(":");
      headerStringListBuilder.append(it.getValue());
      headerStringListBuilder.append(" ");
      if (it.getKey().equalsIgnoreCase("Content-Type")
          && textContentTypes.contains(it.getValue().toLowerCase())) {
        isTextContent = true;
      }
    }
    String headerStringList = headerStringListBuilder.toString();

    StringBuilder contentStringBuilder = new StringBuilder();
    if (this.httpRequestContent.content().readableBytes() == 0) {
      contentStringBuilder.append("<empty>");
      isTextContent = false;
    }
    if (isTextContent) {
      int length = Math.min(50, this.httpRequestContent.content().readableBytes());
      byte[] prefixArray = new byte[length];
      this.httpRequestContent.content().getBytes(0, prefixArray);
      contentStringBuilder.append(new String(prefixArray));
      if (length == 50) {
        contentStringBuilder.append("...");
      }
    } else {
      contentStringBuilder.append("<bytes>");
    }
    String contentString = contentStringBuilder.toString();

    return String.format(
        "id=%d %s %s:%s hdr=%s body=%s",
        this.requestId,
        this.httpRequest.method(),
        this.getServiceName(),
        this.httpRequest.uri(),
        headerStringList,
        contentString);
  }

  @Override
  public String toString() {
    return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
  }

  public static XdnHttpRequest createFromString(String encodedRequest) {
    XdnHttpRequestProto.XdnHttpRequest decodedProto = decodeProto(encodedRequest);
    if (decodedProto == null) {
      return null;
    }

    HttpHeaders headers = new DefaultHttpHeaders(true);
    for (XdnHttpRequestProto.XdnHttpRequest.Header headerProto :
        decodedProto.getRequestHeadersList()) {
      headers.add(headerProto.getName(), headerProto.getValue());
    }

    ByteString requestBodyBytes = decodedProto.getRequestBody();
    ByteBuf requestBodyBuf =
        requestBodyBytes.isEmpty()
            ? Unpooled.EMPTY_BUFFER
            : Unpooled.wrappedBuffer(requestBodyBytes.asReadOnlyByteBuffer());

    HttpRequest httpRequest =
        new DefaultHttpRequest(
            getHttpVersionFromProto(decodedProto.getProtocolVersion()),
            HttpMethod.valueOf(decodedProto.getRequestMethod().name()),
            decodedProto.getRequestUri(),
            headers);

    HttpContent httpContent = new DefaultHttpContent(requestBodyBuf);

    HttpResponse httpResponse =
        decodedProto.hasResponse() ? buildHttpResponse(decodedProto.getResponse()) : null;

    XdnHttpRequest decodedRequest =
        new XdnHttpRequest(decodedProto.getRequestId(), httpRequest, httpContent, null, true);
    if (httpResponse != null) {
      decodedRequest.setHttpResponse(httpResponse);
    }
    return decodedRequest;
  }

  /** Returns {@code true} if the encoded request contains an embedded HTTP response. */
  public static boolean doesHasResponse(String encodedRequest) {
    Objects.requireNonNull(encodedRequest, "encodedRequest");
    byte[] raw = encodedRequest.getBytes(StandardCharsets.ISO_8859_1);
    if (!isValidPacketType(raw)) {
      return false;
    }

    try {
      CodedInputStream stream =
          CodedInputStream.newInstance(raw, Integer.BYTES, raw.length - Integer.BYTES);
      while (!stream.isAtEnd()) {
        int tag = stream.readTag();
        if (tag == 0) {
          break;
        }
        int fieldNumber = WireFormat.getTagFieldNumber(tag);
        if (fieldNumber == 7) { // response field
          return true;
        }
        stream.skipField(tag);
      }
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Failed to inspect encoded request: " + e.getMessage());
    }
    return false;
  }

  /**
   * Decodes only the HTTP response portion from a serialized request string. Returns {@code null}
   * if the input is invalid or contains no response.
   */
  public static HttpResponse parseHttpResponse(String encodedRequest) {
    Objects.requireNonNull(encodedRequest, "encodedRequest");
    byte[] raw = encodedRequest.getBytes(StandardCharsets.ISO_8859_1);
    if (!isValidPacketType(raw)) {
      return null;
    }

    try {
      CodedInputStream stream =
          CodedInputStream.newInstance(raw, Integer.BYTES, raw.length - Integer.BYTES);
      while (!stream.isAtEnd()) {
        int tag = stream.readTag();
        if (tag == 0) {
          break;
        }
        int fieldNumber = WireFormat.getTagFieldNumber(tag);
        if (fieldNumber == 7) {
          XdnHttpRequestProto.XdnHttpRequest.Response responseProto =
              stream.readMessage(
                  XdnHttpRequestProto.XdnHttpRequest.Response.parser(), EMPTY_REGISTRY);
          return buildHttpResponse(responseProto);
        }
        stream.skipField(tag);
      }
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Failed to parse response from encoded request: " + e.getMessage());
    }
    return null;
  }

  private static XdnHttpRequestProto.XdnHttpRequest decodeProto(String encodedRequest) {
    Objects.requireNonNull(encodedRequest, "encodedRequest");
    byte[] raw = encodedRequest.getBytes(StandardCharsets.ISO_8859_1);
    if (!isValidPacketType(raw)) {
      return null;
    }

    try {
      byte[] protoBytes = Arrays.copyOfRange(raw, Integer.BYTES, raw.length);
      return XdnHttpRequestProto.XdnHttpRequest.parseFrom(protoBytes);
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.WARNING, "Invalid protobuf bytes given: " + e.getMessage());
      return null;
    }
  }

  private static boolean isValidPacketType(byte[] raw) {
    if (raw.length < Integer.BYTES) {
      LOG.warning("Invalid encoded request length: " + raw.length);
      return false;
    }
    int packetType = ByteBuffer.wrap(raw, 0, Integer.BYTES).getInt();
    if (packetType != XdnRequestType.XDN_SERVICE_HTTP_REQUEST.getInt()) {
      LOG.warning("Unexpected packet type " + packetType + " while decoding HTTP request");
      return false;
    }
    return true;
  }

  private static HttpResponse buildHttpResponse(
      XdnHttpRequestProto.XdnHttpRequest.Response responseProto) {
    HttpHeaders responseHeaders = new DefaultHttpHeaders(true);
    for (XdnHttpRequestProto.XdnHttpRequest.Header headerProto : responseProto.getHeadersList()) {
      responseHeaders.add(headerProto.getName(), headerProto.getValue());
    }

    ByteString responseBodyBytes = responseProto.getResponseBody();
    ByteBuf responseBodyBuf =
        responseBodyBytes.isEmpty()
            ? Unpooled.EMPTY_BUFFER
            : Unpooled.wrappedBuffer(responseBodyBytes.asReadOnlyByteBuffer());

    return new DefaultFullHttpResponse(
        getHttpVersionFromProto(responseProto.getProtocolVersion()),
        HttpResponseStatus.valueOf(responseProto.getStatusCode()),
        responseBodyBuf,
        responseHeaders,
        new DefaultHttpHeaders(true));
  }

  /**
   * Extracts the XDN request id from a serialized HTTP request string without fully deserializing
   * it into {@link XdnHttpRequest}. Returns {@code null} if the input can not be parsed.
   */
  public static Long parseRequestIdQuickly(String encodedRequest) {
    Objects.requireNonNull(encodedRequest, "encodedRequest");
    byte[] raw = encodedRequest.getBytes(StandardCharsets.ISO_8859_1);
    if (raw.length < 4) {
      LOG.warning("Encoded request too short when parsing request id");
      return null;
    }

    int packetType = ByteBuffer.wrap(raw).getInt(0);
    if (packetType != XdnRequestType.XDN_SERVICE_HTTP_REQUEST.getInt()) {
      LOG.warning("Unexpected packet type when parsing request id: " + packetType);
      return null;
    }

    byte[] protoBytes = Arrays.copyOfRange(raw, 4, raw.length);
    com.google.protobuf.CodedInputStream cis =
        com.google.protobuf.CodedInputStream.newInstance(protoBytes);

    try {
      int tag;
      while ((tag = cis.readTag()) != 0) {
        int fieldNumber = tag >>> 3;
        if (fieldNumber == 1) {
          return cis.readInt64();
        }
        cis.skipField(tag);
      }
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.WARNING, "Failed to parse request id", e);
      return null;
    } catch (java.io.IOException e) {
      LOG.log(Level.WARNING, "IO error while parsing request id", e);
      return null;
    }

    LOG.warning("Request id field missing in encoded request");
    return null;
  }

  private static HttpVersion getHttpVersionFromProto(
      XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion version) {
    if (Objects.requireNonNull(version)
        == XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion.HTTP_1_0) {
      return HttpVersion.HTTP_1_0;
    }
    return HttpVersion.HTTP_1_1;
  }

  public boolean isCreatedFromString() {
    return isCreatedFromString;
  }
}
