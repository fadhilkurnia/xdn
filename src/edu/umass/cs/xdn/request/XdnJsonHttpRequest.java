package edu.umass.cs.xdn.request;

import static io.netty.handler.codec.http.HttpMethod.GET;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import edu.umass.cs.clientcentric.VectorTimestamp;
import edu.umass.cs.clientcentric.interfaces.TimestampedRequest;
import edu.umass.cs.clientcentric.interfaces.TimestampedResponse;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;
import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;
import edu.umass.cs.xdn.proto.XdnHttpRequestProto;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.DefaultCookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.util.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

@Deprecated
@RunWith(Enclosed.class)
public class XdnJsonHttpRequest extends XdnRequest
    implements ClientRequest, BehavioralRequest, TimestampedRequest, TimestampedResponse {

  /** All the serialized XDNHttpRequest starts with "xdn:31300:" */
  public static final String SERIALIZED_PREFIX =
      String.format(
          "%s%d:", XdnRequest.SERIALIZED_PREFIX, XdnRequestType.XDN_SERVICE_HTTP_REQUEST.getInt());

  private static final String XDN_HTTP_REQUEST_ID_HEADER = "XDN-Request-ID";

  private final long requestID;
  private final String serviceName;
  private final HttpRequest httpRequest;
  private final HttpContent httpRequestContent;
  private HttpResponse httpResponse;

  // for BehavioralRequest interface
  private final Set<RequestBehaviorType> behaviors;

  public XdnJsonHttpRequest(
      String serviceName, HttpRequest httpRequest, HttpContent httpRequestContent) {
    assert serviceName != null && httpRequest != null && httpRequestContent != null;
    this.serviceName = serviceName;
    this.httpRequest = httpRequest;
    this.httpRequestContent = httpRequestContent;

    Long inferredRequestID = XdnJsonHttpRequest.inferRequestID(this.httpRequest);
    this.requestID = Objects.requireNonNullElseGet(inferredRequestID, System::currentTimeMillis);
    this.httpRequest.headers().set(XDN_HTTP_REQUEST_ID_HEADER, this.requestID);
    this.behaviors = this.inferRequestBehaviors(httpRequest);
  }

  // TODO: complete this implementation, possibly need to infer from the service properties.
  //  Currently we assume all Http Get/Options/Head request is read-only, while others
  //  are write-only.
  private Set<RequestBehaviorType> inferRequestBehaviors(HttpRequest httpRequest) {
    Set<RequestBehaviorType> types = new HashSet<>();
    HttpMethod httpMethod = httpRequest.method();
    if (httpMethod == GET || httpMethod == HttpMethod.HEAD || httpMethod == HttpMethod.OPTIONS) {
      types.add(RequestBehaviorType.READ_ONLY);
    } else {
      types.add(RequestBehaviorType.WRITE_ONLY);
    }
    return types;
  }

  // The service's name is encoded in the request header.
  // For example, the service name is 'hello' for these cases:
  // - request with "XDN: hello" in the header.
  // - request with "Host: hello.xdnapp.com:80" in the header.
  // return null if service's name cannot be inferred
  public static String inferServiceName(HttpRequest httpRequest) {
    assert httpRequest != null;

    // case-1: encoded in the XDN header (e.g., XDN: alice-book-catalog)
    String xdnHeader = httpRequest.headers().get("XDN");
    if (xdnHeader != null && !xdnHeader.isEmpty()) {
      return xdnHeader;
    }

    // case-2: encoded in the required Host header (e.g., Host: alice-book-catalog.xdnapp.com)
    String hostStr = httpRequest.headers().get(HttpHeaderNames.HOST);
    if (hostStr == null || hostStr.isEmpty()) {
      return null;
    }
    String reqServiceName = hostStr.split("\\.")[0];
    if (!reqServiceName.isEmpty()) {
      return reqServiceName;
    }

    return null;
  }

  // In general, we infer the HTTP request ID based on these headers:
  // (1) `ETag`, (2) `X-Request-ID`, or (3) `XDN-Request-ID`, in that order.
  // If the request does not contain those header, null will be returned.
  // Later, the newly generated request ID should be stored in the header
  // with `XDN-Request-ID` key (check the constructor of XDNHttpRequest).
  public static Long inferRequestID(HttpRequest httpRequest) {
    assert httpRequest != null;

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
    return requestID;
  }

  @Override
  public boolean needsCoordination() {
    return true;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    XdnJsonHttpRequest that = (XdnJsonHttpRequest) o;
    return this.serviceName.equals(that.serviceName)
        && this.httpRequest.equals(that.httpRequest)
        && this.httpRequestContent.equals(that.httpRequestContent);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.serviceName, this.httpRequest, this.httpRequestContent);
  }

  public HttpRequest getHttpRequest() {
    return httpRequest;
  }

  public HttpContent getHttpRequestContent() {
    return httpRequestContent;
  }

  public HttpResponse getHttpResponse() {
    return httpResponse;
  }

  public void setHttpResponse(HttpResponse httpResponse) {
    this.httpResponse = httpResponse;
  }

  @Override
  public String toString() {
    try {
      JSONObject json = new JSONObject();
      json.put("protocolVersion", httpRequest.protocolVersion().toString());
      json.put("method", httpRequest.method().toString());
      json.put("uri", httpRequest.uri());
      JSONArray headerJsonArray = new JSONArray();
      Iterator<Map.Entry<String, String>> it = httpRequest.headers().iteratorAsString();
      while (it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        headerJsonArray.put(String.format("%s:%s", entry.getKey(), entry.getValue()));
      }
      json.put("headers", headerJsonArray);
      json.put("content", httpRequestContent.content().toString(StandardCharsets.ISO_8859_1));

      if (httpResponse != null) {
        json.put("response", serializedHttpResponse(httpResponse));
      }

      return SERIALIZED_PREFIX + json.toString();
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  private String serializedHttpResponse(HttpResponse response) {
    assert response != null;
    JSONObject json = new JSONObject();

    try {
      // serialize version
      json.put("version", response.protocolVersion().toString());

      // serialize status
      json.put("status", response.status().code());

      // serialize response header
      JSONArray headerJsonArray = new JSONArray();
      Iterator<Map.Entry<String, String>> it = response.headers().iteratorAsString();
      while (it.hasNext()) {
        Map.Entry<String, String> entry = it.next();
        headerJsonArray.put(String.format("%s:%s", entry.getKey(), entry.getValue()));
      }
      json.put("headers", headerJsonArray);

      // serialize response body
      assert response instanceof DefaultFullHttpResponse;
      DefaultFullHttpResponse fullHttpResponse = (DefaultFullHttpResponse) response;
      json.put("body", fullHttpResponse.content().toString(StandardCharsets.ISO_8859_1));

    } catch (JSONException e) {
      throw new RuntimeException(e);
    }

    return json.toString();
  }

  private static HttpResponse deserializedHttpResponse(String response) {
    assert response != null;

    try {
      JSONObject json = new JSONObject(response);
      String versionString = json.getString("version");
      int statusCode = json.getInt("status");
      JSONArray headerArray = json.getJSONArray("headers");
      String bodyString = json.getString("body");

      // copy http response headers, if any
      HttpHeaders headers = new DefaultHttpHeaders();
      for (int i = 0; i < headerArray.length(); i++) {
        String[] raw = headerArray.getString(i).split(":");
        headers.add(raw[0], raw[1]);
      }

      // by default, we have an empty header trailing for the response
      HttpHeaders trailingHeaders = new DefaultHttpHeaders();

      return new DefaultFullHttpResponse(
          HttpVersion.valueOf(versionString),
          HttpResponseStatus.valueOf(statusCode),
          Unpooled.copiedBuffer(bodyString.getBytes(StandardCharsets.ISO_8859_1)),
          headers,
          trailingHeaders);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  public static XdnJsonHttpRequest createFromString(String stringified) {
    if (stringified == null || !stringified.startsWith(SERIALIZED_PREFIX)) {
      return null;
    }
    stringified = stringified.substring(SERIALIZED_PREFIX.length());
    try {
      JSONObject json = new JSONObject(stringified);

      // prepare the deserialized variables
      String httpProtocolVersion = json.getString("protocolVersion");
      String httpMethod = json.getString("method");
      String httpURI = json.getString("uri");
      String httpContent = json.getString("content");
      String httpResponse = json.has("response") ? json.getString("response") : null;

      // handle array of header
      JSONArray headerJSONArr = json.getJSONArray("headers");
      HttpHeaders httpHeaders = new DefaultHttpHeaders();
      for (int i = 0; i < headerJSONArr.length(); i++) {
        String headerEntry = headerJSONArr.getString(i);
        String headerKey = headerEntry.split(":")[0];
        String headerVal = headerEntry.substring(headerKey.length() + 1);
        httpHeaders.add(headerKey, headerVal);
      }

      // init request and content, then combine them into XDNHttpRequest
      HttpRequest req =
          new DefaultHttpRequest(
              HttpVersion.valueOf(httpProtocolVersion),
              HttpMethod.valueOf(httpMethod),
              httpURI,
              httpHeaders);
      HttpContent reqContent =
          new DefaultHttpContent(Unpooled.copiedBuffer(httpContent, StandardCharsets.ISO_8859_1));
      String serviceName = XdnJsonHttpRequest.inferServiceName(req);
      XdnJsonHttpRequest xdnHttpRequest = new XdnJsonHttpRequest(serviceName, req, reqContent);

      // handle response, if any
      if (httpResponse != null) {
        xdnHttpRequest.httpResponse = deserializedHttpResponse(httpResponse);
      }

      return xdnHttpRequest;
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ClientRequest getResponse() {
    return this;
  }

  @Override
  public Set<RequestBehaviorType> getBehaviors() {
    return this.behaviors;
  }

  @Override
  public VectorTimestamp getLastTimestamp(String timestampName) {
    assert timestampName.equals("R") || timestampName.equals("W");
    assert this.httpRequest != null;

    // validate if we have cookie in the header
    String cookieRaw =
        this.httpRequest.headers() != null ? this.httpRequest.headers().get("Cookie") : null;
    if (cookieRaw == null) {
      return null;
    }

    // decode the cookie
    Set<Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieRaw);
    if (cookies.isEmpty()) {
      return null;
    }

    // decode the timestamp in the cookie
    final String timestampCookieKey = "XDN-CC-TS-" + timestampName;
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
    assert this.httpResponse != null;
    assert this.httpResponse.headers() != null;

    final String timestampCookieKey = "XDN-CC-TS-" + timestampName;
    Cookie cookie = new DefaultCookie(timestampCookieKey, timestamp.toString());
    cookie.setPath("/");
    cookie.setHttpOnly(true);
    this.httpResponse.headers().add("Set-Cookie", ServerCookieEncoder.STRICT.encode(cookie));
  }

  public static class TestXdnHttpRequest {
    @Test
    public void TestXdnHttpRequestSerializationDeserialization() {
      XdnJsonHttpRequest dummyXdnJsonHttpRequest = createDummyTestRequest();

      // the created XDNRequest from string should equal to the
      // original XDNRequest being used to generate the string.
      XdnJsonHttpRequest deserializedXDNRequest =
          XdnJsonHttpRequest.createFromString(dummyXdnJsonHttpRequest.toString());
      System.out.println(dummyXdnJsonHttpRequest);
      System.out.println(deserializedXDNRequest);
      assert deserializedXDNRequest != null : "deserialized XDNRequest is null";
      assert Objects.equals(dummyXdnJsonHttpRequest, deserializedXDNRequest)
          : "deserialized XDNRequest is different";
    }

    @Test
    public void TestXdnHttpRequestSetResponse() {
      XdnJsonHttpRequest dummyXdnJsonHttpRequest = createDummyTestRequest();
      dummyXdnJsonHttpRequest.setHttpResponse(createDummyTestResponse());

      Request requestWithResponse = dummyXdnJsonHttpRequest.getResponse();
      assert requestWithResponse.equals(dummyXdnJsonHttpRequest)
          : "response must be embedded into the request";
    }

    @Test
    public void TestSerializationPerformance() {
      int repetitions = 1000000;

      HttpRequest dummyHttpRequest =
          new DefaultHttpRequest(
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
                  .add("XDN", "dummy")
                  .add("Random-Char", "=,;:\"'`")
                  .add("Content-Type", "multipart/mixed; boundary=gc0p4Jq0MYt08"));
      HttpContent dummyHttpContent =
          new DefaultHttpContent(
              Unpooled.copiedBuffer("somestringcontent".getBytes(StandardCharsets.UTF_8)));

      System.out.println(">>> serialization");
      int byteLen = 0;
      long startTime = System.nanoTime();
      for (int i = 0; i < repetitions; i++) {
        byte[] encoded = createHttpRequestProto(dummyHttpRequest, dummyHttpContent).toByteArray();
        byteLen = encoded.length;
      }
      long estimatedTimeProto = (System.nanoTime() - startTime) / repetitions;
      System.out.println(">>> protobuf encoded size: " + byteLen + " bytes");

      byteLen = 0;
      startTime = System.nanoTime();
      for (int i = 0; i < repetitions; i++) {
        byte[] encoded = createXdnHttpRequest(dummyHttpRequest, dummyHttpContent).toBytes();
        byteLen = encoded.length;
      }
      long estimatedTimeJson = (System.nanoTime() - startTime) / repetitions;
      System.out.println(">>> json encoded size: " + byteLen + " bytes");

      System.out.println("-----------");
      System.out.println(">>> protobuf estimated time : " + estimatedTimeProto + " ns");
      System.out.println(">>> json estimated time     : " + estimatedTimeJson + " ns");
    }

    @Test
    public void TestDeserializationPerformance() {
      int repetitions = 1000000;

      HttpRequest dummyHttpRequest =
          new DefaultHttpRequest(
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
                  .add("XDN", "dummy")
                  .add("Random-Char", "=,;:\"'`")
                  .add("Content-Type", "multipart/mixed; boundary=gc0p4Jq0MYt08"));
      HttpContent dummyHttpContent =
          new DefaultHttpContent(
              Unpooled.copiedBuffer("somestringcontent".getBytes(StandardCharsets.UTF_8)));

      byte[] encodedProto =
          createHttpRequestProto(dummyHttpRequest, dummyHttpContent).toByteArray();
      byte[] encodedJson = createXdnHttpRequest(dummyHttpRequest, dummyHttpContent).toBytes();

      System.out.println(">>> deserialization");
      long startTime = System.nanoTime();
      XdnHttpRequestProto.XdnHttpRequest resultProto = null;
      for (int i = 0; i < repetitions; i++) {
        try {
          resultProto = XdnHttpRequestProto.XdnHttpRequest.parseFrom(encodedProto);
        } catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
      }
      assert resultProto != null;
      long estimatedTimeProto = (System.nanoTime() - startTime) / repetitions;
      System.out.println(">>> protobuf result: " + resultProto);

      XdnJsonHttpRequest resultJson = null;
      startTime = System.nanoTime();
      for (int i = 0; i < repetitions; i++) {
        resultJson =
            XdnJsonHttpRequest.createFromString(
                new String(encodedJson, StandardCharsets.ISO_8859_1));
      }
      long estimatedTimeJson = (System.nanoTime() - startTime) / repetitions;
      System.out.println(">>> json result: " + resultJson);

      System.out.println("-----------");
      System.out.println(">>> protobuf estimated time : " + estimatedTimeProto + " ns");
      System.out.println(">>> json estimated time     : " + estimatedTimeJson + " ns");
    }

    private static XdnHttpRequestProto.XdnHttpRequest createHttpRequestProto(
        HttpRequest req, HttpContent content) {
      // convert version
      XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion version;
      if (req.protocolVersion().equals(HttpVersion.HTTP_1_0)) {
        version = XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion.HTTP_1_0;
      } else {
        version = XdnHttpRequestProto.XdnHttpRequest.HttpProtocolVersion.HTTP_1_1;
      }

      // convert method
      XdnHttpRequestProto.XdnHttpRequest.HttpMethod method;
      switch (req.method().toString()) {
        case "GET":
          {
            method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.GET;
            break;
          }
        case "HEAD":
          {
            method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.HEAD;
            break;
          }
        case "POST":
          {
            method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.POST;
            break;
          }
        case "PUT":
          {
            method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.PUT;
            break;
          }
        case "DELETE":
          {
            method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.DELETE;
            break;
          }
        case "CONNECT":
          {
            method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.CONNECT;
            break;
          }
        case "OPTIONS":
          {
            method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.OPTIONS;
            break;
          }
        case "PATCH":
          {
            method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.PATCH;
            break;
          }
        default:
          {
            method = XdnHttpRequestProto.XdnHttpRequest.HttpMethod.POST;
          }
      }

      // get the request uri
      String uri = req.uri();

      // parse the content
      byte[] requestBody = content.content().array();
      ByteString requestBodyBytes = ByteString.copyFrom(requestBody);

      // parse header
      Iterator<Map.Entry<String, String>> it = req.headers().iteratorAsString();
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

      XdnHttpRequestProto.XdnHttpRequest.Builder protoBuilder =
          XdnHttpRequestProto.XdnHttpRequest.newBuilder()
              .setProtocolVersion(version)
              .setRequestMethod(method)
              .setRequestUri(uri)
              .addAllRequestHeaders(headerList)
              .setRequestBody(requestBodyBytes);

      return protoBuilder.build();
    }

    public static XdnJsonHttpRequest createXdnHttpRequest(HttpRequest req, HttpContent content) {
      return new XdnJsonHttpRequest("dummy", req, content);
    }

    public static XdnJsonHttpRequest createDummyTestRequest() {
      String serviceName = "dummyServiceName";
      HttpRequest dummyHttpRequest =
          new DefaultHttpRequest(
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
      HttpContent dummyHttpContent =
          new DefaultHttpContent(
              Unpooled.copiedBuffer("somestringcontent".getBytes(StandardCharsets.UTF_8)));

      return new XdnJsonHttpRequest(serviceName, dummyHttpRequest, dummyHttpContent);
    }

    private HttpResponse createDummyTestResponse() {
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      sw.write("http request is successfully executed\n");
      return new DefaultFullHttpResponse(
          HttpVersion.HTTP_1_1,
          HttpResponseStatus.OK,
          Unpooled.copiedBuffer(sw.toString().getBytes()));
    }
  }
}
