package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import java.util.Objects;
import org.json.JSONException;
import org.json.JSONObject;

@Deprecated
public class XDNHttpForwardRequest extends XdnRequest {

  /** All the serialized XDNHttpRequest starts with "xdn:31301:" */
  public static final String SERIALIZED_PREFIX =
      String.format(
          "%s%d:", XdnRequest.SERIALIZED_PREFIX, XdnRequestType.XDN_HTTP_FORWARD_REQUEST.getInt());

  private final XdnJsonHttpRequest request;
  private final String entryNodeID;

  public XDNHttpForwardRequest(XdnJsonHttpRequest request, String entryNodeID) {
    this.request = request;
    this.entryNodeID = entryNodeID;
  }

  @Override
  public IntegerPacketType getRequestType() {
    return XdnRequestType.XDN_HTTP_FORWARD_REQUEST;
  }

  @Override
  public String getServiceName() {
    return request.getServiceName();
  }

  @Override
  public long getRequestID() {
    return 0;
  }

  @Override
  public boolean needsCoordination() {
    return true;
  }

  public XdnJsonHttpRequest getRequest() {
    return request;
  }

  public String getEntryNodeID() {
    return entryNodeID;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    XDNHttpForwardRequest that = (XDNHttpForwardRequest) o;
    return Objects.equals(request, that.request);
  }

  @Override
  public int hashCode() {
    return Objects.hash(request);
  }

  @Override
  public String toString() {
    try {
      JSONObject json = new JSONObject();
      json.put("r", this.request.toString());
      json.put("e", this.entryNodeID);
      return SERIALIZED_PREFIX + json.toString();
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }

  public static XDNHttpForwardRequest createFromString(String stringified) {
    if (stringified == null || !stringified.startsWith(SERIALIZED_PREFIX)) {
      return null;
    }
    stringified = stringified.substring(SERIALIZED_PREFIX.length());
    try {
      JSONObject json = new JSONObject(stringified);
      XdnJsonHttpRequest httpRequest = XdnJsonHttpRequest.createFromString(json.getString("r"));
      String entryNodeID = json.getString("e");
      return new XDNHttpForwardRequest(httpRequest, entryNodeID);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
  }
}
