package edu.umass.cs.xdn.request;

import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import java.nio.charset.StandardCharsets;
import java.util.Set;

public class XdnRequestParser implements AppRequestParser {

  @Override
  public Request getRequest(String encodedRequest) throws RequestParseException {
    XdnRequestType requestType = XdnRequest.getQuickPacketTypeFromEncodedPacket(encodedRequest);

    if (requestType == XdnRequestType.XDN_SERVICE_HTTP_REQUEST) {
      return XdnHttpRequest.createFromString(encodedRequest);
    }

    if (requestType == XdnRequestType.XDN_HTTP_REQUEST_BATCH) {
      return XdnHttpRequestBatch.createFromBytes(
          encodedRequest.getBytes(StandardCharsets.ISO_8859_1));
    }

    if (requestType == XdnRequestType.XDN_STOP_REQUEST) {
      return XdnStopRequest.createFromString(encodedRequest);
    }

    throw new RequestParseException(new Exception("Unknown XDN request type"));
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return Set.of(
        XdnRequestType.XDN_STOP_REQUEST,
        XdnRequestType.XDN_SERVICE_HTTP_REQUEST,
        XdnRequestType.XDN_HTTP_REQUEST_BATCH);
  }
}
