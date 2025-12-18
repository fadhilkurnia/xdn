package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.utils.IntegerPacketTypeMap;
import java.util.HashMap;

public enum XdnRequestType implements IntegerPacketType {
  XDN_SERVICE_HTTP_REQUEST(31300),
  XDN_HTTP_FORWARD_REQUEST(31301),
  XDN_HTTP_FORWARD_RESPONSE(31302),
  XDN_STATEDIFF_APPLY_REQUEST(31303),
  XDN_STOP_REQUEST(31304),
  XDN_GET_PROTOCOL_ROLE(31305),
  XDN_HTTP_REQUEST_BATCH(31306);

  private static final HashMap<Integer, XdnRequestType> numbers = new HashMap<>();

  /* ************** BEGIN static code block to ensure correct initialization *********** */
  static {
    for (XdnRequestType type : XdnRequestType.values()) {
      if (!XdnRequestType.numbers.containsKey(type.number)) {
        XdnRequestType.numbers.put(type.number, type);
      } else {
        assert (false) : "Duplicate or inconsistent enum type";
        throw new RuntimeException("Duplicate or inconsistent enum type");
      }
    }
  }

  /* *************** END static code block to ensure correct initialization *********** */

  private final int number;

  XdnRequestType(int t) {
    this.number = t;
  }

  @Override
  public int getInt() {
    return this.number;
  }

  public static final IntegerPacketTypeMap<XdnRequestType> intToType =
      new IntegerPacketTypeMap<>(XdnRequestType.values());
}
