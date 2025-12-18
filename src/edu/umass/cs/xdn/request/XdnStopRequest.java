package edu.umass.cs.xdn.request;

import com.google.protobuf.InvalidProtocolBufferException;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.xdn.proto.XdnStopRequestProto;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XdnStopRequest extends XdnRequest implements ReconfigurableRequest, Byteable {

  private final long requestID;
  private final String serviceName;
  private final int placementEpochNumber;

  public XdnStopRequest(String serviceName, int placementEpochNumber) {
    this(Math.abs(UUID.randomUUID().getLeastSignificantBits()), serviceName, placementEpochNumber);
  }

  private XdnStopRequest(long requestID, String serviceName, int placementEpochNumber) {
    this.requestID = requestID;
    this.serviceName = serviceName;
    this.placementEpochNumber = placementEpochNumber;
  }

  @Override
  public IntegerPacketType getRequestType() {
    return XdnRequestType.XDN_STOP_REQUEST;
  }

  @Override
  public String getServiceName() {
    return this.serviceName;
  }

  @Override
  public int getEpochNumber() {
    return this.placementEpochNumber;
  }

  @Override
  public boolean isStop() {
    return true;
  }

  @Override
  public long getRequestID() {
    return this.requestID;
  }

  @Override
  public boolean needsCoordination() {
    return true;
  }

  @Override
  public String toString() {
    return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
  }

  @Override
  public byte[] toBytes() {
    ByteArrayOutputStream output = new ByteArrayOutputStream();

    // Serialize the packet type
    int packetType = this.getRequestType().getInt();
    byte[] encodedHeader = ByteBuffer.allocate(4).putInt(packetType).array();

    // Serialize the request
    XdnStopRequestProto.XdnStopRequest.Builder protoBuilder =
        XdnStopRequestProto.XdnStopRequest.newBuilder()
            .setRequestId(this.requestID)
            .setServiceName(this.serviceName)
            .setReconfigurationEpoch(this.placementEpochNumber);

    // Serialize the packetType in the header, followed by the protobuf
    output.writeBytes(encodedHeader);
    output.writeBytes(protoBuilder.build().toByteArray());
    return output.toByteArray();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    XdnStopRequest that = (XdnStopRequest) o;
    return placementEpochNumber == that.placementEpochNumber
        && requestID == that.requestID
        && Objects.equals(serviceName, that.serviceName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(serviceName, placementEpochNumber, requestID);
  }

  public static XdnStopRequest createFromString(String encodedRequest) {
    assert encodedRequest != null;
    byte[] encoded = encodedRequest.getBytes(StandardCharsets.ISO_8859_1);

    // Decode the packet type
    assert encoded.length >= 4;
    ByteBuffer headerBuffer = ByteBuffer.wrap(encoded);
    int packetType = headerBuffer.getInt(0);
    assert packetType == XdnRequestType.XDN_STOP_REQUEST.getInt()
        : "invalid packet header: " + packetType;
    encoded = Arrays.copyOfRange(encoded, 4, encoded.length);

    // Decode the bytes into Proto
    XdnStopRequestProto.XdnStopRequest decodedProto = null;

    try {
      decodedProto = XdnStopRequestProto.XdnStopRequest.parseFrom(encoded);
    } catch (InvalidProtocolBufferException e) {
      Logger.getGlobal().log(Level.WARNING, "Invalid protobuf bytes given: " + e.getMessage());
      return null;
    }

    return new XdnStopRequest(
        decodedProto.getRequestId(),
        decodedProto.getServiceName(),
        decodedProto.getReconfigurationEpoch());
  }
}
