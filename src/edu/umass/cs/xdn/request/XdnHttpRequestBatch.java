package edu.umass.cs.xdn.request;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.Byteable;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.logging.Logger;
import java.util.zip.DataFormatException;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.Inflater;

/**
 * Groups multiple {@link XdnHttpRequest}s into a single batch request so the batch can be
 * replicated and coordinated through the usual gigapaxos pipeline. The individual requests are
 * serialized, length-prefixed, and then the collective payload is compressed before being written
 * on the wire to save bandwidth.
 *
 * <p>Serialization layout (network byte order):
 *
 * <pre>
 *   int    packetType (= {@link XdnRequestType#XDN_HTTP_REQUEST_BATCH})
 *   long   batchRequestId
 *   int    serviceNameLength
 *   bytes  serviceName (ISO-8859-1)
 *   int    uncompressedLength
 *   int    compressedLength
 *   bytes  compressedPayload
 * </pre>
 *
 * The compressed payload expands to:
 *
 * <pre>
 *   int    numRequests
 *   repeat numRequests times {
 *       int    requestLength
 *       bytes  serialized XdnHttpRequest (same form as {@link XdnHttpRequest#toBytes()})
 *   }
 * </pre>
 */
public final class XdnHttpRequestBatch extends XdnRequest implements ClientRequest, Byteable {

  private static final Logger LOG = Logger.getLogger(XdnHttpRequestBatch.class.getName());

  private final long requestId;
  private final String serviceName;
  private final XdnHttpRequest[] requests;

  // A helper flag that is true iff this instance is created via createFromBytes().
  // The flag is particularly useful to decide whether to release the reference-counted
  // httpResponses. In XDN, the XdnHttpRequestBatch instance is created via createFromBytes()
  // in non entry-replica node, and thus we can discard and release the httpResponses immediately.
  private final boolean isCreatedFromBytes;

  public XdnHttpRequestBatch(List<XdnHttpRequest> requests) {
    this(generateRequestId(), null, requests.toArray(new XdnHttpRequest[0]), true, false);
  }

  private XdnHttpRequestBatch(
      long requestId,
      String serviceName,
      XdnHttpRequest[] requests,
      boolean defensiveCopy,
      boolean isCreatedFromBytes) {
    Objects.requireNonNull(requests, "requests");
    if (requests.length == 0) {
      throw new IllegalArgumentException("Batch must contain at least one request");
    }

    this.requests = defensiveCopy ? Arrays.copyOf(requests, requests.length) : requests;
    validateRequests(this.requests);

    String resolvedService = determineServiceName(this.requests);
    if (serviceName != null && !serviceName.equals(resolvedService)) {
      throw new IllegalArgumentException("All requests must target the same service name");
    }

    this.requestId = requestId;
    this.serviceName = serviceName != null ? serviceName : resolvedService;
    assert this.serviceName != null : "Service name is expected";

    this.isCreatedFromBytes = isCreatedFromBytes;
  }

  private static long generateRequestId() {
    return Math.abs(UUID.randomUUID().getLeastSignificantBits());
  }

  private static void validateRequests(XdnHttpRequest[] requests) {
    for (int i = 0; i < requests.length; i++) {
      Objects.requireNonNull(requests[i], "requests[" + i + "]");
    }
  }

  private static String determineServiceName(XdnHttpRequest[] requests) {
    String service = Objects.requireNonNull(requests[0].getServiceName(), "serviceName");
    for (int i = 1; i < requests.length; i++) {
      String other = Objects.requireNonNull(requests[i].getServiceName(), "serviceName");
      if (!service.equals(other)) {
        throw new IllegalArgumentException(
            "All requests in the batch must share the same service name");
      }
    }
    return service;
  }

  public int size() {
    return this.requests.length;
  }

  public boolean isEmpty() {
    return this.requests.length == 0;
  }

  public XdnHttpRequest[] getRequests() {
    return Arrays.copyOf(this.requests, this.requests.length);
  }

  public List<XdnHttpRequest> getRequestList() {
    return Collections.unmodifiableList(Arrays.asList(this.requests));
  }

  @Override
  public IntegerPacketType getRequestType() {
    return XdnRequestType.XDN_HTTP_REQUEST_BATCH;
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
  public byte[] toBytes() {
    byte[][] encodedRequests = new byte[this.requests.length][];

    int rawSize = Integer.BYTES; // number of requests
    for (int i = 0; i < this.requests.length; i++) {
      byte[] encoded = this.requests[i].toBytes();
      Objects.requireNonNull(encoded, "Request " + i + " produced null serialization");
      encodedRequests[i] = encoded;
      rawSize += Integer.BYTES + encoded.length;
    }

    ByteBuffer rawBuffer = ByteBuffer.allocate(rawSize);
    rawBuffer.putInt(this.requests.length);
    for (byte[] encodedRequest : encodedRequests) {
      rawBuffer.putInt(encodedRequest.length);
      rawBuffer.put(encodedRequest);
    }
    byte[] rawBytes = rawBuffer.array();

    byte[] compressedBytes = compress(rawBytes);
    byte[] serviceNameBytes = this.serviceName.getBytes(StandardCharsets.ISO_8859_1);

    int totalSize =
        Integer.BYTES // packet type
            + Long.BYTES // request id
            + Integer.BYTES
            + serviceNameBytes.length // service name length + bytes
            + Integer.BYTES // uncompressed length
            + Integer.BYTES // compressed length
            + compressedBytes.length;

    ByteBuffer buffer = ByteBuffer.allocate(totalSize);
    buffer.putInt(XdnRequestType.XDN_HTTP_REQUEST_BATCH.getInt());
    buffer.putLong(this.requestId);
    buffer.putInt(serviceNameBytes.length);
    buffer.put(serviceNameBytes);
    buffer.putInt(rawBytes.length);
    buffer.putInt(compressedBytes.length);
    buffer.put(compressedBytes);
    return buffer.array();
  }

  public static XdnHttpRequestBatch createFromBytes(byte[] bytes) {
    Objects.requireNonNull(bytes, "bytes");
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    if (buffer.remaining()
        < Integer.BYTES + Long.BYTES + Integer.BYTES + Integer.BYTES + Integer.BYTES) {
      throw new IllegalArgumentException("Malformed batch payload: insufficient header length");
    }

    int packetType = buffer.getInt();
    if (packetType != XdnRequestType.XDN_HTTP_REQUEST_BATCH.getInt()) {
      throw new IllegalArgumentException("Unexpected packet type: " + packetType);
    }

    long requestId = buffer.getLong();

    int serviceNameLength = buffer.getInt();
    if (serviceNameLength < 0
        || buffer.remaining() < serviceNameLength + Integer.BYTES + Integer.BYTES) {
      throw new IllegalArgumentException("Malformed batch payload: invalid service name length");
    }

    byte[] serviceNameBytes = new byte[serviceNameLength];
    buffer.get(serviceNameBytes);
    String serviceName = new String(serviceNameBytes, StandardCharsets.ISO_8859_1);

    int uncompressedLength = buffer.getInt();
    int compressedLength = buffer.getInt();
    if (uncompressedLength < 0 || compressedLength < 0 || buffer.remaining() < compressedLength) {
      throw new IllegalArgumentException("Malformed batch payload: invalid compression lengths");
    }

    byte[] compressedBytes = new byte[compressedLength];
    buffer.get(compressedBytes);

    byte[] rawBytes = decompress(compressedBytes, uncompressedLength);
    ByteBuffer rawBuffer = ByteBuffer.wrap(rawBytes);

    if (rawBuffer.remaining() < Integer.BYTES) {
      throw new IllegalArgumentException("Malformed batch payload: truncated request count");
    }
    int count = rawBuffer.getInt();
    if (count < 0) {
      throw new IllegalArgumentException("Invalid request count: " + count);
    }

    XdnHttpRequest[] decoded = new XdnHttpRequest[count];
    for (int i = 0; i < count; i++) {
      if (rawBuffer.remaining() < Integer.BYTES) {
        throw new IllegalArgumentException("Malformed batch payload: truncated request length");
      }
      int length = rawBuffer.getInt();
      if (length < 0 || rawBuffer.remaining() < length) {
        throw new IllegalArgumentException(
            "Malformed batch payload: invalid request size " + length);
      }
      byte[] requestBytes = new byte[length];
      rawBuffer.get(requestBytes);
      decoded[i] = decodeRequest(requestBytes, serviceName);
    }

    return new XdnHttpRequestBatch(requestId, serviceName, decoded, true, true);
  }

  private static XdnHttpRequest decodeRequest(byte[] requestBytes, String expectedService) {
    XdnHttpRequest request =
        XdnHttpRequest.createFromString(new String(requestBytes, StandardCharsets.ISO_8859_1));
    if (request == null) {
      throw new IllegalArgumentException("Unable to decode XdnHttpRequest from provided bytes");
    }
    if (!expectedService.equals(request.getServiceName())) {
      throw new IllegalArgumentException("Mismatched service name in batched request");
    }
    return request;
  }

  @Override
  public String toString() {
    return new String(this.toBytes(), StandardCharsets.ISO_8859_1);
  }

  private static byte[] compress(byte[] rawBytes) {
    Objects.requireNonNull(rawBytes, "rawBytes");
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DeflaterOutputStream deflaterStream =
            new DeflaterOutputStream(baos, new Deflater(Deflater.BEST_SPEED))) {
      deflaterStream.write(rawBytes);
      deflaterStream.finish();
      return baos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("Failed to compress batch payload", e);
    }
  }

  private static byte[] decompress(byte[] compressedBytes, int expectedLength) {
    Objects.requireNonNull(compressedBytes, "compressedBytes");
    Inflater inflater = new Inflater();
    inflater.setInput(compressedBytes);
    byte[] result = new byte[expectedLength];
    try {
      int actualLength = inflater.inflate(result);
      if (actualLength != expectedLength) {
        throw new IllegalArgumentException(
            "Decompressed batch length mismatch: expected "
                + expectedLength
                + " but got "
                + actualLength);
      }
      if (!inflater.finished()) {
        throw new IllegalArgumentException("Decompressed batch payload has trailing data");
      }
      return result;
    } catch (DataFormatException e) {
      throw new IllegalArgumentException("Failed to decompress batch payload", e);
    } finally {
      inflater.end();
    }
  }

  @Override
  public ClientRequest getResponse() {
    return this;
  }

  public static Long parseRequestIdQuickly(String encodedBatch) {
    if (encodedBatch == null) {
      return null;
    }
    byte[] bytes = encodedBatch.getBytes(StandardCharsets.ISO_8859_1);
    if (bytes.length < Integer.BYTES + Long.BYTES) {
      LOG.warning("Encoded batch too short when parsing request id");
      return null;
    }
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int packetType = buffer.getInt();
    if (packetType != XdnRequestType.XDN_HTTP_REQUEST_BATCH.getInt()) {
      LOG.warning("Unexpected packet type when parsing batch id: " + packetType);
      return null;
    }
    return buffer.getLong();
  }

  public boolean isCreatedFromBytes() {
    return isCreatedFromBytes;
  }
}
