package edu.umass.cs.xdn.request;

import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Objects;

public class XDNStopRequest extends XDNRequest implements ReconfigurableRequest {

    /**
     * All XDNStopRequest starts with "xdn:31304:" prefix.
     */
    public static final String SERIALIZED_PREFIX = String.format("%s%d:",
            XDNRequest.SERIALIZED_PREFIX, XDNRequestType.XDN_STOP_REQUEST.getInt());

    private final String serviceName;
    private final int reconfigurationEpochNumber;
    private final long requestID;

    public XDNStopRequest(String serviceName, int reconfigurationEpochNumber) {
        this(System.currentTimeMillis(), serviceName, reconfigurationEpochNumber);
    }

    private XDNStopRequest(long requestID, String serviceName, int reconfigurationEpochNumber) {
        this.serviceName = serviceName;
        this.reconfigurationEpochNumber = reconfigurationEpochNumber;
        this.requestID = requestID;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return XDNRequestType.XDN_STOP_REQUEST;
    }

    @Override
    public String getServiceName() {
        return this.serviceName;
    }

    @Override
    public int getEpochNumber() {
        return this.reconfigurationEpochNumber;
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
        try {
            JSONObject json = new JSONObject();
            json.put("sn", this.serviceName);
            json.put("id", this.requestID);
            json.put("rce", this.reconfigurationEpochNumber);
            return String.format("%s%s", SERIALIZED_PREFIX, json.toString());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        XDNStopRequest that = (XDNStopRequest) o;
        return reconfigurationEpochNumber == that.reconfigurationEpochNumber &&
                requestID == that.requestID &&
                Objects.equals(serviceName, that.serviceName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(serviceName, reconfigurationEpochNumber, requestID);
    }

    public static XDNStopRequest createFromString(String encodedRequest) {
        System.out.println(">>>>>>> XDNStopRequest createFromString: " + encodedRequest);
        if (encodedRequest == null || !encodedRequest.startsWith(SERIALIZED_PREFIX)) {
            return null;
        }
        encodedRequest = encodedRequest.substring(SERIALIZED_PREFIX.length());
        try {
            JSONObject json = new JSONObject(encodedRequest);
            String serviceName = json.getString("sn");
            int reconfigurationEpochNumber = json.getInt("rce");
            long requestID = json.getLong("id");
            return new XDNStopRequest(requestID, serviceName, reconfigurationEpochNumber);
        } catch (JSONException e) {
            return null;
        }
    }
}
