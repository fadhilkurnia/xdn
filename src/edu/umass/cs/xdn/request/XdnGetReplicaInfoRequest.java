package edu.umass.cs.xdn.request;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class XdnGetReplicaInfoRequest extends XdnRequest implements ClientRequest {

    private final long requestId;
    private final String serviceName;

    // fields for the success response
    String replicaId;
    String protocolName;
    String requestedConsistencyModel;
    String offeredConsistencyModel;
    String protocolRoleName; // e.g., primary, backup, coordinator, replica
    List<String> containerId;
    List<String> createdAtInfo;
    private List<String> containerStatus;

    // TODO: list of container images, deterministic metadata, statedir, request behaviors.

    // fields for the error response
    Integer httpErrorCode;
    String errorMessage;

    public XdnGetReplicaInfoRequest(String serviceName) {
        this.requestId = UUID.randomUUID().getLeastSignificantBits();
        this.serviceName = serviceName;
    }

    @Override
    public IntegerPacketType getRequestType() {
        return XdnRequestType.XDN_GET_PROTOCOL_ROLE;
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
    public ClientRequest getResponse() {
        return this;
    }

    public void setResponse(String replicaId, String protocolName, String requestedConsistencyModel,
                            String offeredConsistencyModel, String roleName) {
        this.replicaId = replicaId;
        this.protocolName = protocolName;
        this.requestedConsistencyModel = requestedConsistencyModel;
        this.offeredConsistencyModel = offeredConsistencyModel;
        this.protocolRoleName = roleName;
    }

    /**
     * Sets container metadata information for this service replica
     */
    public void setContainerMetadata(List<String> containerIds,
                                     List<String> createdAtInfo,
                                     List<String> containerStatus) {
        this.containerId = containerIds;
        this.createdAtInfo = createdAtInfo;
        this.containerStatus = containerStatus;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Integer getHttpErrorCode() {
        return httpErrorCode;
    }

    public void setHttpErrorCode(Integer httpErrorCode) {
        this.httpErrorCode = httpErrorCode;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getJsonResponse() {
        try {
            JSONObject json = new JSONObject();
            json.put("replica", this.replicaId != null ? this.replicaId : "?");
            json.put("protocol", this.protocolName != null ? this.protocolName : "?");
            json.put("consistency", this.offeredConsistencyModel != null
                    ? this.offeredConsistencyModel : "?");
            json.put("requestedConsistency", this.requestedConsistencyModel != null
                    ? this.requestedConsistencyModel : "?");
            json.put("role", this.protocolRoleName != null ? this.protocolRoleName : "?");

            // Add container metadata if available 
            if (this.containerId != null) {
                JSONArray containers = new JSONArray();
                for (int i = 0; i < containerId.size(); i++) {
                    JSONObject container = new JSONObject();
                    container.put("id", containerId.get(i));
                    container.put("created_at", createdAtInfo != null && createdAtInfo.size() > i
                            ? createdAtInfo.get(i) : "?");
                    container.put("status", containerStatus != null && containerStatus.size() > i
                            ? containerStatus.get(i) : "?");
                    containers.put(container);
                }
                json.put("containers", containers);
            }

            return json.toString();
        } catch (JSONException e) {
            System.out.println("ERROR: " + e);
            return "{}";
        }
    }
}
