package edu.umass.cs.xdn.request;

import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.xdn.service.RequestMatcher;
import java.util.List;
import java.util.UUID;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

public class XdnGetReplicaInfoRequest extends XdnRequest implements ClientRequest {

  private final long requestId;
  private final String serviceName;

  // fields for the success response
  Integer placementEpoch;
  String replicaId;
  String protocolName;
  String requestedConsistencyModel;
  String offeredConsistencyModel;
  String protocolRoleName; // e.g., primary, backup, coordinator, replica
  boolean isDeterministic;
  String stateDirectory;
  String entryComponent;
  String statefulComponent;
  List<String> componentNames;
  List<String> imageNames;
  List<String> containerIds;
  List<String> createdAtInfo;
  List<String> containerStatus;
  List<RequestMatcher> requestBehaviors;

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

  public void setResponse(
      String replicaId,
      String protocolName,
      String requestedConsistencyModel,
      String offeredConsistencyModel,
      String roleName) {
    this.replicaId = replicaId;
    this.protocolName = protocolName;
    this.requestedConsistencyModel = requestedConsistencyModel;
    this.offeredConsistencyModel = offeredConsistencyModel;
    this.protocolRoleName = roleName;
  }

  /** Sets container metadata information for this service replica */
  public void setContainerMetadata(
      Integer epoch,
      boolean isDeterministic,
      String entryComponent,
      String stateDirectory,
      String statefulComponent,
      List<String> componentNames,
      List<String> imageNames,
      List<String> containerIds,
      List<String> createdAtInfo,
      List<String> containerStatus) {
    this.placementEpoch = epoch;
    this.isDeterministic = isDeterministic;
    this.entryComponent = entryComponent;
    this.statefulComponent = statefulComponent;
    this.stateDirectory = stateDirectory;
    this.componentNames = componentNames;
    this.imageNames = imageNames;
    this.containerIds = containerIds;
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
      json.put("epoch", this.placementEpoch != null ? this.placementEpoch : "?");
      json.put(
          "consistency", this.offeredConsistencyModel != null ? this.offeredConsistencyModel : "?");
      json.put(
          "requestedConsistency",
          this.requestedConsistencyModel != null ? this.requestedConsistencyModel : "?");
      json.put("role", this.protocolRoleName != null ? this.protocolRoleName : "?");

      if (this.containerIds != null) {
        JSONArray containers = new JSONArray();
        for (int i = 0; i < containerIds.size(); i++) {
          JSONObject container = new JSONObject();
          container.put("idx", i);
          container.put("id", containerIds.get(i));
          container.put(
              "name",
              componentNames != null && componentNames.size() > i ? componentNames.get(i) : "?");
          if (imageNames != null && imageNames.size() > i && imageNames.get(i) != null) {
            container.put("image", imageNames.get(i));
          }
          container.put(
              "createdAt",
              createdAtInfo != null && createdAtInfo.size() > i ? createdAtInfo.get(i) : "?");
          container.put(
              "status",
              containerStatus != null && containerStatus.size() > i ? containerStatus.get(i) : "?");
          containers.put(container);
        }
        json.put("containers", containers);
      }

      json.put("deterministic", this.isDeterministic);
      if (this.entryComponent != null) {
        json.put("entryComponent", this.entryComponent);
      }
      if (this.statefulComponent != null) {
        json.put("statefulComponent", this.statefulComponent);
      }
      if (this.stateDirectory != null) {
        json.put("stateDirectory", this.stateDirectory);
      }

      if (this.requestBehaviors != null && !this.requestBehaviors.isEmpty()) {
        JSONArray behaviors = new JSONArray();
        for (RequestMatcher matcher : this.requestBehaviors) {
          if (matcher == null) {
            continue;
          }
          JSONObject matcherJson = new JSONObject();
          if (matcher.getMatcherName() != null) {
            matcherJson.put("name", matcher.getMatcherName());
          }
          matcherJson.put("prefix", matcher.getPathPrefix());
          JSONArray methods = new JSONArray();
          for (String method : matcher.getHttpMethods()) {
            methods.put(method);
          }
          matcherJson.put("methods", methods);
          matcherJson.put("behavior", matcher.getBehavior().name());
          behaviors.put(matcherJson);
        }
        json.put("requestBehaviors", behaviors);
      }

      return json.toString();
    } catch (JSONException e) {
      System.out.println("ERROR: " + e);
      return "{}";
    }
  }
}
