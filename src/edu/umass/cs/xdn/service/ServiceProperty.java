package edu.umass.cs.xdn.service;

import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;
import io.netty.handler.codec.http.HttpMethod;
import java.util.*;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

// TODO: handle stateless service
public class ServiceProperty {

  public static String XDN_INITIAL_STATE_PREFIX = "xdn:init:";
  public static String XDN_CHECKPOINT_PREFIX = "xdn:checkpoint:";
  public static String XDN_EPOCH_FINAL_STATE_PREFIX = "xdn:final:";
  public static String NON_DETERMINISTIC_CREATE_PREFIX = "nondeter:create:";
  public static String NON_DETERMINISTIC_START_PREFIX = "nondeter:start:";
  public static String NON_DETERMINISTIC_START_BACKUP_PREFIX = "nondeter:start:backup";

  private final String serviceName;
  private final boolean isDeterministic;

  /** either "/data/" or "datastore:/data/" */
  private final String stateDirectory;

  private final ConsistencyModel consistencyModel;
  private final List<ServiceComponent> components;

  private ServiceComponent entryComponent;
  private ServiceComponent statefulComponent;

  private final List<RequestMatcher> requestMatchers;

  private final Integer numReplicas;
  private final Integer minReplicas;
  private final Integer maxReplicas;

  /** REPLICATED (default) or CLUSTER — see {@link DeploymentMode}. */
  private final DeploymentMode deploymentMode;

  /** The cluster's internal/peer protocol port. Non-null only for cluster services. */
  private final Integer peerPort;

  /**
   * Persistent nodeId -&gt; ordinal map for a cluster service. Mutable because ordinals must be
   * preserved across reconfiguration epochs (a relocated replica keeps its ordinal).
   */
  private Map<String, Integer> ordinalMap;

  private ServiceProperty(
      String serviceName,
      boolean isDeterministic,
      String stateDirectory,
      ConsistencyModel consistencyModel,
      List<ServiceComponent> components,
      List<RequestMatcher> requestMatchers,
      Integer numReplicas,
      Integer minReplicas,
      Integer maxReplicas,
      DeploymentMode deploymentMode,
      Integer peerPort,
      Map<String, Integer> ordinalMap) {
    this.serviceName = serviceName;
    this.isDeterministic = isDeterministic;
    this.stateDirectory = stateDirectory;
    this.consistencyModel = consistencyModel;
    this.components = components;
    this.requestMatchers = requestMatchers;
    this.numReplicas = numReplicas;
    this.minReplicas = minReplicas;
    this.maxReplicas = maxReplicas;
    this.deploymentMode = deploymentMode;
    this.peerPort = peerPort;
    this.ordinalMap = ordinalMap;
  }

  public ServiceComponent getEntryComponent() {
    if (entryComponent == null) {
      for (ServiceComponent c : components) {
        if (c.isEntryComponent()) {
          entryComponent = c;
          break;
        }
      }
    }
    return entryComponent;
  }

  public ServiceComponent getStatefulComponent() {
    if (statefulComponent == null) {
      for (ServiceComponent c : components) {
        if (c.isStateful()) {
          statefulComponent = c;
          break;
        }
      }
    }
    return statefulComponent;
  }

  public static ServiceProperty createFromJsonString(String jsonString) throws JSONException {
    JSONObject json = new JSONObject(jsonString);

    // parsing and validating service name
    String serviceName = json.getString("name");
    if (serviceName == null || serviceName.isEmpty()) {
      throw new IllegalStateException("service name is required");
    }
    if (serviceName.length() > 256) {
      throw new IllegalStateException("service name must be <= 256 characters");
    }

    // parsing is-deterministic
    boolean isDeterministic = false;
    if (json.has("deterministic")) {
      isDeterministic = json.getBoolean("deterministic");
    }

    // parsing deployment mode (default REPLICATED). A cluster service handles its own
    // coordination, so XDN only places it, gives it a stable identity, and routes to it.
    DeploymentMode deploymentMode = DeploymentMode.REPLICATED;
    if (json.has("mode") && !json.isNull("mode")) {
      deploymentMode = DeploymentMode.fromString(json.getString("mode"));
    }
    boolean isClusterManaged = deploymentMode == DeploymentMode.CLUSTER;

    // parsing cluster-only fields: the internal peer port. Reconfiguration lifecycle hooks
    // (add/remove-replica commands) live in the per-service spec when Component 6 lands, not
    // in Java code — see project_cluster_reconfig_hooks memory.
    Integer peerPort = null;
    if (isClusterManaged) {
      if (!json.has("peer_port") || json.isNull("peer_port")) {
        throw new IllegalStateException("peer_port is required for a cluster service");
      }
      peerPort = json.getInt("peer_port");
      if (peerPort < 1 || peerPort > 65535) {
        throw new IllegalStateException("peer_port must be in 1..65535 (got " + peerPort + ")");
      }
    }

    // parsing the persistent nodeId -> ordinal map, if carried over from a previous epoch
    Map<String, Integer> ordinalMap = null;
    if (json.has("ordinal_map") && !json.isNull("ordinal_map")) {
      ordinalMap = new HashMap<>();
      JSONObject ordinalMapJson = json.getJSONObject("ordinal_map");
      Iterator it = ordinalMapJson.keys();
      while (it.hasNext()) {
        String nodeId = it.next().toString();
        ordinalMap.put(nodeId, ordinalMapJson.getInt(nodeId));
      }
    }

    // parsing and validating state directory
    String stateDirectory = json.getString("state");
    if (stateDirectory.isEmpty()) {
      stateDirectory = null;
    }
    if (stateDirectory != null) {
      validateStateDirectory(stateDirectory);
    }

    // parsing and validating consistency model, the default consistency
    // model is SEQUENTIAL_CONSISTENCY. A cluster service owns its own consistency, so the
    // field is optional there and the stored value is only a never-consulted placeholder.
    ConsistencyModel consistencyModel;
    if (isClusterManaged) {
      consistencyModel =
          json.has("consistency") && !json.isNull("consistency")
              ? parseConsistencyModel(json.getString("consistency"))
              : ConsistencyModel.SEQUENTIAL;
    } else {
      String consistencyModelString = json.getString("consistency");
      if (consistencyModelString == null) {
        consistencyModel = ConsistencyModel.SEQUENTIAL;
      } else {
        consistencyModel = parseConsistencyModel(consistencyModelString);
      }
    }

    // parsing and validating service component(s)
    List<ServiceComponent> components = new ArrayList<>();
    if (json.has("image") && json.has("components")) {
      throw new IllegalStateException(
          "a service must either have a single component, "
              + "declared with 'image', or have multiple components declared with 'components'");
    }
    // case-1: handle service with a single component
    if (json.has("image")) {
      String imageName = json.getString("image");
      if (imageName == null || imageName.isEmpty()) {
        throw new IllegalStateException("docker image name is required");
      }

      // parse entry port with port 80 as the default
      int entryPort = json.getInt("port");
      if (entryPort == 0) {
        entryPort = 80;
      }

      // parse environment variables, if any
      Map<String, String> env = null;
      if (json.has("environments")) {
        JSONArray envJSON = json.getJSONArray("environments");
        env = parseEnvironmentVariables(envJSON);
      }
      if (json.has("env")) {
        JSONArray envJSON = json.getJSONArray("env");
        env = parseEnvironmentVariables(envJSON);
      }

      ServiceComponent c =
          new ServiceComponent(
              serviceName,
              imageName,
              entryPort,
              (stateDirectory != null),
              true,
              entryPort,
              env,
              null);
      components.add(c);
    }
    // case-2: handle service with multiple components
    if (json.has("components")) {
      JSONArray componentsJSON = json.getJSONArray("components");
      components.addAll(parseServiceComponents(componentsJSON));
    }

    // parsing and validating request matchers
    List<RequestMatcher> parsedRequestMatchers = null;
    if (json.has("requests")) {
      JSONArray requestMatcherArr = json.getJSONArray("requests");
      if (requestMatcherArr != null && requestMatcherArr.length() > 0) {
        parsedRequestMatchers = ServiceProperty.parseRequestMatchers(requestMatcherArr);
      }
    }
    // provide the default request matcher:
    //  - all GET,HEAD,OPTIONS, and TRACE requests are read_only,
    //  - all PUT and DELETE requests are write_only,
    //  - all POST and PATCH requests are read_modify_write.
    // read_modify_write.
    if (parsedRequestMatchers == null) {
      parsedRequestMatchers = createDefaultMatchers();
    }

    Integer numReplicas = optionalReplicaField(json, "num_replicas");
    Integer minReplicas = optionalReplicaField(json, "min_replicas");
    Integer maxReplicas = optionalReplicaField(json, "max_replicas");
    validateReplicaConfig(numReplicas, minReplicas, maxReplicas);

    // cluster services are single-image and must declare a fixed replica count
    if (isClusterManaged) {
      if (json.has("components")) {
        throw new IllegalStateException(
            "a cluster service must be declared with a single 'image', not 'components'");
      }
      if (!json.has("image")) {
        throw new IllegalStateException("a cluster service requires an 'image'");
      }
      if (numReplicas == null) {
        throw new IllegalStateException("num_replicas is required for a cluster service");
      }
    }

    ServiceProperty prop =
        new ServiceProperty(
            serviceName,
            isDeterministic,
            stateDirectory,
            consistencyModel,
            components,
            parsedRequestMatchers,
            numReplicas,
            minReplicas,
            maxReplicas,
            deploymentMode,
            peerPort,
            ordinalMap);

    // automatically infer is-stateful of component via the state directory
    if (stateDirectory != null && stateDirectory.split(":").length == 2) {
      String[] componentStateDir = stateDirectory.split(":");
      String statefulComponent = componentStateDir[0];
      ServiceComponent c = null;
      for (ServiceComponent sc : components)
        if (sc.getComponentName().equals(statefulComponent)) c = sc;
      if (c == null) {
        throw new IllegalStateException("unknown service's component specified in the state dir");
      }
      c.setIsStateful(true);
    }

    // validation: the number of stateful and entry component
    int numStatefulComponent = 0;
    int numEntryComponent = 0;
    for (ServiceComponent c : components) {
      if (c.isStateful()) numStatefulComponent++;
      if (c.isEntryComponent()) numEntryComponent++;
    }
    if (numStatefulComponent > 1) {
      throw new IllegalStateException("only one stateful service's component is allowed");
    }
    if (numEntryComponent != 1) {
      throw new IllegalStateException("there must be one entry component");
    }

    return prop;
  }

  private static Integer optionalReplicaField(JSONObject json, String key) throws JSONException {
    if (!json.has(key) || json.isNull(key)) {
      return null;
    }
    return json.getInt(key);
  }

  private static void validateReplicaConfig(Integer num, Integer min, Integer max) {
    if (num != null && num < 1) {
      throw new IllegalStateException("num_replicas must be >= 1 (got " + num + ")");
    }
    if (min != null && min < 1) {
      throw new IllegalStateException("min_replicas must be >= 1 (got " + min + ")");
    }
    if (max != null && max < 1) {
      throw new IllegalStateException("max_replicas must be >= 1 (got " + max + ")");
    }
    if (min != null && max != null && min > max) {
      throw new IllegalStateException(
          "min_replicas (" + min + ") cannot exceed max_replicas (" + max + ")");
    }
    if (num != null && min != null && num < min) {
      throw new IllegalStateException(
          "num_replicas (" + num + ") cannot be less than min_replicas (" + min + ")");
    }
    if (num != null && max != null && num > max) {
      throw new IllegalStateException(
          "num_replicas (" + num + ") cannot exceed max_replicas (" + max + ")");
    }
  }

  private static List<RequestMatcher> parseRequestMatchers(JSONArray matcherJsonArray)
      throws JSONException {
    List<RequestMatcher> parsedMatchers = new ArrayList<>();
    for (int i = 0; i < matcherJsonArray.length(); i++) {
      JSONObject matcherItem = matcherJsonArray.getJSONObject(i);

      // get the optional name
      String matcherName = matcherItem.has("name") ? matcherItem.getString("name") : null;

      // get the path prefix
      String pathPrefix = null;
      pathPrefix = matcherItem.has("prefix") ? matcherItem.getString("prefix") : null;
      pathPrefix =
          matcherItem.has("path_prefix") ? matcherItem.getString("path_prefix") : pathPrefix;
      if (pathPrefix == null) {
        throw new IllegalStateException("prefix is required for request matcher");
      }

      // get the comma-separated methods
      String[] methods = null;
      String methodsRaw = matcherItem.has("methods") ? matcherItem.getString("methods") : null;
      if (methodsRaw == null) {
        throw new IllegalStateException("methods is required for request matcher");
      }
      methods = methodsRaw.split(",");

      // get the behavior
      String behaviorRaw = null;
      behaviorRaw = matcherItem.has("behavior") ? matcherItem.getString("behavior") : null;
      if (behaviorRaw == null) {
        throw new IllegalStateException("behavior is required for request matcher");
      }
      RequestBehaviorType behaviorType = RequestBehaviorType.fromString(behaviorRaw);

      RequestMatcher matcher =
          new RequestMatcher(matcherName, pathPrefix, List.of(methods), behaviorType);
      parsedMatchers.add(matcher);
    }
    return parsedMatchers;
  }

  public Set<RequestBehaviorType> getAllBehaviors() {
    Set<RequestBehaviorType> allBehaviors = new HashSet<>();
    if (this.requestMatchers != null)
      for (RequestMatcher matcher : this.requestMatchers) {
        allBehaviors.add(matcher.getBehavior());
      }
    return allBehaviors;
  }

  public static List<RequestMatcher> createDefaultMatchers() {
    List<RequestMatcher> defaultMatchers = new ArrayList<>();
    defaultMatchers.add(
        new RequestMatcher(
            null,
            "/",
            List.of(
                HttpMethod.GET.name(),
                HttpMethod.HEAD.name(),
                HttpMethod.OPTIONS.name(),
                HttpMethod.TRACE.name()),
            RequestBehaviorType.READ_ONLY));
    defaultMatchers.add(
        new RequestMatcher(
            null,
            "/",
            List.of(HttpMethod.PUT.name(), HttpMethod.DELETE.name()),
            RequestBehaviorType.WRITE_ONLY));
    defaultMatchers.add(
        new RequestMatcher(
            null,
            "/",
            List.of(HttpMethod.POST.name(), HttpMethod.PATCH.name()),
            RequestBehaviorType.READ_MODIFY_WRITE));
    return defaultMatchers;
  }

  private static void validateStateDirectory(String stateDirectory) {
    if (stateDirectory == null) {
      return;
    }

    String statePath = null;
    String[] componentAndStateDir = stateDirectory.split(":");
    if (componentAndStateDir.length > 2) {
      throw new RuntimeException(
          "invalid format for state directory, " + "expecting '<component>:<path>' or <path>.");
    }
    if (componentAndStateDir.length == 2) {
      statePath = componentAndStateDir[1];
    }
    if (componentAndStateDir.length == 1) {
      statePath = componentAndStateDir[0];
    }
    if (componentAndStateDir.length < 1) {
      throw new RuntimeException("invalid format for state directory");
    }

    if (statePath.isEmpty()) {
      throw new RuntimeException("empty path of state directory");
    }

    if (!statePath.startsWith("/")) {
      throw new RuntimeException("state directory must be in absolute path");
    }

    if (!stateDirectory.endsWith("/")) {
      throw new RuntimeException("state directory must be a directory, ending with '/'");
    }
  }

  private static ConsistencyModel parseConsistencyModel(String model) {
    if (model == null) {
      throw new RuntimeException("consistency model can not be null");
    }
    if (model.isEmpty()) {
      throw new RuntimeException("consistency model can not be empty");
    }

    // try to match the given consistency model with valid consistency model
    model = model.toUpperCase();
    for (ConsistencyModel cm : ConsistencyModel.values()) {
      if (cm.toString().equals(model)) {
        return cm;
      }
    }

    // invalid consistency model was given, prepare exception message
    StringBuilder b = new StringBuilder();
    int counter = 0;
    for (ConsistencyModel cm : ConsistencyModel.values()) {
      b.append(cm.toString());
      counter++;
      if (counter != ConsistencyModel.values().length) {
        b.append(", ");
      }
    }
    throw new RuntimeException("invalid consistency model, valid values are: " + b.toString());
  }

  private static Map<String, String> parseEnvironmentVariables(JSONArray envJSON)
      throws JSONException {
    Map<String, String> env = new HashMap<>();
    if (envJSON != null && envJSON.length() > 0) {
      for (int i = 0; i < envJSON.length(); i++) {
        JSONObject envItem = envJSON.getJSONObject(i);
        String envVarName = null;
        String envVarValue = null;

        Iterator keyIterator = envItem.keys();
        while (keyIterator.hasNext()) {
          Object k = keyIterator.next();
          envVarName = k.toString();
        }

        if (envVarName == null) {
          continue;
        }

        envVarValue = envItem.getString(envVarName);
        env.put(envVarName, envVarValue);
      }
    }
    return env;
  }

  private static List<ServiceComponent> parseServiceComponents(JSONArray componentsJSON)
      throws JSONException {
    List<ServiceComponent> components = new ArrayList<>();
    int len = componentsJSON.length();
    for (int i = 0; i < len; i++) {
      JSONObject componentJSON = componentsJSON.getJSONObject(i);
      String componentName = null;

      Iterator it = componentJSON.keys();
      if (it.hasNext()) {
        componentName = it.next().toString();
      }
      JSONObject componentDetailJSON = componentJSON.getJSONObject(componentName);

      // parse image name
      String imageName = componentDetailJSON.getString("image");
      if (imageName == null || imageName.isEmpty()) {
        throw new RuntimeException(
            "docker image name is required for service component '" + componentName + "'");
      }

      // parse is-stateful
      boolean isStateful = false;
      if (componentDetailJSON.has("stateful")) {
        isStateful = componentDetailJSON.getBoolean("stateful");
      }

      // parse is-entry
      boolean isEntry = false;
      if (componentDetailJSON.has("entry")) {
        isEntry = componentDetailJSON.getBoolean("entry");
      }

      // parse exposed port
      int exposedPort = 0;
      if (componentDetailJSON.has("expose")) {
        exposedPort = componentDetailJSON.getInt("expose");
      }

      // parse entry port, note that ieEntry is set to true for component with
      // http port specified.
      int entryPort = 0;
      if (componentDetailJSON.has("port")) {
        entryPort = componentDetailJSON.getInt("port");
        isEntry = true;
      }

      // parse environments
      Map<String, String> env = null;
      if (componentDetailJSON.has("environments")) {
        JSONArray envJSON = componentDetailJSON.getJSONArray("environments");
        env = parseEnvironmentVariables(envJSON);
      }
      if (componentDetailJSON.has("env")) {
        JSONArray envJSON = componentDetailJSON.getJSONArray("env");
        env = parseEnvironmentVariables(envJSON);
      }

      // parse healthcheck command for multi-component readiness gating
      String healthcheckCommand = null;
      if (componentDetailJSON.has("healthcheck")) {
        Object healthcheckRaw = componentDetailJSON.get("healthcheck");
        if (healthcheckRaw instanceof JSONObject) {
          JSONObject healthcheckObj = (JSONObject) healthcheckRaw;
          if (!healthcheckObj.has("command")) {
            throw new IllegalStateException(
                "healthcheck object requires a 'command' field for component '"
                    + componentName
                    + "'");
          }
          healthcheckCommand = healthcheckObj.getString("command");
        } else if (healthcheckRaw instanceof String) {
          healthcheckCommand = (String) healthcheckRaw;
        } else {
          throw new IllegalStateException(
              "healthcheck must be a string or object for component '" + componentName + "'");
        }

        if (healthcheckCommand == null || healthcheckCommand.isEmpty()) {
          throw new IllegalStateException(
              "healthcheck command cannot be empty for component '" + componentName + "'");
        }
      }

      components.add(
          new ServiceComponent(
              componentName,
              imageName,
              exposedPort == 0 ? null : exposedPort,
              isStateful,
              isEntry,
              entryPort == 0 ? null : entryPort,
              env,
              healthcheckCommand));
    }

    return components;
  }

  public String getServiceName() {
    return serviceName;
  }

  public boolean isDeterministic() {
    return isDeterministic;
  }

  public String getStateDirectory() {
    return stateDirectory;
  }

  public String getStatefulComponentDirectory() {
    if (this.getStatefulComponent() == null) {
      return null;
    }
    if (this.stateDirectory == null || this.stateDirectory.isEmpty()) {
      return null;
    }

    String[] componentStateDir = this.stateDirectory.split(":");
    assert componentStateDir.length <= 2 : "invalid stateDirectory provided";
    if (componentStateDir.length != 2) {
      return this.stateDirectory;
    }

    return componentStateDir[1];
  }

  public ConsistencyModel getConsistencyModel() {
    return consistencyModel;
  }

  public List<ServiceComponent> getComponents() {
    return components;
  }

  public List<RequestMatcher> getRequestMatchers() {
    return requestMatchers;
  }

  public Integer getNumReplicas() {
    return numReplicas;
  }

  public Integer getMinReplicas() {
    return minReplicas;
  }

  public Integer getMaxReplicas() {
    return maxReplicas;
  }

  public DeploymentMode getDeploymentMode() {
    return deploymentMode;
  }

  public boolean isClusterManaged() {
    return deploymentMode == DeploymentMode.CLUSTER;
  }

  public Integer getPeerPort() {
    return peerPort;
  }

  public Map<String, Integer> getOrdinalMap() {
    return ordinalMap;
  }

  /** Persists the nodeId -&gt; ordinal map so identities survive a reconfiguration epoch. */
  public void setOrdinalMap(Map<String, Integer> ordinalMap) {
    this.ordinalMap = ordinalMap;
  }

  public String toJsonString() {
    assert !this.components.isEmpty() : "unexpected empty component";

    // handle service with a single component
    if (this.components.size() == 1) {
      JSONObject jsonObject = new JSONObject();
      try {
        jsonObject.put("name", this.serviceName);
        jsonObject.put("image", this.getEntryComponent().getImageName());
        jsonObject.put("port", this.getEntryComponent().getEntryPort());
        jsonObject.put("state", this.stateDirectory);
        jsonObject.put("consistency", this.consistencyModel.toString().toLowerCase());
        jsonObject.put("deterministic", this.isDeterministic);
        putReplicaFields(jsonObject);
        putClusterFields(jsonObject);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }
      return jsonObject.toString();
    }

    // handle service with multiple components
    JSONObject servicePropertyJsonObject = new JSONObject();
    try {
      servicePropertyJsonObject.put("name", this.serviceName);
      servicePropertyJsonObject.put("state", this.stateDirectory);
      servicePropertyJsonObject.put("deterministic", this.isDeterministic);
      servicePropertyJsonObject.put("consistency", this.consistencyModel.toString().toLowerCase());
      JSONArray componentArray = new JSONArray();
      for (ServiceComponent component : this.components) {
        JSONObject componentJsonObject = component.toJsonObject();
        componentArray.put(componentJsonObject);
      }
      servicePropertyJsonObject.put("components", componentArray);
      putReplicaFields(servicePropertyJsonObject);
      putClusterFields(servicePropertyJsonObject);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }

    return servicePropertyJsonObject.toString();
  }

  private void putReplicaFields(JSONObject target) throws JSONException {
    if (this.numReplicas != null) {
      target.put("num_replicas", this.numReplicas.intValue());
    }
    if (this.minReplicas != null) {
      target.put("min_replicas", this.minReplicas.intValue());
    }
    if (this.maxReplicas != null) {
      target.put("max_replicas", this.maxReplicas.intValue());
    }
  }

  /**
   * Serializes cluster-only fields so a cluster service survives the reconfiguration round-trip.
   */
  private void putClusterFields(JSONObject target) throws JSONException {
    if (this.deploymentMode != DeploymentMode.CLUSTER) {
      return;
    }
    target.put("mode", "cluster");
    if (this.peerPort != null) {
      target.put("peer_port", this.peerPort.intValue());
    }
    if (this.ordinalMap != null && !this.ordinalMap.isEmpty()) {
      target.put("ordinal_map", new JSONObject(this.ordinalMap));
    }
  }
}
