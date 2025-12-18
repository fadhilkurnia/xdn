package edu.umass.cs.xdn;

import edu.umass.cs.causal.CausalReplicaCoordinator;
import edu.umass.cs.clientcentric.BayouReplicaCoordinator;
import edu.umass.cs.eventual.LazyReplicaCoordinator;
import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.AppRequestParser;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.pram.PramReplicaCoordinator;
import edu.umass.cs.primarybackup.PrimaryBackupManager;
import edu.umass.cs.primarybackup.PrimaryBackupReplicaCoordinator;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.primarybackup.packets.ChangePrimaryPacket;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ClientReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.SetCoordinatorNodeRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.sequential.AwReplicaCoordinator;
import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;
import edu.umass.cs.xdn.request.XdnGetReplicaInfoRequest;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnRequestType;
import edu.umass.cs.xdn.service.ConsistencyModel;
import edu.umass.cs.xdn.service.RequestMatcher;
import edu.umass.cs.xdn.service.ServiceInstance;
import edu.umass.cs.xdn.service.ServiceProperty;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * XdnReplicaCoordinator is a wrapper of multiple replica coordinators supported by XDN.
 *
 * @param <NodeIDType>
 */
public class XdnReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

  private final String myNodeID;
  private final XdnGigapaxosApp xdnGigapaxosApp;

  // list of all coordination managers supported in XDN
  private final AbstractReplicaCoordinator<NodeIDType> primaryBackupCoordinator;
  private final AbstractReplicaCoordinator<NodeIDType> paxosCoordinator;
  private final AbstractReplicaCoordinator<NodeIDType> chainReplicationCoordinator;
  private final AbstractReplicaCoordinator<NodeIDType> pramReplicaCoordinator;
  private final AbstractReplicaCoordinator<NodeIDType> awReplicaCoordinator;
  private final AbstractReplicaCoordinator<NodeIDType> clientCentricReplicaCoordinator;
  private final AbstractReplicaCoordinator<NodeIDType> causalReplicaCoordinator;
  private final AbstractReplicaCoordinator<NodeIDType> lazyReplicaCoordinator;

  // mapping between service name to the service's coordination manager
  private final Map<String, AbstractReplicaCoordinator<NodeIDType>> serviceCoordinator;

  // mapping between service name to the service property
  private final Map<String, ServiceProperty> serviceProperties;

  private final Set<IntegerPacketType> requestTypes;

  private final Logger logger = Logger.getLogger(XdnReplicaCoordinator.class.getName());

  public XdnReplicaCoordinator(
      Replicable app,
      NodeIDType myID,
      Stringifiable<NodeIDType> unstringer,
      Messenger<NodeIDType, JSONObject> messenger) {
    super(app, messenger);

    System.out.printf(">> XDNReplicaCoordinator - init at node %s\n", myID);

    assert app.getClass().getSimpleName().equals(XdnGigapaxosApp.class.getSimpleName())
        : "XdnReplicaCoordinator must be used with XdnGigapaxosApp";
    assert myID.getClass().getSimpleName().equals(String.class.getSimpleName())
        : "XdnReplicaCoordinator must use String as the NodeIDType";

    this.myNodeID = myID.toString();
    this.xdnGigapaxosApp = (XdnGigapaxosApp) app;

    try {
      if (!XdnGigapaxosApp.checkSystemRequirements())
        throw new AssertionError("system requirement is unsatisfied");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    // Pre-process the application.
    // This step is needed especially for PrimaryBackupReplicaCoordinator that require
    // a middleware application as the Paxos's app. The Middleware app does Primary Backup logic
    // before handing/forwarding some of the AppRequest to the actual App: XdnGigapaxosApp.
    BackupableApplication backupableApplication = (BackupableApplication) app;
    Replicable preProcessedApp = PrimaryBackupManager.PrimaryBackupMiddlewareApp.wrapApp(app);

    // Pre-process PaxosManager that will be used by multiple coordinators.
    PrimaryBackupManager.setupPaxosConfiguration();
    PaxosManager<NodeIDType> paxosManager =
        new PaxosManager<>(myID, unstringer, messenger, preProcessedApp);

    // Initialize all the wrapped coordinators, using the pre-processed app and paxos manager.
    PaxosReplicaCoordinator<NodeIDType> paxosReplicaCoordinator =
        new PaxosReplicaCoordinator<>(app, myID, unstringer, messenger, paxosManager);
    PrimaryBackupReplicaCoordinator<NodeIDType> primaryBackupReplicaCoordinator =
        new PrimaryBackupReplicaCoordinator<>(
            preProcessedApp, myID, unstringer, messenger, paxosManager);
    AwReplicaCoordinator<NodeIDType> awReplicaCoordinator =
        new AwReplicaCoordinator<>(app, myID, unstringer, messenger, paxosManager);
    PramReplicaCoordinator<NodeIDType> pramReplicaCoordinator =
        new PramReplicaCoordinator<>(app, myID, unstringer, messenger);
    BayouReplicaCoordinator<NodeIDType> bayouReplicaCoordinator =
        new BayouReplicaCoordinator<>(app, myID, unstringer, messenger);
    CausalReplicaCoordinator<NodeIDType> causalReplicaCoordinator =
        new CausalReplicaCoordinator<>(app, myID, unstringer, messenger);
    LazyReplicaCoordinator<NodeIDType> lazyReplicaCoordinator =
        new LazyReplicaCoordinator<>(app, myID, unstringer, messenger);

    this.primaryBackupCoordinator = primaryBackupReplicaCoordinator;
    this.paxosCoordinator = paxosReplicaCoordinator;
    this.chainReplicationCoordinator = null; // not used for now
    this.pramReplicaCoordinator = pramReplicaCoordinator;
    this.awReplicaCoordinator = awReplicaCoordinator;
    this.clientCentricReplicaCoordinator = bayouReplicaCoordinator;
    this.causalReplicaCoordinator = causalReplicaCoordinator;
    this.lazyReplicaCoordinator = lazyReplicaCoordinator;

    // initialize empty service -> coordinator mapping
    this.serviceCoordinator = new ConcurrentHashMap<>();

    // initialize empty service -> service-property mapping
    this.serviceProperties = new ConcurrentHashMap<>();

    // registering all request types handled by XDN,
    // including all request types of each coordination managers.
    Set<IntegerPacketType> types = new HashSet<>();
    types.add(XdnRequestType.XDN_SERVICE_HTTP_REQUEST);
    types.addAll(PrimaryBackupManager.getAllPrimaryBackupPacketTypes());
    types.addAll(PramReplicaCoordinator.getAllPramRequestTypes());
    this.requestTypes = types;

    // get request
    this.setGetRequestImpl(
        new AppRequestParser() {
          @Override
          public Request getRequest(String stringified) {
            return null;
          }

          @Override
          public Set<IntegerPacketType> getRequestTypes() {
            return null;
          }
        });
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return requestTypes;
  }

  @Override
  public boolean coordinateRequest(Request request, ExecutedCallback callback)
      throws IOException, RequestParseException {
    long startCoordinationTimeNs = System.nanoTime();

    // gets service name and its coordinator
    var serviceName = request.getServiceName();
    var coordinator = this.serviceCoordinator.get(serviceName);
    if (coordinator == null) {
      // returns 404 not found back to client
      return createNotFoundResponse(request, callback);
    }
    long endGetCoordinatorTimeNs = System.nanoTime();

    // one edge case, handling XdnGetProtocolRoleRequest
    if (request instanceof XdnGetReplicaInfoRequest xdnGetReplicaInfoRequest) {
      this.handleXdnGetProtocolRoleRequest(xdnGetReplicaInfoRequest, callback);
      return true;
    }

    if (request instanceof SetCoordinatorNodeRequest<?> setCoordinatorNodeRequest) {
      this.handleSetCoordinatorNodeRequest(setCoordinatorNodeRequest, callback);
      return true;
    }

    // prepare gigapaxos' request
    ReplicableClientRequest gpRequest = null;
    if (request instanceof ReplicableClientRequest rcr) {
      gpRequest = rcr;
    } else {
      gpRequest = ReplicableClientRequest.wrap(request);
    }
    long endRequestPrepTimeNs = System.nanoTime();

    // TODO: validate what client address to set here. We need to explicitly set the
    //  client's address because down the pipeline that is being used for equals() method.
    if (gpRequest.getClientAddress() == null) {
      gpRequest.setClientAddress(messenger.getListeningSocketAddress());
    }

    // set the service's request matcher for this request
    if (gpRequest.getRequest() instanceof XdnHttpRequest xdnHttpRequest) {
      List<RequestMatcher> serviceRequestMatchers = new ArrayList<>();
      ServiceProperty serviceProperty = this.serviceProperties.get(serviceName);
      if (serviceProperty == null) {
        logger.log(Level.WARNING, "Unknown service property for service=" + serviceName);
      }
      if (serviceProperty != null) {
        serviceRequestMatchers = serviceProperty.getRequestMatchers();
      }
      xdnHttpRequest.setRequestMatchers(serviceRequestMatchers);
      xdnHttpRequest.getBehaviors(); // populate cached behaviors
    }
    long endPrepReqMatcherTimeNs = System.nanoTime();

    // cache the request in XdnGigapaxosApp, avoiding expensive deserialization
    xdnGigapaxosApp.cacheRequest(gpRequest.getRequest());
    long endReqCacheTimeNs = System.nanoTime();

    // prepare updated callback that logs the elapsed time
    ReplicableClientRequest finalGpRequest = gpRequest;
    ExecutedCallback loggedCallback =
        (response, handled) -> {
          long endTimeNs = System.nanoTime();
          logger.log(
              Level.FINE,
              "{0}:{1} - request coordination within {2}ms "
                  + "(gcor={3}ms prp={4}ms mtc={5}ms cch={6}ms cor={7}ms) [id: {8}]",
              new Object[] {
                this.myNodeID.toLowerCase(),
                this.getClass().getSimpleName(),
                (endTimeNs - startCoordinationTimeNs) / 1_000_000.0,
                (endGetCoordinatorTimeNs - startCoordinationTimeNs) / 1_000_000.0,
                (endRequestPrepTimeNs - endGetCoordinatorTimeNs) / 1_000_000.0,
                (endPrepReqMatcherTimeNs - endRequestPrepTimeNs) / 1_000_000.0,
                (endReqCacheTimeNs - endPrepReqMatcherTimeNs) / 1_000_000.0,
                (endTimeNs - endReqCacheTimeNs) / 1_000_000.0,
                String.valueOf(finalGpRequest.getRequestID())
              });
          callback.executed(response, handled);
        };

    // asynchronously coordinate the request
    boolean isCoordinated = coordinator.coordinateRequest(gpRequest, loggedCallback);
    if (!isCoordinated) {
      logger.log(
          Level.FINE,
          "{0}:{1} - fail coordinating request with {2}",
          new Object[] {
            this.myNodeID.toLowerCase(),
            this.getClass().getSimpleName(),
            coordinator.getClass().getSimpleName()
          });
    }
    return isCoordinated;
  }

  private void handleSetCoordinatorNodeRequest(
      SetCoordinatorNodeRequest<?> setCoordinatorNodeRequest, ExecutedCallback callback) {
    String serviceName = setCoordinatorNodeRequest.getServiceName();
    if (serviceName == null || serviceName.isEmpty()) {
      setCoordinatorNodeRequest.setFailed(
          ClientReconfigurationPacket.ResponseCodes.NONEXISTENT_NAME_ERROR);
      setCoordinatorNodeRequest.setResponseMessage("Unknown serviceName");
      callback.executed(setCoordinatorNodeRequest, true);
      return;
    }

    var coordinator = this.serviceCoordinator.get(serviceName);
    if (coordinator == null) {
      setCoordinatorNodeRequest.setFailed(
          ClientReconfigurationPacket.ResponseCodes.ACTIVE_REPLICA_EXCEPTION);
      setCoordinatorNodeRequest.setResponseMessage("Unknown coordinator for " + serviceName);
      callback.executed(setCoordinatorNodeRequest, true);
      return;
    }

    if (coordinator instanceof PaxosReplicaCoordinator<NodeIDType> pc
        && setCoordinatorNodeRequest.getNewCoordinatorNodeId().equals(myNodeID)) {
      if (!pc.isPaxosCoordinator(serviceName)) {
        pc.tryToBeCoordinator(serviceName);
      }
      setCoordinatorNodeRequest.setResponseMessage("OK");
      callback.executed(setCoordinatorNodeRequest, true);
      return;
    }

    if (coordinator instanceof PrimaryBackupReplicaCoordinator<NodeIDType> pb
        && setCoordinatorNodeRequest.getNewCoordinatorNodeId().equals(myNodeID)) {
      ChangePrimaryPacket cpPacket = new ChangePrimaryPacket(serviceName, myNodeID);
      try {
        if (pb.isPrimary(serviceName)) {
          setCoordinatorNodeRequest.setResponseMessage("OK");
          callback.executed(setCoordinatorNodeRequest, true);
        } else {
          primaryBackupCoordinator.coordinateRequest(
              cpPacket,
              (response, handled) -> {
                assert handled : "Unhandled ChangePrimaryPacket";
                setCoordinatorNodeRequest.setResponseMessage("OK");
                callback.executed(setCoordinatorNodeRequest, true);
              });
        }
      } catch (IOException | RequestParseException e) {
        throw new RuntimeException(e);
      }
      return;
    }

    setCoordinatorNodeRequest.setResponseMessage("OK");
    callback.executed(setCoordinatorNodeRequest, true);
  }

  private boolean createNotFoundResponse(Request request, ExecutedCallback callback) {
    // handle only if the request is a Http request coming from client.
    String serviceName = request.getServiceName();
    XdnHttpRequest httpRequest = null;
    if (request instanceof ReplicableClientRequest rcr
        && rcr.getRequest() instanceof XdnHttpRequest xdnHttpRequest) {
      httpRequest = xdnHttpRequest;
    }
    if (request instanceof XdnHttpRequest xdnHttpRequest) {
      httpRequest = xdnHttpRequest;
    }
    if (httpRequest == null) {
      throw new RuntimeException(
          "Unknown coordinator for name="
              + serviceName
              + " with request type of "
              + request.getClass().getSimpleName());
    }

    // generate 404 response
    String errorMessage =
        String.format("Service '%s' does not exist in this XDN deployment", serviceName);
    HttpHeaders headers = new DefaultHttpHeaders();
    headers.add(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
    HttpResponse notFoundResponse =
        new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.NOT_FOUND,
            Unpooled.copiedBuffer(errorMessage.getBytes()),
            headers,
            new DefaultHttpHeaders());
    httpRequest.setHttpResponse(notFoundResponse);
    callback.executed(httpRequest, true);
    return true;
  }

  private void handleXdnGetProtocolRoleRequest(
      XdnGetReplicaInfoRequest request, ExecutedCallback callback) {
    String serviceName = request.getServiceName();
    assert serviceName != null : "Unknown service name";
    ServiceProperty currServiceProperty = this.serviceProperties.get(serviceName);
    if (currServiceProperty == null) {
      request.setHttpErrorCode(404);
      request.setErrorMessage("Unknown service '" + serviceName + "'");
      callback.executed(request, true);
      return;
    }
    AbstractReplicaCoordinator<NodeIDType> coordinator = this.serviceCoordinator.get(serviceName);

    // prepare for the response
    String protocolName = coordinator != null ? coordinator.getClass().getSimpleName() : "?";
    String requestedConsistency = currServiceProperty.getConsistencyModel().toString();
    String offeredConsistency = getConsistencyModelFromCoordinator(serviceName, coordinator);
    String roleName = "replica";

    // paxos, sequential, and primary-backup internally handle the request to know the role
    // in this replica.
    if (coordinator instanceof PaxosReplicaCoordinator<NodeIDType> paxos) {
      boolean isCoordinator = paxos.isPaxosCoordinator(serviceName);
      roleName = isCoordinator ? "leader" : "follower";
    }
    if (coordinator instanceof AwReplicaCoordinator<NodeIDType> sequentialCoordinator) {
      boolean isCoordinator = sequentialCoordinator.isPaxosCoordinator(serviceName);
      roleName = isCoordinator ? "leader" : "follower";
    }
    if (coordinator instanceof PrimaryBackupReplicaCoordinator<NodeIDType> pbCoordinator) {
      boolean isPrimary = pbCoordinator.isPrimary(serviceName);
      roleName = isPrimary ? "primary" : "backup";
    }

    if (xdnGigapaxosApp != null) {
      List<String> containerIds = xdnGigapaxosApp.getContainerIds(serviceName);
      List<String> createdAtInfo = xdnGigapaxosApp.getContainerCreatedAtInfo(serviceName);
      List<String> containerStatus = xdnGigapaxosApp.getContainerStatus(serviceName);

      ServiceInstance instance = xdnGigapaxosApp.getServiceInstance(serviceName);
      boolean isDeterministic = false;
      String entryComponent = null;
      String stateDirectory = null;
      String statefulComponent = null;
      List<String> componentNames = null;
      List<String> imageNames = null;
      if (instance != null) {
        isDeterministic = instance.property.isDeterministic();
        stateDirectory = instance.stateDirectory;
        componentNames = new ArrayList<>();
        imageNames = new ArrayList<>();
        for (var c : instance.property.getComponents()) {
          var componentName =
              c.getComponentName() != null ? c.getComponentName() : c.getImageName();
          componentNames.add(componentName);
          imageNames.add(c.getImageName());
          if (c.isEntryComponent()) {
            entryComponent = componentName;
          }
          if (c.isStateful()) {
            statefulComponent = componentName;
          }
        }
      }

      Integer epoch = xdnGigapaxosApp.getEpoch(serviceName);

      request.setContainerMetadata(
          epoch,
          isDeterministic,
          entryComponent,
          stateDirectory,
          statefulComponent,
          componentNames,
          imageNames,
          containerIds,
          createdAtInfo,
          containerStatus);
    }

    request.setResponse(
        this.myNodeID, protocolName, requestedConsistency, offeredConsistency, roleName);
    callback.executed(request, true);
  }

  private String getConsistencyModelFromCoordinator(
      String serviceName, AbstractReplicaCoordinator<NodeIDType> coordinator) {
    if (coordinator instanceof PaxosReplicaCoordinator<NodeIDType>) {
      return ConsistencyModel.LINEARIZABILITY.toString();
    }
    if (coordinator instanceof AwReplicaCoordinator<NodeIDType>) {
      return ConsistencyModel.SEQUENTIAL.toString();
    }
    if (coordinator instanceof PrimaryBackupReplicaCoordinator<NodeIDType>) {
      return ConsistencyModel.LINEARIZABILITY.toString();
    }
    if (coordinator instanceof CausalReplicaCoordinator<NodeIDType>) {
      return ConsistencyModel.CAUSAL.toString();
    }
    if (coordinator instanceof PramReplicaCoordinator<NodeIDType>) {
      return ConsistencyModel.PRAM.toString();
    }
    if (coordinator instanceof BayouReplicaCoordinator<NodeIDType> bayou) {
      return bayou.getServiceConsistencyModel(serviceName);
    }
    return "?";
  }

  @Override
  public boolean createReplicaGroup(
      String serviceName,
      int epoch,
      String state,
      Set<NodeIDType> nodes,
      String placementMetadata) {
    logger.log(
        Level.FINEST,
        "{0}:XdnReplicaCoordinator - createReplicaGroup "
            + "name={1}, epoch={2}, state={3}, nodes={4}, metadata={5}",
        new Object[] {myNodeID, serviceName, epoch, state, nodes, placementMetadata});

    // These are the default replica groups from Gigapaxos
    if (serviceName.equals(PaxosConfig.getDefaultServiceName())
        || serviceName.equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString())
        || serviceName.equals(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString())) {
      // FIXME: we need to consider how to handle these default service and meta-name.
      // boolean isSuccess = this.paxosCoordinator.createReplicaGroup(serviceName, epoch, state,
      // nodes);
      // assert isSuccess : "failed to create default services";
      // this.serviceCoordinator.put(serviceName, this.paxosCoordinator);
      return true;
    }

    return this.initializeReplicaGroup(serviceName, state, nodes, epoch, placementMetadata);
  }

  private boolean initializeReplicaGroup(
      String serviceName,
      String initialState,
      Set<NodeIDType> nodes,
      int placementEpoch,
      String placementMetadata) {
    // Validates the serviceName, initialState, and nodes
    assert serviceName != null && !serviceName.isEmpty()
        : "Cannot initialize an XDN service with null or empty service name";
    assert nodes != null && !nodes.isEmpty()
        : "Cannot initialize an XDN service with unknown target nodes";
    assert initialState != null && !initialState.isEmpty()
        : "Cannot initialize an XDN service with null or empty initial state";
    final String validInitialStatePrefix = "xdn:init:";
    final String validEpochFinalStatePrefix = "xdn:final:";
    assert initialState.startsWith(validInitialStatePrefix)
            || initialState.startsWith(validEpochFinalStatePrefix)
        : "Incorrect initial state prefix: " + initialState;

    // Parse and validate the service's properties
    String encodedProperties = null;
    if (initialState.startsWith(validInitialStatePrefix)) {
      encodedProperties = initialState.substring(validInitialStatePrefix.length());
    }
    if (initialState.startsWith(validEpochFinalStatePrefix)) {
      // format: xdn:final:<epoch>::<serviceProperty>::<finalState>
      String[] raw = initialState.split("::");
      assert raw.length >= 2;
      encodedProperties = raw[1];
    }
    assert encodedProperties != null;
    ServiceProperty serviceProperty = null;
    try {
      serviceProperty = ServiceProperty.createFromJsonString(encodedProperties);
    } catch (JSONException e) {
      logger.log(Level.SEVERE, "Invalid service properties given: " + encodedProperties);
      throw new RuntimeException(e);
    }

    // Infer the replica coordinator based on the declared properties
    AbstractReplicaCoordinator<NodeIDType> coordinator =
        inferCoordinatorByProperties(serviceProperty);
    assert coordinator != null
        : "XDN does not know what coordinator to be used for the specified service";

    // Create the replica group using the coordinator
    boolean isSuccess;
    if (isClientCentricConsistency(serviceProperty.getConsistencyModel())) {
      // A special case for client-centric replica coordinator, Bayou, that require
      // us to specify the client-centric consistency model because Bayou support
      // four different client-centric consistency models.
      assert coordinator instanceof BayouReplicaCoordinator<NodeIDType>;
      isSuccess =
          ((BayouReplicaCoordinator<NodeIDType>) coordinator)
              .createReplicaGroup(
                  getBayouConsistencyModel(serviceProperty.getConsistencyModel()),
                  serviceName,
                  placementEpoch,
                  initialState,
                  nodes);
      // TODO: handle placement metadata for Bayou, if there is any. For now it is not
      //   needed because there is no "preferred" coordinator for Client Centric Consistency.
    } else {
      // for all other coordinators, we use the generic createReplicaGroup method.
      isSuccess =
          coordinator.createReplicaGroup(
              serviceName, placementEpoch, initialState, nodes, placementMetadata);
    }
    assert isSuccess : "failed to initialize service";

    // Store the service->coordinator mapping
    this.serviceCoordinator.put(serviceName, coordinator);

    // Store the service->service-property mapping
    this.serviceProperties.put(serviceName, serviceProperty);

    System.out.printf(
        ">> XDNReplicaCoordinator:%s name=%s coordinator=%s\n",
        myNodeID, serviceName, coordinator.getClass().getSimpleName());
    return true;
  }

  private boolean isClientCentricConsistency(ConsistencyModel consistencyModel) {
    return consistencyModel.equals(ConsistencyModel.MONOTONIC_READS)
        || consistencyModel.equals(ConsistencyModel.MONOTONIC_WRITES)
        || consistencyModel.equals(ConsistencyModel.READ_YOUR_WRITES)
        || consistencyModel.equals(ConsistencyModel.WRITES_FOLLOW_READS);
  }

  private edu.umass.cs.clientcentric.ConsistencyModel getBayouConsistencyModel(
      ConsistencyModel xdnClientCentricConsistencyModel) {
    switch (xdnClientCentricConsistencyModel) {
      case MONOTONIC_READS -> {
        return edu.umass.cs.clientcentric.ConsistencyModel.MONOTONIC_READS;
      }
      case MONOTONIC_WRITES -> {
        return edu.umass.cs.clientcentric.ConsistencyModel.MONOTONIC_WRITES;
      }
      case READ_YOUR_WRITES -> {
        return edu.umass.cs.clientcentric.ConsistencyModel.READ_YOUR_WRITES;
      }
      case WRITES_FOLLOW_READS -> {
        return edu.umass.cs.clientcentric.ConsistencyModel.WRITES_FOLLOW_READS;
      }
      default -> {
        return BayouReplicaCoordinator.DEFAULT_CLIENT_CENTRIC_CONSISTENCY_MODEL;
      }
    }
  }

  private AbstractReplicaCoordinator<NodeIDType> inferCoordinatorByProperties(
      ServiceProperty serviceProperties) {
    // For non-deterministic service we always use primary-backup, the only coordinator
    // we have implemented that can handle non-determinism.
    if (!serviceProperties.isDeterministic()) {
      return this.primaryBackupCoordinator;
    }

    // For deterministic service, we have more options based on the consistency model,
    // especially when the service only has read-only and write-only requests.
    // TODO: introduce new coordinator for different consistency models.
    else {
      ConsistencyModel declaredConsModel = serviceProperties.getConsistencyModel();
      Set<RequestBehaviorType> allDeclaredBehaviors = serviceProperties.getAllBehaviors();

      if (declaredConsModel.equals(ConsistencyModel.LINEARIZABILITY)
          || declaredConsModel.equals(ConsistencyModel.LINEARIZABLE)) {
        return this.paxosCoordinator;
      }

      // Our sequential consistency protocol does not have many constraints, it coordinates
      // all non read-oly requests using Paxos, and execute all read-only requests locally.
      if (declaredConsModel.equals(ConsistencyModel.SEQUENTIAL)) {
        return this.awReplicaCoordinator;
      }

      // Our protocol implementing client-centric consistency models support almost all
      // behaviors. It generally treats all non-read-only requests as write.
      Set<ConsistencyModel> clientCentricConsModels =
          new HashSet<>(
              Arrays.asList(
                  ConsistencyModel.MONOTONIC_READS,
                  ConsistencyModel.MONOTONIC_WRITES,
                  ConsistencyModel.READ_YOUR_WRITES,
                  ConsistencyModel.WRITES_FOLLOW_READS));
      if (clientCentricConsModels.contains(declaredConsModel)) {
        return this.clientCentricReplicaCoordinator;
      }

      // Our implemented causal consistency protocol supports all behaviors other than
      // read-modify-write requests. which can cause diverging state between replica and
      // thus require reconciliation mechanism, which is generally hard with blackbox service
      // (i.e., we need to know the state semantics to "merge" the state). We fall back to
      // sequential consistency protocol, which support ready-modify-write operations.
      Set<RequestBehaviorType> nonRmwBehaviors =
          new HashSet<>(
              Arrays.asList(
                  RequestBehaviorType.READ_ONLY, RequestBehaviorType.WRITE_ONLY,
                  RequestBehaviorType.NIL_EXTERNAL, RequestBehaviorType.MONOTONIC));
      if (declaredConsModel.equals(ConsistencyModel.CAUSAL)
          && nonRmwBehaviors.containsAll(allDeclaredBehaviors)) {
        return this.causalReplicaCoordinator;
      }
      if (declaredConsModel.equals(ConsistencyModel.CAUSAL)
          && !nonRmwBehaviors.containsAll(allDeclaredBehaviors)) {
        return this.awReplicaCoordinator;
      }

      // Our implemented protocol for PRAM does not support read-modify-write as it
      // can cause diverging state across replicas, similar reasoning as in the causal
      // consistency protocol. We fall back to sequential consistency if the service has
      // read-modify-write operations.
      if (declaredConsModel.equals(ConsistencyModel.PRAM)
          && nonRmwBehaviors.containsAll(allDeclaredBehaviors)) {
        return this.pramReplicaCoordinator;
      }
      if (declaredConsModel.equals(ConsistencyModel.PRAM)
          && !nonRmwBehaviors.containsAll(allDeclaredBehaviors)) {
        return this.awReplicaCoordinator;
      }

      // Lazy replications generally works for service whose operations are monotonic.
      Set<RequestBehaviorType> protocolConstraints =
          new HashSet<>(
              Arrays.asList(
                  RequestBehaviorType.MONOTONIC,
                  RequestBehaviorType.READ_ONLY,
                  RequestBehaviorType.WRITE_ONLY));
      allDeclaredBehaviors.remove(RequestBehaviorType.NIL_EXTERNAL); // optional
      if (declaredConsModel.equals(ConsistencyModel.EVENTUAL)
          && allDeclaredBehaviors.equals(protocolConstraints)) {
        return this.lazyReplicaCoordinator;
      }

      // It is always safe to fall back with sequential consistency, if the service
      // does not have any read-only operations, it essentially similar to Paxos with
      // linearizability.
      return this.awReplicaCoordinator;
    }
  }

  @Override
  public boolean deleteReplicaGroup(String serviceName, int epoch) {
    AbstractReplicaCoordinator<NodeIDType> coordinator = this.serviceCoordinator.get(serviceName);
    if (coordinator == null) {
      return true;
    }
    this.serviceProperties.remove(serviceName);
    return coordinator.deleteReplicaGroup(serviceName, epoch);
  }

  @Override
  public Set<NodeIDType> getReplicaGroup(String serviceName) {
    var coordinator = this.serviceCoordinator.get(serviceName);
    if (coordinator == null) {
      return null;
    }
    return coordinator.getReplicaGroup(serviceName);
  }
}
