package edu.umass.cs.xdn;

import static org.junit.Assert.assertNotNull;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.InitialStateValidator;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.utils.ZipFiles;
import edu.umass.cs.xdn.recorder.*;
import edu.umass.cs.xdn.request.*;
import edu.umass.cs.xdn.service.ServiceComponent;
import edu.umass.cs.xdn.service.ServiceInstance;
import edu.umass.cs.xdn.service.ServiceProperty;
import edu.umass.cs.xdn.utils.Shell;
import edu.umass.cs.xdn.utils.ShellOutput;
import edu.umass.cs.xdn.utils.Utils;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import java.io.*;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.json.JSONException;

public class XdnGigapaxosApp
    implements Replicable, Reconfigurable, BackupableApplication, InitialStateValidator {

  private final boolean IS_RESTART_UPON_STATE_DIFF_APPLY = false;

  // TODO: deprecate the variables below.
  private final String FUSELOG_BIN_PATH = "/users/fadhil/fuse/fuselog";
  private final String FUSELOG_APPLY_BIN_PATH = "/users/fadhil/fuse/apply";

  private final String myNodeId;
  private final Set<IntegerPacketType> packetTypes;

  // TODO: remove this metadata as the port is already stored inside the ServiceInstance.
  private final HashMap<String, Integer> activeServicePorts;

  // FIXME: need to check
  //  (1) multicontainer with primary-backup, how to restart?
  //  (2) multicontainer with primary-backup via FUSE
  //  (3) multicontainer with paxos / active replication
  // private ConcurrentHashMap<String, XDNServiceProperties> serviceProperties;

  // mapping of service name to the current placement epoch
  private final Map<String, Integer> servicePlacementEpoch;

  // mapping of service name to the current instance with its placement epoch
  private final Map<String, Map<Integer, ServiceInstance>> serviceInstances;

  // mapping of service name to the current service instance
  private final Map<String, ServiceInstance> services;

  private final HashMap<String, SocketChannel> fsSocketConnection;
  private final HashMap<String, Boolean> isServiceActive;
  private final String stateDiffRecorderTypeString =
      Config.getGlobalString(ReconfigurationConfig.RC.XDN_PB_STATEDIFF_RECORDER_TYPE);
  private RecorderType recorderType;
  private AbstractStateDiffRecorder stateDiffRecorder;

  // Client to forward HTTP requests into the containerized services.
  // It is specifically designed with Netty, matching HttpActiveReplica, to avoid
  // unnecessary request conversion (e.g., Netty request to OpenJDK request).
  private final XdnHttpForwarderClient httpForwarderClient;

  // HTTP request cache to prevent deserializing an already deserialized request
  // upon coordination. This is useful for the entry replica.
  private static final int REQUEST_CACHE_CAPACITY = 4096;
  private final Map<Long, Request> requestCache =
      Collections.synchronizedMap(
          new LinkedHashMap<>(REQUEST_CACHE_CAPACITY, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<Long, Request> eldest) {
              return size() > REQUEST_CACHE_CAPACITY;
            }
          });

  // LargeCheckpointer for reconfiguration final state
  private final LargeCheckpointer largeCheckpointer;

  // Backdoor HTTP header used to emulate NoOp execution by directly returning dummy response.
  // This is used mainly for measuring the coordination, not execution, overhead.
  private static final String DBG_HDR_NO_OP_EXECUTION = "___DNO";

  private final Logger logger = Logger.getLogger(XdnGigapaxosApp.class.getName());

  public XdnGigapaxosApp(String[] args) {
    System.out.println(">> XDNGigapaxosApp initialization ...");
    assert args.length > 0 : "expecting args for XdnGigapaxosApp";

    this.myNodeId = args[args.length - 1].toLowerCase();
    this.serviceInstances = new ConcurrentHashMap<>();
    this.servicePlacementEpoch = new ConcurrentHashMap<>();
    this.activeServicePorts = new HashMap<>();
    this.services = new ConcurrentHashMap<>();
    this.fsSocketConnection = new HashMap<>();
    this.isServiceActive = new HashMap<>();
    this.httpForwarderClient = new XdnHttpForwarderClient();
    this.largeCheckpointer =
        new LargeCheckpointer(String.format("/tmp/xdn/final/%s/", this.myNodeId), this.myNodeId);

    // Validate and initialize the stateDiff recorder for Primary Backup.
    // We need to check the Operating System as currently FUSE (i.e., Fuselog)
    // is only supported on Linux.
    if (stateDiffRecorderTypeString.equalsIgnoreCase(RecorderType.RSYNC.toString())) {
      recorderType = RecorderType.RSYNC;
    } else if (stateDiffRecorderTypeString.equalsIgnoreCase(RecorderType.FUSELOG.toString())) {
      recorderType = RecorderType.FUSELOG;
    } else if (stateDiffRecorderTypeString.equalsIgnoreCase(RecorderType.ZIP.toString())) {
      recorderType = RecorderType.ZIP;
    } else if (stateDiffRecorderTypeString.equalsIgnoreCase(RecorderType.FUSERUST.toString())) {
      recorderType = RecorderType.FUSERUST;
    } else {
      String errMsg = "[ERROR] Unknown StateDiff recorder type of " + stateDiffRecorderTypeString;
      System.out.println(errMsg);
      throw new RuntimeException(errMsg);
    }

    if (recorderType.equals(RecorderType.FUSELOG) || recorderType.equals(RecorderType.FUSERUST)) {
      String osName = System.getProperty("os.name");
      if (!osName.equalsIgnoreCase("linux")) {
        String errMsg = "[WARNING] FUSE can only be used in Linux. Failing back to rsync.";
        System.out.println(errMsg);
        logger.log(Level.WARNING, errMsg);
        recorderType = RecorderType.RSYNC;
      }
    }

    switch (recorderType) {
      case RSYNC:
        this.stateDiffRecorder = new RsyncStateDiffRecorder(myNodeId);
        break;
      case ZIP:
        this.stateDiffRecorder = new ZipStateDiffRecorder(myNodeId);
        break;
      case FUSELOG:
        this.stateDiffRecorder = new FuselogStateDiffRecorder(myNodeId);
        break;
      case FUSERUST:
        this.stateDiffRecorder = new FuseRustStateDiffRecorder(myNodeId);
        break;
      default:
        String errMessage = "unknown stateDiff recorder " + recorderType;
        throw new RuntimeException(errMessage);
    }

    // Set app request type
    this.packetTypes = new HashSet<>();
    this.packetTypes.add(XdnRequestType.XDN_SERVICE_HTTP_REQUEST);
    this.packetTypes.add(XdnRequestType.XDN_HTTP_REQUEST_BATCH);
    this.packetTypes.add(XdnRequestType.XDN_STOP_REQUEST);
  }

  // TODO: complete this implementation
  public static boolean checkSystemRequirements() {
    boolean isDockerAvailable = isDockerAvailable();
    if (!isDockerAvailable) {
      String exceptionMessage =
          """
                    docker is unavailable for xdn, common reasons include:\s
                    (1) docker is not yet installed,
                    (2) docker needs to be accessed with sudo,
                    (3) docker daemon is not yet started.""";
      throw new RuntimeException(exceptionMessage);
    }

    // TODO:
    //  - Validate fuselog binary exist
    //  - Validate rsync exist
    return true;
  }

  private static boolean isDockerAvailable() {
    String cmd = "docker version";
    int exitCode = Shell.runCommand(cmd);
    return exitCode == 0;
  }

  @Override
  public boolean execute(Request request) {
    long startExecuteTimeNs = System.nanoTime();
    String serviceName = request.getServiceName();

    // Prepare XdnHttpRequest
    boolean isXdnHttpReq = request instanceof XdnHttpRequest;
    XdnHttpRequest xdnHttpRequest = null;
    if (isXdnHttpReq) {
      xdnHttpRequest = (XdnHttpRequest) request;
    }

    // Debug: skip execution and return dummy response to emulate NoOp.
    if (isXdnHttpReq
        && xdnHttpRequest.getHttpRequest().headers().contains(DBG_HDR_NO_OP_EXECUTION)) {
      ByteBuf content = Unpooled.copiedBuffer("NoOp".getBytes());
      FullHttpResponse dummyResponse =
          new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
      dummyResponse.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
      xdnHttpRequest.setHttpResponse(dummyResponse);
      return true;
    }

    // The actual HTTP request execution.
    if (isXdnHttpReq) {
      long preFwdTimeNs = System.nanoTime();
      forwardHttpRequestToContainerizedService(xdnHttpRequest);
      assert xdnHttpRequest.getHttpResponse() != null
          : "Obtained null response after request execution";
      assert ReferenceCountUtil.refCnt(xdnHttpRequest.getHttpResponse()) == 1
          : String.format(
              "Unexpected refCnt of response after execution (%d!=1)",
              ReferenceCountUtil.refCnt(xdnHttpRequest.getHttpResponse()));
      releaseHttpResponseOnNonEntryReplica(xdnHttpRequest);
      long endFwdTimeNs = System.nanoTime();

      requestCache.remove(xdnHttpRequest.getRequestID());
      long endRmCacheTimeNs = System.nanoTime();

      long endTime = System.nanoTime();
      logger.log(
          Level.FINE,
          "{0}:{1} - execution within {2}ms, {3} {4}:{5} " + "(fwd={6}ms rmc={7}ms) [id: {8}]",
          new Object[] {
            this.myNodeId.toLowerCase(),
            this.getClass().getSimpleName(),
            (endTime - startExecuteTimeNs) / 1_000_000.0,
            xdnHttpRequest.getHttpRequest().method(),
            serviceName,
            xdnHttpRequest.getHttpRequest().uri(),
            (endFwdTimeNs - preFwdTimeNs) / 1_000_000.0,
            (endRmCacheTimeNs - endFwdTimeNs) / 1_000_000.0,
            String.valueOf(xdnHttpRequest.getRequestID())
          });
      return true;
    }

    if (request instanceof XdnHttpRequestBatch xdnBatch) {
      long startTime = System.nanoTime();
      forwardHttpRequestBatchToContainerizedService(xdnBatch);
      releaseHttpResponsesOnNonEntryReplica(xdnBatch);
      requestCache.remove(xdnBatch.getRequestID());
      long elapsedTime = System.nanoTime() - startTime;
      logger.log(
          Level.FINE,
          "{0}:{1} - batch execution within {2}ms, size={3} service={4}",
          new Object[] {
            this.myNodeId,
            this.getClass().getSimpleName(),
            (elapsedTime / 1_000_000.0),
            xdnBatch.size(),
            serviceName
          });
      return true;
    }

    if (request instanceof XdnStopRequest stopRequest) {
      int stoppedPlacementEpoch = stopRequest.getEpochNumber();

      boolean isCaptureSuccess =
          this.captureContainerizedServiceFinalState(serviceName, stoppedPlacementEpoch);
      assert isCaptureSuccess : "failed to store the final state before stopping";
      return this.stopContainerizedServiceInstance(serviceName, stoppedPlacementEpoch);
    }

    String exceptionMessage =
        String.format(
            "%s:XdnGigapaxosApp executing unknown request %s",
            this.myNodeId, request.getClass().getSimpleName());
    throw new RuntimeException(exceptionMessage);
  }

  //  Releasing buffer for httpResponse is tricky because
  //  the release depends on the replica's role. If the replica
  //  is the entry replica, then HttpActiveReplica is responsible
  //  to release the response after writing it for the end client.
  //  However, if the replica is not the entry replica, then we can
  //  immediately release the response here as it will be discarded.
  //  .
  //  Our approach is to have a flag inside XdnHttpRequest (i.e., isCreatedFromString)
  //  that indicates whether it is created in HttpActiveReplica (and
  //  is in the entry replica) or it is created by XdnGigapaxosApp.getRequest()
  //  in a non-entry replica.
  //  .
  //  Another complexity is regarding the requests in our cache.
  private void releaseHttpResponseOnNonEntryReplica(XdnHttpRequest xdnHttpRequest) {
    if (xdnHttpRequest == null) return;
    if (xdnHttpRequest.getHttpResponse() == null) return;
    if (xdnHttpRequest.isCreatedFromString()) {
      ReferenceCountUtil.release(xdnHttpRequest.getHttpResponse());
    }
  }

  private void releaseHttpResponsesOnNonEntryReplica(XdnHttpRequestBatch batch) {
    if (batch == null) return;
    if (!batch.isCreatedFromBytes()) return;
    for (var requestWithResponse : batch.getRequestList()) {
      if (requestWithResponse.getHttpResponse() == null) {
        continue;
      }
      assert ReferenceCountUtil.refCnt(requestWithResponse.getHttpResponse()) == 1
          : String.format(
              "Unexpected refCnt of response after execution (%d!=1)",
              ReferenceCountUtil.refCnt(requestWithResponse.getHttpResponse()));
      ReferenceCountUtil.release(requestWithResponse.getHttpResponse());
    }
  }

  private void activate(String serviceName) {
    System.out.println(">> " + myNodeId + " activate...");

    // without FUSE (i.e., using Zip archive, we need the service container to be running
    if (!recorderType.equals(RecorderType.FUSELOG)) {
      return;
    }

    // mount the filesystem
    String containerName = String.format("%s.%s.xdn.io", serviceName, this.myNodeId);
    String stateDirPath = String.format("/tmp/xdn/fuselog/state/%s/", containerName);
    String fsSocketDir = "/tmp/xdn/fuselog/socket/";
    String fsSocketFile = String.format("%s%s.sock", fsSocketDir, containerName);
    String mountCommand =
        String.format("%s -o allow_other -f -s %s", FUSELOG_BIN_PATH, stateDirPath);
    var t =
        new Thread() {
          public void run() {
            Map<String, String> envVars = new HashMap<>();
            envVars.put("FUSELOG_SOCKET_FILE", fsSocketFile);
            int exitCode = runShellCommand(mountCommand, false, envVars);
            if (exitCode != 0) {
              System.err.println("failed to mount filesystem");
              return;
            }
          }
        };
    t.start();

    try {
      TimeUnit.MILLISECONDS.sleep(10);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // connect into filesystem socket
    try {
      UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(fsSocketFile));
      SocketChannel socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
      boolean isConnEstablished = socketChannel.connect(address);
      if (!isConnEstablished) {
        System.err.println("failed to connect to the filesystem");
        return;
      }

      fsSocketConnection.put(serviceName, socketChannel);
    } catch (IOException e) {
      e.printStackTrace();
      return;
    }

    // start container
    String startCommand = String.format("docker start %s", containerName);
    int exitCode = runShellCommand(startCommand, false);
    if (exitCode != 0) {
      System.err.println("failed to start container");
      return;
    }
  }

  @Override
  public boolean execute(Request request, boolean doNotReplyToClient) {
    return this.execute(request);
  }

  @Override
  public String checkpoint(String name) {
    // TODO: implement me
    return "dummyXDNServiceCheckpoint";
  }

  @Override
  public boolean restore(String name, String state) {
    System.out.println(
        ">> XdnGigapaxosApp:" + this.myNodeId + " - restore name=" + name + " state=" + state);

    // A corner case when name is empty, which fundamentally must not happen.
    if (name == null || name.isEmpty()) {
      String exceptionMessage =
          String.format(
              "%s:XdnGigapaxosApp's restore(.) is called " + "with empty service name",
              this.myNodeId);
      throw new RuntimeException(exceptionMessage);
    }

    // Case-1: Gigapaxos started meta service with name == XdnGigaPaxosApp0 and state == {}
    if (name.equals(PaxosConfig.getDefaultServiceName()) && state != null && state.equals("{}")) {
      return true;
    }

    // Case-2: initialize a brand-new service name.
    // Note that in gigapaxos, initialization is a special case of restore
    // with state == initialState. In XDN, the initialState is always started with "xdn:init:".
    // Example of the initState is "xdn:init:bookcatalog:8000:linearizable:true:/app/data",
    if (state != null && state.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)) {
      boolean isServiceCreated = createServiceInstance(name, state);
      if (!isServiceCreated) {
        throw new RuntimeException(
            String.format("%s: Failed to create service %s", this.myNodeId, name));
      }

      boolean isServiceInitialized = initContainerizedService2(name);
      if (isServiceInitialized) isServiceActive.put(name, true);
      return isServiceInitialized;
    }

    // Case-3: restore with null state implies terminating the service. This is used by
    // PaxosInstanceStateMachine.nullifyAppState(.).
    if (state == null) {
      return true;
      // FIXME: should we remove the running container here?
      // return deleteContainerizedService(name);
    }

    // Case-4: the actual restore, i.e., initialize service in new epoch (>0) with state
    // obtained from the latest checkpoint (possibly from different active replica).
    if (state.startsWith(ServiceProperty.XDN_CHECKPOINT_PREFIX)) {
      // TODO: implement me
      throw new RuntimeException("unimplemented! restore(.) with latest checkpoint state");
    }

    // Case-5: handle the reconfiguration case in new higher placement epoch
    if (state.startsWith(ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX)) {
      // format: xdn:final:<epoch>::<serviceProperty>::<finalState>
      String[] raw = state.split("::");
      assert raw.length == 3;
      String encodedServiceProperty = raw[1];
      String encodedFinalState = raw[2]; // FIXME: handle if finalState has :: in it.
      String[] prefixRaw = raw[0].split(":");
      assert prefixRaw.length == 3;
      int prevPlacementEpoch = Integer.parseInt(prefixRaw[2]);
      int newPlacementEpoch = prevPlacementEpoch + 1; // FIXME: this is prone to error!
      return this.reviveContainerizedService(
          name, encodedServiceProperty, encodedFinalState, newPlacementEpoch);
    }

    // Case-6: handle create service for non-deterministic initialization
    if (state.startsWith(ServiceProperty.NON_DETERMINISTIC_CREATE_PREFIX)) {
      state = state.substring(ServiceProperty.NON_DETERMINISTIC_CREATE_PREFIX.length());
      boolean isServiceCreated = createServiceInstance(name, state);
      if (!isServiceCreated) {
        throw new RuntimeException(
            String.format("%s: Failed to create service %s", this.myNodeId, name));
      }

      return isServiceCreated;
    }

    // Case-7: handle start Fuselog in backup (non-deterministic init)
    if (state.startsWith(ServiceProperty.NON_DETERMINISTIC_START_BACKUP_PREFIX)) {
      System.out.println("Running postInitialization() in backup");
      int placementEpoch = 0;

      return this.stateDiffRecorder.postInitialization(name, placementEpoch);
    }

    // Case-8: handle start a created service (non-deterministic init)
    if (state.startsWith(ServiceProperty.NON_DETERMINISTIC_START_PREFIX)) {
      boolean isServiceInitialized = initContainerizedService2(name);
      if (isServiceInitialized) isServiceActive.put(name, true);
      return isServiceInitialized;
    }

    // Unknown cases, should not be triggered
    String exceptionMessage =
        String.format(
            "unknown case for %s:XdnGigapaxosApp's restore(.)" + " with name=%s state=%s",
            this.myNodeId, name, state);
    throw new RuntimeException(exceptionMessage);
  }

  @Override
  public Request getRequest(String stringified) throws RequestParseException {
    XdnRequestType packetType = XdnRequest.getQuickPacketTypeFromEncodedPacket(stringified);
    assert packetType != null
        : "Unknown XDN Request handled by XdnGigapaxosApp. Request: '" + stringified + "'";

    if (packetType.equals(XdnRequestType.XDN_SERVICE_HTTP_REQUEST)) {
      Long cachedId = XdnHttpRequest.parseRequestIdQuickly(stringified);
      if (cachedId != null) {
        Request cached = requestCache.get(cachedId);
        if (cached instanceof XdnHttpRequest xdnHttpRequest) {
          if (xdnHttpRequest.getHttpResponse() == null
              && XdnHttpRequest.doesHasResponse(stringified)) {
            HttpResponse parsedResponse = XdnHttpRequest.parseHttpResponse(stringified);
            if (parsedResponse != null) {
              ((XdnHttpRequest) cached).setHttpResponse(parsedResponse);
            }
          }
          return cached;
        }
      }
      XdnHttpRequest httpRequest = XdnHttpRequest.createFromString(stringified);
      return configureHttpRequest(httpRequest);
    }

    if (packetType.equals(XdnRequestType.XDN_HTTP_REQUEST_BATCH)) {
      Long cachedId = XdnHttpRequestBatch.parseRequestIdQuickly(stringified);
      if (cachedId != null) {
        Request cached = requestCache.get(cachedId);
        if (cached instanceof XdnHttpRequestBatch) {
          // TODO: handle parsed response quickly.
          return cached;
        }
      }
      XdnHttpRequestBatch batch =
          XdnHttpRequestBatch.createFromBytes(stringified.getBytes(StandardCharsets.ISO_8859_1));
      for (XdnHttpRequest request : batch.getRequestList()) {
        configureHttpRequest(request);
      }
      return batch;
    }

    if (packetType.equals(XdnRequestType.XDN_STOP_REQUEST)) {
      return XdnStopRequest.createFromString(stringified);
    }

    throw new RuntimeException("Unknown encoded request or packet format: " + stringified);
  }

  public void cacheRequest(Request request) {
    if (request == null) {
      return;
    }
    if (request instanceof XdnHttpRequest xdnHttpRequest) {
      requestCache.put(xdnHttpRequest.getRequestID(), xdnHttpRequest);
    } else if (request instanceof XdnHttpRequestBatch batch) {
      requestCache.put(batch.getRequestID(), batch);
    }
  }

  private XdnHttpRequest configureHttpRequest(XdnHttpRequest httpRequest) {
    if (httpRequest == null) {
      return null;
    }

    String serviceName = httpRequest.getServiceName();
    Map<Integer, ServiceInstance> currServiceInstance = this.serviceInstances.get(serviceName);
    if (currServiceInstance == null) {
      logger.log(Level.WARNING, "Deserializing http request for unknown service " + serviceName);
      return httpRequest;
    }
    Integer currServicePlacementEpoch = this.servicePlacementEpoch.get(serviceName);
    if (currServicePlacementEpoch == null) {
      logger.log(
          Level.WARNING, "Deserializing http request for unknown epoch of service " + serviceName);
      return httpRequest;
    }
    ServiceInstance currInstance = currServiceInstance.get(currServicePlacementEpoch);
    if (currInstance == null) {
      logger.log(
          Level.WARNING,
          "Deserializing http request for unknown instance of epoch "
              + currServicePlacementEpoch
              + " from service "
              + serviceName);
    } else {
      ServiceProperty serviceProperty = currInstance.property;
      httpRequest.setRequestMatchers(serviceProperty.getRequestMatchers());
    }
    return httpRequest;
  }

  @Override
  public Set<IntegerPacketType> getRequestTypes() {
    return this.packetTypes;
  }

  /**
   * initContainerizedService initializes a containerized service, in idempotent manner.
   *
   * @param serviceName name of the to-be-initialized service.
   * @param initialState the initial state with "init:" prefix.
   * @return false if failed to initialized the service.
   */
  private boolean initContainerizedService(String serviceName, String initialState) {

    // if the service is already initialized previously, in this active replica,
    // then stop and remove the previous service.
    if (activeServicePorts.containsKey(serviceName)) {
      boolean isSuccess = false;
      isSuccess = stopContainer(serviceName);
      if (!isSuccess) {
        return false;
      }
      isSuccess = removeContainer(serviceName);
      if (!isSuccess) {
        return false;
      }
    }

    // it is possible to have the previous service still running, but the serviceName
    // does not exist in our memory, this is possible when XDN crash while docker is still
    // running.
    if (isContainerRunning(serviceName)) {
      boolean isSuccess = false;
      isSuccess = stopContainer(serviceName);
      if (!isSuccess) {
        return false;
      }
      isSuccess = removeContainer(serviceName);
      if (!isSuccess) {
        return false;
      }
    }

    // decode and validate the initial state
    String[] decodedInitialState = initialState.split(":");
    if (decodedInitialState.length < 7 || !initialState.startsWith("xdn:init:")) {
      System.err.println(
          "incorrect initial state, example of expected state is"
              + " 'xdn:init:bookcatalog:8000:linearizable:true:/app/data'");
      return false;
    }
    String dockerImageNames = decodedInitialState[2];
    String dockerPortStr = decodedInitialState[3];
    String consistencyModel = decodedInitialState[4];
    boolean isDeterministic = decodedInitialState[5].equalsIgnoreCase("true");
    String stateDir = decodedInitialState[6];
    int dockerPort = 0;
    try {
      dockerPort = Integer.parseInt(dockerPortStr);
    } catch (NumberFormatException e) {
      System.err.println("incorrect docker port: " + e);
      return false;
    }

    // TODO: assign port systematically to avoid port conflict
    int publicPort = getRandomNumber(50000, 65000);

    XdnServiceProperties prop = new XdnServiceProperties();
    prop.serviceName = serviceName;
    prop.dockerImages.addAll(List.of(dockerImageNames.split(",")));
    prop.exposedPort = dockerPort;
    prop.consistencyModel = consistencyModel;
    prop.isDeterministic = isDeterministic;
    prop.stateDir = stateDir;
    prop.mappedPort = publicPort;

    // create docker network, via command line
    String networkName = String.format("net::%s:%s", myNodeId, prop.serviceName);
    int exitCode = createDockerNetwork(networkName);
    if (exitCode != 0) {
      return false;
    }

    // actually start the containerized service, via command line
    boolean isSuccess = startContainer(prop, networkName);
    if (!isSuccess) {
      return false;
    }

    // store the service's public port for request forwarding.
    activeServicePorts.put(serviceName, publicPort);
    // serviceProperties.put(serviceName, prop);

    return true;
  }

  /**
   * Create service instance from initial state. The service is not started and is assigned an empty
   * port number.
   *
   * @param serviceName name of the to-be-initialized service.
   * @param initialState the initial state with "xdn:init:" prefix.
   */
  private boolean createServiceInstance(String serviceName, String initialState) {
    if (initialState.startsWith(ServiceProperty.NON_DETERMINISTIC_CREATE_PREFIX)) {
      initialState =
          initialState.substring(ServiceProperty.NON_DETERMINISTIC_CREATE_PREFIX.length());
    }

    if (initialState.startsWith(ServiceProperty.NON_DETERMINISTIC_START_BACKUP_PREFIX)) {
      initialState =
          initialState.substring(ServiceProperty.NON_DETERMINISTIC_START_BACKUP_PREFIX.length());
    }

    // validate the initial state
    boolean isReconfiguration = false;
    if (initialState.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)) {
      initialState = initialState.substring(ServiceProperty.XDN_INITIAL_STATE_PREFIX.length());
    } else if (initialState.startsWith(ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX)) {
      isReconfiguration = true;
    } else {
      throw new RuntimeException("Invalid initial state");
    }

    ServiceProperty property = null;
    String networkName = String.format("net::%s:%s", myNodeId, serviceName);
    ServiceInstance service;
    int initialPlacementEpoch = 0;

    if (isReconfiguration) {
      // format: xdn:final:<epoch>::<serviceProperty>::<finalState>
      String[] raw = initialState.split("::");
      assert raw.length == 3;
      String encodedServiceProperty = raw[1];
      String encodedFinalState = raw[2]; // FIXME: handle if finalState has :: in it.
      String[] prefixRaw = raw[0].split(":");
      assert prefixRaw.length == 3;
      int prevPlacementEpoch = Integer.parseInt(prefixRaw[2]);
      int newPlacementEpoch = prevPlacementEpoch + 1; // FIXME: this is prone to error!
      initialPlacementEpoch = newPlacementEpoch;

      try {
        property = ServiceProperty.createFromJsonString(encodedServiceProperty);
      } catch (JSONException e) {
        throw new RuntimeException(e);
      }

      // prepare statediff directory, if required
      String stateDirMountSource =
          stateDiffRecorder.getTargetDirectory(serviceName, newPlacementEpoch);
      String stateDirMountTarget = property.getStatefulComponentDirectory();
      stateDiffRecorder.preInitialization(serviceName, newPlacementEpoch);

      // Validates the previous epoch final state, then put it into the to-be-mounted dir.
      // First, prepare the mounted dir.
      stateDiffRecorder.removeServiceRecorder(serviceName, newPlacementEpoch);
      String mountDir = stateDiffRecorder.getTargetDirectory(serviceName, newPlacementEpoch);
      String removeDirCommand = String.format("rm -rf %s", mountDir);
      int rmDirRetCode = Shell.runCommand(removeDirCommand);
      assert rmDirRetCode == 0;
      String createDirCommand = String.format("mkdir -p %s", mountDir);
      int mkDirRetCode = Shell.runCommand(createDirCommand);
      assert mkDirRetCode == 0;

      // write the previous epoch final state into .tar file
      String destinationDirPath =
          String.format("/tmp/xdn/final/%s/%s/", this.myNodeId, serviceName);
      int code = Shell.runCommand(String.format("mkdir -p %s ", destinationDirPath));
      assert code == 0;
      String destinationTarFilePath =
          String.format(
              "/tmp/xdn/final/%s/%s/rcv_final_state::%d.tar",
              this.myNodeId, serviceName, newPlacementEpoch);

      // if < 500 Kb
      if (encodedFinalState.startsWith("url:")) {
        String finalStateUrl = encodedFinalState.substring("url:".length());
        LargeCheckpointer.restoreCheckpointHandle(finalStateUrl, destinationTarFilePath);
      } else {
        byte[] prevEpochFinalStateBytes = Base64.getDecoder().decode(encodedFinalState);
        try {
          Files.write(
              Paths.get(destinationTarFilePath),
              prevEpochFinalStateBytes,
              StandardOpenOption.CREATE,
              StandardOpenOption.DSYNC);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }

      // un-archive the .tar file into the target mount dir
      ZipFiles.unzip(destinationTarFilePath, mountDir);

      List<String> containerNames = new ArrayList<>();
      int idx = 0;
      for (ServiceComponent c : property.getComponents()) {
        String containerName =
            String.format("c%d.e%d.%s.%s.xdn.io", idx, newPlacementEpoch, serviceName, myNodeId);
        containerNames.add(containerName);
        idx++;
      }

      int allocatedPort = getRandomPort();
      service =
          new ServiceInstance(property, serviceName, networkName, allocatedPort, containerNames);
    } else {

      try {
        property = ServiceProperty.createFromJsonString(initialState);
      } catch (JSONException e) {
        throw new RuntimeException("Invalid initial state as JSON: " + e);
      }

      stateDiffRecorder.preInitialization(serviceName, initialPlacementEpoch);
      // Prepares container names for each service component.
      // Format  : c<component-id>.e<reconfiguration-epoch>.<service-name>.<node-id>.xdn.io
      // Example : c0.e2.bookcatalog.ar2.xdn.io
      List<String> containerNames = new ArrayList<>();
      int idx = 0;
      int epoch = 0;
      for (ServiceComponent c : property.getComponents()) {
        String containerName =
            String.format("c%d.e%d.%s.%s.xdn.io", idx, epoch, serviceName, myNodeId);
        containerNames.add(containerName);
        idx++;
      }

      // prepare the initialized service
      service = new ServiceInstance(property, serviceName, networkName, containerNames);
    }

    // store service
    this.services.put(serviceName, service);
    this.servicePlacementEpoch.put(serviceName, initialPlacementEpoch);

    if (this.serviceInstances.containsKey(serviceName)) {
      this.serviceInstances.get(serviceName).put(initialPlacementEpoch, service);
    } else {
      Map<Integer, ServiceInstance> epochToInstanceMap = new ConcurrentHashMap<>();
      epochToInstanceMap.put(initialPlacementEpoch, service);
      this.serviceInstances.put(serviceName, epochToInstanceMap);
    }

    logger.log(
        Level.FINE,
        "{0}:{1} created {2} ServiceInstance for {3}",
        new Object[] {
          this.myNodeId.toUpperCase(),
          this.getClass().getSimpleName(),
          serviceName,
          (isReconfiguration ? "reconfiguration" : "initialization")
        });

    return true;
  }

  /**
   * initContainerizedService2 initializes a containerized service, in idempotent manner. This is a
   * new implementation of initContainerizedService, but with support on service with multiple
   * container components.
   *
   * @param serviceName name of the to-be-initialized service.
   * @return false if failed to initialize the service.
   */
  private boolean initContainerizedService2(String serviceName) {
    int initialPlacementEpoch = this.servicePlacementEpoch.get(serviceName);
    assertNotNull("initialPlacementEpoch must not be null", initialPlacementEpoch);

    // decode the initial state, containing the service property
    ServiceInstance service = this.serviceInstances.get(serviceName).get(initialPlacementEpoch);
    if (service == null) {
      throw new RuntimeException("Service instance not found");
    }

    if (service.initializationSucceed) {
      logger.log(
          Level.FINE,
          "{0}:{1} initContainerizedService2 {2} service already initialized. Cancelling new"
              + " initialization",
          new Object[] {this.myNodeId.toUpperCase(), this.getClass().getSimpleName(), serviceName});
      return true;
    }

    int allocatedPort = getRandomPort();
    service.allocatedHttpPort = allocatedPort;

    // Remove already running containers, if any
    for (String name : service.containerNames) {
      String command = String.format("docker rm -f %s", name);
      int code = Shell.runCommand(command, true);
      if (code == 0) {
        logger.log(
            Level.INFO,
            "{0}:{1} failed to remove {2}. Container doesn't exist",
            new Object[] {this.myNodeId.toUpperCase(), this.getClass().getSimpleName(), name});
      } else if (code == 1) {
        logger.log(
            Level.INFO,
            "{0}:{1} - {2} successfully removed",
            new Object[] {this.myNodeId.toUpperCase(), this.getClass().getSimpleName(), name});
      }
    }

    // create docker network, via command line
    int exitCode = createDockerNetwork(service.networkName);
    if (exitCode != 0) {
      return false;
    }

    // TODO: prepare statediff directory, if required
    String stateDirMountSource =
        stateDiffRecorder.getTargetDirectory(serviceName, initialPlacementEpoch);
    String stateDirMountTarget = service.property.getStatefulComponentDirectory();

    // TODO: Fix ordering. Must be:
    // preInitialization -> startContainer -> postInitialization
    stateDiffRecorder.preInitialization(serviceName, initialPlacementEpoch);
    stateDiffRecorder.postInitialization(serviceName, initialPlacementEpoch);

    // actually start the service, run each component as container, in the same order as they
    // are specified in the declared service property.
    int idx = 0;
    for (ServiceComponent c : service.property.getComponents()) {
      boolean isSuccess =
          startContainer(
              c.getImageName(),
              service.containerNames.get(idx),
              service.networkName,
              c.getComponentName(),
              c.getExposedPort(),
              c.getEntryPort(),
              c.isEntryComponent() ? allocatedPort : null,
              c.isStateful() ? stateDirMountSource : null,
              c.isStateful() ? stateDirMountTarget : null,
              c.getEnvironmentVariables());
      if (!isSuccess) {
        throw new RuntimeException(
            "failed to start container for component " + c.getComponentName());
      }

      idx++;
    }

    // Handle non-deterministic initialization,
    //  e.g., a node that initialize a filename with current time or random number.
    if (!service.property.isDeterministic()) {
      System.err.println("WARNING:  " + "initial state");
      logger.log(
          Level.WARNING,
          "{0}:{1} non-deterministic service {2} can generate different initial state",
          new Object[] {
            this.myNodeId.toUpperCase(), this.getClass().getSimpleName(), service.serviceName
          });
    } else {
      stateDiffRecorder.postInitialization(serviceName, initialPlacementEpoch);
      service.initializationSucceed = true;
    }

    // store all the current service metadata
    this.activeServicePorts.put(serviceName, allocatedPort);
    return true;
  }

  private boolean isContainerRunning(String containerName) {
    // If container is already running, the docker inspect command will return 0.
    String inspectCommand = String.format("docker inspect --type=container %s", containerName);
    int code = Shell.runCommand(inspectCommand, true);
    return code != 0;
  }

  /**
   * Revives containerized service instance with a new higher placement-epoch.
   *
   * @param serviceName
   * @param encodedServiceProperty
   * @param prevEpochFinalState
   * @param placementEpoch
   * @return
   */
  private boolean reviveContainerizedService(
      String serviceName,
      String encodedServiceProperty,
      String prevEpochFinalState,
      int placementEpoch) {
    assert serviceName != null && !serviceName.isEmpty();
    assert encodedServiceProperty != null && !encodedServiceProperty.isEmpty();
    assert placementEpoch > 0 : "expecting non initial epoch";

    Integer currEpoch = this.getEpoch(serviceName);
    if (currEpoch != null && currEpoch >= placementEpoch) {
      return true;
    }

    System.out.printf(
        ">> %s:XdnGigapaxosApp reviveService name=%s epoch=%d state=%s\n",
        this.myNodeId, serviceName, placementEpoch, prevEpochFinalState);

    // Validate and parse the encoded service property
    ServiceProperty property;
    try {
      property = ServiceProperty.createFromJsonString(encodedServiceProperty);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }

    // prepare statediff directory, if required
    String stateDirMountSource = stateDiffRecorder.getTargetDirectory(serviceName, placementEpoch);
    String stateDirMountTarget = property.getStatefulComponentDirectory();
    stateDiffRecorder.preInitialization(serviceName, placementEpoch);

    // Validates the previous epoch final state, then put it into the to-be-mounted dir.
    // First, prepare the mounted dir.
    // TODO: test with fuse
    stateDiffRecorder.removeServiceRecorder(serviceName, placementEpoch);
    String mountDir = stateDiffRecorder.getTargetDirectory(serviceName, placementEpoch);
    String removeDirCommand = String.format("rm -rf %s", mountDir);
    int rmDirRetCode = Shell.runCommand(removeDirCommand);
    assert rmDirRetCode == 0;
    String createDirCommand = String.format("mkdir -p %s", mountDir);
    int mkDirRetCode = Shell.runCommand(createDirCommand);
    assert mkDirRetCode == 0;

    // write the previous epoch final state into .tar file
    String destinationDirPath = String.format("/tmp/xdn/final/%s/%s/", this.myNodeId, serviceName);
    int code = Shell.runCommand(String.format("mkdir -p %s ", destinationDirPath));
    assert code == 0;
    String destinationTarFilePath =
        String.format(
            "/tmp/xdn/final/%s/%s/rcv_final_state::%d.tar",
            this.myNodeId, serviceName, placementEpoch);
    byte[] prevEpochFinalStateBytes = Base64.getDecoder().decode(prevEpochFinalState);
    try {
      Files.write(
          Paths.get(destinationTarFilePath),
          prevEpochFinalStateBytes,
          StandardOpenOption.CREATE,
          StandardOpenOption.DSYNC);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // un-archive the .tar file into the target mount dir
    ZipFiles.unzip(destinationTarFilePath, mountDir);

    // Prepares container names for each service component.
    // Format  : c<component-id>.e<reconfiguration-epoch>.<service-name>.<node-id>.xdn.io
    // Example : c0.e2.bookcatalog.ar2.xdn.io
    List<String> containerNames = new ArrayList<>();
    int idx = 0;
    for (ServiceComponent c : property.getComponents()) {
      String containerName =
          String.format("c%d.e%d.%s.%s.xdn.io", idx, placementEpoch, serviceName, myNodeId);
      containerNames.add(containerName);
      idx++;
    }

    // prepare the initialized service
    String networkName = String.format("net::%s:%s", myNodeId, serviceName);
    int allocatedPort = getRandomPort();
    ServiceInstance service =
        new ServiceInstance(property, serviceName, networkName, allocatedPort, containerNames);

    // create docker network, via command line
    int exitCode = createDockerNetwork(networkName);
    if (exitCode != 0) {
      return false;
    }

    // actually start the service, run each component as container, in the same order as they
    // are specified in the declared service property.
    idx = 0;
    for (ServiceComponent c : property.getComponents()) {
      boolean isSuccess =
          startContainer(
              c.getImageName(),
              containerNames.get(idx),
              networkName,
              c.getComponentName(),
              c.getExposedPort(),
              c.getEntryPort(),
              c.isEntryComponent() ? allocatedPort : null,
              c.isStateful() ? stateDirMountSource : null,
              c.isStateful() ? stateDirMountTarget : null,
              c.getEnvironmentVariables());
      if (!isSuccess) {
        throw new RuntimeException(
            "failed to start container for component " + c.getComponentName());
      }

      idx++;
    }

    // TODO: need to handle non-deterministic initialization,
    //  e.g., a node that initialize a filename with current time or random number.
    if (!property.isDeterministic()) {
      System.err.println(
          "WARNING: non-deterministic service can generate different " + "initial state");
    }
    stateDiffRecorder.postInitialization(serviceName, placementEpoch);

    this.services.put(serviceName, service);
    this.activeServicePorts.put(serviceName, allocatedPort);

    // store the service placement epoch metadata
    this.servicePlacementEpoch.put(serviceName, placementEpoch);
    Map<Integer, ServiceInstance> epochToInstanceMap =
        this.serviceInstances.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>());
    epochToInstanceMap.put(placementEpoch, service);

    return true;
  }

  private boolean deleteContainerizedServiceInstance(String serviceName, int placementEpoch) {
    assert serviceName != null && !serviceName.isEmpty();
    assert placementEpoch >= 0;
    System.out.println(
        ">>> Deleting a containerized service name=" + serviceName + " epoch=" + placementEpoch);

    // validate and get the service metadata for the provided serviceName and placementEpoch
    if (!this.serviceInstances.containsKey(serviceName)) {
      Logger.getGlobal()
          .log(
              Level.WARNING,
              "Ignoring delete request for unknown service with name=" + serviceName);
      return true;
    }
    ServiceInstance serviceInstance = this.serviceInstances.get(serviceName).get(placementEpoch);
    if (serviceInstance == null) {
      Logger.getGlobal()
          .log(
              Level.WARNING,
              String.format(
                  "Ignoring delete request for unknown " + "epoch=%d for service with name=%s",
                  placementEpoch, serviceName));
      return true;
    }

    // get the container names and mount dir, then remove the in-memory service metadata
    List<String> toBeRemovedContainerNames = serviceInstance.containerNames;
    String toBeRemovedMountDir = stateDiffRecorder.getTargetDirectory(serviceName, placementEpoch);
    this.serviceInstances.get(serviceName).remove(placementEpoch);
    if (this.serviceInstances.get(serviceName).isEmpty()) {
      this.serviceInstances.remove(serviceName);
    }

    // Terminate service containers for this epoch
    for (String containerName : toBeRemovedContainerNames) {
      boolean isSuccess = this.removeContainer(containerName);
      assert isSuccess : "failed to remove container " + containerName;
    }

    // clean the mounted dir for this epoch
    stateDiffRecorder.removeServiceRecorder(serviceName, placementEpoch);
    String removeDirCommand = String.format("rm -rf %s", toBeRemovedMountDir);
    int code = Shell.runCommand(removeDirCommand);
    assert code == 0;

    logger.log(
        Level.FINE,
        "{0}:{1} successfully deleted ServiceInstance for {2}:{3}",
        new Object[] {
          this.myNodeId.toUpperCase(), this.getClass().getSimpleName(), serviceName, placementEpoch
        });

    return true;
  }

  private boolean stopContainerizedServiceInstance(String serviceName, int placementEpoch) {
    assert serviceName != null && !serviceName.isEmpty();
    assert placementEpoch >= 0;

    System.out.printf(
        ">> %s:XdnGigapaxosApp stopServiceInstance name=%s epoch=%d\n",
        this.myNodeId, serviceName, placementEpoch);

    // validate and get the service metadata for the provided serviceName and placementEpoch
    if (!this.serviceInstances.containsKey(serviceName)) {
      Logger.getGlobal()
          .log(Level.WARNING, "Ignoring stop request for unknown service with name=" + serviceName);
      return true;
    }
    ServiceInstance serviceInstance = this.serviceInstances.get(serviceName).get(placementEpoch);
    if (serviceInstance == null) {
      Logger.getGlobal()
          .log(
              Level.WARNING,
              String.format(
                  "Ignoring stop request for unknown " + "epoch=%d for service with name=%s",
                  placementEpoch, serviceName));
      return true;
    }

    List<String> toBeStoppedContainerNames = serviceInstance.containerNames;
    for (String containerName : toBeStoppedContainerNames) {
      boolean isSuccess = this.stopContainer(containerName);
      assert isSuccess : "failed to stop container " + containerName;
    }

    logger.log(
        Level.FINE,
        "{0}:{1} successfully stopped docker instance for {2}:{3}",
        new Object[] {
          this.myNodeId.toUpperCase(), this.getClass().getSimpleName(), serviceName, placementEpoch
        });

    return true;
  }

  /** startContainer runs the bash command below to start running a docker container. */
  private boolean startContainer(XdnServiceProperties properties, String networkName) {
    if (recorderType.equals(RecorderType.FUSELOG)) {
      return startContainerWithFSMount(properties, networkName);
    }

    String containerName = String.format("%s.%s.xdn.io", properties.serviceName, this.myNodeId);
    String startCommand =
        String.format(
            "docker run -d --name=%s --network=%s --publish=%d:%d %s",
            containerName,
            networkName,
            properties.mappedPort,
            properties.exposedPort,
            properties.dockerImages.get(0));
    int exitCode = runShellCommand(startCommand, false);
    if (exitCode != 0) {
      System.err.println("failed to start container");
      return false;
    }

    return true;
  }

  private boolean startContainerWithFSMount(XdnServiceProperties properties, String networkName) {
    String containerName = String.format("%s.%s.xdn.io", properties.serviceName, this.myNodeId);

    // remove previous directory, if exist
    String stateDirPath = String.format("/tmp/xdn/fuselog/state/%s/", containerName);
    String cleanupCommand = String.format("rm -rf %s", stateDirPath);
    int exitCode = runShellCommand(cleanupCommand, false);
    if (exitCode != 0) {
      System.err.println("failed to remove previous state directory");
      return false;
    }

    // prepare state directory
    String mkdirCommand = String.format("mkdir -p %s", stateDirPath);
    exitCode = runShellCommand(mkdirCommand, false);
    if (exitCode != 0) {
      System.err.println("failed to create state directory");
      return false;
    }

    // TODO: copy initial state from the image into the host directory

    // prepare socket file for the filesystem
    String fsSocketDir = "/tmp/xdn/fuselog/socket/";
    String fsSocketFile = String.format("%s%s.sock", fsSocketDir, containerName);
    mkdirCommand = String.format("mkdir -p %s", fsSocketDir);
    exitCode = runShellCommand(mkdirCommand, false);
    if (exitCode != 0) {
      System.err.println("failed to create socket directory");
      return false;
    }

    // mount the filesystem
    String mountCommand =
        String.format("%s -o allow_other -f -s %s", FUSELOG_BIN_PATH, stateDirPath);
    var t =
        new Thread() {
          public void run() {
            Map<String, String> envVars = new HashMap<>();
            envVars.put("FUSELOG_SOCKET_FILE", fsSocketFile);
            int exitCode = runShellCommand(mountCommand, false, envVars);
            if (exitCode != 0) {
              System.err.println("failed to mount filesystem");
              return;
            }
          }
        };
    t.start();

    try {
      TimeUnit.MILLISECONDS.sleep(10);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }

    // establish connection to the filesystem
    try {
      UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(fsSocketFile));
      SocketChannel socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
      boolean isConnEstablished = socketChannel.connect(address);
      if (!isConnEstablished) {
        System.err.println("failed to connect to the filesystem");
        return false;
      }

      fsSocketConnection.put(properties.serviceName, socketChannel);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // start the docker container
    String startCommand =
        String.format(
            "docker run -d --name=%s --network=%s --publish=%d:%d"
                + " --mount type=bind,source=%s,target=%s %s",
            containerName,
            networkName,
            properties.mappedPort,
            properties.exposedPort,
            stateDirPath,
            properties.stateDir,
            properties.dockerImages.get(0));
    exitCode = runShellCommand(startCommand, false);
    if (exitCode != 0) {
      System.err.println("failed to start container");
      return false;
    }

    return true;
  }

  private boolean stopContainer(String containerName) {
    String stopCommand = String.format("docker container stop %s", containerName);
    int exitCode = runShellCommand(stopCommand, true);
    if (exitCode != 0 && exitCode != 1) {
      // 1 is the exit code of stopping a non-existent container
      System.err.println("Failed to stop container");
      return false;
    }

    return true;
  }

  private boolean removeContainer(String containerName) {
    String removeCommand = String.format("docker container rm --force %s", containerName);
    int exitCode = runShellCommand(removeCommand, true);
    if (exitCode != 0 && exitCode != 1) {
      // 1 is the exit code of removing a non-existent container
      System.err.println("Failed to remove container");
      return false;
    }

    return true;
  }

  private int createDockerNetwork(String networkName) {
    String createNetCmd = String.format("docker network create %s", networkName);
    int exitCode = runShellCommand(createNetCmd, true);
    if (exitCode != 0 && exitCode != 1) {
      // 1 is the exit code of creating already exist network
      System.err.println("Error: failed to create network");
      return exitCode;
    }
    return 0;
  }

  private String copyContainerDirectory(String serviceName) {
    ServiceInstance service = services.get(serviceName);
    if (service == null) {
      throw new RuntimeException("unknown service " + serviceName);
    }

    // gather the required service properties
    String serviceStatediffName = String.format("%s.%s.xdn.io", serviceName, myNodeId);
    String statediffDirPath = String.format("/tmp/xdn/statediff/%s", serviceStatediffName);
    String statediffZipPath = String.format("/tmp/xdn/zip/%s.zip", serviceStatediffName);

    // create /tmp/xdn/statediff/ and /tmp/xdn/zip/ directories, if needed
    createXDNStatediffDirIfNotExist();

    // remove previous statediff, if any
    int exitCode = runShellCommand(String.format("rm -rf %s", statediffDirPath), false);
    if (exitCode != 0) {
      System.err.println("failed to remove previous statediff");
      return null;
    }

    // remove previous statediff archive, if any
    exitCode = runShellCommand(String.format("rm -rf %s", statediffZipPath), false);
    if (exitCode != 0) {
      System.err.println("failed to remove previous statediff archive");
      return null;
    }

    // get the statediff into statediffDirPath
    String command =
        String.format(
            "docker cp %s:%s %s",
            service.statefulContainer, service.stateDirectory + "/.", statediffDirPath + "/");
    exitCode = runShellCommand(command, false);
    if (exitCode != 0) {
      System.err.println("failed to copy statediff");
      return "null";
    }

    // archive the statediff into statediffZipPath
    ZipFiles.zipDirectory(new File(statediffDirPath), statediffZipPath);

    // convert the archive into String
    try {
      byte[] statediffBytes = Files.readAllBytes(Path.of(statediffZipPath));
      return String.format(
          "xdn:statediff:%s:%s",
          this.myNodeId, new String(statediffBytes, StandardCharsets.ISO_8859_1));
    } catch (IOException e) {
      e.printStackTrace();
      return null;
    }
  }

  private String captureStatediffWithFuse(XdnServiceProperties properties) {
    try {
      SocketChannel fsConn = fsSocketConnection.get(properties.serviceName);

      // send get statediff command (g) to the filesystem
      fsConn.write(ByteBuffer.wrap("g\n".getBytes()));

      // wait for response indicating the size of the statediff
      ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
      sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
      sizeBuffer.clear();
      System.out.println(">> reading response ...");
      int numRead = fsConn.read(sizeBuffer);
      if (numRead < 8) {
        System.err.println("failed to read size of the statediff");
        return null;
      }
      long statediffSize = sizeBuffer.getLong(0);
      System.out.println(">> statediff size=" + statediffSize);

      // read all the statediff
      ByteBuffer statediffBuffer = ByteBuffer.allocate((int) statediffSize);
      numRead = 0;
      while (numRead < statediffSize) {
        numRead += fsConn.read(statediffBuffer);
      }
      System.out.println("complete reading statediff ...");

      return String.format(
          "xdn:statediff:%s:%s",
          this.myNodeId, new String(statediffBuffer.array(), StandardCharsets.ISO_8859_1));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void createXDNStatediffDirIfNotExist() {
    try {
      String xdnDirPath = "/tmp/xdn";
      String xdnStateDirPath = "/tmp/xdn/state";
      Files.createDirectories(Paths.get(xdnDirPath));
      Files.createDirectories(Paths.get(xdnStateDirPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      String statediffDirPath = "/tmp/xdn/statediff";
      Files.createDirectories(Paths.get(statediffDirPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    try {
      String statediffDirPath = "/tmp/xdn/zip";
      Files.createDirectories(Paths.get(statediffDirPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Retrieves docker container IDs for all components in the service by inspecting each known
   * container name.
   */
  public List<String> getContainerIds(String serviceName) {
    ServiceInstance service = services.get(serviceName);
    if (service == null) {
      return null;
    }

    List<String> containerIds = new ArrayList<>(service.containerNames.size());
    for (String containerName : service.containerNames) {
      ProcessBuilder pb =
          new ProcessBuilder(
              "docker", "container", "inspect", "--format", "{{.Id}}", containerName);
      pb.redirectErrorStream(true);
      try {
        Process process = pb.start();
        String output;
        try (InputStream inputStream = process.getInputStream()) {
          output = new String(inputStream.readAllBytes(), StandardCharsets.ISO_8859_1).trim();
        }
        int exitCode = process.waitFor();
        if (exitCode == 0 && !output.isEmpty()) {
          containerIds.add(output);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    return containerIds;
  }

  /**
   * Get human readable uptime duration for all components in the service.
   *
   * @param serviceName Name of the service to get container uptimes for
   * @return List of container uptime durations (e.g. "5 minutes ago", "3 days ago"), or null if
   *     service not found
   */
  public List<String> getContainerCreatedAtInfo(String serviceName) {
    ServiceInstance service = services.get(serviceName);
    if (service == null) {
      return null;
    }

    List<String> createdAtInfo = new ArrayList<>(service.containerNames.size());
    for (String containerName : service.containerNames) {
      ProcessBuilder pb =
          new ProcessBuilder(
              "docker", "container", "inspect", "--format", "{{.State.StartedAt}}", containerName);
      pb.redirectErrorStream(true);
      try {
        Process process = pb.start();
        String output;
        try (InputStream inputStream = process.getInputStream()) {
          output = new String(inputStream.readAllBytes(), StandardCharsets.ISO_8859_1).trim();
        }
        int exitCode = process.waitFor();
        if (exitCode == 0 && !output.isEmpty()) {
          // Parse the ISO 8601 timestamp and convert to human readable format
          try {
            Instant startedAt = Instant.parse(output);
            Duration duration = Duration.between(startedAt, Instant.now());

            // Format duration in human readable form
            String humanReadable;
            if (duration.toDays() > 365) {
              long years = duration.toDays() / 365;
              humanReadable = years + (years == 1 ? " year" : " years") + " ago";
            } else if (duration.toDays() > 30) {
              long months = duration.toDays() / 30;
              humanReadable = months + (months == 1 ? " month" : " months") + " ago";
            } else if (duration.toDays() > 0) {
              long days = duration.toDays();
              humanReadable = days + (days == 1 ? " day" : " days") + " ago";
            } else if (duration.toHours() > 0) {
              long hours = duration.toHours();
              humanReadable = hours + (hours == 1 ? " hour" : " hours") + " ago";
            } else if (duration.toMinutes() > 0) {
              long minutes = duration.toMinutes();
              humanReadable = minutes + (minutes == 1 ? " minute" : " minutes") + " ago";
            } else {
              long seconds = duration.getSeconds();
              humanReadable = seconds + (seconds == 1 ? " second" : " seconds") + " ago";
            }
            createdAtInfo.add(humanReadable);
          } catch (DateTimeParseException e) {
            // If parsing fails, just add the original timestamp
            createdAtInfo.add(output);
          }
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    return createdAtInfo;
  }

  /**
   * Get container status for all components in the service.
   *
   * @param serviceName Name of the service to get container status for
   * @return List of container status strings (e.g. "running", "exited", "created"), or null if
   *     service not found
   */
  public List<String> getContainerStatus(String serviceName) {
    ServiceInstance service = services.get(serviceName);
    if (service == null) {
      return null;
    }

    List<String> statusInfo = new ArrayList<>(service.containerNames.size());
    for (String containerName : service.containerNames) {
      ProcessBuilder pb =
          new ProcessBuilder(
              "docker", "container", "inspect", "--format", "{{.State.Status}}", containerName);
      pb.redirectErrorStream(true);
      try {
        Process process = pb.start();
        String output;
        try (InputStream inputStream = process.getInputStream()) {
          output = new String(inputStream.readAllBytes(), StandardCharsets.ISO_8859_1).trim();
        }
        int exitCode = process.waitFor();
        if (exitCode == 0 && !output.isEmpty()) {
          statusInfo.add(output);
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
    return statusInfo;
  }

  public ServiceInstance getServiceInstance(String serviceName) {
    return services.get(serviceName);
  }

  /**********************************************************************************************
   *             Begin implementation methods for BackupableApplication interface               *
   *********************************************************************************************/

  private static final String XDN_STATE_DIFF_PREFIX = "xdn:sd:";

  @Override
  public String captureStatediff(String serviceName) {
    long startCaptureTimeNs = System.nanoTime();
    int currentPlacementEpoch = this.getEpoch(serviceName);
    String stateDiff = stateDiffRecorder.captureStateDiff(serviceName, currentPlacementEpoch);
    String finalStateDiff = XDN_STATE_DIFF_PREFIX + stateDiff;
    long endCaptureTimeNs = System.nanoTime();

    logger.log(
        Level.FINE,
        "{0}:{1} - capture stateDiff within {2}ms, size={3}bytes service={4}",
        new Object[] {
          this.myNodeId,
          this.getClass().getSimpleName(),
          (endCaptureTimeNs - startCaptureTimeNs) / 1_000_000.0,
          finalStateDiff.length(),
          serviceName
        });

    return finalStateDiff;
  }

  @Override
  public boolean applyStatediff(String serviceName, String statediff) {
    ServiceInstance service = services.get(serviceName);
    if (service == null) {
      throw new RuntimeException("unknown service " + serviceName);
    }

    // validate the stateDiff
    if (statediff == null || !statediff.startsWith(XDN_STATE_DIFF_PREFIX)) {
      System.err.println("invalid XDN statediff format, ignoring it");
      return false;
    }

    // apply the stateDiff
    String stateDiffContent = statediff.substring(XDN_STATE_DIFF_PREFIX.length());
    Integer currentPlacementEpoch = this.getEpoch(serviceName);
    assert currentPlacementEpoch != null;
    boolean isApplySuccess =
        stateDiffRecorder.applyStateDiff(serviceName, currentPlacementEpoch, stateDiffContent);
    if (!isApplySuccess) {
      throw new RuntimeException("failed to apply stateDiff");
    }

    // restart the container
    if (IS_RESTART_UPON_STATE_DIFF_APPLY) {
      String restartCommand =
          String.format("docker container restart %s", service.statefulContainer);
      int exitCode = runShellCommand(restartCommand, true);
      if (exitCode != 0) {
        System.err.println("Fail to restart the container with exit code " + exitCode);
        return false;
      }
    }

    return true;
  }

  public boolean applyStatediff2(String serviceName, String statediff) {
    return true;
  }

  private boolean applyStatediffWithFuse(String serviceName, String statediff) {
    // TODO: when applying statediff the service need to be stopped/paused, the filesystem need to
    // be stopped.

    try {
      // gather the required service properties
      String containerName = String.format("%s.%s.xdn.io", serviceName, this.myNodeId);

      // store statediff into an external file
      runShellCommand("mkdir -p /tmp/xdn/fuselog/statediff/", false);
      String tempStatediffFilePath = String.format("/tmp/xdn/fuselog/statediff/%s", containerName);
      FileOutputStream outputStream = new FileOutputStream(tempStatediffFilePath);
      outputStream.write(statediff.getBytes(StandardCharsets.ISO_8859_1));
      outputStream.flush();
      outputStream.close();

      String stateDir = String.format("/tmp/xdn/fuselog/state/%s/", containerName);
      String applyCommand = String.format("%s %s", FUSELOG_APPLY_BIN_PATH, stateDir);
      Map<String, String> env = new HashMap<>();
      env.put("FUSELOG_STATEDIFF_FILE", tempStatediffFilePath);
      int errCode = runShellCommand(applyCommand, false, env);
      if (errCode != 0) {
        System.err.println("failed to apply statediff");
        return false;
      }

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return true;
  }

  /**********************************************************************************************
   *             End of implementation methods for BackupableApplication interface              *
   *********************************************************************************************/

  @Override
  public void validateInitialState(String initialState) throws InvalidInitialStateException {
    String validInitialStatePrefix = ServiceProperty.XDN_INITIAL_STATE_PREFIX;

    // validate the prefix of the initialState
    if (!initialState.startsWith(validInitialStatePrefix)) {
      throw new InvalidInitialStateException(
          "Invalid prefix for the initial state, expecting " + validInitialStatePrefix);
    }

    // try to decode the initialState (without prefix), containing the service property encoded
    // as JSON data
    initialState = initialState.substring(validInitialStatePrefix.length());
    ServiceProperty property = null;
    try {
      property = ServiceProperty.createFromJsonString(initialState);
    } catch (JSONException e) {
      throw new InvalidInitialStateException(
          "Invalid initial state, expecting valid JSON data. Error: " + e.getMessage());
    }

    // try to validate all the provided container image names
    Set<String> containerNames = new HashSet<>();
    for (ServiceComponent c : property.getComponents()) {
      containerNames.add(c.getImageName());
    }
    for (String imageName : containerNames) {
      String command = String.format("docker pull %s:latest", imageName);
      int errCode = Shell.runCommand(command, true);
      if (errCode != 0) {
        String exceptionMessage =
            String.format(
                "Unknown container image with name '%s'. Ensure the image is accessible "
                    + "at Docker Hub, our default container registry. An example of "
                    + "container available from Docker Hub is "
                    + "'fadhilkurnia/xdn-bookcatalog'. You can also use container from "
                    + "other registries (e.g., ghcr.io, quay.io, etc).",
                imageName);
        throw new InvalidInitialStateException(exceptionMessage);
      }
    }
  }

  /**********************************************************************************************
   *                  Begin implementation methods for Replicable interface                     *
   *********************************************************************************************/

  @Override
  public ReconfigurableRequest getStopRequest(String name, int epoch) {
    return new XdnStopRequest(name, epoch);
  }

  /**
   * Captures the final state of a containerized service at specific placement epoch, then put the
   * captured final state into a temporary archive at
   * /tmp/xdn/final/<nodeId>/<serviceName>/<epoch>/final_state.tar.
   *
   * @param serviceName name of the service which final state want to be captured.
   * @param epoch the placement epoch whose final state want to be captured.
   * @return true on success.
   */
  private boolean captureContainerizedServiceFinalState(String serviceName, int epoch) {
    String finalStateDirPath =
        String.format("/tmp/xdn/final_tmp/%s/%s/%d/", this.myNodeId, serviceName, epoch);

    // Validate the service instance
    Map<Integer, ServiceInstance> epochToInstanceMap = this.serviceInstances.get(serviceName);
    if (epochToInstanceMap == null) {
      Logger.getGlobal()
          .log(
              Level.WARNING,
              "Ignoring capture final state request for an unknown service with name="
                  + serviceName);
      return true;
    }
    ServiceInstance serviceInstance = epochToInstanceMap.get(epoch);
    if (serviceInstance == null) {
      Logger.getGlobal()
          .log(
              Level.WARNING,
              String.format(
                  "Ignoring capture final state request for an unknown " + "epoch=%d for name=%s",
                  epoch, serviceName));
      return true;
    }

    // Remove the previously captured final state, if any.
    String removeCommand = String.format("rm -rf %s", finalStateDirPath);
    int code = Shell.runCommand(removeCommand, true);
    assert code == 0;

    // Create the directory to store the final state
    String createDirCommand = String.format("mkdir -p %s", finalStateDirPath);
    code = Shell.runCommand(createDirCommand, true);
    assert code == 0;

    // Copy the service state into the prepared directory.
    String hostMountDir = stateDiffRecorder.getTargetDirectory(serviceName, epoch);
    String stateCopyCommand = String.format("cp -a %s %s", hostMountDir, finalStateDirPath);
    int count = 0;
    while (true) {
      if (++count >= 10) {
        logger.log(
            Level.WARNING,
            "{0}:{1} rsync failed to capture {2}:{3} final state after {4} iterations",
            new Object[] {
              this.myNodeId.toUpperCase(),
              this.getClass().getSimpleName(),
              serviceName,
              epoch,
              count
            });
        break;
      }

      logger.log(
          Level.INFO,
          ">>>>>>>>>> {0}:{1} begin running rsync to copy {2}:{3} final state to {4}",
          new Object[] {
            this.myNodeId.toUpperCase(),
            this.getClass().getSimpleName(),
            serviceName,
            epoch,
            finalStateDirPath
          });

      int exitCode = Shell.runCommand(stateCopyCommand, true);
      if (exitCode == 0) break;
    }

    // Archive the final state into a tar file
    String finalStateTarDirPath =
        String.format("/tmp/xdn/final/%s/%s/%d/", this.myNodeId, serviceName, epoch);
    String removeTarDirCommand = String.format("rm -rf %s", finalStateTarDirPath);
    code = Shell.runCommand(removeTarDirCommand, true);
    assert code == 0;
    String createTarDirCommand = String.format("mkdir -p %s", finalStateTarDirPath);
    code = Shell.runCommand(createTarDirCommand, true);
    assert code == 0;
    String finalStateTarFilePath = finalStateTarDirPath + "final_state.tar";
    ZipFiles.zipDirectory(new File(finalStateDirPath), finalStateTarFilePath);

    logger.log(
        Level.FINE,
        "{0}:{1} successfully stored {2}:{3} final state in {4}",
        new Object[] {
          this.myNodeId.toUpperCase(),
          this.getClass().getSimpleName(),
          serviceName,
          epoch,
          finalStateTarFilePath
        });

    return true;
  }

  @Override
  public String getFinalState(String name, int epoch) {
    // TODO: validate whether this works with PrimaryBackup or not
    System.out.println(
        ">>> "
            + this.myNodeId
            + ":XdnGigapaxosApp - getFinalState name="
            + name
            + " epoch="
            + epoch);

    // Validate the service instance
    Map<Integer, ServiceInstance> epochToInstanceMap = this.serviceInstances.get(name);
    if (epochToInstanceMap == null) {
      Logger.getGlobal()
          .log(
              Level.WARNING,
              "Ignoring capture final state request for an unknown service with name=" + name);
      return null;
    }
    ServiceInstance serviceInstance = epochToInstanceMap.get(epoch);
    if (serviceInstance == null) {
      Logger.getGlobal()
          .log(
              Level.WARNING,
              String.format(
                  "Ignoring capture final state request for an unknown " + "epoch=%d for name=%s",
                  epoch, name));
      return null;
    }

    // Check if the final state is already captured previously
    String finalStatePath =
        String.format("/tmp/xdn/final/%s/%s/%d/final_state.tar", this.myNodeId, name, epoch);
    File finalStateArchive = new File(finalStatePath);
    if (!finalStateArchive.exists()) {
      boolean isCaptureSuccess = this.captureContainerizedServiceFinalState(name, epoch);
      assert isCaptureSuccess;
    }

    // Equivalent to 500 Kb
    int maxBytes = 62500;
    String finalStatePrefix =
        String.format("%s%d:", ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX, epoch);
    if (finalStateArchive.length() <= maxBytes) {
      // Convert the final state archive into String
      // Read the archive and convert into string
      byte[] finalStateBytes = null;
      try {
        finalStateBytes = Files.readAllBytes(Paths.get(finalStatePath));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

      // combine the prefix with the actual final state
      // format: xdn:final:<epoch>::<serviceProperty>::<finalState>
      return String.format(
          "%s:%s::%s",
          finalStatePrefix,
          serviceInstance.property.toJsonString(),
          Base64.getEncoder().encodeToString(finalStateBytes));
    } else {
      // send directory path.
      String finalStateUrl =
          this.largeCheckpointer.createCheckpointHandle(this.myNodeId, finalStatePath);

      // format: xdn:final:<epoch>::<serviceProperty>::url:<finalStateUrl>
      return String.format(
          "%s:%s::url:%s",
          finalStatePrefix, serviceInstance.property.toJsonString(), finalStateUrl);
    }
  }

  @Override
  public void putInitialState(String name, int epoch, String state) {
    var exceptionMessage =
        String.format(
            "XDNGigapaxosApp.putInitialState is unimplemented, serviceName=%s epoch=%d state=%s",
            name, epoch, state);
    throw new RuntimeException(exceptionMessage);
  }

  @Override
  public boolean deleteFinalState(String name, int epoch) {
    System.out.println(
        ">> XdnGigapaxosApp:"
            + this.myNodeId
            + " -- deleteFinalState(name="
            + name
            + ",epoch="
            + epoch
            + ")");
    return this.deleteContainerizedServiceInstance(name, epoch);
  }

  @Override
  public Integer getEpoch(String name) {
    assert name != null && !name.isEmpty();
    Integer currentPlacementEpoch = this.servicePlacementEpoch.get(name);
    return currentPlacementEpoch;
  }

  /**********************************************************************************************
   *                   End implementation methods for Replicable interface                      *
   *********************************************************************************************/

  /**********************************************************************************************
   *                                  Begin utility methods                                     *
   *********************************************************************************************/

  private int getRandomNumber(int min, int max) {
    return (int) ((Math.random() * (max - min)) + min);
  }

  /**
   * This method tries to find available port, most of the time. It is possible, in race condition,
   * that the port deemed as available is being used by others even when this method return the
   * port.
   *
   * @return port number that is potentially available
   */
  private int getRandomPort() {
    int maxAttempt = 5;
    int port = getRandomNumber(50000, 65000);
    boolean isPortAvailable = false;

    // check if port is already used by others
    while (!isPortAvailable && maxAttempt > 0) {
      try {
        // success connection means the port is already used
        Socket s = new Socket("localhost", port);
        s.close();
        port = getRandomNumber(50000, 65000);
      } catch (IOException e) {
        // unsuccessful connection could mean that the port is available
        isPortAvailable = true;
      }
      maxAttempt--;
    }

    return port;
  }

  private boolean startContainer(
      String imageName,
      String containerName,
      String networkName,
      String hostName,
      Integer exposedPort,
      Integer publishedPort,
      Integer allocatedHttpPort,
      String mountDirSource,
      String mountDirTarget,
      Map<String, String> env) {

    String publishPortSubCmd = "";
    if (publishedPort != null && allocatedHttpPort != null) {
      publishPortSubCmd = String.format("--publish=%d:%d", allocatedHttpPort, publishedPort);
    }
    if (publishedPort != null && allocatedHttpPort == null) {
      publishPortSubCmd = String.format("--publish=%d:%d", publishedPort, publishedPort);
    }

    // Note that the exposed port will be ignored if there is published port
    String exposePortSubCmd = "";
    if (exposedPort != null && publishPortSubCmd.isEmpty()) {
      exposePortSubCmd = String.format("--expose=%d", exposedPort);
    }

    String mountSubCmd = "";
    if (mountDirSource != null
        && !mountDirSource.isEmpty()
        && mountDirTarget != null
        && !mountDirTarget.isEmpty()) {
      mountSubCmd =
          String.format("--mount type=bind,source=%s,target=%s", mountDirSource, mountDirTarget);
    }

    String envSubCmd = "";
    if (env != null) {
      StringBuilder sb = new StringBuilder();
      for (Map.Entry<String, String> keyVal : env.entrySet()) {
        sb.append(String.format("--env %s=%s ", keyVal.getKey(), keyVal.getValue()));
      }
      envSubCmd = sb.toString();
    }

    String userSubCmd = "";
    int uid = Utils.getUid();
    int gid = Utils.getGid();

    // Run with arbitrary user if the container is stateful
    // Only been tested on PostgreSQL, MySQL, MariaDb
    // --cap-add is for MySQL reconfiguration to allow it to handle errors silently
    if (uid != 0 && mountDirTarget != null && !mountDirTarget.isEmpty()) {
      userSubCmd =
          String.format("-v /etc/passwd:/etc/passwd:ro --user=%d:%d --cap-add=sys_nice", uid, gid);
    }

    String clearCommand = String.format("docker container rm --force %s", containerName);
    Shell.runCommand(clearCommand, true);

    String startCommand =
        String.format(
            "docker run -d --restart unless-stopped --name=%s --hostname=%s --network=%s "
                + "%s %s %s %s %s %s",
            containerName,
            hostName,
            networkName,
            publishPortSubCmd,
            exposePortSubCmd,
            mountSubCmd,
            envSubCmd,
            userSubCmd,
            imageName);
    int exitCode = Shell.runCommand(startCommand, false);
    if (exitCode != 0) {
      throw new RuntimeException(
          String.format(
              "%s:%s failed to start container %s",
              this.myNodeId.toUpperCase(), this.getClass().getSimpleName(), containerName));
    }

    logger.log(
        Level.INFO,
        "{0}:{1} - {2} docker instance started",
        new Object[] {this.myNodeId.toUpperCase(), this.getClass().getSimpleName(), containerName});

    return true;
  }

  /**
   * Forwards the HttpRequest and HttpRequestContent inside xdnRequest into the containerized
   * service. When there is no error, xdnRequest contains the response from the service. NOTE: It is
   * the responsibility of the caller of this method to release the buffer
   *
   * <pre>{@code
   * forwardHttpRequestToContainerizedService(xdnRequest);
   * ...
   * ReferenceCountUtil.release(xdnRequest.getHttpResponse());
   * }</pre>
   *
   * @param xdnRequest non-null XdnHttpRequest that contains valid HttpRequest.
   */
  private void forwardHttpRequestToContainerizedService(XdnHttpRequest xdnRequest) {
    Objects.requireNonNull(xdnRequest, "xdnRequest");
    Objects.requireNonNull(xdnRequest.getHttpRequest(), "xdnRequest.httpRequest");
    Objects.requireNonNull(xdnRequest.getHttpRequestContent(), "xdnRequest.httpRequestContent");
    assert xdnRequest.getHttpResponse() == null;

    long startTime = System.nanoTime();
    String serviceName = xdnRequest.getServiceName();
    assert serviceName != null;
    Integer targetPort = this.activeServicePorts.get(serviceName);
    assert targetPort != null;
    long endValidationTime = System.nanoTime();

    String requestRcvTimestampStr =
        xdnRequest.getHttpRequest().headers().get("X-S-EXC-TS-" + myNodeId);
    if (requestRcvTimestampStr != null) {
      long requestRcvTimestamp = Long.parseLong(requestRcvTimestampStr);
      long preExecutionElapsedTime = System.nanoTime() - requestRcvTimestamp;
      logger.log(
          Level.FINE,
          "{0}:{1} - HTTP pre-execution over {2}ms",
          new Object[] {
            this.myNodeId, this.getClass().getSimpleName(), (preExecutionElapsedTime / 1_000_000.0)
          });
    }

    FullHttpRequest forwardedHttpRequest = copyHttpRequest(xdnRequest);
    long endRequestCreationTime = System.nanoTime();

    long endRequestResponseTime;
    long endConversionTime;
    long endResponseStoreTime;
    try {
      // forward request to the underlying containerized service
      FullHttpResponse httpResponse =
          httpForwarderClient.execute("127.0.0.1", targetPort, forwardedHttpRequest);
      endRequestResponseTime = System.nanoTime();

      // store the response
      xdnRequest.setHttpResponse(httpResponse);
      endConversionTime = System.nanoTime();
      endResponseStoreTime = System.nanoTime();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    logger.log(
        Level.FINE,
        "{0}:{1} - docker proxy takes {2}ms (val={3}ms crt={4}ms "
            + "exc={5}ms conv={6}ms sto={7}ms)",
        new Object[] {
          this.myNodeId,
          this.getClass().getSimpleName(),
          (endResponseStoreTime - startTime) / 1_000_000.0,
          (endValidationTime - startTime) / 1_000_000.0,
          (endRequestCreationTime - endValidationTime) / 1_000_000.0,
          (endRequestResponseTime - endRequestCreationTime) / 1_000_000.0,
          (endConversionTime - endRequestResponseTime) / 1_000_000.0,
          (endResponseStoreTime - endConversionTime) / 1_000_000.0
        });
  }

  private void forwardHttpRequestBatchToContainerizedService(XdnHttpRequestBatch batch) {
    List<XdnHttpRequest> requests = batch.getRequestList();
    CompletableFuture<?>[] futures = new CompletableFuture<?>[requests.size()];
    for (int i = 0; i < requests.size(); i++) {
      XdnHttpRequest httpRequest = requests.get(i);
      futures[i] =
          CompletableFuture.runAsync(
              () -> {
                forwardHttpRequestToContainerizedService(httpRequest);
                assert httpRequest.getHttpResponse() != null
                    : "Obtained null response after request execution";
                assert ReferenceCountUtil.refCnt(httpRequest.getHttpResponse()) == 1
                    : String.format(
                        "Unexpected refCnt of response after execution (%d!=1)",
                        ReferenceCountUtil.refCnt(httpRequest.getHttpResponse()));
                // NOTE: we are not releasing the reference-counter response here because
                //       it will be released in the HttpActiveReplica.
              });
    }
    CompletableFuture.allOf(futures).join();
  }

  private FullHttpRequest copyHttpRequest(XdnHttpRequest httpRequest) {
    ByteBuf content = httpRequest.getHttpRequestContent().content();
    ByteBuf copiedContent =
        content != null && content.isReadable()
            ? Unpooled.copiedBuffer(content)
            : Unpooled.EMPTY_BUFFER;

    DefaultFullHttpRequest copy =
        new DefaultFullHttpRequest(
            httpRequest.getHttpRequest().protocolVersion(),
            httpRequest.getHttpRequest().method(),
            httpRequest.getHttpRequest().uri(),
            copiedContent);
    copy.headers().set(httpRequest.getHttpRequest().headers());
    if (httpRequest.getHttpRequest() instanceof FullHttpRequest fullHttpRequest) {
      copy.trailingHeaders().set(fullHttpRequest.trailingHeaders());
    }
    if (copiedContent != Unpooled.EMPTY_BUFFER) {
      HttpUtil.setContentLength(copy, copiedContent.readableBytes());
    } else {
      copy.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
    }
    return copy;
  }

  public static java.net.http.HttpRequest createOutboundHttpRequest(
      HttpRequest originalRequest, HttpContent originalContent, int targetPort) {
    assert originalRequest != null;
    String uri = originalRequest.uri();
    if (uri == null || uri.isEmpty()) {
      uri = "/";
    }

    if (uri.startsWith("http://") || uri.startsWith("https://")) {
      int startIdx = uri.indexOf("//");
      int pathIdx = startIdx >= 0 ? uri.indexOf('/', startIdx + 2) : -1;
      uri = pathIdx >= 0 ? uri.substring(pathIdx) : "/";
    }

    byte[] payload = null;
    if (originalContent != null
        && originalContent.content() != null
        && originalContent.content().isReadable()) {
      ByteBuf buffer = originalContent.content();
      payload = new byte[buffer.readableBytes()];
      buffer.getBytes(buffer.readerIndex(), payload);
    }

    URI targetUri = URI.create("http://127.0.0.1:" + targetPort + uri);
    java.net.http.HttpRequest.Builder builder =
        java.net.http.HttpRequest.newBuilder(targetUri).timeout(Duration.ofSeconds(10));

    HttpHeaders originalHeaders = originalRequest.headers();
    Iterator<Map.Entry<String, String>> iterator = originalHeaders.iteratorAsString();
    while (iterator.hasNext()) {
      Map.Entry<String, String> entry = iterator.next();
      CharSequence name = entry.getKey();
      if (HttpHeaderNames.HOST.contentEqualsIgnoreCase(name)
          || HttpHeaderNames.CONTENT_LENGTH.contentEqualsIgnoreCase(name)
          || HttpHeaderNames.TRANSFER_ENCODING.contentEqualsIgnoreCase(name)
          || HttpHeaderNames.CONNECTION.contentEqualsIgnoreCase(name)) {
        continue;
      }
      builder.header(name.toString(), entry.getValue());
    }

    builder.header(HttpHeaderNames.HOST.toString(), String.format("127.0.0.1:%d", targetPort));
    builder.header(HttpHeaderNames.CONNECTION.toString(), HttpHeaderValues.KEEP_ALIVE.toString());

    boolean hasBody = payload != null && payload.length > 0;
    if (hasBody) {
      builder.header(HttpHeaderNames.CONTENT_LENGTH.toString(), Integer.toString(payload.length));
    }

    java.net.http.HttpRequest.BodyPublisher publisher =
        hasBody
            ? java.net.http.HttpRequest.BodyPublishers.ofByteArray(payload)
            : java.net.http.HttpRequest.BodyPublishers.noBody();

    builder.method(originalRequest.method().name(), publisher);
    return builder.build();
  }

  public static FullHttpResponse toNettyHttpResponse(java.net.http.HttpResponse<byte[]> response) {
    byte[] body = response.body() != null ? response.body() : new byte[0];
    ByteBuf content = Unpooled.wrappedBuffer(body);
    HttpResponseStatus status = HttpResponseStatus.valueOf(response.statusCode());
    DefaultFullHttpResponse nettyResponse =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);

    response
        .headers()
        .map()
        .forEach(
            (name, values) -> {
              if (name == null || values == null) {
                return;
              }
              for (String value : values) {
                if (value != null) {
                  nettyResponse.headers().add(name, value);
                }
              }
            });

    if (!nettyResponse.headers().contains(HttpHeaderNames.CONTENT_LENGTH)) {
      HttpUtil.setContentLength(nettyResponse, body.length);
    }
    if (!nettyResponse.headers().contains(HttpHeaderNames.CONNECTION)) {
      nettyResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
    }
    return nettyResponse;
  }

  private io.netty.handler.codec.http.HttpResponse createNettyHttpErrorResponse(Exception e) {
    StringWriter sw = new StringWriter();
    sw.write("{\"error\":\"Failed to get response from the containerized service.\",");
    sw.write("\"message\":\"");
    sw.write(e.getMessage());
    sw.write("\"}");
    DefaultFullHttpResponse response =
        new DefaultFullHttpResponse(
            HttpVersion.HTTP_1_1,
            HttpResponseStatus.INTERNAL_SERVER_ERROR,
            Unpooled.copiedBuffer(sw.toString().getBytes()));
    response.headers().add(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
    return response;
  }

  private int runShellCommand(String command, boolean isSilent) {
    return Shell.runCommand(command, isSilent, null);
  }

  private int runShellCommand(
      String command, boolean isSilent, Map<String, String> environmentVariables) {
    return Shell.runCommand(command, isSilent, environmentVariables);
  }

  /**********************************************************************************************
   *                                  End utility methods                                     *
   *********************************************************************************************/

  /**********************************************************************************************
   *                        Non-Deterministic Initialization Methods                            *
   *********************************************************************************************/
  // This function is only called by the primary replica
  public void nonDeterministicInitialization(
      String serviceName, Map<String, InetAddress> ipAddresses, String sshKey) {
    int placementEpoch = this.servicePlacementEpoch.get(serviceName);
    assertNotNull("placementEpoch must not be null", placementEpoch);

    ServiceInstance service = this.serviceInstances.get(serviceName).get(placementEpoch);
    if (service == null)
      throw new RuntimeException(String.format("%s: service %s not found.", myNodeId, serviceName));

    if (service.initializationSucceed) {
      logger.log(
          Level.FINE,
          "{0}:{1} cancelling non-deterministic initializaition for {2}:{3}. Service already"
              + " initialized.",
          new Object[] {
            this.myNodeId.toUpperCase(),
            this.getClass().getSimpleName(),
            serviceName,
            placementEpoch
          });
      return;
    }

    logger.log(
        Level.FINE,
        "{0}:{1} starting non-deterministic initializaition for {2}:{3}",
        new Object[] {
          this.myNodeId.toUpperCase(), this.getClass().getSimpleName(), serviceName, placementEpoch
        });

    String databaseImage = service.property.getStatefulComponent().getImageName().split(":")[0];

    // There's no way to detect if SQLite is used
    String dbReadyMsg = null;
    int migrateWaitTime = 5;
    int numOfLines = 10;
    switch (databaseImage) {
      case "mysql":
        dbReadyMsg = "mysqld: ready for connections";
        migrateWaitTime = 60;
        break;
      case "postgres":
        dbReadyMsg = "database system is ready to accept connections";
        migrateWaitTime = 10;
        break;
      case "mariadb":
        dbReadyMsg = "mariadbd: ready for connections";
        migrateWaitTime = 30;
        break;
      case "mongo":
        dbReadyMsg = "Waiting for connections";
        migrateWaitTime = 30;
        numOfLines = 100;
        break;
      default:
        System.out.printf(
            "Database unknown. Timeout for 5 seconds to wait for database to initialize...\n");
        try {
          Thread.sleep(5000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
    }

    if (dbReadyMsg != null) {
      Pattern p = Pattern.compile(dbReadyMsg);

      int iter = 0;
      int MAX_ITER = 100;

      // Assumes that the stateful container is always the first
      // declared service property (not always the case)
      int idx = 0;
      int epoch = this.servicePlacementEpoch.get(serviceName);

      ShellOutput commandOutput = null;

      // Format  : c<component-id>.e<reconfiguration-epoch>.<service-name>.<node-id>.xdn.io
      // Example : c1.e2.bookcatalog.ar2.xdn.io
      do {
        iter++;
        commandOutput =
            Shell.runCommandWithOutput(
                String.format(
                    "docker logs --tail %d c%d.e%d.%s.%s.xdn.io",
                    numOfLines, idx, epoch, serviceName, this.myNodeId),
                true);

        String output =
            ((commandOutput.stdout == null) ? "" : commandOutput.stdout)
                + ((commandOutput.stderr == null) ? "" : commandOutput.stderr);
        Matcher matcher = p.matcher(output);

        if (commandOutput.exitCode != 0) {
          System.out.println("Command failed to run");
        } else if (!matcher.find()) {
          System.out.printf("Failed detecting database initialization (iter=%d)\n", iter);
          if (iter >= MAX_ITER) {
            System.out.printf(
                "Failed detecting database initialization after %d iterations. Forcefully exit"
                    + " loop\n",
                iter);
            break;
          }
        } else {
          break;
        }

        try {
          Thread.sleep(3000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      } while (true);

      System.out.println("Database initialized");
    }

    System.out.printf("Waiting another %d seconds for migration...\n", migrateWaitTime);
    try {
      Thread.sleep(migrateWaitTime * 1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    this.stateDiffRecorder.initContainerSync(
        this.myNodeId, serviceName, ipAddresses, placementEpoch, sshKey);
    service.initializationSucceed = true;
  }

  /**********************************************************************************************
   *                      End Non-Deterministic Initialization Methods                          *
   *********************************************************************************************/

}
