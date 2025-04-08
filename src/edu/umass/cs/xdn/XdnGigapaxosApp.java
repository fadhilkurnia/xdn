package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.primarybackup.packets.*;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.http.HttpActiveReplicaRequest;
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
import edu.umass.cs.xdn.utils.Utils;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONException;

import java.io.*;
import java.net.Socket;
import java.net.StandardProtocolFamily;
import java.net.URI;
import java.net.UnixDomainSocketAddress;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public class XdnGigapaxosApp implements Replicable, Reconfigurable, BackupableApplication,
        InitialStateValidator {

    private final boolean IS_RESTART_UPON_STATE_DIFF_APPLY = false;
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
    private final HttpClient serviceClient = HttpClient.newHttpClient();

    private final String stateDiffRecorderTypeString =
            Config.getGlobalString(ReconfigurationConfig.RC.XDN_PB_STATEDIFF_RECORDER_TYPE);
    private RecorderType recorderType;
    private AbstractStateDiffRecorder stateDiffRecorder;

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

        // Validate and initialize the stateDiff recorder for Primary Backup.
        // We need to check the Operating System as currently FUSE (i.e., Fuselog)
        // is only supported on Linux.
        if (stateDiffRecorderTypeString.equalsIgnoreCase(RecorderType.RSYNC.toString())) {
            recorderType = RecorderType.RSYNC;
        } else if (stateDiffRecorderTypeString.equalsIgnoreCase(RecorderType.FUSELOG.toString())) {
            recorderType = RecorderType.FUSELOG;
        } else if (stateDiffRecorderTypeString.equalsIgnoreCase(RecorderType.ZIP.toString())) {
            recorderType = RecorderType.ZIP;
        } else {
            String errMsg = "[ERROR] Unknown StateDiff recorder type of " +
                    stateDiffRecorderTypeString;
            System.out.println(errMsg);
            throw new RuntimeException(errMsg);
        }
        if (recorderType.equals(RecorderType.FUSELOG)) {
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
            default:
                String errMessage = "unknown stateDiff recorder " + recorderType.toString();
                throw new RuntimeException(errMessage);
        }

        // Set app request type
        this.packetTypes = new HashSet<>();
        this.packetTypes.add(XdnRequestType.XDN_SERVICE_HTTP_REQUEST);
        this.packetTypes.add(XdnRequestType.XDN_STOP_REQUEST);
    }

    // TODO: complete this implementation
    public static boolean checkSystemRequirements() {
        boolean isDockerAvailable = isDockerAvailable();
        if (!isDockerAvailable) {
            String exceptionMessage = """
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
        String serviceName = request.getServiceName();

        if (request instanceof HttpActiveReplicaRequest) {
            throw new RuntimeException("XdnGigapaxosApp should not receive " +
                    "HttpActiveReplicaRequest");
        }

        if (request instanceof PrimaryBackupPacket) {
            throw new RuntimeException("XdnGigapaxosApp should not receive PrimaryBackupPacket");
        }

        if (request instanceof XDNStatediffApplyRequest) {
            throw new RuntimeException("XdnGigapaxosApp should not receive " +
                    "XDNStatediffApplyRequest");
        }

        if (request instanceof XdnJsonHttpRequest) {
            throw new RuntimeException("XdnGigapaxosApp should not receive XdnJsonHttpRequest");
        }

        if (request instanceof XdnHttpRequest xdnHttpRequest) {
            long startTime = System.nanoTime();
            forwardHttpRequestToContainerizedService(xdnHttpRequest);
            long elapsedTime = System.nanoTime() - startTime;
            logger.log(Level.FINE, "{0}:{1} - execution within {2}ms, {3} {4}:{5} (id: {6})",
                    new Object[]{this.myNodeId, this.getClass().getSimpleName(),
                            (elapsedTime / 1_000_000.0),
                            xdnHttpRequest.getHttpRequest().method(),
                            serviceName,
                            xdnHttpRequest.getHttpRequest().uri(),
                            xdnHttpRequest.getRequestID()});
            return true;
        }

        if (request instanceof XdnStopRequest stopRequest) {
            int stoppedPlacementEpoch = stopRequest.getEpochNumber();

            boolean isCaptureSuccess = this.captureContainerizedServiceFinalState(
                    serviceName, stoppedPlacementEpoch);
            assert isCaptureSuccess : "failed to store the final state before stopping";
            return this.stopContainerizedServiceInstance(serviceName, stoppedPlacementEpoch);
        }

        String exceptionMessage = String.format("%s:XdnGigapaxosApp executing unknown request %s",
                this.myNodeId, request.getClass().getSimpleName());
        throw new RuntimeException(exceptionMessage);
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
        String mountCommand = String.format("%s -o allow_other -f -s %s", FUSELOG_BIN_PATH, stateDirPath);
        var t = new Thread() {
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
        //
        System.out.println(">> XDNGigapaxosApp:" + this.myNodeId + " - checkpoint ... name=" + name);
        return "dummyXDNServiceCheckpoint";
    }

    @Override
    public boolean restore(String name, String state) {
        System.out.println(">> XdnGigapaxosApp:" + this.myNodeId +
                " - restore name=" + name + " state=" + state);

        // A corner case when name is empty, which fundamentally must not happen.
        if (name == null || name.isEmpty()) {
            String exceptionMessage = String.format("%s:XdnGigapaxosApp's restore(.) is called " +
                    "with empty service name", this.myNodeId);
            throw new RuntimeException(exceptionMessage);
        }

        // Case-1: Gigapaxos started meta service with name == XdnGigaPaxosApp0 and state == {}
        if (name.equals(PaxosConfig.getDefaultServiceName()) &&
                state != null && state.equals("{}")) {
            return true;
        }

        // Case-2: initialize a brand-new service name.
        // Note that in gigapaxos, initialization is a special case of restore
        // with state == initialState. In XDN, the initialState is always started with "xdn:init:".
        // Example of the initState is "xdn:init:bookcatalog:8000:linearizable:true:/app/data",
        if (state != null && state.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)) {
            boolean isServiceInitialized = initContainerizedService2(name, state);
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
            String encodedFinalState = raw[2];  // FIXME: handle if finalState has :: in it.
            String[] prefixRaw = raw[0].split(":");
            assert prefixRaw.length == 3;
            int prevPlacementEpoch = Integer.parseInt(prefixRaw[2]);
            int newPlacementEpoch = prevPlacementEpoch + 1;  // FIXME: this is prone to error!
            return this.reviveContainerizedService(
                    name,
                    encodedServiceProperty,
                    encodedFinalState,
                    newPlacementEpoch);
        }

        // Unknown cases, should not be triggered
        String exceptionMessage = String.format("unknown case for %s:XdnGigapaxosApp's restore(.)" +
                " with name=%s state=%s", this.myNodeId, name, state);
        throw new RuntimeException(exceptionMessage);
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        XdnRequestType packetType = XdnRequest.getQuickPacketTypeFromEncodedPacket(stringified);
        assert packetType != null :
                "Unknown XDN Request handled by XdnGigapaxosApp. Request: '" + stringified + "'";

        if (packetType.equals(XdnRequestType.XDN_SERVICE_HTTP_REQUEST)) {
            XdnHttpRequest httpRequest = XdnHttpRequest.createFromString(stringified);

            // we need to set the request matcher after creating XdnHttpRequest
            // TODO: this code will fail if the service name doesnt exist in this replica,
            //   we need to refactor XdnHttpRequest so that we don't need to set the
            //   request matcher after each creation.
            //   The issue is there because the behavior of each request is service specific.
            //   The request can either be constructed in the entry replica, or when a replica
            //   receive forwarded request.
            if (httpRequest == null) return null;
            String serviceName = httpRequest.getServiceName();
            Map<Integer, ServiceInstance> currServiceInstance =
                    this.serviceInstances.get(serviceName);
            if (currServiceInstance == null) return null;
            Integer currServicePlacementEpoch = this.servicePlacementEpoch.get(serviceName);
            if (currServicePlacementEpoch == null) return null;
            ServiceInstance currInstance = currServiceInstance.get(currServicePlacementEpoch);
            if (currInstance == null) return null;
            ServiceProperty serviceProperty = currInstance.property;
            httpRequest.setRequestMatchers(serviceProperty.getRequestMatchers());
            return httpRequest;
        }

        if (packetType.equals(XdnRequestType.XDN_STOP_REQUEST)) {
            return XdnStopRequest.createFromString(stringified);
        }

        throw new RuntimeException("Unknown encoded request or packet format: "
                + stringified);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return this.packetTypes;
    }

    /**
     * initContainerizedService initializes a containerized service, in idempotent manner.
     *
     * @param serviceName  name of the to-be-initialized service.
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
            System.err.println("incorrect initial state, example of expected state is" +
                    " 'xdn:init:bookcatalog:8000:linearizable:true:/app/data'");
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
     * initContainerizedService2 initializes a containerized service, in idempotent manner.
     * This is a new implementation of initContainerizedService, but with support on service with
     * multiple container components.
     *
     * @param serviceName  name of the to-be-initialized service.
     * @param initialState the initial state with "xdn:init:" prefix.
     * @return false if failed to initialize the service.
     */
    private boolean initContainerizedService2(String serviceName, String initialState) {
        String validInitialStatePrefix = ServiceProperty.XDN_INITIAL_STATE_PREFIX;
        int initialPlacementEpoch = 0;

        // validate the initial state
        if (!initialState.startsWith(validInitialStatePrefix)) {
            throw new RuntimeException("Invalid initial state");
        }

        // decode the initial state, containing the service property
        ServiceProperty property = null;
        String networkName = String.format("net::%s:%s", myNodeId, serviceName);
        int allocatedPort = getRandomPort();
        try {
            initialState = initialState.substring(validInitialStatePrefix.length());
            property = ServiceProperty.createFromJsonString(initialState);
        } catch (JSONException e) {
            throw new RuntimeException("Invalid initial state as JSON: " + e);
        }

        // Prepares container names for each service component.
        // Format  : c<component-id>.e<reconfiguration-epoch>.<service-name>.<node-id>.xdn.io
        // Example : c0.e2.bookcatalog.ar2.xdn.io
        List<String> containerNames = new ArrayList<>();
        int idx = 0;
        int epoch = 0;
        for (ServiceComponent c : property.getComponents()) {
            String containerName = String.format("c%d.e%d.%s.%s.xdn.io",
                    idx, epoch, serviceName, myNodeId);
            containerNames.add(containerName);
            idx++;
        }

        // prepare the initialized service
        ServiceInstance service = new ServiceInstance(
                property,
                serviceName,
                networkName,
                allocatedPort,
                containerNames
        );

        // TODO: remove already running containers, if any

        // create docker network, via command line
        int exitCode = createDockerNetwork(networkName);
        if (exitCode != 0) {
            return false;
        }

        // TODO: prepare statediff directory, if required
        String stateDirMountSource = stateDiffRecorder.getTargetDirectory(
                serviceName, initialPlacementEpoch);
        String stateDirMountTarget = property.getStatefulComponentDirectory();

        stateDiffRecorder.preInitialization(serviceName, initialPlacementEpoch);

        // actually start the service, run each component as container, in the same order as they
        // are specified in the declared service property.
        idx = 0;
        for (ServiceComponent c : property.getComponents()) {
            boolean isSuccess = startContainer(
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
                throw new RuntimeException("failed to start container for component " +
                        c.getComponentName());
            }

            idx++;
        }

        // TODO: need to handle non-deterministic initialization,
        //  e.g., a node that initialize a filename with current time or random number.
        if (!property.isDeterministic()) {
            System.err.println("WARNING: non-deterministic service can generate different " +
                    "initial state");
        }
        stateDiffRecorder.postInitialization(serviceName, initialPlacementEpoch);

        // store all the current service metadata
        this.services.put(serviceName, service);
        this.activeServicePorts.put(serviceName, allocatedPort);

        // store the service placement epoch metadata
        this.servicePlacementEpoch.put(serviceName, initialPlacementEpoch);
        Map<Integer, ServiceInstance> epochToInstanceMap = new ConcurrentHashMap<>();
        epochToInstanceMap.put(initialPlacementEpoch, service);
        this.serviceInstances.put(serviceName, epochToInstanceMap);

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
    private boolean reviveContainerizedService(String serviceName, String encodedServiceProperty,
                                               String prevEpochFinalState, int placementEpoch) {
        assert serviceName != null && !serviceName.isEmpty();
        assert encodedServiceProperty != null && !encodedServiceProperty.isEmpty();
        assert placementEpoch > 0 : "expecting non initial epoch";

        Integer currEpoch = this.getEpoch(serviceName);
        if (currEpoch != null && currEpoch >= placementEpoch) {
            return true;
        }

        System.out.printf(">> %s:XdnGigapaxosApp reviveService name=%s epoch=%d state=%s\n",
                this.myNodeId, serviceName, placementEpoch, prevEpochFinalState);

        // Validate and parse the encoded service property
        ServiceProperty property;
        try {
            property = ServiceProperty.createFromJsonString(encodedServiceProperty);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }

        // prepare statediff directory, if required
        String stateDirMountSource = stateDiffRecorder.getTargetDirectory(
                serviceName, placementEpoch);
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
        String destinationDirPath = String.format("/tmp/xdn/final/%s/%s/",
                this.myNodeId, serviceName);
        int code = Shell.runCommand(String.format("mkdir -p %s ", destinationDirPath));
        assert code == 0;
        String destinationTarFilePath =
                String.format("/tmp/xdn/final/%s/%s/rcv_final_state::%d.tar",
                        this.myNodeId, serviceName, placementEpoch);
        byte[] prevEpochFinalStateBytes = Base64.getDecoder().decode(prevEpochFinalState);
        try {
            Files.write(Paths.get(destinationTarFilePath),
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
            String containerName = String.format("c%d.e%d.%s.%s.xdn.io",
                    idx, placementEpoch, serviceName, myNodeId);
            containerNames.add(containerName);
            idx++;
        }

        // prepare the initialized service
        String networkName = String.format("net::%s:%s", myNodeId, serviceName);
        int allocatedPort = getRandomPort();
        ServiceInstance service = new ServiceInstance(
                property,
                serviceName,
                networkName,
                allocatedPort,
                containerNames
        );

        // create docker network, via command line
        int exitCode = createDockerNetwork(networkName);
        if (exitCode != 0) {
            return false;
        }

        // actually start the service, run each component as container, in the same order as they
        // are specified in the declared service property.
        idx = 0;
        for (ServiceComponent c : property.getComponents()) {
            boolean isSuccess = startContainer(
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
                throw new RuntimeException("failed to start container for component " +
                        c.getComponentName());
            }

            idx++;
        }

        // TODO: need to handle non-deterministic initialization,
        //  e.g., a node that initialize a filename with current time or random number.
        if (!property.isDeterministic()) {
            System.err.println("WARNING: non-deterministic service can generate different " +
                    "initial state");
        }
        stateDiffRecorder.postInitialization(serviceName, placementEpoch);

        this.services.put(serviceName, service);
        this.activeServicePorts.put(serviceName, allocatedPort);

        // store the service placement epoch metadata
        this.servicePlacementEpoch.put(serviceName, placementEpoch);
        Map<Integer, ServiceInstance> epochToInstanceMap =
                this.serviceInstances.computeIfAbsent(serviceName, k ->
                        new ConcurrentHashMap<>());
        epochToInstanceMap.put(placementEpoch, service);

        return true;
    }

    private boolean deleteContainerizedServiceInstance(String serviceName, int placementEpoch) {
        assert serviceName != null && !serviceName.isEmpty();
        assert placementEpoch >= 0;
        System.out.println(">>> Deleting a containerized service name=" + serviceName +
                " epoch=" + placementEpoch);

        // validate and get the service metadata for the provided serviceName and placementEpoch
        if (!this.serviceInstances.containsKey(serviceName)) {
            Logger.getGlobal().log(Level.WARNING,
                    "Ignoring delete request for unknown service with name=" + serviceName);
            return true;
        }
        ServiceInstance serviceInstance =
                this.serviceInstances.get(serviceName).get(placementEpoch);
        if (serviceInstance == null) {
            Logger.getGlobal().log(Level.WARNING,
                    String.format("Ignoring delete request for unknown " +
                            "epoch=%d for service with name=%s", placementEpoch, serviceName));
            return true;
        }

        // get the container names and mount dir, then remove the in-memory service metadata
        List<String> toBeRemovedContainerNames = serviceInstance.containerNames;
        String toBeRemovedMountDir = stateDiffRecorder.getTargetDirectory(
                serviceName, placementEpoch);
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

        return true;
    }

    private boolean stopContainerizedServiceInstance(String serviceName, int placementEpoch) {
        assert serviceName != null && !serviceName.isEmpty();
        assert placementEpoch >= 0;

        System.out.printf(">> %s:XdnGigapaxosApp stopServiceInstance name=%s epoch=%d\n",
                this.myNodeId, serviceName, placementEpoch);

        // validate and get the service metadata for the provided serviceName and placementEpoch
        if (!this.serviceInstances.containsKey(serviceName)) {
            Logger.getGlobal().log(Level.WARNING,
                    "Ignoring stop request for unknown service with name=" + serviceName);
            return true;
        }
        ServiceInstance serviceInstance = this.serviceInstances.get(serviceName).get(placementEpoch);
        if (serviceInstance == null) {
            Logger.getGlobal().log(Level.WARNING,
                    String.format("Ignoring stop request for unknown " +
                            "epoch=%d for service with name=%s", placementEpoch, serviceName));
            return true;
        }

        List<String> toBeStoppedContainerNames = serviceInstance.containerNames;
        for (String containerName : toBeStoppedContainerNames) {
            boolean isSuccess = this.stopContainer(containerName);
            assert isSuccess : "failed to stop container " + containerName;
        }

        return true;
    }

    /**
     * startContainer runs the bash command below to start running a docker container.
     */
    private boolean startContainer(XdnServiceProperties properties, String networkName) {
        if (recorderType.equals(RecorderType.FUSELOG)) {
            return startContainerWithFSMount(properties, networkName);
        }

        String containerName = String.format("%s.%s.xdn.io",
                properties.serviceName, this.myNodeId);
        String startCommand = String.format("docker run -d --name=%s --network=%s --publish=%d:%d %s",
                containerName, networkName, properties.mappedPort, properties.exposedPort,
                properties.dockerImages.get(0));
        int exitCode = runShellCommand(startCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to start container");
            return false;
        }

        return true;
    }

    private boolean startContainerWithFSMount(XdnServiceProperties properties, String networkName) {
        String containerName = String.format("%s.%s.xdn.io",
                properties.serviceName, this.myNodeId);

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
        String mountCommand = String.format("%s -o allow_other -f -s %s", FUSELOG_BIN_PATH, stateDirPath);
        var t = new Thread() {
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
        String startCommand = String.format("docker run -d --name=%s --network=%s --publish=%d:%d" +
                        " --mount type=bind,source=%s,target=%s %s",
                containerName, networkName, properties.mappedPort, properties.exposedPort,
                stateDirPath, properties.stateDir, properties.dockerImages.get(0));
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
        String createNetCmd = String.format("docker network create %s",
                networkName);
        int exitCode = runShellCommand(createNetCmd, false);
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
        String command = String.format("docker cp %s:%s %s",
                service.statefulContainer,
                service.stateDirectory + "/.",
                statediffDirPath + "/");
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
            return String.format("xdn:statediff:%s:%s",
                    this.myNodeId,
                    new String(statediffBytes, StandardCharsets.ISO_8859_1));
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

            return String.format("xdn:statediff:%s:%s",
                    this.myNodeId,
                    new String(statediffBuffer.array(), StandardCharsets.ISO_8859_1));
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

    /**********************************************************************************************
     *             Begin implementation methods for BackupableApplication interface               *
     *********************************************************************************************/

    private static final String XDN_STATE_DIFF_PREFIX = "xdn:sd:";

    @Override
    public String captureStatediff(String serviceName) {
        int currentPlacementEpoch = this.getEpoch(serviceName);
        String stateDiff = stateDiffRecorder.captureStateDiff(serviceName, currentPlacementEpoch);
        return XDN_STATE_DIFF_PREFIX + stateDiff;
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
        boolean isApplySuccess = stateDiffRecorder.applyStateDiff(
                serviceName, currentPlacementEpoch, stateDiffContent);
        if (!isApplySuccess) {
            throw new RuntimeException("failed to apply stateDiff");
        }

        // restart the container
        if (IS_RESTART_UPON_STATE_DIFF_APPLY) {
            String restartCommand = String.format("docker container restart %s", service.statefulContainer);
            int exitCode = runShellCommand(restartCommand, true);
            if (exitCode != 0) {
                return false;
            }
        }

        return true;
    }

    public boolean applyStatediff2(String serviceName, String statediff) {
        return true;
    }


    private boolean applyStatediffWithFuse(String serviceName, String statediff) {
        // TODO: when applying statediff the service need to be stopped/paused, the filesystem need to be stopped.

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
                String exceptionMessage = String.format(
                        "Unknown container image with name '%s'. Ensure the image is accessible " +
                                "at Docker Hub, our default container registry. An example of " +
                                "container available from Docker Hub is " +
                                "'fadhilkurnia/xdn-bookcatalog'. You can also use container from " +
                                "other registries (e.g., ghcr.io, quay.io, etc)."
                        , imageName);
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
     * Captures the final state of a containerized service at specific placement epoch, then put
     * the captured final state into a temporary archive at
     * /tmp/xdn/final/<nodeId>/<serviceName>/<epoch>/final_state.tar.
     *
     * @param serviceName name of the service which final state want to be captured.
     * @param epoch       the placement epoch whose final state want to be captured.
     * @return true on success.
     */
    private boolean captureContainerizedServiceFinalState(String serviceName, int epoch) {
        String finalStateDirPath = String.format("/tmp/xdn/final_tmp/%s/%s/%d/",
                this.myNodeId, serviceName, epoch);

        // Validate the service instance
        Map<Integer, ServiceInstance> epochToInstanceMap = this.serviceInstances.get(serviceName);
        if (epochToInstanceMap == null) {
            Logger.getGlobal().log(Level.WARNING,
                    "Ignoring capture final state request for an unknown service with name=" +
                            serviceName);
            return true;
        }
        ServiceInstance serviceInstance = epochToInstanceMap.get(epoch);
        if (serviceInstance == null) {
            Logger.getGlobal().log(Level.WARNING,
                    String.format("Ignoring capture final state request for an unknown " +
                            "epoch=%d for name=%s", epoch, serviceName));
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
        code = Shell.runCommand(stateCopyCommand, true);
        assert code == 0;

        // Archive the final state into a tar file
        String finalStateTarDirPath = String.format("/tmp/xdn/final/%s/%s/%d/",
                this.myNodeId, serviceName, epoch);
        String removeTarDirCommand = String.format("rm -rf %s", finalStateTarDirPath);
        code = Shell.runCommand(removeTarDirCommand, true);
        assert code == 0;
        String createTarDirCommand = String.format("mkdir -p %s", finalStateTarDirPath);
        code = Shell.runCommand(createTarDirCommand, true);
        assert code == 0;
        String finalStateTarFilePath = finalStateTarDirPath + "final_state.tar";
        ZipFiles.zipDirectory(new File(finalStateDirPath), finalStateTarFilePath);

        return true;
    }

    @Override
    public String getFinalState(String name, int epoch) {
        // TODO: validate whether this works with PrimaryBackup or not
        System.out.println(">>> " + this.myNodeId + ":XdnGigapaxosApp - getFinalState name=" +
                name + " epoch=" + epoch);

        // Validate the service instance
        Map<Integer, ServiceInstance> epochToInstanceMap = this.serviceInstances.get(name);
        if (epochToInstanceMap == null) {
            Logger.getGlobal().log(Level.WARNING,
                    "Ignoring capture final state request for an unknown service with name=" +
                            name);
            return null;
        }
        ServiceInstance serviceInstance = epochToInstanceMap.get(epoch);
        if (serviceInstance == null) {
            Logger.getGlobal().log(Level.WARNING,
                    String.format("Ignoring capture final state request for an unknown " +
                            "epoch=%d for name=%s", epoch, name));
            return null;
        }

        // Check if the final state is already captured previously
        String finalStatePath = String.format("/tmp/xdn/final/%s/%s/%d/final_state.tar",
                this.myNodeId, name, epoch);
        File finalStateArchive = new File(finalStatePath);
        if (!finalStateArchive.exists()) {
            boolean isCaptureSuccess = this.captureContainerizedServiceFinalState(name, epoch);
            assert isCaptureSuccess;
        }

        // Convert the final state archive into String
        // Read the archive and convert into string
        byte[] finalStateBytes = null;
        String finalStatePrefix =
                String.format("%s%d:", ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX, epoch);
        try {
            finalStateBytes = Files.readAllBytes(Paths.get(finalStatePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // combine the prefix with the actual final state
        // format: xdn:final:<epoch>::<serviceProperty>::<finalState>
        return String.format("%s:%s::%s",
                finalStatePrefix,
                serviceInstance.property.toJsonString(),
                Base64.getEncoder().encodeToString(finalStateBytes));
    }

    @Override
    public void putInitialState(String name, int epoch, String state) {
        var exceptionMessage = String.format(
                "XDNGigapaxosApp.putInitialState is unimplemented, serviceName=%s epoch=%d state=%s",
                name, epoch, state);
        throw new RuntimeException(exceptionMessage);
    }

    @Override
    public boolean deleteFinalState(String name, int epoch) {
        System.out.println(">> XdnGigapaxosApp:" + this.myNodeId +
                " -- deleteFinalState(name=" + name + ",epoch=" + epoch + ")");
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
     * This method tries to find available port, most of the time.
     * It is possible, in race condition, that the port deemed as available
     * is being used by others even when this method return the port.
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

    private boolean startContainer(String imageName, String containerName, String networkName,
                                   String hostName, Integer exposedPort, Integer publishedPort,
                                   Integer allocatedHttpPort, String mountDirSource,
                                   String mountDirTarget, Map<String, String> env) {

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
        if (mountDirSource != null && !mountDirSource.isEmpty() &&
                mountDirTarget != null && !mountDirTarget.isEmpty()) {
            mountSubCmd = String.format("--mount type=bind,source=%s,target=%s",
                    mountDirSource, mountDirTarget);
        }

        String envSubCmd = "";
        if (env != null) {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<String, String> keyVal : env.entrySet()) {
                sb.append(String.format("--env %s=%s ", keyVal.getKey(), keyVal.getValue()));
            }
            envSubCmd = sb.toString();
        }

        // TODO: investigate the use of this user id with fuselog
        String userSubCmd = "";
        int uid = Utils.getUid();
        int gid = Utils.getGid();
        if (uid != 0 && this.recorderType == RecorderType.FUSELOG) {
            userSubCmd = String.format("--user=%d:%d", uid, gid);
        }

        String clearCommand = String.format("docker container rm --force %s", containerName);
        Shell.runCommand(clearCommand, true);
        String startCommand =
                String.format("docker run --rm -d --name=%s --hostname=%s --network=%s " +
                                "%s %s %s %s %s %s",
                        containerName, hostName, networkName, publishPortSubCmd, exposePortSubCmd,
                        mountSubCmd, envSubCmd, userSubCmd, imageName);
        int exitCode = Shell.runCommand(startCommand, false);
        if (exitCode != 0) {
            System.err.println("failed to start container");
            return false;
        }

        return true;
    }

    private void forwardHttpRequestToContainerizedService(XdnHttpRequest xdnRequest) {
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
            logger.log(Level.FINE, "{0}:{1} - HTTP pre-execution over {2}ms",
                    new Object[]{this.myNodeId, this.getClass().getSimpleName(),
                            (preExecutionElapsedTime / 1_000_000.0)});
        }

        // Create Http Request
        HttpRequest javaNetHttpRequest = xdnRequest
                .getJavaNetHttpRequest(true, targetPort);
        long endRequestCreationTime = System.nanoTime();

        // Forward request to the containerized service, get the http response.
        HttpResponse<byte[]> response;
        try {
            response = this.serviceClient.send(
                    javaNetHttpRequest, HttpResponse.BodyHandlers.ofByteArray());
        } catch (IOException | InterruptedException e) {
            xdnRequest.setHttpResponse(createNettyHttpErrorResponse(e));
            return;
        }
        long endRequestResponseTime = System.nanoTime();

        // Convert the response into Netty Http Response
        io.netty.handler.codec.http.HttpResponse nettyHttpResponse =
                createNettyHttpResponse(response);
        long endConversionTime = System.nanoTime();

        // Store the response in the xdn request, which will be returned to the end client.
        nettyHttpResponse.headers().set("X-E-EXC-TS-" + myNodeId, System.nanoTime());
        xdnRequest.setHttpResponse(nettyHttpResponse);
        long endResponseStoreTime = System.nanoTime();

        logger.log(Level.FINE, "{0}:{1} - docker proxy takes {2}ms, val={3}ms crt={4}ms " +
                        "exc={5}ms conv={6}ms sto={7}ms)",
                new Object[]{this.myNodeId, this.getClass().getSimpleName(),
                        (endResponseStoreTime - startTime) / 1_000_000.0,
                        (endValidationTime - startTime) / 1_000_000.0,
                        (endRequestCreationTime - endValidationTime) / 1_000_000.0,
                        (endRequestResponseTime - endRequestCreationTime) / 1_000_000.0,
                        (endConversionTime - endRequestResponseTime) / 1_000_000.0,
                        (endResponseStoreTime - endConversionTime) / 1_000_000.0});
    }

    private boolean forwardHttpRequestToContainerizedService(XdnJsonHttpRequest xdnRequest) {
        String serviceName = xdnRequest.getServiceName();
        if (isServiceActive.get(serviceName) != null && !isServiceActive.get(serviceName)) {
            activate(serviceName);
            isServiceActive.put(serviceName, true);
        }

        try {
            // create http request
            HttpRequest httpRequest = convertXDNRequestToHttpRequest(xdnRequest);
            if (httpRequest == null) {
                return false;
            }

            // forward request to the containerized service, and get the http response
            HttpResponse<byte[]> response = serviceClient.send(httpRequest,
                    HttpResponse.BodyHandlers.ofByteArray());

            // convert the response into netty's http response
            io.netty.handler.codec.http.HttpResponse nettyHttpResponse =
                    createNettyHttpResponse(response);

            // store the response in the xdn request, later to be returned to the end client.
            // System.out.println(">> " + nodeID + " storing response " + nettyHttpResponse);
            xdnRequest.setHttpResponse(nettyHttpResponse);
            return true;
        } catch (Exception e) {
            xdnRequest.setHttpResponse(createNettyHttpErrorResponse(e));
            e.printStackTrace();
            return true;
        }
    }

    // convertXDNRequestToHttpRequest converts Netty's HTTP request into Java's HTTP request
    private HttpRequest convertXDNRequestToHttpRequest(XdnJsonHttpRequest xdnRequest) {
        try {
            // preparing url to the containerized service
            String url = String.format("http://127.0.0.1:%d%s",
                    this.activeServicePorts.get(xdnRequest.getServiceName()),
                    xdnRequest.getHttpRequest().uri());

            // preparing the HTTP request body, if any
            // TODO: handle non text body, ie. file or binary data
            HttpRequest.BodyPublisher bodyPublisher = HttpRequest.BodyPublishers.noBody();
            if (xdnRequest.getHttpRequestContent() != null &&
                    xdnRequest.getHttpRequestContent().content() != null) {
                bodyPublisher = HttpRequest
                        .BodyPublishers
                        .ofString(xdnRequest.getHttpRequestContent().content()
                                .toString(StandardCharsets.UTF_8));
            }

            // preparing the HTTP request builder
            HttpRequest.Builder httpReqBuilder = HttpRequest.newBuilder()
                    .uri(new URI(url))
                    .method(xdnRequest.getHttpRequest().method().toString(),
                            bodyPublisher);

            // preparing the HTTP headers, if any.
            // note that the code need to be run with the following flag:
            // "-Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host",
            // otherwise setting those restricted headers here will later trigger
            // java.lang.IllegalArgumentException, such as: restricted header name: "Host".
            if (xdnRequest.getHttpRequest().headers() != null) {
                Iterator<Map.Entry<String, String>> it = xdnRequest.getHttpRequest()
                        .headers().iteratorAsString();
                while (it.hasNext()) {
                    Map.Entry<String, String> entry = it.next();
                    httpReqBuilder.setHeader(entry.getKey(), entry.getValue());
                }
            }

            return httpReqBuilder.build();
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private io.netty.handler.codec.http.HttpResponse createNettyHttpResponse(
            HttpResponse<byte[]> httpResponse) {
        // copy http headers, if any
        HttpHeaders headers = new DefaultHttpHeaders();
        for (String headerKey : httpResponse.headers().map().keySet()) {
            for (String headerVal : httpResponse.headers().allValues(headerKey)) {
                headers.add(headerKey, headerVal);
            }
        }

        // by default, we have an empty header trailing for the response
        HttpHeaders trailingHeaders = new DefaultHttpHeaders();

        // build the http response
        io.netty.handler.codec.http.HttpResponse result = new DefaultFullHttpResponse(
                getNettyHttpVersion(httpResponse.version()),
                HttpResponseStatus.valueOf(httpResponse.statusCode()),
                Unpooled.copiedBuffer(httpResponse.body()),
                headers,
                trailingHeaders
        );
        return result;
    }

    private io.netty.handler.codec.http.HttpResponse createNettyHttpErrorResponse(Exception e) {
        StringWriter sw = new StringWriter();
        sw.write("{\"error\":\"Failed to get response from the containerized service.\",");
        sw.write("\"message\":\"");
        sw.write(e.getMessage());
        sw.write("\"}");
        DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                Unpooled.copiedBuffer(sw.toString().getBytes()));
        response.headers().add(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        return response;
    }

    private HttpVersion getNettyHttpVersion(HttpClient.Version httpClientVersion) {
        // TODO: upgrade netty that support HTTP2
        switch (httpClientVersion) {
            case HTTP_1_1, HTTP_2 -> {
                return HttpVersion.HTTP_1_1;
            }
        }
        return HttpVersion.HTTP_1_1;
    }


    private int runShellCommand(String command, boolean isSilent) {
        return Shell.runCommand(command, isSilent, null);
    }

    private int runShellCommand(String command, boolean isSilent,
                                Map<String, String> environmentVariables) {
        return Shell.runCommand(command, isSilent, environmentVariables);
    }


    /**********************************************************************************************
     *                                  End utility methods                                     *
     *********************************************************************************************/

}
