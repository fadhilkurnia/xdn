package edu.umass.cs.primarybackup;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.JSONMessenger;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.primarybackup.packets.*;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.XdnGigapaxosApp;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import edu.umass.cs.xdn.service.ServiceProperty;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class PrimaryBackupManager<NodeIDType> implements AppRequestParser {

    private final boolean ENABLE_INTERNAL_REDIRECT_PRIMARY = true;

    private final NodeIDType myNodeID;
    private final Stringifiable<NodeIDType> nodeIDTypeStringifiable;
    private final PaxosManager<NodeIDType> paxosManager;
    private final Replicable paxosMiddlewareApp;
    private final Replicable replicableApp;
    private final BackupableApplication backupableApp;

    private final Map<String, PrimaryEpoch<NodeIDType>> currentPrimaryEpoch;
    private final Map<String, Role> currentRole;
    private final Map<String, NodeIDType> currentPrimary;

    private final Messenger<NodeIDType, ?> messenger;

    // requests while waiting role change from PRIMARY_CANDIDATE to PRIMARY
    private final Queue<RequestAndCallback> outstandingRequests;

    // requests forwarded to the PRIMARY
    private final Map<Long, RequestAndCallback> forwardedRequests;

    private final boolean ENABLE_NON_DETERMINISTIC_INIT = Config.getGlobalBoolean(
            ReconfigurationConfig.RC.XDN_PB_ENABLE_NON_DETERMINISTIC_INIT);

    private final Logger logger = Logger.getLogger(PrimaryBackupManager.class.getName());

    /**
     * The default constructor
     *
     * @param nodeId
     * @param nodeIdDeserializer
     * @param replicableApp
     * @param messenger
     */
    protected PrimaryBackupManager(NodeIDType nodeId,
                                   Stringifiable<NodeIDType> nodeIdDeserializer,
                                   Replicable replicableApp,
                                   Messenger<NodeIDType, JSONObject> messenger) {
        this(nodeId,
                nodeIdDeserializer,
                PrimaryBackupMiddlewareApp.wrapApp(replicableApp),
                null,
                messenger);
    }

    /**
     * The alternative constructor when we need to use an existing PaxosManager
     *
     * @param nodeId
     * @param nodeIdDeserializer
     * @param middlewareApp
     * @param paxosManager
     * @param messenger
     */
    protected PrimaryBackupManager(NodeIDType nodeId,
                                   Stringifiable<NodeIDType> nodeIdDeserializer,
                                   PrimaryBackupMiddlewareApp middlewareApp,
                                   PaxosManager<NodeIDType> paxosManager,
                                   Messenger<NodeIDType, JSONObject> messenger) {
        assert nodeId != null;
        assert nodeIdDeserializer != null;
        assert middlewareApp != null;
        assert messenger != null;

        // Ensure the Replicable App and Backupable App wrapped by the PrimaryBackupMiddlewareApp
        // is the same Application because captureStateDiff(.) in the BackupableApplication is
        // invoked right after the execute(.) in the Replicable.
        Replicable replicableApp = middlewareApp.getReplicableApp();
        BackupableApplication backupableApp = middlewareApp.getBackupableApp();
        assert replicableApp.getClass().getSimpleName().
                equals(backupableApp.getClass().getSimpleName()) :
                "The wrapped Replicable and Backupable application must be the same App.";

        // Set the Application. Note that all these applications below should refer to the same
        // Application. We set them as different variables because of their
        // different responsibilities.
        this.replicableApp = replicableApp;
        this.backupableApp = backupableApp;
        this.paxosMiddlewareApp = middlewareApp;
        middlewareApp.setManager(this);

        // Initialize PaxosManager, if null is given.
        if (paxosManager == null) {
            PrimaryBackupManager.setupPaxosConfiguration();
            paxosManager = new PaxosManager<>(nodeId,
                    nodeIdDeserializer,
                    messenger,
                    middlewareApp,
                    "/tmp/gigapaxos/pb_paxos_logs/",
                    true)
                    .initClientMessenger(new InetSocketAddress(
                                    messenger.getNodeConfig().getNodeAddress(nodeId),
                                    messenger.getNodeConfig().getNodePort(nodeId)),
                            messenger);
        }

        // Ensure the given application to Paxos Manager is our middleware App. This is needed
        // because Primary Backup needs to check the request before Paxos invokes the execute(.)
        // method. For example, in Primary Backup the StateDiffRequest will be ignored if it is
        // stale, e.g., the primary epoch already changed previously.
        assert paxosManager.isAppEquals(middlewareApp) :
                "The Replicable application handled by Paxos Manager must be " +
                        "Primary Backup Middleware App";
        this.validatePaxosConfiguration();
        this.paxosManager = paxosManager;

        this.myNodeID = nodeId;
        this.nodeIDTypeStringifiable = nodeIdDeserializer;
        this.currentPrimaryEpoch = new ConcurrentHashMap<>();
        this.currentRole = new ConcurrentHashMap<>();
        this.currentPrimary = new ConcurrentHashMap<>();

        this.messenger = messenger;
        this.outstandingRequests = new ConcurrentLinkedQueue<>();
        this.forwardedRequests = new ConcurrentHashMap<>();
    }

    // setupPaxosConfiguration sets Paxos configuration required for PrimaryBackup use case
    public static void setupPaxosConfiguration() {
        String[] args = {
                String.format("%s=%b", PaxosConfig.PC.ENABLE_EMBEDDED_STORE_SHUTDOWN, true),
                String.format("%s=%b", PaxosConfig.PC.ENABLE_STARTUP_LEADER_ELECTION, false),
                String.format("%s=%b", PaxosConfig.PC.FORWARD_PREEMPTED_REQUESTS, false),
                String.format("%s=%d", PaxosConfig.PC.PACKET_DEMULTIPLEXER_THREADS, 0),
                String.format("%s=%b", PaxosConfig.PC.HIBERNATE_OPTION, false),
                String.format("%s=%b", PaxosConfig.PC.BATCHING_ENABLED, false),
        };
        Config.register(args);
        // TODO: investigate how to enable batching without stateDiff reordering
    }

    private void validatePaxosConfiguration() {
        assert Config.getGlobalBoolean(PaxosConfig.PC.ENABLE_EMBEDDED_STORE_SHUTDOWN);
        assert !Config.getGlobalBoolean(PaxosConfig.PC.ENABLE_STARTUP_LEADER_ELECTION);
        assert !Config.getGlobalBoolean(PaxosConfig.PC.FORWARD_PREEMPTED_REQUESTS);
        assert Config.getGlobalInt(PaxosConfig.PC.PACKET_DEMULTIPLEXER_THREADS) == 0;
        assert !Config.getGlobalBoolean(PaxosConfig.PC.HIBERNATE_OPTION);
        assert !Config.getGlobalBoolean(PaxosConfig.PC.BATCHING_ENABLED);
    }


    public static Set<IntegerPacketType> getAllPrimaryBackupPacketTypes() {
        return new HashSet<>(List.of(PrimaryBackupPacketType.values()));
    }

    public boolean handlePrimaryBackupPacket(
            PrimaryBackupPacket packet, ExecutedCallback callback) {
        assert packet != null;
        String serviceName = packet.getServiceName();
        logger.log(Level.FINE,
                String.format("%s:%s - handling packet name=%s %s isPrimary=%b isPaxosCoordinator=%b",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        serviceName, packet.getRequestType(),
                        isCurrentPrimary(serviceName),
                        this.paxosManager.isPaxosCoordinator(serviceName)));

        // RequestPacket: client -> entry replica
        if (packet instanceof RequestPacket requestPacket) {
            return handleRequestPacket(requestPacket, callback);
        }

        // ForwardedRequestPacket: entry replica -> primary
        if (packet instanceof ForwardedRequestPacket forwardedRequestPacket) {
            return handleForwardedRequestPacket(forwardedRequestPacket);
        }

        // ResponsePacket: primary -> entry replica
        if (packet instanceof ResponsePacket responsePacket) {
            return handleResponsePacket(responsePacket);
        }

        // ChangePrimaryPacket: client -> entry replica
        if (packet instanceof ChangePrimaryPacket changePrimaryPacket) {
            return handleChangePrimaryPacket(changePrimaryPacket, callback);
        }

        // ApplyStateDiffPacket: primary -> replica
        // only executed by XDNGigapaxosApp
        if (packet instanceof ApplyStateDiffPacket applyStateDiffPacket) {
            return executeApplyStateDiffPacket(applyStateDiffPacket);
        }

        // StartEpochPacket: primary candidate -> replica
        // only executed by XDNGigapaxosApp
        if (packet instanceof StartEpochPacket startEpochPacket) {
            return executeStartEpochPacket(startEpochPacket);
        }

        // InitBackupPacket: primary -> replica
        // only executed by XDNGigapaxosApp (backup)
        if (packet instanceof InitBackupPacket initBackupPacket) {
            return executeInitBackupPacket(initBackupPacket);
        }

        String exceptionMsg = String.format("unknown primary backup packet '%s'",
                packet.getClass().getSimpleName());
        logger.log(Level.SEVERE,
                String.format("%s:%s - %s",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        exceptionMsg));
        throw new RuntimeException(exceptionMsg);
    }

    private boolean handleRequestPacket(RequestPacket packet, ExecutedCallback callback) {
        String serviceName = packet.getServiceName();

        Role currentServiceRole = this.currentRole.get(serviceName);
        if (currentServiceRole == null) {
            logger.log(Level.FINE, String.format("%s:%s - unknown service name=%s",
                    myNodeID, PrimaryBackupManager.class.getSimpleName(),
                    serviceName));
            return true;
        }

        if (currentServiceRole == Role.PRIMARY) {
            return executeRequestCoordinateStateDiff(packet, callback);
        }

        if (currentServiceRole == Role.PRIMARY_CANDIDATE) {
            RequestAndCallback rc = new RequestAndCallback(packet, callback);
            outstandingRequests.add(rc);
            return true;
        }

        if (currentServiceRole == Role.BACKUP) {
            return handRequestToPrimary(packet, callback);
        }

        String exceptionMessage = String.format("unknown role %s for service %s\n",
                currentServiceRole, serviceName);
        logger.log(Level.SEVERE, String.format("%s:%s - %s",
                myNodeID, PrimaryBackupManager.class.getSimpleName(),
                exceptionMessage));
        throw new RuntimeException(String.format("Unknown role %s for service %s\n",
                currentServiceRole, serviceName));
    }

    // TODO: handle a batch of request from outstanding queue, instead of handling it one by one.
    private boolean executeRequestCoordinateStateDiff(RequestPacket packet,
                                                      ExecutedCallback callback) {
        String serviceName = packet.getServiceName();

        // ensure this method is only invoked by the primary node
        Role currentServiceRole = this.currentRole.get(serviceName);
        assert currentServiceRole == Role.PRIMARY : String.format("%s my role for %s is %s",
                myNodeID, serviceName, currentServiceRole.toString());

        boolean isPaxosCoordinator = this.paxosManager.isPaxosCoordinator(serviceName);
        if (!isPaxosCoordinator) {
            this.paxosManager.tryToBePaxosCoordinator(serviceName);
        }

        // RequestPacket -> AppRequest -> execute() -> AppResponse -> RequestPacket (with response)
        Request appRequest = null;
        try {
            // parse the encapsulated application request
            String encodedServiceRequest = new String(packet.getEncodedServiceRequest(),
                    StandardCharsets.ISO_8859_1);
            appRequest = replicableApp.getRequest(encodedServiceRequest);
        } catch (RequestParseException e) {
            throw new RuntimeException(e);
        }

        logger.log(Level.FINER,
                String.format(
                        "%s:%s - handling request on primary name=%s request=%s reqSize=%d bytes",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        packet.getServiceName(),
                        appRequest.getClass().getSimpleName(),
                        packet.getEncodedServiceRequest().length));

        PrimaryEpoch<NodeIDType> currentEpoch = this.currentPrimaryEpoch.get(serviceName);
        if (currentEpoch == null) {
            logger.log(Level.WARNING,
                    String.format(
                            "%s:%s - unknown current primary epoch for %s",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            packet.getServiceName()));
            return false;
        }

        // execute the app request, and capture the stateDiff
        boolean isExecuteSuccess;
        String stateDiff;
        synchronized (currentEpoch) {
            long startTime = System.nanoTime();
            isExecuteSuccess = replicableApp.execute(appRequest);
            if (!isExecuteSuccess) {
                logger.log(Level.WARNING,
                        String.format(
                                "%s:%s - failed to execute request for %s id=%d",
                                myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                packet.getServiceName(),
                                packet.getRequestID()));
                return false;
            }
            long endTime = System.nanoTime();
            long elapsedTime = endTime - startTime;
            double elapsedTimeMs = (double) elapsedTime / 1_000_000.0;
            logger.log(Level.FINER,
                    String.format(
                            "%s:%s - executing request within %f ms",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            elapsedTimeMs));
            long sdCaptureStartTime = System.nanoTime();
            stateDiff = backupableApp.captureStatediff(serviceName);
            endTime = System.nanoTime();
            elapsedTime = endTime - sdCaptureStartTime;
            elapsedTimeMs = (double) elapsedTime / 1_000_000.0;
            logger.log(Level.FINER,
                    String.format(
                            "%s:%s - capturing stateDiff within %f ms size=%d bytes",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            elapsedTimeMs,
                            stateDiff.length()));
        }

        // put response if request is ClientRequest
        if (appRequest instanceof ClientRequest) {
            ClientRequest responsePacket = ((ClientRequest) appRequest).getResponse();
            if (responsePacket == null) {
                logger.log(Level.WARNING,
                        String.format(
                                "%s:%s - expecting non-null response for client request (svc=%s)",
                                myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                packet.getServiceName()));
                return false;
            }

            packet.setResponse(responsePacket);
            logger.log(Level.FINER,
                    String.format(
                            "%s:%s - primary set response id=%d name=%s pbEpoch=%s",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            packet.getRequestID(),
                            packet.getServiceName(),
                            currentEpoch));
        }

        // propose the stateDiff
        ApplyStateDiffPacket applyStateDiffPacket = new ApplyStateDiffPacket(
                serviceName, currentEpoch, stateDiff);
        ReplicableClientRequest gpPacket = ReplicableClientRequest.wrap(applyStateDiffPacket);
        gpPacket.setClientAddress(messenger.getListeningSocketAddress());
        logger.log(Level.FINER,
                String.format(
                        "%s:%s - primary proposing stateDiff id=%d name=%s pbEpoch=%s len=%d bytes",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        applyStateDiffPacket.getRequestID(),
                        packet.getServiceName(),
                        currentEpoch,
                        stateDiff.length()));
        long proposeStartTime = System.nanoTime();
        this.paxosManager.propose(
                serviceName,
                gpPacket,
                (stateDiffPacket, handled) -> {
                    long currTime = System.nanoTime();
                    long proposeElapsedTime = currTime - proposeStartTime;
                    double proposeElapsedTimeMs = (double) proposeElapsedTime / 1_000_000.0;
                    assert stateDiffPacket instanceof ApplyStateDiffPacket :
                            String.format("Unexpected accepted request, expecting %s but found %s",
                                    ApplyStateDiffPacket.class.getSimpleName(),
                                    stateDiffPacket.getClass().getSimpleName());
                    ApplyStateDiffPacket acceptedStateDiffPacket =
                            (ApplyStateDiffPacket) stateDiffPacket;
                    logger.log(Level.FINER,
                            String.format(
                                    "%s:%s - stateDiff is committed within %f ms id=%d name=%s pbEpoch=%s len=%d bytes",
                                    myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                    proposeElapsedTimeMs,
                                    acceptedStateDiffPacket.getRequestID(),
                                    acceptedStateDiffPacket.getServiceName(),
                                    acceptedStateDiffPacket.getPrimaryEpochString(),
                                    acceptedStateDiffPacket.getStateDiff().length()));
                    callback.executed(packet, handled);
                });

        return true;
    }

    private boolean handRequestToPrimary(RequestPacket packet, ExecutedCallback callback) {
        if (!ENABLE_INTERNAL_REDIRECT_PRIMARY) {
            askClientToContactPrimary(packet, callback);
        }

        // get the current primary for the serviceName
        String serviceName = packet.getServiceName();
        NodeIDType currentPrimaryIdStr = currentPrimary.get(serviceName);
        if (currentPrimaryIdStr == null) {
            throw new RuntimeException("Unknown primary ID");
            // TODO: potential fix would be to ask the current Paxos' coordinator to be the Primary
        }

        logger.log(Level.FINE,
                String.format(
                        "%s:%s - backup forwarding request to primary at %s reqSize=%d bytes",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        currentPrimaryIdStr,
                        packet.getEncodedServiceRequest().length));

        // store the request and callback, so later we can send the response back to client
        // after receiving the response from the primary
        RequestAndCallback rc = new RequestAndCallback(packet, callback);
        this.forwardedRequests.put(packet.getRequestID(), rc);

        // prepare the forwarded request
        ForwardedRequestPacket forwardPacket = new ForwardedRequestPacket(
                serviceName,
                myNodeID.toString(),
                packet.toString().getBytes(StandardCharsets.ISO_8859_1));
        GenericMessagingTask<NodeIDType, PrimaryBackupPacket> m =
                new GenericMessagingTask<>(
                        currentPrimaryIdStr, forwardPacket);

        // send the forwarded request to the primary
        try {
            this.messenger.send(m);
        } catch (IOException | JSONException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    private void askClientToContactPrimary(RequestPacket packet, ExecutedCallback callback) {
        throw new RuntimeException("Unimplemented");
    }

    private boolean handleForwardedRequestPacket(
            ForwardedRequestPacket forwardedRequestPacket) {

        String groupName = forwardedRequestPacket.getServiceName();
        Role curentRole = null;
        synchronized (this) {
            curentRole = this.currentRole.get(groupName);
        }

        if (curentRole.equals(Role.BACKUP)) {
            throw new RuntimeException("Unimplemented: should re-forward request to primary");
        }

        if (curentRole.equals(Role.PRIMARY_CANDIDATE)) {
            throw new RuntimeException("Unimplemented: should buffer request");
        }

        if (curentRole.equals(Role.PRIMARY)) {
            byte[] encodedRequest = forwardedRequestPacket.getEncodedForwardedRequest();
            String encodedRequestString = new String(encodedRequest, StandardCharsets.ISO_8859_1);
            RequestPacket rp = RequestPacket.createFromString(encodedRequestString);
            this.executeRequestCoordinateStateDiff(rp, (executedRequest, handled) -> {
                // Forwarded request is executed, forwarding response back to the entry replica
                assert handled : "Unhandled request";
                assert executedRequest instanceof RequestPacket :
                        "Unexpected executedRequest of type "
                                + executedRequest.getClass().getSimpleName();

                ClientRequest requestWithResponse = (ClientRequest) executedRequest;
                ResponsePacket resp = new ResponsePacket(
                        executedRequest.getServiceName(),
                        rp.getRequestID(),
                        requestWithResponse.getResponse().toString().
                                getBytes(StandardCharsets.ISO_8859_1));
                String entryNodeIDStr = forwardedRequestPacket.getEntryNodeId();
                NodeIDType entryNodeID = nodeIDTypeStringifiable.valueOf(entryNodeIDStr);
                GenericMessagingTask<NodeIDType, ResponsePacket> m =
                        new GenericMessagingTask<>(entryNodeID, resp);
                try {
                    messenger.send(m);
                } catch (IOException | JSONException e) {
                    throw new RuntimeException(e);
                }

                // callback.executed(request, handled);
            });

            return true;
        }

        String exceptionMsg = String.format("%s:PrimaryBackupManager - unknown role for group '%s'",
                myNodeID, groupName);
        logger.log(Level.SEVERE,
                String.format(
                        "%s:%s - %s",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(), exceptionMsg));
        throw new RuntimeException(exceptionMsg);
    }

    private boolean handleResponsePacket(ResponsePacket responsePacket) {
        Request appRequest;
        byte[] encodedResponse = responsePacket.getEncodedResponse();
        String encodedResponseStr = new String(encodedResponse, StandardCharsets.ISO_8859_1);
        try {
            // Deserialize the response.
            appRequest = this.replicableApp.getRequest(encodedResponseStr);
            if (appRequest == null) {
                logger.log(Level.WARNING,
                        String.format(
                                "%s:%s - receiving ResponsePacket with malformed response",
                                myNodeID, PrimaryBackupManager.class.getSimpleName()));
                return false;
            }
        } catch (RequestParseException e) {
            throw new RuntimeException(e);
        }

        // TODO: re-architecture me.
        //   This is a quick hack for XDN to avoid request cache in XdnGigapaxosApp.
        //   The `this.replicableApp.getRequest(encodedResponseStr)` above uses cache and returns
        //   the original request *without* the response contained in the encodedResponseStr.
        //   That is why we need to specifically *force* to deserialize the encodedResponse here.
        //   Potential fixes:
        //    - update the cache in getRequest() of XdnGigapaxosApp to consider the request.
        if (appRequest instanceof XdnHttpRequestBatch) {
            appRequest = XdnHttpRequestBatch.createFromBytes(encodedResponse);
        }

        if (appRequest instanceof ClientRequest appRequestWithResponse) {
            Long executedRequestID = responsePacket.getRequestID();
            RequestAndCallback rc = forwardedRequests.get(executedRequestID);
            if (rc == null) {
                logger.log(Level.WARNING,
                        String.format(
                                "%s:%s - unknown callback for RequestPacket-%d (%s)",
                                myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                executedRequestID,
                                this.getAllForwardedRequestIDs()));
                return false;
            }
            rc.requestPacket().setResponse(appRequestWithResponse);
            rc.callback().executed(rc.requestPacket(), true);
        } else {
            logger.log(Level.WARNING,
                    String.format(
                            "%s:%s - unexpected non ClientRequest inside ResponsePacket",
                            myNodeID, PrimaryBackupManager.class.getSimpleName()));
            return false;
        }
        return true;
    }

    private boolean handleChangePrimaryPacket(ChangePrimaryPacket packet,
                                              ExecutedCallback callback) {

        // ignore ChangePrimary with incorrect nodeID
        if (!Objects.equals(packet.getNodeID(), myNodeID.toString())) {
            callback.executed(packet, false);
        }

        String groupName = packet.getServiceName();
        Role myCurrentRole = null;
        PrimaryEpoch<?> curEpoch = null;
        synchronized (this) {
            myCurrentRole = this.currentRole.get(groupName);
            curEpoch = this.currentPrimaryEpoch.get(groupName);
        }
        if (myCurrentRole == null) {
            logger.log(Level.SEVERE,
                    String.format(
                            "%s:%s - unknown role for service name '%s'",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(), groupName));
            return false;
        }
        if (myCurrentRole.equals(Role.PRIMARY)) {
            logger.log(Level.FINER,
                    String.format(
                            "%s:%s - already the primary for service name name '%s'",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(), groupName));
            callback.executed(packet, true);
            this.paxosManager.tryToBePaxosCoordinator(groupName);
            return true;
        }

        if (curEpoch == null) {
            logger.log(Level.SEVERE,
                    String.format(
                            "%s:%s - unknown current epoch for service name '%s'",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(), groupName));
            return true;
        }
        PrimaryEpoch<NodeIDType> newEpoch = new PrimaryEpoch<NodeIDType>(
                myNodeID, curEpoch.counter + 1);

        this.paxosManager.tryToBePaxosCoordinator(groupName); // could still be fail
        this.currentRole.put(groupName, Role.PRIMARY_CANDIDATE);
        this.currentPrimaryEpoch.put(groupName, newEpoch);
        StartEpochPacket startPacket = new StartEpochPacket(groupName, newEpoch);
        this.paxosManager.propose(
                groupName,
                startPacket,
                (proposedPacket, isHandled) -> {
                    System.out.printf("\n\n>> %s I'M THE PRIMARY NOW FOR %s!!\n\n",
                            myNodeID, groupName);
                    currentRole.put(groupName, Role.PRIMARY);
                    currentPrimary.put(groupName, myNodeID);
                    processOutstandingRequests();

                    callback.executed(packet, isHandled);

                    this.paxosManager.tryToBePaxosCoordinator(groupName);
                    this.paxosManager.tryToBePaxosCoordinator(groupName);
                }
        );
        return true;
    }

    // executeApplyStateDiffPacket is being called by execute() in the PaxosMiddlewareApp
    private boolean executeApplyStateDiffPacket(ApplyStateDiffPacket packet) {
        String groupName = packet.getServiceName();
        String primaryEpochStr = packet.getPrimaryEpochString();
        PrimaryEpoch<NodeIDType> primaryEpoch = new PrimaryEpoch<>(primaryEpochStr);
        PrimaryEpoch<NodeIDType> currentEpoch = this.currentPrimaryEpoch.get(groupName);
        Role myCurrentRole = this.currentRole.get(groupName);

        // System.out.printf(">>> %s:PaxosMiddlewareApp:executeStateDiff role=%s myEpoch=%s epoch=%s stateDiff=%s\n",
        //        myNodeID, myCurrentRole, currentEpoch, packet.getPrimaryEpoch(), packet.getStateDiff());

        // Invariant: when executing stateDiff for epoch e, this node must already execute
        //  startEpoch for epoch e.
        assert currentEpoch != null : "currentEpoch has not been updated";
        assert myCurrentRole != null : "Unknown role for " + groupName;

        // Case-1: lower epoch, ignoring stale stateDiff from older primary.
        if (primaryEpoch.compareTo(currentEpoch) < 0) {
            System.out.printf(">>> %s:PBManager ignoring stateDiff from old primary " +
                            "(%s, myEpoch=%s)\n",
                    myNodeID,
                    primaryEpoch,
                    currentEpoch);
            return true;
        }

        // Case-2: epoch is already current
        if (primaryEpoch.equals(currentEpoch)) {

            // Ignoring stateDiff generated by myself since myself is the primary and thus
            // already applied the stateDiff right after execution
            if (myCurrentRole.equals(Role.PRIMARY)) {
                // System.out.printf(">> PBManager-%s: ignoring stateDiff from myself\n",
                //        myNodeID);
                return true;
            }

            // This case should not happen, the epoch is already current yet this node
            // is still a primary candidate. The node should already change its role to PRIMARY
            // in the callback of propose(StartEpoch), before the node is able to propose a
            // stateDiff with current epoch.
            if (myCurrentRole.equals(Role.PRIMARY_CANDIDATE)) {
                assert false : "executing stateDiff from myself while still being a candidate";
                return true;
            }

            // As a backup, this node simply apply the stateDiff coming from the PRIMARY
            if (myCurrentRole.equals(Role.BACKUP)) {
                this.backupableApp.applyStatediff(groupName, packet.getStateDiff());
                return true;
            }

            throw new RuntimeException(String.format("PaxosMiddlewareApp: Unhandled case " +
                    "myEpoch=primaryEpoch=%s role=%s", primaryEpoch, myCurrentRole));
        }

        // Case-3: get higher epoch
        if (primaryEpoch.compareTo(currentEpoch) > 0) {
            throw new RuntimeException(String.format("PaxosMiddlewareApp: Executing higher " +
                    "epoch=%s before executing startEpoch", primaryEpoch));
        }

        return true;
    }

    // executeStartEpochPacket is being called by execute() in the PaxosMiddlewareApp
    private boolean executeStartEpochPacket(StartEpochPacket packet) {
        String groupName = packet.getServiceName();
        String newPrimaryEpochStr = packet.getStartingEpochString();
        PrimaryEpoch<NodeIDType> newPrimaryEpoch = new PrimaryEpoch<>(newPrimaryEpochStr);
        PrimaryEpoch<NodeIDType> currentEpoch = this.currentPrimaryEpoch.get(groupName);
        String newPrimaryIDStr = newPrimaryEpoch.nodeID;
        NodeIDType newPrimaryID = nodeIDTypeStringifiable.valueOf(newPrimaryIDStr);

        // update my current epoch, if its unknown
        if (currentEpoch == null) {
            this.currentPrimaryEpoch.put(groupName, newPrimaryEpoch);
            this.currentPrimary.put(groupName, newPrimaryID);
            currentEpoch = newPrimaryEpoch;
        }

        // receive smaller, ignore that epoch.
        // receive current epoch from myself, ignore the StartEpoch packet as it already
        // handled via callback of propose(StartEpoch)
        if (newPrimaryEpoch.compareTo(currentEpoch) <= 0) {
            return true;
        }

        // step down to be backup node
        if (newPrimaryEpoch.compareTo(currentEpoch) > 0) {
            Role myCurrentRole = this.currentRole.get(groupName);

            if (myCurrentRole == null) {
                this.currentRole.put(groupName, Role.BACKUP);
                this.currentPrimaryEpoch.put(groupName, newPrimaryEpoch);
                this.currentPrimary.put(groupName, newPrimaryID);
                System.out.printf(">> %s putting current primary for %s as %s\n",
                        myNodeID, groupName, newPrimaryID);
                return true;
            }

            if (myCurrentRole.equals(Role.BACKUP)) {
                this.currentRole.put(groupName, Role.BACKUP);
                this.currentPrimaryEpoch.put(groupName, newPrimaryEpoch);
                this.currentPrimary.put(groupName, newPrimaryID);
                System.out.printf(">> %s putting current primary for %s as %s\n",
                        myNodeID, groupName, newPrimaryID);
                return true;
            }

            if (myCurrentRole.equals(Role.PRIMARY) ||
                    myCurrentRole.equals(Role.PRIMARY_CANDIDATE)) {
                this.currentRole.put(groupName, Role.BACKUP);
                this.currentPrimaryEpoch.put(groupName, newPrimaryEpoch);
                this.currentPrimary.put(groupName, newPrimaryID);

                System.out.printf(">> %s shutting down .... \n", myNodeID);
                this.restartPaxosInstance(groupName);
                return true;
            }

            throw new RuntimeException(String.format("PrimaryBackupManager: Unhandled case " +
                            "myEpoch=%s primaryEpoch=%s role=%s", currentEpoch, newPrimaryEpoch,
                    myCurrentRole));
        }

        throw new RuntimeException(String.format("PrimaryBackupManager: Unhandled case " +
                "myEpoch=%s primaryEpoch=%s", currentEpoch, newPrimaryEpoch));
    }

    // executeInitBackupPacket is being called by execute() in the PaxosMiddlewareApp
    private boolean executeInitBackupPacket(InitBackupPacket packet) {
        String serviceName = packet.getServiceName();
        return this.replicableApp.restore(serviceName, "nondeter:start:backup");
    }

    // executeGetCheckpoint is being called by checkpoint() in the PaxosMiddlewareApp
    private String executeGetCheckpoint(String groupName) {
        return this.replicableApp.checkpoint(groupName);
    }

    // executeRestore is being called by restore() in the PaxosMiddlewareApp
    private boolean executeRestore(String groupName, String state) {
        if (state == null || state.isEmpty()) {
            this.currentPrimaryEpoch.remove(groupName);
            this.currentRole.put(groupName, Role.BACKUP);
        }
        return this.replicableApp.restore(groupName, state);
    }

    private String getAllForwardedRequestIDs() {
        StringBuilder result = new StringBuilder();
        for (RequestAndCallback rc : forwardedRequests.values()) {
            result.append(rc.requestPacket().getRequestID()).append(", ");
        }
        return result.toString();
    }

    /**
     * Note that PlacementEpoch, being used in this method, is not the same as the primaryEpoch.
     * PlacementEpoch increases when placement changes (i.e., reconfiguration, possibly with the
     * same set of nodes), however, PrimaryEpoch can increase even if there is no reconfiguration.
     * PrimaryEpoch increases when a new Primary emerges, even within the same replica group.
     */
    public boolean createPrimaryBackupInstance(String groupName,
                                               int placementEpoch,
                                               String initialState,
                                               Set<NodeIDType> nodes,
                                               String placementMetadata) {
        System.out.printf(">> %s PrimaryBackupManager - createPrimaryBackupInstance | " +
                        "groupName: %s, placementEpoch: %d, initialState: %s, nodes: %s\n",
                myNodeID, groupName, placementEpoch, initialState, nodes.toString());

        if (initialState.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)
                | initialState.startsWith(ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX)) {
            initialState = String.format("nondeter:create:%s", initialState);
        }

        boolean created = this.paxosManager.createPaxosInstanceForcibly(
                groupName, placementEpoch, nodes, this.paxosMiddlewareApp, initialState, 0);
        boolean alreadyExist = this.paxosManager.equalOrHigherVersionExists(
                groupName, placementEpoch);

        if (!created && !alreadyExist) {
            throw new PaxosInstanceCreationException((this
                    + " failed to create " + groupName + ":" + placementEpoch
                    + " with state [" + initialState + "]") + "; existing_version=" +
                    this.paxosManager.getVersion(groupName));
        }

        // FIXME: these three default replica groups must be handled with
        //  specific app that uses Paxos instead of PrimaryBackup.
        if (groupName.equals(PaxosConfig.getDefaultServiceName()) ||
                groupName.equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString()) ||
                groupName.equals(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString())) {
            return true;
        }

        // There are two ways to initialize a primary-backup replica group:
        // (1) if preferred coordinator is specified in placement metadata,
        //     then we set paxos' coordinator and the primary.
        // (2) if preferred coordinator is not specified: detect paxos leader -> set primary.
        if (placementMetadata != null) {
            boolean isInitSuccess = this.initializePrimaryEpoch(
                    groupName, nodes, placementMetadata, placementEpoch);
            assert isInitSuccess;
            return true;
        }
        boolean isInitializationSuccess = initializePrimaryEpoch(groupName, nodes, placementEpoch);
        if (!isInitializationSuccess) {
            logger.log(Level.WARNING,
                    String.format("Failed to initialize replica group for %s", groupName));
            return false;
        }

        return true;
    }

    private boolean initializePrimaryEpoch(String groupName, Set<NodeIDType> nodes,
                                           String placementMetadata, int placementEpoch) {
        assert groupName != null && !groupName.isEmpty();
        assert placementMetadata != null && !placementMetadata.isEmpty();

        logger.log(Level.INFO,
                String.format("%s:%s:initializePrimaryEpoch - name=%s placement=%s epoch=%d",
                        this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        groupName, placementMetadata, placementEpoch));

        // all nodes start as BACKUP
        this.currentRole.put(groupName, Role.BACKUP);

        // attempts to parse the metadata
        String preferredCoordinatorNodeId = null;
        try {
            JSONObject json = new JSONObject(placementMetadata);
            preferredCoordinatorNodeId = json.getString(
                    AbstractDemandProfile.Keys.PREFERRED_COORDINATOR.toString());
        } catch (JSONException e) {
            logger.log(Level.WARNING,
                    "{0} failed to parse preferred coordinator in the placement metadata: {1}",
                    new Object[]{this, e});
            return false;
        }

        // attempts to be the coordinator, if this node is the preferred coordinator
        // specified in the placement metadata.
        if (this.myNodeID.toString().equals(preferredCoordinatorNodeId)) {

            // try to be Paxos' coordinator since we try to co-locate
            // the Primary and Paxos' coordinator.
            int maxAttempt = 10;
            int currAttempt = 0;
            int attemptWaitTimeMs = 3000;
            while (!this.paxosManager.getPaxosCoordinator(groupName).equals(this.myNodeID)) {
                if (++currAttempt > maxAttempt) {
                    String errorMsg = String.format("%s:%s:initializePrimaryEpoch - " +
                                    "unable to become paxos' coordinator for " +
                                    "name=%s:%d after %d trials",
                            this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            groupName, placementEpoch, currAttempt);
                    logger.log(Level.WARNING, errorMsg);
                    throw new RuntimeException(errorMsg);
                }

                this.paxosManager.tryToBePaxosCoordinator(groupName);
                try {
                    Thread.sleep(attemptWaitTimeMs);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // FIXME: we might need to wait for other replicas (majority) to active first,
            //  before we can propose something.

            // Get IP Addresses of all nodes for rsync (non-deterministic initialization)
            Map<String, InetAddress> ipAddresses = new HashMap<>();
            NodeConfig<NodeIDType> initNodeConfig = this.messenger.getNodeConfig();
            nodes.forEach(node -> ipAddresses.put(
                    String.valueOf(node).toLowerCase(),
                    initNodeConfig.getNodeAddress(node)
            ));

            logger.log(Level.INFO,
                    String.format("%s:%s:initializePrimaryEpoch - proposing to be PRIMARY for %s:%d",
                            this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            groupName, placementEpoch));

            // Note that PrimaryEpoch is different to PlacementEpoch. It is possible to have
            // different PrimaryEpoch with the same PlacementEpoch. We bump up PrimaryEpoch when
            // we change the Primary within the same replica group (i.e., when the Primary becomes
            // unavailable). We bump up PlacementEpoch when the control-plane move the
            // replica-group to another set of servers.
            // So the identifier is <groupName:placementEpoch:primaryEpoch>.
            PrimaryEpoch<NodeIDType> zero = new PrimaryEpoch<>(myNodeID, 0);
            // Start as PRIMARY_CANDIDATE before becoming PRIMARY
            this.currentRole.put(groupName, Role.PRIMARY_CANDIDATE);
            this.currentPrimaryEpoch.put(groupName, zero);
            StartEpochPacket startPacket = new StartEpochPacket(groupName, zero);
            this.paxosManager.propose(
                    groupName,
                    startPacket,
                    (proposedPacket, isHandled) -> {
                        System.out.printf("\n\n>> %s I'M THE PRIMARY NOW FOR %s!!\n\n",
                                myNodeID, groupName);
                        currentRole.put(groupName, Role.PRIMARY);
                        currentPrimary.put(groupName, this.myNodeID);
                        currentPrimaryEpoch.put(groupName, zero);
                        processOutstandingRequests();

                        PrimaryBackupMiddlewareApp middleware;
                        XdnGigapaxosApp xdnApp;

                        // non-deterministic sync at initialization works only for
                        // specific gigapaxos App
                        if (!(this.paxosMiddlewareApp instanceof PrimaryBackupMiddlewareApp))
                            return;
                        middleware = (PrimaryBackupMiddlewareApp) this.paxosMiddlewareApp;
                        if (!(middleware.getReplicableApp() instanceof XdnGigapaxosApp))
                            return;
                        xdnApp = (XdnGigapaxosApp) middleware.getReplicableApp();

                        // TODO: use `Config.getGlobalString(PaxosConfig.PC.SSH_KEY_PATH)`
                        String sshKey = PaxosConfig.getAsProperties()
                                .getProperty("SSH_KEY_PATH", "");
                        logger.log(Level.INFO, String.format(
                                "%s:%s - Handling non-deterministic service initialization",
                                myNodeID, PrimaryBackupManager.class.getSimpleName()));

                        xdnApp.restore(groupName, "nondeter:start:");

                        if (ENABLE_NON_DETERMINISTIC_INIT) {
                            xdnApp.nonDeterministicInitialization(groupName, ipAddresses, sshKey);

                            while (!this.paxosManager.getPaxosCoordinator(groupName).equals(this.myNodeID)) {
                                logger.log(Level.INFO, String.format(
                                        "%s:%s - PRIMARY re-electing itself due to coordinator issues %s:%d",
                                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                        groupName, placementEpoch));
                                this.paxosManager.tryToBePaxosCoordinator(groupName);
                                try {
                                    Thread.sleep(3000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }

                        // TODO: Move fuselog-apply startup to executeStartEpochPacket
                        // Start fuselog-apply in backup instances
                        Set<NodeIDType> backupNodes = nodes.stream()
                                .filter(node -> !node.equals(myNodeID))
                                .collect(Collectors.toSet());

                        InitBackupPacket initPacket = new InitBackupPacket(groupName);
                        GenericMessagingTask<NodeIDType, InitBackupPacket> m =
                                new GenericMessagingTask<>(backupNodes.toArray(), initPacket);

                        // send packet to all backup replicas
                        try {
                            this.messenger.send(m);
                            logger.log(Level.INFO, String.format(
                                    "%s:%s - fuselog-apply started in backup instances",
                                    myNodeID, PrimaryBackupManager.class.getSimpleName()));
                        } catch (IOException | JSONException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );

        }

        return true;
    }

    private boolean initializePrimaryEpoch(String groupName, Set<NodeIDType> nodes,
                                           int placementEpoch) {
        assert groupName != null && !groupName.isEmpty();
        assert nodes != null && !nodes.isEmpty();

        // all nodes start as BACKUP, initially.
        this.currentRole.put(groupName, Role.BACKUP);

        // detect the current paxos' coordinator
        NodeIDType paxosCoordinatorID = this.paxosManager.getPaxosCoordinator(groupName);
        if (paxosCoordinatorID == null) {
            throw new RuntimeException("Failed to get paxos coordinator for " + groupName);
        }

        logger.log(Level.INFO,
                String.format("%s:%s:initializePrimaryEpoch - name=%s coord=%s epoch=%d",
                        this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        groupName, paxosCoordinatorID, placementEpoch));

        // try to be the primary, co-located with the paxos' coordinator
        if (paxosCoordinatorID.equals(myNodeID)) {
            if (!(this.messenger instanceof JSONMessenger)) {
                throw new RuntimeException(
                        "PrimaryBackupManager.messenger is not an instance of JSONMessenger.");
            }

            Map<String, InetAddress> ipAddresses = new HashMap<>();
            NodeConfig<NodeIDType> initNodeConfig = this.messenger.getNodeConfig();
            nodes.forEach(node -> ipAddresses.put(
                    String.valueOf(node).toLowerCase(),
                    initNodeConfig.getNodeAddress(node)
            ));

            System.out.printf(">> %s Initializing primary epoch for %s\n", myNodeID, groupName);
            logger.log(Level.INFO,
                    String.format("%s:%s:initializePrimaryEpoch - initializing PRIMARY for %s:%d",
                            this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            groupName, placementEpoch));

            // Note that PrimaryEpoch != PlacementEpoch
            PrimaryEpoch<NodeIDType> zero = new PrimaryEpoch<>(myNodeID, 0);
            this.currentRole.put(groupName, Role.PRIMARY_CANDIDATE);
            this.currentPrimaryEpoch.put(groupName, zero);
            StartEpochPacket startPacket = new StartEpochPacket(groupName, zero);
            this.paxosManager.propose(
                    groupName,
                    startPacket,
                    (proposedPacket, isHandled) -> {
                        System.out.printf("\n\n>> %s I'M THE PRIMARY NOW FOR %s!!\n\n",
                                myNodeID, groupName);
                        currentRole.put(groupName, Role.PRIMARY);
                        currentPrimary.put(groupName, paxosCoordinatorID);
                        currentPrimaryEpoch.put(groupName, zero);
                        processOutstandingRequests();

                        PrimaryBackupMiddlewareApp middleware;
                        XdnGigapaxosApp xdnApp;

                        if (!(this.paxosMiddlewareApp instanceof PrimaryBackupMiddlewareApp))
                            return;
                        middleware = (PrimaryBackupMiddlewareApp) this.paxosMiddlewareApp;
                        if (!(middleware.getReplicableApp() instanceof XdnGigapaxosApp))
                            return;
                        xdnApp = (XdnGigapaxosApp) middleware.getReplicableApp();

                        String sshKey = PaxosConfig.getAsProperties().
                                getProperty("SSH_KEY_PATH", "");
                        logger.log(Level.INFO, String.format(
                                "%s:%s - Handling non-deterministic service initialization",
                                myNodeID, PrimaryBackupManager.class.getSimpleName()));

                        xdnApp.restore(groupName, "nondeter:start:");

                        if (ENABLE_NON_DETERMINISTIC_INIT) {
                            xdnApp.nonDeterministicInitialization(groupName, ipAddresses, sshKey);

                            logger.log(Level.INFO, String.format(
                                    "%s:%s - non-deterministic service initialization complete",
                                    myNodeID, PrimaryBackupManager.class.getSimpleName()));

                            while (!this.paxosManager.getPaxosCoordinator(groupName).equals(this.myNodeID)) {
                                logger.log(Level.INFO, String.format(
                                        "%s:%s - PRIMARY re-electing itself due to coordinator issues %s:%d",
                                        myNodeID, PrimaryBackupManager.class.getSimpleName(), groupName, placementEpoch));

                                this.paxosManager.tryToBePaxosCoordinator(groupName);
                                try {
                                    Thread.sleep(3000);
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        }

                        // Start fuselog-apply in backup instances
                        Set<NodeIDType> backupNodes = nodes.stream()
                                .filter(node -> !node.equals(myNodeID))
                                .collect(Collectors.toSet());

                        InitBackupPacket initPacket = new InitBackupPacket(groupName);
                        GenericMessagingTask<NodeIDType, InitBackupPacket> m = new GenericMessagingTask<>(backupNodes.toArray(), initPacket);

                        // send packet to all backup replicas
                        try {
                            this.messenger.send(m);
                            logger.log(Level.INFO, String.format("%s:%s - fuselog-apply started in backup instances",
                                    myNodeID, PrimaryBackupManager.class.getSimpleName()));
                        } catch (IOException | JSONException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        } else {
            PrimaryEpoch<NodeIDType> zero = new PrimaryEpoch<>(paxosCoordinatorID, 0);
            currentRole.put(groupName, Role.BACKUP);
            currentPrimary.put(groupName, paxosCoordinatorID);
            currentPrimaryEpoch.put(groupName, zero);
        }

        // FIXME: as a temporary measure, we wait until StartEpochPacket is being agreed upon.
        //  This is needed for now as the current implementation has not handle the case when
        //  a node does not know the current Primary.
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    private void processOutstandingRequests() {
        assert this.outstandingRequests != null;
        while (!this.outstandingRequests.isEmpty()) {
            RequestAndCallback rc = this.outstandingRequests.poll();
            executeRequestCoordinateStateDiff(rc.requestPacket(), rc.callback());
        }
    }

    // TODO: also handle deletion of PBInstance with placement epoch
    public boolean deletePrimaryBackupInstance(String groupName, int placementEpoch) {
        System.out.printf(">> %s:PbManager deletePrimaryBackupInstance name=%s epoch=%d\n",
                this.myNodeID, groupName, placementEpoch);
        boolean isPaxosStopped = this.paxosManager.
                deleteStoppedPaxosInstance(groupName, placementEpoch);
        if (!isPaxosStopped) {
            return false;
        }

        // TODO: handle placement epoch

        return true;
    }

    public Set<NodeIDType> getReplicaGroup(String groupName) {
        // System.out.printf(">> %s:PBManager getReplicaGroup - %s\n",
        //        myNodeID, groupName);
        return this.paxosManager.getReplicaGroup(groupName);
    }

    public final void stop() {
        this.paxosManager.close();
    }

    public boolean isCurrentPrimary(String groupName) {
        Role myCurrentRole = this.currentRole.get(groupName);
        if (myCurrentRole == null) {
            return false;
        }
        return myCurrentRole.equals(Role.PRIMARY);
    }

    // same as isCurrentPrimary, but proactively try to make this node to be
    // paxos coordinator as well.
    public boolean isCurrentPrimary2(String groupName) {
        Role myCurrentRole = this.currentRole.get(groupName);
        if (myCurrentRole == null) {
            return false;
        }
        boolean isPrimary = myCurrentRole.equals(Role.PRIMARY);
        if (isPrimary) {
            this.paxosManager.tryToBePaxosCoordinator(groupName);
        }
        return isPrimary;
    }

    private void restartPaxosInstance(String groupName) {
        this.paxosManager.restartFromLastCheckpoint(groupName);
    }

    private boolean handleStopRequest(ReconfigurableRequest stopRequest) {
        assert stopRequest.isStop() : "incorrect request type";
        return this.replicableApp.execute(stopRequest, true);
    }

    protected boolean handleReconfigurationPacket(ReconfigurableRequest reconfigurationPacket,
                                                  ExecutedCallback callback) {
        System.out.printf("%s:PbManager handling reconfiguration packet of %s with callback=%s\n",
                this.myNodeID, reconfigurationPacket.getClass().getSimpleName(),
                callback.getClass().getSimpleName());

        if (reconfigurationPacket.isStop()) {
            String serviceName = reconfigurationPacket.getServiceName();
            int reconfigurationEpoch = reconfigurationPacket.getEpochNumber();

            System.out.printf("%s:PbManager stopping service name=%s epoch=%d\n",
                    this.myNodeID, serviceName, reconfigurationEpoch);

            boolean isExecStopSuccess = this.handleStopRequest(reconfigurationPacket);
            assert isExecStopSuccess : "must be successful on executing stop request";
            callback.executed(reconfigurationPacket, true);

            return true;
        }

        System.out.println("WARNING: Unhandled reconfigurationPacket of " +
                reconfigurationPacket.getClass().getSimpleName() + ": " + reconfigurationPacket);
        return false;
    }


    //--------------------------------------------------------------------------------------------||
    //                  Begin implementation for AppRequestParser interface                       ||
    // Despite the interface name, the requests parsed here are intended for the replica          ||
    // coordinator packets, i.e., PrimaryBackupPacket, and not for AppRequest.                    ||
    //--------------------------------------------------------------------------------------------||

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        assert stringified != null;
        if (!stringified.startsWith(PrimaryBackupPacket.SERIALIZED_PREFIX)) {
            throw new RuntimeException(String.format("PBManager-%s: request for primary backup " +
                    "coordinator has invalid prefix %s", myNodeID, stringified));
        }

        if (stringified.startsWith(ForwardedRequestPacket.SERIALIZED_PREFIX)) {
            return ForwardedRequestPacket.createFromString(stringified);
        }

        if (stringified.startsWith(ResponsePacket.SERIALIZED_PREFIX)) {
            return ResponsePacket.createFromString(stringified);
        }

        throw new RuntimeException(String.format("PBManager-%s: Unknown encoded request %s\n",
                myNodeID, stringified));
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return getAllPrimaryBackupPacketTypes();
    }

    //--------------------------------------------------------------------------------------------||
    //                     End implementation for AppRequestParser interface                      ||
    //--------------------------------------------------------------------------------------------||

    //--------------------------------------------------------------------------------------------||
    // Begin implementation for PrimaryBackupMiddlewareApp.                                               ||
    // A middleware application that handle execute(.) before the PrimaryBackupManager can do     ||
    // execution in the BackupableApplication.                                                    ||
    //--------------------------------------------------------------------------------------------||


    /**
     * PrimaryBackupMiddlewareApp is the application of Paxos used in the PrimaryBackupManager.
     * As an application of Paxos, generally PaxosMiddlewareApp apply the stateDiffs being
     * agreed upon from Paxos. Thus, the execute() method simply apply the stateDiffs.
     * Additionally, PaxosMiddlewareApp needs to ignore stateDiff from 'stale' primary
     * to ensure primary integrity. i.e., making the execution as no-op.
     */
    public static class PrimaryBackupMiddlewareApp implements Replicable {

        private final Replicable app;
        private final Set<IntegerPacketType> requestTypes;
        private PrimaryBackupManager<?> primaryBackupManager;

        public static PrimaryBackupMiddlewareApp wrapApp(Replicable app) {
            assert app instanceof BackupableApplication :
                    "The application for Primary Backup must be a BackupableApplication";
            return new PrimaryBackupMiddlewareApp(app);
        }

        protected void setManager(PrimaryBackupManager<?> primaryBackupManager) {
            this.primaryBackupManager = primaryBackupManager;
        }

        protected Replicable getReplicableApp() {
            return app;
        }

        protected BackupableApplication getBackupableApp() {
            return (BackupableApplication) app;
        }

        private PrimaryBackupMiddlewareApp(Replicable app) {
            this.app = app;

            // only two packet/request type required by this PaxosMiddlewareApp
            Set<IntegerPacketType> types = new HashSet<>();
            types.add(PrimaryBackupPacketType.PB_START_EPOCH_PACKET);
            types.add(PrimaryBackupPacketType.PB_STATE_DIFF_PACKET);
            this.requestTypes = types;
        }

        @Override
        public Request getRequest(String stringified) throws RequestParseException {
            if (stringified == null || stringified.isEmpty()) return null;
            PrimaryBackupPacketType packetType =
                    PrimaryBackupPacket.getQuickPacketTypeFromEncodedPacket(stringified);

            if (packetType != null) {
                return PrimaryBackupPacket.createFromString(stringified);
            }

            return this.app.getRequest(stringified);
        }

        @Override
        public Set<IntegerPacketType> getRequestTypes() {
            return this.requestTypes;
        }

        @Override
        public boolean execute(Request request) {
            return this.execute(request, true);
        }

        @Override
        public boolean execute(Request request, boolean doNotReplyToClient) {
            if (request == null) return true;
            assert this.primaryBackupManager != null :
                    "Ensure to set the manager for this middleware app";

            if (request instanceof StartEpochPacket startEpochPacket) {
                return this.primaryBackupManager.executeStartEpochPacket(startEpochPacket);
            }

            if (request instanceof ApplyStateDiffPacket stateDiffPacket) {
                return this.primaryBackupManager.executeApplyStateDiffPacket(stateDiffPacket);
            }

            if (this.app.getRequestTypes().contains(request.getRequestType())) {
                return this.app.execute(request);
            }

            if (request instanceof ReconfigurableRequest rcRequest && rcRequest.isStop()) {
                return this.app.restore(request.getServiceName(), null);
            }

            throw new RuntimeException(
                    String.format("PrimaryBackupMiddlewareApp: Unknown execute handler" +
                            " for request %s: %s", request.getClass().getSimpleName(), request));
        }

        @Override
        public String checkpoint(String name) {
            assert this.primaryBackupManager != null :
                    "Ensure to set the manager for this middleware app";
            return this.primaryBackupManager.executeGetCheckpoint(name);
        }

        @Override
        public boolean restore(String name, String state) {
            // FIXME: all names will go through primary backup, we need a mapper
            //  that somehow bypass primary backup for names that use other coordinator.
            //  For now, it is fine as executeRestore is only storing data in a map.
            if (this.primaryBackupManager != null) {
                return this.primaryBackupManager.executeRestore(name, state);
            }
            Logger.getGlobal().log(Level.WARNING,
                    "PrimaryBackupManager was not set before restore");
            return this.app.restore(name, state);
        }

    }

}
