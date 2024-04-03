package edu.umass.cs.reconfiguration;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.paxosutil.Ballot;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.primarybackup.PBEpoch;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.primarybackup.packets.PrimaryBackupPacketType;
import edu.umass.cs.primarybackup.packets.StartEpochPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.xdn.request.*;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.PaxosInstanceCreationException;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * PrimaryBackupReplicaCoordinator handles replica groups that use primary-backup, which
 * only allow request execution in the coordinator, leaving other replicas as backups.
 * Generally, primary-backup is suitable to provide fault-tolerance for non-deterministic
 * services.
 * <p>
 * PrimaryBackupReplicaCoordinator uses PaxosManager to ensure all replicas agree on
 * the order of statediffs, generated from each request execution.
 * <p>
 * Note that this coordinator is currently only tested when HttpActiveReplica is used.
 * More adjustments are needed to support plain ActiveReplica.
 *
 * @param <NodeIDType>
 */
public class PrimaryBackupReplicaCoordinator<NodeIDType>
        extends AbstractReplicaCoordinator<NodeIDType> {

    enum Role {
        PRIMARY_CANDIDATE,
        PRIMARY,
        BACKUP
    }

    private final boolean ENABLE_INTERNAL_REDIRECT_PRIMARY = true;
    private final PaxosManager<NodeIDType> paxosManager;
    private final Set<IntegerPacketType> appRequestTypes;
    private final Set<IntegerPacketType> coordinatorRequestTypes;
    private final NodeIDType myNodeID;
    private final BackupableApplication backupableApplication;

    // per service metadata
    private ConcurrentHashMap<String, Role> currentRole;
    private ConcurrentHashMap<String, PBEpoch> currentEpoch;

    // outstanding request forwarded to primary
    ConcurrentHashMap<Long, XDNRequestAndCallback> outstanding = new ConcurrentHashMap<>();

    public PrimaryBackupReplicaCoordinator(
            Replicable app,
            NodeIDType myID,
            Stringifiable<NodeIDType> unstringer,
            Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);

        if (!(app instanceof BackupableApplication)) {
            String exceptionMessage = String.format("Cannot use %s for non %s",
                    this.getClass().getSimpleName(),
                    BackupableApplication.class.getSimpleName());
            throw new RuntimeException(exceptionMessage);
        }

        // store the backupable application so this coordinator can
        // later capture the statediff after request execution.
        this.backupableApplication = (BackupableApplication) app;

        // initialize the Paxos Manager for this Node
        this.paxosManager = new PaxosManager<NodeIDType>(myID, unstringer, messenger, this)
                .initClientMessenger(
                        new InetSocketAddress(
                                messenger.getNodeConfig().getNodeAddress(myID),
                                messenger.getNodeConfig().getNodePort(myID)),
                        messenger);

        // initialize all the app packet/request types handled
        Set<IntegerPacketType> appPackets = new HashSet<>(app.getRequestTypes());
        appPackets.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        this.appRequestTypes = appPackets;

        // initialize all the coordinator packet types handled
        Set<IntegerPacketType> pbPackets = new HashSet<>();
        pbPackets.add(PrimaryBackupPacketType.PB_START_EPOCH_PACKET);
        this.coordinatorRequestTypes = pbPackets;

        // store my node id
        this.myNodeID = myID;

        this.currentRole = new ConcurrentHashMap<>();
        this.currentEpoch = new ConcurrentHashMap<>();
    }

    private void inferCurrentRole() {
        // TODO: implement me
        // this.paxosManager.getPaxosCoordinator();
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        System.out.println(">> PrimaryBackupReplicaCoordinator - getRequestTypes");
        Set<IntegerPacketType> allTypes = new HashSet<>();
        allTypes.addAll(this.appRequestTypes);
        allTypes.addAll(this.coordinatorRequestTypes);
        return allTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        System.out.println(">> coordinateRequest " + request.getClass().getName() + " " + request.getServiceName());
        System.out.println(">> myID:" + getMyID() + " isCoordinator? " +
                this.paxosManager.isPaxosCoordinator(request.getServiceName()) +
                " callback: " + callback);

        if (request instanceof ReplicableClientRequest rcr) {
            request = rcr.getRequest();
        }
        assert request instanceof XDNRequest;
        IntegerPacketType requestType = request.getRequestType();

        switch (requestType) {
            case XDNRequestType.XDN_SERVICE_HTTP_REQUEST:
            case XDNRequestType.XDN_HTTP_FORWARD_REQUEST:
                return handleServiceRequest((XDNRequest) request, callback);

            case XDNRequestType.XDN_HTTP_FORWARD_RESPONSE:
                return handleForwardedResponse((XDNHttpForwardResponse) request);

            case XDNRequestType.XDN_STATEDIFF_APPLY_REQUEST:
                throw new IllegalStateException("Statediff apply request can only be handled in" +
                        " App.execute(.)");

            default:
                throw new IllegalStateException("Unexpected request type: " + requestType);
        }
    }

    private boolean handleServiceRequest(XDNRequest request, ExecutedCallback callback) {
        String serviceName = request.getServiceName();
        NodeIDType currPrimaryID = this.paxosManager.getPaxosCoordinator(serviceName);

        // TODO: detect state changes
//        if (!currPrimaryID.equals(myNodeID) && this.currentRole.get(serviceName) == Role.BACKUP) {
////            this.paxosManager.propose(serviceName,
////                    new StartEpochPacket())
//        }

        if (currPrimaryID == null) {
            return sendErrorResponse(request, callback, "unknown coordinator");
        }

        // if this node is not a primary, forward the request to the primary, either
        // (1) using internal redirection, or
        // (2) asking the client to contact the primary.
        if (!currPrimaryID.equals(myNodeID)) {

            // case-1: use internal redirection
            if (ENABLE_INTERNAL_REDIRECT_PRIMARY) {
                return forwardRequestToPrimary(currPrimaryID, request, callback);
            }

            // case-2: ask client to contact primary
            return askClientToContactPrimary(currPrimaryID, request, callback);
        }

        // else, if this node is a primary then execute the request and
        // capture the statediff.
        return executeRequestCoordinateStatediff(request, callback);
    }

    private boolean handleForwardedResponse(XDNHttpForwardResponse response) {
        XDNRequestAndCallback rc = outstanding.get(response.getRequestID());
        if (rc == null) {
            System.out.println(">> unknown client :(");
            return false;
        }

        XDNHttpRequest request = (XDNHttpRequest) rc.request();
        request.setHttpResponse(response.getHttpResponse());
        rc.callback().executed(request, true);
        return true;
    }

    private boolean askClientToContactPrimary(NodeIDType primaryNodeID, Request request,
                                              ExecutedCallback callback) {
        XDNHttpRequest xdnRequest = XDNHttpRequest.createFromString(request.toString());
        xdnRequest.setHttpResponse(createRedirectResponse(
                primaryNodeID, request.getServiceName(), xdnRequest.getHttpRequest()));
        callback.executed(xdnRequest, false);
        return true;
    }

    private HttpResponse createRedirectResponse(NodeIDType primaryNodeID, String serviceName,
                                                HttpRequest httpRequest) {
        // prepare the response headers
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);

        // set the redirected location
        String primaryHost = String.format("http://%s.%s.xdn.io:%d",
                serviceName,
                primaryNodeID,
                ReconfigurationConfig.getHTTPPort(
                        PaxosConfig.getActives().get(primaryNodeID.toString()).getPort()));
        String redirectURL = primaryHost + httpRequest.uri();
        headers.set(HttpHeaderNames.LOCATION, redirectURL);

        // by default, we have an empty header trailing for the response
        HttpHeaders trailingHeaders = new DefaultHttpHeaders();

        String respMessage = String.format("Please contact the primary replica in %s",
                primaryHost);

        return new DefaultFullHttpResponse(
                httpRequest.protocolVersion(),
                HttpResponseStatus.TEMPORARY_REDIRECT,
                Unpooled.copiedBuffer(respMessage.getBytes(StandardCharsets.UTF_8)),
                headers,
                trailingHeaders);
    }

    private boolean sendErrorResponse(Request request, ExecutedCallback callback,
                                      String errorMessage) {
        XDNHttpRequest xdnRequest = XDNHttpRequest.createFromString(request.toString());
        xdnRequest.setHttpResponse(createErrorResponse(xdnRequest.getHttpRequest(), errorMessage));
        callback.executed(xdnRequest, false);
        return true;
    }

    private HttpResponse createErrorResponse(HttpRequest httpRequest, String errorMessage) {
        String respMessage = String.format("XDN internal server error. Reason: %s", errorMessage);
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        HttpHeaders trailingHeaders = new DefaultHttpHeaders();
        return new DefaultFullHttpResponse(
                httpRequest.protocolVersion(),
                HttpResponseStatus.INTERNAL_SERVER_ERROR,
                Unpooled.copiedBuffer(respMessage.getBytes(StandardCharsets.UTF_8)),
                headers,
                trailingHeaders);
    }

    // currently this request execution method support two different type of Request:
    // (1) XDNHttpRequest, and
    // (2) XDNHttpForwardRequest, which also contains XDNHttpRequest, being forwarded
    //     by entry replica to this coordinator node.
    private boolean executeRequestCoordinateStatediff(Request request, ExecutedCallback callback) {
        System.out.println(">> " + this.myNodeID + " primaryBackup execution:   " + request.getClass().getSimpleName());

        assert request instanceof XDNHttpRequest ||
                request instanceof XDNHttpForwardRequest;

        boolean isEntryNode = false;
        XDNHttpRequest primaryRequest = null;
        if (request instanceof XDNHttpRequest) {
            primaryRequest = (XDNHttpRequest) request;
            isEntryNode = true;
        }
        if (request instanceof XDNHttpForwardRequest forwardRequest) {
            primaryRequest = forwardRequest.getRequest();
        }
        assert primaryRequest != null;

        // TODO: ensure atomicity of batch execution
        this.app.execute(primaryRequest);

        String statediff = this.backupableApplication.captureStatediff(request.getServiceName());
        XDNStatediffApplyRequest statediffApplyRequest =
                new XDNStatediffApplyRequest(
                        request.getServiceName(),
                        statediff);

        System.out.println(">> " + this.myNodeID + " propose ...");
        System.out.println(" request type " + primaryRequest.getClass().getSimpleName());

        Request finalPrimaryRequest = primaryRequest;
        if (isEntryNode) {
            this.paxosManager.propose(
                    request.getServiceName(),
                    statediffApplyRequest,
                    (statediffRequest, handled) -> {
                        callback.executed(finalPrimaryRequest, handled);
                    });
        } else {
            GenericMessagingTask<NodeIDType, ?> responseMessagingTask =
                    getResponseMessagingTask(request, primaryRequest);
            Messenger<NodeIDType, ?> myMessenger = this.messenger;
            this.paxosManager.propose(
                    request.getServiceName(),
                    statediffApplyRequest,
                    (statediffRequest, handled) -> {
                        try {
                            System.out.println(">> " + myNodeID + " sending response back to entry node ...");
                            myMessenger.send(responseMessagingTask);
                        } catch (IOException | JSONException e) {
                            throw new RuntimeException(e);
                        }
                    }
            );
        }
        return true;
    }

    private GenericMessagingTask<NodeIDType, ?> getResponseMessagingTask(
            Request request, XDNHttpRequest primaryRequest) {
        XDNHttpForwardRequest forwardRequest = (XDNHttpForwardRequest) request;
        XDNHttpForwardResponse response = new XDNHttpForwardResponse(
                primaryRequest.getRequestID(),
                request.getServiceName(),
                primaryRequest.getHttpResponse()
        );
        NodeIDType entryNodeID = (NodeIDType) forwardRequest.getEntryNodeID();
        System.out.println(">> " + myNodeID + " sending response back to " + entryNodeID);
        GenericMessagingTask<NodeIDType, ?> responseMessage = new GenericMessagingTask<>(
                entryNodeID, response);
        return responseMessage;
    }

    private boolean forwardRequestToPrimary(NodeIDType primaryNodeID, Request request,
                                            ExecutedCallback callback) {

        // validate that the request is http request
        assert (request instanceof XDNHttpRequest) : "requestType: " + request.getClass().getSimpleName();
        XDNHttpRequest httpRequest = (XDNHttpRequest) request;

        // put request and callback into outstanding map
        XDNRequestAndCallback rc = new XDNRequestAndCallback(httpRequest, callback);
        outstanding.put(httpRequest.getRequestID(), rc);

        // prepare forwarded request
        XDNHttpForwardRequest forwardRequest = new XDNHttpForwardRequest(
                httpRequest, myNodeID.toString());

        // forward request to the current primary
        System.out.println("current coordinator is " + primaryNodeID);
        GenericMessagingTask<NodeIDType, XDNRequest> m = new GenericMessagingTask<>(
                primaryNodeID, forwardRequest);
        try {
            this.messenger.send(m);
        } catch (IOException | JSONException e) {
            throw new RuntimeException(e);
        }

        return true;
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes) {
        System.out.println(">> PrimaryBackupCoor - createReplicaGroup | serviceName:" + serviceName
                + " epoch:" + epoch
                + " state:" + state
                + " nodes:" + nodes);

        boolean created = this.paxosManager.createPaxosInstanceForcibly(serviceName,
                epoch, nodes, this, state, 0);
        boolean alreadyExist = this.paxosManager.equalOrHigherVersionExists(serviceName, epoch);

        if (!created && !alreadyExist) {
            throw new PaxosInstanceCreationException((this
                    + " failed to create " + serviceName + ":" + epoch
                    + " with state [" + state + "]") + "; existing_version=" +
                    this.paxosManager.getVersion(serviceName));
        }

        // TODO: confirming this:
        //  by default Gigapaxos creates these 3 service replica groups, which we will
        //  ignore in our primary-backup replica coordinator.
        if (serviceName.equals(PaxosConfig.getDefaultServiceName()) ||
                serviceName.equals(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString()) ||
                serviceName.equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString())) {
            return true;
        }

        this.currentRole.put(serviceName, Role.BACKUP);
        NodeIDType paxosCoorNodeID = this.paxosManager.getPaxosCoordinator(serviceName);
        System.out.println(">> " + myNodeID + " createReplicaGroup paxos-coordinator: " + paxosCoorNodeID);
//        if (paxosCoorNodeID.equals(myNodeID)) {
//            System.out.println(">> " + myNodeID + " I am a paxos coordinator of " + serviceName);
//            this.currentRole.put(serviceName, Role.PRIMARY_CANDIDATE);
//            PBEpoch initEpoch = new PBEpoch(myNodeID.toString(), 0);
//            StartEpochPacket startEpochPacket = new StartEpochPacket(
//                    serviceName,
//                    initEpoch);
//            currentEpoch.put(serviceName, initEpoch);
//            this.paxosManager.propose(
//                    serviceName,
//                    startEpochPacket,
//                    (proposedRequest, isHandled) -> {
//                        System.out.println(">>>>>>>>> " + myNodeID + " IAM THE PRIMARY NOW!!!");
//                        currentRole.put(serviceName, Role.PRIMARY);
//                    }
//            );
//        }

        return true;
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        System.out.println(">> deleteReplicaGroup - " + serviceName);
        return this.paxosManager.deleteStoppedPaxosInstance(serviceName, epoch);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        System.out.println(">> getReplicaGroup - " + serviceName);
        return this.paxosManager.getReplicaGroup(serviceName);
    }


}
