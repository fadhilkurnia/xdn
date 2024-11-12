package edu.umass.cs.xdn;

import edu.umass.cs.clientcentric.BayouReplicaCoordinator;
import edu.umass.cs.causal.CausalReplicaCoordinator;
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
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.PaxosReplicaCoordinator;
import edu.umass.cs.primarybackup.PrimaryBackupReplicaCoordinator;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.sequential.AwReplicaCoordinator;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnRequestType;
import edu.umass.cs.xdn.service.ConsistencyModel;
import edu.umass.cs.xdn.service.ServiceProperty;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * XdnReplicaCoordinator is a wrapper of multiple replica coordinators supported by XDN.
 *
 * @param <NodeIDType>
 */
public class XdnReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private final String myNodeID;

    // list of all coordination managers supported in XDN
    private final AbstractReplicaCoordinator<NodeIDType> primaryBackupCoordinator;
    private final AbstractReplicaCoordinator<NodeIDType> paxosCoordinator;
    private final AbstractReplicaCoordinator<NodeIDType> chainReplicationCoordinator;
    private final AbstractReplicaCoordinator<NodeIDType> pramReplicaCoordinator;
    private final AbstractReplicaCoordinator<NodeIDType> awReplicaCoordinator;
    private final AbstractReplicaCoordinator<NodeIDType> clientCentricReplicaCoordinator;
    private final AbstractReplicaCoordinator<NodeIDType> causalReplicaCoordinator;

    // mapping between service name to the service's coordination manager
    private final Map<String, AbstractReplicaCoordinator<NodeIDType>> serviceCoordinator;

    private final Set<IntegerPacketType> requestTypes;

    private final Logger logger = Logger.getGlobal();

    public XdnReplicaCoordinator(Replicable app,
                                 NodeIDType myID,
                                 Stringifiable<NodeIDType> unstringer,
                                 Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);

        System.out.printf(">> XDNReplicaCoordinator - init at node %s\n", myID);

        assert app.getClass().getSimpleName().equals(XdnGigapaxosApp.class.getSimpleName()) :
                "XdnReplicaCoordinator must be used with XdnGigapaxosApp";
        assert myID.getClass().getSimpleName().equals(String.class.getSimpleName()) :
                "XdnReplicaCoordinator must use String as the NodeIDType";

        this.myNodeID = myID.toString();

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
        // FIXME: how does the modified paxos manager impact sequential here?
        AwReplicaCoordinator<NodeIDType> awReplicaCoordinator =
                new AwReplicaCoordinator<>(app, myID, unstringer, messenger, paxosManager);
        PramReplicaCoordinator<NodeIDType> pramReplicaCoordinator =
                new PramReplicaCoordinator<>(app, myID, unstringer, messenger);
        BayouReplicaCoordinator<NodeIDType> bayouReplicaCoordinator =
                new BayouReplicaCoordinator<>(app, myID, unstringer, messenger);
        CausalReplicaCoordinator<NodeIDType> causalReplicaCoordinator =
                new CausalReplicaCoordinator<>(app, myID, unstringer, messenger);

        this.primaryBackupCoordinator = primaryBackupReplicaCoordinator;
        this.paxosCoordinator = paxosReplicaCoordinator;
        this.chainReplicationCoordinator = null;
        this.pramReplicaCoordinator = pramReplicaCoordinator;
        this.awReplicaCoordinator = awReplicaCoordinator;
        this.clientCentricReplicaCoordinator = bayouReplicaCoordinator;
        this.causalReplicaCoordinator = causalReplicaCoordinator;

        // initialize empty service -> coordinator mapping
        this.serviceCoordinator = new ConcurrentHashMap<>();

        // registering all request types handled by XDN,
        // including all request types of each coordination managers.
        Set<IntegerPacketType> types = new HashSet<>();
        types.add(XdnRequestType.XDN_SERVICE_HTTP_REQUEST);
        types.addAll(PrimaryBackupManager.getAllPrimaryBackupPacketTypes());
        types.addAll(PramReplicaCoordinator.getAllPramRequestTypes());
        this.requestTypes = types;

        // get request
        this.setGetRequestImpl(new AppRequestParser() {
            @Override
            public Request getRequest(String stringified) throws RequestParseException {
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
        var serviceName = request.getServiceName();
        var coordinator = this.serviceCoordinator.get(serviceName);
        if (coordinator == null) {
            // returns 404 not found back to client
            return createNotFoundResponse(request, callback);
        }

        ReplicableClientRequest gpRequest = ReplicableClientRequest.wrap(request);
        gpRequest.setClientAddress(messenger.getListeningSocketAddress());
        return coordinator.coordinateRequest(gpRequest, callback);
    }

    private boolean createNotFoundResponse(Request request, ExecutedCallback callback) {
        // handle only if the request is a Http request coming from client.
        String serviceName = request.getServiceName();
        XdnHttpRequest httpRequest = null;
        if (request instanceof ReplicableClientRequest rcr &&
                rcr.getRequest() instanceof XdnHttpRequest xdnHttpRequest) {
            httpRequest = xdnHttpRequest;
        }
        if (request instanceof XdnHttpRequest xdnHttpRequest) {
            httpRequest = xdnHttpRequest;
        }
        if (httpRequest == null) {
            throw new RuntimeException("Unknown coordinator for name=" + serviceName +
                    " with request type of " + request.getClass().getSimpleName());
        }

        // generate 404 response
        String errorMessage =
                String.format("Service '%s' does not exist in this XDN deployment", serviceName);
        HttpHeaders headers = new DefaultHttpHeaders();
        headers.add(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
        HttpResponse notFoundResponse = new DefaultFullHttpResponse(
                HttpVersion.HTTP_1_1,
                HttpResponseStatus.NOT_FOUND,
                Unpooled.copiedBuffer(errorMessage.getBytes()),
                headers,
                new DefaultHttpHeaders());
        httpRequest.setHttpResponse(notFoundResponse);
        callback.executed(httpRequest, true);
        return true;
    }

    @Override
    public boolean createReplicaGroup(String serviceName,
                                      int epoch,
                                      String state,
                                      Set<NodeIDType> nodes) {
        System.out.printf(">> %s:XdnReplicaCoordinator - createReplicaGroup name=%s, epoch=%d, state=%s, nodes=%s\n",
                myNodeID, serviceName, epoch, state, nodes);

        // These are the default replica groups from Gigapaxos
        if (serviceName.equals(PaxosConfig.getDefaultServiceName()) ||
                serviceName.equals(AbstractReconfiguratorDB.RecordNames.AR_AR_NODES.toString()) ||
                serviceName.equals(AbstractReconfiguratorDB.RecordNames.AR_RC_NODES.toString())) {
            // FIXME: we need to consider how to handle these default service and meta-name.
            // boolean isSuccess = this.paxosCoordinator.createReplicaGroup(serviceName, epoch, state, nodes);
            // assert isSuccess : "failed to create default services";
            // this.serviceCoordinator.put(serviceName, this.paxosCoordinator);
            return true;
        }

        return this.initializeReplicaGroup(serviceName, state, nodes, epoch);
    }

    private boolean initializeReplicaGroup(String serviceName,
                                           String initialState,
                                           Set<NodeIDType> nodes,
                                           int placementEpoch) {
        System.out.printf(">> %s:XdnReplicaCoordinator - initializeReplicaGroup " +
                        "name=%s, state=%s, nodes=%s\n",
                myNodeID, serviceName, initialState, nodes);

        // Validate the serviceName, initialState, and nodes
        assert serviceName != null && !serviceName.isEmpty()
                : "Cannot initialize an XDN service with null or empty service name";
        assert nodes != null && !nodes.isEmpty()
                : "Cannot initialize an XDN service with unknown target nodes";
        assert initialState != null && !initialState.isEmpty()
                : "Cannot initialize an XDN service with null or empty initial state";
        final String validInitialStatePrefix = "xdn:init:";
        final String validEpochFinalStatePrefix = "xdn:final:";
        assert initialState.startsWith(validInitialStatePrefix) ||
                initialState.startsWith(validEpochFinalStatePrefix)
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
        ServiceProperty serviceProperties = null;
        try {
            serviceProperties = ServiceProperty.createFromJsonString(encodedProperties);
        } catch (JSONException e) {
            logger.log(Level.SEVERE, "Invalid service properties given: " + encodedProperties);
            throw new RuntimeException(e);
        }

        // Infer the replica coordinator based on the declared properties
        AbstractReplicaCoordinator<NodeIDType> coordinator =
                inferCoordinatorByProperties(serviceProperties);
        assert coordinator != null :
                "XDN does not know what coordinator to be used for the specified service";

        // Create the replica group using the coordinator
        boolean isSuccess;
        if (isClientCentricConsistency(serviceProperties.getConsistencyModel())) {
            // A special case for client-centric replica coordinator, Bayou, that require
            // us to specify the client-centric consistency model because Bayou support
            // four different client-centric consistency models.
            assert coordinator instanceof BayouReplicaCoordinator<NodeIDType>;
            isSuccess = ((BayouReplicaCoordinator<NodeIDType>) coordinator)
                    .createReplicaGroup(
                            getBayouConsistencyModel(
                                    serviceProperties.getConsistencyModel()),
                            serviceName,
                            placementEpoch,
                            initialState,
                            nodes);
        } else {
            // for all other coordinators, we use the generic createReplicaGroup method.
            isSuccess = coordinator.createReplicaGroup(
                    serviceName,
                    placementEpoch,
                    initialState,
                    nodes);
        }
        assert isSuccess : "failed to initialize service";

        // Store the service->coordinator mapping
        this.serviceCoordinator.put(serviceName, coordinator);

        System.out.printf(">> XDNReplicaCoordinator:%s name=%s coordinator=%s\n",
                myNodeID, serviceName, coordinator.getClass().getSimpleName());
        return true;
    }

    private boolean isClientCentricConsistency(ConsistencyModel consistencyModel) {
        return consistencyModel.equals(ConsistencyModel.MONOTONIC_READS) ||
                consistencyModel.equals(ConsistencyModel.MONOTONIC_WRITES) ||
                consistencyModel.equals(ConsistencyModel.READ_YOUR_WRITES) ||
                consistencyModel.equals(ConsistencyModel.WRITES_FOLLOW_READS);
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
        // for non-deterministic service we always use primary-backup
        if (!serviceProperties.isDeterministic()) {
            return this.primaryBackupCoordinator;
        }

        // for deterministic service, we have more options based on the consistency model
        // but for now we just use paxos for all consistency model
        // TODO: introduce new coordinator for different consistency models.
        else {
            switch (serviceProperties.getConsistencyModel()) {
                case PRAM -> {
                    return this.pramReplicaCoordinator;
                }
                case SEQUENTIAL -> {
                    return this.awReplicaCoordinator;
                }
                case READ_YOUR_WRITES,
                        WRITES_FOLLOW_READS,
                        MONOTONIC_READS,
                        MONOTONIC_WRITES -> {
                    return this.clientCentricReplicaCoordinator;
                }
                case CAUSAL -> {
                    return this.causalReplicaCoordinator;
                }
                case LINEARIZABILITY,
                        EVENTUAL -> {
                    return this.paxosCoordinator;
                }
                default -> {
                    return null;
                }
            }
        }
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        AbstractReplicaCoordinator<NodeIDType> coordinator =
                this.serviceCoordinator.get(serviceName);
        if (coordinator == null) {
            return true;
        }
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
