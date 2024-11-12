package edu.umass.cs.primarybackup;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.*;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.primarybackup.interfaces.BackupableApplication;
import edu.umass.cs.primarybackup.packets.PrimaryBackupPacket;
import edu.umass.cs.primarybackup.packets.RequestPacket;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import org.json.JSONObject;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Set;

/**
 * PrimaryBackupReplicaCoordinator handles replica groups that use primary-backup, which
 * only allow request execution in the primary, leaving other replicas as backups.
 * Generally, primary-backup is suitable to provide fault-tolerance for non-deterministic
 * services.
 * <p>
 * PrimaryBackupReplicaCoordinator uses PaxosManager to ensure all replicas agree on the
 * order of stateDiffs, generated from each request execution.
 * <p>
 *
 * @param <NodeIDType>
 */
public class PrimaryBackupReplicaCoordinator<NodeIDType>
        extends AbstractReplicaCoordinator<NodeIDType> {

    private final PrimaryBackupManager<NodeIDType> pbManager;
    private final Set<IntegerPacketType> requestTypes;

    // FIXME: app and paxosManager need to be treated at the beginning
    public PrimaryBackupReplicaCoordinator(Replicable app,
                                           NodeIDType myID,
                                           Stringifiable<NodeIDType> unstringer,
                                           Messenger<NodeIDType, JSONObject> messenger,
                                           PaxosManager<NodeIDType> paxosManager) {
        super(app, messenger);

        // Initialize the Paxos Manager
        if (paxosManager == null) {
            // the Replicable application used for PrimaryBackupCoordinator must also implement
            // BackupableApplication interface.
            assert app instanceof BackupableApplication;
            this.pbManager = new PrimaryBackupManager<>(myID, unstringer, app, messenger);

        } else {
            // Other case with a given Paxos Manager
            assert app instanceof PrimaryBackupManager.PrimaryBackupMiddlewareApp;
            PrimaryBackupManager.PrimaryBackupMiddlewareApp middlewarePaxosApp =
                    (PrimaryBackupManager.PrimaryBackupMiddlewareApp) app;
            this.pbManager = new PrimaryBackupManager<>(
                    myID, unstringer, middlewarePaxosApp, paxosManager, messenger);
        }

        // initialize all the request types handled
        Set<IntegerPacketType> types = new HashSet<>(app.getRequestTypes());
        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        types.addAll(PrimaryBackupManager.getAllPrimaryBackupPacketTypes());
        this.requestTypes = types;

        // Add packet demultiplexer for PrimaryBackupPacket that will invoke coordinateRequest().
        PrimaryBackupPacketDemultiplexer packetDemultiplexer
                = new PrimaryBackupPacketDemultiplexer(this);
        this.messenger.precedePacketDemultiplexer(packetDemultiplexer);

        // update the coordinator request parser
        // FIXME: revisit me
        this.setGetRequestImpl(this.pbManager);
    }

    /**
     * The default constructor for Primary Backup without providing paxos manager and paxos
     * middleware application. This constructor should be used when primary-backup is *the only*
     * coordinator used in the deployment.
     *
     * @param app                    the replicated Application.
     * @param myNodeId               the ID of this node.
     * @param nodeIdTypeDeserializer deserializer of the node Id.
     * @param messenger              messenger to send packet between nodes.
     */
    public PrimaryBackupReplicaCoordinator(Replicable app,
                                           NodeIDType myNodeId,
                                           Stringifiable<NodeIDType> nodeIdTypeDeserializer,
                                           Messenger<NodeIDType, JSONObject> messenger) {
        this(
                app,
                myNodeId,
                nodeIdTypeDeserializer,
                messenger,
                null
        );
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return requestTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        ExecutedCallback chainedCallback = callback;

        // System.out.printf(">>> %s:PBRCoordinator - coordinateRequest %s %s\n\n",
        //        getMyID(), request.getClass().getSimpleName(),
        //        request instanceof ReplicableClientRequest rcr ? rcr.getRequest().getClass().getSimpleName() : "null");

        // if packet comes from client (i.e., ReplicableClientRequest), wrap the
        // containing request with RequestPacket, and re-chain the callback.
        // Nvm, ReplicableClientRequest can contain other PrimaryBackupPacket :(
        if (request instanceof ReplicableClientRequest rcr) {
            boolean isEndUserRequest = (rcr.getRequest() instanceof ClientRequest);

            if (isEndUserRequest) {
                ClientRequest appRequest = (ClientRequest) rcr.getRequest();
                request = new RequestPacket(
                        rcr.getServiceName(),
                        appRequest.toString().getBytes(StandardCharsets.ISO_8859_1));
                chainedCallback = (executedRequestPacket, handled) -> {
                    assert executedRequestPacket instanceof RequestPacket;
                    RequestPacket response = (RequestPacket) executedRequestPacket;
                    callback.executed(response.getResponse(), handled);
                };
            }

            if (!isEndUserRequest) {
                request = rcr.getRequest();
            }
        }

        if (request instanceof PrimaryBackupPacket packet) {
            return this.pbManager.handlePrimaryBackupPacket(packet, chainedCallback);
        }

        // handle application stop request
        if (request instanceof ReconfigurableRequest reconfigurableRequest) {
            return this.pbManager.handleReconfigurationPacket(reconfigurableRequest, callback);
        }

        // printout a helpful exception message by showing the possible acceptable packets
        StringBuilder requestTypeString = new StringBuilder();
        for (IntegerPacketType p : this.app.getRequestTypes()) {
            requestTypeString.append(p.toString()).append(" ");
        }
        throw new RuntimeException(String.format(
                "Unknown request of class '%s' for Primary Backup Coordinator. " +
                        "Request must use either %s, %s, or one of the app request types: %s.",
                request.getClass().getSimpleName(),
                ReplicableClientRequest.class.getSimpleName(),
                PrimaryBackupPacket.class.getSimpleName(),
                requestTypeString));
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes) {
        assert serviceName != null && !serviceName.isEmpty();
        assert epoch >= 0;
        assert !nodes.isEmpty();
        return this.pbManager.createPrimaryBackupInstance(serviceName, epoch, state, nodes);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        return this.pbManager.deletePrimaryBackupInstance(serviceName, epoch);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return this.pbManager.getReplicaGroup(serviceName);
    }

    public AppRequestParser getRequestParser() {
        return this.pbManager;
    }

    public PrimaryBackupManager<NodeIDType> getPrimaryBackupManager() {
        return this.pbManager;
    }

    public final void close() {
        this.stop();
        this.pbManager.stop();
    }

}
