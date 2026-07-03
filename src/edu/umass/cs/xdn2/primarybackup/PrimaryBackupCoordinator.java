package edu.umass.cs.xdn2.primarybackup;

import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.xdn2.XdnApp;
import edu.umass.cs.xdn2.primarybackup.packets.ApplyStateDiffPacket;
import edu.umass.cs.xdn2.primarybackup.packets.PrimaryBackupPacket;
import edu.umass.cs.xdn2.primarybackup.packets.PrimaryBackupPacketType;
import edu.umass.cs.xdn2.primarybackup.packets.StartEpochPacket;
import edu.umass.cs.xdn2.request.XdnHttpRequest;
import io.netty.handler.codec.http.HttpMethod;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * PrimaryBackupCoordinator is the {@link AbstractReplicaCoordinator} implementation
 * for primary-backup replication in XDN. It handles non-deterministic services where
 * only the primary executes client requests, and state diffs are replicated to backups
 * via Paxos-ordered {@link ApplyStateDiffPacket}s.
 *
 * This class is intentionally a thin unwrap-and-delegate layer: it satisfies
 * GigaPaxos's {@link AbstractReplicaCoordinator} contract, but all actual
 * primary-backup protocol state and logic lives in {@link PrimaryBackupManager}.
 *
 * @param <NodeIDType> the type used to identify nodes in the system.
 */
public class PrimaryBackupCoordinator<NodeIDType>
        extends AbstractReplicaCoordinator<NodeIDType> {

    private final PaxosManager<NodeIDType> paxosManager;
    private final PrimaryBackupManager<NodeIDType> pbManager;

    private final java.util.logging.Logger logger =
            java.util.logging.Logger.getLogger(
                    PrimaryBackupCoordinator.class.getSimpleName());

    /**
     * @param app          the replicated application — must implement
     *                     {@code BackupableApplication}.
     * @param myID         this node's ID.
     * @param unstringer   deserializer for {@code NodeIDType}.
     * @param messenger    messenger for inter-node communication.
     * @param paxosManager the Paxos manager to use for consensus.
     */
    public PrimaryBackupCoordinator(XdnApp app,
                                    NodeIDType myID,
                                    Stringifiable<NodeIDType> unstringer,
                                    Messenger<NodeIDType, JSONObject> messenger,
                                    PaxosManager<NodeIDType> paxosManager) {
        super(app, messenger);
        this.paxosManager = paxosManager;
        this.pbManager = new PrimaryBackupManager<>(myID, unstringer, app, paxosManager, messenger);

        PrimaryBackupPacketDemultiplexer demultiplexer =
                new PrimaryBackupPacketDemultiplexer(this);
        this.messenger.precedePacketDemultiplexer(demultiplexer);
    }

    // -------------------------------------------------------------------------
    // AbstractReplicaCoordinator — required overrides
    // -------------------------------------------------------------------------

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        Set<IntegerPacketType> types = new HashSet<>();
        types.add(PrimaryBackupPacketType.PB2_START_EPOCH_PACKET);
        types.add(PrimaryBackupPacketType.PB2_APPLY_STATE_DIFF_PACKET);
        types.add(ReconfigurationPacket.PacketType.REPLICABLE_CLIENT_REQUEST);
        // TODO: add app.getRequestTypes() once `app` field accessibility is
        //  confirmed (AbstractReplicaCoordinator stores it as `this.app`,
        //  wrapped in TrivialRepliconfigurable if not already Repliconfigurable).
        return types;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {

        // PrimaryBackupPacket: StartEpochPacket / ApplyStateDiffPacket,
        // committed via Paxos and delivered here on commit.
        if (request instanceof PrimaryBackupPacket packet) {
            return this.pbManager.handlePrimaryBackupPacket(packet, callback);
        }

        // ReplicableClientRequest: an actual end-user request.
        if (request instanceof ReplicableClientRequest rcr) {
            String serviceName = rcr.getServiceName();

            // TODO: extract clientStateDiffCount and isWriteRequest from the
            //  unwrapped request. The exact shape of the client request type
            //  in xdn2 (e.g. XdnHttpRequest) and where the client-supplied
            //  stateDiffCount cookie/header lives was not traced in this
            //  pass -- placeholders below.
            Integer clientStateDiffCount = null; // TODO: extract from request headers/cookie
            boolean isWriteRequest = false;
            if (rcr.getRequest() instanceof XdnHttpRequest xdnHttpRequest) {
                HttpMethod method = xdnHttpRequest.getHttpRequest().method();
                isWriteRequest = method.equals(HttpMethod.POST)
                        || method.equals(HttpMethod.PUT)
                        || method.equals(HttpMethod.DELETE)
                        || method.equals(HttpMethod.PATCH);
            }
            return this.pbManager.handleClientRequest(
                    serviceName, rcr.getRequest(), clientStateDiffCount,
                    isWriteRequest, callback);
        }

        // ReconfigurableRequest: e.g. stop requests during reconfiguration.
        if (request instanceof ReconfigurableRequest reconfigurableRequest) {
            // TODO: delegate to PrimaryBackupManager once
            //  handleReconfigurationPacket()-equivalent is designed. Deferred
            //  per earlier design discussion (steady-state/reconfiguration
            //  teardown not yet finalized).
            throw new UnsupportedOperationException(
                    "Reconfiguration request handling not yet implemented");
        }


        // printout a helpful exception message by showing the possible acceptable packets
        throw new RuntimeException(String.format(
                "Unknown request of class '%s' for Primary Backup Coordinator. " +
                        "Request must use either %s, %s, or %s.",
                request.getClass().getSimpleName(),
                ReplicableClientRequest.class.getSimpleName(),
                PrimaryBackupPacket.class.getSimpleName(),
                ReconfigurableRequest.class.getSimpleName()));
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state,
                                      Set<NodeIDType> nodes, String placementMetadata) {
        assert serviceName != null && !serviceName.isEmpty();
        assert epoch >= 0;
        assert !nodes.isEmpty();
        return this.pbManager.createReplicaGroup(
                serviceName, epoch, state, nodes, placementMetadata);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        // TODO: delegate to PrimaryBackupManager.deleteReplicaGroup() once
        //  the teardown/Upon deleteReplicaGroup event is designed.
        return false;
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return this.pbManager.getReplicaGroup(serviceName);
    }

    // =========================================================================
    // Public Methods
    // =========================================================================

    public PrimaryBackupManager<NodeIDType> getPrimaryBackupManager() {
        return this.pbManager;
    }

    public boolean isPrimary(String serviceName) {
        return this.pbManager.isPrimary(serviceName);
    }
}