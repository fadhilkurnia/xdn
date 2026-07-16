package edu.umass.cs.eventual;

import edu.umass.cs.clientcentric.VectorTimestamp;
import edu.umass.cs.eventual.interfaces.CheckpointableApplication;
import edu.umass.cs.eventual.packets.LazyCheckpointPacket;
import edu.umass.cs.eventual.packets.LazyPacket;
import edu.umass.cs.eventual.packets.LazyPacketType;
import edu.umass.cs.eventual.packets.LazySyncPacket;
import edu.umass.cs.eventual.packets.LazyWriteAfterPacket;
import edu.umass.cs.gigapaxos.interfaces.ClientRequest;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.AbstractReplicaCoordinator;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.interfaces.behavior.BehavioralRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.json.JSONObject;

/**
 * LazyReplicaCoordinator implements eventual consistency with frontier-based anti-entropy.
 *
 * <p>Every replica accepts client requests: reads execute locally; writes execute locally, are
 * acknowledged immediately, increment the replica's own component of a per-service vector clock,
 * and are broadcast to peers as a best-effort {@link LazyWriteAfterPacket} fast path (peers apply
 * a write-after only when contiguous with their clock, otherwise drop it).
 *
 * <p>Convergence is guaranteed by periodic anti-entropy: every
 * {@code XDN_EVENTUAL_ANTI_ENTROPY_INTERVAL_MS} each replica broadcasts its vector clock and a
 * digest of its state ({@link LazySyncPacket}). A replica that detects itself as the
 * <em>frontier</em> relative to a peer (larger applied-write set, i.e., larger clock sum, with
 * ties broken by node ID) captures a checkpoint of its whole service state and ships it to that
 * peer ({@link LazyCheckpointPacket}). The receiver installs the checkpoint, then re-applies its
 * own writes the checkpoint lacks; each replica therefore retains its own executed writes until
 * every peer's clock covers them. Once writes stop, every write reaches the frontier and its
 * final checkpoint propagates unchanged, leaving all replicas byte-identical, regardless of
 * service determinism or request commutativity/monotonicity.
 *
 * <p>Known limitations: protocol state (vector clock and own-write log) is in-memory, so a
 * replica process restart rejoins with a zeroed clock and re-adopts its own component from the
 * first checkpoint it installs; a membership/epoch change resets protocol state (packets carry
 * the epoch and mismatches are dropped); convergence liveness assumes replicas eventually
 * communicate.
 */
public class LazyReplicaCoordinator<NodeIDType> extends AbstractReplicaCoordinator<NodeIDType> {

    private final NodeIDType myNodeId;
    private final String myNodeIdStr;
    private final Replicable app;
    private final CheckpointableApplication checkpointableApp; // null if app is not checkpointable
    private final Set<IntegerPacketType> packetTypes;
    private final Messenger<NodeIDType, JSONObject> messenger;
    private final Stringifiable<NodeIDType> nodeIdDeserializer;

    private final ConcurrentMap<String, LazyServiceState<NodeIDType>> currentInstances;

    private final ScheduledExecutorService antiEntropyScheduler;
    private final long antiEntropyIntervalMs;
    private final long checkpointInlineMaxBytes;

    private final Logger logger = Logger.getLogger(LazyReplicaCoordinator.class.getSimpleName());

    public LazyReplicaCoordinator(Replicable app,
                                  NodeIDType myId,
                                  Stringifiable<NodeIDType> nodeIdDeserializer,
                                  Messenger<NodeIDType, JSONObject> messenger) {
        super(app, messenger);
        this.myNodeId = myId;
        this.myNodeIdStr = myId.toString();
        this.messenger = messenger;
        this.app = app;
        this.nodeIdDeserializer = nodeIdDeserializer;
        this.checkpointableApp = (app instanceof CheckpointableApplication ca) ? ca : null;

        // validate the nodeIdDeserializer
        assert messenger.getMyID().equals(myId) : "Invalid node ID given in the messenger";
        assert nodeIdDeserializer.valueOf(this.myNodeId.toString()).equals(this.myNodeId)
                : "Invalid node ID deserializer given";

        // initialize all the supported packet types
        this.packetTypes = new HashSet<>();
        this.packetTypes.addAll(List.of(LazyPacketType.values()));

        this.currentInstances = new ConcurrentHashMap<>();

        this.antiEntropyIntervalMs = Config.getGlobalLong(
                ReconfigurationConfig.RC.XDN_EVENTUAL_ANTI_ENTROPY_INTERVAL_MS);
        this.checkpointInlineMaxBytes = Config.getGlobalLong(
                ReconfigurationConfig.RC.XDN_EVENTUAL_CHECKPOINT_INLINE_MAX_BYTES);

        // Periodic anti-entropy round with a randomized initial delay so replicas
        // de-synchronize their rounds (same pattern as gigapaxos' FailureDetection).
        this.antiEntropyScheduler = Executors.newScheduledThreadPool(1, r -> {
            Thread t = new Thread(r, "lazy-anti-entropy-" + myNodeId);
            t.setDaemon(true);
            return t;
        });
        long initialDelayMs = antiEntropyIntervalMs
                + new Random().nextLong(Math.max(1, antiEntropyIntervalMs / 2));
        this.antiEntropyScheduler.scheduleAtFixedRate(this::runAntiEntropyRound,
                initialDelayMs, antiEntropyIntervalMs, TimeUnit.MILLISECONDS);

        // add packet demultiplexer for LazyPacket that will invoke
        // the coordinateRequest() method.
        LazyPacketDemultiplexer packetDemultiplexer =
                new LazyPacketDemultiplexer(this, app);
        this.messenger.precedePacketDemultiplexer(packetDemultiplexer);
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return this.packetTypes;
    }

    @Override
    public boolean coordinateRequest(Request request, ExecutedCallback callback)
            throws IOException, RequestParseException {
        if (logger.isLoggable(Level.FINE)) {
            logger.log(Level.FINE, ">> " + myNodeId + " LazyReplicaCoordinator -- receiving request " +
                    request.getClass().getSimpleName());
        }
        if (!(request instanceof ReplicableClientRequest) && !(request instanceof LazyPacket)) {
            throw new RuntimeException("Unknown request/packet handled by LazyReplicaCoordinator");
        }

        // validate that service exists
        String serviceName = request.getServiceName();
        LazyServiceState<NodeIDType> state = this.currentInstances.get(serviceName);
        if (state == null) {
            logger.log(Level.WARNING, "Ignoring request for unknown service=" + serviceName);
            return true;
        }

        // unwrap the request if needed
        Request currRequestOrPacket = request;
        if (currRequestOrPacket instanceof ReplicableClientRequest rcr) {
            currRequestOrPacket = rcr.getRequest();
        }

        // handle StopEpoch packet
        if (currRequestOrPacket instanceof ReconfigurableRequest rcRequest &&
                rcRequest.isStop()) {
            boolean isSuccess = this.app.restore(serviceName, null);
            callback.executed(rcRequest, isSuccess);
            return true;
        }

        // handle client-initiated request: reads execute locally; every other request
        // (write-only, read-modify-write, batches, or undeclared behavior) is treated as
        // a write, executed locally, and propagated lazily.
        if (currRequestOrPacket instanceof ClientRequest clientRequest) {
            boolean isReadOnly = (clientRequest instanceof BehavioralRequest br)
                    && br.isReadOnlyRequest();
            if (isReadOnly) {
                ReentrantReadWriteLock.ReadLock readLock = state.lock.readLock();
                readLock.lock();
                boolean isExecSuccess;
                try {
                    isExecSuccess = this.app.execute(clientRequest);
                } finally {
                    readLock.unlock();
                }
                callback.executed(clientRequest, isExecSuccess);
                return true;
            }
            return handleClientWrite(state, clientRequest, callback);
        }

        // handle peer-initiated packets
        if (currRequestOrPacket instanceof LazyWriteAfterPacket writeAfterPacket) {
            return handleWriteAfterPacket(state, writeAfterPacket);
        }
        if (currRequestOrPacket instanceof LazySyncPacket syncPacket) {
            return handleSyncPacket(state, syncPacket);
        }
        if (currRequestOrPacket instanceof LazyCheckpointPacket checkpointPacket) {
            return handleCheckpointPacket(state, checkpointPacket);
        }

        throw new IllegalStateException("Unexpected LazyPacket/Request: " + request.getRequestType());
    }

    private boolean handleClientWrite(LazyServiceState<NodeIDType> state,
                                      ClientRequest clientRequest,
                                      ExecutedCallback callback) {
        boolean isExecSuccess;
        long seqNum = -1;
        byte[] encodedRequest = null;
        ReentrantReadWriteLock.WriteLock writeLock = state.lock.writeLock();
        writeLock.lock();
        try {
            isExecSuccess = this.app.execute(clientRequest);
            if (isExecSuccess) {
                // Encode synchronously, while the request content buffers are guaranteed
                // alive, so the write can be retained for later re-application and shipped
                // to peers without reference-count juggling.
                encodedRequest = clientRequest.toBytes();
                seqNum = state.vc.getNodeTimestamp(myNodeIdStr) + 1;
                state.vc.updateNodeTimestamp(myNodeIdStr, seqNum);
                state.ownLog.add(encodedRequest);
                state.digestCache = null;
            }
        } finally {
            writeLock.unlock();
        }

        if (!isExecSuccess) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.log(Level.WARNING, "Failed to execute request: " + clientRequest);
            }
            callback.executed(clientRequest, false);
            return true;
        }
        callback.executed(clientRequest, true);

        // Best-effort fast path: broadcast the write to peers on a virtual thread.
        final long finalSeqNum = seqNum;
        final byte[] finalEncodedRequest = encodedRequest;
        Thread.ofVirtual().start(() -> {
            try {
                LazyPacket writeAfterPacket = new LazyWriteAfterPacket(
                        myNodeIdStr, finalSeqNum, state.epoch, clientRequest, finalEncodedRequest);
                Set<NodeIDType> myPeers = new HashSet<>(state.nodes);
                myPeers.remove(myNodeId);
                if (myPeers.isEmpty()) return;
                GenericMessagingTask<NodeIDType, LazyPacket> m =
                        new GenericMessagingTask<>(myPeers.toArray(), writeAfterPacket);
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, "Sending WRITE_AFTER packet ...");
                }
                messenger.send(m);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Failed to send WRITE_AFTER", e);
            }
        });
        return true;
    }

    private boolean handleWriteAfterPacket(LazyServiceState<NodeIDType> state,
                                           LazyWriteAfterPacket packet) {
        if (packet.getEpoch() != state.epoch) {
            logger.log(Level.FINE, "Dropping WRITE_AFTER with mismatched epoch");
            return true;
        }
        String sender = packet.getSenderId();
        // Never block the shared NIO demultiplexer thread behind a long checkpoint
        // install: the fast path is best-effort, so drop on lock contention and let
        // anti-entropy recover the write.
        ReentrantReadWriteLock.WriteLock writeLock = state.lock.writeLock();
        if (!writeLock.tryLock()) {
            logger.log(Level.FINE, "Dropping WRITE_AFTER due to lock contention");
            return true;
        }
        try {
            long expectedSeq = state.vc.getNodeTimestamp(sender) + 1;
            if (packet.getSenderSeqNum() != expectedSeq) {
                // Duplicate or gapped fast-path write: drop it; the periodic anti-entropy
                // checkpoint transfer delivers the sender's writes in bulk instead.
                if (logger.isLoggable(Level.FINE)) {
                    logger.log(Level.FINE, String.format(
                            "%s: dropping WRITE_AFTER from %s seq=%d expected=%d",
                            myNodeIdStr, sender, packet.getSenderSeqNum(), expectedSeq));
                }
                return true;
            }
            boolean isExecSuccess = this.app.execute(packet.getClientRequest(), true);
            if (isExecSuccess) {
                state.vc.updateNodeTimestamp(sender, expectedSeq);
                state.digestCache = null;
            }
            return true;
        } finally {
            writeLock.unlock();
        }
    }

    /** One periodic anti-entropy tick: advertise (vector clock, state digest) to all peers. */
    private void runAntiEntropyRound() {
        for (LazyServiceState<NodeIDType> state : this.currentInstances.values()) {
            try {
                VectorTimestamp vcSnapshot;
                byte[] digest;
                ReentrantReadWriteLock.ReadLock readLock = state.lock.readLock();
                // Skip this service for a round if an install holds the write lock.
                if (!readLock.tryLock(antiEntropyIntervalMs / 2, TimeUnit.MILLISECONDS)) {
                    continue;
                }
                try {
                    vcSnapshot = VectorClockCodec.copy(state.vc);
                    digest = getOrComputeDigest(state);
                } finally {
                    readLock.unlock();
                }

                Set<NodeIDType> myPeers = new HashSet<>(state.nodes);
                myPeers.remove(myNodeId);
                if (myPeers.isEmpty()) continue;

                LazySyncPacket syncPacket = new LazySyncPacket(
                        myNodeIdStr, state.serviceName, vcSnapshot, digest, state.epoch);
                GenericMessagingTask<NodeIDType, LazyPacket> m =
                        new GenericMessagingTask<>(myPeers.toArray(), syncPacket);
                messenger.send(m);
            } catch (Exception e) {
                logger.log(Level.WARNING, "Anti-entropy round failed for service="
                        + state.serviceName, e);
            }
        }
    }

    // Must be called while holding state.lock (read or write).
    private byte[] getOrComputeDigest(LazyServiceState<NodeIDType> state) {
        if (state.digestCache != null) return state.digestCache;
        byte[] digest = this.checkpointableApp != null
                ? this.checkpointableApp.getStateDigest(state.serviceName)
                : fallbackDigest(state.serviceName);
        state.digestCache = digest;
        return digest;
    }

    // Digest fallback for plain Replicable apps without CheckpointableApplication.
    private byte[] fallbackDigest(String serviceName) {
        String checkpoint = this.app.checkpoint(serviceName);
        if (checkpoint == null) return null;
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(checkpoint.getBytes(StandardCharsets.ISO_8859_1));
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    private boolean handleSyncPacket(LazyServiceState<NodeIDType> state, LazySyncPacket packet) {
        if (packet.getEpoch() != state.epoch) {
            logger.log(Level.FINE, "Dropping SYNC with mismatched epoch");
            return true;
        }
        String sender = packet.getSenderId();
        VectorTimestamp theirVc = packet.getVectorClock();
        if (!theirVc.isComparableWith(state.vc)) {
            logger.log(Level.WARNING, "Dropping SYNC with incomparable vector clock from "
                    + sender);
            return true;
        }

        boolean shouldShip;
        VectorTimestamp myVcSnapshot;
        byte[] myDigest;
        // Runs on the shared NIO demultiplexer thread: skip this round on lock
        // contention (e.g., an install in progress) instead of blocking the pipeline.
        ReentrantReadWriteLock.ReadLock readLock = state.lock.readLock();
        if (!readLock.tryLock()) {
            logger.log(Level.FINE, "Skipping SYNC handling due to lock contention");
            return true;
        }
        try {
            synchronized (state.latestReport) {
                state.latestReport.put(sender, new LazyServiceState.PeerReport(
                        theirVc, packet.getStateDigest(), System.currentTimeMillis()));
                VectorTimestamp floor = state.peerVcFloor.get(sender);
                if (floor == null) {
                    state.peerVcFloor.put(sender, VectorClockCodec.copy(theirVc));
                } else {
                    VectorClockCodec.maxMergeInto(floor, theirVc);
                }
            }
            myVcSnapshot = VectorClockCodec.copy(state.vc);
            myDigest = getOrComputeDigest(state);
            shouldShip = evaluateShipDecision(state, sender, theirVc, packet.getStateDigest(),
                    myVcSnapshot, myDigest);
        } finally {
            readLock.unlock();
        }

        pruneOwnLog(state);

        if (shouldShip) {
            shipCheckpointAsync(state, sender);
        }
        return true;
    }

    // Must be called while holding state.lock (read or write).
    private boolean evaluateShipDecision(LazyServiceState<NodeIDType> state, String peer,
                                         VectorTimestamp theirVc, byte[] theirDigest,
                                         VectorTimestamp myVc, byte[] myDigest) {
        long mySum = VectorClockCodec.sum(myVc);
        long theirSum = VectorClockCodec.sum(theirVc);
        boolean frontierWins = VectorClockCodec.frontierWins(mySum, myNodeIdStr, theirSum, peer);
        if (!frontierWins) return false;

        boolean peerLags = false;
        for (String nodeId : myVc.getNodeIds()) {
            if (theirVc.getNodeTimestamp(nodeId) < myVc.getNodeTimestamp(nodeId)) {
                peerLags = true;
                break;
            }
        }
        // At equal clocks, replicas of a non-deterministic service may still hold different
        // bytes (fast-path re-execution diverges); the digest comparison forces one final
        // verbatim state transfer from the tie-break frontier.
        boolean tieDiverged = theirVc.isEqualTo(myVc)
                && theirDigest != null && myDigest != null
                && !Arrays.equals(theirDigest, myDigest);
        if (!peerLags && !tieDiverged) return false;

        // Dedup/backoff: skip if we already shipped this exact (clock, digest) to this peer
        // recently; a re-ship is allowed after 3 intervals in case the checkpoint was lost.
        synchronized (state.lastShip) {
            LazyServiceState.ShipRecord last = state.lastShip.get(peer);
            if (last != null && last.vc().isEqualTo(myVc)
                    && Arrays.equals(last.digest(), myDigest)
                    && System.currentTimeMillis() - last.timeMs() < 3 * antiEntropyIntervalMs) {
                return false;
            }
        }
        return true;
    }

    private void shipCheckpointAsync(LazyServiceState<NodeIDType> state, String peer) {
        if (!state.shipInFlight.compareAndSet(false, true)) return;
        Thread.ofVirtual().start(() -> {
            try {
                VectorTimestamp vcAtCapture;
                byte[] checkpointBytes = null;
                String checkpointHandle = null;
                byte[] digest;
                ReentrantReadWriteLock.ReadLock readLock = state.lock.readLock();
                readLock.lock();
                try {
                    vcAtCapture = VectorClockCodec.copy(state.vc);
                    digest = getOrComputeDigest(state);
                    if (this.checkpointableApp != null) {
                        checkpointBytes = this.checkpointableApp
                                .captureCheckpoint(state.serviceName);
                        if (checkpointBytes != null
                                && checkpointBytes.length > checkpointInlineMaxBytes) {
                            checkpointBytes = null;
                            checkpointHandle = this.checkpointableApp
                                    .captureCheckpointHandle(state.serviceName);
                        }
                    } else {
                        String checkpoint = this.app.checkpoint(state.serviceName);
                        checkpointBytes = checkpoint != null
                                ? checkpoint.getBytes(StandardCharsets.ISO_8859_1) : null;
                    }
                } finally {
                    readLock.unlock();
                }

                if (checkpointBytes == null && checkpointHandle == null) {
                    logger.log(Level.SEVERE, String.format(
                            "%s: failed to capture checkpoint of service=%s for peer=%s",
                            myNodeIdStr, state.serviceName, peer));
                    return;
                }

                LazyCheckpointPacket packet = checkpointBytes != null
                        ? LazyCheckpointPacket.createInline(myNodeIdStr, state.serviceName,
                        vcAtCapture, digest, state.epoch, checkpointBytes)
                        : LazyCheckpointPacket.createWithHandle(myNodeIdStr, state.serviceName,
                        vcAtCapture, digest, state.epoch, checkpointHandle);

                NodeIDType peerNodeId = nodeIdDeserializer.valueOf(peer);
                GenericMessagingTask<NodeIDType, LazyPacket> m =
                        new GenericMessagingTask<>(peerNodeId, packet);
                messenger.send(m);
                synchronized (state.lastShip) {
                    state.lastShip.put(peer, new LazyServiceState.ShipRecord(
                            vcAtCapture, digest, System.currentTimeMillis()));
                }
                logger.log(Level.INFO, String.format(
                        "%s: shipped checkpoint of service=%s to lagging peer=%s vc=%s bytes=%d",
                        myNodeIdStr, state.serviceName, peer, vcAtCapture,
                        checkpointBytes != null ? checkpointBytes.length : -1));
            } catch (Exception e) {
                logger.log(Level.SEVERE, "Failed to ship checkpoint of service="
                        + state.serviceName + " to peer=" + peer, e);
            } finally {
                state.shipInFlight.set(false);
            }
        });
    }

    private boolean handleCheckpointPacket(LazyServiceState<NodeIDType> state,
                                           LazyCheckpointPacket packet) {
        if (packet.getEpoch() != state.epoch) {
            logger.log(Level.FINE, "Dropping CHECKPOINT with mismatched epoch");
            return true;
        }
        if (!packet.getVectorClock().isComparableWith(state.vc)) {
            logger.log(Level.WARNING, "Dropping CHECKPOINT with incomparable vector clock from "
                    + packet.getSenderId());
            return true;
        }
        // Installs stop the container and restart it, taking seconds: run them on the
        // per-service serial executor, off the NIO demultiplexer thread.
        state.installExecutor.execute(() -> installCheckpoint(state, packet));
        return true;
    }

    private void installCheckpoint(LazyServiceState<NodeIDType> state,
                                   LazyCheckpointPacket packet) {
        String sender = packet.getSenderId();
        VectorTimestamp vcK = packet.getVectorClock();
        ReentrantReadWriteLock.WriteLock writeLock = state.lock.writeLock();
        writeLock.lock();
        try {
            // Strict install rule, re-checked against the current clock:
            // (a) never regress a foreign component (would silently lose foreign writes
            //     this state already reflects, and would break pruning monotonicity);
            // (b) the checkpoint must cover our pruned prefix, so the own-write suffix
            //     can bridge the gap;
            // (c) install only a strictly newer checkpoint, or an equal-clock one from a
            //     tie-break winner (byte reconciliation for non-deterministic services).
            for (String nodeId : state.vc.getNodeIds()) {
                if (nodeId.equals(myNodeIdStr)) continue;
                if (vcK.getNodeTimestamp(nodeId) < state.vc.getNodeTimestamp(nodeId)) {
                    logger.log(Level.FINE, myNodeIdStr
                            + ": dropping checkpoint that regresses component " + nodeId);
                    return;
                }
            }
            if (vcK.getNodeTimestamp(myNodeIdStr) < state.firstSeq - 1) {
                logger.log(Level.WARNING, myNodeIdStr
                        + ": dropping checkpoint older than the pruned own-write log");
                return;
            }
            boolean strictlyNewer = false;
            for (String nodeId : state.vc.getNodeIds()) {
                if (vcK.getNodeTimestamp(nodeId) > state.vc.getNodeTimestamp(nodeId)) {
                    strictlyNewer = true;
                    break;
                }
            }
            boolean tieInstall = vcK.isEqualTo(state.vc)
                    && VectorClockCodec.frontierWins(
                    VectorClockCodec.sum(vcK), sender,
                    VectorClockCodec.sum(state.vc), myNodeIdStr);
            if (!strictlyNewer && !tieInstall) {
                logger.log(Level.FINE, myNodeIdStr + ": dropping dominated/duplicate checkpoint");
                return;
            }

            // Install the checkpoint state.
            boolean isApplied;
            if (this.checkpointableApp != null) {
                isApplied = packet.getMode() == LazyCheckpointPacket.MODE_INLINE
                        ? this.checkpointableApp.applyCheckpoint(
                        state.serviceName, packet.getCheckpointData())
                        : this.checkpointableApp.applyCheckpointHandle(
                        state.serviceName, packet.getCheckpointHandle());
            } else {
                isApplied = packet.getMode() == LazyCheckpointPacket.MODE_INLINE
                        && this.app.restore(state.serviceName, new String(
                        packet.getCheckpointData(), StandardCharsets.ISO_8859_1));
            }
            if (!isApplied) {
                // Leave the clock untouched: it now underclaims at worst, which is the safe
                // direction; a later anti-entropy round re-ships and re-installs.
                logger.log(Level.SEVERE, String.format(
                        "%s: failed to install checkpoint of service=%s from %s",
                        myNodeIdStr, state.serviceName, sender));
                return;
            }

            // Advance the clock to the checkpoint, keeping our own component monotone.
            long ownCovered = vcK.getNodeTimestamp(myNodeIdStr);
            long myOwnCount = state.vc.getNodeTimestamp(myNodeIdStr);
            for (String nodeId : state.vc.getNodeIds()) {
                if (nodeId.equals(myNodeIdStr)) continue;
                state.vc.updateNodeTimestamp(nodeId, vcK.getNodeTimestamp(nodeId));
            }
            state.vc.updateNodeTimestamp(myNodeIdStr, Math.max(myOwnCount, ownCovered));

            // Re-apply our own writes the checkpoint lacks, in sequence order.
            int reapplied = 0;
            for (long seq = ownCovered + 1; seq <= state.vc.getNodeTimestamp(myNodeIdStr); seq++) {
                int index = (int) (seq - state.firstSeq);
                if (index < 0 || index >= state.ownLog.size()) break;
                byte[] encodedRequest = state.ownLog.get(index);
                try {
                    Request ownWrite = this.app.getRequest(
                            new String(encodedRequest, StandardCharsets.ISO_8859_1));
                    boolean ok = this.app.execute(ownWrite, true);
                    if (!ok) {
                        logger.log(Level.SEVERE, myNodeIdStr
                                + ": failed to re-apply own write seq=" + seq);
                    }
                    reapplied++;
                } catch (RequestParseException e) {
                    logger.log(Level.SEVERE, myNodeIdStr
                            + ": failed to decode own write seq=" + seq, e);
                }
            }
            state.digestCache = null;

            logger.log(Level.INFO, String.format(
                    "%s: installed checkpoint of service=%s from %s vc=%s reappliedOwnWrites=%d",
                    myNodeIdStr, state.serviceName, sender, vcK, reapplied));
        } finally {
            writeLock.unlock();
        }
    }

    /** Drops own writes that every peer's clock is known to cover (classic vector-clock GC). */
    private void pruneOwnLog(LazyServiceState<NodeIDType> state) {
        ReentrantReadWriteLock.WriteLock writeLock = state.lock.writeLock();
        if (!writeLock.tryLock()) return; // opportunistic; retried on the next sync
        try {
            long minCovered = Long.MAX_VALUE;
            synchronized (state.latestReport) {
                for (NodeIDType node : state.nodes) {
                    String nodeId = node.toString();
                    if (nodeId.equals(myNodeIdStr)) continue;
                    VectorTimestamp floor = state.peerVcFloor.get(nodeId);
                    long covered = floor != null ? floor.getNodeTimestamp(myNodeIdStr) : 0;
                    minCovered = Math.min(minCovered, covered);
                }
            }
            if (minCovered == Long.MAX_VALUE) return; // no peers
            while (!state.ownLog.isEmpty() && state.firstSeq <= minCovered) {
                state.ownLog.remove(0);
                state.firstSeq++;
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public boolean createReplicaGroup(String serviceName, int epoch, String state, Set<NodeIDType> nodes, String placementMetadata) {
        if (logger.isLoggable(Level.INFO)) {
            logger.log(Level.INFO, String.format(">> %s:LazyReplicaCoordinator -- " +
                            "createReplicaGroup name=%s nodes=%s epoch=%d state=%s",
                    myNodeId, serviceName, nodes, epoch, state));
        }
        LazyServiceState<NodeIDType> serviceState =
                new LazyServiceState<>(serviceName, epoch, nodes);
        serviceState.firstSeq = 1;
        LazyServiceState<NodeIDType> previous =
                this.currentInstances.put(serviceName, serviceState);
        if (previous != null) {
            previous.shutdown();
        }

        // Creating a replica group is a special case for reconfiguration where we reconfigure
        // from nothing to something. In that case, we call app.restore(.) with initialState.
        return this.app.restore(serviceName, state);
    }

    @Override
    public boolean deleteReplicaGroup(String serviceName, int epoch) {
        LazyServiceState<NodeIDType> state = this.currentInstances.get(serviceName);
        if (state == null) return true;
        if (state.epoch != epoch) return true;
        this.currentInstances.remove(serviceName);
        state.shutdown();

        // Deleting a replica group is a special case for reconfiguration where we reconfigure
        // from something into nothing. In that case, we call app.restore(.) with null state.
        return this.app.restore(serviceName, null);
    }

    @Override
    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        LazyServiceState<NodeIDType> state = this.currentInstances.get(serviceName);
        return state != null ? state.nodes : null;
    }
}
