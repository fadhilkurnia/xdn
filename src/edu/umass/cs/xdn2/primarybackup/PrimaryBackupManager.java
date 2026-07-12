package edu.umass.cs.xdn2.primarybackup;

import edu.umass.cs.gigapaxos.PaxosConfig;
import edu.umass.cs.gigapaxos.PaxosManager;
import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.GenericMessagingTask;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.interfaces.Messenger;
import edu.umass.cs.nio.interfaces.NodeConfig;
import edu.umass.cs.nio.interfaces.Stringifiable;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn2.XdnApp;
import edu.umass.cs.xdn2.primarybackup.packets.*;
import edu.umass.cs.xdn2.recorder.AbstractStateDiffRecorder;
import edu.umass.cs.xdn2.request.XdnHttpRequest;
import edu.umass.cs.xdn2.service.ConsistencyModel;
import edu.umass.cs.xdn2.service.ServiceInstance;
import edu.umass.cs.xdn2.service.ServiceProperty;
import edu.umass.cs.xdn2.utils.Shell;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * PrimaryBackupManager implements the xdn2 primary-backup event/action
 * protocol designed across the prior conversation. This is a from-scratch
 * implementation, independent of edu.umass.cs.primarybackup.PrimaryBackupManager.
 *
 * This is a BOILERPLATE / starting point. Event handler bodies follow the
 * pseudocode exactly as designed, with container/recorder integration left
 * as TODO stubs pointing at SandboxManager / AbstractStateDiffRecorder,
 * since the exact ServiceInstance construction wasn't traced in this pass.
 *
 * Known deliberate gaps (per design discussion, left for the fuzzer/tester
 * to surface before hardening):
 *   - Any single missed/reordered ApplyStateDiff aggressively triggers this
 *     node to attempt to become primary (no distinction between "primary is
 *     actually dead" and "one packet got dropped").
 *   - No drain-before-stop when switching blue/green backup containers.
 *   - No explicit handling of duplicate/stale (difference &lt;= 0) state
 *     diffs beyond falling through as a no-op.
 *   - Possible unsynchronized race between createReplicaGroup()'s own
 *     coordinator-nudge retry loop and the background
 *     startPollingForCoordinatorStatus() poller, both of which may call
 *     tryToBePaxosCoordinator()/getPaxosCoordinator() concurrently.
 *
 * TODO: see ApplyStateDiffPacket's javadoc for an unresolved question about
 *  which field name (currPlacementEpoch) the Notify(ApplyStateDiff) handler
 *  should compare against -- not yet fixed per explicit instruction.
 *
 * @param <NodeIDType> the type used to identify nodes in the system.
 */
public class PrimaryBackupManager<NodeIDType> {

    private enum Role {
        BACKUP,
        PRIMARY_CANDIDATE,
        PRIMARY
    }

    private final NodeIDType myNodeID;
    private final Stringifiable<NodeIDType> unstringer;
    private final XdnApp app;
    private final PaxosManager<NodeIDType> paxosManager;
    private final Messenger<NodeIDType, JSONObject> messenger;
    private final ConcurrentHashMap<Long, RequestAndCallback> forwardedRequests =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AbstractStateDiffRecorder.LiveDirType> currentLiveDirType =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, ReentrantLock> snpDiffApplyLocks =
            new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, AtomicBoolean> snpDiffApplyStopFlags =
            new ConcurrentHashMap<>();

    private record RequestAndCallback(Request request, ExecutedCallback callback) {}

    private final Logger logger =
            Logger.getLogger(PrimaryBackupManager.class.getSimpleName());

    // -------------------------------------------------------------------------
    // Per-service protocol state, keyed by serviceName.
    // -------------------------------------------------------------------------
    /** Keep track of the nodes in the replica group */
    private final ConcurrentHashMap<String, Set<NodeIDType>> replicaGroups = new ConcurrentHashMap<>();

    /** Placement epoch currently known for this service (set of nodes hosting it). */
    private final ConcurrentHashMap<String, Integer> currPlacement = new ConcurrentHashMap<>();

    /** Node ID of the current primary for this service. */
    private final ConcurrentHashMap<String, NodeIDType> currPrimaryID = new ConcurrentHashMap<>();

    /** This node's current role for the service. */
    private final ConcurrentHashMap<String, Role> currentRole = new ConcurrentHashMap<>();

    /**
     * Epoch of the current primary within the current placement.
     */
    private final ConcurrentHashMap<String, Integer> currPlacementEpoch = new ConcurrentHashMap<>();

    // Count of state diffs commited so far, used for gap detection.
    private final ConcurrentHashMap<String, Integer> cmtDiffCount = new ConcurrentHashMap<>();
    // Count of state diffs applied to `snp/` from `cmtDiff/`
    private final ConcurrentHashMap<String, Integer> snpDiffCount = new ConcurrentHashMap<>();
    // stateDiffCount at the time the backup last switched live containers (blue/green).
    private final ConcurrentHashMap<String, Integer> liveDiffCount = new ConcurrentHashMap<>();

    /** Per-service single thread executor: serializes captureStateDiff() + propose(). */
    private final ConcurrentHashMap<String, ExecutorService> captureExecutors =
            new ConcurrentHashMap<>();

    /** Flags used to stop the coordinator-status background poller per service. */
    private final ConcurrentHashMap<String, AtomicBoolean> coordinatorPollerStopFlags =
            new ConcurrentHashMap<>();

    /** Flags used to stop the backup blue/green poller per service. */
    private final ConcurrentHashMap<String, AtomicBoolean> backupPollerStopFlags =
            new ConcurrentHashMap<>();

    // TODO: thread pool for coordinator polling / backup polling background
    //  threads. Using a simple cached pool here as a placeholder.
    private final ExecutorService backgroundThreadPool = Executors.newCachedThreadPool();

    public PrimaryBackupManager(NodeIDType myNodeID,
                                Stringifiable<NodeIDType> unstringer,
                                XdnApp app,
                                PaxosManager<NodeIDType> paxosManager,
                                Messenger<NodeIDType, JSONObject> messenger) {
        this.myNodeID = myNodeID;
        this.unstringer = unstringer;
        this.app = app;
        this.paxosManager = paxosManager;
        this.messenger = messenger;
    }

    // =========================================================================
    // Upon createReplicaGroup(serviceName, epoch, initialState, nodes, placementMetadata)
    // =========================================================================

    public boolean createReplicaGroup(String serviceName, int epoch, String initialState,
                                      Set<NodeIDType> nodes, String placementMetadata) {

        boolean serviceAlreadyExists = currentRole.containsKey(serviceName);
        if (!serviceAlreadyExists) {
            startPollingForCoordinatorStatus(serviceName);
        }

        if (epoch < currPlacement.getOrDefault(serviceName, Integer.MIN_VALUE)) {
            return true;
        }

        paxosManager.createPaxosInstanceForcibly(
                serviceName,
                epoch,
                nodes,
                app,
                initialState,  // passes "xdn:init:..." or "xdn:final:..." directly
                // XdnApp.restore() routes these to NonDeterministicService.restore()
                // which calls createServiceInstance() (no container started yet)
                0L             // timeout — clamped internally to PC.CAN_CREATE_TIMEOUT
        );
        replicaGroups.put(serviceName, nodes);

        NodeIDType preferredCoordinator = null;
        if (placementMetadata != null) {
            try {
                JSONObject json = new JSONObject(placementMetadata);
                String preferredCoordinatorNodeId = json.getString(
                        AbstractDemandProfile.Keys.PREFERRED_COORDINATOR.toString());
                preferredCoordinator = unstringer.valueOf(preferredCoordinatorNodeId);
            } catch (JSONException e) {
                logger.log(Level.WARNING,
                        "{0}:PrimaryBackupManager failed to parse preferred coordinator " +
                                "in placement metadata: {1}",
                        new Object[]{myNodeID, e});
            }
        }

        if (preferredCoordinator == null || !preferredCoordinator.equals(myNodeID)) {
            return true;
        }

        int attempt = 0;
        int attemptLimit = 10; // TODO: move to config
        while (++attempt <= attemptLimit) {
            if (myNodeID.equals(paxosManager.getPaxosCoordinator(serviceName))) {
                break;
            }
            paxosManager.tryToBePaxosCoordinator(serviceName);
            // TODO: sleep between attempts (e.g. Thread.sleep) -- omitted for
            //  now since this whole method is allowed to block per design
            //  discussion, but a backoff is still needed here.
        }

        return true;
    }

    // =========================================================================
    // Upon startPollingForCoordinatorStatus(serviceName) -- background thread
    // =========================================================================

    private void startPollingForCoordinatorStatus(String serviceName) {
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        coordinatorPollerStopFlags.put(serviceName, stopFlag);

        currentRole.put(serviceName, Role.BACKUP);

        backgroundThreadPool.submit(() -> {
            while (!stopFlag.get()) {
                NodeIDType coordinator = paxosManager.getPaxosCoordinator(serviceName);
                Integer placement = paxosManager.getVersion(serviceName);

                if (coordinator == null || placement == null) {
                    sleepQuietly(1000); // TODO: tune poll interval
                    continue;
                }

                Integer currentPlacementVal = currPlacement.get(serviceName);
                int nextPrimaryEpoch;
                if (currentPlacementVal == null || placement > currentPlacementVal) {
                    nextPrimaryEpoch = 0;
                } else if (placement.equals(currentPlacementVal)) {

                    if (myNodeID.equals(currPrimaryID.get(serviceName))) {
                        continue;
                    }

                    nextPrimaryEpoch = currPlacementEpoch.getOrDefault(serviceName, -1) + 1;
                } else {
                    sleepQuietly(1000);
                    continue;
                }

                if (myNodeID.equals(coordinator)) {
                    currentRole.put(serviceName, Role.PRIMARY_CANDIDATE);
                    StartEpochPacket startPacket = new StartEpochPacket(
                            serviceName, placement, nextPrimaryEpoch,
                            myNodeID.toString());
                    paxosManager.propose(serviceName, startPacket, (executedRequest, handled) -> {
                        // Success
                    });
                }

                sleepQuietly(1000); // TODO: tune poll interval
            }
        });
    }

    // =========================================================================
    // Upon Notify(StartEpochPacket packet)
    // =========================================================================

    private boolean handleStartEpochPacket(StartEpochPacket packet) {
        String serviceName = packet.getServiceName();
        logger.log(Level.WARNING, "{0}:PBM handleStartEpochPacket fired for {1} packet={2}",
                new Object[]{myNodeID, serviceName, packet.getNextPrimaryID()});

        boolean isNewService = currPlacement.get(serviceName) == null
                || currPrimaryID.get(serviceName) == null
                || currPlacementEpoch.get(serviceName) == null;
        Integer curPlacementVal = currPlacement.get(serviceName);
        boolean isNewPlacement = curPlacementVal == null
                || packet.getNextPlacement() > curPlacementVal;

        NodeIDType oldPrimaryID = currPrimaryID.get(serviceName);
        boolean isOldPrimary = myNodeID.equals(oldPrimaryID);

        NodeIDType nextPrimaryID = unstringer.valueOf(packet.getNextPrimaryID());
        boolean isNewPrimary = myNodeID.equals(nextPrimaryID);

        boolean isNewRoleTheSame = (isOldPrimary && isNewPrimary)
                || (!isOldPrimary && !isNewPrimary);

        logger.log(Level.WARNING,
                "{0}:PBM handleStartEpochPacket svc={1} nextPrimary={2} isNewService={3} " +
                        "isNewPlacement={4} isOldPrimary={5} isNewPrimary={6} " +
                        "packetPlacement={7} isNewRoleTheSame={8} currentRole={9} " +
                        "currPlacement={10} currPrimaryID={11} currPlacementEpoch={12}",
                new Object[]{myNodeID, serviceName, packet.getNextPrimaryID(),
                        isNewService, isNewPlacement, isOldPrimary, isNewPrimary,
                        packet.getNextPlacement(), isNewRoleTheSame, currentRole.get(serviceName),
                        currPlacement.get(serviceName), currPrimaryID.get(serviceName),
                        currPlacementEpoch.get(serviceName)});


        if (isNewService || isNewPlacement) {
            currPlacement.put(serviceName, packet.getNextPlacement());
            currPrimaryID.put(serviceName, nextPrimaryID);
            currPlacementEpoch.put(serviceName, packet.getNextPrimaryEpoch());
            cmtDiffCount.put(serviceName, -1);
            snpDiffCount.put(serviceName, -1);
            snpDiffApplyLocks.put(serviceName, new ReentrantLock());
            AtomicBoolean stopFlag = new AtomicBoolean(false);
            snpDiffApplyStopFlags.put(serviceName, stopFlag);
            startSnpDiffApplyThread(serviceName, stopFlag);
        } else if (packet.getNextPlacement() == curPlacementVal) {
            currPrimaryID.put(serviceName, nextPrimaryID);
            currPlacementEpoch.put(serviceName, packet.getNextPrimaryEpoch());
        }

        if (isNewRoleTheSame) {
            // Even if role is the same, a backup with eventual consistency
            // needs to start its container if it hasn't done so yet
            if (!isNewPrimary
                    && isEventualConsistency(serviceName)
                    && currentLiveDirType.get(serviceName) == null
                    && backupPollerStopFlags.get(serviceName) == null) {
                initializeBackupContainer(serviceName);
            }
            return true;
        }

        stopOldContainer(serviceName); // Blocking

        if (isNewPrimary) {
            currentRole.put(serviceName, Role.PRIMARY);
            initializePrimaryContainer(serviceName); // Blocking
        } else if (isEventualConsistency(serviceName)) {
            initializeBackupContainer(serviceName); // Runs in background
        }

        return true;
    }

    // =========================================================================
    // Upon initializePrimaryContainer(serviceName)
    // =========================================================================

    private void initializePrimaryContainer(String serviceName) {
        // Starts container + recorder via NonDeterministicService.startContainerAsPrimary()
        this.app.restore(serviceName, ServiceProperty.NON_DETERMINISTIC_START_PRIMARY_PREFIX);

        // TODO: stateful container healthcheck is already done inside startService()
        //  before non-stateful containers start. This waitUntilReady() call is therefore
        //  redundant for the stateful container — only the non-stateful containers
        //  actually need to be waited on here. Refactor waitUntilReady() to skip
        //  already-healthy containers, or split into stateful/non-stateful variants.
        //  NOTE: Also consider XdnApp.waitUntilReady
        boolean ready = this.app.waitUntilReady(serviceName);
        if (!ready) {
            logger.log(Level.SEVERE,
                    "{0}:PrimaryBackupManager initializePrimaryContainer() " +
                            "container not ready for {1}",
                    new Object[]{myNodeID, serviceName});
            return;
        }

        // Initialize stateDiffCount for this service
        int bootstrapCount = 0;

        // Capture bootstrap diff
        byte[] diff = this.app.captureStatediff(serviceName);
        logger.log(Level.WARNING,
                "{0}:PBM initializePrimaryContainer captureStatediff result={1} bytes for {2}",
                new Object[]{myNodeID, diff == null ? "null" : diff.length, serviceName});
        if (diff == null) diff = new byte[0];

        logger.log(Level.WARNING,
                "{0}:PBM initializePrimaryContainer proposing ApplyStateDiff count={1} size={2} for {3}",
                new Object[]{myNodeID, bootstrapCount, diff.length, serviceName});

        proposeStateDiff(
                serviceName,
                currPlacement.get(serviceName),
                currPlacementEpoch.get(serviceName),
                bootstrapCount,
                diff,
                (executedRequest, handled) -> logger.log(Level.WARNING,
                        "{0}:PBM initializePrimaryContainer bootstrap diff committed handled={1} for {2}",
                        new Object[]{myNodeID, handled, serviceName}));
    }

    // =========================================================================
    // Upon Notify(ApplyStateDiffPacket packet)
    // =========================================================================

    private boolean handleApplyStateDiffPacket(ApplyStateDiffPacket packet) {
        String serviceName = packet.getServiceName();
        logger.log(Level.INFO,
                "{0}:PBM handleApplyStateDiffPacket checks: " +
                        "packet.placement={1} currPlacement={2} " +
                        "packet.primaryID={3} currPrimaryID={4} " +
                        "packet.primaryEpoch={5} currPlacementEpoch={6}",
                new Object[]{myNodeID,
                        packet.getPlacement(), currPlacement.getOrDefault(serviceName, -1),
                        packet.getPrimaryID(), currPrimaryID.get(serviceName),
                        packet.getPrimaryEpoch(), currPlacementEpoch.get(serviceName)});

        if (packet.getPlacement() != currPlacement.getOrDefault(serviceName, -1)) {
            logger.log(Level.SEVERE, "{0}:PBM handleApplyStateDiffPacket placement mismatch for {1}",
                    new Object[]{myNodeID, serviceName});
            throw new IllegalStateException("placement mismatch for " + serviceName);
        }

        NodeIDType packetPrimaryID = unstringer.valueOf(packet.getPrimaryID());
        if (!packetPrimaryID.equals(currPrimaryID.get(serviceName))) {
            logger.log(Level.SEVERE, "{0}:PBM handleApplyStateDiffPacket primaryID mismatch for {1}",
                    new Object[]{myNodeID, serviceName});
            throw new IllegalStateException("primaryID mismatch for " + serviceName);
        }

        Integer curEpoch = currPlacementEpoch.get(serviceName);
        if (curEpoch == null || curEpoch != packet.getPrimaryEpoch()) {
            logger.log(Level.SEVERE, "{0}:PBM handleApplyStateDiffPacket primaryEpoch mismatch for {1} " +
                            "curEpoch={2} packet.primaryEpoch={3}",
                    new Object[]{myNodeID, serviceName, curEpoch, packet.getPrimaryEpoch()});
            throw new IllegalStateException("primaryEpoch mismatch for " + serviceName);
        }

        int currentCount = cmtDiffCount.getOrDefault(serviceName, 0);
        int difference = packet.getStateDiffCount() - currentCount;

        if (difference == 1) {
            boolean applied;
            if (!packet.isLargeDiff()) {
                // Small diff - apply inline bytes
                applied = app.saveStatediff(serviceName, packet.getStateDiff(), packet.getDiffFilename());
            } else {
                // Large diff - mv prpDiff/ -> cmtDiff/
                String filename = packet.getDiffFilename();
                String prpPath = app.getPrpDiffFilePath(serviceName, filename);

                // Wait for file to appear (scp may still be in flight on non-primary nodes)
                int maxWaitMs = 30_000;
                int waitedMs = 0;
                while (!new File(prpPath).exists() && waitedMs < maxWaitMs) {
                    sleepQuietly(500);
                    waitedMs += 500;
                }

                if (!new File(prpPath).exists()) {
                    logger.log(Level.SEVERE,
                            "{0}:PBM handleApplyStateDiffPacket large diff file not found " +
                                    "after {1}ms: {2} for {3}",
                            new Object[]{myNodeID, maxWaitMs, prpPath, serviceName});
                    applied = false;
                } else {
                    boolean moved = app.movePrpDiffToCmtDiff(serviceName, filename);
                    if (!moved) {
                        logger.log(Level.SEVERE,
                                "{0}:PBM handleApplyStateDiffPacket failed to mv {1} for {2}",
                                new Object[]{myNodeID, filename, serviceName});
                        applied = false;
                    }

                    // File already in cmtDiff/ after mv - no further action needed here.
                    // applySnpDiff will be called by applyCmtDiffToSnpDiff before next backup refresh.
                    applied = true;
                }
            }

            if (!applied) {
                logger.log(Level.WARNING,
                        "{0}:PBM handleApplyStateDiffPacket applyStatediff failed for {1} count={2}",
                        new Object[]{myNodeID, serviceName, packet.getStateDiffCount()});
            }
            cmtDiffCount.put(serviceName, packet.getStateDiffCount());
        } else if (difference > 1) {
            // Deliberate per design: any gap triggers this node to attempt
            // to become the new primary itself, rather than resyncing.
            currentRole.put(serviceName, Role.PRIMARY_CANDIDATE);
            int nextEpoch = currPlacementEpoch.getOrDefault(serviceName, 0) + 1;
            StartEpochPacket startPacket = new StartEpochPacket(
                    serviceName, currPlacement.get(serviceName), nextEpoch,
                    myNodeID.toString());
            paxosManager.propose(serviceName, startPacket, (executedRequest, handled) -> {
                // Propose succeed
            });
        }
        // difference <= 0: duplicate or stale diff -- deliberate no-op for now.

        return true;
    }

    // =========================================================================
    // Upon stopOldContainer(serviceName)
    // =========================================================================

    private void stopOldContainer(String serviceName) {
        boolean isOldPrimary = myNodeID.equals(currPrimaryID.get(serviceName));

        if (isOldPrimary) {
            // TODO: stop stateDiffRecorder
            // TODO: stop primary container via SandboxManager.stopService(...)
            // TODO: clear "primaryLive/"
        } else if (isEventualConsistency(serviceName)) {
            AtomicBoolean backupStopFlag = backupPollerStopFlags.get(serviceName);
            if (backupStopFlag != null) {
                backupStopFlag.set(true);
            }
            // TODO: stop backupContainer
            // TODO: clear "backupLive1/" and "backupLive2/"
        }
    }

    // =========================================================================
    // Upon initializeBackupContainer(serviceName) -- blue/green poller
    // =========================================================================

    private void initializeBackupContainer(String serviceName) {
        AtomicBoolean stopFlag = new AtomicBoolean(false);
        backupPollerStopFlags.put(serviceName, stopFlag);

        backgroundThreadPool.submit(() -> {
            AbstractStateDiffRecorder.LiveDirType currentLiveType = null;
            long lastSwitchTimeMs = 0;

            while (!stopFlag.get()) {
                long now = System.currentTimeMillis();
                if (lastSwitchTimeMs != 0 && now - lastSwitchTimeMs < 30_000) {
                    sleepQuietly(1000);
                    continue;
                }

                // Decide next live type
                AbstractStateDiffRecorder.LiveDirType nextLiveType =
                        (currentLiveType == null ||
                                currentLiveType == AbstractStateDiffRecorder.LiveDirType.BACKUP2)
                                ? AbstractStateDiffRecorder.LiveDirType.BACKUP1
                                : AbstractStateDiffRecorder.LiveDirType.BACKUP2;

                String nextLivePrefix =
                        nextLiveType == AbstractStateDiffRecorder.LiveDirType.BACKUP1
                                ? ServiceProperty.NON_DETERMINISTIC_START_BACKUP1_PREFIX
                                : ServiceProperty.NON_DETERMINISTIC_START_BACKUP2_PREFIX;

                ReentrantLock snpLock = snpDiffApplyLocks.get(serviceName);
                if (snpLock == null) {
                    // TODO: stop snpDiffApplyThread properly - not yet implemented
                    throw new IllegalStateException(
                            "snpDiffApplyLock is null for " + serviceName);
                }

                snpLock.lock();
                int currentSnpDiffCount;
                boolean started;
                try {
                    currentSnpDiffCount = snpDiffCount.getOrDefault(serviceName, -1);
                    logger.log(Level.WARNING,
                            "{0}:PBM initializeBackupContainer starting {1} " +
                                    "at snpDiffCount={2} for {3}",
                            new Object[]{myNodeID, nextLiveType, currentSnpDiffCount, serviceName});
                    started = this.app.restore(serviceName, nextLivePrefix);
                    liveDiffCount.put(serviceName, currentSnpDiffCount);
                } finally {
                    snpLock.unlock();
                }

                if (!started) {
                    logger.log(Level.SEVERE,
                            "{0}:PBM initializeBackupContainer failed to start {1} for {2}",
                            new Object[]{myNodeID, nextLiveType, serviceName});
                    // Do NOT update lastSwitchTimeMs
                    app.stopBackupContainer(serviceName, nextLiveType);
                    sleepQuietly(5000);
                    continue;
                }

                // Wait for new container to be healthy
                boolean ready = this.app.waitUntilReady(serviceName);
                if (!ready) {
                    logger.log(Level.SEVERE,
                            "{0}:PBM initializeBackupContainer container failed healthcheck {1} for {2}",
                            new Object[]{myNodeID, nextLiveType, serviceName});
                    app.stopBackupContainer(serviceName, nextLiveType);
                    sleepQuietly(5000);
                    continue;
                }

                // Reroute: update currentLiveDirType BEFORE liveDiffCount
                // liveDiffCount already set inside the lock above
                currentLiveDirType.put(serviceName, nextLiveType);

                logger.log(Level.WARNING,
                        "{0}:PBM initializeBackupContainer switched to {1} " +
                                "liveDiffCount={2} for {3} — container healthy and serving requests",
                        new Object[]{myNodeID, nextLiveType, currentSnpDiffCount, serviceName});

                // Record switchover to log file
                String switchoverLog = app.getServiceBaseDir(serviceName) + "switchovers.log";
                String logLine = System.currentTimeMillis() + " "
                        + myNodeID.toString() + " "
                        + nextLiveType.name().toLowerCase() + " "
                        + currentSnpDiffCount + "\n";
                try {
                    java.nio.file.Files.writeString(
                            java.nio.file.Path.of(switchoverLog),
                            logLine,
                            java.nio.file.StandardOpenOption.CREATE,
                            java.nio.file.StandardOpenOption.APPEND);
                } catch (java.io.IOException e) {
                    logger.log(Level.WARNING,
                            "{0}:PBM initializeBackupContainer failed to write " +
                                    "switchover log for {1}: {2}",
                            new Object[]{myNodeID, serviceName, e.getMessage()});
                }

                // Stop old container (skip on first iteration)
                if (currentLiveType != null) {
                    logger.log(Level.WARNING,
                            "{0}:PBM initializeBackupContainer stopping old {1} for {2}",
                            new Object[]{myNodeID, currentLiveType, serviceName});
                    this.app.stopBackupContainer(serviceName, currentLiveType);
                }

                // Update tracking — only on success
                currentLiveType = nextLiveType;
                lastSwitchTimeMs = now;
            }
        });
    }

    // =========================================================================
    // Upon receiving request({clientHeader})
    // =========================================================================

    public boolean handleClientRequest(String serviceName, Request request,
                                       Integer clientStateDiffCount,
                                       boolean isWriteRequest,
                                       ExecutedCallback callback) {

        if (!currentRole.containsKey(serviceName)) {
            if (request instanceof XdnHttpRequest xdnHttpRequest) {
                ByteBuf content = Unpooled.copiedBuffer(
                        ("Service not found: " + serviceName).getBytes());
                FullHttpResponse response =
                        new DefaultFullHttpResponse(
                                HttpVersion.HTTP_1_1,
                                HttpResponseStatus.NOT_FOUND,
                                content);
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
                xdnHttpRequest.setHttpResponse(response);
                callback.executed(xdnHttpRequest, true);
            }

            return true;
        }

        Role role = currentRole.get(serviceName);
        logger.log(Level.INFO, "{0}:PBM handleClientRequest serviceName={1} role={2}",
                new Object[]{myNodeID, serviceName, role});


        if (role == Role.PRIMARY_CANDIDATE) {
            if (request instanceof XdnHttpRequest xdnHttpRequest) {
                ByteBuf content = Unpooled.copiedBuffer(
                        "Service unavailable: primary election in progress".getBytes());
                FullHttpResponse response =
                        new DefaultFullHttpResponse(
                                HttpVersion.HTTP_1_1,
                                HttpResponseStatus.SERVICE_UNAVAILABLE,
                                content);
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
                xdnHttpRequest.setHttpResponse(response);
                callback.executed(xdnHttpRequest, true);
            }
            return true;
        } else if (role == Role.PRIMARY) {
            logger.log(Level.INFO, "{0}:PBM handleClientRequest PRIMARY executing request for {1}",
                    new Object[]{myNodeID, serviceName});

            app.execute(request);

            if (!isWriteRequest) {
                callback.executed(request, true);
                return true;
            }

            logger.log(Level.INFO, "{0}:PBM handleClientRequest PRIMARY executed, capturing diff for {1}",
                    new Object[]{myNodeID, serviceName});

            byte[] diff = app.captureStatediff(serviceName);
            if (diff == null) diff = new byte[0];

            logger.log(Level.INFO, "{0}:PBM handleClientRequest PRIMARY diff captured size={2} for {1}",
                    new Object[]{myNodeID, serviceName, diff.length});

            int nextCount = cmtDiffCount.getOrDefault(serviceName, 0) + 1;
            // Do NOT update stateDiffCount here n let handleApplyStateDiffPacket
            // update it after Paxos commits, uniformly on all nodes including primary.
            int pEpoch = currPlacementEpoch.get(serviceName);
            int placement = currPlacement.get(serviceName);
            logger.log(Level.FINE,
                    "{0}:PBM handleClientRequest PRIMARY proposing ApplyStateDiff count={2} for {1}",
                    new Object[]{myNodeID, serviceName, nextCount});

            proposeStateDiff(serviceName, placement, pEpoch, nextCount, diff,
                    (executedRequest, handled) -> {
                        logger.log(Level.FINE,
                                "{0}:PBM handleClientRequest PRIMARY propose callback handled={2} for {1}",
                                new Object[]{myNodeID, serviceName, handled});
                        callback.executed(request, handled);
                    });
            return true;

        } else if (role == Role.BACKUP) {
            if (!isEventualConsistency(serviceName) || isWriteRequest) {
                return forwardRequestToPrimary(serviceName, request, callback);
            }

            Integer liveCount = liveDiffCount.get(serviceName);
            if (liveCount == null
                    || (clientStateDiffCount != null && clientStateDiffCount > liveCount)) {
                return forwardRequestToPrimary(serviceName, request, callback);
            }

            AbstractStateDiffRecorder.LiveDirType liveType = currentLiveDirType.get(serviceName);
            if (liveType == null) {
                if (request instanceof XdnHttpRequest xdnHttpRequest) {
                    ByteBuf content = Unpooled.copiedBuffer(
                            "Service unavailable: backup container not yet ready".getBytes());
                    FullHttpResponse response = new DefaultFullHttpResponse(
                            HttpVersion.HTTP_1_1,
                            HttpResponseStatus.SERVICE_UNAVAILABLE,
                            content);
                    response.headers().setInt(
                            HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
                    xdnHttpRequest.setHttpResponse(response);
                    callback.executed(xdnHttpRequest, true);
                }
                return true;
            }

            Integer backupPort = app.getActiveBackupPort(serviceName, liveType);
            if (backupPort == null) {
                logger.log(Level.SEVERE,
                        "{0}:PBM handleClientRequest backup port not found for {1} type={2}",
                        new Object[]{myNodeID, serviceName, liveType});
                return forwardRequestToPrimary(serviceName, request, callback);
            }

            return app.forwardToBackupContainer(serviceName, backupPort, request, callback);
        }

        throw new IllegalStateException(
                "Unhandled role " + role + " for service " + serviceName);
    }

    // =========================================================================
    // Upon startSnpDiffApplyThread(serviceName)
    // =========================================================================

    private void startSnpDiffApplyThread(String serviceName, AtomicBoolean stopFlag) {
        String stateDiffDir = app.getStateDiffDir(serviceName);
        backgroundThreadPool.submit(() -> {
            while (!stopFlag.get()) {
                int nextCount = snpDiffCount.getOrDefault(serviceName, -1) + 1;

                // Scan cmtDiff/ for file matching *:<nextCount>.diff
                if (stateDiffDir == null) {
                    sleepQuietly(100);
                    continue;
                }

                File dir = new File(stateDiffDir);
                final int count = nextCount;
                File[] matches = dir.listFiles((d, name) ->
                        name.endsWith(":" + count + ".diff"));

                if (matches == null || matches.length == 0) {
                    sleepQuietly(100);
                    continue;
                }

                String filename = matches[0].getName();

                ReentrantLock lock = snpDiffApplyLocks.get(serviceName);
                if (lock == null) {
                    sleepQuietly(100);
                    continue;
                }

                lock.lock();
                try {
                    logger.log(Level.FINEST,
                            "{0}:PBM snpDiffApplyThread applying count={1} file={2} for {3}",
                            new Object[]{myNodeID, nextCount, filename, serviceName});

                    boolean applied = app.applySnpDiff(serviceName, filename);
                    if (!applied) {
                        logger.log(Level.SEVERE,
                                "{0}:PBM snpDiffApplyThread failed to apply count={1} for {2}",
                                new Object[]{myNodeID, nextCount, serviceName});
                    } else {
                        snpDiffCount.put(serviceName, nextCount);
                        logger.log(Level.WARNING,
                                "{0}:PBM snpDiffApplyThread applied count={1} for {2} " +
                                        "snpDiffCount={3}",
                                new Object[]{myNodeID, nextCount, serviceName, nextCount});
                    }
                } finally {
                    lock.unlock();
                }
            }

            logger.log(Level.WARNING,
                    "{0}:PBM snpDiffApplyThread stopped for {1}",
                    new Object[]{myNodeID, serviceName});
        });
    }

    // =========================================================================
    // Dispatch entry point -- routes committed packets to the right handler.
    // Called from XdnApp.execute() for Paxos-delivered PrimaryBackupPackets.
    // =========================================================================

    public boolean handlePrimaryBackupPacket(PrimaryBackupPacket packet,
                                             ExecutedCallback callback) {
        if (packet instanceof StartEpochPacket startEpochPacket) {
            return handleStartEpochPacket(startEpochPacket);
        }
        if (packet instanceof ApplyStateDiffPacket applyStateDiffPacket) {
            return handleApplyStateDiffPacket(applyStateDiffPacket);
        }
        if (packet instanceof ForwardedRequestPacket forwardedRequestPacket) {
            return handleForwardedRequestPacket(forwardedRequestPacket);
        }
        if (packet instanceof ResponsePacket responsePacket) {
            return handleResponsePacket(responsePacket);
        }

        throw new RuntimeException(
                "Unhandled PrimaryBackupPacket type: " + packet.getClass().getSimpleName());
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    private void proposeStateDiff(String serviceName, int placement, int pEpoch,
                                  int count, byte[] diff, ExecutedCallback callback) {
        String filename = "p" + pEpoch + ":" + myNodeID.toString() + ":" + count + ".diff";
        ApplyStateDiffPacket applyPacket;

        if (diff.length <= 500 * 1024) {
            // Small diff — propose inline
            applyPacket = new ApplyStateDiffPacket(
                    serviceName, placement, pEpoch, myNodeID.toString(), count, diff);
        } else {
            // Large diff — write to prpDiff/, scp to backups, propose with isLargeDiff=true
            boolean written = app.writeToPrpDiff(serviceName, filename, diff);
            if (!written) {
                logger.log(Level.SEVERE,
                        "{0}:PBM proposeStateDiff failed to write large diff for {1}",
                        new Object[]{myNodeID, serviceName});
                if (callback != null) callback.executed(null, false);
                return;
            }

            String localPath = app.getPrpDiffFilePath(serviceName, filename);
            boolean scpOk = scpToBackups(serviceName, localPath, filename);
            if (!scpOk) {
                logger.log(Level.SEVERE,
                        "{0}:PBM proposeStateDiff scp failed for {1} file={2}",
                        new Object[]{myNodeID, serviceName, filename});
                if (callback != null) callback.executed(null, false);
                return;
            }

            applyPacket = new ApplyStateDiffPacket(
                    serviceName, placement, pEpoch, myNodeID.toString(), count);
        }

        paxosManager.propose(serviceName, applyPacket, (executedRequest, handled) -> {
            if (callback != null) callback.executed(executedRequest, handled);
        });
    }

    private boolean isEventualConsistency(String serviceName) {
        ServiceInstance instance = this.app.getServiceInstance(serviceName);
        if (instance == null) return false;
        return instance.property.getConsistencyModel().equals(ConsistencyModel.EVENTUAL);
    }

    private boolean forwardRequestToPrimary(String serviceName, Request request,
                                            ExecutedCallback callback) {
        NodeIDType primaryID = currPrimaryID.get(serviceName);
        if (primaryID == null) {
            logger.log(Level.WARNING,
                    "{0}:PBM forwardRequestToPrimary unknown primary for {1}",
                    new Object[]{myNodeID, serviceName});
            return false;
        }

        byte[] encodedRequest = request.toString().getBytes(StandardCharsets.ISO_8859_1);
        ForwardedRequestPacket forwardPacket = new ForwardedRequestPacket(
                serviceName, myNodeID.toString(), encodedRequest);

        forwardedRequests.put(forwardPacket.getRequestID(),
                new RequestAndCallback(request, callback));

        logger.log(Level.INFO,
                "{0}:PBM forwardRequestToPrimary forwarding to {1} for {2}",
                new Object[]{myNodeID, primaryID, serviceName});

        try {
            messenger.send(new GenericMessagingTask<>(primaryID, forwardPacket));
        } catch (IOException | JSONException e) {
            forwardedRequests.remove(forwardPacket.getRequestID());
            throw new RuntimeException(e);
        }
        return true;
    }

    private boolean handleForwardedRequestPacket(ForwardedRequestPacket packet) {
        String serviceName = packet.getServiceName();

        logger.log(Level.INFO,
                "{0}:PBM handleForwardedRequestPacket from {1} for {2}",
                new Object[]{myNodeID, packet.getEntryNodeId(), serviceName});

        String encodedRequestStr = new String(
                packet.getEncodedForwardedRequest(), StandardCharsets.ISO_8859_1);

        Request request;
        try {
            request = app.getRequest(encodedRequestStr);
        } catch (RequestParseException e) {
            throw new RuntimeException(e);
        }

        if (request == null) {
            logger.log(Level.WARNING,
                    "{0}:PBM handleForwardedRequestPacket failed to parse request for {1}",
                    new Object[]{myNodeID, serviceName});
            return false;
        }

        NodeIDType entryNodeID = unstringer.valueOf(packet.getEntryNodeId());
        long originalRequestId = packet.getRequestID();

        boolean isWriteRequest = false;
        if (request instanceof XdnHttpRequest xdnHttpRequest) {
            HttpMethod method = xdnHttpRequest.getHttpRequest().method();
            isWriteRequest = method.equals(HttpMethod.POST)
                    || method.equals(HttpMethod.PUT)
                    || method.equals(HttpMethod.DELETE)
                    || method.equals(HttpMethod.PATCH);
        }

        return handleClientRequest(serviceName, request, null, isWriteRequest,
                (executedRequest, handled) -> {
                    byte[] encodedResponse = executedRequest.toString()
                            .getBytes(StandardCharsets.ISO_8859_1);

                    if (executedRequest instanceof XdnHttpRequest xhr) {
                        encodedResponse = xhr.toBytes(true);
                    }

                    ResponsePacket responsePacket = new ResponsePacket(
                            serviceName, originalRequestId, encodedResponse);

                    try {
                        messenger.send(new GenericMessagingTask<>(entryNodeID, responsePacket));
                    } catch (IOException | JSONException e) {
                        throw new RuntimeException(e);
                    }
                });
    }

    private boolean handleResponsePacket(ResponsePacket packet) {
        logger.log(Level.INFO,
                "{0}:PBM handleResponsePacket for {1} requestId={2}",
                new Object[]{myNodeID, packet.getServiceName(), packet.getRequestID()});

        RequestAndCallback rc = forwardedRequests.remove(packet.getRequestID());
        if (rc == null) {
            logger.log(Level.WARNING,
                    "{0}:PBM handleResponsePacket unknown requestId={1} for {2}",
                    new Object[]{myNodeID, packet.getRequestID(), packet.getServiceName()});
            return false;
        }

        String encodedResponseStr = new String(
                packet.getEncodedResponse(), StandardCharsets.ISO_8859_1);

        Request response;
        try {
            response = app.getRequest(encodedResponseStr);
            if (response instanceof edu.umass.cs.xdn2.request.XdnHttpRequest) {
                response = edu.umass.cs.xdn2.request.XdnHttpRequest
                        .createFromString(encodedResponseStr);
            }
        } catch (RequestParseException e) {
            throw new RuntimeException(e);
        }

        if (response == null) {
            logger.log(Level.WARNING,
                    "{0}:PBM handleResponsePacket failed to parse response for {1}",
                    new Object[]{myNodeID, packet.getServiceName()});
            return false;
        }

        rc.callback().executed(response, true);
        return true;
    }

    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private boolean scpToBackups(String serviceName, String localPath, String filename) {
        Set<NodeIDType> nodes = replicaGroups.get(serviceName);
        if (nodes == null) return true;

        String sshKey = edu.umass.cs.utils.Config.getGlobalString(
                edu.umass.cs.gigapaxos.PaxosConfig.PC.SSH_KEY_PATH);

        // Cast unstringer to NodeConfig to get IP addresses
        @SuppressWarnings("unchecked")
        NodeConfig<NodeIDType> nodeConfig = (NodeConfig<NodeIDType>) unstringer;

        // Run scp to all backup nodes in parallel
        java.util.List<java.util.concurrent.Future<Boolean>> futures = new java.util.ArrayList<>();
        java.util.concurrent.ExecutorService scpPool =
                java.util.concurrent.Executors.newFixedThreadPool(nodes.size());

        for (NodeIDType node : nodes) {
            if (node.equals(myNodeID)) continue; // skip self

            java.net.InetAddress addr = nodeConfig.getNodeAddress(node);
            String ip = addr.getHostAddress();
            String destPath = app.getPrpDiffFilePath(node.toString(), serviceName, filename);
            String destDir = destPath.substring(0, destPath.lastIndexOf('/') + 1);

            futures.add(scpPool.submit(() -> {
                String cmd;
                if (ip.equals("127.0.0.1") || ip.equals("localhost")) {
                    // Same machine — use cp
                    Shell.runCommand("mkdir -p " + destDir);
                    cmd = String.format("cp %s %s", localPath, destPath);
                } else {
                    // Remote machine — use scp
                    String scpOpts = (sshKey != null && !sshKey.isBlank())
                            ? "-i " + sshKey + " -o StrictHostKeyChecking=no"
                            : "-o StrictHostKeyChecking=no";
                    cmd = String.format("scp %s %s %s:%s", scpOpts, localPath, ip, destPath);
                }
                logger.log(Level.WARNING,
                        "{0}:PBM scpToBackups cmd={1}",
                        new Object[]{myNodeID, cmd});
                int code = Shell.runCommand(cmd);
                if (code != 0) {
                    logger.log(Level.SEVERE,
                            "{0}:PBM scpToBackups failed exit={1} cmd={2}",
                            new Object[]{myNodeID, code, cmd});
                }
                return code == 0;
            }));
        }

        scpPool.shutdown();
        boolean allOk = true;
        for (java.util.concurrent.Future<Boolean> f : futures) {
            try {
                if (!f.get()) allOk = false;
            } catch (Exception e) {
                logger.log(Level.SEVERE,
                        "{0}:PBM scpToBackups exception: {1}",
                        new Object[]{myNodeID, e.getMessage()});
                allOk = false;
            }
        }
        return allOk;
    }

    // =========================================================================
    // Public Methods
    // =========================================================================

    public Set<NodeIDType> getReplicaGroup(String serviceName) {
        return replicaGroups.get(serviceName);
    }

    public boolean isPrimary(String serviceName) {
        return Role.PRIMARY.equals(currentRole.get(serviceName));
    }

    // =========================================================================
    // PrimaryBackupMiddlewareApp — Replicable wrapper passed to PaxosManager.
    // Intercepts committed PrimaryBackupPackets and routes them to the manager.
    // Everything else is passed through to XdnApp.
    // =========================================================================
    public static class PrimaryBackupMiddlewareApp implements Replicable {
        private final XdnApp xdnApp;
        private PrimaryBackupManager<?> manager;
        private final Logger logger =
                Logger.getLogger(PrimaryBackupManager.class.getSimpleName());

        public PrimaryBackupMiddlewareApp(XdnApp xdnApp) {
            this.xdnApp = xdnApp;
        }

        public void setManager(PrimaryBackupManager<?> manager) {
            this.manager = manager;
        }

        @Override
        public boolean execute(Request request) {
            logger.log(Level.WARNING, "PBM MiddlewareApp.execute() request={0}",
                    new Object[]{request.getClass().getSimpleName()});
            return execute(request, true);
        }

        @Override
        public boolean execute(Request request, boolean doNotReplyToClient) {
            if (request == null) return true;
            assert manager != null : "setManager() must be called before execute()";

            if (request instanceof PrimaryBackupPacket packet) {
                return manager.handlePrimaryBackupPacket(packet, null);
            }

            return xdnApp.execute(request, doNotReplyToClient);
        }

        @Override
        public Request getRequest(String stringified) throws RequestParseException {
            if (stringified == null || stringified.isEmpty()) return null;
            PrimaryBackupPacketType packetType = PrimaryBackupPacket.getQuickPacketTypeFromEncodedPacket(
                    stringified.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
            if (packetType != null) {
                return PrimaryBackupPacket.createFromBytes(stringified.getBytes(
                        java.nio.charset.StandardCharsets.ISO_8859_1));
            }
            return xdnApp.getRequest(stringified);
        }

        @Override
        public Set<IntegerPacketType> getRequestTypes() {
            Set<IntegerPacketType> types = new HashSet<>(xdnApp.getRequestTypes());
            types.addAll(java.util.Arrays.asList(PrimaryBackupPacketType.values()));
            return types;
        }

        @Override
        public String checkpoint(String name) {
            return xdnApp.checkpoint(name);
        }

        @Override
        public boolean restore(String name, String state) {
            return xdnApp.restore(name, state);
        }
    }

    // =========================================================================
    // Paxos configuration required for primary-backup correctness.
    // =========================================================================
    public static void setupPaxosConfiguration() {
        String[] args = {
                String.format("%s=%b", PaxosConfig.PC.ENABLE_EMBEDDED_STORE_SHUTDOWN, true),
                String.format("%s=%b", PaxosConfig.PC.ENABLE_STARTUP_LEADER_ELECTION, false),
                String.format("%s=%b", PaxosConfig.PC.FORWARD_PREEMPTED_REQUESTS, false),
                String.format("%s=%d", PaxosConfig.PC.PACKET_DEMULTIPLEXER_THREADS, 0),
                String.format("%s=%b", PaxosConfig.PC.HIBERNATE_OPTION, false),
                String.format("%s=%b", PaxosConfig.PC.BATCHING_ENABLED, true),
        };
        Config.register(args);
    }

}