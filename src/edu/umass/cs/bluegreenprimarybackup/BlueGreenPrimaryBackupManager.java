package edu.umass.cs.bluegreenprimarybackup;

import edu.umass.cs.bluegreenprimarybackup.packets.*;
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
import edu.umass.cs.xdn.XdnApp;
import edu.umass.cs.xdn.recorder.AbstractStateDiffRecorder;
import edu.umass.cs.xdn.recorder.AbstractStateDiffRecorder.LiveDirType;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import edu.umass.cs.xdn.service.ConsistencyModel;
import edu.umass.cs.xdn.service.ServiceInstance;
import edu.umass.cs.xdn.service.ServiceProperty;
import edu.umass.cs.xdn.utils.Shell;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * BlueGreenPrimaryBackupManager implements the xdn primary-backup event/action protocol designed
 * across the prior conversation. This is a from-scratch implementation, independent of
 * edu.umass.cs.primarybackup.PrimaryBackupManager.
 *
 * <p>This is a BOILERPLATE / starting point. Event handler bodies follow the pseudocode exactly as
 * designed, with container/recorder integration left as TODO stubs pointing at SandboxManager /
 * AbstractStateDiffRecorder, since the exact ServiceInstance construction wasn't traced in this
 * pass.
 *
 * <p>Known deliberate gaps (per design discussion, left for the fuzzer/tester to surface before
 * hardening): - Any single missed/reordered ApplyStateDiff aggressively triggers this node to
 * attempt to become primary (no distinction between "primary is actually dead" and "one packet got
 * dropped"). - No drain-before-stop when switching blue/green backup containers. - No explicit
 * handling of duplicate/stale (difference &lt;= 0) state diffs beyond falling through as a no-op. -
 * Possible unsynchronized race between createReplicaGroup()'s own coordinator-nudge retry loop and
 * the background startPollingForCoordinatorStatus() poller, both of which may call
 * tryToBePaxosCoordinator()/getPaxosCoordinator() concurrently.
 *
 * @param <NodeIDType> the type used to identify nodes in the system.
 */
public class BlueGreenPrimaryBackupManager<NodeIDType> {

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
  private final ConcurrentHashMap<String, AbstractStateDiffRecorder.LiveDirType>
      currentLiveDirType = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ReentrantLock> snpDiffApplyLocks =
      new ConcurrentHashMap<>();

  private record RequestAndCallback(
      Request request, ExecutedCallback callback, long forwardStartNanos, boolean isWriteRequest) {}

  private final Logger logger =
      Logger.getLogger(BlueGreenPrimaryBackupManager.class.getSimpleName());

  // -------------------------------------------------------------------------
  // Per-service protocol state, keyed by serviceName.
  // -------------------------------------------------------------------------
  /** Keep track of the nodes in the replica group */
  private final ConcurrentHashMap<String, Set<NodeIDType>> replicaGroups =
      new ConcurrentHashMap<>();

  /** Placement epoch currently known for this service (set of nodes hosting it). */
  private final ConcurrentHashMap<String, Integer> currPlacement = new ConcurrentHashMap<>();

  /** Node ID of the current primary for this service. */
  private final ConcurrentHashMap<String, NodeIDType> currPrimaryID = new ConcurrentHashMap<>();

  /** This node's current role for the service. */
  private final ConcurrentHashMap<String, Role> currentRole = new ConcurrentHashMap<>();

  /** Epoch of the current primary within the current placement. */
  private final ConcurrentHashMap<String, Integer> currPrimaryEpoch = new ConcurrentHashMap<>();

  // Count of state diffs commited so far, used for gap detection.
  private final ConcurrentHashMap<String, Integer> cmtStateDiffCount = new ConcurrentHashMap<>();
  // Count of state diffs applied to `snp/` from `cmtDiff/`
  private final ConcurrentHashMap<String, Integer> snpStateDiffCount = new ConcurrentHashMap<>();
  // stateDiffCount at the time the backup last switched live containers (blue/green).
  private final ConcurrentHashMap<String, Integer> liveStateDiffCount = new ConcurrentHashMap<>();

  // -------------------------------------------------------------------------
  // Event action protocol state
  // -------------------------------------------------------------------------

  private final ConcurrentHashMap<String, BlockingQueue<Object>> eventQueues =
          new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Thread> dispatcherThreads = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, BlockingQueue<SnpApplyItem>> snpApplyQueues =
          new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Future<?>> snpApplyLoops = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, BlockingQueue<PendingWrite>> doneQueues =
          new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, Future<?>> captureLoops = new ConcurrentHashMap<>();

  private final ConcurrentHashMap<String, ScheduledFuture<?>> coordinatorStatusPollers =
          new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ScheduledFuture<?>> refreshTimers =
          new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ExecutorService> refreshBackupExecutors =
          new ConcurrentHashMap<>();

  private final ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(2);

  {
    scheduler.setRemoveOnCancelPolicy(true);
  }

  private static final class CreateReplicaGroupEvent<N> {
    final int placement;
    final String initialState;
    final Set<N> nodes;
    final String placementMetadata;
    final CompletableFuture<Boolean> result = new CompletableFuture<>();

    CreateReplicaGroupEvent(
            int placement, String initialState, Set<N> nodes, String placementMetadata) {
      this.placement = placement;
      this.initialState = initialState;
      this.nodes = nodes;
      this.placementMetadata = placementMetadata;
    }
  }

  private static final class CoordinatorStatusPolledEvent<N> {
    final N coordinator;
    final Integer placement;

    CoordinatorStatusPolledEvent(N coordinator, Integer placement) {
      this.coordinator = coordinator;
      this.placement = placement;
    }
  }

  private static final class RefreshBackupStartEvent {
    final LiveDirType liveDir;
    int stateDiffCount; // set by the dispatcher, under the snp apply lock

    RefreshBackupStartEvent(LiveDirType liveDir) {
      this.liveDir = liveDir;
    }
  }

  private static final class RefreshBackupCompleteEvent {
    final int stateDiffCount;
    final LiveDirType liveDir;

    RefreshBackupCompleteEvent(int stateDiffCount, LiveDirType liveDir) {
      this.stateDiffCount = stateDiffCount;
      this.liveDir = liveDir;
    }
  }

  /** The executor could not bring up the refreshed backup. The dispatcher retries later. */
  static final class RefreshBackupFailedEvent {
    final LiveDirType liveDir;

    RefreshBackupFailedEvent(LiveDirType liveDir) {
      this.liveDir = liveDir;
    }
  }

  private record DiffCommittedEvent(
          int placement, int pEpoch, int count, boolean handled, ExecutedCallback callback) {}

  private record PendingWrite(Request request, ExecutedCallback callback, int placement, int pEpoch) {
    boolean sameEpochAs(PendingWrite p) {
      return (placement == p.placement) && (pEpoch == p.pEpoch);
    }
  }

  private record SnpApplyItem(String filename, int count, boolean isLargeDiff, byte[] diff) {}

  private ConsistencyModel getConsistencyModel(String serviceName) {
    ServiceInstance instance = this.app.getServiceInstance(serviceName);
    if (instance == null) throw new RuntimeException(serviceName + " instance does not exist");
    return instance.property.getConsistencyModel();
  }

  // LINEARIZABLE and LINEARIZABILITY both mean linearizable.
  private boolean isLinearizable(String serviceName) {
    ConsistencyModel model = getConsistencyModel(serviceName);
    return model == ConsistencyModel.LINEARIZABLE || model == ConsistencyModel.LINEARIZABILITY;
  }

  // -------------------------------------------------------------------------
  // Dispatcher loop. One thread per service, started by createReplicaGroup().
  // -------------------------------------------------------------------------

  private void runDispatcherLoop(String serviceName, BlockingQueue<Object> queue) {
    // Initialize the replica's service state
    currPlacement.put(serviceName, -1);
    currPrimaryEpoch.put(serviceName, -1);
    cmtStateDiffCount.put(serviceName, -1);
    snpStateDiffCount.put(serviceName, -1);
    liveStateDiffCount.put(serviceName, -1);
    currentRole.put(serviceName, Role.BACKUP);

    startPollingForCoordinatorStatus(serviceName);

    while (true) {
      Object event;
      try {
        event = queue.take(); // Blocks until an event enters the queue
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }

      try {
        if (event instanceof CreateReplicaGroupEvent<?> e) {
          boolean success = handleCreateReplicaGroupEvent(serviceName, e);
          e.result.complete(success);
        } else if (event instanceof CoordinatorStatusPolledEvent<?> e) {
          handleCoordinatorStatusPolledEvent(serviceName, e);
        } else if (event instanceof StartEpochPacket p) {
          // Blocking. Stops and starts containers.
          handleStartEpochPacket(serviceName, p);
        } else if (event instanceof ApplyStateDiffPacket p) {
          handleApplyStateDiffPacket(serviceName, p);
        } else if (event instanceof RefreshBackupStartEvent e) {
          Role roleNow = currentRole.get(serviceName);
          if (roleNow == Role.PRIMARY_CANDIDATE) {
            // Might lose the race and stay a backup. Keep the timer alive and look again later.
            scheduleRefresh(serviceName, e.liveDir);
            continue;
          }
          if (roleNow != Role.BACKUP) {
            continue;
          }

          ReentrantLock lock = snpDiffApplyLocks.get(serviceName);
          lock.lock();
          try {
            // Read the count and copy snp/ under one lock, so an apply cannot slip in between
            e.stateDiffCount = snpStateDiffCount.get(serviceName);
            if (currentLiveDirType.get(serviceName) != null
                    && e.stateDiffCount <= liveStateDiffCount.get(serviceName)) {
              // Nothing new. Try again later.
              scheduleRefresh(serviceName, e.liveDir);
              continue;
            }
            this.app.initializeLiveDirectory(serviceName, e.liveDir);
          } finally {
            lock.unlock();
          }
          // Non-blocking. Hands the slow part to the refresh executor.
          handleRefreshBackupStartEvent(serviceName, e);
        } else if (event instanceof RefreshBackupCompleteEvent e) {
          handleRefreshBackupCompleteEvent(serviceName, e);
        } else if (event instanceof RefreshBackupFailedEvent e) {
          handleRefreshBackupFailedEvent(serviceName, e);
        } else if (event instanceof DiffCommittedEvent e) {
          handleDiffCommittedEvent(serviceName, e);
        } else {
          throw new RuntimeException(
                  "Unknown Dispatcher Event: " + event.getClass().getSimpleName());
        }
      } catch (Throwable t) {
        logger.log(Level.SEVERE, myNodeID + ":PBM event failed for " + serviceName, t);
        if (event instanceof CreateReplicaGroupEvent<?> e) {
          e.result.complete(false);
        } else if (event instanceof RefreshBackupStartEvent e) {
          scheduleRefresh(serviceName, e.liveDir);
        }
      }
    }
  }

  // -------------------------------------------------------------------------
  // Dispatcher handlers
  // -------------------------------------------------------------------------

  private boolean handleCreateReplicaGroupEvent(
          String serviceName, CreateReplicaGroupEvent<?> event) {
    // Early return for stale placement epochs
    if (event.placement < currPlacement.get(serviceName)) {
      return true;
    }

    @SuppressWarnings("unchecked")
    Set<NodeIDType> nodes = (Set<NodeIDType>) event.nodes;

    // Passes "xdn:init..." or "xdn:final..." to createServiceInstance(). No container started yet.
    paxosManager.createPaxosInstanceForcibly(
            serviceName, event.placement, nodes, app, event.initialState, 0L);
    replicaGroups.put(serviceName, nodes);

    // If the client names a preferred node as the primary, *try* to make that node the paxos
    // coordinator (the coordinator automatically becomes the primary).
    NodeIDType preferredCoordinator = null;
    if (event.placementMetadata != null) {
      try {
        JSONObject json = new JSONObject(event.placementMetadata);
        String preferredCoordinatorNodeId =
                json.getString(AbstractDemandProfile.Keys.PREFERRED_COORDINATOR.toString());
        preferredCoordinator = unstringer.valueOf(preferredCoordinatorNodeId);
      } catch (JSONException e) {
        logger.log(
                Level.WARNING,
                "{0}:PBM failed to parse preferred coordinator in placement metadata: {1}",
                new Object[] {myNodeID, e});
      }
    }

    if (preferredCoordinator != null && preferredCoordinator.equals(myNodeID)) {
      paxosManager.tryToBePaxosCoordinator(serviceName);
    }

    return true;
  }

  private boolean handleCoordinatorStatusPolledEvent(
          String serviceName, CoordinatorStatusPolledEvent<?> event) {
    if (event.coordinator == null || event.placement == null) {
      return true;
    }

    // StartEpochPacket is only proposed by the node designated as the primary
    if (!myNodeID.equals(event.coordinator)) {
      return true;
    }

    int curPlacement = currPlacement.get(serviceName);
    if (event.placement < curPlacement) {
      return true; // stale
    }

    int nextPrimaryEpoch;
    if (curPlacement < event.placement) {
      nextPrimaryEpoch = 0;
    } else {
      if (myNodeID.equals(currPrimaryID.get(serviceName))) {
        return true;
      }
      nextPrimaryEpoch = currPrimaryEpoch.get(serviceName) + 1;
    }

    currentRole.put(serviceName, Role.PRIMARY_CANDIDATE);
    StartEpochPacket packet =
            new StartEpochPacket(serviceName, event.placement, nextPrimaryEpoch, myNodeID.toString());
    paxosManager.propose(serviceName, packet, (r, handled) -> {});
    return true;
  }

  private boolean handleApplyStateDiffPacket(String serviceName, ApplyStateDiffPacket p) {
    // A diff from an old placement or an old primary epoch is stale. Drop it.
    if (p.getPlacement() != currPlacement.get(serviceName)
            || p.getPrimaryEpoch() != currPrimaryEpoch.get(serviceName)) {
      logger.log(
              Level.WARNING,
              "{0}:PBM dropping stale diff for {1}",
              new Object[] {myNodeID, serviceName});
      return true;
    }

    int count = p.getStateDiffCount();
    int difference = count - cmtStateDiffCount.get(serviceName);
    if (difference == 1) {
      snpApplyQueues
              .get(serviceName)
              .add(new SnpApplyItem(p.getDiffFilename(), count, p.isLargeDiff(), p.getStateDiff()));
      cmtStateDiffCount.put(serviceName, count);
    } else if (difference > 1) {
      // A diff was lost. Try to become the primary. One proposal is enough while one is pending.
      if (currentRole.get(serviceName) != Role.PRIMARY_CANDIDATE) {
        currentRole.put(serviceName, Role.PRIMARY_CANDIDATE);
        paxosManager.propose(
                serviceName,
                new StartEpochPacket(
                        serviceName,
                        currPlacement.get(serviceName),
                        currPrimaryEpoch.get(serviceName) + 1,
                        myNodeID.toString()),
                (r, handled) -> {});
      }
    }
    // difference <= 0 is a duplicate, ignore it.
    return true;
  }

  // -------------------------------------------------------------------------
  // Apply thread. Applies committed diffs to snp/ in order, one per service.
  // -------------------------------------------------------------------------

  private static final int LARGE_DIFF_MAX_WAIT_MS = 30_000;

  private void startApplyThread(String serviceName) {
    stopApplyThread(serviceName);
    BlockingQueue<SnpApplyItem> queue = new LinkedBlockingQueue<>();
    snpApplyQueues.put(serviceName, queue);
    snpApplyLoops.put(
            serviceName, backgroundThreadPool.submit(() -> applyLoop(serviceName, queue)));
  }

  private void stopApplyThread(String serviceName) {
    Future<?> loop = snpApplyLoops.remove(serviceName);
    if (loop != null) loop.cancel(true);
    snpApplyQueues.remove(serviceName);
  }

  private void applyLoop(String serviceName, BlockingQueue<SnpApplyItem> queue) {
    try {
      while (true) {
        SnpApplyItem item = queue.take(); // sleeps until a committed diff arrives
        try {
          // 1. Put the diff file into the stateDiff directory.
          if (!item.isLargeDiff()) {
            if (!app.saveStatediff(serviceName, item.diff(), item.filename())) {
              throw new IllegalStateException("failed to save " + item.filename());
            }
          } else {
            // The scp from the primary may still be in flight on this node.
            String prpPath = app.getPrpDiffFilePath(serviceName, item.filename());
            for (int waited = 0; !new File(prpPath).exists(); waited += 500) {
              if (waited >= LARGE_DIFF_MAX_WAIT_MS) {
                throw new IllegalStateException("large diff not found " + prpPath);
              }
              Thread.sleep(500);
            }
            if (!app.movePrpDiffToCmtDiff(serviceName, item.filename())) {
              throw new IllegalStateException("failed to move " + item.filename());
            }
          }

          // 2. Apply it to snp/ under the lock, then publish the new count.
          ReentrantLock lock = snpDiffApplyLocks.get(serviceName);
          lock.lock();
          try {
            if (!app.applySnpDiff(serviceName, item.filename())) {
              throw new IllegalStateException("failed to apply " + item.filename());
            }
            snpStateDiffCount.put(serviceName, item.count());
          } finally {
            lock.unlock();
          }

          // 3. The diff is in snp/ now, so the file is no longer needed.
          try {
            app.deleteStateDiff(serviceName, item.filename());
          } catch (Throwable t) {
            logger.log(
                    Level.WARNING, myNodeID + ":PBM could not delete " + item.filename(), t);
          }
        } catch (Throwable t) {
          if (Thread.currentThread().isInterrupted()
                  || t instanceof InterruptedException
                  || t.getCause() instanceof InterruptedException) {
            throw new InterruptedException(); // Shell wraps the interrupt and clears the flag
          }
          // A hole in snp/ means later diffs cannot be applied on top of it. Fail the process.
          logger.log(
                  Level.SEVERE,
                  myNodeID + ":PBM snp apply failed for " + serviceName + ", exiting",
                  t);
          System.exit(1);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  private static final long BACKUP_REFRESH_INTERVAL_MS = 30_000;

  /** Starts the refreshed backup container off the dispatcher thread. */
  private void handleRefreshBackupStartEvent(String serviceName, RefreshBackupStartEvent event) {
    refreshBackupExecutors
            .computeIfAbsent(serviceName, k -> Executors.newSingleThreadExecutor())
            .submit(
                    () -> {
                      try {
                        String startPrefix =
                                (event.liveDir == LiveDirType.BACKUP1)
                                        ? ServiceProperty.NON_DETERMINISTIC_START_BACKUP1_PREFIX
                                        : ServiceProperty.NON_DETERMINISTIC_START_BACKUP2_PREFIX;

                        if (!this.app.restore(serviceName, startPrefix)) {
                          throw new IllegalStateException("restore failed for " + event.liveDir);
                        }
                        if (!this.app.waitUntilReady(serviceName, event.liveDir)) {
                          throw new IllegalStateException("backup not ready for " + event.liveDir);
                        }
                        eventQueues
                                .get(serviceName)
                                .add(new RefreshBackupCompleteEvent(event.stateDiffCount, event.liveDir));
                      } catch (Throwable t) {
                        logger.log(
                                Level.SEVERE,
                                myNodeID + ":backup refresh failed for " + serviceName + " " + event.liveDir,
                                t);
                        BlockingQueue<Object> queue = eventQueues.get(serviceName);
                        if (queue != null) {
                          queue.add(new RefreshBackupFailedEvent(event.liveDir));
                        }
                      }
                    });
  }

  private void handleRefreshBackupCompleteEvent(
          String serviceName, RefreshBackupCompleteEvent event) {
    Role role = currentRole.get(serviceName);
    if (role != Role.BACKUP) {
      // The role changed while the refresh was starting. Remove the container it just started.
      this.app.stopBackupContainer(serviceName, event.liveDir);
      if (role == Role.PRIMARY_CANDIDATE) {
        scheduleRefresh(serviceName, event.liveDir); // may stay a backup, so keep the timer alive
      }
      return;
    }

    LiveDirType prevLiveDir = currentLiveDirType.get(serviceName);
    LiveDirType currLiveDir = event.liveDir;
    LiveDirType nextLiveDir =
            (currLiveDir == LiveDirType.BACKUP1) ? LiveDirType.BACKUP2 : LiveDirType.BACKUP1;

    // Update currentLiveDirType before liveStateDiffCount is trusted by readers
    currentLiveDirType.put(serviceName, currLiveDir);
    liveStateDiffCount.put(serviceName, event.stateDiffCount);
    logSwitchover(serviceName, currLiveDir, event.stateDiffCount);
    if (prevLiveDir != null) {
      this.app.stopBackupContainer(serviceName, prevLiveDir);
    }

    scheduleRefresh(serviceName, nextLiveDir);
  }

  /** Appends one line per backup switch to switchovers.log. A failed write is only a warning. */
  private void logSwitchover(String serviceName, LiveDirType liveDir, int snpStateDiffCount) {
    String switchoverLog = app.getServiceBaseDir(serviceName) + "switchovers.log";
    String logLine =
            System.currentTimeMillis()
                    + " "
                    + myNodeID.toString()
                    + " "
                    + liveDir.name().toLowerCase()
                    + " "
                    + snpStateDiffCount
                    + "\n";
    try {
      Files.writeString(
              java.nio.file.Path.of(switchoverLog),
              logLine,
              java.nio.file.StandardOpenOption.CREATE,
              java.nio.file.StandardOpenOption.APPEND);
    } catch (IOException e) {
      logger.log(
              Level.WARNING,
              "{0}:PBM failed to write switchover log for {1}: {2}",
              new Object[] {myNodeID, serviceName, e.getMessage()});
    }
  }

  private void handleRefreshBackupFailedEvent(String serviceName, RefreshBackupFailedEvent event) {
    // Remove whatever the failed start left behind. This is not the live container.
    try {
      this.app.stopBackupContainer(serviceName, event.liveDir);
    } catch (Throwable t) {
      logger.log(Level.WARNING, myNodeID + ":could not stop failed backup " + event.liveDir, t);
    }

    Role role = currentRole.get(serviceName);
    if (role == Role.BACKUP || role == Role.PRIMARY_CANDIDATE) {
      scheduleRefresh(serviceName, event.liveDir); // retry on the normal timer
    }
  }

  /** Must be called from the dispatcher thread. A new timer replaces the previous one. */
  private void scheduleRefresh(String serviceName, LiveDirType liveDir) {
    ScheduledFuture<?> next =
            scheduler.schedule(
                    () -> {
                      BlockingQueue<Object> queue = eventQueues.get(serviceName);
                      if (queue != null) {
                        queue.add(new RefreshBackupStartEvent(liveDir));
                      }
                    },
                    BACKUP_REFRESH_INTERVAL_MS,
                    TimeUnit.MILLISECONDS);

    ScheduledFuture<?> old = refreshTimers.put(serviceName, next);
    if (old != null) {
      old.cancel(false);
    }
  }

  private static final long CAPTURE_POLL_TIMEOUT_MS = 100;
  private static final int MAX_INLINE_DIFF_BYTES = 500 * 1024;

  /** One callback that answers every client in the group. The bootstrap entry has no callback. */
  private ExecutedCallback release(Collection<PendingWrite> writes) {
    return (ignored, handled) ->
            writes.forEach(
                    w -> {
                      if (w.callback() == null) return;
                      try {
                        w.callback().executed(w.request(), handled);
                      } catch (Throwable t) {
                        logger.log(Level.WARNING, myNodeID + ":PBM callback failed for one client", t);
                      }
                    });
  }

  private void startCaptureLoop(String serviceName, int placement, int pEpoch) {
    stopCaptureLoop(serviceName);
    BlockingQueue<PendingWrite> queue =
            doneQueues.computeIfAbsent(serviceName, k -> new LinkedBlockingQueue<>());
    // Bootstrap entry. Forces the first diff to be proposed even when it is empty.
    queue.add(new PendingWrite(null, null, placement, pEpoch));
    captureLoops.put(
            serviceName,
            backgroundThreadPool.submit(() -> captureLoop(serviceName, placement, pEpoch, queue)));
  }

  private void stopCaptureLoop(String serviceName) {
    Future<?> loop = captureLoops.remove(serviceName);
    if (loop != null) loop.cancel(true);
    BlockingQueue<PendingWrite> queue = doneQueues.get(serviceName);
    if (queue != null) {
      List<PendingWrite> left = new ArrayList<>();
      queue.drainTo(left);
      release(left).executed(null, false); // no longer primary, these writes are not replicated
    }
  }

  private void captureLoop(
          String serviceName, int placement, int pEpoch, BlockingQueue<PendingWrite> queue) {
    Deque<PendingWrite> pending = new ArrayDeque<>();
    int count = cmtStateDiffCount.get(serviceName) + 1;
    try {
      while (!Thread.currentThread().isInterrupted()) {
        // Sleeps here until a write completes or 100 ms pass.
        if (pending.isEmpty()) {
          PendingWrite first = queue.poll(CAPTURE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
          if (first != null) pending.add(first);
        }
        queue.drainTo(pending);

        // One batch holds the writes of one (placement, pEpoch). The rest wait for the next round.
        List<PendingWrite> batch = new ArrayList<>();
        while (!pending.isEmpty()
                && (batch.isEmpty() || batch.get(0).sameEpochAs(pending.peek()))) {
          batch.add(pending.poll());
        }
        int bPlacement = batch.isEmpty() ? placement : batch.get(0).placement();
        int bPEpoch = batch.isEmpty() ? pEpoch : batch.get(0).pEpoch();

        try {
          // An empty batch is the idle capture. It proposes only when the diff is not empty.
          if (captureAndProposeStateDiff(
                  serviceName, bPlacement, bPEpoch, count, release(batch), batch.isEmpty())) {
            count++;
          }
        } catch (Throwable t) {
          // The capture may have drained the diff already, so burn the count. Backups see the gap.
          eventQueues
                  .get(serviceName)
                  .add(new DiffCommittedEvent(bPlacement, bPEpoch, count++, false, release(batch)));
          if (t instanceof InterruptedException || t.getCause() instanceof InterruptedException) {
            throw new InterruptedException(); // Shell wraps the interrupt and clears the flag
          }
          logger.log(Level.SEVERE, myNodeID + ":PBM capture failed for " + serviceName, t);
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    } finally {
      release(pending).executed(null, false);
    }
  }

  /** Blocking. Returns false when skipIfEmpty is set and there was nothing to propose. */
  private boolean captureAndProposeStateDiff(
          String serviceName,
          int placement,
          int pEpoch,
          int stateDiffCount,
          ExecutedCallback callback,
          boolean skipIfEmpty)
          throws Exception {
    byte[] diff = this.app.captureStatediff(serviceName);
    byte[] finalDiff = (diff == null) ? new byte[0] : diff;
    if (skipIfEmpty && finalDiff.length == 0) return false;

    ApplyStateDiffPacket packet;
    if (finalDiff.length <= MAX_INLINE_DIFF_BYTES) {
      packet =
              new ApplyStateDiffPacket(
                      serviceName, placement, pEpoch, myNodeID.toString(), stateDiffCount, finalDiff);
    } else {
      String filename = "p" + pEpoch + ":" + myNodeID + ":" + stateDiffCount + ".diff";
      if (!app.writeToPrpDiff(serviceName, filename, finalDiff)) {
        throw new IllegalStateException("failed to write large diff " + filename);
      }

      String localPath = app.getPrpDiffFilePath(serviceName, filename);
      if (!scpToBackups(serviceName, localPath, filename)) {
        throw new IllegalStateException("scp failed for " + filename);
      }

      packet =
              new ApplyStateDiffPacket(
                      serviceName, placement, pEpoch, myNodeID.toString(), stateDiffCount);
    }

    paxosManager.propose(
            serviceName,
            packet,
            (executedRequest, handled) ->
                    eventQueues
                            .get(serviceName)
                            .add(new DiffCommittedEvent(placement, pEpoch, stateDiffCount, handled, callback)));
    return true;
  }

  private void handleDiffCommittedEvent(String serviceName, DiffCommittedEvent event) {
    // Paxos may have committed the diff while handleApplyStateDiffPacket dropped it
    // (stale epoch or a gap). Only a diff that was kept counts as replicated.
    boolean applied =
            event.placement() == currPlacement.get(serviceName)
                    && event.pEpoch() == currPrimaryEpoch.get(serviceName)
                    && event.count() <= cmtStateDiffCount.get(serviceName);
    boolean handled = event.handled() && applied;
    if (event.handled() && !applied) {
      logger.log(
              Level.WARNING,
              "{0}:PBM committed diff was not applied for {1} placement={2} pEpoch={3} count={4},"
                      + " answering its writes as not handled",
              new Object[] {
                      myNodeID, serviceName, event.placement(), event.pEpoch(), event.count()
              });
    }
    if (event.callback() != null) {
      event.callback().executed(null, handled);
    }
  }

  private static final long SNP_CATCH_UP_TIMEOUT_MS = 60_000;

  private boolean handleStartEpochPacket(String serviceName, StartEpochPacket p) {
    boolean isNewService =
            (currPlacement.get(serviceName) == -1)
                    || (currPrimaryEpoch.get(serviceName) == -1)
                    || (currPrimaryID.get(serviceName) == null);
    boolean isNewPlacement = currPlacement.get(serviceName) < p.getNextPlacement();
    NodeIDType nextPrimaryID = unstringer.valueOf(p.getNextPrimaryID());
    boolean isOldPrimary = myNodeID.equals(currPrimaryID.get(serviceName));
    boolean isNewPrimary = myNodeID.equals(nextPrimaryID);

    if (isNewService || isNewPlacement) {
      // New service or new placement epoch
      currPlacement.put(serviceName, p.getNextPlacement());
      currPrimaryID.put(serviceName, nextPrimaryID);
      currPrimaryEpoch.put(serviceName, p.getNextPrimaryEpoch());
      cmtStateDiffCount.put(serviceName, -1);
      snpStateDiffCount.put(serviceName, -1);
      liveStateDiffCount.put(serviceName, -1);
      snpDiffApplyLocks.put(serviceName, new ReentrantLock());
      startApplyThread(serviceName); // a fresh queue and thread for this placement
    } else {
      // Same placement. Only a higher primary epoch changes anything.
      if (p.getNextPlacement() < currPlacement.get(serviceName)) {
        return true;
      }

      // Lower is stale, equal is a duplicate or a lost race, and the first one committed wins
      if (p.getNextPrimaryEpoch() <= currPrimaryEpoch.get(serviceName)) {
        return true;
      }

      currPrimaryID.put(serviceName, nextPrimaryID);
      currPrimaryEpoch.put(serviceName, p.getNextPrimaryEpoch());

      if (!isOldPrimary && !isNewPrimary) {
        // backup -> backup
        // A lost StartEpoch race leaves the role at PRIMARY_CANDIDATE, so reset it.
        currentRole.put(serviceName, Role.BACKUP);
        return true;
      }

      if (isOldPrimary) {
        // primary -> backup or primary -> primary
        // The live state may hold writes that were never replicated,
        // so tear it down and restart from snp/.
        // Clients get a 503 while the role is PRIMARY_CANDIDATE.
        currentRole.put(serviceName, Role.PRIMARY_CANDIDATE);
        stopCaptureLoop(serviceName);
        stopOldContainer(serviceName, Role.PRIMARY);
      } else {
        // backup -> primary
        stopOldContainer(serviceName, Role.BACKUP);
      }

      // restore() seeds the primary from snp/, so snp/ must have every committed diff first
      if (isNewPrimary) {
        waitForSnpCatchUp(serviceName);
      }
    }

    if (isNewPrimary) {
      currentRole.put(serviceName, Role.PRIMARY_CANDIDATE);
      if (!this.app.restore(serviceName, ServiceProperty.NON_DETERMINISTIC_START_PRIMARY_PREFIX)) {
        throw new IllegalStateException("primary restore failed for " + serviceName);
      }
      if (!this.app.waitUntilReady(serviceName)) {
        throw new IllegalStateException("primary not ready for " + serviceName);
      }

      // Start background thread to captureStateDiff
      startCaptureLoop(serviceName, p.getNextPlacement(), p.getNextPrimaryEpoch());
      currentRole.put(serviceName, Role.PRIMARY); // only now do clients reach the container
    } else {
      currentRole.put(serviceName, Role.BACKUP);
      currentLiveDirType.remove(serviceName); // no live container yet, reads get a 503 until one is ready

      if (isLinearizable(serviceName)) {
        return true; // linearizable backups never serve reads, so they need no container
      }

      // Start backup container
      ReentrantLock lock = snpDiffApplyLocks.get(serviceName);
      lock.lock();
      try {
        this.app.initializeLiveDirectory(serviceName, LiveDirType.BACKUP1);
        liveStateDiffCount.put(serviceName, snpStateDiffCount.get(serviceName)); // read together with the copy
      } finally {
        lock.unlock();
      }

      boolean started =
              this.app.restore(serviceName, ServiceProperty.NON_DETERMINISTIC_START_BACKUP1_PREFIX)
                      && this.app.waitUntilReady(serviceName, LiveDirType.BACKUP1);
      if (!started) {
        logger.log(
                Level.SEVERE,
                "{0}:PBM first backup container failed for {1}, retrying on the refresh timer",
                new Object[] {myNodeID, serviceName});
        this.app.stopBackupContainer(serviceName, LiveDirType.BACKUP1);
        liveStateDiffCount.put(serviceName, -1);
        scheduleRefresh(serviceName, LiveDirType.BACKUP1);
        return true;
      }
      currentLiveDirType.put(serviceName, LiveDirType.BACKUP1);

      // Schedule container refresh
      scheduleRefresh(serviceName, LiveDirType.BACKUP2);
    }
    return true;
  }

  private void waitForSnpCatchUp(String serviceName) {
    long deadline = System.currentTimeMillis() + SNP_CATCH_UP_TIMEOUT_MS;
    while (snpStateDiffCount.get(serviceName) < cmtStateDiffCount.get(serviceName)) {
      if (System.currentTimeMillis() > deadline) {
        throw new IllegalStateException(
                "snp/ did not catch up to the committed diffs for " + serviceName);
      }
      try {
        Thread.sleep(50);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IllegalStateException(e);
      }
    }
  }

  private void stopOldContainer(String serviceName, Role role) {
    if (role == Role.PRIMARY) {
      if (!app.stopPrimaryContainer(serviceName)) {
        throw new IllegalStateException("failed to stop the primary for " + serviceName);
      }
    } else if (role == Role.BACKUP) {
      boolean b1 = app.stopBackupContainer(serviceName, LiveDirType.BACKUP1);
      boolean b2 = app.stopBackupContainer(serviceName, LiveDirType.BACKUP2);
      if (!b1 || !b2) {
        throw new IllegalStateException("failed to stop the backup for " + serviceName);
      }
    } else {
      throw new RuntimeException("Role: " + role + " is not implemented");
    }
  }

  // TODO: thread pool for coordinator polling / backup polling background
  //  threads. Using a simple cached pool here as a placeholder.
  private final ExecutorService backgroundThreadPool = Executors.newCachedThreadPool();

  public BlueGreenPrimaryBackupManager(
      NodeIDType myNodeID,
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
  // createReplicaGroup(). Starts the dispatcher thread, then waits for its answer.
  // =========================================================================

  public boolean createReplicaGroup(
          String serviceName,
          int placement,
          String initialState,
          Set<NodeIDType> nodes,
          String placementMetadata) {

    // Create the event queue and the dispatcher thread for serviceName, once
    BlockingQueue<Object> queue =
            eventQueues.computeIfAbsent(serviceName, k -> new LinkedBlockingQueue<>());
    dispatcherThreads.computeIfAbsent(
            serviceName,
            k -> {
              Thread t = new Thread(() -> runDispatcherLoop(serviceName, queue));
              t.setDaemon(true);
              t.start();
              return t;
            });

    // Hand the work to the dispatcher thread
    CreateReplicaGroupEvent<NodeIDType> event =
            new CreateReplicaGroupEvent<>(placement, initialState, nodes, placementMetadata);
    queue.add(event);

    try {
      // Blocks THIS caller until the dispatcher has processed the event
      return event.result.get();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    } catch (ExecutionException e) {
      return false;
    }
  }

  // =========================================================================
  // Coordinator status poller
  // =========================================================================

  private static final long COORDINATOR_POLL_INTERVAL_MS = 1000;

  private void startPollingForCoordinatorStatus(String serviceName) {
    coordinatorStatusPollers.computeIfAbsent(
            serviceName,
            name ->
                    scheduler.scheduleWithFixedDelay(
                            () -> {
                              try {
                                NodeIDType coordinator = paxosManager.getPaxosCoordinator(name);
                                Integer placement = paxosManager.getVersion(name);
                                BlockingQueue<Object> queue = eventQueues.get(name);
                                if (queue != null) {
                                  queue.add(new CoordinatorStatusPolledEvent<>(coordinator, placement));
                                }
                              } catch (Throwable t) {
                                // Do not rethrow. An uncaught throw cancels all future runs.
                                logger.log(Level.WARNING, "poll failed for " + name, t);
                              }
                            },
                            0,
                            COORDINATOR_POLL_INTERVAL_MS,
                            TimeUnit.MILLISECONDS));
  }

  private void stopPollingForCoordinatorStatus(String serviceName) {
    ScheduledFuture<?> poller = coordinatorStatusPollers.remove(serviceName);
    if (poller != null) {
      poller.cancel(true);
    }
  }

  // =========================================================================
  // Upon receiving request({clientHeader})
  // =========================================================================

  public boolean handleClientRequest(
          String serviceName,
          Request request,
          Integer clientStateDiffCount, // unused until the cookie exists
          boolean isWriteRequest,
          ExecutedCallback callback) {

    Role role = currentRole.get(serviceName);
    if (role == null) {
      replyWithStatus(
              request,
              callback,
              HttpResponseStatus.NOT_FOUND,
              "Service not found: " + serviceName);
      return true;
    }

    if (role == Role.PRIMARY_CANDIDATE) {
      replyWithStatus(
              request,
              callback,
              HttpResponseStatus.SERVICE_UNAVAILABLE,
              "Service unavailable: primary election in progress");
      return true;
    }

    if (role == Role.PRIMARY) {
      app.execute(request);

      // Only writes need a diff. Reads under linearizable also wait for one, so that every
      // read is ordered after the writes before it.
      if (!isWriteRequest && !isLinearizable(serviceName)) {
        callback.executed(request, true);
        return true;
      }

      BlockingQueue<PendingWrite> queue = doneQueues.get(serviceName);
      Integer placement = currPlacement.get(serviceName);
      Integer pEpoch = currPrimaryEpoch.get(serviceName);
      if (queue == null || placement == null || pEpoch == null) {
        replyWithStatus(
                request,
                callback,
                HttpResponseStatus.SERVICE_UNAVAILABLE,
                "Service unavailable: primary not ready");
        return true;
      }

      PendingWrite write = new PendingWrite(request, callback, placement, pEpoch);
      queue.add(write);

      // The role may have changed after the check above. If the capture loop was already
      // stopped, nobody will answer this write, so answer it here.
      if (currentRole.get(serviceName) != Role.PRIMARY && queue.remove(write)) {
        callback.executed(request, false);
      }
      return true;
    }

    if (role == Role.BACKUP) {
      // Writes always go to the primary. Under linearizable, reads go there too.
      if (isWriteRequest || isLinearizable(serviceName)) {
        return forwardRequestToPrimary(serviceName, request, callback, isWriteRequest);
      }

      // Every other model serves reads from the live backup container
      LiveDirType liveType = currentLiveDirType.get(serviceName);
      if (liveType == null) {
        replyWithStatus(
                request,
                callback,
                HttpResponseStatus.SERVICE_UNAVAILABLE,
                "Service unavailable: backup container not yet ready");
        return true;
      }

      Integer backupPort = app.getActiveBackupPort(serviceName, liveType);
      if (backupPort == null) {
        logger.log(
                Level.SEVERE,
                "{0}:PBM handleClientRequest backup port not found for {1} type={2}",
                new Object[] {myNodeID, serviceName, liveType});
        return forwardRequestToPrimary(serviceName, request, callback, isWriteRequest);
      }

      return app.forwardToBackupContainer(serviceName, backupPort, request, callback);
    }

    throw new IllegalStateException("Unhandled role " + role + " for service " + serviceName);
  }

  /** Answers an HTTP request or a batch of them with a plain status and message. */
  private void replyWithStatus(
          Request request, ExecutedCallback callback, HttpResponseStatus status, String message) {
    if (request instanceof XdnHttpRequest xdnHttpRequest) {
      setStatusResponse(xdnHttpRequest, status, message);
      callback.executed(xdnHttpRequest, true);
      return;
    }
    if (request instanceof XdnHttpRequestBatch batch) {
      // Each request in the batch needs its own response object
      for (XdnHttpRequest inner : batch.getRequestList()) {
        setStatusResponse(inner, status, message);
      }
      callback.executed(batch, true);
    }
  }

  private void setStatusResponse(
          XdnHttpRequest request, HttpResponseStatus status, String message) {
    ByteBuf content = Unpooled.copiedBuffer(message.getBytes(StandardCharsets.UTF_8));
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
    response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, content.readableBytes());
    request.setHttpResponse(response);
  }

  /** Sets the same status response on one request or on every request of a batch. */
  private void setStatusResponseOn(
          Request request, HttpResponseStatus status, String message) {
    if (request instanceof XdnHttpRequest xdnHttpRequest) {
      setStatusResponse(xdnHttpRequest, status, message);
    } else if (request instanceof XdnHttpRequestBatch batch) {
      for (XdnHttpRequest inner : batch.getRequestList()) {
        setStatusResponse(inner, status, message);
      }
    }
  }

  /**
   * A single request is a write if its HTTP method is POST, PUT, DELETE or PATCH. A batch is a
   * write if any request inside it is a write. Anything else is not a write.
   */
  public static boolean isWriteRequest(Request request) {
    if (request instanceof XdnHttpRequest xdnHttpRequest) {
      HttpMethod method = xdnHttpRequest.getHttpRequest().method();
      return method.equals(HttpMethod.POST)
              || method.equals(HttpMethod.PUT)
              || method.equals(HttpMethod.DELETE)
              || method.equals(HttpMethod.PATCH);
    }
    if (request instanceof XdnHttpRequestBatch batch) {
      for (XdnHttpRequest inner : batch.getRequestList()) {
        if (isWriteRequest(inner)) return true;
      }
    }
    return false;
  }

  // =========================================================================
  // Dispatch entry point -- routes committed packets to the right handler.
  // Called from XdnApp.execute() for Paxos-delivered BlueGreenPrimaryBackupPackets.
  // =========================================================================

  public boolean handleBlueGreenPrimaryBackupPacket(
          PrimaryBackupPacket packet, ExecutedCallback callback) {
    if (packet instanceof StartEpochPacket || packet instanceof ApplyStateDiffPacket) {
      // Committed by Paxos. The service's dispatcher thread handles it, in commit order.
      BlockingQueue<Object> queue = eventQueues.get(packet.getServiceName());
      if (queue == null) {
        // Paxos replays its log before createReplicaGroup() runs, so there may be no queue yet.
        logger.log(
                Level.WARNING,
                "{0}:PBM dropping {1} for {2}, no event queue",
                new Object[] {myNodeID, packet.getClass().getSimpleName(), packet.getServiceName()});
        return true;
      }
      queue.add(packet);
      return true;
    }
    if (packet instanceof ForwardedRequestPacket forwardedRequestPacket) {
      return handleBlueGreenForwardedRequestPacket(forwardedRequestPacket);
    }
    if (packet instanceof ResponsePacket responsePacket) {
      return handleBlueGreenResponsePacket(responsePacket);
    }

    throw new RuntimeException(
        "Unhandled PrimaryBackupPacket type: " + packet.getClass().getSimpleName());
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  private boolean forwardRequestToPrimary(
      String serviceName, Request request, ExecutedCallback callback, boolean isWriteRequest) {
    NodeIDType primaryID = currPrimaryID.get(serviceName);
    if (primaryID == null) {
      logger.log(
          Level.WARNING,
          "{0}:PBM forwardRequestToPrimary unknown primary for {1}",
          new Object[] {myNodeID, serviceName});
      return false;
    }

    byte[] encodedRequest = request.toString().getBytes(StandardCharsets.ISO_8859_1);
    ForwardedRequestPacket forwardPacket =
        new ForwardedRequestPacket(serviceName, myNodeID.toString(), encodedRequest);

    long forwardStartNanos = System.nanoTime();
    forwardedRequests.put(
        forwardPacket.getRequestID(),
        new RequestAndCallback(request, callback, forwardStartNanos, isWriteRequest));

    logger.log(
        Level.INFO,
        "{0}:PBM forwardRequestToPrimary forwarding to {1} for {2}",
        new Object[] {myNodeID, primaryID, serviceName});

    try {
      messenger.send(new GenericMessagingTask<>(primaryID, forwardPacket));
    } catch (IOException | JSONException e) {
      forwardedRequests.remove(forwardPacket.getRequestID());
      throw new RuntimeException(e);
    }
    return true;
  }

  private boolean handleBlueGreenForwardedRequestPacket(ForwardedRequestPacket packet) {
    String serviceName = packet.getServiceName();

    logger.log(
        Level.INFO,
        "{0}:PBM handleBlueGreenForwardedRequestPacket from {1} for {2}",
        new Object[] {myNodeID, packet.getEntryNodeId(), serviceName});

    String encodedRequestStr =
        new String(packet.getEncodedForwardedRequest(), StandardCharsets.ISO_8859_1);

    Request request;
    try {
      request = app.getRequest(encodedRequestStr);
    } catch (RequestParseException e) {
      throw new RuntimeException(e);
    }

    if (request == null) {
      logger.log(
          Level.WARNING,
          "{0}:PBM handleBlueGreenForwardedRequestPacket failed to parse request for {1}",
          new Object[] {myNodeID, serviceName});
      return false;
    }

    long localReqId =
        (request instanceof XdnHttpRequest xdnHttpRequest) ? xdnHttpRequest.getRequestID() : -1L;
    logger.log(
        Level.WARNING,
        "FORWARD_MAP forwardId={0} localReqId={1} service={2}",
        new Object[] {packet.getRequestID(), localReqId, serviceName});

    NodeIDType entryNodeID = unstringer.valueOf(packet.getEntryNodeId());
    long originalRequestId = packet.getRequestID();

    boolean isWriteRequest = isWriteRequest(request);

    return handleClientRequest(
        serviceName,
        request,
        null,
        isWriteRequest,
            (executedRequest, handled) -> {
              if (!handled) {
                // The write was not replicated. Do not let the entry node answer it as a success.
                setStatusResponseOn(
                        executedRequest,
                        HttpResponseStatus.SERVICE_UNAVAILABLE,
                        "Service unavailable: write was not replicated");
              }
              byte[] encodedResponse = executedRequest.toString().getBytes(StandardCharsets.ISO_8859_1);

          if (executedRequest instanceof XdnHttpRequest xhr) {
            encodedResponse = xhr.toBytes(true);
          } else if (executedRequest instanceof XdnHttpRequestBatch batch) {
            encodedResponse = batch.toBytes(true);
          }

          ResponsePacket responsePacket =
              new ResponsePacket(serviceName, originalRequestId, encodedResponse);

          try {
            messenger.send(new GenericMessagingTask<>(entryNodeID, responsePacket));
          } catch (IOException | JSONException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private boolean handleBlueGreenResponsePacket(ResponsePacket packet) {
    logger.log(
        Level.INFO,
        "{0}:PBM handleBlueGreenResponsePacket for {1} requestId={2}",
        new Object[] {myNodeID, packet.getServiceName(), packet.getRequestID()});

    RequestAndCallback rc = forwardedRequests.remove(packet.getRequestID());
    if (rc == null) {
      logger.log(
          Level.WARNING,
          "{0}:PBM handleBlueGreenResponsePacket unknown requestId={1} for {2}",
          new Object[] {myNodeID, packet.getRequestID(), packet.getServiceName()});
      return false;
    }

    double forwardRoundTripMs = (System.nanoTime() - rc.forwardStartNanos()) / 1_000_000.0;
    logger.log(
        Level.WARNING,
        "LATENCY_FORWARD forwardId={0} service={1} isWrite={2} forwardRoundTripMs={3}",
        new Object[] {
          packet.getRequestID(), packet.getServiceName(), rc.isWriteRequest(), forwardRoundTripMs
        });

    String encodedResponseStr =
        new String(packet.getEncodedResponse(), StandardCharsets.ISO_8859_1);

    Request response;
    try {
      response = app.getRequest(encodedResponseStr);
      if (response instanceof XdnHttpRequestBatch) {
        // getRequest() may return the cached batch without the primary's responses
        response = XdnHttpRequestBatch.createFromBytes(packet.getEncodedResponse());
      } else if (response instanceof XdnHttpRequest) {
        response = XdnHttpRequest.createFromString(encodedResponseStr);
      }
    } catch (RequestParseException e) {
      throw new RuntimeException(e);
    }

    if (response == null) {
      logger.log(
          Level.WARNING,
          "{0}:PBM handleBlueGreenResponsePacket failed to parse response for {1}",
          new Object[] {myNodeID, packet.getServiceName()});
      return false;
    }

    rc.callback().executed(response, true);
    return true;
  }

  private boolean scpToBackups(String serviceName, String localPath, String filename) {
    Set<NodeIDType> nodes = replicaGroups.get(serviceName);
    if (nodes == null) return true;

    String sshKey =
        edu.umass.cs.utils.Config.getGlobalString(
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
      String destPath =
          app.getPrpDiffFilePath(node.toString().toLowerCase(), serviceName, filename);
      String destDir = destPath.substring(0, destPath.lastIndexOf('/') + 1);

      futures.add(
          scpPool.submit(
              () -> {
                String cmd;
                if (ip.equals("127.0.0.1") || ip.equals("localhost")) {
                  // Same machine: use cp
                  Shell.runCommand("mkdir -p " + destDir);
                  cmd = String.format("cp %s %s", localPath, destPath);
                } else {
                  // Remote machine: use scp
                  String scpOpts =
                      (sshKey != null && !sshKey.isBlank())
                          ? "-i " + sshKey + " -o StrictHostKeyChecking=no"
                          : "-o StrictHostKeyChecking=no";
                  String mkdirCmd = String.format("ssh %s %s mkdir -p %s", scpOpts, ip, destDir);
                  int mkdirCode = Shell.runCommand(mkdirCmd);
                  if (mkdirCode != 0) {
                    logger.log(
                        Level.WARNING,
                        "{0}:PBM scpToBackups remote mkdir failed exit={1} cmd={2}",
                        new Object[] {myNodeID, mkdirCode, mkdirCmd});
                  }
                  cmd = String.format("scp %s %s %s:%s", scpOpts, localPath, ip, destPath);
                }
                logger.log(
                    Level.WARNING, "{0}:PBM scpToBackups cmd={1}", new Object[] {myNodeID, cmd});
                int code = Shell.runCommand(cmd);
                if (code != 0) {
                  logger.log(
                      Level.SEVERE,
                      "{0}:PBM scpToBackups failed exit={1} cmd={2}",
                      new Object[] {myNodeID, code, cmd});
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
        logger.log(
            Level.SEVERE,
            "{0}:PBM scpToBackups exception: {1}",
            new Object[] {myNodeID, e.getMessage()});
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
  // Intercepts committed BlueGreenPrimaryBackupPackets and routes them to the manager.
  // Everything else is passed through to XdnApp.
  // =========================================================================
  public static class PrimaryBackupMiddlewareApp implements Replicable {
    private final XdnApp xdnApp;
    private volatile BlueGreenPrimaryBackupManager<?> manager;
    private final Logger logger =
        Logger.getLogger(BlueGreenPrimaryBackupManager.class.getSimpleName());

    public PrimaryBackupMiddlewareApp(XdnApp xdnApp) {
      this.xdnApp = xdnApp;
    }

    public void setManager(BlueGreenPrimaryBackupManager<?> manager) {
      this.manager = manager;
    }

    @Override
    public boolean execute(Request request) {
      logger.log(
          Level.WARNING,
          "PBM MiddlewareApp.execute() request={0}",
          new Object[] {request.getClass().getSimpleName()});
      return execute(request, true);
    }

    @Override
    public boolean execute(Request request, boolean doNotReplyToClient) {
      if (request == null) return true;

      if (request instanceof PrimaryBackupPacket packet) {
        // Only Blue-Green packets need the manager. Plain requests must not depend on it, because
        // gigapaxos replays its log inside the PaxosManager constructor, before the manager can
        // be wired, and a failing execute() there makes the replay drop the request.
        BlueGreenPrimaryBackupManager<?> m = manager;
        if (m == null) {
          logger.log(
                  Level.WARNING,
                  "PBM MiddlewareApp dropping {0}, manager not wired yet",
                  new Object[] {request.getClass().getSimpleName()});
          return true;
        }
        return m.handleBlueGreenPrimaryBackupPacket(packet, null);
      }

      return xdnApp.execute(request, doNotReplyToClient);
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
      if (stringified == null || stringified.isEmpty()) return null;
      PrimaryBackupPacketType packetType =
          PrimaryBackupPacket.getQuickPacketTypeFromEncodedPacket(
              stringified.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
      if (packetType != null) {
        return PrimaryBackupPacket.createFromBytes(
            stringified.getBytes(java.nio.charset.StandardCharsets.ISO_8859_1));
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
