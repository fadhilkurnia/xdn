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
import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.AbstractReconfiguratorDB;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.XdnGigapaxosApp;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import edu.umass.cs.xdn.service.ServiceProperty;
import io.netty.util.ReferenceCountUtil;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

    // Retry configuration for StartEpochPacket proposal
    // This handles the race condition where other nodes may not have created their paxos instances yet
    private static final int START_EPOCH_PROPOSAL_MAX_RETRIES = 50;
    private static final long START_EPOCH_PROPOSAL_TIMEOUT_MS = 500;
    private static final long START_EPOCH_PROPOSAL_RETRY_DELAY_MS = 200;

    private static final int PB_BATCH_SIZE = 128;
    private static final int N_PARALLEL_WORKERS =
        Integer.getInteger("PB_N_PARALLEL_WORKERS", 16);

    // After receiving the first queued request, wait up to this many milliseconds for
    // additional requests to accumulate before executing the batch.  A larger window
    // allows more requests to be combined into a single parallel-execute + single
    // Paxos proposal at the cost of a small fixed latency addition at low load.
    // Set to 0 to drain only what is already queued (original drainTo behaviour).
    // History:
    //   run9 (WordPress, rate=10): 5ms → WORSE (100ms inter-arrival, window too short).
    //   capture-thread (bookcatalog, 1ms): WORSE — peak dropped from 1445 to 1197 rps.
    //     With 8 workers competing on take(), poll(1ms) loses the race and wastes 1ms
    //     idle. Batch-size-2 increased (3.2% → 15.9%) but doesn't offset the idle cost.
    //     Accumulation windows only help with fewer workers (1-2) where queue can build.
    private static final long BATCH_ACCUMULATION_MS =
        Long.getLong("PB_BATCH_ACCUMULATION_MS", 0);

    // Accumulation window for the CAPTURE THREAD (done queue), in milliseconds.
    // After the first completed batch arrives, the capture thread waits up to this many ms
    // for additional completions before calling captureStateDiff + propose.  A larger window
    // amortizes the ~1-2ms captureStateDiff and ~12ms Paxos commit across more requests.
    // Set to 0 to disable (original take+drainTo behaviour).
    private static final long CAPTURE_ACCUMULATION_MS =
        Long.getLong("PB_CAPTURE_ACCUMULATION_MS", 0);

    // Debug flag: skip captureStateDiff + Paxos propose in the capture thread.
    // When enabled, callbacks fire immediately after workers finish execution,
    // isolating the PBM worker pipeline from the Paxos consensus overhead.
    private static final boolean SKIP_REPLICATION =
        Boolean.getBoolean("PB_SKIP_REPLICATION");

    // Maximum number of concurrent execute() calls per service.
    // Limits how many PBM workers can simultaneously execute requests against
    // the containerized service. This prevents database lock contention (e.g.,
    // PostgreSQL row-level locks on TPC-C hot rows) from causing a convoy effect
    // where increasing concurrency leads to exponentially longer exec times.
    // Set to 0 (default) to disable the limit (all workers can execute concurrently).
    private static final int MAX_CONCURRENT_EXECUTE =
        Integer.getInteger("PB_MAX_CONCURRENT_EXECUTE", 0);

    // Per-service semaphore for limiting concurrent execute() calls.
    // Only populated when MAX_CONCURRENT_EXECUTE > 0.
    private final ConcurrentHashMap<String, Semaphore> serviceExecuteSemaphores =
        new ConcurrentHashMap<>();

    // ── Execution Pipelining with Single Capture Thread ─────────────────
    //
    // Primary-backup allows pipelined execution: multiple workers execute
    // requests against the containerized service concurrently. State diff
    // capture and Paxos propose are handled by a SINGLE DEDICATED THREAD
    // per service (the "capture thread").
    //
    // The pipeline:
    //
    //   Worker1: take(queue) → execute → enqueue(done) → wait → copy response → loop
    //   Worker2:    take(queue) → execute → enqueue(done) → wait → copy response → loop
    //   Worker3:       take(queue) → execute → enqueue(done) → wait → copy response → loop
    //   Capture:                               drain → captureStateDiff → propose → signal all
    //
    // Why this is correct for primary-backup:
    //
    //   captureStateDiff() returns ALL filesystem changes since the last
    //   capture, regardless of which worker caused them. When the capture
    //   thread drains N completed batches, it captures ONCE and proposes
    //   ONCE — the single diff covers all N batches' state changes.
    //   Backups apply diffs in Paxos-commit order, reaching the same final
    //   state as the primary.
    //
    //   The capture thread provides natural serialization of
    //   captureStateDiff() + propose() without any synchronized block.
    //   Paxos slot order matches capture order because both happen
    //   sequentially on the same thread.
    //
    // Performance benefits:
    //   - Workers never contend on a shared lock — pure parallel execution.
    //   - captureStateDiff() called once per drain cycle instead of once
    //     per worker, reducing Fuselog socket round-trips.
    //   - propose() called once per drain cycle instead of once per worker,
    //     reducing Paxos overhead.
    //   - Workers are fully async: execute, copy responses, enqueue for
    //     capture, and immediately loop back without blocking.
    //
    // ──────────────────────────────────────────────────────────────────────

    // Per-service batch queue: requests waiting to be executed as a batch
    private final ConcurrentHashMap<String, LinkedBlockingQueue<RequestAndCallback>>
        serviceBatchQueues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Thread> batchWorkerThreads =
        new ConcurrentHashMap<>();

    // Maps ApplyStateDiffPacket.requestID → the N (packet, callback) pairs it represents
    private final ConcurrentHashMap<Long, List<RequestAndCallback>>
        pendingBatchCallbacks = new ConcurrentHashMap<>();

    // Per-service single-threaded executor for async backup statediff application.
    // Single-threaded per service preserves application order without blocking the Paxos thread.
    private final ConcurrentHashMap<String, ExecutorService>
        backupApplyExecutors = new ConcurrentHashMap<>();

    // ── Single Capture Thread ──────────────────────────────────────────────
    //
    // Workers enqueue completed batches here after execution. A dedicated
    // capture thread drains this queue, calls captureStateDiff() once for
    // all accumulated completions, proposes the combined diff via Paxos,
    // and signals each waiting worker.
    //
    // Why a single capture thread?
    //   - captureStateDiff() does atomic get-and-clear: it returns ALL
    //     filesystem changes since the last call, regardless of which
    //     worker caused them. Calling it once per drain cycle is sufficient.
    //   - propose() must be serialized with captureStateDiff() to ensure
    //     Paxos slot order matches statediff capture order. A single thread
    //     provides this naturally without any synchronized block.
    //   - Workers no longer contend on synchronized(currentEpoch). They
    //     execute, copy responses, enqueue, and return — fully parallel.

    // Enum to distinguish the three execution paths when the capture thread
    // needs to copy responses back to the original RequestPackets.
    enum BatchType { XDN_HTTP, XDN_HTTP_BATCH, SINGLE }

    // Represents a completed execution awaiting capture+propose.
    record CompletedBatch(
        BatchType type,
        List<RequestAndCallback> batch,
        List<XdnHttpRequest> xdnRequests,         // non-null for XDN_HTTP
        List<XdnHttpRequestBatch> xdnBatches,     // non-null for XDN_HTTP_BATCH
        Request singleAppReq,                     // non-null for SINGLE
        PrimaryEpoch<?> epoch
    ) {}

    // Per-service done queue: completed executions waiting for capture
    private final ConcurrentHashMap<String, LinkedBlockingQueue<CompletedBatch>>
        serviceDoneQueues = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Thread> captureThreads =
        new ConcurrentHashMap<>();

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
                String.format("%s=%b", PaxosConfig.PC.BATCHING_ENABLED, true),
        };
        Config.register(args);
    }

    private void validatePaxosConfiguration() {
        assert Config.getGlobalBoolean(PaxosConfig.PC.ENABLE_EMBEDDED_STORE_SHUTDOWN);
        assert !Config.getGlobalBoolean(PaxosConfig.PC.ENABLE_STARTUP_LEADER_ELECTION);
        assert !Config.getGlobalBoolean(PaxosConfig.PC.FORWARD_PREEMPTED_REQUESTS);
        assert Config.getGlobalInt(PaxosConfig.PC.PACKET_DEMULTIPLEXER_THREADS) == 0;
        assert !Config.getGlobalBoolean(PaxosConfig.PC.HIBERNATE_OPTION);
        assert Config.getGlobalBoolean(PaxosConfig.PC.BATCHING_ENABLED);
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

        // Submit to the per-service batch worker; the worker drains the queue and executes
        // requests in parallel batches before proposing a single stateDiff per batch.
        ensureBatchWorker(serviceName).offer(new RequestAndCallback(packet, callback));
        return true;
    }

    private LinkedBlockingQueue<RequestAndCallback> ensureBatchWorker(String serviceName) {
        return serviceBatchQueues.computeIfAbsent(serviceName, svc -> {
            LinkedBlockingQueue<RequestAndCallback> q = new LinkedBlockingQueue<>();

            // Create done queue + single capture thread for this service.
            LinkedBlockingQueue<CompletedBatch> doneQ = new LinkedBlockingQueue<>();
            serviceDoneQueues.put(svc, doneQ);
            Thread captureThread = Thread.ofVirtual()
                    .name("pb-capture-" + svc)
                    .start(() -> captureThreadLoop(svc, doneQ));
            captureThreads.put(svc, captureThread);

            // Create N worker threads.
            for (int i = 0; i < N_PARALLEL_WORKERS; i++) {
                final int workerId = i;
                Thread worker = Thread.ofVirtual()
                        .name("pb-batch-" + svc + "-" + workerId)
                        .start(() -> batchWorkerLoop(svc, q));
                batchWorkerThreads.put(svc + "-" + workerId, worker);
            }
            return q;
        });
    }

    // ── Capture Thread Loop ────────────────────────────────────────────────
    //
    // Dedicated thread that serializes captureStateDiff() + propose() for
    // a single service. Drains all completed executions, captures state
    // once, proposes once, then signals all waiting workers.
    //
    // This thread provides the same ordering guarantee as the old
    // synchronized(currentEpoch) block: captureStateDiff() and propose()
    // happen atomically in sequence, ensuring Paxos slot order matches
    // statediff capture order.
    //
    // Safety arguments:
    //
    //   1. SERIALIZATION: Only this thread calls captureStateDiff() and
    //      propose() for this service. No synchronized block needed —
    //      single-threaded execution provides mutual exclusion naturally.
    //
    //   2. SLOT ORDERING: Each drain cycle produces exactly one
    //      captureStateDiff() → one propose(). The diff includes ALL
    //      filesystem changes from all workers' executions that completed
    //      before this drain. The next drain's diff covers the next batch.
    //      Paxos slots are assigned in drain order = capture order. ✓
    //
    //   3. FUSELOG IDEMPOTENCY: Fuselog's get-and-clear is atomic — it
    //      returns accumulated diffs and clears the buffer in one operation.
    //      A single caller (this thread) means no risk of two callers
    //      splitting or duplicating the diff.
    //
    //   4. EPOCH TRANSITIONS: If the epoch changes between a worker's
    //      execute and this thread's drain, the CompletedBatch carries the
    //      worker's epoch snapshot. This thread checks currentPrimaryEpoch
    //      before capture+propose and drops stale batches (same as before).
    //
    //   5. FULLY ASYNC WORKERS: Workers copy HTTP responses from XdnHttpRequest
    //      objects to RequestPackets BEFORE enqueuing. Workers then return
    //      immediately without blocking — no CompletableFuture.join(). The
    //      Paxos propose callback reads responses via requestPacket.getResponse()
    //      and invokes client callbacks asynchronously. This eliminates worker
    //      blocking on capture+propose latency, enabling faster queue draining.
    //
    //   7. NULL/EMPTY DIFFS: If no filesystem changes occurred (e.g., all
    //      requests were reads), captureStateDiff() returns null or empty.
    //      This is still proposed (as empty byte[]) — same as current code.
    //      Backups apply the no-op diff harmlessly.
    //
    // Maximum time the capture thread waits on doneQueue before draining the
    // FUSELOG socket anyway.  Prevents a deadlock where workers block on
    // PostgreSQL writes that go through FUSELOG → socket fills up → capture
    // thread is stuck on take() → nobody reads the socket → deadlock.
    private static final long CAPTURE_POLL_TIMEOUT_MS =
            Long.getLong("PB_CAPTURE_POLL_TIMEOUT_MS", 100);

    private void captureThreadLoop(
            String serviceName, LinkedBlockingQueue<CompletedBatch> doneQueue) {
        while (!Thread.currentThread().isInterrupted()) {
            List<CompletedBatch> drained = null;
            try {
                // Poll (not take!) so we periodically drain the FUSELOG socket
                // even when no completions are arriving.  Without this, a deadlock
                // can occur: workers block on PostgreSQL writes through FUSELOG,
                // the FUSELOG socket fills up because the capture thread isn't
                // reading it, and the capture thread is stuck waiting here.
                long cycleStart = System.nanoTime();
                long takeStart = System.nanoTime();
                CompletedBatch first = doneQueue.poll(
                        CAPTURE_POLL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                long takeEnd = System.nanoTime();

                if (first == null) {
                    // No completions arrived within the timeout.  Capture the
                    // state diff AND propose it to keep the FUSELOG socket
                    // drained AND ensure backups receive all state changes.
                    // Without this, a deadlock occurs: workers block on
                    // PostgreSQL writes through FUSELOG, the socket fills up
                    // because nobody is reading it, and this thread is stuck
                    // waiting on poll() — nobody reads the socket.
                    PrimaryEpoch<NodeIDType> epoch =
                            this.currentPrimaryEpoch.get(serviceName);
                    if (epoch == null) continue;
                    try {
                        byte[] diff = backupableApp.captureStatediff(serviceName);
                        if (diff != null && diff.length > 0) {
                            ApplyStateDiffPacket pkt = new ApplyStateDiffPacket(
                                    serviceName, epoch,
                                    diff);
                            ReplicableClientRequest gpPkt =
                                    ReplicableClientRequest.wrap(pkt);
                            gpPkt.setClientAddress(
                                    messenger.getListeningSocketAddress());
                            this.paxosManager.propose(serviceName, gpPkt,
                                    (p, h) -> {
                                        logger.log(Level.FINE, String.format(
                                                "%s:%s - no-op diff committed "
                                                + "(size=%d)",
                                                myNodeID,
                                                PrimaryBackupManager.class
                                                        .getSimpleName(),
                                                diff.length));
                                    });
                            logger.log(Level.FINE, String.format(
                                    "%s:%s - capture thread: proposed no-op "
                                    + "diff (size=%d) to keep socket drained",
                                    myNodeID,
                                    PrimaryBackupManager.class.getSimpleName(),
                                    diff.length));
                        }
                    } catch (Exception e) {
                        // Ignore — service might not be initialized yet.
                    }
                    continue;
                }

                drained = new ArrayList<>();
                drained.add(first);

                // Accumulation window: wait up to CAPTURE_ACCUMULATION_MS for more
                // completions so we can batch them into a single captureStateDiff + propose.
                if (CAPTURE_ACCUMULATION_MS > 0) {
                    long deadlineNs = System.nanoTime()
                            + CAPTURE_ACCUMULATION_MS * 1_000_000L;
                    CompletedBatch next;
                    while ((next = doneQueue.poll(
                            Math.max(0, deadlineNs - System.nanoTime()),
                            TimeUnit.NANOSECONDS)) != null) {
                        drained.add(next);
                    }
                }

                // Non-blocking drain of any additional completions that
                // arrived while we were processing the previous cycle.
                doneQueue.drainTo(drained);

                logger.log(Level.INFO, String.format(
                        "%s:%s - capture thread: drained=%d doneQueueRemaining=%d takeWait=%.3fms",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        drained.size(), doneQueue.size(),
                        (takeEnd - takeStart) / 1_000_000.0));

                // Validate epoch: drop completions from stale epochs.
                PrimaryEpoch<NodeIDType> currentEpoch =
                        this.currentPrimaryEpoch.get(serviceName);
                List<CompletedBatch> valid = new ArrayList<>();
                for (CompletedBatch cb : drained) {
                    if (currentEpoch != null && currentEpoch.equals(cb.epoch())) {
                        valid.add(cb);
                    } else {
                        // Epoch changed — fail these requests so the client retries.
                        logger.log(Level.WARNING, String.format(
                                "%s:%s - epoch changed, dropping %d requests from capture queue",
                                myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                cb.batch().size()));
                        cb.batch().forEach(rc ->
                                rc.callback().executed(rc.requestPacket(), false));
                    }
                }

                if (valid.isEmpty()) continue;

                // Collect ALL RequestAndCallbacks from all valid completions
                // into a single flat list for the Paxos callback.
                List<RequestAndCallback> allCallbacks = new ArrayList<>();
                for (CompletedBatch cb : valid) {
                    allCallbacks.addAll(cb.batch());
                }

                // BYPASS: skip capture+propose, fire callbacks immediately.
                // This isolates the PBM worker pipeline from Paxos overhead.
                if (SKIP_REPLICATION) {
                    double cycleMs = (System.nanoTime() - cycleStart) / 1_000_000.0;
                    logger.log(Level.INFO, String.format(
                            "%s:%s - capture thread BYPASS: %d requests, cycle=%.3fms",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            allCallbacks.size(), cycleMs));
                    allCallbacks.forEach(rc ->
                            rc.callback().executed(rc.requestPacket(), true));
                    continue;
                }

                // ONE captureStateDiff() for ALL completed executions in this cycle.
                long sdStart = System.nanoTime();
                byte[] stateDiff = backupableApp.captureStatediff(serviceName);
                long sdEnd = System.nanoTime();
                logger.log(Level.INFO, String.format(
                        "%s:%s - capture thread: stateDiff in %.3f ms, size=%d bytes, "
                        + "covering %d completed batches (%d requests)",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        (sdEnd - sdStart) / 1_000_000.0,
                        stateDiff != null ? stateDiff.length : 0,
                        valid.size(), allCallbacks.size()));

                // ONE propose() for the combined diff.
                ApplyStateDiffPacket applyPkt = new ApplyStateDiffPacket(
                        serviceName, currentEpoch,
                        stateDiff != null ? stateDiff : new byte[0]);
                ReplicableClientRequest gpPacket =
                        ReplicableClientRequest.wrap(applyPkt);
                gpPacket.setClientAddress(messenger.getListeningSocketAddress());

                List<RequestAndCallback> callbackSnapshot = new ArrayList<>(allCallbacks);
                pendingBatchCallbacks.put(applyPkt.getRequestID(), callbackSnapshot);

                long proposeStart = System.nanoTime();
                this.paxosManager.propose(
                        serviceName,
                        gpPacket,
                        (proposedPacket, handled) -> {
                            double elapsedMs = (System.nanoTime() - proposeStart) / 1_000_000.0;
                            logger.log(Level.INFO, String.format(
                                    "%s:%s - capture thread: %d requests committed in %.3f ms (diffSize=%d)",
                                    myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                    callbackSnapshot.size(), elapsedMs,
                                    stateDiff != null ? stateDiff.length : 0));
                            Long batchId = ((ApplyStateDiffPacket) proposedPacket)
                                    .getRequestID();
                            List<RequestAndCallback> cbs =
                                    pendingBatchCallbacks.remove(batchId);
                            if (cbs != null) {
                                cbs.forEach(rc -> rc.callback()
                                        .executed(rc.requestPacket(), handled));
                            }
                        });
                double proposeCallMs = (System.nanoTime() - proposeStart) / 1_000_000.0;
                double cycleMs = (System.nanoTime() - cycleStart) / 1_000_000.0;
                logger.log(Level.INFO, String.format(
                        "%s:%s - capture thread: cycle=%.3fms propose=%.3fms drained=%d requests=%d",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        cycleMs, proposeCallMs, valid.size(), allCallbacks.size()));


            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                logger.log(Level.SEVERE, String.format(
                        "%s:%s - uncaught exception in capture thread for %s: %s",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        serviceName, e.getMessage()), e);
                // Fail all drained requests so clients can retry.
                if (drained != null) {
                    for (CompletedBatch cb : drained) {
                        cb.batch().forEach(rc ->
                                rc.callback().executed(rc.requestPacket(), false));
                    }
                }
            }
        }
    }

    private void batchWorkerLoop(
            String serviceName, LinkedBlockingQueue<RequestAndCallback> queue) {
        while (!Thread.currentThread().isInterrupted()) {
            List<RequestAndCallback> batch = null;
            try {
                // Block until the first request arrives.
                RequestAndCallback first = queue.take();
                batch = new ArrayList<>();
                batch.add(first);

                // Do NOT drain additional items from the queue.  Let each of
                // the N_PARALLEL_WORKERS grab one item via take() so that
                // execution fans out across workers (pipelined) instead of
                // one worker bursting N concurrent writes to the container
                // (which causes SQLite WAL lock contention and a positive
                // feedback loop that collapses throughput).
                //
                // The capture thread already batches completions from all
                // workers into a single captureStateDiff + Paxos propose,
                // so the replication-amortisation benefit is preserved.

                int queueDepthAfterDrain = queue.size();
                logger.log(Level.FINE, String.format(
                        "%s:%s - worker pickup: batchSize=%d queueRemaining=%d",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        batch.size(), queueDepthAfterDrain));

                if (MAX_CONCURRENT_EXECUTE > 0) {
                    Semaphore sem = serviceExecuteSemaphores.computeIfAbsent(
                        serviceName, k -> new Semaphore(MAX_CONCURRENT_EXECUTE));
                    sem.acquire();
                    try {
                        executeRequestBatchCoordinateStateDiff(serviceName, batch);
                    } finally {
                        sem.release();
                    }
                } else {
                    executeRequestBatchCoordinateStateDiff(serviceName, batch);
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                // Catch-all to prevent virtual thread death from unchecked exceptions
                // (e.g. CompletionException wrapping CancellationException from Netty pool).
                // Log, fail the batch, and continue — do NOT let the thread die.
                logger.log(Level.SEVERE, String.format(
                        "%s:%s - uncaught exception in batch worker for %s, dropping %d requests: %s",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        serviceName, batch != null ? batch.size() : 0, e.getMessage()), e);
                if (batch != null) {
                    batch.forEach(rc -> rc.callback().executed(rc.requestPacket(), false));
                }
            }
        }
    }

    private void executeRequestBatchCoordinateStateDiff(
            String serviceName, List<RequestAndCallback> batch) {

        // Re-check role: requests were enqueued when PRIMARY but role may have changed
        // by the time the batch worker picks them up.
        Role currentServiceRole = this.currentRole.get(serviceName);
        if (currentServiceRole == Role.PRIMARY_CANDIDATE) {
            // Re-queue; will be drained again by processOutstandingRequests() on PRIMARY promotion
            batch.forEach(rc -> outstandingRequests.add(rc));
            return;
        }
        if (currentServiceRole == Role.BACKUP) {
            // Forward each request to the current primary
            batch.forEach(rc -> handRequestToPrimary(rc.requestPacket(), rc.callback()));
            return;
        }
        if (currentServiceRole != Role.PRIMARY) {
            logger.log(Level.WARNING,
                    String.format("%s:%s - unexpected role %s for %s, dropping %d requests",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            currentServiceRole, serviceName, batch.size()));
            batch.forEach(rc -> rc.callback().executed(rc.requestPacket(), false));
            return;
        }

        PrimaryEpoch<NodeIDType> currentEpoch = this.currentPrimaryEpoch.get(serviceName);
        if (currentEpoch == null) {
            logger.log(Level.WARNING,
                    String.format(
                            "%s:%s - unknown current primary epoch for %s, dropping %d requests",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            serviceName, batch.size()));
            batch.forEach(rc -> rc.callback().executed(rc.requestPacket(), false));
            return;
        }

        // 1. Parse each RequestPacket → Request, track type uniformity
        List<Request> parsedRequests = new ArrayList<>(batch.size());
        boolean allXdnHttp = true;
        boolean allXdnHttpBatch = true;
        for (RequestAndCallback rc : batch) {
            try {
                Request original = rc.requestPacket().getOriginalRequest();
                Request appReq;
                if (original != null) {
                    appReq = original;
                } else {
                    String encoded = new String(rc.requestPacket().getEncodedServiceRequest(),
                            StandardCharsets.ISO_8859_1);
                    appReq = replicableApp.getRequest(encoded);
                }
                parsedRequests.add(appReq);
                if (!(appReq instanceof XdnHttpRequest)) {
                    allXdnHttp = false;
                }
                if (!(appReq instanceof XdnHttpRequestBatch)) {
                    allXdnHttpBatch = false;
                }
            } catch (RequestParseException e) {
                logger.log(Level.WARNING,
                        String.format(
                                "%s:%s - failed to parse request in batch: %s",
                                myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                e.getMessage()));
                parsedRequests.add(null);
                allXdnHttp = false;
                allXdnHttpBatch = false;
            }
        }

        if (allXdnHttp && !parsedRequests.isEmpty()) {
            // Fast path: all requests are XdnHttpRequest — execute in parallel via
            // XdnHttpRequestBatch (CompletableFuture.allOf internally), one stateDiff capture,
            // and one Paxos proposal for the whole batch.
            List<XdnHttpRequest> xdnRequests = parsedRequests.stream()
                    .map(r -> (XdnHttpRequest) r).collect(Collectors.toList());
            executeXdnBatch(serviceName, currentEpoch, batch, xdnRequests);
        } else if (allXdnHttpBatch && !parsedRequests.isEmpty()) {
            // Batch-of-batches path: when the HTTP batcher is enabled, each write request arrives
            // as a singleton XdnHttpRequestBatch. Combine their inner XdnHttpRequests for
            // parallel execution — single synchronized block, single capture, single proposal.
            List<XdnHttpRequestBatch> xdnBatches = parsedRequests.stream()
                    .map(r -> (XdnHttpRequestBatch) r).collect(Collectors.toList());
            executeXdnBatchOfBatches(serviceName, currentEpoch, batch, xdnBatches);
        } else {
            // Fallback path: non-XdnHttpRequest app (e.g., MonotonicApp in tests), or parse
            // failure. Execute each request individually with its own capture + propose.
            for (int i = 0; i < batch.size(); i++) {
                Request appReq = parsedRequests.get(i);
                if (appReq == null) {
                    batch.get(i).callback().executed(batch.get(i).requestPacket(), false);
                    continue;
                }
                executeSingleRequestCoordinateStateDiff(
                        serviceName, currentEpoch, batch.get(i), appReq);
            }
        }
    }

    // Batch path: execute N XdnHttpRequests in parallel (within this batch), then enqueue
    // for the capture thread to capture stateDiff and propose once. Multiple workers may
    // run this method concurrently — see "Execution Pipelining" comment above.
    private void executeXdnBatch(
            String serviceName,
            PrimaryEpoch<NodeIDType> currentEpoch,
            List<RequestAndCallback> batch,
            List<XdnHttpRequest> xdnRequests) {

        XdnHttpRequestBatch batchRequest = new XdnHttpRequestBatch(xdnRequests);

        // Execute OUTSIDE any lock — fully parallel with other workers.
        long startTime = System.nanoTime();
        replicableApp.execute(batchRequest);
        long endTime = System.nanoTime();
        logger.log(Level.FINE,
                String.format(
                        "%s:%s - executing batch of %d requests within %f ms",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        batch.size(),
                        (double) (endTime - startTime) / 1_000_000.0));

        // Re-validate epoch: primary may have changed during execute.
        PrimaryEpoch<NodeIDType> epochAfterExecute = this.currentPrimaryEpoch.get(serviceName);
        if (!currentEpoch.equals(epochAfterExecute)) {
            logger.log(Level.WARNING,
                    String.format(
                            "%s:%s - epoch changed during execute for %s, dropping %d requests",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            serviceName, batch.size()));
            batch.forEach(rc -> rc.callback().executed(rc.requestPacket(), false));
            return;
        }

        // Copy per-request responses (set during execute) to RequestPackets
        // BEFORE enqueue — Paxos callback reads them via requestPacket.getResponse().
        for (int i = 0; i < batch.size(); i++) {
            XdnHttpRequest req = xdnRequests.get(i);
            if (req instanceof ClientRequest) {
                ClientRequest resp = ((ClientRequest) req).getResponse();
                if (resp != null) batch.get(i).requestPacket().setResponse(resp);
            }
        }

        // Enqueue completion for the capture thread — worker does NOT wait.
        // The capture thread will call captureStateDiff() + propose() for us.
        serviceDoneQueues.get(serviceName).offer(new CompletedBatch(
                BatchType.XDN_HTTP, batch, xdnRequests, null, null, currentEpoch));
    }

    // Batch-of-batches path: when the HTTP batcher is enabled, each write arrives as a singleton
    // XdnHttpRequestBatch. Combine their inner XdnHttpRequests for parallel execution, then
    // enqueue for the capture thread. Multiple workers may run this method concurrently —
    // see "Execution Pipelining" comment above.
    private void executeXdnBatchOfBatches(
            String serviceName,
            PrimaryEpoch<NodeIDType> currentEpoch,
            List<RequestAndCallback> batch,
            List<XdnHttpRequestBatch> xdnBatches) {

        // Execute each outer batch in parallel — avoids flattening into one large fan-out
        // that amplifies tail latency when the backend serializes writes (e.g., SQLite WAL).
        long startTime = System.nanoTime();
        CompletableFuture<?>[] batchFutures = new CompletableFuture<?>[xdnBatches.size()];
        for (int i = 0; i < xdnBatches.size(); i++) {
            XdnHttpRequestBatch xdnBatch = xdnBatches.get(i);
            batchFutures[i] = CompletableFuture.runAsync(() -> replicableApp.execute(xdnBatch));
        }
        CompletableFuture.allOf(batchFutures).join();
        long endTime = System.nanoTime();
        int totalInnerRequests = xdnBatches.stream().mapToInt(XdnHttpRequestBatch::size).sum();
        logger.log(Level.INFO,
                String.format(
                        "%s:%s - executing batch-of-batches (%d batches, %d reqs) in parallel within %f ms",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        batch.size(), totalInnerRequests,
                        (double) (endTime - startTime) / 1_000_000.0));

        // Re-validate epoch.
        PrimaryEpoch<NodeIDType> epochAfterExecute = this.currentPrimaryEpoch.get(serviceName);
        if (!currentEpoch.equals(epochAfterExecute)) {
            logger.log(Level.WARNING,
                    String.format(
                            "%s:%s - epoch changed during execute for %s, dropping %d requests",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            serviceName, batch.size()));
            batch.forEach(rc -> rc.callback().executed(rc.requestPacket(), false));
            return;
        }

        // Copy responses BEFORE enqueue — Paxos callback reads them via requestPacket.getResponse().
        for (int i = 0; i < batch.size(); i++) {
            XdnHttpRequestBatch xdnBatch = xdnBatches.get(i);
            ClientRequest resp = xdnBatch.getResponse();
            if (resp != null) batch.get(i).requestPacket().setResponse(resp);
        }

        // Enqueue completion for the capture thread — worker does NOT wait.
        serviceDoneQueues.get(serviceName).offer(new CompletedBatch(
                BatchType.XDN_HTTP_BATCH, batch, null, xdnBatches, null, currentEpoch));
    }

    // Fallback single-request path for non-XdnHttpRequest apps (original behavior).
    // Multiple workers may run this concurrently — see "Execution Pipelining" above.
    private void executeSingleRequestCoordinateStateDiff(
            String serviceName,
            PrimaryEpoch<NodeIDType> currentEpoch,
            RequestAndCallback rc,
            Request appReq) {

        // Execute OUTSIDE any lock.
        long startTime = System.nanoTime();
        boolean success = replicableApp.execute(appReq);
        if (!success) {
            logger.log(Level.WARNING,
                    String.format(
                            "%s:%s - failed to execute request for %s id=%d",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            rc.requestPacket().getServiceName(),
                            rc.requestPacket().getRequestID()));
            rc.callback().executed(rc.requestPacket(), false);
            return;
        }
        long endTime = System.nanoTime();
        logger.log(Level.FINE,
                String.format(
                        "%s:%s - executing request within %f ms",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        (double) (endTime - startTime) / 1_000_000.0));

        // Re-validate epoch.
        PrimaryEpoch<NodeIDType> epochAfterExecute = this.currentPrimaryEpoch.get(serviceName);
        if (!currentEpoch.equals(epochAfterExecute)) {
            logger.log(Level.WARNING,
                    String.format(
                            "%s:%s - epoch changed during execute for %s, dropping request id=%d",
                            myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            serviceName, rc.requestPacket().getRequestID()));
            rc.callback().executed(rc.requestPacket(), false);
            return;
        }

        // Copy response BEFORE enqueue — Paxos callback reads it via requestPacket.getResponse().
        if (appReq instanceof ClientRequest) {
            ClientRequest resp = ((ClientRequest) appReq).getResponse();
            if (resp != null) rc.requestPacket().setResponse(resp);
        }

        // Enqueue completion for the capture thread — worker does NOT wait.
        serviceDoneQueues.get(serviceName).offer(new CompletedBatch(
                BatchType.SINGLE, List.of(rc), null, null, appReq, currentEpoch));
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
                } finally {
                    releaseForwardedResponse(requestWithResponse.getResponse());
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

    private void releaseForwardedResponse(ClientRequest response) {
        if (response == null) {
            return;
        }
        if (response instanceof XdnHttpRequest xdnHttpRequest) {
            if (xdnHttpRequest.getHttpResponse() != null
                    && ReferenceCountUtil.refCnt(xdnHttpRequest.getHttpResponse()) > 0) {
                ReferenceCountUtil.release(xdnHttpRequest.getHttpResponse());
            }
            return;
        }
        if (response instanceof XdnHttpRequestBatch batch) {
            for (var requestWithResponse : batch.getRequestList()) {
                if (requestWithResponse.getHttpResponse() == null) {
                    continue;
                }
                if (ReferenceCountUtil.refCnt(requestWithResponse.getHttpResponse()) > 0) {
                    ReferenceCountUtil.release(requestWithResponse.getHttpResponse());
                }
            }
        }
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
        logger.log(Level.INFO,
                String.format(
                        "%s:%s - handleChangePrimaryPacket svc=%s target=%s role=%s epoch=%s isPrimary=%b isPaxosCoordinator=%b",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        groupName, packet.getNodeID(), myCurrentRole, curEpoch,
                        isCurrentPrimary(groupName),
                        this.paxosManager.isPaxosCoordinator(groupName)));
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

                    logger.log(Level.INFO,
                            String.format(
                                    "%s:%s - change primary completed svc=%s target=%s epoch=%s handled=%b",
                                    myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                    groupName, packet.getNodeID(), newEpoch, isHandled));
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

            // As a backup, apply the stateDiff asynchronously to avoid blocking the Paxos
            // execution thread (which would prevent acceptance of new proposals and cause
            // TCP backpressure on the connection from the primary).  A single-threaded
            // executor per service preserves application order.
            if (myCurrentRole.equals(Role.BACKUP)) {
                final byte[] stateDiff = packet.getStateDiff();
                backupApplyExecutors
                    .computeIfAbsent(groupName, k -> Executors.newSingleThreadExecutor(r -> {
                        Thread t = new Thread(r, "BackupApply-" + k);
                        t.setDaemon(true);
                        return t;
                    }))
                    .submit(() -> this.backupableApp.applyStatediff(groupName, stateDiff));
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
        logger.log(Level.INFO,
                String.format(
                        "%s:%s - executeStartEpoch svc=%s newEpoch=%s currentEpoch=%s role=%s",
                        myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        groupName, newPrimaryEpoch, currentEpoch, this.currentRole.get(groupName)));
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
                logger.log(Level.INFO,
                        String.format(
                                "%s:%s - stepping down svc=%s role=%s oldEpoch=%s newEpoch=%s",
                                myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                groupName, myCurrentRole, currentEpoch, newPrimaryEpoch));
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

        // We encapsulate the initial/final state with primary-backup prefix since we need
        // different app initialization (i.e., only starting the full app in primary, not backups).
        if (initialState != null
                && (initialState.startsWith(ServiceProperty.XDN_INITIAL_STATE_PREFIX)
                || initialState.startsWith(ServiceProperty.XDN_EPOCH_FINAL_STATE_PREFIX))) {
            initialState = String.format("%s%s",
                    ServiceProperty.NON_DETERMINISTIC_CREATE_PREFIX, initialState);
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
        //     Option (2) is the default when we are bootstrapping.
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

    // Initialize a replica in a primary-backup replica group without placement metadata.
    // Thus, we set the node that is the paxos' coordinator to be the primary.
    private boolean initializePrimaryEpoch(String groupName, Set<NodeIDType> nodes,
                                           int placementEpoch) {
        assert groupName != null && !groupName.isEmpty();
        assert nodes != null && !nodes.isEmpty();

        // all nodes start as BACKUP, initially.
        this.currentRole.put(groupName, Role.BACKUP);

        // Detect the current paxos' coordinator.
        // WARNING: We assume paxos' coordinator is chosen already, which should be the case
        //  when we set ENABLE_STARTUP_LEADER_ELECTION=false (the default) that will cause
        //  deterministic coordinator picking upon replica-group creation.
        NodeIDType paxosCoordinatorID = this.paxosManager.getPaxosCoordinator(groupName);
        if (paxosCoordinatorID == null) {
            throw new RuntimeException("Failed to get paxos coordinator for " + groupName);
        }

        logger.log(Level.INFO,
                String.format("%s:%s:initializePrimaryEpoch - name=%s coord=%s epoch=%d",
                        this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                        groupName, paxosCoordinatorID, placementEpoch));

        // Try to be the primary, co-located with the paxos' coordinator.
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

            logger.log(Level.INFO,
                    String.format("%s:%s:initializePrimaryEpoch - initializing PRIMARY for %s:%d",
                            this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                            groupName, placementEpoch));

            // Note that PrimaryEpoch != PlacementEpoch.
            PrimaryEpoch<NodeIDType> zero = new PrimaryEpoch<>(myNodeID, 0);
            this.currentRole.put(groupName, Role.PRIMARY_CANDIDATE);
            this.currentPrimaryEpoch.put(groupName, zero);

            // Retry loop for StartEpochPacket proposal
            // This handles the race condition where other nodes may not have created their paxos instances yet
            boolean becamePrimary = false;
            for (int attempt = 1; attempt <= START_EPOCH_PROPOSAL_MAX_RETRIES && !becamePrimary; attempt++) {
                final CountDownLatch proposalLatch = new CountDownLatch(1);
                final AtomicBoolean proposalSuccess = new AtomicBoolean(false);

                StartEpochPacket startPacket = new StartEpochPacket(groupName, zero);
                logger.log(Level.FINE,
                        String.format("%s:%s:initializePrimaryEpoch - proposing StartEpochPacket attempt %d/%d for %s",
                                this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                attempt, START_EPOCH_PROPOSAL_MAX_RETRIES, groupName));

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

                            proposalSuccess.set(true);
                            proposalLatch.countDown();

                            // Handle XDN-specific initialization on a separate thread so we don't
                            // block the Paxos executor. nonDeterministicInitialization() can take
                            // 10+ seconds (docker start, DB init, rsync), and blocking the Paxos
                            // callback thread prevents other services' Paxos messages from being
                            // processed, causing timeouts in concurrent service launches.
                            Thread.ofVirtual()
                                .name("xdn-pb-init-" + groupName)
                                .start(() -> handleXdnPrimaryInitialization(
                                    groupName, nodes, ipAddresses, placementEpoch));
                        }
                );

                // Wait for the proposal to complete with timeout
                try {
                    boolean completed = proposalLatch.await(START_EPOCH_PROPOSAL_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                    if (completed && proposalSuccess.get()) {
                        becamePrimary = true;
                        logger.log(Level.INFO,
                                String.format("%s:%s:initializePrimaryEpoch - successfully became PRIMARY for %s on attempt %d",
                                        this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                        groupName, attempt));
                    } else if (attempt < START_EPOCH_PROPOSAL_MAX_RETRIES) {
                        // Check if we became PRIMARY through another path (e.g., executeStartEpochPacket)
                        Role currentServiceRole = this.currentRole.get(groupName);
                        if (currentServiceRole == Role.PRIMARY) {
                            becamePrimary = true;
                            logger.log(Level.INFO,
                                    String.format("%s:%s:initializePrimaryEpoch - became PRIMARY for %s (detected on attempt %d)",
                                            this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                            groupName, attempt));
                        } else {
                            logger.log(Level.FINE,
                                    String.format("%s:%s:initializePrimaryEpoch - proposal timeout for %s on attempt %d, retrying...",
                                            this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                            groupName, attempt));
                            Thread.sleep(START_EPOCH_PROPOSAL_RETRY_DELAY_MS);
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.log(Level.WARNING,
                            String.format("%s:%s:initializePrimaryEpoch - interrupted while waiting for proposal for %s",
                                    this.myNodeID, PrimaryBackupManager.class.getSimpleName(), groupName));
                    break;
                }
            }

            if (!becamePrimary) {
                // Final check - we might have become PRIMARY through executeStartEpochPacket
                Role finalRole = this.currentRole.get(groupName);
                if (finalRole == Role.PRIMARY) {
                    becamePrimary = true;
                } else {
                    logger.log(Level.WARNING,
                            String.format("%s:%s:initializePrimaryEpoch - failed to become PRIMARY for %s after %d attempts (current role: %s)",
                                    this.myNodeID, PrimaryBackupManager.class.getSimpleName(),
                                    groupName, START_EPOCH_PROPOSAL_MAX_RETRIES, finalRole));
                }
            }
        } else {
            PrimaryEpoch<NodeIDType> zero = new PrimaryEpoch<>(paxosCoordinatorID, 0);
            currentRole.put(groupName, Role.BACKUP);
            currentPrimary.put(groupName, paxosCoordinatorID);
            currentPrimaryEpoch.put(groupName, zero);
        }

        return true;
    }

    /**
     * Handles XDN-specific initialization after becoming PRIMARY.
     * This is called from the proposal callback and handles non-deterministic initialization
     * and starting fuselog-apply in backup instances.
     */
    private void handleXdnPrimaryInitialization(String groupName, Set<NodeIDType> nodes,
                                                 Map<String, InetAddress> ipAddresses, int placementEpoch) {
        PrimaryBackupMiddlewareApp middleware;
        XdnGigapaxosApp xdnApp;

        if (!(this.paxosMiddlewareApp instanceof PrimaryBackupMiddlewareApp))
            return;
        middleware = (PrimaryBackupMiddlewareApp) this.paxosMiddlewareApp;
        if (!(middleware.getReplicableApp() instanceof XdnGigapaxosApp))
            return;
        xdnApp = (XdnGigapaxosApp) middleware.getReplicableApp();

        String sshKey = PaxosConfig.getAsProperties().getProperty("SSH_KEY_PATH", "");
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

    private void processOutstandingRequests() {
        assert this.outstandingRequests != null;
        while (!this.outstandingRequests.isEmpty()) {
            RequestAndCallback rc = this.outstandingRequests.poll();
            ensureBatchWorker(rc.requestPacket().getServiceName()).offer(rc);
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

        // Interrupt and remove capture thread for this service.
        Thread captureThread = captureThreads.remove(groupName);
        if (captureThread != null) captureThread.interrupt();
        serviceDoneQueues.remove(groupName);

        // Interrupt and remove worker threads for this service.
        for (int i = 0; i < N_PARALLEL_WORKERS; i++) {
            Thread worker = batchWorkerThreads.remove(groupName + "-" + i);
            if (worker != null) worker.interrupt();
        }
        serviceBatchQueues.remove(groupName);

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

    public NodeIDType getCurrentPrimary(String groupName) {
        return this.currentPrimary.get(groupName);
    }

    public PrimaryEpoch<NodeIDType> getCurrentPrimaryEpoch(String groupName) {
        return this.currentPrimaryEpoch.get(groupName);
    }

    public Role getCurrentRole(String groupName) {
        return this.currentRole.get(groupName);
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

            // Handle ReplicableClientRequest JSON (used by Paxos-based coordinators
            // like AwReplicaCoordinator for sequential consistency).
            if (JSONPacket.couldBeJSON(stringified)) {
                try {
                    JSONObject json = new JSONObject(stringified);
                    Integer type = JSONPacket.getPacketType(json);
                    if (type != null && type == ReconfigurationPacket.PacketType
                            .REPLICABLE_CLIENT_REQUEST.getInt()) {
                        return new ReplicableClientRequest(json, null);
                    }
                } catch (JSONException | UnsupportedEncodingException e) {
                    // not a ReplicableClientRequest, fall through to app
                }
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

            // Unwrap ReplicableClientRequest to get the inner app request
            // (e.g., XdnHttpRequest) that the app knows how to execute.
            // The inner request may be null if deserialized from JSON/bytes,
            // so use getRequest(parser) to trigger lazy parsing via the app.
            long unwrapStartNs = System.nanoTime();
            Request appRequest = request;
            if (request instanceof ReplicableClientRequest rcr) {
                try {
                    appRequest = rcr.getRequest(this.app);
                } catch (UnsupportedEncodingException | RequestParseException e) {
                    throw new RuntimeException(
                            "PrimaryBackupMiddlewareApp: failed to parse inner request", e);
                }
            }
            long unwrapEndNs = System.nanoTime();

            if (this.app.getRequestTypes().contains(appRequest.getRequestType())) {
                long appExecStartNs = System.nanoTime();
                boolean result = this.app.execute(appRequest);
                long appExecEndNs = System.nanoTime();
                long unwrapMs = (unwrapEndNs - unwrapStartNs) / 1_000_000;
                long appExecMs = (appExecEndNs - appExecStartNs) / 1_000_000;
                if (unwrapMs + appExecMs > 3) {
                    Logger.getLogger(PrimaryBackupMiddlewareApp.class.getSimpleName()).log(Level.INFO,
                            "PBMiddlewareApp.execute SLOW unwrap={0}ms appExec={1}ms reqType={2} wasRCR={3}",
                            new Object[]{unwrapMs, appExecMs,
                                    appRequest.getClass().getSimpleName(),
                                    request instanceof ReplicableClientRequest});
                }
                return result;
            }

            if (appRequest instanceof ReconfigurableRequest rcRequest && rcRequest.isStop()) {
                return this.app.restore(appRequest.getServiceName(), null);
            }

            throw new RuntimeException(
                    String.format("PrimaryBackupMiddlewareApp: Unknown execute handler" +
                            " for request %s: %s", appRequest.getClass().getSimpleName(), appRequest));
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
