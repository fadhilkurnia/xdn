package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.NodeIdsMetadataPair;
import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;
import edu.umass.cs.xdn.placementalgorithms.Centroid;
import edu.umass.cs.xdn.placementalgorithms.GreedyHeuristic;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Demand profiler that splits Earth into a {@code NUM_GRID_ROWS × NUM_GRID_COLUMNS}
 * lat/lon grid and tracks per-cell read and write request counts separately.
 *
 * <h3>Hot path</h3>
 * {@link #shouldReportDemandStats} classifies each request as a read (GET/HEAD) or
 * write (POST/PUT/PATCH/DELETE), enqueues its {@link Geolocation} with the flag onto
 * a bounded queue, and returns. All lat/lon → cell conversion happens on a background
 * worker thread so the request path stays minimal.
 *
 * <h3>Reporting</h3>
 * After {@code MIN_DEMAND_REPORT_PERIOD_MS} (10 s) the profiler signals the AR to push
 * demand stats to the RC. {@link #getDemandStats} snapshots and resets both sparse
 * grids, trims each to {@code MAX_GRID_ENTRIES_PER_REPORT} top-traffic cells, and
 * serialises them as two base64-encoded binary blobs inside a {@link JSONObject},
 * staying within GigaPaxos's 4 KB {@code MAX_DEMAND_PROFILE_SIZE} cap.
 *
 * <h3>Placement</h3>
 * On the RC side, {@link #getNewActivesPlacement} reads the service's registered
 * {@link ConsistencyModel} to obtain the readType/writeType access strings, then
 * delegates to whichever {@link PlacementAlgorithm} is selected by the
 * {@link #ALGORITHM} constant.  Switch between {@code CENTROID} and {@code GREEDY_KNOWN}
 * by changing that one constant and recompiling.
 *
 * <h3>Consistency model registration</h3>
 * Call {@link #registerConsistencyModel(String, ConsistencyModel)} at startup, before
 * any traffic arrives, to associate a service with its consistency model.  Unregistered
 * services default to {@link ConsistencyModel#EVENTUAL}.
 */
public class XdnGeoDemandProfiler2 extends AbstractDemandProfile {

    // ---------------------------------------------------------------------------
    // Grid configuration
    // ---------------------------------------------------------------------------
    private static final int    NUM_GRID_ROWS    = 1000;
    private static final int    NUM_GRID_COLUMNS = 1000;

    /**
     * Approximate milliseconds per grid-unit of Euclidean distance.
     * Calibrated against a known real-world benchmark: US (Utah, ~40°N 112°W) to
     * Europe (Amsterdam, ~52°N 4°E) RTT ≈ 100 ms, which corresponds to ~329 grid
     * units → 0.30 ms/unit.  Override via system property {@code xdn.placement.msPerUnit}.
     */
    private static final double MS_PER_UNIT =
            Double.parseDouble(System.getProperty("xdn.placement.msPerUnit", "0.3"));

    // ---------------------------------------------------------------------------
    // Reporting / serialisation limits
    // Budget per grid: 150 entries × 8 bytes × (4/3 base64) × 2 grids ≈ 3 200 bytes;
    // with JSON field overhead the total stays well under the 4 096-byte DB cap.
    // ---------------------------------------------------------------------------
    private static final long MIN_DEMAND_REPORT_PERIOD_MS  = 10_000;
    private static final int  MAX_GRID_ENTRIES_PER_REPORT  = 150;
    private static final int  EVENT_QUEUE_CAPACITY         = 16_384;

    // ---------------------------------------------------------------------------
    // JSON keys
    // ---------------------------------------------------------------------------
    private static final String KEY_NAME                   = "name";
    private static final String KEY_NUM_REQS               = "num_reqs";
    private static final String KEY_GRID_SPARSE_READS_B64  = "grid_sparse_reads_b64";
    private static final String KEY_GRID_SPARSE_WRITES_B64 = "grid_sparse_writes_b64";

    // ---------------------------------------------------------------------------
    // Algorithm selection
    // Change ALGORITHM and recompile to switch placement strategies.
    // ---------------------------------------------------------------------------
    public enum PlacementAlgorithmType { CENTROID, GREEDY_KNOWN }

    private static final PlacementAlgorithmType ALGORITHM = PlacementAlgorithmType.GREEDY_KNOWN;

    private static final PlacementAlgorithm CENTROID_ALGO =
            new Centroid(NUM_GRID_ROWS, NUM_GRID_COLUMNS);

    private static final PlacementAlgorithm GREEDY_ALGO =
            new GreedyHeuristic(NUM_GRID_ROWS, NUM_GRID_COLUMNS, MS_PER_UNIT, /*wrapAround=*/ false);

    private static PlacementAlgorithm getAlgorithm() {
        return ALGORITHM == PlacementAlgorithmType.GREEDY_KNOWN ? GREEDY_ALGO : CENTROID_ALGO;
    }

    // ---------------------------------------------------------------------------
    // Quorum size — global, configurable via system property xdn.placement.quorumSize.
    // ---------------------------------------------------------------------------
    private static final int QUORUM_SIZE =
            Integer.getInteger("xdn.placement.quorumSize", 2);

    // ---------------------------------------------------------------------------
    // Consistency model registry
    // ---------------------------------------------------------------------------
    private static final ConcurrentHashMap<String, ConsistencyModel> SERVICE_CONSISTENCY_MODELS =
            new ConcurrentHashMap<>();

    /**
     * Register the consistency model for a named service.
     *
     * <p>Should be called at application startup, before the service receives traffic.
     * Calling again with the same {@code serviceName} replaces the previous model.
     * Services that are never registered default to {@link ConsistencyModel#EVENTUAL}.
     *
     * @param serviceName the exact service name used in XDN requests
     * @param model       the desired consistency model
     */
    public static void registerConsistencyModel(String serviceName, ConsistencyModel model) {
        SERVICE_CONSISTENCY_MODELS.put(serviceName, model);
    }

    private ConsistencyModel getConsistencyModel() {
        return SERVICE_CONSISTENCY_MODELS.getOrDefault(this.name, ConsistencyModel.EVENTUAL);
    }

    // ---------------------------------------------------------------------------
    // Per-instance demand state
    // Both maps are written only by the worker thread or the JSONObject constructor,
    // and read/cleared inside getDemandStats / combine / getNewActivesPlacement —
    // all protected by mapLock.
    // ---------------------------------------------------------------------------
    private final HashMap<Integer, Integer> sparseReadGrid  = new HashMap<>();
    private final HashMap<Integer, Integer> sparseWriteGrid = new HashMap<>();
    private long totalRequests = 0;
    private final ReentrantLock mapLock = new ReentrantLock();

    private final AtomicLong lastDemandReportTimestamp = new AtomicLong(0);

    /** Carries a geolocation and its read/write classification off the hot path. */
    private record GeolocatedRequest(Geolocation geo, boolean isWrite) {}

    private final LinkedBlockingQueue<GeolocatedRequest> eventQueue =
            new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);
    private volatile ExecutorService worker;

    private static final Logger LOGGER =
            Logger.getLogger(XdnGeoDemandProfiler2.class.getName());

    // ---------------------------------------------------------------------------
    // Constructors
    // ---------------------------------------------------------------------------

    public XdnGeoDemandProfiler2(String name) {
        super(name);
    }

    /** Deserialisation constructor — called by the RC when it receives a demand report. */
    public XdnGeoDemandProfiler2(JSONObject stats) throws JSONException {
        super(stats.getString(KEY_NAME));
        if (stats.has(KEY_NUM_REQS)) {
            this.totalRequests = stats.getLong(KEY_NUM_REQS);
        }
        decodeGrid(stats.optString(KEY_GRID_SPARSE_READS_B64,  null), sparseReadGrid);
        decodeGrid(stats.optString(KEY_GRID_SPARSE_WRITES_B64, null), sparseWriteGrid);
    }

    // ---------------------------------------------------------------------------
    // AbstractDemandProfile — hot path
    // ---------------------------------------------------------------------------

    @Override
    public boolean shouldReportDemandStats(
            Request request, InetAddress sender, ReconfigurableAppInfo nodeConfig) {

        if (request == null || !request.getServiceName().equals(this.name)) return false;

        boolean enqueuedAny = false;
        if (request instanceof XdnHttpRequest xdnReq) {
            enqueuedAny = enqueueIfGeo(xdnReq);
        } else if (request instanceof XdnHttpRequestBatch batch) {
            for (XdnHttpRequest sub : batch.getRequests()) enqueuedAny |= enqueueIfGeo(sub);
        } else {
            return false;
        }
        if (!enqueuedAny) return false;
        ensureWorkerStarted();

        long now  = System.currentTimeMillis();
        long last = lastDemandReportTimestamp.get();
        if (last == 0) {
            lastDemandReportTimestamp.compareAndSet(0, now);
            return false;
        }
        return now - last >= MIN_DEMAND_REPORT_PERIOD_MS
                && lastDemandReportTimestamp.compareAndSet(last, now);
    }

    // ---------------------------------------------------------------------------
    // AbstractDemandProfile — RC side
    // ---------------------------------------------------------------------------

    @Override
    public JSONObject getDemandStats() {
        List<int[]> readEntries, writeEntries;
        long numReqs;

        // Snapshot and reset under the lock — each report covers demand since the last report.
        // The RC's combine() accumulates across reports.
        mapLock.lock();
        try {
            readEntries  = toSortedEntries(sparseReadGrid);
            writeEntries = toSortedEntries(sparseWriteGrid);
            numReqs      = totalRequests;
            sparseReadGrid.clear();
            sparseWriteGrid.clear();
            totalRequests = 0;
        } finally {
            mapLock.unlock();
        }

        // Trim to budget: keep highest-traffic cells (they dominate the centroid / cost).
        if (readEntries.size()  > MAX_GRID_ENTRIES_PER_REPORT)
            readEntries  = readEntries.subList(0, MAX_GRID_ENTRIES_PER_REPORT);
        if (writeEntries.size() > MAX_GRID_ENTRIES_PER_REPORT)
            writeEntries = writeEntries.subList(0, MAX_GRID_ENTRIES_PER_REPORT);

        JSONObject stats = new JSONObject();
        try {
            stats.put(KEY_NAME,                  this.name);
            stats.put(KEY_NUM_REQS,              numReqs);
            stats.put(KEY_GRID_SPARSE_READS_B64,  encodeEntries(readEntries));
            stats.put(KEY_GRID_SPARSE_WRITES_B64, encodeEntries(writeEntries));
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return stats;
    }

    @Override
    public void combine(AbstractDemandProfile update) {
        assert update instanceof XdnGeoDemandProfiler2 : "Invalid profiler type";
        XdnGeoDemandProfiler2 incoming = (XdnGeoDemandProfiler2) update;
        assert incoming.name.equals(this.name) : "Expecting profiler for the same service";

        mapLock.lock();
        try {
            this.totalRequests += incoming.totalRequests;
            incoming.sparseReadGrid.forEach((k, v)  -> this.sparseReadGrid.merge(k,  v, Integer::sum));
            incoming.sparseWriteGrid.forEach((k, v) -> this.sparseWriteGrid.merge(k, v, Integer::sum));
        } finally {
            mapLock.unlock();
        }
    }

    @Override
    public Set<String> reconfigure(Set<String> curActives, ReconfigurableAppInfo appInfo) {
        NodeIdsMetadataPair<String> result = getNewActivesPlacement(curActives, appInfo);
        return result == null ? null : result.nodeIds();
    }

    @Override
    public NodeIdsMetadataPair<String> getNewActivesPlacement(
            Set<String> curActives, ReconfigurableAppInfo appInfo) {

        if (this.totalRequests == 0) return null;
        if (curActives == null || curActives.isEmpty()) return null;

        Map<String, Geolocation> nodeGeo = appInfo.getActiveReplicaGeolocations();
        int targetReplicas = Math.min(curActives.size(), nodeGeo.size());
        if (targetReplicas == 0 || nodeGeo.size() < curActives.size()) return null;

        // Snapshot grids without holding the lock during (potentially slow) algorithm execution.
        Map<Integer, Integer> readSnapshot, writeSnapshot;
        mapLock.lock();
        try {
            readSnapshot  = new HashMap<>(sparseReadGrid);
            writeSnapshot = new HashMap<>(sparseWriteGrid);
        } finally {
            mapLock.unlock();
        }
        if (readSnapshot.isEmpty() && writeSnapshot.isEmpty()) return null;

        ConsistencyModel model = getConsistencyModel();
        PlacementResult result = getAlgorithm().selectReplicas(
                readSnapshot, writeSnapshot, nodeGeo,
                targetReplicas, model.readType, model.writeType, QUORUM_SIZE);
        if (result == null) return null;

        JSONObject metadata = new JSONObject();
        try {
            metadata.put(Keys.PREFERRED_COORDINATOR.toString(), result.preferredCoordinator());
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }

        LOGGER.log(Level.FINE,
                "XdnGeoDemandProfiler2 [{0}] algorithm={1} model={2} actives={3} coordinator={4}",
                new Object[]{
                        this.name, ALGORITHM, model,
                        result.nodeIds(), result.preferredCoordinator()
                });

        return new NodeIdsMetadataPair<>(result.nodeIds(), metadata.toString());
    }

    @Override
    public void justReconfigured() { /* no-op */ }

    // ---------------------------------------------------------------------------
    // Hot-path helpers
    // ---------------------------------------------------------------------------

    private boolean enqueueIfGeo(XdnHttpRequest req) {
        Geolocation geo = req.getClientGeolocation();
        if (geo == null) return false;

        Set<RequestBehaviorType> behaviors = req.getBehaviors();
        boolean isWrite = behaviors.contains(RequestBehaviorType.WRITE_ONLY)
                || behaviors.contains(RequestBehaviorType.READ_MODIFY_WRITE);

        if (!eventQueue.offer(new GeolocatedRequest(geo, isWrite))) {
            LOGGER.log(Level.FINE,
                    "XdnGeoDemandProfiler2 event queue full, dropping sample for {0}", this.name);
            return false;
        }
        return true;
    }

    private void ensureWorkerStarted() {
        if (worker != null) return;
        synchronized (this) {
            if (worker != null) return;
            ThreadFactory tf = r -> {
                Thread t = new Thread(r, "xdn-geoprofiler-" + this.name);
                t.setDaemon(true);
                return t;
            };
            ExecutorService es = Executors.newSingleThreadExecutor(tf);
            es.submit(this::workerLoop);
            worker = es;
        }
    }

    private void workerLoop() {
        try {
            while (!Thread.currentThread().isInterrupted()) {
                GeolocatedRequest event = eventQueue.take();
                int row = PlacementAlgorithm.latToRow(event.geo().latitude(),  NUM_GRID_ROWS);
                int col = PlacementAlgorithm.lonToCol(event.geo().longitude(), NUM_GRID_COLUMNS);
                int idx = row * NUM_GRID_COLUMNS + col;
                mapLock.lock();
                try {
                    if (event.isWrite()) sparseWriteGrid.merge(idx, 1, Integer::sum);
                    else                  sparseReadGrid.merge(idx,  1, Integer::sum);
                    totalRequests++;
                } finally {
                    mapLock.unlock();
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    // ---------------------------------------------------------------------------
    // Serialisation helpers
    // ---------------------------------------------------------------------------

    /** Returns entries sorted descending by count (highest-traffic cells first). */
    private static List<int[]> toSortedEntries(HashMap<Integer, Integer> grid) {
        List<int[]> entries = new ArrayList<>(grid.size());
        for (Map.Entry<Integer, Integer> e : grid.entrySet())
            entries.add(new int[]{e.getKey(), e.getValue()});
        entries.sort((a, b) -> Integer.compare(b[1], a[1]));
        return entries;
    }

    /** Binary layout: [int32 count][count × (int32 cellIdx, int32 requests)] → base64. */
    private static String encodeEntries(List<int[]> entries) {
        ByteBuffer buf = ByteBuffer.allocate(4 + entries.size() * 8);
        buf.putInt(entries.size());
        for (int[] e : entries) { buf.putInt(e[0]); buf.putInt(e[1]); }
        return Base64.getEncoder().encodeToString(buf.array());
    }

    private static void decodeGrid(String b64, HashMap<Integer, Integer> grid) {
        if (b64 == null || b64.isEmpty()) return;
        ByteBuffer buf = ByteBuffer.wrap(Base64.getDecoder().decode(b64));
        int n = buf.getInt();
        for (int i = 0; i < n; i++) grid.put(buf.getInt(), buf.getInt());
    }
}