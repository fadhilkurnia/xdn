package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.reconfiguration.ReconfigurationConfig.RC;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.NodeIdsMetadataPair;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.interfaces.behavior.RequestBehaviorType;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.json.JSONException;
import org.json.JSONObject;

/**
 * Demand profiler that splits Earth into a {@code NUM_GRID_ROWS x NUM_GRID_COLUMNS} latitude /
 * longitude grid and accumulates per-cell request counts from clients that supply their geolocation
 * via the {@code X-Client-Location} HTTP header (parsed into {@link
 * XdnHttpRequest#getClientGeolocation()}), or via the client IP resolved with GeoIP.
 *
 * <p>Demand is split by request kind: each cell tracks separate READ and WRITE counts, where WRITE
 * folds in {@code READ_MODIFY_WRITE} (it mutates state). The split is surfaced for observability
 * (the {@code /demand} heatmap exposes {@code read}/{@code write}/{@code count} per cell).
 *
 * <p>Aggregation runs on a background worker per profiler instance: {@code shouldReportDemandStats}
 * only captures the client location + write flag and enqueues it, keeping the request hot path
 * cheap. {@code getDemandStats} snapshots the sparse counts, serializes them, then resets the local
 * map so each report covers demand since the previous report (the reconfigurator's {@link #combine}
 * accumulates across reports).
 *
 * <p>On the reconfigurator side the accumulation is governed by {@link
 * RC#XDN_DEMAND_WINDOW_MINUTES}: {@code -1} keeps cumulative all-time demand, while a positive
 * {@code N} keeps only the last {@code N} minutes (a rolling window via {@link
 * #expireWindowLocked}), so the heatmap and placement reflect current load. {@link
 * #getNewActivesPlacement} then computes the demand-weighted centroid and picks the {@code
 * curActives.size()} closest nodes, surfacing the closest as {@code PREFERRED_COORDINATOR}.
 *
 * <p>Prototype limitations (future work): threading the service's configured {@code --num-replicas}
 * through {@link edu.umass.cs.xdn.service.ServiceProperty} (today the replica count is the current
 * group size); top-K serialization truncation can discard tail demand under the demand-profile cap.
 */
public class XdnGeoDemandProfiler extends AbstractDemandProfile {

  private static final int NUM_GRID_ROWS = 1000;
  private static final int NUM_GRID_COLUMNS = 1000;

  // At most one demand report every 10 seconds per profiler instance.
  private static final long MIN_DEMAND_REPORT_PERIOD_MS = 10_000;

  // ReconfigurationConfig.MAX_DEMAND_PROFILE_SIZE defaults to 4096 bytes. Each serialized cell is
  // now
  // 12 bytes (int32 index, int32 read, int32 write) plus a 4-byte header; base64 inflates the
  // string
  // 4:3. 200 entries (~2.4 KB raw, ~3.2 KB base64) is a safe conservative budget under the 4 KB
  // cap.
  private static final int MAX_GRID_ENTRIES_PER_REPORT = 200;

  // Bounded queue for the hot path; drops on overflow rather than back-pressuring request handling.
  private static final int EVENT_QUEUE_CAPACITY = 16_384;

  // Per-cell demand vector slots: index 0 = READ, index 1 = WRITE (READ_MODIFY_WRITE folds in
  // here).
  private static final int READ = 0;
  private static final int WRITE = 1;

  private static final String KEY_NAME = "name";
  private static final String KEY_NUM_REQS = "num_reqs"; // back-compat: reads + writes
  private static final String KEY_NUM_READS = "num_reads";
  private static final String KEY_NUM_WRITES = "num_writes";
  private static final String KEY_GRID_RW_B64 = "grid_rw_b64"; // [idx, read, write] triples
  private static final String KEY_GRID_SPARSE_B64 = "grid_sparse_b64"; // legacy [idx, count] pairs

  private static final Logger LOGGER = Logger.getLogger(XdnGeoDemandProfiler.class.getName());

  // The hot path only captures the client location reference + write flag; lat/lon -> (row, col)
  // conversion is deferred to the worker so shouldReportDemandStats stays minimal.
  // Sparse per-cell demand, key = row * NUM_GRID_COLUMNS + col, value = {readCount, writeCount}.
  // Written only by the worker thread, combine(), or the constructor; read under mapLock.
  private final HashMap<Integer, long[]> sparseGrid = new HashMap<>();
  private long totalReads = 0;
  private long totalWrites = 0;
  private final ReentrantLock mapLock = new ReentrantLock();

  private final AtomicLong lastDemandReportTimestamp = new AtomicLong(0);

  // Reconfigurator-side aggregation window. < 0 => CUMULATIVE (never decays). Otherwise a rolling
  // window of windowMillis: combine() records each report's delta with a timestamp, and
  // expireWindowLocked() subtracts deltas older than the window from the running grid so the view
  // reflects only recent demand. Inert on the ActiveReplica side (which accumulates via the worker
  // and resets each report, so combine()/windowDeltas are never exercised there).
  private final long windowMillis;
  private final Deque<TimedDelta> windowDeltas = new ArrayDeque<>();

  // Lazily started worker: one daemon thread per profiler instance.
  private final LinkedBlockingQueue<Sample> eventQueue =
      new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);
  private volatile ExecutorService worker;

  // Shared GeoIP resolver for the IP-based demand fallback; null when no GeoLite2 db is configured
  // (then only the X-Client-Location header contributes demand). Volatile + injectable so a test
  // can
  // supply a resolver bound to a known .mmdb without depending on the JVM-wide lazy singleton.
  private volatile GeoIpResolver geoIpResolver = GeoIpResolver.getDefaultOrNull();

  // A queued demand sample: the client location (a Geolocation from the header, or an InetAddress
  // resolved via GeoIP off the hot path) plus whether the originating request was a write.
  private record Sample(Object loc, boolean write) {}

  // One reconfigurator-side report delta retained for windowed aggregation (so it can be expired).
  private static final class TimedDelta {
    final long ts;
    final Map<Integer, long[]> grid;
    final long reads;
    final long writes;

    TimedDelta(long ts, Map<Integer, long[]> grid, long reads, long writes) {
      this.ts = ts;
      this.grid = grid;
      this.reads = reads;
      this.writes = writes;
    }
  }

  // Test-only override so unit tests can exercise windowing without depending on the global config
  // (which is in whole minutes); null means "use the configured value".
  private static volatile Long windowMillisOverrideForTesting = null;

  static void setWindowMillisForTesting(Long ms) {
    windowMillisOverrideForTesting = ms;
  }

  // Test-only: keep the async worker from starting so samples stay queued, letting a test verify
  // that a report synchronously drains pending samples instead of dropping them.
  private volatile boolean workerDisabledForTesting = false;

  void setWorkerDisabledForTesting(boolean disabled) {
    this.workerDisabledForTesting = disabled;
  }

  private static long computeWindowMillis() {
    Long override = windowMillisOverrideForTesting;
    if (override != null) {
      return override;
    }
    int minutes = Config.getGlobalInt(RC.XDN_DEMAND_WINDOW_MINUTES);
    return minutes < 0 ? -1L : (long) minutes * 60_000L;
  }

  // Test-only: override the GeoIP resolver before the first request is enqueued.
  void setGeoIpResolverForTesting(GeoIpResolver resolver) {
    this.geoIpResolver = resolver;
  }

  public XdnGeoDemandProfiler(String name) {
    super(name);
    this.windowMillis = computeWindowMillis();
  }

  public XdnGeoDemandProfiler(JSONObject stats) throws JSONException {
    super(stats.getString(KEY_NAME));
    this.windowMillis = computeWindowMillis();

    if (stats.has(KEY_NUM_READS) || stats.has(KEY_NUM_WRITES)) {
      this.totalReads = stats.optLong(KEY_NUM_READS, 0);
      this.totalWrites = stats.optLong(KEY_NUM_WRITES, 0);
    } else if (stats.has(KEY_NUM_REQS)) {
      // Legacy report (no read/write split): attribute the unknown total to reads.
      this.totalReads = stats.getLong(KEY_NUM_REQS);
    }

    String rw = stats.optString(KEY_GRID_RW_B64, null);
    if (rw != null && !rw.isEmpty()) {
      ByteBuffer buf = ByteBuffer.wrap(Base64.getDecoder().decode(rw));
      int n = buf.getInt();
      for (int i = 0; i < n; i++) {
        int idx = buf.getInt();
        int r = buf.getInt();
        int w = buf.getInt();
        sparseGrid.put(idx, new long[] {r, w});
      }
    } else {
      // Legacy grid [idx, count]: attribute the count to reads.
      String legacy = stats.optString(KEY_GRID_SPARSE_B64, null);
      if (legacy != null && !legacy.isEmpty()) {
        ByteBuffer buf = ByteBuffer.wrap(Base64.getDecoder().decode(legacy));
        int n = buf.getInt();
        for (int i = 0; i < n; i++) {
          int idx = buf.getInt();
          int count = buf.getInt();
          sparseGrid.put(idx, new long[] {count, 0});
        }
      }
    }

    // CRITICAL for windowing: the reconfigurator uses the FIRST report's profile -- built via
    // this constructor -- DIRECTLY as the stored aggregate (AggregateDemandProfiler.combine's
    // "else existing = update" branch), and reloads DB-persisted stats the same way. The loaded
    // grid must be backed by a window delta or it can never expire: that initial demand would sit
    // in the running grid with nothing to subtract it, leaving a permanent residual. Harmless when
    // this object is only a transient combine() argument, since combine() reads the argument's
    // sparseGrid, not its windowDeltas.
    if (windowMillis >= 0 && !sparseGrid.isEmpty()) {
      Map<Integer, long[]> copy = new HashMap<>(sparseGrid.size());
      for (Map.Entry<Integer, long[]> e : sparseGrid.entrySet()) {
        copy.put(e.getKey(), new long[] {e.getValue()[READ], e.getValue()[WRITE]});
      }
      windowDeltas.addLast(
          new TimedDelta(System.currentTimeMillis(), copy, totalReads, totalWrites));
    }
  }

  // Test-only: drive window expiry at a caller-supplied wall-clock so tests need not sleep.
  void expireForTesting(long now) {
    mapLock.lock();
    try {
      expireWindowLocked(now);
    } finally {
      mapLock.unlock();
    }
  }

  @Override
  public boolean shouldReportDemandStats(
      Request request, InetAddress sender, ReconfigurableAppInfo nodeConfig) {
    if (request == null || !request.getServiceName().equals(this.name)) {
      return false;
    }
    // A client contributes demand via its X-Client-Location header, or (when absent) via its
    // source IP geolocated off the hot path. When HTTP batching is enabled requests arrive as
    // XdnHttpRequestBatch; iterate its entries so per-request client locations still flow through.
    boolean enqueuedAny = false;
    if (request instanceof XdnHttpRequest xdnReq) {
      enqueuedAny = enqueueLocation(xdnReq, sender);
    } else if (request instanceof XdnHttpRequestBatch batch) {
      for (XdnHttpRequest sub : batch.getRequests()) {
        enqueuedAny |= enqueueLocation(sub, null);
      }
    } else {
      return false;
    }
    if (!enqueuedAny) {
      return false;
    }
    ensureWorkerStarted();

    long now = System.currentTimeMillis();
    long last = lastDemandReportTimestamp.get();
    if (last == 0) {
      // Treat the first observation as the start of the reporting window.
      lastDemandReportTimestamp.compareAndSet(0, now);
      return false;
    }
    if (now - last >= MIN_DEMAND_REPORT_PERIOD_MS
        && lastDemandReportTimestamp.compareAndSet(last, now)) {
      return true;
    }
    return false;
  }

  // Enqueue a demand sample for one request: the X-Client-Location header geolocation if present,
  // else the client IP (resolved to a geolocation in the worker via GeoIP). 'sender' is null for
  // batch sub-requests, which therefore only contribute via their header. The request's READ/WRITE
  // classification (cached on the request) rides along so the worker can bump the right counter.
  private boolean enqueueLocation(XdnHttpRequest req, InetAddress sender) {
    Object item = req.getClientGeolocation();
    if (item == null
        && geoIpResolver != null
        && sender != null
        && !GeoIpResolver.isNonGeolocatable(sender)) {
      item = sender; // resolved off the hot path in the worker
    }
    if (item == null) {
      return false;
    }
    if (!eventQueue.offer(new Sample(item, isWrite(req)))) {
      LOGGER.log(
          Level.FINE,
          "XdnGeoDemandProfiler event queue full, dropping demand sample for {0}",
          this.name);
      return false;
    }
    return true;
  }

  // A request counts as a WRITE iff it carries a WRITE_ONLY or READ_MODIFY_WRITE behavior (RMW
  // folds
  // into write since it mutates state). Everything else -- including the default when no matcher
  // was
  // applied, which is READ_MODIFY_WRITE -> write -- is conservative for placement. getBehaviors()
  // is
  // memoized on the request, so this is a cached lookup on the hot path.
  private static boolean isWrite(XdnHttpRequest req) {
    Set<RequestBehaviorType> b = req.getBehaviors();
    return b != null
        && (b.contains(RequestBehaviorType.WRITE_ONLY)
            || b.contains(RequestBehaviorType.READ_MODIFY_WRITE));
  }

  private void ensureWorkerStarted() {
    if (worker != null || workerDisabledForTesting) {
      return;
    }
    synchronized (this) {
      if (worker != null) {
        return;
      }
      ThreadFactory tf =
          r -> {
            Thread t = new Thread(r, "xdn-geoprofiler-" + this.name);
            t.setDaemon(true);
            return t;
          };
      ExecutorService es = Executors.newSingleThreadExecutor(tf);
      es.submit(this::workerLoop);
      worker = es;
    }
  }

  // Stop the async worker (interrupting its blocking take()) so it isn't leaked when this profile
  // is
  // discarded after a report. Coordinated with ensureWorkerStarted() via 'this' so they don't race
  // on the worker field; the shutdown itself runs outside that monitor.
  private void stopWorker() {
    ExecutorService es;
    synchronized (this) {
      es = worker;
      worker = null;
    }
    if (es != null) {
      es.shutdownNow();
    }
  }

  // Test-only: whether the async worker is currently running.
  boolean isWorkerActiveForTesting() {
    return worker != null;
  }

  private void workerLoop() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        recordSample(eventQueue.take());
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  // Resolve a sample's geolocation (GeoIP mmdb lookup for IP-based samples happens here, off the
  // request hot path) and increment its read/write cell. Shared by the async worker and by
  // drainPendingSamples().
  private void recordSample(Sample s) {
    Object item = s.loc();
    Geolocation geo;
    if (item instanceof Geolocation g) {
      geo = g;
    } else if (item instanceof InetAddress ip) {
      geo = geoIpResolver != null ? geoIpResolver.resolve(ip) : null;
    } else {
      return;
    }
    if (geo == null) {
      return; // GeoIP miss -> no demand cell
    }
    int idx = latToRow(geo.latitude()) * NUM_GRID_COLUMNS + lonToCol(geo.longitude());
    mapLock.lock();
    try {
      long[] cell = sparseGrid.computeIfAbsent(idx, k -> new long[2]);
      if (s.write()) {
        cell[WRITE]++;
        totalWrites++;
      } else {
        cell[READ]++;
        totalReads++;
      }
    } finally {
      mapLock.unlock();
    }
  }

  // Synchronously process samples the async worker hasn't drained yet. Called by getDemandStats so
  // a
  // report does not drop in-flight samples: on the ActiveReplica a report plucks-and-REPLACES this
  // profile (AggregateDemandProfiler.pluckDemandProfile), orphaning the worker and its queued
  // samples -- which under-counts demand, most visibly for writes whose completion callbacks
  // enqueue
  // in a delayed burst. The worker may drain concurrently; each queued sample goes to exactly one
  // of
  // us, so at most the worker's single in-hand sample can still slip to the next report.
  private void drainPendingSamples() {
    Sample s;
    while ((s = eventQueue.poll()) != null) {
      recordSample(s);
    }
  }

  @Override
  public JSONObject getDemandStats() {
    // Drain any samples still queued for the async worker so this report (which snapshots + resets,
    // and on the AR replaces this whole profile) doesn't lose them -- the cause of under-counted
    // (esp. write) demand.
    drainPendingSamples();
    // Snapshot + reset under the lock so each report covers demand since the previous report.
    List<long[]> entries; // {idx, read, write}
    long numReads;
    long numWrites;
    mapLock.lock();
    try {
      entries = new ArrayList<>(sparseGrid.size());
      for (Map.Entry<Integer, long[]> e : sparseGrid.entrySet()) {
        entries.add(new long[] {e.getKey(), e.getValue()[READ], e.getValue()[WRITE]});
      }
      numReads = totalReads;
      numWrites = totalWrites;
      sparseGrid.clear();
      totalReads = 0;
      totalWrites = 0;
    } finally {
      mapLock.unlock();
    }

    // A report means the reconfigurator is about to discard this profile and swap in a fresh one
    // (AggregateDemandProfiler.pluckDemandProfile), which would otherwise orphan the worker -- a
    // leaked daemon thread blocked on take() per report. Stop it here; ensureWorkerStarted()
    // re-creates it on the next sample if this profile is reused instead.
    stopWorker();

    // Top-K trim (by total demand per cell) to stay under MAX_DEMAND_PROFILE_SIZE.
    if (entries.size() > MAX_GRID_ENTRIES_PER_REPORT) {
      entries.sort((a, b) -> Long.compare(b[1] + b[2], a[1] + a[2]));
      entries = entries.subList(0, MAX_GRID_ENTRIES_PER_REPORT);
    }

    ByteBuffer buf = ByteBuffer.allocate(4 + entries.size() * 12);
    buf.putInt(entries.size());
    for (long[] e : entries) {
      buf.putInt((int) e[0]); // idx
      buf.putInt((int) e[1]); // read
      buf.putInt((int) e[2]); // write
    }
    String b64 = Base64.getEncoder().encodeToString(buf.array());

    JSONObject stats = new JSONObject();
    try {
      stats.put(KEY_NAME, this.name);
      stats.put(KEY_NUM_READS, numReads);
      stats.put(KEY_NUM_WRITES, numWrites);
      stats.put(KEY_NUM_REQS, numReads + numWrites); // back-compat
      stats.put(KEY_GRID_RW_B64, b64);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    return stats;
  }

  /**
   * Read-only snapshot of the demand grid as {@code {lat, lon, read, write, count}} cells (where
   * {@code count = read + write}), WITHOUT resetting (unlike {@link #getDemandStats()}). Applies
   * the rolling window first so the dashboard sees current demand. Used by the geo-demand heatmap;
   * each populated grid cell becomes one point at its center.
   */
  @Override
  public org.json.JSONArray getDemandGeoCells() {
    org.json.JSONArray cells = new org.json.JSONArray();
    mapLock.lock();
    try {
      expireWindowLocked(System.currentTimeMillis());
      for (Map.Entry<Integer, long[]> e : sparseGrid.entrySet()) {
        int idx = e.getKey();
        long r = e.getValue()[READ];
        long w = e.getValue()[WRITE];
        JSONObject cell = new JSONObject();
        cell.put("lat", rowCenterToLat(idx / NUM_GRID_COLUMNS));
        cell.put("lon", colCenterToLon(idx % NUM_GRID_COLUMNS));
        cell.put("read", r);
        cell.put("write", w);
        cell.put("count", r + w);
        cells.put(cell);
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    } finally {
      mapLock.unlock();
    }
    return cells;
  }

  @Override
  public void combine(AbstractDemandProfile update) {
    assert update instanceof XdnGeoDemandProfiler : "Invalid profiler type";
    XdnGeoDemandProfiler incoming = (XdnGeoDemandProfiler) update;
    assert incoming.name.equals(this.name) : "Expecting profiler for the same service";

    mapLock.lock();
    try {
      long now = System.currentTimeMillis();

      // Fold the incoming report into the running grid (per kind).
      for (Map.Entry<Integer, long[]> e : incoming.sparseGrid.entrySet()) {
        long[] cell = sparseGrid.computeIfAbsent(e.getKey(), k -> new long[2]);
        cell[READ] += e.getValue()[READ];
        cell[WRITE] += e.getValue()[WRITE];
      }
      totalReads += incoming.totalReads;
      totalWrites += incoming.totalWrites;

      // Windowed aggregation: remember this delta (with its timestamp) so it can be expired later,
      // then drop anything that has now aged out. Cumulative (windowMillis < 0) keeps everything.
      if (windowMillis >= 0) {
        Map<Integer, long[]> copy = new HashMap<>(incoming.sparseGrid.size());
        for (Map.Entry<Integer, long[]> e : incoming.sparseGrid.entrySet()) {
          copy.put(e.getKey(), new long[] {e.getValue()[READ], e.getValue()[WRITE]});
        }
        windowDeltas.addLast(new TimedDelta(now, copy, incoming.totalReads, incoming.totalWrites));
        expireWindowLocked(now);
      }
    } finally {
      mapLock.unlock();
    }
  }

  // Subtract demand deltas older than the rolling window from the running grid. Caller holds
  // mapLock.
  // No-op when windowMillis < 0 (cumulative).
  private void expireWindowLocked(long now) {
    if (windowMillis < 0) {
      return;
    }
    long cutoff = now - windowMillis;
    while (!windowDeltas.isEmpty() && windowDeltas.peekFirst().ts <= cutoff) {
      TimedDelta d = windowDeltas.removeFirst();
      for (Map.Entry<Integer, long[]> e : d.grid.entrySet()) {
        long[] cell = sparseGrid.get(e.getKey());
        if (cell == null) {
          continue;
        }
        cell[READ] -= e.getValue()[READ];
        cell[WRITE] -= e.getValue()[WRITE];
        if (cell[READ] <= 0 && cell[WRITE] <= 0) {
          sparseGrid.remove(e.getKey());
        }
      }
      totalReads = Math.max(0, totalReads - d.reads);
      totalWrites = Math.max(0, totalWrites - d.writes);
    }
  }

  @Override
  public Set<String> reconfigure(Set<String> curActives, ReconfigurableAppInfo appInfo) {
    NodeIdsMetadataPair<String> result = this.getNewActivesPlacement(curActives, appInfo);
    return result == null ? null : result.nodeIds();
  }

  @Override
  public NodeIdsMetadataPair<String> getNewActivesPlacement(
      Set<String> curActives, ReconfigurableAppInfo appInfo) {
    // Apply the rolling window before reading demand so placement tracks current load.
    mapLock.lock();
    try {
      expireWindowLocked(System.currentTimeMillis());
    } finally {
      mapLock.unlock();
    }

    if (this.totalReads + this.totalWrites == 0 || this.sparseGrid.isEmpty()) {
      return null;
    }
    if (curActives == null || curActives.isEmpty()) {
      return null;
    }

    Map<String, Geolocation> nodeGeo = appInfo.getActiveReplicaGeolocations();
    // TODO: thread ServiceProperty.numReplicas through a future ReconfigurableAppInfo method so
    // replica-count changes (scale up / down) are honored. For now we target the current group
    // size, which equals the configured --num-replicas in steady state.
    int targetReplicas = Math.min(curActives.size(), nodeGeo.size());
    if (targetReplicas == 0 || nodeGeo.size() < curActives.size()) {
      // Not enough geolocated nodes to place a full replica group; skip reconfiguration.
      return null;
    }

    double[] centroid = calculateCentroidLatLon();
    Set<String> newActives = findClosestServers(centroid[0], centroid[1], targetReplicas, nodeGeo);
    String preferredCoordinator =
        findClosestServers(centroid[0], centroid[1], 1, nodeGeo).iterator().next();

    JSONObject metadataJson = new JSONObject();
    try {
      metadataJson.put(Keys.PREFERRED_COORDINATOR.toString(), preferredCoordinator);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    LOGGER.log(
        Level.FINE,
        "XdnGeoDemandProfiler placement for {0}: centroid=({1},{2}) actives={3} coord={4}",
        new Object[] {this.name, centroid[0], centroid[1], newActives, preferredCoordinator});

    return new NodeIdsMetadataPair<>(newActives, metadataJson.toString());
  }

  // Demand-weighted centroid over total (read + write) demand per cell.
  private double[] calculateCentroidLatLon() {
    double totalWeight = 0;
    double weightedLatSum = 0;
    double weightedLonSum = 0;
    mapLock.lock();
    try {
      for (Map.Entry<Integer, long[]> e : sparseGrid.entrySet()) {
        int idx = e.getKey();
        long count = e.getValue()[READ] + e.getValue()[WRITE];
        double lat = rowCenterToLat(idx / NUM_GRID_COLUMNS);
        double lon = colCenterToLon(idx % NUM_GRID_COLUMNS);
        totalWeight += count;
        weightedLatSum += lat * count;
        weightedLonSum += lon * count;
      }
    } finally {
      mapLock.unlock();
    }
    if (totalWeight == 0) {
      return new double[] {0.0, 0.0};
    }
    return new double[] {weightedLatSum / totalWeight, weightedLonSum / totalWeight};
  }

  private Set<String> findClosestServers(
      double lat, double lon, int numClosest, Map<String, Geolocation> nodeGeo) {
    record ServerDistance(String id, double distance) {}
    List<ServerDistance> servers = new ArrayList<>(nodeGeo.size());
    for (Map.Entry<String, Geolocation> e : nodeGeo.entrySet()) {
      Geolocation g = e.getValue();
      double dLat = g.latitude() - lat;
      double dLon = g.longitude() - lon;
      servers.add(new ServerDistance(e.getKey(), dLat * dLat + dLon * dLon));
    }
    servers.sort(Comparator.comparingDouble(ServerDistance::distance));
    Set<String> result = new HashSet<>();
    for (int i = 0; i < numClosest && i < servers.size(); i++) {
      result.add(servers.get(i).id());
    }
    return result;
  }

  @Override
  public void justReconfigured() {
    // no-op
  }

  // Cell math: latitude ∈ [-90, 90] maps north-to-south to row ∈ [0, NUM_GRID_ROWS), longitude ∈
  // [-180, 180] maps west-to-east to col ∈ [0, NUM_GRID_COLUMNS).
  private static int latToRow(double latitude) {
    int row = (int) Math.floor((90.0 - latitude) * NUM_GRID_ROWS / 180.0);
    return Math.max(0, Math.min(NUM_GRID_ROWS - 1, row));
  }

  private static int lonToCol(double longitude) {
    int col = (int) Math.floor((longitude + 180.0) * NUM_GRID_COLUMNS / 360.0);
    return Math.max(0, Math.min(NUM_GRID_COLUMNS - 1, col));
  }

  private static double rowCenterToLat(int row) {
    return 90.0 - (row + 0.5) * 180.0 / NUM_GRID_ROWS;
  }

  private static double colCenterToLon(int col) {
    return -180.0 + (col + 0.5) * 360.0 / NUM_GRID_COLUMNS;
  }
}
