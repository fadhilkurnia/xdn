package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
import edu.umass.cs.reconfiguration.reconfigurationutils.NodeIdsMetadataPair;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Comparator;
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
 * longitude grid and accumulates per-cell request counts from clients that explicitly supply their
 * geolocation via the {@code X-Client-Location} HTTP header (parsed into {@link
 * XdnHttpRequest#getClientGeolocation()}).
 *
 * <p>Aggregation runs on a background worker per profiler instance: {@code shouldReportDemandStats}
 * only computes the cell index and enqueues it, keeping the request hot path cheap. {@code
 * getDemandStats} snapshots the sparse counts, serializes them, then resets the local map so each
 * report covers demand since the previous report (the reconfigurator's {@link #combine} accumulates
 * across reports).
 *
 * <p>On the reconfigurator side, {@link #getNewActivesPlacement} computes the weighted centroid of
 * observed demand and picks the {@code curActives.size()} node IDs closest to that centroid (using
 * node geolocations from {@link ReconfigurableAppInfo#getActiveReplicaGeolocations()}). The closest
 * node is also surfaced via the {@code PREFERRED_COORDINATOR} placement metadata so the paxos layer
 * can try to make it the leader.
 *
 * <p>Prototype limitations (future work): IP → (lat, lon) inference for clients that don't send
 * {@code X-Client-Location}; threading the service's configured {@code --num-replicas} through
 * {@link edu.umass.cs.xdn.service.ServiceProperty} (today the replica count is derived from the
 * current group size); top-K serialization truncation can discard tail demand under the 4 KB DB
 * cap.
 */
public class XdnGeoDemandProfiler extends AbstractDemandProfile {

  private static final int NUM_GRID_ROWS = 1000;
  private static final int NUM_GRID_COLUMNS = 1000;

  // At most one demand report every 10 seconds per profiler instance.
  private static final long MIN_DEMAND_REPORT_PERIOD_MS = 10_000;

  // ReconfigurationConfig.MAX_DEMAND_PROFILE_SIZE defaults to 4096 bytes. Each serialized cell is
  // 8 bytes (int32 index, int32 count) plus a 4-byte header; base64 inflates the string 4:3; the
  // JSONObject also carries "name" and "num_reqs". 300 entries is a safe conservative budget.
  private static final int MAX_GRID_ENTRIES_PER_REPORT = 300;

  // Bounded queue for the hot path; drops on overflow rather than back-pressuring request handling.
  private static final int EVENT_QUEUE_CAPACITY = 16_384;

  private static final String KEY_NUM_REQS = "num_reqs";
  private static final String KEY_GRID_SPARSE_B64 = "grid_sparse_b64";
  private static final String KEY_NAME = "name";

  private static final Logger LOGGER = Logger.getLogger(XdnGeoDemandProfiler.class.getName());

  // The hot path only captures the client Geolocation reference; lat/lon -> (row, col)
  // conversion is deferred to the worker so shouldReportDemandStats stays minimal.
  // Sparse cell counts, key = row * NUM_GRID_COLUMNS + col. Written only by the worker thread or by
  // the constructor path; read by getDemandStats() under mapLock.
  private final HashMap<Integer, Integer> sparseGrid = new HashMap<>();
  private long totalRequests = 0;
  private final ReentrantLock mapLock = new ReentrantLock();

  private final AtomicLong lastDemandReportTimestamp = new AtomicLong(0);

  // Lazily started worker: one daemon thread per profiler instance.
  private final LinkedBlockingQueue<Geolocation> eventQueue =
      new LinkedBlockingQueue<>(EVENT_QUEUE_CAPACITY);
  private volatile ExecutorService worker;

  public XdnGeoDemandProfiler(String name) {
    super(name);
  }

  public XdnGeoDemandProfiler(JSONObject stats) throws JSONException {
    super(stats.getString(KEY_NAME));
    if (stats.has(KEY_NUM_REQS)) {
      this.totalRequests = stats.getLong(KEY_NUM_REQS);
    }
    String b64 = stats.optString(KEY_GRID_SPARSE_B64, null);
    if (b64 == null || b64.isEmpty()) {
      return;
    }
    byte[] raw = Base64.getDecoder().decode(b64);
    ByteBuffer buf = ByteBuffer.wrap(raw);
    int n = buf.getInt();
    for (int i = 0; i < n; i++) {
      int idx = buf.getInt();
      int count = buf.getInt();
      sparseGrid.put(idx, count);
    }
  }

  @Override
  public boolean shouldReportDemandStats(
      Request request, InetAddress sender, ReconfigurableAppInfo nodeConfig) {
    if (request == null || !request.getServiceName().equals(this.name)) {
      return false;
    }
    // Prototype: only clients that supply X-Client-Location contribute demand.
    // When HTTP batching is enabled requests arrive as XdnHttpRequestBatch; iterate
    // its entries so per-request client locations still flow through.
    boolean enqueuedAny = false;
    if (request instanceof XdnHttpRequest xdnReq) {
      enqueuedAny = enqueueIfGeo(xdnReq);
    } else if (request instanceof XdnHttpRequestBatch batch) {
      for (XdnHttpRequest sub : batch.getRequests()) {
        enqueuedAny |= enqueueIfGeo(sub);
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

  private boolean enqueueIfGeo(XdnHttpRequest req) {
    Geolocation geo = req.getClientGeolocation();
    if (geo == null) {
      return false;
    }
    if (!eventQueue.offer(geo)) {
      LOGGER.log(
          Level.FINE,
          "XdnGeoDemandProfiler event queue full, dropping demand sample for {0}",
          this.name);
      return false;
    }
    return true;
  }

  private void ensureWorkerStarted() {
    if (worker != null) {
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

  private void workerLoop() {
    try {
      while (!Thread.currentThread().isInterrupted()) {
        Geolocation geo = eventQueue.take();
        int row = latToRow(geo.latitude());
        int col = lonToCol(geo.longitude());
        int idx = row * NUM_GRID_COLUMNS + col;
        mapLock.lock();
        try {
          sparseGrid.merge(idx, 1, Integer::sum);
          totalRequests++;
        } finally {
          mapLock.unlock();
        }
      }
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public JSONObject getDemandStats() {
    // Snapshot + reset under the lock so each report covers demand since the previous report.
    List<int[]> entries;
    long numReqs;
    mapLock.lock();
    try {
      entries = new ArrayList<>(sparseGrid.size());
      for (Map.Entry<Integer, Integer> e : sparseGrid.entrySet()) {
        entries.add(new int[] {e.getKey(), e.getValue()});
      }
      numReqs = totalRequests;
      sparseGrid.clear();
      totalRequests = 0;
    } finally {
      mapLock.unlock();
    }

    // Top-K trim to stay under ReconfigurationConfig.MAX_DEMAND_PROFILE_SIZE.
    if (entries.size() > MAX_GRID_ENTRIES_PER_REPORT) {
      entries.sort((a, b) -> Integer.compare(b[1], a[1]));
      entries = entries.subList(0, MAX_GRID_ENTRIES_PER_REPORT);
    }

    ByteBuffer buf = ByteBuffer.allocate(4 + entries.size() * 8);
    buf.putInt(entries.size());
    for (int[] e : entries) {
      buf.putInt(e[0]);
      buf.putInt(e[1]);
    }
    String b64 = Base64.getEncoder().encodeToString(buf.array());

    JSONObject stats = new JSONObject();
    try {
      stats.put(KEY_NAME, this.name);
      stats.put(KEY_NUM_REQS, numReqs);
      stats.put(KEY_GRID_SPARSE_B64, b64);
    } catch (JSONException e) {
      throw new RuntimeException(e);
    }
    return stats;
  }

  @Override
  public void combine(AbstractDemandProfile update) {
    assert update instanceof XdnGeoDemandProfiler : "Invalid profiler type";
    XdnGeoDemandProfiler incoming = (XdnGeoDemandProfiler) update;
    assert incoming.name.equals(this.name) : "Expecting profiler for the same service";

    mapLock.lock();
    try {
      this.totalRequests += incoming.totalRequests;
      for (Map.Entry<Integer, Integer> e : incoming.sparseGrid.entrySet()) {
        this.sparseGrid.merge(e.getKey(), e.getValue(), Integer::sum);
      }
    } finally {
      mapLock.unlock();
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
    if (this.totalRequests == 0 || this.sparseGrid.isEmpty()) {
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

  private double[] calculateCentroidLatLon() {
    double totalWeight = 0;
    double weightedLatSum = 0;
    double weightedLonSum = 0;
    for (Map.Entry<Integer, Integer> e : sparseGrid.entrySet()) {
      int idx = e.getKey();
      int count = e.getValue();
      int row = idx / NUM_GRID_COLUMNS;
      int col = idx % NUM_GRID_COLUMNS;
      double lat = rowCenterToLat(row);
      double lon = colCenterToLon(col);
      totalWeight += count;
      weightedLatSum += lat * count;
      weightedLonSum += lon * count;
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
