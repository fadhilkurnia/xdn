package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.reconfigurationutils.NodeIdsMetadataPair;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.DefaultHttpContent;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.DefaultHttpRequest;
import io.netty.handler.codec.http.HttpContent;
import io.netty.handler.codec.http.HttpMethod;
import io.netty.handler.codec.http.HttpRequest;
import io.netty.handler.codec.http.HttpVersion;
import java.io.File;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;

public class XdnGeoDemandProfilerTest {

  private static final String SERVICE_NAME = "svc-geo-test";

  @Test
  public void testRoundTripAndResetOnReport() throws Exception {
    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);

    // Feed 5 events from Boston-ish coordinates.
    for (int i = 0; i < 5; i++) {
      profiler.shouldReportDemandStats(makeRequest(42.36, -71.06), null, null);
    }
    awaitWorkerDrain(profiler, 5);

    JSONObject first = profiler.getDemandStats();
    assertEquals(SERVICE_NAME, first.getString("name"));
    assertEquals(5L, first.getLong("num_reqs"));

    // Round-trip through the JSON ctor.
    XdnGeoDemandProfiler round = new XdnGeoDemandProfiler(first);
    JSONObject rehydrated = round.getDemandStats();
    assertEquals(5L, rehydrated.getLong("num_reqs"));
    // The rehydrated profiler's sparse (read/write) map matches the original.
    assertEquals(first.getString("grid_rw_b64"), rehydrated.getString("grid_rw_b64"));

    // Second call on the original should now be empty (reset-on-report).
    JSONObject second = profiler.getDemandStats();
    assertEquals(0L, second.getLong("num_reqs"));
  }

  @Test
  public void testWindowedFirstReportProfileExpires() throws Exception {
    // Reproduces the "stuck residual" bug: the reconfigurator stores the FIRST demand report's
    // profile (built via the JSON ctor) DIRECTLY as the aggregate (AggregateDemandProfiler.combine
    // 'else existing = update'). With a window configured, that loaded grid must be expirable --
    // before the fix the JSON ctor seeded no window delta, so the first report's demand never
    // decayed (we observed exactly this live: one read cell frozen while all later cells expired).
    XdnGeoDemandProfiler.setWindowMillisForTesting(1_000L); // 1s window
    try {
      // Build a report carrying some demand.
      XdnGeoDemandProfiler src = new XdnGeoDemandProfiler(SERVICE_NAME);
      for (int i = 0; i < 5; i++) {
        src.shouldReportDemandStats(makeRequest(34.05, -118.24), null, null);
      }
      awaitWorkerDrain(src, 5);
      JSONObject report = src.getDemandStats();

      // The "first report becomes the stored profile" path: construct from the report JSON.
      XdnGeoDemandProfiler stored = new XdnGeoDemandProfiler(report);
      long t0 = System.currentTimeMillis();
      assertEquals(
          1, stored.getDemandGeoCells().length(), "demand present right after becoming the store");

      // Past the window, the demand must expire -- before the fix this residual stayed forever.
      stored.expireForTesting(t0 + 60_000);
      assertEquals(
          0,
          stored.getDemandGeoCells().length(),
          "windowed demand from the first report must expire (no stuck residual)");
    } finally {
      XdnGeoDemandProfiler.setWindowMillisForTesting(null);
    }
  }

  @Test
  public void testReportDrainsInFlightSamples() throws Exception {
    // Reproduces write-demand under-sampling: sampling is async (a worker drains the queue into the
    // grid), but a report snapshots only the grid AND on the AR replaces the whole profile,
    // dropping
    // any still-queued samples. Disable the worker so samples stay queued, then assert the report
    // drains them. Before the fix this returned 0 (queued samples lost); after, all are counted.
    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);
    profiler.setWorkerDisabledForTesting(true);
    for (int i = 0; i < 7; i++) {
      profiler.shouldReportDemandStats(makeRequest(40.0, -75.0), null, null);
    }
    JSONObject stats = profiler.getDemandStats();
    assertEquals(7L, stats.getLong("num_reqs"), "report must drain in-flight async samples");
  }

  @Test
  public void testReportStopsWorkerToAvoidLeak() throws Exception {
    // On the ActiveReplica a report (getDemandStats) is immediately followed by the reconfigurator
    // discarding this profile and swapping in a fresh one
    // (AggregateDemandProfiler.pluckDemandProfile).
    // The per-profile worker must be stopped on report, or it leaks (a blocked thread per report).
    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);
    profiler.shouldReportDemandStats(makeRequest(40.0, -75.0), null, null); // starts the worker
    assertTrue(profiler.isWorkerActiveForTesting(), "worker runs while sampling");

    profiler.getDemandStats(); // a report
    assertFalse(
        profiler.isWorkerActiveForTesting(), "worker must be stopped after a report (no leak)");

    // If the profile is reused, sampling resumes (worker re-created on the next sample).
    profiler.shouldReportDemandStats(makeRequest(40.0, -75.0), null, null);
    assertTrue(profiler.isWorkerActiveForTesting(), "worker restarts on the next sample");
  }

  @Test
  public void testNullClientGeoIsIgnored() throws Exception {
    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);
    // No X-Client-Location header.
    Request req = makeRequestNoGeo();
    assertFalse(profiler.shouldReportDemandStats(req, null, null));

    JSONObject stats = profiler.getDemandStats();
    assertEquals(0L, stats.getLong("num_reqs"));
  }

  // Relative path to the GeoLite2-City db shipped in the repo (also used by xdn-dns).
  private static final String MMDB_PATH = "xdn-dns/geolocation_city_data.mmdb";

  @Test
  public void testIpFallbackProducesDemandCell() throws Exception {
    File mmdb = new File(MMDB_PATH);
    assumeTrue(mmdb.isFile(), "GeoLite2-City db not present at " + MMDB_PATH + "; skipping");

    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);
    profiler.setGeoIpResolverForTesting(new GeoIpResolver(mmdb));

    // A header-less request from a public client IP (UMass Amherst, ~42.37,-72.47). With no
    // X-Client-Location header, the profiler must geolocate the source IP off the hot path.
    InetAddress umass = InetAddress.getByName("128.119.240.84");
    profiler.shouldReportDemandStats(makeRequestNoGeo(), umass, null);
    awaitWorkerDrain(profiler, 1);

    // getDemandGeoCells() is the non-destructive read; query it before any getDemandStats() call
    // (which would snapshot-and-reset the grid). The single demand cell should sit on Amherst's
    // grid cell (quantized, so allow a wide margin).
    JSONArray cells = profiler.getDemandGeoCells();
    assertEquals(1, cells.length(), "expected exactly one demand cell");
    JSONObject cell = cells.getJSONObject(0);
    assertEquals(1, cell.getInt("count"));
    assertTrue(Math.abs(cell.getDouble("lat") - 42.37) < 1.0, "lat near Amherst: " + cell);
    assertTrue(Math.abs(cell.getDouble("lon") - (-72.47)) < 1.0, "lon near Amherst: " + cell);
  }

  @Test
  public void testLocalIpProducesNoDemand() throws Exception {
    File mmdb = new File(MMDB_PATH);
    assumeTrue(mmdb.isFile(), "GeoLite2-City db not present at " + MMDB_PATH + "; skipping");

    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);
    profiler.setGeoIpResolverForTesting(new GeoIpResolver(mmdb));

    // A loopback client is non-geolocatable: header-less + local IP contributes no demand.
    Request req = makeRequestNoGeo();
    assertFalse(profiler.shouldReportDemandStats(req, InetAddress.getByName("127.0.0.1"), null));

    Thread.sleep(50); // give any (erroneously) enqueued worker item a chance to land
    assertEquals(0L, profiler.getDemandStats().getLong("num_reqs"));
    assertEquals(0, profiler.getDemandGeoCells().length());
  }

  @Test
  public void testWrongServiceIsIgnored() throws Exception {
    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);
    // Build a request for a different service name.
    Request other = makeRequestForService("different-svc", 42.0, -71.0);
    assertFalse(profiler.shouldReportDemandStats(other, null, null));
    assertEquals(0L, profiler.getDemandStats().getLong("num_reqs"));
  }

  @Test
  public void testCentroidPicksClosestReplicaGroup() throws Exception {
    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);

    // Demand biased to (42, -71) — Boston area.
    for (int i = 0; i < 50; i++) {
      profiler.shouldReportDemandStats(makeRequest(42.0, -71.0), null, null);
    }
    awaitWorkerDrain(profiler, 50);

    Map<String, Geolocation> nodeGeo = new HashMap<>();
    nodeGeo.put("AR_boston", new Geolocation(42.3, -71.1));
    nodeGeo.put("AR_london", new Geolocation(51.5, -0.1));
    nodeGeo.put("AR_tokyo", new Geolocation(35.7, 139.7));
    nodeGeo.put("AR_sydney", new Geolocation(-33.9, 151.2));

    ReconfigurableAppInfo appInfo = makeAppInfo(nodeGeo);
    Set<String> curActives = Set.of("AR_london", "AR_tokyo");
    NodeIdsMetadataPair<String> result = profiler.getNewActivesPlacement(curActives, appInfo);

    assertNotNull(result);
    assertEquals(2, result.nodeIds().size());
    assertTrue(result.nodeIds().contains("AR_boston"), "Boston must be among closest to demand");
    assertTrue(result.placementMetadata().contains("AR_boston"), "Boston must be preferred coord");
  }

  @Test
  public void testNoReconfigWhenNoDemand() {
    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);
    Map<String, Geolocation> nodeGeo = new HashMap<>();
    nodeGeo.put("AR_a", new Geolocation(0.0, 0.0));
    nodeGeo.put("AR_b", new Geolocation(10.0, 10.0));
    ReconfigurableAppInfo appInfo = makeAppInfo(nodeGeo);
    assertNull(profiler.getNewActivesPlacement(Set.of("AR_a", "AR_b"), appInfo));
  }

  @Test
  public void testNoReconfigWhenAppInfoHasNoGeo() throws Exception {
    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);
    profiler.shouldReportDemandStats(makeRequest(42.0, -71.0), null, null);
    awaitWorkerDrain(profiler, 1);
    ReconfigurableAppInfo appInfo = makeAppInfo(Map.of());
    assertNull(profiler.getNewActivesPlacement(Set.of("AR_a", "AR_b"), appInfo));
  }

  @Test
  public void testTopKTrimUnderSizeCap() throws Exception {
    XdnGeoDemandProfiler profiler = new XdnGeoDemandProfiler(SERVICE_NAME);

    // Populate many distinct cells so the sparse map exceeds the top-K budget.
    // Walk latitude in 0.1-degree steps and send a few hits per cell.
    int sent = 0;
    for (int i = 0; i < 500; i++) {
      double lat = -70.0 + 0.25 * i;
      if (lat > 70.0) break;
      double lon = -170.0 + 0.25 * i;
      if (lon > 170.0) break;
      int hits = (i % 5) + 1; // 1..5 hits per cell
      for (int h = 0; h < hits; h++) {
        profiler.shouldReportDemandStats(makeRequest(lat, lon), null, null);
        sent++;
      }
    }
    awaitWorkerDrain(profiler, sent);

    JSONObject stats = profiler.getDemandStats();
    // Payload must fit within the configured DB cap (4096 bytes).
    assertTrue(
        stats.toString().length() < 4096,
        "Serialized stats exceed MAX_DEMAND_PROFILE_SIZE: " + stats.toString().length());
    // Round-trip survives and does not crash on truncation.
    XdnGeoDemandProfiler round = new XdnGeoDemandProfiler(stats);
    assertNotNull(round.getDemandStats());
  }

  // --- helpers ---

  private static void awaitWorkerDrain(XdnGeoDemandProfiler profiler, long expectedTotal)
      throws Exception {
    // getDemandStats() blocks on the same ReentrantLock the worker uses, but the queue drain is
    // asynchronous. Poll a few times to let the worker catch up.
    long deadline = System.currentTimeMillis() + 2000;
    while (System.currentTimeMillis() < deadline) {
      // Peek num_reqs via a dummy snapshot call isn't ideal because it resets state. Instead we
      // just sleep a short while — worker consumes events well under 1 ms each.
      Thread.sleep(25);
      // Snapshot-and-return: we consume and then re-inject so the next caller sees the same
      // aggregate. This keeps the test helper non-destructive.
      JSONObject s = profiler.getDemandStats();
      long gotReqs = s.getLong("num_reqs");
      // Re-inject by combining a rehydrated profiler back in.
      if (gotReqs > 0) {
        profiler.combine(new XdnGeoDemandProfiler(s));
      }
      if (gotReqs >= expectedTotal) {
        return;
      }
    }
    throw new AssertionError(
        "Worker did not drain " + expectedTotal + " events in time for " + profiler);
  }

  private static Request makeRequest(double lat, double lon) {
    return makeRequestForService(SERVICE_NAME, lat, lon);
  }

  private static Request makeRequestForService(String serviceName, double lat, double lon) {
    HttpRequest raw =
        new DefaultHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.GET,
            "/?_xdnsvc=" + serviceName,
            new DefaultHttpHeaders()
                .add("XDN", serviceName)
                .add(XdnHttpRequest.X_CLIENT_LOCATION_HEADER, lat + "," + lon));
    HttpContent content =
        new DefaultHttpContent(Unpooled.copiedBuffer("x".getBytes(StandardCharsets.UTF_8)));
    return new XdnHttpRequest(raw, content);
  }

  private static Request makeRequestNoGeo() {
    HttpRequest raw =
        new DefaultHttpRequest(
            HttpVersion.HTTP_1_1,
            HttpMethod.GET,
            "/?_xdnsvc=" + SERVICE_NAME,
            new DefaultHttpHeaders().add("XDN", SERVICE_NAME));
    HttpContent content =
        new DefaultHttpContent(Unpooled.copiedBuffer("x".getBytes(StandardCharsets.UTF_8)));
    return new XdnHttpRequest(raw, content);
  }

  private static ReconfigurableAppInfo makeAppInfo(Map<String, Geolocation> nodeGeo) {
    return new ReconfigurableAppInfo() {
      @Override
      public Set<String> getReplicaGroup(String serviceName) {
        return nodeGeo.keySet();
      }

      @Override
      public String snapshot(String serviceName) {
        return null;
      }

      @Override
      public Map<String, InetSocketAddress> getAllActiveReplicas() {
        Map<String, InetSocketAddress> m = new HashMap<>();
        for (String id : nodeGeo.keySet()) {
          m.put(id, new InetSocketAddress("127.0.0.1", 0));
        }
        return m;
      }

      @Override
      public Map<String, Geolocation> getActiveReplicaGeolocations() {
        return nodeGeo;
      }
    };
  }
}
