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
    // The rehydrated profiler's sparse map matches the original.
    assertEquals(first.getString("grid_sparse_b64"), rehydrated.getString("grid_sparse_b64"));

    // Second call on the original should now be empty (reset-on-report).
    JSONObject second = profiler.getDemandStats();
    assertEquals(0L, second.getLong("num_reqs"));
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
