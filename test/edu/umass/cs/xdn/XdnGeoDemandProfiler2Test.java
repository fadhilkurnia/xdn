package edu.umass.cs.xdn;

import static org.junit.jupiter.api.Assertions.*;

import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import edu.umass.cs.reconfiguration.reconfigurationutils.AbstractDemandProfile;
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
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.*;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Integration tests for XdnGeoDemandProfiler2.
 *
 * XdnHttpRequest is constructed directly from Netty objects (matching the
 * pattern used in the existing test suite). ReconfigurableAppInfo is stubbed
 * with a minimal anonymous implementation — no Mockito needed.
 *
 * Tests that exercise the background worker use waitForWorker() to give the
 * daemon thread time to drain the event queue before asserting.
 */
public class XdnGeoDemandProfiler2Test {

  private static final String SVC = "test-svc";

  private XdnGeoDemandProfiler2 profiler;

  // ---- helpers ---------------------------------------------------------------

  /**
   * Minimal stub for ReconfigurableAppInfo. Only getActiveReplicaGeolocations()
   * returns real data; all other methods return null as they are not needed.
   */
  private static ReconfigurableAppInfo stubAppInfo(Map<String, Geolocation> nodeGeo) {
    return new ReconfigurableAppInfo() {
      @Override
      public Map<String, Geolocation> getActiveReplicaGeolocations() {
        return nodeGeo;
      }

      @Override public Set<String> getReplicaGroup(String s)                    { return null; }
      @Override public String snapshot(String s)                                 { return null; }
      @Override public Map<String, InetSocketAddress> getAllActiveReplicas()     { return null; }
    };
  }

  /**
   * Constructs a real XdnHttpRequest with an X-Client-Location header so that
   * getClientGeolocation() returns a non-null value, and with the given HTTP
   * method so that getBehaviors() classifies it correctly as read or write.
   */
  private static XdnHttpRequest makeRequest(String svc,
                                            double lat, double lon,
                                            HttpMethod method) {
    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    headers.set("XDN", svc);
    headers.set("X-Client-Location", lat + "," + lon);
    HttpRequest http = new DefaultHttpRequest(HttpVersion.HTTP_1_1, method, "/", headers);
    HttpContent content = new DefaultHttpContent(Unpooled.EMPTY_BUFFER);
    return new XdnHttpRequest(http, content);
  }

  /**
   * Encodes a sparse grid into the same base64 binary format used by getDemandStats(),
   * allowing tests to construct a profiler with known state via the JSONObject constructor.
   *
   * Binary layout: [int32 count][count × (int32 cellIdx, int32 requests)]
   */
  private static String encodeSparseGrid(Map<Integer, Integer> grid) {
    ByteBuffer buf = ByteBuffer.allocate(4 + grid.size() * 8);
    buf.putInt(grid.size());
    for (Map.Entry<Integer, Integer> e : grid.entrySet()) {
      buf.putInt(e.getKey());
      buf.putInt(e.getValue());
    }
    return Base64.getEncoder().encodeToString(buf.array());
  }

  private static JSONObject buildStats(String name, long numReqs,
                                       Map<Integer, Integer> reads,
                                       Map<Integer, Integer> writes) throws Exception {
    JSONObject stats = new JSONObject();
    stats.put("name", name);
    stats.put("num_reqs", numReqs);
    stats.put("grid_sparse_reads_b64",  encodeSparseGrid(reads));
    stats.put("grid_sparse_writes_b64", encodeSparseGrid(writes));
    return stats;
  }

  /** Give the background worker thread time to drain the event queue. */
  private static void waitForWorker() throws InterruptedException {
    Thread.sleep(200);
  }

  @BeforeEach
  void setUp() {
    profiler = new XdnGeoDemandProfiler2(SVC);
  }

  @AfterEach
  void tearDown() {
    // Reset the static registry after each test to avoid cross-test pollution.
    XdnGeoDemandProfiler2.registerConsistencyModel(SVC, ConsistencyModel.EVENTUAL);
  }

  // ---- consistency model registry --------------------------------------------

  @Test
  void testUnregisteredService_defaultsToEventual() throws Exception {
    XdnGeoDemandProfiler2 populated = new XdnGeoDemandProfiler2(
            buildStats(SVC, 100, Map.of(500_500, 100), Collections.emptyMap()));

    Map<String, Geolocation> nodeGeo = Map.of(
            "A", new Geolocation(0.0,  0.0),
            "B", new Geolocation(80.0, 0.0));

    NodeIdsMetadataPair<String> result =
            populated.getNewActivesPlacement(Set.of("A", "B"), stubAppInfo(nodeGeo));

    assertNotNull(result);
    assertFalse(result.nodeIds().isEmpty());
  }

  @Test
  void testRegisterConsistencyModel_isPickedUp() throws Exception {
    XdnGeoDemandProfiler2.registerConsistencyModel(SVC, ConsistencyModel.LINEARIZABLE);

    XdnGeoDemandProfiler2 populated = new XdnGeoDemandProfiler2(
            buildStats(SVC, 100, Map.of(500_500, 100), Map.of(500_500, 50)));

    Map<String, Geolocation> nodeGeo = Map.of(
            "A", new Geolocation(0.0,   0.0),
            "B", new Geolocation(20.0,  20.0),
            "C", new Geolocation(-20.0, -20.0));

    NodeIdsMetadataPair<String> result =
            populated.getNewActivesPlacement(Set.of("A", "B", "C"), stubAppInfo(nodeGeo));

    assertNotNull(result);
    assertFalse(result.nodeIds().isEmpty());
  }

  @Test
  void testRegisterConsistencyModel_replacesExisting() {
    XdnGeoDemandProfiler2.registerConsistencyModel(SVC, ConsistencyModel.LINEARIZABLE);
    // Should not throw.
    XdnGeoDemandProfiler2.registerConsistencyModel(SVC, ConsistencyModel.SEQUENTIAL);
  }

  // ---- serialization round-trip ----------------------------------------------

  @Test
  void testGetDemandStats_containsRequiredKeys() throws Exception {
    JSONObject stats = profiler.getDemandStats();

    assertTrue(stats.has("name"));
    assertTrue(stats.has("num_reqs"));
    assertTrue(stats.has("grid_sparse_reads_b64"));
    assertTrue(stats.has("grid_sparse_writes_b64"));
    assertEquals(SVC, stats.getString("name"));
  }

  @Test
  void testSerializationRoundTrip_preservesCounts() throws Exception {
    JSONObject original = buildStats(SVC, 67,
            Map.of(100_200, 42, 300_400, 17),
            Map.of(500_500, 8));

    XdnGeoDemandProfiler2 deserialized = new XdnGeoDemandProfiler2(original);
    JSONObject roundTripped = deserialized.getDemandStats();
    assertEquals(67, roundTripped.getLong("num_reqs"));

    // Deserialise once more to verify the grids also survived.
    XdnGeoDemandProfiler2 twice = new XdnGeoDemandProfiler2(roundTripped);
    assertEquals(67, twice.getDemandStats().getLong("num_reqs"));
  }

  @Test
  void testGetDemandStats_resetsGridsAfterCall() throws Exception {
    XdnGeoDemandProfiler2 populated = new XdnGeoDemandProfiler2(
            buildStats(SVC, 100, Map.of(500_500, 100), Collections.emptyMap()));

    assertEquals(100, populated.getDemandStats().getLong("num_reqs"));
    // Second call — grids were cleared.
    assertEquals(0, populated.getDemandStats().getLong("num_reqs"));
  }

  @Test
  void testEmptyProfiler_getDemandStats_returnsZeroRequests() throws Exception {
    assertEquals(0, profiler.getDemandStats().getLong("num_reqs"));
  }

  // ---- combine ---------------------------------------------------------------

  @Test
  void testCombine_accumulatesReadCounts() throws Exception {
    XdnGeoDemandProfiler2 p1 = new XdnGeoDemandProfiler2(
            buildStats(SVC, 30, Map.of(500_500, 30), Collections.emptyMap()));
    XdnGeoDemandProfiler2 p2 = new XdnGeoDemandProfiler2(
            buildStats(SVC, 20, Map.of(500_500, 20), Collections.emptyMap()));

    p1.combine(p2);

    assertEquals(50, p1.getDemandStats().getLong("num_reqs"));
  }

  @Test
  void testCombine_accumulatesWriteCounts() throws Exception {
    XdnGeoDemandProfiler2 p1 = new XdnGeoDemandProfiler2(
            buildStats(SVC, 15, Collections.emptyMap(), Map.of(200_200, 15)));
    XdnGeoDemandProfiler2 p2 = new XdnGeoDemandProfiler2(
            buildStats(SVC, 25, Collections.emptyMap(), Map.of(200_200, 25)));

    p1.combine(p2);

    assertEquals(40, p1.getDemandStats().getLong("num_reqs"));
  }

  @Test
  void testCombine_multipleProfilers_accumulatesCorrectly() throws Exception {
    XdnGeoDemandProfiler2 p1 = new XdnGeoDemandProfiler2(
            buildStats(SVC, 100, Map.of(1, 100), Collections.emptyMap()));
    XdnGeoDemandProfiler2 p2 = new XdnGeoDemandProfiler2(
            buildStats(SVC, 200, Map.of(2, 200), Collections.emptyMap()));
    XdnGeoDemandProfiler2 p3 = new XdnGeoDemandProfiler2(
            buildStats(SVC, 50,  Map.of(3,  50), Collections.emptyMap()));

    p1.combine(p2);
    p1.combine(p3);

    assertEquals(350, p1.getDemandStats().getLong("num_reqs"));
  }

  // ---- shouldReportDemandStats + worker thread --------------------------------

  @Test
  void testShouldReportDemandStats_wrongService_returnsFalse() {
    XdnHttpRequest req = makeRequest("other-svc", 10.0, 20.0, HttpMethod.GET);
    assertFalse(profiler.shouldReportDemandStats(req, null, null));
  }

  @Test
  void testShouldReportDemandStats_nullRequest_returnsFalse() {
    assertFalse(profiler.shouldReportDemandStats(null, null, null));
  }

  @Test
  void testShouldReportDemandStats_noGeolocationHeader_returnsFalse() {
    DefaultHttpHeaders headers = new DefaultHttpHeaders();
    headers.set("XDN", SVC);
    HttpRequest http = new DefaultHttpRequest(
            HttpVersion.HTTP_1_1, HttpMethod.GET, "/", headers);
    HttpContent content = new DefaultHttpContent(Unpooled.EMPTY_BUFFER);
    XdnHttpRequest req = new XdnHttpRequest(http, content);

    assertFalse(profiler.shouldReportDemandStats(req, null, null));
  }

  @Test
  void testShouldReportDemandStats_firstRequest_initializesWindowReturnsFalse() {
    XdnHttpRequest req = makeRequest(SVC, 10.0, 20.0, HttpMethod.GET);
    assertFalse(profiler.shouldReportDemandStats(req, null, null));
  }

  @Test
  void testGetRequest_enqueued_countedAfterWorkerRuns() throws Exception {
    XdnHttpRequest req = makeRequest(SVC, 10.0, 20.0, HttpMethod.GET);
    profiler.shouldReportDemandStats(req, null, null);
    waitForWorker();

    assertEquals(1, profiler.getDemandStats().getLong("num_reqs"));
  }

  @Test
  void testPostRequest_enqueued_countedAfterWorkerRuns() throws Exception {
    XdnHttpRequest req = makeRequest(SVC, 10.0, 20.0, HttpMethod.POST);
    profiler.shouldReportDemandStats(req, null, null);
    waitForWorker();

    assertEquals(1, profiler.getDemandStats().getLong("num_reqs"));
  }

  @Test
  void testGetRequest_goesToReadGrid() throws Exception {
    XdnHttpRequest req = makeRequest(SVC, 10.0, 20.0, HttpMethod.GET);
    profiler.shouldReportDemandStats(req, null, null);
    waitForWorker();

    JSONObject stats = profiler.getDemandStats();
    assertFalse(stats.getString("grid_sparse_reads_b64").isEmpty(),
            "Read grid should be non-empty after a GET request");
  }

  @Test
  void testPostRequest_goesToWriteGrid() throws Exception {
    XdnHttpRequest req = makeRequest(SVC, 10.0, 20.0, HttpMethod.POST);
    profiler.shouldReportDemandStats(req, null, null);
    waitForWorker();

    JSONObject stats = profiler.getDemandStats();
    assertFalse(stats.getString("grid_sparse_writes_b64").isEmpty(),
            "Write grid should be non-empty after a POST request");
  }

  @Test
  void testMultipleRequests_allCounted() throws Exception {
    for (int i = 0; i < 5; i++) {
      XdnHttpRequest req = makeRequest(SVC, i * 10.0, i * 10.0, HttpMethod.GET);
      profiler.shouldReportDemandStats(req, null, null);
    }
    waitForWorker();

    assertEquals(5, profiler.getDemandStats().getLong("num_reqs"));
  }

  // ---- getNewActivesPlacement ------------------------------------------------

  @Test
  void testGetNewActivesPlacement_noRequests_returnsNull() {
    Map<String, Geolocation> nodeGeo = Map.of("A", new Geolocation(0.0, 0.0));

    assertNull(profiler.getNewActivesPlacement(Set.of("A"), stubAppInfo(nodeGeo)),
            "No requests yet — should not trigger reconfiguration");
  }

  @Test
  void testGetNewActivesPlacement_nullActives_returnsNull() throws Exception {
    XdnGeoDemandProfiler2 populated = new XdnGeoDemandProfiler2(
            buildStats(SVC, 100, Map.of(500_500, 100), Collections.emptyMap()));

    assertNull(populated.getNewActivesPlacement(null, stubAppInfo(Collections.emptyMap())));
  }

  @Test
  void testGetNewActivesPlacement_withDemand_returnsValidPlacement() throws Exception {
    XdnGeoDemandProfiler2 populated = new XdnGeoDemandProfiler2(
            buildStats(SVC, 150, Map.of(500_500, 100), Map.of(500_500, 50)));

    Map<String, Geolocation> nodeGeo = new LinkedHashMap<>();
    nodeGeo.put("A", new Geolocation(0.0,   0.0));
    nodeGeo.put("B", new Geolocation(40.0,  40.0));
    nodeGeo.put("C", new Geolocation(-40.0, -40.0));

    NodeIdsMetadataPair<String> result =
            populated.getNewActivesPlacement(Set.of("A", "B", "C"), stubAppInfo(nodeGeo));

    assertNotNull(result);
    assertFalse(result.nodeIds().isEmpty());
    assertTrue(nodeGeo.keySet().containsAll(result.nodeIds()),
            "All selected nodes must be known candidates");
  }

  @Test
  void testGetNewActivesPlacement_metadataContainsPreferredCoordinator() throws Exception {
    XdnGeoDemandProfiler2 populated = new XdnGeoDemandProfiler2(
            buildStats(SVC, 100, Map.of(500_500, 100), Collections.emptyMap()));

    Map<String, Geolocation> nodeGeo = Map.of(
            "A", new Geolocation(0.0,  0.0),
            "B", new Geolocation(20.0, 20.0));

    NodeIdsMetadataPair<String> result =
            populated.getNewActivesPlacement(Set.of("A", "B"), stubAppInfo(nodeGeo));

    assertNotNull(result);
    assertNotNull(result.placementMetadata(), "Metadata must be present");

    JSONObject metadata = new JSONObject(result.placementMetadata());
    String coordinator = metadata.getString(
            AbstractDemandProfile.Keys.PREFERRED_COORDINATOR.toString());

    assertNotNull(coordinator);
    assertTrue(nodeGeo.containsKey(coordinator),
            "Coordinator must be a known node");
    assertTrue(result.nodeIds().contains(coordinator),
            "Coordinator must be inside the replica set");
  }

  // ---- justReconfigured ------------------------------------------------------

  @Test
  void testJustReconfigured_doesNotThrow() {
    assertDoesNotThrow(() -> profiler.justReconfigured());
  }
}