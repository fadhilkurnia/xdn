package edu.umass.cs.xdn.placementalgorithms;
 
import static org.junit.jupiter.api.Assertions.*;
 
import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.xdn.PlacementResult;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
 
public class CentroidTest {
 
  private static final int ROWS = 1000;
  private static final int COLS = 1000;
 
  private Centroid centroid;
 
  // ---- helpers ---------------------------------------------------------------

  private static Geolocation geo(double lat, double lon) {
    return new Geolocation(lat, lon);
  }
 
  private static int idx(int row, int col) {
    return row * COLS + col;
  }
 
  @BeforeEach
  void setUp() {
    centroid = new Centroid(ROWS, COLS);
  }
 
  // ---- tests -----------------------------------------------------------------
 
  @Test
  void testSingleCellSingleNode_thatNodeSelected() {
    Map<Integer, Integer> reads  = Map.of(idx(500, 500), 100);
    Map<Integer, Integer> writes = Map.of(idx(500, 500), 50);
    Map<String, Geolocation> nodes = Map.of("A", geo(0.0, 0.0));
 
    PlacementResult result = centroid.selectReplicas(
        reads, writes, nodes, 1, "Closest", "Closest", 1);
 
    assertNotNull(result);
    assertEquals(Set.of("A"), result.nodeIds());
    assertEquals("A", result.preferredCoordinator());
  }
 
  @Test
  void testSingleCellMultipleNodes_closestNodeSelected() {
    Map<Integer, Integer> reads = Map.of(idx(500, 500), 100);
 
    Map<String, Geolocation> nodes = new LinkedHashMap<>();
    nodes.put("near",  geo(0.0,   0.0));
    nodes.put("far-N", geo(80.0,  0.0));
    nodes.put("far-S", geo(-80.0, 0.0));
 
    PlacementResult result = centroid.selectReplicas(
        reads, Collections.emptyMap(), nodes, 1, "Closest", "Closest", 1);
 
    assertNotNull(result);
    assertEquals(1, result.nodeIds().size());
    assertTrue(result.nodeIds().contains("near"));
    assertEquals("near", result.preferredCoordinator());
  }
 
  @Test
  void testEmptyDemand_returnsNull() {
    PlacementResult result = centroid.selectReplicas(
        Collections.emptyMap(), Collections.emptyMap(),
        Map.of("A", geo(0.0, 0.0)), 1, "Closest", "Closest", 1);
 
    assertNull(result);
  }
 
  @Test
  void testReadsAndWritesCombined_centroidInMiddle() {
    // Equal read demand north and write demand south → combined centroid at equator.
    Map<Integer, Integer> reads  = Map.of(idx(100, 500), 100);
    Map<Integer, Integer> writes = Map.of(idx(900, 500), 100);
 
    Map<String, Geolocation> nodes = new LinkedHashMap<>();
    nodes.put("north",  geo(72.0,  0.0));
    nodes.put("center", geo(0.0,   0.0));
    nodes.put("south",  geo(-72.0, 0.0));
 
    PlacementResult result = centroid.selectReplicas(
        reads, writes, nodes, 1, "Closest", "Closest", 1);
 
    assertNotNull(result);
    assertEquals("center", result.preferredCoordinator());
  }
 
  @Test
  void testHeavierWriteDemand_centroidPullsTowardWrites() {
    // Read demand north (weight 10), write demand south (weight 90) → south wins.
    Map<Integer, Integer> reads  = Map.of(idx(100, 500), 10);
    Map<Integer, Integer> writes = Map.of(idx(900, 500), 90);
 
    Map<String, Geolocation> nodes = new LinkedHashMap<>();
    nodes.put("north", geo(72.0,  0.0));
    nodes.put("south", geo(-72.0, 0.0));
 
    PlacementResult result = centroid.selectReplicas(
        reads, writes, nodes, 1, "Closest", "Closest", 1);
 
    assertNotNull(result);
    assertEquals("south", result.preferredCoordinator());
  }
 
  @Test
  void testTargetReplicasExceedsNodes_allNodesReturned() {
    Map<Integer, Integer> reads = Map.of(idx(500, 500), 100);
    Map<String, Geolocation> nodes = Map.of(
        "A", geo(0.0,   0.0),
        "B", geo(10.0, 10.0));
 
    PlacementResult result = centroid.selectReplicas(
        reads, Collections.emptyMap(), nodes, 5, "Closest", "Closest", 1);
 
    assertNotNull(result);
    assertEquals(2, result.nodeIds().size());
  }
 
  @Test
  void testCoordinatorAlwaysInReplicaSet() {
    Map<Integer, Integer> reads = Map.of(idx(200, 200), 100);
    Map<String, Geolocation> nodes = new LinkedHashMap<>();
    nodes.put("A", geo(54.0,  -54.0));
    nodes.put("B", geo(0.0,    0.0));
    nodes.put("C", geo(-54.0,  54.0));
 
    PlacementResult result = centroid.selectReplicas(
        reads, Collections.emptyMap(), nodes, 2, "Closest", "Closest", 1);
 
    assertNotNull(result);
    assertTrue(result.nodeIds().contains(result.preferredCoordinator()),
        "Coordinator must be inside the replica set");
  }
 
  @Test
  void testReadTypeAndWriteTypeIgnored_sameResultRegardless() {
    Map<Integer, Integer> reads = Map.of(idx(500, 500), 100);
    Map<String, Geolocation> nodes = Map.of("A", geo(0.0, 0.0));
 
    PlacementResult r1 = centroid.selectReplicas(
        reads, Collections.emptyMap(), nodes, 1, "Majority", "Source",  3);
    PlacementResult r2 = centroid.selectReplicas(
        reads, Collections.emptyMap(), nodes, 1, "Closest",  "Closest", 1);
 
    assertNotNull(r1);
    assertNotNull(r2);
    assertEquals(r1.nodeIds(), r2.nodeIds());
    assertEquals(r1.preferredCoordinator(), r2.preferredCoordinator());
  }
}
