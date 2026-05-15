package edu.umass.cs.xdn.placementalgorithms;
 
import static org.junit.jupiter.api.Assertions.*;
 
import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.xdn.PlacementResult;
import java.util.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
 
public class GreedyHeuristicTest {
 
  private static final int    ROWS        = 1000;
  private static final int    COLS        = 1000;
  private static final double MS_PER_UNIT = 0.3;
 
  private Greedy greedy;
 
  // ---- helpers ---------------------------------------------------------------
 
  private static Geolocation geo(double lat, double lon) {
    return new Geolocation(lat, lon);
  }
 
  private static int idx(int row, int col) {
    return row * COLS + col;
  }
 
  @BeforeEach
  void setUp() {
    greedy = new Greedy(ROWS, COLS, MS_PER_UNIT, false);
  }
 
  // ---- basic placement -------------------------------------------------------
 
  @Test
  void testSingleCellSingleNode_thatNodeSelected() {
    Map<Integer, Integer> reads  = Map.of(idx(500, 500), 100);
    Map<Integer, Integer> writes = Map.of(idx(500, 500), 50);
    Map<String, Geolocation> nodes = Map.of("A", geo(0.0, 0.0));
 
    PlacementResult result = greedy.selectReplicas(
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
 
    PlacementResult result = greedy.selectReplicas(
        reads, Collections.emptyMap(), nodes, 1, "Closest", "Closest", 1);
 
    assertNotNull(result);
    assertEquals(1, result.nodeIds().size());
    assertTrue(result.nodeIds().contains("near"));
  }
 
  @Test
  void testEmptyDemand_returnsNull() {
    PlacementResult result = greedy.selectReplicas(
        Collections.emptyMap(), Collections.emptyMap(),
        Map.of("A", geo(0.0, 0.0)), 1, "Closest", "Closest", 1);
 
    assertNull(result);
  }
 
  // ---- multi-replica placement -----------------------------------------------
 
  @Test
  void testTwoHotspots_K2_oneReplicaNearEach() {
    Map<Integer, Integer> reads = Map.of(
        idx(50,  50),  500,
        idx(950, 950), 500);
    Map<Integer, Integer> writes = Map.of(
        idx(50,  50),  250,
        idx(950, 950), 250);
 
    Map<String, Geolocation> nodes = new LinkedHashMap<>();
    nodes.put("NW", geo( 80.0, -170.0));
    nodes.put("SE", geo(-80.0,  170.0));
    nodes.put("C",  geo(  0.0,    0.0));
 
    PlacementResult result = greedy.selectReplicas(
        reads, writes, nodes, 2, "Closest", "Closest", 1);

    assertNotNull(result);
    assertEquals(2, result.nodeIds().size());
    assertEquals(Set.of("C", "SE"), result.nodeIds());
  }
 
  @Test
  void testKEqualsNodeCount_allNodesSelected() {
    Map<Integer, Integer> reads = Map.of(idx(500, 500), 100);
 
    Map<String, Geolocation> nodes = new LinkedHashMap<>();
    nodes.put("A", geo( 40.0, -74.0));
    nodes.put("B", geo( 51.0,   0.0));
    nodes.put("C", geo(-33.0, 151.0));
 
    PlacementResult result = greedy.selectReplicas(
        reads, Collections.emptyMap(), nodes, 3, "Closest", "Closest", 2);
 
    assertNotNull(result);
    assertEquals(3, result.nodeIds().size());
    assertTrue(result.nodeIds().containsAll(nodes.keySet()));
  }
 
  // ---- leader / coordinator --------------------------------------------------
 
  @Test
  void testCoordinatorAlwaysInReplicaSet_allAccessTypes() {
    Map<Integer, Integer> reads  = Map.of(idx(300, 300), 100);
    Map<Integer, Integer> writes = Map.of(idx(300, 300), 100);
 
    Map<String, Geolocation> nodes = new LinkedHashMap<>();
    nodes.put("A", geo( 54.0, -54.0));
    nodes.put("B", geo(  0.0,   0.0));
    nodes.put("C", geo(-54.0,  54.0));
 
    for (String readType  : List.of("Closest", "Source", "Majority")) {
      for (String writeType : List.of("Closest", "Source", "Majority")) {
        PlacementResult result = greedy.selectReplicas(
            reads, writes, nodes, 2, readType, writeType, 2);
        assertNotNull(result, "Result was null for " + readType + "/" + writeType);
        assertTrue(
            result.nodeIds().contains(result.preferredCoordinator()),
            "Coordinator not in replica set for " + readType + "/" + writeType);
      }
    }
  }
 
  // ---- access type / cost model ----------------------------------------------
 
  @Test
  void testClosestAccessType_doesNotCrash() {
    Map<Integer, Integer> reads  = Map.of(idx(500, 500), 100);
    Map<Integer, Integer> writes = Map.of(idx(500, 500), 50);
    Map<String, Geolocation> nodes = Map.of(
        "A", geo(0.0,  0.0),
        "B", geo(20.0, 20.0));
 
    assertNotNull(greedy.selectReplicas(
        reads, writes, nodes, 1, "Closest", "Closest", 1));
  }
 
  @Test
  void testSourceAccessType_doesNotCrash() {
    Map<Integer, Integer> reads  = Map.of(idx(500, 500), 100);
    Map<Integer, Integer> writes = Map.of(idx(500, 500), 50);
    Map<String, Geolocation> nodes = Map.of(
        "A", geo(0.0,  0.0),
        "B", geo(20.0, 20.0));
 
    assertNotNull(greedy.selectReplicas(
        reads, writes, nodes, 2, "Source", "Source", 2));
  }
 
  @Test
  void testMajorityAccessType_doesNotCrash() {
    Map<Integer, Integer> reads  = Map.of(idx(500, 500), 100);
    Map<Integer, Integer> writes = Map.of(idx(500, 500), 50);
    Map<String, Geolocation> nodes = Map.of(
        "A", geo(0.0,   0.0),
        "B", geo(20.0,  20.0),
        "C", geo(-20.0, -20.0));
 
    assertNotNull(greedy.selectReplicas(
        reads, writes, nodes, 3, "Majority", "Majority", 2));
  }
 
  @Test
  void testInvalidAccessType_throwsIllegalArgumentException() {
    Map<Integer, Integer> reads = Map.of(idx(500, 500), 100);
    Map<String, Geolocation> nodes = Map.of("A", geo(0.0, 0.0));
 
    assertThrows(IllegalArgumentException.class, () ->
        greedy.selectReplicas(reads, Collections.emptyMap(), nodes, 1,
            "BadType", "Closest", 1));
  }
 
  // ---- quorum edge cases -----------------------------------------------------
 
  @Test
  void testQuorumLargerThanK_clampedSafely() {
    Map<Integer, Integer> reads = Map.of(idx(500, 500), 100);
    Map<String, Geolocation> nodes = Map.of(
        "A", geo(0.0,  0.0),
        "B", geo(10.0, 10.0));
 
    PlacementResult result = greedy.selectReplicas(
        reads, Collections.emptyMap(), nodes, 2, "Majority", "Majority", 10);
 
    assertNotNull(result);
    assertEquals(2, result.nodeIds().size());
  }
 
  @Test
  void testQ1_alwaysValidQuorum() {
    Map<Integer, Integer> reads  = Map.of(idx(200, 200), 100);
    Map<Integer, Integer> writes = Map.of(idx(800, 800), 100);
    Map<String, Geolocation> nodes = Map.of(
        "A", geo(54.0,  -36.0),
        "B", geo(-54.0,  36.0));
 
    PlacementResult result = greedy.selectReplicas(
        reads, writes, nodes, 2, "Majority", "Majority", 1);
 
    assertNotNull(result);
    assertEquals(2, result.nodeIds().size());
  }
 
  // ---- read vs write demand --------------------------------------------------
 
  @Test
  void testReadOnlyDemand_doesNotCrash() {
    Map<Integer, Integer> reads = Map.of(idx(300, 300), 200);
    Map<String, Geolocation> nodes = Map.of(
        "A", geo(54.0,  -54.0),
        "B", geo(-54.0,  54.0));
 
    assertNotNull(greedy.selectReplicas(
        reads, Collections.emptyMap(), nodes, 1, "Closest", "Closest", 1));
  }
 
  @Test
  void testWriteOnlyDemand_doesNotCrash() {
    Map<Integer, Integer> writes = Map.of(idx(700, 700), 200);
    Map<String, Geolocation> nodes = Map.of(
        "A", geo(54.0,  -54.0),
        "B", geo(-54.0,  54.0));
 
    assertNotNull(greedy.selectReplicas(
        Collections.emptyMap(), writes, nodes, 1, "Source", "Source", 1));
  }
 
  @Test
  void testSeparateReadWriteHotspots_bothContributeToPlacement() {
    // Read demand NW, write demand SE — with K=2 both regions should be covered.
    Map<Integer, Integer> reads  = Map.of(idx(100, 100), 300);
    Map<Integer, Integer> writes = Map.of(idx(900, 900), 300);
 
    Map<String, Geolocation> nodes = new LinkedHashMap<>();
    nodes.put("NW", geo( 72.0, -144.0));
    nodes.put("SE", geo(-72.0,  144.0));
    nodes.put("C",  geo(  0.0,    0.0));
 
    PlacementResult result = greedy.selectReplicas(
        reads, writes, nodes, 2, "Closest", "Closest", 1);
 
    assertNotNull(result);
    assertEquals(2, result.nodeIds().size());
    assertTrue(result.nodeIds().contains("NW"));
    assertTrue(result.nodeIds().contains("SE"));
  }
}
