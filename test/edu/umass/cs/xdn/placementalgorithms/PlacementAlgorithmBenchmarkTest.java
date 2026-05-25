package edu.umass.cs.xdn.placementalgorithms;

import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.xdn.PlacementAlgorithm;
import edu.umass.cs.xdn.PlacementResult;
import edu.umass.cs.xdn.placementalgorithms.utils.WorkloadGeneration;
import edu.umass.cs.xdn.placementalgorithms.utils.WorkloadResult;
import java.io.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.Test;

/**
 * Benchmark test for {@link PlacementAlgorithm} implementations.
 *
 * <p>Pipeline per run: 1. WorkloadGeneration produces a dense request grid and candidate node
 * positions. 2. The dense grid is converted to the sparse format our algorithms expect. 3. Each
 * enabled algorithm selects replica nodes via {@code selectReplicas()}. 4. The benchmark evaluates
 * the cost of that placement using the standard latency cost model (Closest / Source / Majority)
 * and writes a CSV row.
 *
 * <p>Two things to edit before running: {@link #SCALE} — controls grid sizes and seed count. {@link
 * #ALGORITHMS} — comment/uncomment entries to enable or disable algorithms.
 *
 * <p>CSV results: {@code out/placementbenchmark/<algorithm>/<model>.csv}
 */
public class PlacementAlgorithmBenchmarkTest {

  // ============================================================================
  // !! EDIT THESE TWO TO CONTROL THE BENCHMARK !!
  // ============================================================================

  /** Controls grid sizes and seed count. QUICK ≈ seconds | MEDIUM ≈ 1–2 min | FULL ≈ 5 min */
  private static final BenchmarkScale SCALE = BenchmarkScale.QUICK;

  private static final double MS_PER_UNIT = 10.0;

  /**
   * Algorithms to benchmark. Comment out an entry to skip it. The factory receives the grid
   * dimensions so each algorithm instance is sized correctly for the current sweep step.
   */
  private static final List<AlgorithmEntry> ALGORITHMS =
      Arrays.asList(
          new AlgorithmEntry(
              "greedy_heuristic", (rows, cols) -> new Greedy(rows, cols, MS_PER_UNIT, false)),
          new AlgorithmEntry("centroid", (rows, cols) -> new Centroid(rows, cols)));

  // ============================================================================
  // Scale presets
  // ============================================================================

  enum BenchmarkScale {
    /** One small grid, 2 seeds. Fast sanity check. */
    QUICK(new int[][] {{80, 40}}, buildRange(0, 2)),
    /** Two grid sizes, 5 seeds. */
    MEDIUM(new int[][] {{80, 40}, {400, 200}}, buildRange(0, 5)),
    /** All grid sizes, 20 seeds. Identical to Benchmark.java. */
    FULL(new int[][] {{80, 40}, {400, 200}, {800, 400}}, buildRange(0, 20));

    final int[][] grids;
    final int[] seeds;

    BenchmarkScale(int[][] grids, int[] seeds) {
      this.grids = grids;
      this.seeds = seeds;
    }
  }

  // ============================================================================
  // Fixed configuration (mirrors Benchmark.java)
  // ============================================================================

  private static final int CANDIDATE_COUNT = 10;
  private static final int REQUESTS_PER_CELL = 10;
  private static final int CLUSTER_SIZE = 3; // K
  private static final int QUORUM_SIZE = 2; // Q
  private static final int TIMEOUT_SECONDS = 20;
  private static final String OUTPUT_DIR = "out/placementbenchmark";

  private static final String[] DISTRIBUTIONS = {"uniform", "hotspot", "clustered"};

  private static final ModelConfig[] MODELS = {
    new ModelConfig("linearizability", "Majority", "Majority", new double[][] {{0.8, 0.2}}),
    new ModelConfig("sequential", "Closest", "Majority", new double[][] {{0.8, 0.2}, {0.5, 0.5}}),
    new ModelConfig("eventual", "Closest", "Closest", new double[][] {{0.8, 0.2}}),
    new ModelConfig("primary-backup", "Source", "Source", new double[][] {{0.8, 0.2}}),
  };

  // ============================================================================
  // Test entry point
  // ============================================================================

  @Test
  void runPlacementBenchmark() throws Exception {
    System.out.printf("Placement benchmark — scale=%s  algorithms=%s%n%n", SCALE, algorithmNames());

    for (ModelConfig model : MODELS) {
      runModel(model);
    }

    System.out.printf("%nAll results written to: %s%n", OUTPUT_DIR);
  }

  // ============================================================================
  // Per-model sweep
  // ============================================================================

  private void runModel(ModelConfig model) throws Exception {
    String bar = "=".repeat(60);
    System.out.printf(
        "%n%s%nModel: %-20s  read=%-8s  write=%s%n%s%n",
        bar, model.name, model.readType, model.writeType, bar);

    // Algorithms disabled after a timeout stay disabled for this model only.
    Set<String> disabled = new HashSet<>();

    // Open one CSV per algorithm, kept open for the whole model sweep.
    Map<String, PrintWriter> writers = new LinkedHashMap<>();
    try {
      for (AlgorithmEntry entry : ALGORITHMS) {
        String path = csvPath(entry.name, model.name);
        new File(path).getParentFile().mkdirs();
        PrintWriter pw = new PrintWriter(new BufferedWriter(new FileWriter(path)));
        pw.println(
            "seed,grid_rows,grid_cols,distribution,read_ratio,write_ratio,"
                + "consistency_model,algorithm,cost,time_ms,nodes,coordinator,status");
        writers.put(entry.name, pw);
      }

      for (int[] dim : SCALE.grids) {
        int gridRows = dim[0], gridCols = dim[1];
        int minDim = Math.min(gridRows, gridCols);

        for (String dist : DISTRIBUTIONS) {
          for (double[] ratio : model.ratios) {
            double readR = ratio[0], writeR = ratio[1];

            for (int seed : SCALE.seeds) {

              // 1. Generate workload
              WorkloadResult wl;
              try {
                int totalReqs = gridRows * gridCols * REQUESTS_PER_CELL;
                wl =
                    WorkloadGeneration.generateWorkload(
                        gridRows,
                        gridCols,
                        CANDIDATE_COUNT,
                        totalReqs,
                        readR,
                        writeR,
                        dist,
                        3,
                        0.95,
                        0.04 * minDim,
                        2,
                        0.15 * minDim,
                        0.7,
                        seed);
              } catch (Exception e) {
                System.err.printf(
                    "  Workload gen failed %dx%d dist=%s seed=%d: %s%n",
                    gridRows, gridCols, dist, seed, e.getMessage());
                continue;
              }

              // 2. Convert dense workload to sparse format
              final Map<Integer, Integer> sparseReads =
                  toSparseMap(wl.requestGrid, gridRows, gridCols, 0);
              final Map<Integer, Integer> sparseWrites =
                  toSparseMap(wl.requestGrid, gridRows, gridCols, 1);
              final Map<String, Geolocation> nodeGeo =
                  toNodeGeo(wl.candidateNodes, gridRows, gridCols);

              System.out.printf(
                  "%n  %dx%d  dist=%-9s  ratio=%.2f/%.2f  seed=%d%n",
                  gridRows, gridCols, dist, readR, writeR, seed);

              for (AlgorithmEntry entry : ALGORITHMS) {
                PrintWriter pw = writers.get(entry.name);

                if (disabled.contains(entry.name)) {
                  System.out.printf("    %-20s SKIPPED (timed out earlier)%n", entry.name);
                  pw.printf(
                      "%d,%d,%d,%s,%.2f,%.2f,%s,%s,,,,,skipped%n",
                      seed, gridRows, gridCols, dist, readR, writeR, model.name, entry.name);
                  pw.flush();
                  continue;
                }

                // Create a fresh algorithm instance sized for this grid.
                final PlacementAlgorithm algo = entry.factory.create(gridRows, gridCols);
                final int fRows = gridRows, fCols = gridCols;
                final ModelConfig fModel = model;

                ExecutorService ex =
                    Executors.newSingleThreadExecutor(
                        r -> {
                          Thread t = new Thread(r, "bench-" + entry.name);
                          t.setDaemon(true);
                          return t;
                        });

                PlacementResult result = null;
                String errMsg = null;
                boolean timedOut = false;
                long startNs = System.nanoTime();

                try {
                  // 3. Run placement algorithm
                  Future<PlacementResult> future =
                      ex.submit(
                          () ->
                              algo.selectReplicas(
                                  sparseReads,
                                  sparseWrites,
                                  nodeGeo,
                                  CLUSTER_SIZE,
                                  fModel.readType,
                                  fModel.writeType,
                                  QUORUM_SIZE));
                  result = future.get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (TimeoutException e) {
                  timedOut = true;
                  disabled.add(entry.name);
                } catch (ExecutionException e) {
                  errMsg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
                } catch (InterruptedException e) {
                  Thread.currentThread().interrupt();
                  errMsg = "interrupted";
                } finally {
                  ex.shutdownNow();
                }

                double elapsedMs = (System.nanoTime() - startNs) / 1e6;

                if (timedOut) {
                  System.out.printf(
                      "    %-20s TIMEOUT (%.0f ms) — disabling for %s%n",
                      entry.name, elapsedMs, model.name);
                  pw.printf(
                      "%d,%d,%d,%s,%.2f,%.2f,%s,%s,,%.2f,,,timeout%n",
                      seed,
                      gridRows,
                      gridCols,
                      dist,
                      readR,
                      writeR,
                      model.name,
                      entry.name,
                      elapsedMs);

                } else if (errMsg != null) {
                  System.err.printf("    %-20s ERROR: %s%n", entry.name, errMsg);
                  pw.printf(
                      "%d,%d,%d,%s,%.2f,%.2f,%s,%s,,,,,%s%n",
                      seed,
                      gridRows,
                      gridCols,
                      dist,
                      readR,
                      writeR,
                      model.name,
                      entry.name,
                      "error:" + errMsg.replace(',', ';'));

                } else {
                  // 4. Evaluate cost of the returned placement
                  double cost =
                      evaluateCost(
                          sparseReads,
                          sparseWrites,
                          nodeGeo,
                          result.nodeIds(),
                          result.preferredCoordinator(),
                          model.readType,
                          model.writeType,
                          QUORUM_SIZE,
                          gridRows,
                          gridCols);

                  System.out.printf(
                      "    %-20s cost=%14.2f  time=%.1f ms%n", entry.name, cost, elapsedMs);
                  pw.printf(
                      "%d,%d,%d,%s,%.2f,%.2f,%s,%s,%.4f,%.2f,\"%s\",%s,ok%n",
                      seed,
                      gridRows,
                      gridCols,
                      dist,
                      readR,
                      writeR,
                      model.name,
                      entry.name,
                      cost,
                      elapsedMs,
                      String.join("|", result.nodeIds()),
                      result.preferredCoordinator());
                }
                pw.flush();
              }
            } // seeds
          } // ratios
        } // distributions
      } // grid dimensions

    } finally {
      writers
          .values()
          .forEach(
              pw -> {
                if (pw != null) pw.close();
              });
    }
  }

  // ============================================================================
  // Cost evaluator
  //
  // Applies the same cost model as GreedyHeuristic to any placement returned by
  // any algorithm, making results comparable across algorithms.
  //
  // Cost model:
  //   Closest  : 2 · d(cell, entry)
  //   Source   : 2 · d(cell, entry) + 2 · (d(entry, leader) + Δ)
  //                  Δ = max distance from leader to any replica
  //   Majority : 2 · d(cell, entry) + 2 · (d(entry, leader) + W)
  //                  W = distance from leader to Q-th nearest replica
  // ============================================================================

  private static double evaluateCost(
      Map<Integer, Integer> sparseReads,
      Map<Integer, Integer> sparseWrites,
      Map<String, Geolocation> nodeGeo,
      Set<String> selectedNodes,
      String preferredCoordinator,
      String readType,
      String writeType,
      int Q,
      int gridRows,
      int gridCols) {

    if (selectedNodes == null || selectedNodes.isEmpty()) return Double.NaN;

    // Convert selected node geolocations to grid coordinates.
    List<String> nodeList = new ArrayList<>(selectedNodes);
    List<int[]> nodePos = new ArrayList<>(nodeList.size());
    for (String id : nodeList) {
      Geolocation g = nodeGeo.get(id);
      nodePos.add(
          new int[] {
            PlacementAlgorithm.latToRow(g.latitude(), gridRows),
            PlacementAlgorithm.lonToCol(g.longitude(), gridCols)
          });
    }

    // Find leader index within the selected set.
    int leaderIdx = 0; // default to first node if coordinator not found
    for (int i = 0; i < nodeList.size(); i++) {
      if (nodeList.get(i).equals(preferredCoordinator)) {
        leaderIdx = i;
        break;
      }
    }

    // Leader-side invariants (only needed for Source / Majority).
    boolean usesLeader = !readType.equals("Closest") || !writeType.equals("Closest");
    double deltaSrc = 0.0, W = 0.0;
    if (usesLeader) {
      double[] leaderDists = new double[nodePos.size()];
      for (int i = 0; i < nodePos.size(); i++) {
        leaderDists[i] = gridDist(nodePos.get(leaderIdx), nodePos.get(i)) * MS_PER_UNIT;
        deltaSrc = Math.max(deltaSrc, leaderDists[i]);
      }
      int effectiveQ = Math.min(Q, nodePos.size());
      Arrays.sort(leaderDists);
      W = leaderDists[effectiveQ - 1];
    }

    // Accumulate cost over all active cells.
    Set<Integer> allCells = new HashSet<>(sparseReads.keySet());
    allCells.addAll(sparseWrites.keySet());

    double total = 0.0;
    for (int cellIdx : allCells) {
      int row = cellIdx / gridCols;
      int col = cellIdx % gridCols;

      double fRead = sparseReads.getOrDefault(cellIdx, 0);
      double fWrite = sparseWrites.getOrDefault(cellIdx, 0);

      // Find nearest selected node and distance in ms.
      double minDist = Double.MAX_VALUE;
      int nearestIdx = 0;
      for (int j = 0; j < nodePos.size(); j++) {
        double d = gridDist(new int[] {row, col}, nodePos.get(j)) * MS_PER_UNIT;
        if (d < minDist) {
          minDist = d;
          nearestIdx = j;
        }
      }

      double dEntryLeader =
          usesLeader
              ? gridDist(nodePos.get(nearestIdx), nodePos.get(leaderIdx)) * MS_PER_UNIT
              : 0.0;

      total +=
          fRead * cellCost(readType, minDist, dEntryLeader, deltaSrc, W)
              + fWrite * cellCost(writeType, minDist, dEntryLeader, deltaSrc, W);
    }
    return total;
  }

  private static double gridDist(int[] a, int[] b) {
    double dr = a[0] - b[0], dc = a[1] - b[1];
    return Math.sqrt(dr * dr + dc * dc);
  }

  private static double cellCost(
      String type, double dEntry, double dEntryLeader, double deltaSrc, double W) {
    return switch (type) {
      case "Closest" -> 2.0 * dEntry;
      case "Source" -> 2.0 * dEntry + 2.0 * (dEntryLeader + deltaSrc);
      case "Majority" -> 2.0 * dEntry + 2.0 * (dEntryLeader + W);
      default -> throw new IllegalArgumentException("Unknown access type: " + type);
    };
  }

  // ============================================================================
  // Conversion helpers
  // ============================================================================

  /**
   * Converts a channel of the dense requestGrid into a sparse map. channel=0 → reads, channel=1 →
   * writes.
   */
  private static Map<Integer, Integer> toSparseMap(
      double[][][] requestGrid, int gridRows, int gridCols, int channel) {
    Map<Integer, Integer> map = new HashMap<>();
    for (int r = 0; r < gridRows; r++) {
      for (int c = 0; c < gridCols; c++) {
        int count = (int) requestGrid[r][c][channel];
        if (count > 0) map.put(r * gridCols + c, count);
      }
    }
    return map;
  }

  /**
   * Converts int[][] candidateNodes (grid row/col coordinates) to Map&lt;String, Geolocation&gt;
   * using the inverse of latToRow/lonToCol.
   */
  private static Map<String, Geolocation> toNodeGeo(
      int[][] candidateNodes, int gridRows, int gridCols) {
    Map<String, Geolocation> map = new LinkedHashMap<>();
    for (int j = 0; j < candidateNodes.length; j++) {
      double lat = PlacementAlgorithm.rowCenterToLat(candidateNodes[j][0], gridRows);
      double lon = PlacementAlgorithm.colCenterToLon(candidateNodes[j][1], gridCols);
      map.put("node-" + j, new Geolocation(lat, lon));
    }
    return map;
  }

  // ============================================================================
  // Data classes
  // ============================================================================

  /** Creates a PlacementAlgorithm sized for a specific grid. */
  @FunctionalInterface
  interface AlgorithmFactory {
    PlacementAlgorithm create(int gridRows, int gridCols);
  }

  static final class AlgorithmEntry {
    final String name;
    final AlgorithmFactory factory;

    AlgorithmEntry(String name, AlgorithmFactory factory) {
      this.name = name;
      this.factory = factory;
    }
  }

  static final class ModelConfig {
    final String name, readType, writeType;
    final double[][] ratios;

    ModelConfig(String name, String readType, String writeType, double[][] ratios) {
      this.name = name;
      this.readType = readType;
      this.writeType = writeType;
      this.ratios = ratios;
    }
  }

  // ============================================================================
  // Helpers
  // ============================================================================

  private static String csvPath(String algo, String model) {
    return OUTPUT_DIR + File.separator + algo + File.separator + model + ".csv";
  }

  private static String algorithmNames() {
    List<String> names = new ArrayList<>();
    for (AlgorithmEntry a : ALGORITHMS) names.add(a.name);
    return names.toString();
  }

  private static int[] buildRange(int start, int end) {
    int[] arr = new int[end - start];
    for (int i = 0; i < arr.length; i++) arr[i] = start + i;
    return arr;
  }
}
