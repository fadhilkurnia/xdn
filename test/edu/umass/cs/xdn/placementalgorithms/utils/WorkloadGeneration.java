package edu.umass.cs.xdn.placementalgorithms.utils;

import java.util.*;

/**
 * Deterministic workload generator — port of workload_generation.py.
 *
 * <p>NOTE ON RNG FIDELITY Python uses numpy's PCG64 generator; Java uses java.util.Random (LCG).
 * For the same seed the numerical values will differ, but the statistical properties (distribution
 * shape, candidate spread) are equivalent. Intra-Java reproducibility is guaranteed: same seed →
 * same workload.
 */
public final class WorkloadGeneration {

  private WorkloadGeneration() {}

  // =========================================================================
  // Public API
  // =========================================================================

  /**
   * Generate a deterministic request grid and candidate node set.
   *
   * @param gridRows grid height
   * @param gridCols grid width
   * @param numCandidateNodes number of candidate replica locations
   * @param totalRequests total request volume (reads + writes)
   * @param readRatio fraction of reads (must sum to 1 with writeRatio)
   * @param writeRatio fraction of writes
   * @param distribution "uniform", "hotspot", or "clustered"
   * @param hotspotCount blob centres for hotspot distribution
   * @param hotspotConcentration fraction of traffic in hotspot blobs
   * @param hotspotRadius Gaussian σ for hotspot blobs (grid units)
   * @param clusterCount blob centres for clustered distribution
   * @param clusterRadius Gaussian σ for clustered blobs (grid units)
   * @param clusterConcentration fraction of traffic in clustered blobs
   * @param seed RNG seed for reproducibility
   */
  public static WorkloadResult generateWorkload(
      int gridRows,
      int gridCols,
      int numCandidateNodes,
      int totalRequests,
      double readRatio,
      double writeRatio,
      String distribution,
      int hotspotCount,
      double hotspotConcentration,
      double hotspotRadius,
      int clusterCount,
      double clusterRadius,
      double clusterConcentration,
      long seed) {

    if (Math.abs(readRatio + writeRatio - 1.0) > 1e-6)
      throw new IllegalArgumentException(
          "read/write ratios must sum to 1.0; got " + (readRatio + writeRatio));

    Random rng = new Random(seed);

    int totalReads = (int) (totalRequests * readRatio);
    int totalWrites = (int) (totalRequests * writeRatio);

    double[][] readGrid =
        distribute(
            gridRows,
            gridCols,
            totalReads,
            distribution,
            hotspotCount,
            hotspotConcentration,
            hotspotRadius,
            clusterCount,
            clusterRadius,
            clusterConcentration,
            rng);
    double[][] writeGrid =
        distribute(
            gridRows,
            gridCols,
            totalWrites,
            distribution,
            hotspotCount,
            hotspotConcentration,
            hotspotRadius,
            clusterCount,
            clusterRadius,
            clusterConcentration,
            rng);

    double[][][] requestGrid = new double[gridRows][gridCols][2];
    for (int r = 0; r < gridRows; r++)
      for (int c = 0; c < gridCols; c++) {
        requestGrid[r][c][0] = readGrid[r][c];
        requestGrid[r][c][1] = writeGrid[r][c];
      }

    int[][] candidateNodes = generateCandidateNodes(gridRows, gridCols, numCandidateNodes, rng);

    return new WorkloadResult(requestGrid, candidateNodes);
  }

  // =========================================================================
  // Distribution dispatch
  // =========================================================================

  private static double[][] distribute(
      int gridRows,
      int gridCols,
      int total,
      String distribution,
      int hotspotCount,
      double hotspotConcentration,
      double hotspotRadius,
      int clusterCount,
      double clusterRadius,
      double clusterConcentration,
      Random rng) {
    switch (distribution) {
      case "uniform":
        return distributeUniform(gridRows, gridCols, total, rng);
      case "hotspot":
        return distributeHotspot(
            gridRows, gridCols, total, hotspotCount, hotspotConcentration, hotspotRadius, rng);
      case "clustered":
        return distributeClustered(
            gridRows, gridCols, total, clusterCount, clusterRadius, clusterConcentration, rng);
      default:
        throw new IllegalArgumentException(
            "distribution must be 'uniform', 'hotspot', or 'clustered'; got: " + distribution);
    }
  }

  // =========================================================================
  // Uniform distribution
  // =========================================================================

  /**
   * Spread total requests uniformly with small random variation ~U(0.5,1.5)×base. Rescaled to match
   * total exactly, then rounded.
   */
  private static double[][] distributeUniform(int gridRows, int gridCols, int total, Random rng) {

    if (total <= 0) return new double[gridRows][gridCols];

    double base = (double) total / (gridRows * gridCols);
    double[][] grid = new double[gridRows][gridCols];
    double sum = 0.0;

    for (int r = 0; r < gridRows; r++)
      for (int c = 0; c < gridCols; c++) {
        // U[base*0.5, base*1.5]
        grid[r][c] = base * 0.5 + rng.nextDouble() * base;
        sum += grid[r][c];
      }

    if (sum > 0.0) {
      double scale = (double) total / sum;
      for (int r = 0; r < gridRows; r++)
        for (int c = 0; c < gridCols; c++) grid[r][c] = Math.round(grid[r][c] * scale);
    }
    return grid;
  }

  // =========================================================================
  // Hotspot distribution (Zipf-weighted Gaussian blobs)
  // =========================================================================

  /**
   * hotspotCount blob centres chosen at random. Each blob uses Gaussian falloff of width
   * hotspotRadius (σ). Blob weights follow Zipf: blob k gets weight 1/(k+1), normalised.
   * concentration fraction of total traffic goes into blobs; remainder is spread with uniform
   * noise.
   */
  private static double[][] distributeHotspot(
      int gridRows,
      int gridCols,
      int total,
      int hotspotCount,
      double concentration,
      double hotspotRadius,
      Random rng) {

    if (total <= 0) return new double[gridRows][gridCols];

    int nCells = gridRows * gridCols;
    if (hotspotCount > nCells) hotspotCount = nCells;

    // Shuffle cell indices to pick hotspot centres
    int[] indices = new int[nCells];
    for (int i = 0; i < nCells; i++) indices[i] = i;
    fisherYatesShuffle(indices, rng);

    // Blob centres (row, col)
    int[] crArr = new int[hotspotCount];
    int[] ccArr = new int[hotspotCount];
    for (int k = 0; k < hotspotCount; k++) {
      crArr[k] = indices[k] / gridCols;
      ccArr[k] = indices[k] % gridCols;
    }

    // Zipf-like weights: w_k = 1/(k+1), normalised
    double[] blobWeights = new double[hotspotCount];
    double wSum = 0.0;
    for (int k = 0; k < hotspotCount; k++) {
      blobWeights[k] = 1.0 / (k + 1);
      wSum += blobWeights[k];
    }
    for (int k = 0; k < hotspotCount; k++) blobWeights[k] /= wSum;

    double hotspotVolume = total * concentration;
    double twoSigmaSq = 2.0 * hotspotRadius * hotspotRadius;

    double[][] grid = new double[gridRows][gridCols];

    for (int k = 0; k < hotspotCount; k++) {
      int cr = crArr[k], cc = ccArr[k];

      // Compute unnormalised Gaussian blob over entire grid
      double blobSum = 0.0;
      double[][] blob = new double[gridRows][gridCols];
      for (int r = 0; r < gridRows; r++)
        for (int c = 0; c < gridCols; c++) {
          double dSq = (double) (r - cr) * (r - cr) + (double) (c - cc) * (c - cc);
          blob[r][c] = Math.exp(-dSq / twoSigmaSq);
          blobSum += blob[r][c];
        }

      if (blobSum > 0.0) {
        double scale = hotspotVolume * blobWeights[k] / blobSum;
        for (int r = 0; r < gridRows; r++)
          for (int c = 0; c < gridCols; c++) grid[r][c] += scale * blob[r][c];
      }
    }

    // Add uniform noise for remaining traffic
    double remaining = total * (1.0 - concentration);
    if (remaining > 0.0) {
      double noiseSum = 0.0;
      double[] noise = new double[nCells];
      for (int i = 0; i < nCells; i++) {
        noise[i] = 0.5 + rng.nextDouble(); // U[0.5, 1.5]
        noiseSum += noise[i];
      }
      double noiseScale = remaining / noiseSum;
      for (int r = 0; r < gridRows; r++)
        for (int c = 0; c < gridCols; c++) grid[r][c] += noiseScale * noise[r * gridCols + c];
    }

    // Rescale to total, then round
    rescaleAndRound(grid, gridRows, gridCols, total);
    return grid;
  }

  // =========================================================================
  // Clustered distribution (geographic Gaussian blobs, equal weights)
  // =========================================================================

  /**
   * clusterCount blobs, each with Gaussian falloff of width clusterRadius. Unlike hotspot, blobs
   * are NOT individually normalised — raw Gaussian values are accumulated and then collectively
   * rescaled.
   */
  private static double[][] distributeClustered(
      int gridRows,
      int gridCols,
      int total,
      int clusterCount,
      double clusterRadius,
      double concentration,
      Random rng) {

    if (total <= 0) return new double[gridRows][gridCols];

    double[][] grid = new double[gridRows][gridCols];
    double twoSigmaSq = 2.0 * clusterRadius * clusterRadius;

    // Random cluster centres
    for (int k = 0; k < clusterCount; k++) {
      int cr = rng.nextInt(gridRows);
      int cc = rng.nextInt(gridCols);
      for (int r = 0; r < gridRows; r++) {
        for (int c = 0; c < gridCols; c++) {
          double dist = Math.sqrt((double) (r - cr) * (r - cr) + (double) (c - cc) * (c - cc));
          grid[r][c] += Math.exp(-(dist * dist) / twoSigmaSq);
        }
      }
    }

    // Scale cluster part to concentration fraction of total
    double clusterVolume = total * concentration;
    double gridSum = arraySum(grid, gridRows, gridCols);
    if (gridSum > 0.0) {
      double scale = clusterVolume / gridSum;
      for (int r = 0; r < gridRows; r++) for (int c = 0; c < gridCols; c++) grid[r][c] *= scale;
    }

    // Add uniform noise for remaining traffic
    // Python: base * noise / noise.sum() * nCells  →  noise normalised to sum=nCells
    double remaining = total * (1.0 - concentration);
    if (remaining > 0.0) {
      int nCells = gridRows * gridCols;
      double base = remaining / nCells;
      double noiseSum = 0.0;
      double[] noise = new double[nCells];
      for (int i = 0; i < nCells; i++) {
        noise[i] = 0.5 + rng.nextDouble(); // U[0.5, 1.5]
        noiseSum += noise[i];
      }
      // noise normalised to sum = nCells, then multiplied by base
      double normFactor = (double) nCells / noiseSum;
      for (int r = 0; r < gridRows; r++)
        for (int c = 0; c < gridCols; c++)
          grid[r][c] += base * noise[r * gridCols + c] * normFactor;
    }

    rescaleAndRound(grid, gridRows, gridCols, total);
    return grid;
  }

  // =========================================================================
  // Candidate node generation  (jittered uniform spread)
  // =========================================================================

  /**
   * Divide the grid into numNodes roughly-equal rectangular regions; pick one random cell from each
   * region. Mirrors Python's _generate_candidate_nodes.
   */
  private static int[][] generateCandidateNodes(
      int gridRows, int gridCols, int numNodes, Random rng) {

    if (numNodes > gridRows * gridCols)
      throw new IllegalArgumentException(
          "numCandidateNodes=" + numNodes + " exceeds grid size " + (gridRows * gridCols));

    int colsRegions =
        Math.max(1, (int) Math.round(Math.sqrt((double) numNodes * gridCols / gridRows)));
    int rowsRegions = Math.max(1, (int) Math.ceil((double) numNodes / colsRegions));

    // Build list-of-lists of cell coordinates per region
    List<List<int[]>> regionCells = new ArrayList<>();
    for (int ri = 0; ri < rowsRegions; ri++) {
      int rStart = ri * gridRows / rowsRegions;
      int rEnd = (ri + 1) * gridRows / rowsRegions;
      for (int ci = 0; ci < colsRegions; ci++) {
        int cStart = ci * gridCols / colsRegions;
        int cEnd = (ci + 1) * gridCols / colsRegions;
        List<int[]> cells = new ArrayList<>();
        for (int r = rStart; r < rEnd; r++)
          for (int c = cStart; c < cEnd; c++) cells.add(new int[] {r, c});
        if (!cells.isEmpty()) regionCells.add(cells);
      }
    }

    // Shuffle the region list (Fisher-Yates on the list)
    for (int i = regionCells.size() - 1; i > 0; i--) {
      int j = rng.nextInt(i + 1);
      List<int[]> tmp = regionCells.get(i);
      regionCells.set(i, regionCells.get(j));
      regionCells.set(j, tmp);
    }

    List<int[]> candidates = new ArrayList<>(numNodes);
    Set<Long> seen = new HashSet<>(); // packed row*10007+col key
    int limit = Math.min(numNodes, regionCells.size());
    for (int k = 0; k < limit; k++) {
      List<int[]> cells = regionCells.get(k);
      int[] cell = cells.get(rng.nextInt(cells.size()));
      candidates.add(cell);
      seen.add(packCell(cell[0], cell[1]));
    }

    // Fill remainder with random unique cells (should rarely trigger)
    while (candidates.size() < numNodes) {
      int r = rng.nextInt(gridRows);
      int c = rng.nextInt(gridCols);
      long key = packCell(r, c);
      if (!seen.contains(key)) {
        candidates.add(new int[] {r, c});
        seen.add(key);
      }
    }

    // Sort by (row, col) for deterministic ordering
    candidates.sort(
        (a, b) -> a[0] != b[0] ? Integer.compare(a[0], b[0]) : Integer.compare(a[1], b[1]));
    return candidates.toArray(new int[0][]);
  }

  // =========================================================================
  // Utilities
  // =========================================================================

  private static void fisherYatesShuffle(int[] arr, Random rng) {
    for (int i = arr.length - 1; i > 0; i--) {
      int j = rng.nextInt(i + 1);
      int t = arr[i];
      arr[i] = arr[j];
      arr[j] = t;
    }
  }

  private static double arraySum(double[][] g, int rows, int cols) {
    double s = 0.0;
    for (int r = 0; r < rows; r++) for (int c = 0; c < cols; c++) s += g[r][c];
    return s;
  }

  /** Rescale grid so it sums to total, then apply Math.round() per cell. */
  private static void rescaleAndRound(double[][] grid, int gridRows, int gridCols, int total) {
    double sum = arraySum(grid, gridRows, gridCols);
    if (sum > 0.0) {
      double scale = (double) total / sum;
      for (int r = 0; r < gridRows; r++)
        for (int c = 0; c < gridCols; c++) grid[r][c] = Math.round(grid[r][c] * scale);
    }
  }

  /** Pack (row, col) into a single long for O(1) duplicate detection. */
  private static long packCell(int row, int col) {
    return ((long) row << 20) | col;
  }
}
