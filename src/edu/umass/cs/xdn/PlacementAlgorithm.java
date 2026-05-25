package edu.umass.cs.xdn;

import edu.umass.cs.nio.interfaces.Geolocation;
import java.util.Map;

/**
 * Strategy interface for geo-distributed replica placement.
 *
 * <p>Implementations receive the sparse read/write demand grids (cell index → request count), the
 * available node geolocations, and the consistency model's read/write access types, and return a
 * {@link PlacementResult} identifying which nodes to use and which should be the preferred Paxos
 * coordinator.
 *
 * <p>Static grid-math helpers are provided here so all implementations share a single source of
 * truth for the lat/lon ↔ row/col conversion formulas. Pass {@code numRows} and {@code numCols}
 * from the profiler constants rather than hard-coding them inside each algorithm.
 */
public interface PlacementAlgorithm {

  /**
   * Select a replica placement given the current demand snapshot.
   *
   * @param sparseReadGrid cell index → read request count since last report
   * @param sparseWriteGrid cell index → write request count since last report
   * @param nodeGeo node ID → geolocation for all known active replicas
   * @param targetReplicas number of replicas to select (= current group size)
   * @param readType "Closest", "Source", or "Majority"
   * @param writeType "Closest", "Source", or "Majority"
   * @param Q quorum size used in cost calculations
   * @return placement result, or {@code null} if no change is warranted
   */
  PlacementResult selectReplicas(
      Map<Integer, Integer> sparseReadGrid,
      Map<Integer, Integer> sparseWriteGrid,
      Map<String, Geolocation> nodeGeo,
      int targetReplicas,
      String readType,
      String writeType,
      int Q);

  // ---------------------------------------------------------------------------
  // Shared grid-math utilities
  // Cell layout:
  //   latitude  ∈ [-90,  90] → row ∈ [0, numRows),   north → row 0
  //   longitude ∈ [-180, 180] → col ∈ [0, numCols),  west  → col 0
  //   flat index = row * numCols + col
  // ---------------------------------------------------------------------------

  static int latToRow(double latitude, int numRows) {
    int row = (int) Math.floor((90.0 - latitude) * numRows / 180.0);
    return Math.max(0, Math.min(numRows - 1, row));
  }

  static int lonToCol(double longitude, int numCols) {
    int col = (int) Math.floor((longitude + 180.0) * numCols / 360.0);
    return Math.max(0, Math.min(numCols - 1, col));
  }

  /** Latitude of the centre of a row cell. */
  static double rowCenterToLat(int row, int numRows) {
    return 90.0 - (row + 0.5) * 180.0 / numRows;
  }

  /** Longitude of the centre of a column cell. */
  static double colCenterToLon(int col, int numCols) {
    return -180.0 + (col + 0.5) * 360.0 / numCols;
  }
}
