package edu.umass.cs.xdn.placementalgorithms;

import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.xdn.PlacementAlgorithm;
import edu.umass.cs.xdn.PlacementResult;
import java.util.*;

/**
 * Centroid-based replica placement.
 *
 * <p>Algorithm:
 * <ol>
 *   <li>Merge the read and write demand grids into a single combined count per cell.
 *   <li>Compute the demand-weighted centroid in lat/lon space.
 *   <li>Rank all candidate nodes by squared lat/lon distance to the centroid.
 *   <li>Select the {@code targetReplicas} closest nodes; the closest becomes the
 *       preferred coordinator.
 * </ol>
 *
 * <p>The {@code readType}, {@code writeType}, and {@code Q} parameters are accepted
 * for interface compatibility but are not used — the centroid model does not
 * distinguish access types.
 */
public class Centroid implements PlacementAlgorithm {

    private final int numGridRows;
    private final int numGridCols;

    public Centroid(int numGridRows, int numGridCols) {
        this.numGridRows = numGridRows;
        this.numGridCols = numGridCols;
    }

    @Override
    public PlacementResult selectReplicas(
            Map<Integer, Integer> sparseReadGrid,
            Map<Integer, Integer> sparseWriteGrid,
            Map<String, Geolocation> nodeGeo,
            int targetReplicas,
            String readType,
            String writeType,
            int Q) {

        // Merge reads and writes into a single demand map.
        Map<Integer, Integer> combined = new HashMap<>(sparseReadGrid);
        sparseWriteGrid.forEach((k, v) -> combined.merge(k, v, Integer::sum));
        if (combined.isEmpty()) return null;

        // Compute demand-weighted centroid in lat/lon space.
        double totalWeight = 0.0, weightedLat = 0.0, weightedLon = 0.0;
        for (Map.Entry<Integer, Integer> e : combined.entrySet()) {
            int    idx   = e.getKey();
            int    count = e.getValue();
            int    row   = idx / numGridCols;
            int    col   = idx % numGridCols;
            double lat   = PlacementAlgorithm.rowCenterToLat(row, numGridRows);
            double lon   = PlacementAlgorithm.colCenterToLon(col, numGridCols);
            totalWeight  += count;
            weightedLat  += lat * count;
            weightedLon  += lon * count;
        }
        if (totalWeight == 0.0) return null;

        double centLat = weightedLat / totalWeight;
        double centLon = weightedLon / totalWeight;

        // Sort nodes by squared Euclidean distance in lat/lon space to the centroid.
        List<Map.Entry<String, Geolocation>> nodes = new ArrayList<>(nodeGeo.entrySet());
        nodes.sort(Comparator.comparingDouble(e -> {
            double dLat = e.getValue().latitude()  - centLat;
            double dLon = e.getValue().longitude() - centLon;
            return dLat * dLat + dLon * dLon;
        }));

        // Pick the top targetReplicas; the closest is the preferred coordinator.
        Set<String> result = new LinkedHashSet<>();
        for (int i = 0; i < targetReplicas && i < nodes.size(); i++) {
            result.add(nodes.get(i).getKey());
        }
        String coordinator = nodes.isEmpty() ? null : nodes.get(0).getKey();
        return new PlacementResult(result, coordinator);
    }
}
