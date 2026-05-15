package edu.umass.cs.xdn.placementalgorithms;

import edu.umass.cs.nio.interfaces.Geolocation;
import edu.umass.cs.xdn.PlacementAlgorithm;
import edu.umass.cs.xdn.PlacementResult;
import java.util.*;

/**
 * Greedy additive placement with a known leader — adapted from {@code greedy_known.py}.
 *
 * <p>At each of the K steps the algorithm picks the candidate node that minimises
 * the total weighted latency cost across all active demand cells, simultaneously
 * searching for the best leader within the tentative cluster.  The cost model for
 * each cell depends on the access type:
 *
 * <ul>
 *   <li><b>Closest</b>  — {@code 2 · d(cell, entry)}
 *   <li><b>Source</b>   — {@code 2 · d(cell, entry) + 2 · (d(entry, L) + Δ)}
 *                         where {@code Δ = max_k d(L, replica_k)}
 *   <li><b>Majority</b> — {@code 2 · d(cell, entry) + 2 · (d(entry, L) + W)}
 *                         where {@code W = d(L, Q-th nearest replica)}
 * </ul>
 *
 * <p>Node geolocations are converted to grid (row, col) coordinates before the
 * algorithm runs; distances are in grid-unit multiples of {@code msPerUnit}.
 */
public class Greedy implements PlacementAlgorithm {
    private final int     numGridRows;
    private final int     numGridCols;
    private final double  msPerUnit;
    private final boolean wrapAround;

    public Greedy(int numGridRows, int numGridCols, double msPerUnit, boolean wrapAround) {
        this.numGridRows = numGridRows;
        this.numGridCols = numGridCols;
        this.msPerUnit   = msPerUnit;
        this.wrapAround  = wrapAround;
    }

    // ---------------------------------------------------------------------------
    // PlacementAlgorithm
    // ---------------------------------------------------------------------------

    @Override
    public PlacementResult selectReplicas(
            Map<Integer, Integer> sparseReadGrid,
            Map<Integer, Integer> sparseWriteGrid,
            Map<String, Geolocation> nodeGeo,
            int targetReplicas,
            String readType,
            String writeType,
            int Q) {

        if (nodeGeo.isEmpty() || targetReplicas == 0) return null;

        // --- Build ordered node list for index ↔ ID mapping ---------------------
        List<String> nodeIds = new ArrayList<>(nodeGeo.keySet());
        int nJ = nodeIds.size();
        int[][] candidateNodes = new int[nJ][2];
        for (int j = 0; j < nJ; j++) {
            Geolocation g = nodeGeo.get(nodeIds.get(j));
            candidateNodes[j][0] = PlacementAlgorithm.latToRow(g.latitude(),  numGridRows);
            candidateNodes[j][1] = PlacementAlgorithm.lonToCol(g.longitude(), numGridCols);
        }

        // --- Build active-cell arrays from sparse demand grids ------------------
        Set<Integer> allCells = new HashSet<>(sparseReadGrid.keySet());
        allCells.addAll(sparseWriteGrid.keySet());
        if (allCells.isEmpty()) return null;

        int     nActive   = allCells.size();
        int[]   activeRows = new int[nActive];
        int[]   activeCols = new int[nActive];
        double[] fRead    = new double[nActive];
        double[] fWrite   = new double[nActive];
        int i = 0;
        for (int idx : allCells) {
            activeRows[i] = idx / numGridCols;
            activeCols[i] = idx % numGridCols;
            fRead[i]  = sparseReadGrid.getOrDefault(idx,  0);
            fWrite[i] = sparseWriteGrid.getOrDefault(idx, 0);
            i++;
        }

        int K          = Math.min(targetReplicas, nJ);
        int effectiveQ = Math.min(Q, K);

        // --- Pre-compute distance matrices --------------------------------------
        double[][] dIj = buildDij(activeRows, activeCols, candidateNodes);
        double[][] dJl = buildDjl(candidateNodes);

        boolean usesLeader = !readType.equals("Closest") || !writeType.equals("Closest");
        double  worstPenalty = worstCaseQuorumPenalty();

        // --- Greedy placement loop ----------------------------------------------
        int     clusterSize  = 0;
        int[]   clusterIdx   = new int[K];
        boolean[] available  = new boolean[nJ];
        Arrays.fill(available, true);

        int[]    routingCandIdx = new int[nActive];
        double[] dCellToEntry   = new double[nActive];
        Arrays.fill(dCellToEntry, Double.MAX_VALUE);

        for (int step = 0; step < K; step++) {
            int      bestCand    = -1;
            double   bestCost    = Double.MAX_VALUE;
            int      bestLeader  = -1;
            int[]    bestRouting = null;
            double[] bestDEntry  = null;

            for (int candIdx = 0; candIdx < nJ; candIdx++) {
                if (!available[candIdx]) continue;

                // Count cells that would improve by routing to this candidate.
                int nCloser = 0;
                for (int ci = 0; ci < nActive; ci++)
                    if (dIj[ci][candIdx] < dCellToEntry[ci]) nCloser++;

                int[]    trialRouting;
                double[] trialD;
                if (nCloser == 0) {
                    // No cell improves — reuse live arrays (no copy needed).
                    trialRouting = routingCandIdx;
                    trialD       = dCellToEntry;
                } else {
                    trialRouting = Arrays.copyOf(routingCandIdx, nActive);
                    trialD       = Arrays.copyOf(dCellToEntry,   nActive);
                    for (int ci = 0; ci < nActive; ci++) {
                        if (dIj[ci][candIdx] < dCellToEntry[ci]) {
                            trialRouting[ci] = candIdx;
                            trialD[ci]       = dIj[ci][candIdx];
                        }
                    }
                }

                int[] tentCluster = Arrays.copyOf(clusterIdx, clusterSize + 1);
                tentCluster[clusterSize] = candIdx;

                double[] lr = bestLeaderCost(
                        trialRouting, trialD, tentCluster, clusterSize + 1,
                        dJl, fRead, fWrite, effectiveQ, readType, writeType,
                        worstPenalty, usesLeader, nActive);

                if (lr[0] < bestCost) {
                    bestCost    = lr[0];
                    bestCand    = candIdx;
                    bestLeader  = (int) lr[1];
                    bestRouting = (nCloser == 0) ? trialRouting.clone() : trialRouting;
                    bestDEntry  = (nCloser == 0) ? trialD.clone()       : trialD;
                }
            }

            clusterIdx[clusterSize++] = bestCand;
            available[bestCand]       = false;
            routingCandIdx            = bestRouting;
            dCellToEntry              = bestDEntry;
        }

        // --- Final leader pass on committed cluster ----------------------------
        double[] fr = bestLeaderCost(
                routingCandIdx, dCellToEntry, clusterIdx, clusterSize,
                dJl, fRead, fWrite, effectiveQ, readType, writeType,
                worstPenalty, usesLeader, nActive);
        int finalLeaderIdx = (int) fr[1];

        // --- Map indices back to node IDs --------------------------------------
        Set<String> result = new LinkedHashSet<>();
        for (int k = 0; k < K; k++) result.add(nodeIds.get(clusterIdx[k]));
        String coordinator = (finalLeaderIdx >= 0)
                ? nodeIds.get(finalLeaderIdx)
                : nodeIds.get(clusterIdx[0]);

        return new PlacementResult(result, coordinator);
    }

    // ---------------------------------------------------------------------------
    // Leader search
    // ---------------------------------------------------------------------------

    /**
     * Try every placed replica as leader; return {@code [bestCost, bestLeaderCandIdx]}.
     * If {@code usesLeader} is false (all-Closest model), leaderIdx is -1.
     */
    private double[] bestLeaderCost(
            int[] routingCandIdx, double[] dCellToEntry,
            int[] clusterIdx, int clusterSize,
            double[][] dJl,
            double[] fRead, double[] fWrite,
            int Q, String readType, String writeType,
            double worstPenalty, boolean usesLeader, int nActive) {

        if (!usesLeader) {
            double cost = computeCost(routingCandIdx, dCellToEntry,
                    -1, clusterIdx, clusterSize, dJl,
                    fRead, fWrite, Q, readType, writeType, worstPenalty, nActive);
            return new double[]{cost, -1};
        }

        double bestCost   = Double.MAX_VALUE;
        int    bestLeader = clusterIdx[0];
        for (int s = 0; s < clusterSize; s++) {
            int leaderCand = clusterIdx[s];
            double cost = computeCost(routingCandIdx, dCellToEntry,
                    leaderCand, clusterIdx, clusterSize, dJl,
                    fRead, fWrite, Q, readType, writeType, worstPenalty, nActive);
            if (cost < bestCost) { bestCost = cost; bestLeader = leaderCand; }
        }
        return new double[]{bestCost, bestLeader};
    }

    // ---------------------------------------------------------------------------
    // Cost evaluation
    // ---------------------------------------------------------------------------

    private static double computeCost(
            int[] routingCandIdx, double[] dCellToEntry,
            int leaderIdx, int[] clusterIdx, int clusterSize,
            double[][] dJl,
            double[] fRead, double[] fWrite,
            int Q, String readType, String writeType,
            double worstPenalty, int nActive) {

        double deltaSrc = 0.0, W = 0.0;
        if (leaderIdx >= 0) {
            double[] leaderDists = new double[clusterSize];
            for (int s = 0; s < clusterSize; s++) {
                double d = dJl[leaderIdx][clusterIdx[s]];
                leaderDists[s] = d;
                if (d > deltaSrc) deltaSrc = d;
            }
            if (clusterSize < Q) {
                W = worstPenalty;
            } else {
                Arrays.sort(leaderDists, 0, clusterSize);
                W = leaderDists[Q - 1];
            }
        }

        double total = 0.0;
        for (int i = 0; i < nActive; i++) {
            double dEntry       = dCellToEntry[i];
            double dEntryLeader = (leaderIdx >= 0) ? dJl[routingCandIdx[i]][leaderIdx] : 0.0;
            total += fRead[i]  * cellCost(readType,  dEntry, dEntryLeader, deltaSrc, W)
                    + fWrite[i] * cellCost(writeType, dEntry, dEntryLeader, deltaSrc, W);
        }
        return total;
    }

    static double cellCost(String type, double dEntry, double dEntryLeader,
                           double deltaSrc, double W) {
        return switch (type) {
            case "Closest"  -> 2.0 * dEntry;
            case "Source"   -> 2.0 * dEntry + 2.0 * (dEntryLeader + deltaSrc);
            case "Majority" -> 2.0 * dEntry + 2.0 * (dEntryLeader + W);
            default -> throw new IllegalArgumentException("Unknown access type: " + type);
        };
    }

    // ---------------------------------------------------------------------------
    // Distance matrix builders
    // ---------------------------------------------------------------------------

    /** d_ij[i][j] = latency from active cell i to candidate node j (ms). */
    private double[][] buildDij(int[] activeRows, int[] activeCols, int[][] candidates) {
        int nActive = activeRows.length, nJ = candidates.length;
        double[][] d = new double[nActive][nJ];
        for (int i = 0; i < nActive; i++) {
            int ar = activeRows[i], ac = activeCols[i];
            for (int j = 0; j < nJ; j++) {
                int dr = Math.abs(ar - candidates[j][0]);
                int dc = Math.abs(ac - candidates[j][1]);
                if (wrapAround) {
                    if (dr > numGridRows - dr) dr = numGridRows - dr;
                    if (dc > numGridCols - dc) dc = numGridCols - dc;
                }
                d[i][j] = Math.sqrt((double) dr * dr + (double) dc * dc) * msPerUnit;
            }
        }
        return d;
    }

    /** d_jl[j][l] = latency between candidate j and candidate l (ms). */
    private double[][] buildDjl(int[][] candidates) {
        int nJ = candidates.length;
        double[][] d = new double[nJ][nJ];
        for (int j = 0; j < nJ; j++) {
            int jr = candidates[j][0], jc = candidates[j][1];
            for (int l = 0; l < nJ; l++) {
                int dr = Math.abs(jr - candidates[l][0]);
                int dc = Math.abs(jc - candidates[l][1]);
                if (wrapAround) {
                    if (dr > numGridRows - dr) dr = numGridRows - dr;
                    if (dc > numGridCols - dc) dc = numGridCols - dc;
                }
                d[j][l] = Math.sqrt((double) dr * dr + (double) dc * dc) * msPerUnit;
            }
        }
        return d;
    }

    private double worstCaseQuorumPenalty() {
        double hr = numGridRows / 2.0, hc = numGridCols / 2.0;
        return Math.sqrt(hr * hr + hc * hc) * msPerUnit;
    }
}