package edu.umass.cs.xdn.placementalgorithms.utils;

/**
 * Output of WorkloadGeneration.generateWorkload().
 *
 * <p>requestGrid : [gridRows][gridCols][2] [r][c][0] = read frequency at cell (r,c) [r][c][1] =
 * write frequency at cell (r,c) candidateNodes : [numCandidates][2], each entry is {row, col}.
 */
public final class WorkloadResult {

  public final double[][][] requestGrid;
  public final int[][] candidateNodes;

  public WorkloadResult(double[][][] requestGrid, int[][] candidateNodes) {
    this.requestGrid = requestGrid;
    this.candidateNodes = candidateNodes;
  }
}
