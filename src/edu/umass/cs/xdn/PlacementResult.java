package edu.umass.cs.xdn;

import java.util.Set;

/**
 * Result returned by a {@link PlacementAlgorithm}: the chosen replica node IDs and the preferred
 * Paxos coordinator within that set.
 */
public record PlacementResult(Set<String> nodeIds, String preferredCoordinator) {}
