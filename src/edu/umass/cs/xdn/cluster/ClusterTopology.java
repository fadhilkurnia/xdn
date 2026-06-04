package edu.umass.cs.xdn.cluster;

/**
 * Per-replica identity that {@link StatefulClusterReplicaCoordinator} pushes to {@code
 * XdnGigapaxosApp} before starting the cluster's container.
 *
 * <p>Carries only what is derived from the replica set itself — ordinal, total size, and the launch
 * phase. Spec-level fields (peer port, cluster adapter) stay in {@code ServiceProperty}, which the
 * app already has at container-start time.
 *
 * @param myOrdinal this replica's ordinal in {@code 0 .. clusterSize - 1}; stable across
 *     reconfiguration so {@code replica-0} stays {@code replica-0}.
 * @param clusterSize total number of replicas in the cluster.
 * @param phase {@link Phase#BOOTSTRAP} on epoch-0 cluster formation, {@link Phase#JOIN} when a
 *     replica is added to an existing cluster.
 */
public record ClusterTopology(int myOrdinal, int clusterSize, Phase phase) {

  public enum Phase {
    /** First-time cluster formation — all replicas come up together. */
    BOOTSTRAP,
    /** This replica is being added to an already-running cluster. */
    JOIN
  }
}
