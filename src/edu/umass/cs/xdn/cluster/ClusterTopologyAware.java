package edu.umass.cs.xdn.cluster;

/**
 * Side-channel by which {@link StatefulClusterReplicaCoordinator} delivers the per-replica {@link
 * ClusterTopology} to the application layer ({@code XdnGigapaxosApp}) before {@code restore()} is
 * called.
 *
 * <p>Defined here rather than as a method on {@code Replicable} so the {@code
 * edu.umass.cs.xdn.cluster} package can be imported by the coordinator without pulling in the full
 * {@code XdnGigapaxosApp} class.
 */
public interface ClusterTopologyAware {

  /** Stores the topology for {@code serviceName}; must be called before {@code restore()}. */
  void setClusterTopology(String serviceName, ClusterTopology topology);

  /** Drops any stored topology for {@code serviceName}; called on replica-group deletion. */
  void clearClusterTopology(String serviceName);
}
