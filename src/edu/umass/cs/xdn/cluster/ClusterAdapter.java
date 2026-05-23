package edu.umass.cs.xdn.cluster;

import java.util.List;

/**
 * Image-specific lifecycle hooks invoked by XDN during a cluster's reconfiguration.
 *
 * <p>For a self-clustering service like etcd, adding a new replica is a two-step dance: (a) an
 * existing cluster member must <em>register</em> the new peer (e.g. {@code etcdctl member add}),
 * and only then can (b) the new container start with {@code ETCD_INITIAL_CLUSTER_STATE=existing}.
 * Symmetrically, removing a replica usually needs a {@code member remove} call against a surviving
 * member before its container is stopped.
 *
 * <p>{@code ClusterAdapter} is the interface a cluster image authors implement to bridge the
 * generic {@code XDN_CLUSTER_*} contract to their image's native admin protocol. The adapter is
 * invoked on the AR node hosting the lowest-ordinal surviving replica — that node knows it owns a
 * member that can run the admin call, and the lowest-ordinal rule avoids races without needing any
 * extra coordination.
 *
 * <p>Adapters are looked up by name (see {@link ClusterAdapterRegistry}); a service selects one via
 * the {@code "adapter"} field in its launch JSON (e.g. {@code "adapter":"etcd"}).
 */
public interface ClusterAdapter {

  /** Returns the adapter's name, matching the {@code adapter} field of a cluster spec. */
  String name();

  /**
   * Registers a new peer with an already-running cluster, before that peer's container starts.
   * Implementations typically run an image-specific admin command via {@code docker exec} against a
   * surviving member's local container.
   *
   * @param ctx execution context: existing-member container name on this host, peer port, etc.
   * @param newReplicaName the stable hostname (e.g. {@code replica-3}) of the joiner.
   * @return true on success.
   */
  boolean preJoin(AdapterContext ctx, String newReplicaName);

  /**
   * Drops a departing peer from the cluster, before its container is stopped.
   *
   * @param ctx execution context.
   * @param departingReplicaName stable hostname of the replica being removed.
   * @return true on success.
   */
  boolean preRemove(AdapterContext ctx, String departingReplicaName);

  /**
   * Context passed to adapter hooks. Carries just enough state for an adapter to run the right
   * admin command on the right local container without coupling it to XdnGigapaxosApp internals.
   *
   * @param surviorContainerName container name on this host that the adapter should {@code docker
   *     exec} into to talk to the cluster.
   * @param peerPort the cluster's internal peer port.
   * @param survivingPeerNames stable hostnames of replicas the cluster already knows about (the set
   *     carried into this epoch from the previous one).
   */
  record AdapterContext(
      String surviorContainerName, int peerPort, List<String> survivingPeerNames) {}
}
