package edu.umass.cs.xdn.service;

/**
 * How a service is deployed and replicated.
 *
 * <ul>
 *   <li>{@link #REPLICATED} (default) — XDN performs the replication and coordination; the
 *       containerized service is a blackbox that is unaware it is replicated.
 *   <li>{@link #CLUSTER} — the containerized service handles its own coordination/replication (e.g.
 *       etcd, CockroachDB, a Postgres replication cluster). XDN only provides placement, a stable
 *       per-replica identity, an overlay network for peer discovery, and request routing.
 * </ul>
 */
public enum DeploymentMode {
  REPLICATED,
  CLUSTER;

  public static DeploymentMode fromString(String raw) {
    if (raw == null || raw.isEmpty()) {
      return REPLICATED;
    }
    switch (raw.trim().toLowerCase()) {
      case "replicated":
      case "service":
        return REPLICATED;
      case "cluster":
      case "clustered":
        return CLUSTER;
      default:
        throw new IllegalStateException(
            "invalid deployment mode '" + raw + "', valid values are: replicated, cluster");
    }
  }
}
