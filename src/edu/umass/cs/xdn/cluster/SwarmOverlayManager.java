package edu.umass.cs.xdn.cluster;

import edu.umass.cs.xdn.utils.Shell;

/**
 * Idempotent helpers for the per-cluster Docker Swarm overlay network.
 *
 * <p>Each cluster service gets one attachable overlay network ({@code xdn-cluster-<svc>}) that
 * spans every node hosting a replica. Containers join it with {@code docker run --network
 * xdn-cluster-<svc> --network-alias replica-<ordinal>}; Docker's embedded DNS then resolves {@code
 * replica-0..replica-N} clusterwide, so peers talk to each other on the overlay without any host
 * port publishing.
 *
 * <p>Requires that {@code docker swarm init}/{@code docker swarm join} has already been run on
 * every XDN node — see {@code bin/xdnd dist-init}. Swarm is used <em>only</em> for the network; XDN
 * keeps doing its own placement with plain {@code docker run}.
 */
public final class SwarmOverlayManager {

  /** Prefix applied to {@code serviceName} to derive the overlay network name. */
  public static final String OVERLAY_NETWORK_PREFIX = "xdn-cluster-";

  private SwarmOverlayManager() {}

  /** Returns the overlay network name conventionally used for {@code serviceName}. */
  public static String networkName(String serviceName) {
    return OVERLAY_NETWORK_PREFIX + serviceName;
  }

  /**
   * Ensures an attachable overlay network {@code networkName} exists. No-op if already present.
   *
   * @return true on success (existed or was created), false on failure.
   */
  public static boolean ensureOverlay(String networkName) {
    // already exists?
    if (Shell.runCommand("docker network inspect " + networkName, true) == 0) {
      return true;
    }
    // create. We don't pass --subnet/--ip-range; Docker picks a non-conflicting one.
    if (Shell.runCommand("docker network create -d overlay --attachable " + networkName, true)
        == 0) {
      return true;
    }
    // When multiple ARs race to create the same overlay on the same swarm, all but one of
    // their `create` calls fail. Re-inspect: if a peer won the race, the network now exists
    // and we're done.
    return Shell.runCommand("docker network inspect " + networkName, true) == 0;
  }

  /** Removes the overlay network. Treats "already gone" as success. */
  public static boolean removeOverlay(String networkName) {
    int rm = Shell.runCommand("docker network rm " + networkName, true);
    return rm == 0 || Shell.runCommand("docker network inspect " + networkName, true) != 0;
  }
}
