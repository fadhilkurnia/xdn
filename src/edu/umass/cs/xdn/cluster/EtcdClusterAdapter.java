package edu.umass.cs.xdn.cluster;

import edu.umass.cs.xdn.utils.Shell;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link ClusterAdapter} for the {@code xdn-etcd-cluster} reference image.
 *
 * <p>etcd's static-bootstrap initial-cluster format doesn't accept runtime member additions on its
 * own — a surviving member must run {@code etcdctl member add <name> --peer-urls=...} before the
 * new container starts. This adapter does exactly that via {@code docker exec} against a survivor's
 * local container, which is enough for both single-host swarm tests and multi-host deployments (the
 * surviving lowest-ordinal node runs it).
 */
public class EtcdClusterAdapter implements ClusterAdapter {

  private static final Logger logger = Logger.getLogger(EtcdClusterAdapter.class.getSimpleName());

  @Override
  public String name() {
    return "etcd";
  }

  @Override
  public boolean preJoin(AdapterContext ctx, String newReplicaName) {
    if (ctx.surviorContainerName() == null) {
      logger.log(
          Level.WARNING,
          "EtcdClusterAdapter.preJoin called without a survivor container on this node;"
              + " membership add must be driven from a different AR");
      return true;
    }
    String peerUrl = "http://" + newReplicaName + ":" + ctx.peerPort();
    String cmd =
        String.format(
            "docker exec %s etcdctl member add %s --peer-urls=%s",
            ctx.surviorContainerName(), newReplicaName, peerUrl);
    int code = Shell.runCommand(cmd, false);
    if (code != 0) {
      logger.log(
          Level.SEVERE,
          "etcdctl member add failed for {0} (exit code {1})",
          new Object[] {newReplicaName, code});
      return false;
    }
    return true;
  }

  @Override
  public boolean preRemove(AdapterContext ctx, String departingReplicaName) {
    if (ctx.surviorContainerName() == null) {
      return true;
    }
    // `etcdctl member remove <id>` takes a hex member ID, not a name. The simplest reliable
    // path is to look up the ID via `member list` and pipe it through. For v1 we shell out
    // through `sh -c` to keep the chain inline.
    String cmd =
        String.format(
            "sh -c \"docker exec %s etcdctl member list --write-out=simple | "
                + "awk -F',' '/, %s, /{print $1}' | "
                + "xargs -r docker exec %s etcdctl member remove\"",
            ctx.surviorContainerName(), departingReplicaName, ctx.surviorContainerName());
    int code = Shell.runCommand(cmd, false);
    if (code != 0) {
      logger.log(
          Level.WARNING,
          "etcdctl member remove returned non-zero for {0} (exit code {1}) — continuing",
          new Object[] {departingReplicaName, code});
    }
    return true;
  }
}
