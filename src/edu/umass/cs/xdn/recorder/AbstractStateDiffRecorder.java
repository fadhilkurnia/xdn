package edu.umass.cs.xdn.recorder;

import edu.umass.cs.xdn.XdnConfig;
import edu.umass.cs.xdn.utils.Shell;
import java.net.InetAddress;
import java.util.Map;

public abstract class AbstractStateDiffRecorder {

  protected final String nodeID;
  protected final String baseDirectoryPath;

  /**
   * Creates StetDiffRecorder in a particular node and base path. Examples of the implemented
   * recorder include rsync, zip, and our custom filesystem (fuselog).
   *
   * @param nodeID ID of node where this recorder live, used for differentiator if all replicas live
   *     in the same machine. Example: "ar0".
   * @param basePath Base directory to store the safety critical state of the application (e.g.,
   *     "/tmp/xdn/").
   */
  protected AbstractStateDiffRecorder(String nodeID, String basePath) {
    assert nodeID != null && !nodeID.isEmpty() : "nodeID must be defined";
    assert basePath != null && basePath.endsWith("/") : "basePath must end with '/'";
    this.nodeID = nodeID;
    this.baseDirectoryPath = basePath;
  }

  /**
   * Enumerates the live directory types for a service replica. PRIMARY — the FUSE-mounted capture
   * directory for the primary container. BACKUP1/2 — the rsync-seeded directories for the two
   * blue/green backup containers.
   */
  public enum LiveDirType {
    PRIMARY,
    BACKUP1,
    BACKUP2
  }

  public static final String DIR_SNAPSHOT = "snp/";
  public static final String DIR_COMMITTED_STATEDIFF = "cmtDiff/";
  public static final String DIR_PROPOSED_STATEDIFF = "prpDiff/";
  public static final String DIR_PRIMARY = "primary/";
  public static final String DIR_BACKUP1 = "backup1/";
  public static final String DIR_BACKUP2 = "backup2/";

  /** Creates a recorder instance based on the configured recorder type. */
  public static AbstractStateDiffRecorder create(XdnConfig config, String nodeId) {
    return switch (config.getRecorderType()) {
      case RSYNC -> new RsyncStateDiffRecorder(nodeId);
        // NOTE: For FUSELOG, see XdnConfig.getFuselogBaseDir()'s javadoc: FUSELOG_BASE_DIR is not
        // wired to anything here.
      case FUSELOG -> new FuselogStateDiffRecorder(nodeId);
      case FUSENODE -> new FusenodeStateDiffRecorder(nodeId);
      case FUSERUST -> new FuseRustStateDiffRecorder(nodeId);
      case ZIP -> new ZipStateDiffRecorder(nodeId);
      default -> throw new RuntimeException("Unknown recorder type: " + config.getRecorderType());
    };
  }

  // -------------------------------------------------------------------------
  // Path getter helper functions
  // - default: /tmp/xdn/state/<recorder>/<node-id>/<service-name>/e<epoch>/<directory>/
  // -------------------------------------------------------------------------
  public String getServiceBaseDir(String serviceName, int epoch) {
    return String.format("%s%s/%s/e%d/", baseDirectoryPath, nodeID, serviceName, epoch);
  }

  public String getServiceBaseDir(String nodeID, String serviceName, int epoch) {
    return String.format("%s%s/%s/e%d/", baseDirectoryPath, nodeID, serviceName, epoch);
  }

  // DIR_SNAPSHOT: Directory where committed stateDiffs are applied to
  // /tmp/xdn/state/<recorder>/<node-id>/<service-name>/e<epoch>/snp/
  public String getSnapshotDir(String serviceName, int epoch) {
    return getServiceBaseDir(serviceName, epoch) + DIR_SNAPSHOT;
  }

  // DIR_COMMITTED_STATEDIFF: Directory that stores committed stateDiffs
  // /tmp/xdn/state/<recorder>/<node-id>/<service-name>/e<epoch>/cmtDiff/
  public String getStateDiffDir(String serviceName, int epoch) {
    return getServiceBaseDir(serviceName, epoch) + DIR_COMMITTED_STATEDIFF;
  }

  // DIR_PROPOSED_STATEDIFF: Directory that stores proposed* stateDiffs
  // (captured stateDiffs whose propose hasn't received an ACK)
  // /tmp/xdn/state/<recorder>/<node-id>/<service-name>/e<epoch>/prpDiff/
  public String getPrpDiffDir(String serviceName, int epoch) {
    return getServiceBaseDir(serviceName, epoch) + DIR_PROPOSED_STATEDIFF;
  }

  // Get specific stateDiff file inside prpDiff/ directory
  public String getPrpDiffFilePath(String serviceName, int epoch, String filename) {
    return getPrpDiffDir(serviceName, epoch) + filename;
  }

  /**
   * Returns the directory for a specific live role (PRIMARY/BACKUP1/BACKUP2) of a service replica.
   * Used by the primary-backup replication path; unrelated to the legacy {@link
   * #getTargetDirectoryOld(String, int)}.
   */
  public String getTargetDirectory(String serviceName, int epoch, LiveDirType type) {
    String base = getServiceBaseDir(serviceName, epoch);
    return switch (type) {
      case PRIMARY -> base + DIR_PRIMARY;
      case BACKUP1 -> base + DIR_BACKUP1;
      case BACKUP2 -> base + DIR_BACKUP2;
    };
  }

  // -------------------------------------------------------------------------
  // Path setup helper functions. Simplifies interaction with directories:
  // - setup directories
  // - write file to directory
  // - move file between directories
  // All these functions are part of AbstractStateDiffRecorder because the logic
  // is identical across all recorders. No need to rewrite the same behavior across them all.
  // -------------------------------------------------------------------------

  // Create the following directories inside
  // /tmp/xdn/state/<recorder>/<node-id>/<service-name>/e<epoch>/:
  // - snpDiff/
  // - cmtDiff/
  // - prpDiff/
  public boolean prepareServiceDirectories(String serviceName, int placementEpoch) {
    int code1 = Shell.runCommand("mkdir -p " + getSnapshotDir(serviceName, placementEpoch));
    int code2 = Shell.runCommand("mkdir -p " + getStateDiffDir(serviceName, placementEpoch));
    int code3 = Shell.runCommand("mkdir -p " + getPrpDiffDir(serviceName, placementEpoch));
    return code1 == 0 && code2 == 0 && code3 == 0;
  }

  // Write a captured stateDiff into a file inside prpDiff/ directory
  public boolean writeToPrpDiff(
      String serviceName, int placementEpoch, String filename, byte[] encodedState) {
    String filePath = getPrpDiffFilePath(serviceName, placementEpoch, filename);
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(filePath)) {
      fos.write(encodedState);
      fos.flush();
      return true;
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }

  // Moves committed stateDiffs from prpDiff/ into cmtDiff/
  public boolean movePrpDiffToCmtDiff(String serviceName, int placementEpoch, String filename) {
    String src = getPrpDiffFilePath(serviceName, placementEpoch, filename);
    String dest = getStateDiffDir(serviceName, placementEpoch) + filename;
    return Shell.runCommand(String.format("mv %s %s", src, dest)) == 0;
  }

  // -------------------------------------------------------------------------
  // Abstract function for the new Primary-Backup (with read-only backup containers)
  // -------------------------------------------------------------------------

  /**
   * Prepares the state directory before the service is initialized, for the new primary-backup
   * design. Called on the primary replica before its container starts.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch.
   * @return true iff all operations successfully executed.
   */
  public abstract boolean preInitialization(String serviceName, int placementEpoch);

  /**
   * Prepares the state directory after the service is initialized, for the new primary-backup
   * design.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return true iff all operations successfully executed.
   */
  public abstract boolean postInitialization(String serviceName, int placementEpoch);

  /**
   * Captures the state diff generated after each request execution, for the new primary-backup
   * design.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return the captured state diff (e.g., new data written into a file).
   */
  public abstract byte[] captureStateDiff(String serviceName, int placementEpoch);

  /**
   * Removes the target directory holding the safety-critical state for the new primary-backup
   * design, including unmounting the filesystem if needed. Mainly used when a service is removed,
   * or its placement epoch is bumped.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return true iff all operations successfully executed.
   */
  public abstract boolean removeServiceRecorder(String serviceName, int placementEpoch);

  // Saves a proposed state diff into cmtDiff/, without applying it to the live snapshot yet
  public abstract boolean saveStateDiff(
      String serviceName, int placementEpoch, byte[] encodedState, String filename);

  // Applies a stateDiff in cmtDiff/ into snpDiff/
  // Assumes that filename exists inside the cmtDiff/ directory
  public abstract boolean applySnpDiff(String serviceName, int placementEpoch, String filename);

  // -------------------------------------------------------------------------
  // Deprecated / Legacy - used only by XdnGigapaxosApp's current (old primary-backup)
  // code paths. Do not delete: still actively called in production.
  // -------------------------------------------------------------------------

  /**
   * Returns the actual directory in which the state is stored for a specific placement epoch.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch.
   * @return the actual path ending with '/' where the state is stored (e.g.,
   *     "/tmp/xdn/state/rsync/ar0/mnt/my-service/e0/")
   */
  public abstract String getTargetDirectoryOld(String serviceName, int placementEpoch);

  /**
   * Prepares the state directory before the service is initialized. Examples of things that we can
   * do include removing stale state, creating needed directory, putting initial state, mounting
   * filesystem, etc.
   *
   * <p>TODO: new @param encodedInitialState the initial state encoded as a string. The content can
   * be defined by the implementation, e.g., it can be a base64-encoded string of the initial state,
   * or a URL to the initial state stored in remote server.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch.
   * @return true iff all operations successfully executed.
   */
  public abstract boolean preInitializationOld(String serviceName, int placementEpoch);

  /**
   * Prepares the state directory after the service is initialized. Examples of things that we can
   * do include gathering the initialization state generated while the service is starting, checking
   * the state integrity, etc.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return true iff all operations successfully executed.
   */
  public abstract boolean postInitializationOld(String serviceName, int placementEpoch);

  /**
   * Captures the state diff generated after each request execution.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return the captured state diff (e.g., new data written into a file).
   */
  public abstract byte[] captureStateDiffOld(String serviceName, int placementEpoch);

  /**
   * Applies the previously captured state diff into the state directory. Mainly used by backups.
   *
   * @deprecated legacy one-shot apply, used only by XdnGigapaxosApp's current code paths. New code
   *     should use {@link #saveStateDiff} followed by {@link #applySnpDiff}.
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @param encodedState the state diff captured by primary.
   * @return true iff all operations successfully executed.
   */
  @Deprecated
  public abstract boolean applyStateDiff(
      String serviceName, int placementEpoch, byte[] encodedState);

  /**
   * Removes the target directory that hold the safety-critical state, include unmounting filesystem
   * if needed. Mainly used when we remove a service, or bump-up the placement epoch.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return true iff all operations successfully executed.
   */
  public abstract boolean removeServiceRecorderOld(String serviceName, int placementEpoch);

  /**
   * @deprecated legacy rsync-based init sync, used only by XdnGigapaxosApp's current code paths
   *     (active when XDN_PB_INIT_SYNC_MODE=RSYNC, the default). New primary-backup code paths
   *     configured with XDN_PB_INIT_SYNC_MODE=RECORDER use {@link #saveStateDiff}/ {@link
   *     #applySnpDiff} instead and never call this method.
   */
  @Deprecated
  public abstract void initContainerSync(
      String myNodeId,
      String serviceName,
      Map<String, InetAddress> ipAddresses,
      int placementEpoch,
      String sshKey);
}
