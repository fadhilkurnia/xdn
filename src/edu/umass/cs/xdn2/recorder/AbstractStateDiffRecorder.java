package edu.umass.cs.xdn2.recorder;

import edu.umass.cs.xdn2.XdnConfig;
import edu.umass.cs.xdn2.utils.Shell;

import java.net.InetAddress;
import java.util.Map;

public abstract class AbstractStateDiffRecorder {

  protected final String nodeID;
  protected final String baseDirectoryPath;

  /**
   * Enumerates the live directory types for a service replica.
   * PRIMARY     — the FUSE-mounted capture directory for the primary container.
   * BACKUP1/2   — the rsync-seeded directories for the two blue/green backup containers.
   */
  public enum LiveDirType {
    PRIMARY,
    BACKUP1,
    BACKUP2
  }

  public static final String DIR_SNAPSHOT  = "snp/";
  public static final String DIR_COMMITTED_STATEDIFF = "cmtDiff/";
  public static final String DIR_PROPOSED_STATEDIFF = "prpDiff/";
  public static final String DIR_PRIMARY   = "primary/";
  public static final String DIR_BACKUP1   = "backup1/";
  public static final String DIR_BACKUP2   = "backup2/";

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

  public String getSnapshotDir(String serviceName, int epoch) {
    return getServiceBaseDir(serviceName, epoch) + DIR_SNAPSHOT;
  }

  public String getStateDiffDir(String serviceName, int epoch) {
    return getServiceBaseDir(serviceName, epoch) + DIR_COMMITTED_STATEDIFF;
  }

  public String getPrpDiffDir(String serviceName, int epoch) {
    return getServiceBaseDir(serviceName, epoch) + DIR_PROPOSED_STATEDIFF;
  }

  public String getPrpDiffFilePath(String serviceName, int epoch, String filename) {
    return getPrpDiffDir(serviceName, epoch) + filename;
  }

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
  // -------------------------------------------------------------------------
  public boolean prepareServiceDirectories(String serviceName, int placementEpoch) {
    int code1 = Shell.runCommand("mkdir -p " + getSnapshotDir(serviceName, placementEpoch));
    int code2 = Shell.runCommand("mkdir -p " + getStateDiffDir(serviceName, placementEpoch));
    int code3 = Shell.runCommand("mkdir -p " + getPrpDiffDir(serviceName, placementEpoch));
    return code1 == 0 && code2 == 0 && code3 == 0;
  }

  public boolean writeToPrpDiff(String serviceName, int placementEpoch,
                                String filename, byte[] encodedState) {
    String filePath = getPrpDiffFilePath(serviceName, placementEpoch, filename);
    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(filePath)) {
      fos.write(encodedState);
      fos.flush();
      return true;
    } catch (java.io.IOException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean movePrpDiffToCmtDiff(String serviceName, int placementEpoch, String filename) {
    String src  = getPrpDiffFilePath(serviceName, placementEpoch, filename);
    String dest = getStateDiffDir(serviceName, placementEpoch) + filename;
    return Shell.runCommand(String.format("mv %s %s", src, dest)) == 0;
  }

  public static AbstractStateDiffRecorder create(XdnConfig config, String nodeId) {
    return switch (config.getRecorderType()) {
      case RSYNC    -> new RsyncStateDiffRecorder(nodeId);
      case FUSELOG -> new FuselogStateDiffRecorder(nodeId, config.getFuselogBaseDir());
      case FUSERUST -> new FuseRustStateDiffRecorder(nodeId);
      case ZIP -> new ZipStateDiffRecorder(nodeId);
      default -> throw new RuntimeException(
              "Unknown recorder type: " + config.getRecorderType());
    };
  }

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

  // -------------------------------------------------------------------------
  // Abstract Functions
  // -------------------------------------------------------------------------

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
  public abstract boolean preInitialization(String serviceName, int placementEpoch);

  /**
   * Prepares the state directory after the service is initialized. Examples of things that we can
   * do include gathering the initialization state generated while the service is starting, checking
   * the state integrity, etc.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return true iff all operations successfully executed.
   */
  public abstract boolean postInitialization(String serviceName, int placementEpoch);

  /**
   * Captures the state diff generated after each request execution.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return the captured state diff (e.g., new data written into a file).
   */
  public abstract byte[] captureStateDiff(String serviceName, int placementEpoch);

  /**
   * Writes the state diff bytes to cmtDiff/<filename>.
   * Does NOT apply the diff to snp/ — that is done separately by applySnpDiff().
   *
   * @param serviceName    name of the service
   * @param placementEpoch current placement epoch
   * @param encodedState   the state diff bytes
   * @param filename       filename e.g. "p0:east1b:5.diff"
   * @return true iff write succeeded
   */
  public abstract boolean saveStateDiff(
          String serviceName, int placementEpoch, byte[] encodedState, String filename);

  /**
   * Applies a previously saved diff from cmtDiff/<filename> to snp/
   * via fuselog-apply. Called by applyCmtDiffToSnpDiff before backup refresh.
   *
   * @param serviceName    name of the service
   * @param placementEpoch current placement epoch
   * @param filename       filename e.g. "p0:east1b:5.diff"
   * @return true iff fuselog-apply succeeded
   */
  public abstract boolean applySnpDiff(
          String serviceName, int placementEpoch, String filename);

  /**
   * Removes the target directory that hold the safety-critical state, include unmounting filesystem
   * if needed. Mainly used when we remove a service, or bump-up the placement epoch.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return true if all operations successfully executed.
   */
  public abstract boolean removeServiceRecorder(String serviceName, int placementEpoch);
}
