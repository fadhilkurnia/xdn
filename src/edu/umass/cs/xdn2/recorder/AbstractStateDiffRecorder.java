package edu.umass.cs.xdn2.recorder;

import edu.umass.cs.xdn2.XdnConfig;

import java.net.InetAddress;
import java.util.Map;

public abstract class AbstractStateDiffRecorder {

  protected final String nodeID;
  protected final String baseDirectoryPath;

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
  public static final String DIR_STATEDIFF = "cmtDiff/";
  public static final String DIR_STATEDIFF_UNCOMMITED = "prpDiff/";
  public static final String DIR_PRIMARY   = "primary/";
  public static final String DIR_BACKUP1   = "backup1/";
  public static final String DIR_BACKUP2   = "backup2/";

  /**
   * Creates the service-specific directories needed before the service starts.
   * For fuselog: creates snapshot/ and stateDiff/.
   * For other recorders: may be a no-op.
   *
   * Called once per service per epoch on all nodes (primary and backup)
   * before any container starts.
   *
   * @param serviceName    name of the service
   * @param placementEpoch current placement epoch
   * @return true iff all directories were successfully created
   */
  public abstract boolean prepareServiceDirectories(String serviceName, int placementEpoch);

  /**
   * Returns the actual directory in which the state is stored for a specific placement epoch.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch.
   * @return the actual path ending with '/' where the state is stored (e.g.,
   *     "/tmp/xdn/state/rsync/ar0/mnt/my-service/e0/")
   */
  public abstract String getTargetDirectory(String serviceName, int placementEpoch, LiveDirType type);

  /**
   * Returns the snapshot directory path for a service epoch.
   * This is the directory where applyStateDiff writes committed state,
   * and from which primaryLive/ and backupLive1/ or backupLive2/ are seeded via rsync.
   */
  public abstract String getSnapshotDir(String serviceName, int placementEpoch);

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
   * Applies the previously captured state diff into snapshot/.
   * The stateDiffCount is used to name the diff file (e.g., stateDiff/42.diff).
   *
   * @param serviceName      name of the app/service
   * @param placementEpoch   current placement epoch number
   * @param encodedState     the state diff bytes captured by primary
   * @param primaryEpoch     primary epoch number (different from placement epoch)
   * @param primaryID        node-id of the primary that proposes the stateDiff
   * @param stateDiffCount   the monotonically increasing count for this diff
   * @return true if all operations successfully executed
   */
  public abstract boolean applyStateDiff(
          String serviceName, int placementEpoch, byte[] encodedState,
          int primaryEpoch, String primaryID, int stateDiffCount);

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
