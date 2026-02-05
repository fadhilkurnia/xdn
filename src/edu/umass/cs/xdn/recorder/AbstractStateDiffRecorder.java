package edu.umass.cs.xdn.recorder;

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
   * Returns the actual directory in which the state is stored for a specific placement epoch.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch.
   * @return the actual path ending with '/' where the state is stored (e.g.,
   *     "/tmp/xdn/state/rsync/ar0/mnt/my-service/e0/")
   */
  public abstract String getTargetDirectory(String serviceName, int placementEpoch);

  /**
   * Prepares the state directory before the service is initialized. Examples of things that we can
   * do include removing stale state, creating needed directory, mounting filesystem, etc.
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
  public abstract String captureStateDiff(String serviceName, int placementEpoch);

  /**
   * Applies the previously captured state diff into the state directory. Mainly used by backups.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @param encodedState the state diff captured by primary.
   * @return true iff all operations successfully executed.
   */
  public abstract boolean applyStateDiff(
      String serviceName, int placementEpoch, String encodedState);

  /**
   * Removes the target directory that hold the safety-critical state. Mainly used when we remove a
   * service, or bump-up the placement epoch.
   *
   * @param serviceName name of the app/service (e.g., "my-service")
   * @param placementEpoch current placement epoch number.
   * @return true iff all operations successfully executed.
   */
  public abstract boolean removeServiceRecorder(String serviceName, int placementEpoch);

  public abstract void initContainerSync(
      String myNodeId,
      String serviceName,
      Map<String, InetAddress> ipAddresses,
      int placementEpoch,
      String sshKey);
}
