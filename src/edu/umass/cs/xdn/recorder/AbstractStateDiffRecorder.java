package edu.umass.cs.xdn.recorder;

import java.net.InetAddress;
import java.util.Map;

public abstract class AbstractStateDiffRecorder {

  protected final String nodeID;
  protected final String baseDirectoryPath;

  protected AbstractStateDiffRecorder(String nodeID, String basePath) {
    this.nodeID = nodeID;
    this.baseDirectoryPath = basePath;
  }

  public abstract String getTargetDirectory(String serviceName, int placementEpoch);

  public abstract boolean preInitialization(String serviceName, int placementEpoch);

  public abstract boolean postInitialization(String serviceName, int placementEpoch);

  public abstract String captureStateDiff(String serviceName, int placementEpoch);

  public abstract boolean applyStateDiff(
      String serviceName, int placementEpoch, String encodedState);

  public abstract boolean removeServiceRecorder(String serviceName, int placementEpoch);

  @Deprecated
  public abstract String getDefaultBasePath();

  public abstract void initContainerSync(
      String myNodeId,
      String serviceName,
      Map<String, InetAddress> ipAddresses,
      int placementEpoch,
      String sshKey);
}
