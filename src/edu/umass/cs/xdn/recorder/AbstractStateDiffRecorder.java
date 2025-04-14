package edu.umass.cs.xdn.recorder;

public abstract class AbstractStateDiffRecorder {

    protected final String nodeID;
    protected final String baseDirectoryPath;

    protected AbstractStateDiffRecorder(String nodeID, String basePath) {
        this.nodeID = nodeID;
        this.baseDirectoryPath = basePath;
    }

    abstract public String getTargetDirectory(String serviceName, int placementEpoch);

    abstract public boolean preInitialization(String serviceName, int placementEpoch);

    abstract public boolean postInitialization(String serviceName, int placementEpoch);

    abstract public String captureStateDiff(String serviceName, int placementEpoch);

    abstract public boolean applyStateDiff(String serviceName, int placementEpoch,
                                           String encodedState);

    abstract public boolean removeServiceRecorder(String serviceName, int placementEpoch);

}
