package edu.umass.cs.xdn.recorder;

import edu.umass.cs.utils.ZipFiles;
import edu.umass.cs.xdn.utils.Shell;
import edu.umass.cs.xdn.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Base64;
import java.util.Map;

public class ZipStateDiffRecorder extends AbstractStateDiffRecorder {

  private static final String defaultWorkingBasePath = "/tmp/xdn/state/zip/";

  private final String baseMountDirPath;
  private final String baseSnapshotDirPath;
  private final String baseZipDirPath;

  public ZipStateDiffRecorder(String nodeID) {
    super(nodeID, defaultWorkingBasePath + nodeID + "/mnt/");

    // create working mount dir, if not exist
    // e.g., /tmp/xdn/state/zip/node1/mnt/
    this.baseMountDirPath = this.baseDirectoryPath;
    try {
      Files.createDirectories(Paths.get(this.baseMountDirPath));
    } catch (IOException e) {
      System.err.println("ERROR: " + e);
      throw new RuntimeException(e);
    }

    // create snapshot dir, if not exist
    // e.g., /tmp/xdn/state/zip/node1/snp/
    this.baseSnapshotDirPath = defaultWorkingBasePath + nodeID + "/snp/";
    try {
      Files.createDirectories(Paths.get(this.baseSnapshotDirPath));
    } catch (IOException e) {
      System.err.println("ERROR: " + e);
      throw new RuntimeException(e);
    }

    // create diff dir, if not exist
    // e.g., /tmp/xdn/state/zip/node1/diff/
    this.baseZipDirPath = defaultWorkingBasePath + nodeID + "/diff/";
    try {
      Files.createDirectories(Paths.get(this.baseZipDirPath));
    } catch (IOException e) {
      System.err.println("ERROR: " + e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getTargetDirectory(String serviceName, int placementEpoch) {
    // location: /tmp/xdn/state/zip/<nodeId>/mnt/<serviceName>/e<epoch>/
    return String.format("%s%s/e%d/", baseMountDirPath, serviceName, placementEpoch);
  }

  @Override
  public boolean preInitialization(String serviceName, int placementEpoch) {
    // remove and re-create target mnt dir
    // e.g., /tmp/xdn/state/rsync/node1/mnt/service1/e0/
    String targetDir = this.getTargetDirectory(serviceName, placementEpoch);
    try {
      int code = Shell.runCommand("rm -rf " + targetDir);
      assert code == 0;
      Files.createDirectory(Paths.get(targetDir));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // remove and re-create snapshot dir
    // e.g., /tmp/xdn/state/rsync/node1/snp/service1/e0/
    String snapshotDirPath =
        String.format("%s%s/e%d/", this.baseSnapshotDirPath, serviceName, placementEpoch);
    try {
      int code = Shell.runCommand("rm -rf " + snapshotDirPath);
      assert code == 0;
      Files.createDirectory(Paths.get(snapshotDirPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // remove and re-create state diff dir
    // e.g., /tmp/xdn/state/rsync/node1/diff/service1/
    String stateDiffDirPath = String.format("%s%s/", this.baseZipDirPath, serviceName);
    try {
      int code = Shell.runCommand("rm -rf " + stateDiffDirPath);
      assert code == 0;
      Files.createDirectory(Paths.get(stateDiffDirPath));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return true;
  }

  @Override
  public boolean postInitialization(String serviceName, int placementEpoch) {
    // do nothing
    return true;
  }

  @Override
  public String captureStateDiff(String serviceName, int placementEpoch) {
    // for rsync, assuming the initialization is deterministic, we update the state in
    // the snapshot dir.
    // mount dir    : /tmp/xdn/state/zip/<nodeId>/mnt/<serviceName>/e<epoch>/
    // snapshot dir : /tmp/xdn/state/zip/<nodeId>/snp/<serviceName>/e<epoch>/
    // diff file    : /tmp/xdn/state/zip/<nodeId>/diff/<serviceName>/e<epoch>.zip
    String targetMountDir =
        String.format("%s%s/e%d/", this.baseMountDirPath, serviceName, placementEpoch);
    String targetSnpDir =
        String.format("%s%s/e%d/", this.baseSnapshotDirPath, serviceName, placementEpoch);
    String targetZipFile =
        String.format("%s%s/e%d.zip", this.baseZipDirPath, serviceName, placementEpoch);

    // remove previous snapshot, if any
    Shell.runCommand("rm -rf " + targetSnpDir);

    // remove previous snapshot zip file, if any
    Shell.runCommand("rm -rf " + targetZipFile);

    // copy the whole state
    int exitCode = Shell.runCommand(String.format("cp -a %s %s", targetMountDir, targetSnpDir));
    if (exitCode != 0) {
      throw new RuntimeException("failed to copy the state");
    }

    // archive the copied state
    ZipFiles.zipDirectory(new File(targetSnpDir), targetZipFile);

    // read the archive into byte[]
    byte[] stateDiff;
    try {
      stateDiff = Files.readAllBytes(Path.of(targetZipFile));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // compress stateDiff
    byte[] compressedStateDiff;
    try {
      compressedStateDiff = Utils.compressBytes(stateDiff);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    return Base64.getEncoder().encodeToString(compressedStateDiff);
  }

  @Override
  public boolean applyStateDiff(String serviceName, int placementEpoch, String encodedState) {
    // important location
    // mount dir    : /tmp/xdn/state/zip/<nodeId>/mnt/<serviceName>/e<epoch>/
    // snapshot dir : /tmp/xdn/state/zip/<nodeId>/snp/<serviceName>/e<epoch>/
    // diff file    : /tmp/xdn/state/zip/<nodeId>/diff/<serviceName>/e<epoch>.zip
    String targetMountDir =
        String.format("%s%s/e%d/", this.baseMountDirPath, serviceName, placementEpoch);
    String targetSnpDir =
        String.format("%s%s/e%d/", this.baseSnapshotDirPath, serviceName, placementEpoch);
    String targetZipFile =
        String.format("%s%s/e%d.zip", this.baseZipDirPath, serviceName, placementEpoch);

    // convert the compressed stateDiff back to byte[]
    byte[] compressedStateDiff = Base64.getDecoder().decode(encodedState);

    // decompress the stateDiff
    byte[] stateDiff;
    try {
      stateDiff = Utils.decompressBytes(compressedStateDiff);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // write the stateDiff back to .zip file
    try {
      Files.write(
          Paths.get(targetZipFile), stateDiff, StandardOpenOption.CREATE, StandardOpenOption.DSYNC);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // un-archive the .zip file
    ZipFiles.unzip(targetZipFile, targetSnpDir);

    // copy back the un-archived state to the mount dir
    int exitCode = Shell.runCommand(String.format("cp -a %s %s", targetSnpDir, targetMountDir));
    if (exitCode != 0) {
      throw new RuntimeException("failed to apply zip stateDiff");
    }

    return true;
  }

  @Override
  public boolean removeServiceRecorder(String serviceName, int placementEpoch) {
    String targetMountDir = this.getTargetDirectory(serviceName, placementEpoch);
    int code = Shell.runCommand("rm -rf " + targetMountDir);
    assert code == 0;
    return true;
  }

  /**********************************************************************************************
   *                        Non-Deterministic Initialization Methods                            *
   *********************************************************************************************/
  @Override
  public String getDefaultBasePath() {
    return ZipStateDiffRecorder.defaultWorkingBasePath;
  }

  @Override
  public void initContainerSync(
      String myNodeId,
      String serviceName,
      Map<String, InetAddress> ipAddresses,
      int placementEpoch,
      String sshKey) {
    System.out.println("Multi-file initialization is not supported on Zip.");
    return;
  }
}
