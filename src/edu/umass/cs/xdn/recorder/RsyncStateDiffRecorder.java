package edu.umass.cs.xdn.recorder;

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
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RsyncStateDiffRecorder extends AbstractStateDiffRecorder {

  private static final String RSYNC_BIN_PATH = "/usr/bin/rsync";
  private static final String defaultWorkingBasePath = "/tmp/xdn/state/rsync/";

  private final String baseMountDirPath;
  private final String baseSnapshotDirPath;
  private final String baseDiffDirPath;

  public RsyncStateDiffRecorder(String nodeID) {
    super(nodeID, defaultWorkingBasePath + nodeID + "/mnt/");
    File rsync = new File(RSYNC_BIN_PATH);
    assert rsync.exists() : "rsync binary does not exist at " + RSYNC_BIN_PATH;

    // create working mount dir, if not exist
    // e.g., /tmp/xdn/state/rsync/node1/mnt/
    this.baseMountDirPath = this.baseDirectoryPath;
    try {
      Files.createDirectories(Paths.get(this.baseMountDirPath));
    } catch (IOException e) {
      System.err.println("err: " + e);
      throw new RuntimeException(e);
    }

    // create snapshot dir, if not exist
    // e.g., /tmp/xdn/state/rsync/node1/snp/
    this.baseSnapshotDirPath = defaultWorkingBasePath + nodeID + "/snp/";
    try {
      Files.createDirectories(Paths.get(this.baseSnapshotDirPath));
    } catch (IOException e) {
      System.err.println("err: " + e);
      throw new RuntimeException(e);
    }

    // create diff dir, if not exist
    // e.g., /tmp/xdn/state/rsync/node1/diff/
    this.baseDiffDirPath = defaultWorkingBasePath + nodeID + "/diff/";
    try {
      Files.createDirectories(Paths.get(this.baseDiffDirPath));
    } catch (IOException e) {
      System.err.println("err: " + e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public String getTargetDirectory(String serviceName, int placementEpoch) {
    // location: /tmp/xdn/state/rsync/<nodeId>/mnt/<serviceName>/e<epoch>/
    return String.format("%s%s/e%d/", baseMountDirPath, serviceName, placementEpoch);
  }

  @Override
  public boolean preInitialization(String serviceName, int placementEpoch) {
    // remove and then re-create target mnt dir
    // e.g., /tmp/xdn/state/rsync/node1/mnt/service1/e0/
    String targetDirPath = this.getTargetDirectory(serviceName, placementEpoch);
    String removeDirCommand = String.format("rm -rf %s", targetDirPath);
    int code = Shell.runCommand(removeDirCommand);
    assert code == 0;
    String createDirCommand = String.format("mkdir -p %s", targetDirPath);
    code = Shell.runCommand(createDirCommand);
    assert code == 0;

    // remove and then re-create snapshot dir
    // e.g., /tmp/xdn/state/rsync/node1/snp/service1/e0/
    String snapshotDirPath =
        String.format("%s%s/e%d/", this.baseSnapshotDirPath, serviceName, placementEpoch);
    removeDirCommand = String.format("rm -rf %s", snapshotDirPath);
    code = Shell.runCommand(removeDirCommand);
    assert code == 0;
    createDirCommand = String.format("mkdir -p %s", snapshotDirPath);
    code = Shell.runCommand(createDirCommand);
    assert code == 0;

    // remove and then re-create stateDiff dir
    // e.g., /tmp/xdn/state/rsync/node1/diff/service1/
    String stateDiffDirPath = String.format("%s%s/", this.baseDiffDirPath, serviceName);
    removeDirCommand = String.format("rm -rf %s", stateDiffDirPath);
    code = Shell.runCommand(removeDirCommand);
    assert code == 0;
    createDirCommand = String.format("mkdir -p %s", stateDiffDirPath);
    code = Shell.runCommand(createDirCommand);
    assert code == 0;

    return true;
  }

  @Override
  public boolean postInitialization(String serviceName, int placementEpoch) {
    // for rsync, assuming the initialization is deterministic, we update the state in
    // the snapshot dir.
    // mount dir    : /tmp/xdn/state/rsync/<nodeId>/mnt/<serviceName>/e<epoch>/
    // snapshot dir : /tmp/xdn/state/rsync/<nodeId>/snp/<serviceName>/e<epoch>/
    // diff file    : /tmp/xdn/state/rsync/<nodeId>/diff/<serviceName>/e<epoch>.diff
    String targetSourceDir =
        String.format("%s%s/e%d/", this.baseMountDirPath, serviceName, placementEpoch);
    String targetDestDir =
        String.format("%s%s/e%d/", this.baseSnapshotDirPath, serviceName, placementEpoch);
    String targetDiffFile =
        String.format("%s%s/e%d.diff", this.baseDiffDirPath, serviceName, placementEpoch);

    int removeTargetDirRetCode = Shell.runCommand("rm -rf " + targetDestDir);
    int removeDiffDirRetCode = Shell.runCommand("rm -rf " + targetDiffFile);
    int copySnapshotRetCode =
        Shell.runCommand(String.format("cp -a %s %s", targetSourceDir, targetDestDir));
    assert removeTargetDirRetCode == 0 && removeDiffDirRetCode == 0 && copySnapshotRetCode == 0;

    return true;
  }

  @Override
  public String captureStateDiff(String serviceName, int placementEpoch) {
    // important location:
    // mount dir    : /tmp/xdn/state/rsync/<nodeId>/mnt/<serviceName>/e<epoch>/
    // snapshot dir : /tmp/xdn/state/rsync/<nodeId>/snp/<serviceName>/e<epoch>/
    // diff file    : /tmp/xdn/state/rsync/<nodeId>/diff/<serviceName>/e<epoch>.diff
    String targetSourceDir =
        String.format("%s%s/e%d/", this.baseMountDirPath, serviceName, placementEpoch);
    String targetDestDir =
        String.format("%s%s/e%d/", this.baseSnapshotDirPath, serviceName, placementEpoch);
    String targetDiffFile =
        String.format("%s%s/e%d.diff", this.baseDiffDirPath, serviceName, placementEpoch);

    String command =
        String.format(
            "%s -ar --write-batch=%s %s %s",
            RSYNC_BIN_PATH, targetDiffFile, targetSourceDir, targetDestDir);
    int exitCode = Shell.runCommand(command, true);
    if (exitCode != 0) {
      throw new RuntimeException("failed to capture stateDiff");
    }

    // read diff into byte[]
    byte[] stateDiff;
    try {
      stateDiff = Files.readAllBytes(Path.of(targetDiffFile));
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

    // convert the compressed stateDiff to String
    return Base64.getEncoder().encodeToString(compressedStateDiff);
  }

  @Override
  public boolean applyStateDiff(String serviceName, int placementEpoch, String encodedState) {
    // important location:
    // target dir   : /tmp/xdn/state/rsync/<nodeId>/mnt/<serviceName>/e<epoch>/
    // diff file    : /tmp/xdn/state/rsync/<nodeId>/diff/<serviceName>/e<epoch>.diff
    String targetDir =
        String.format("%s%s/e%d/", this.baseMountDirPath, serviceName, placementEpoch);
    String targetDiffFile =
        String.format("%s%s/e%d.diff", this.baseDiffDirPath, serviceName, placementEpoch);

    int retCode = Shell.runCommand("rm -rf " + targetDiffFile);
    assert retCode == 0;

    // convert stateDiff from String back to byte[], then decompress
    byte[] compressedStateDiff = Base64.getDecoder().decode(encodedState);
    byte[] stateDiff;
    try {
      stateDiff = Utils.decompressBytes(compressedStateDiff);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // write stateDiff to .diff file
    try {
      Files.write(
          Paths.get(targetDiffFile),
          stateDiff,
          StandardOpenOption.CREATE,
          StandardOpenOption.DSYNC);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // apply the stateDiff inside the .diff file using rsync
    String command =
        String.format("%s -ar --read-batch=%s %s", RSYNC_BIN_PATH, targetDiffFile, targetDir);
    retCode = Shell.runCommand(command);
    assert retCode == 0;

    return true;
  }

  @Override
  public boolean removeServiceRecorder(String serviceName, int placementEpoch) {
    String targetDir = this.getTargetDirectory(serviceName, placementEpoch);
    int retCode = Shell.runCommand("rm -rf " + targetDir);
    assert retCode == 0;
    return true;
  }

  /**********************************************************************************************
   *                        Non-Deterministic Initialization Methods                            *
   *********************************************************************************************/
  @Override
  public String getDefaultBasePath() {
    return RsyncStateDiffRecorder.defaultWorkingBasePath;
  }

  @Override
  public void initContainerSync(
      String myNodeId,
      String serviceName,
      Map<String, InetAddress> ipAddresses,
      int placementEpoch,
      String sshKey) {
    Set<String> backupNodes =
        ipAddresses.keySet().stream()
            .filter(node -> !node.equals(myNodeId.toString()))
            .map(String::toLowerCase)
            .collect(Collectors.toSet());

    String currentReplica = String.format("%s%s/", this.defaultWorkingBasePath, myNodeId);

    Map<String, String> backupReplicas = new HashMap<>();
    backupNodes.forEach(
        node ->
            backupReplicas.put(node, String.format("%s%s/", this.defaultWorkingBasePath, node)));

    String mntDir = String.format("mnt/%s/", serviceName);
    String snpDir = String.format("snp/%s/", serviceName);

    String username = Shell.runCommandWithOutput("whoami").stdout.trim();

    while (true) {
      int exitCode =
          Shell.runCommand(
              String.format(
                  "rsync -avz --delete --human-readable %s/%s %s/%s",
                  currentReplica, mntDir, currentReplica, snpDir),
              true);

      if (exitCode != 0) {
        System.out.printf("Failed to sync /mnt/ to /snp/ in %s%n", currentReplica);
      } else {
        break;
      }

      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // Copy data to other replicas
    Boolean allSyncSuccess = false;
    String sshOption =
        sshKey != null && !sshKey.trim().isEmpty() ? String.format("-e \"ssh -i %s\"", sshKey) : "";
    int count = 0;
    while (!allSyncSuccess) {
      if (++count > 10) {
        throw new RuntimeException(
            String.format(
                "%s failed to rsync files for %s:%d during non-deterministic init after %d tries",
                this.getClass().getSimpleName(), serviceName, placementEpoch, count));
      }

      allSyncSuccess = true;

      for (String key : backupReplicas.keySet()) {
        String hostAddr = ipAddresses.get(key).getHostAddress();

        int exitCode = 0;
        if (hostAddr.equals("127.0.0.1")) {
          exitCode =
              Shell.runCommand(
                  String.format(
                      """
                                    rsync -avz --delete --human-readable \
                                    --include='mnt/' --include='%s' --include='%s***' \
                                    --exclude='*' \
                                    %s %s""",
                      mntDir, mntDir, currentReplica, backupReplicas.get(key)),
                  true);
        } else {
          exitCode =
              Shell.runCommand(
                  String.format(
                      """
                                    rsync -avz --delete --human-readable \
                                    %s \
                                    --include='mnt/' --include='%s' --include='%s***' \
                                    --exclude='*' \
                                    %s %s@%s:%s""",
                      sshOption,
                      mntDir,
                      mntDir,
                      currentReplica,
                      username,
                      hostAddr,
                      backupReplicas.get(key)),
                  true);
        }

        if (exitCode != 0) {
          System.out.println(
              String.format("Failed to sync %s to %s", currentReplica, backupReplicas.get(key)));
          allSyncSuccess = false;
        }
      }

      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }
}
