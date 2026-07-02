package edu.umass.cs.xdn2.recorder;

import edu.umass.cs.xdn2.utils.Shell;
import edu.umass.cs.xdn2.utils.Utils;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
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
  public boolean prepareServiceDirectories(String serviceName, int placementEpoch) {
    // TODO: implement if this recorder needs per-service directory setup
    return true;
  }

  @Override
  public String getTargetDirectory(String serviceName, int placementEpoch, LiveDirType type) {
    // location: /tmp/xdn/state/rsync/<nodeId>/mnt/<serviceName>/e<epoch>/
    return String.format("%s%s/e%d/", baseMountDirPath, serviceName, placementEpoch);
  }

  @Override
  public String getSnapshotDir(String serviceName, int epoch) {
    // TODO: implement if this recorder needs per-service snapshot dir access
    return "";
  }

  @Override
  public boolean preInitialization(String serviceName, int placementEpoch) {
    // remove and then re-create target mnt dir
    // e.g., /tmp/xdn/state/rsync/node1/mnt/service1/e0/
    String targetDirPath = this.getTargetDirectory(serviceName, placementEpoch, null);

    // Preserve any state already materialized at targetDirPath across the wipe below. This dir is
    // the container's bind-mount source, and by the time preInitialization runs it may already hold
    // the service state: a reconfiguration restored the previous epoch's final state here
    // (reviveContainerizedService), or a promoted backup holds its applied state. Without this, the
    // rm -rf discards it and the container starts empty -- the latest writes are lost. Snapshot the
    // exact bytes, wipe, then restore them. Database-agnostic: no awareness of file internals.
    // snapshotDir is a sibling of mnt/ so the rm -rf below cannot touch it.
    String preinitSnapshotDir = targetDirPath.replaceFirst("/mnt/", "/preinit-snapshot/");
    String[] existingState = new File(targetDirPath).list();
    boolean hadState = existingState != null && existingState.length > 0;
    if (hadState) {
      Shell.runCommand("rm -rf " + preinitSnapshotDir);
      Shell.runCommand("mkdir -p " + preinitSnapshotDir);
      int snapCode =
          Shell.runCommand(String.format("rsync -a %s %s", targetDirPath, preinitSnapshotDir));
      hadState = (snapCode == 0);
    }

    String removeDirCommand = String.format("rm -rf %s", targetDirPath);
    int code = Shell.runCommand(removeDirCommand);
    assert code == 0;
    String createDirCommand = String.format("mkdir -p %s", targetDirPath);
    code = Shell.runCommand(createDirCommand);
    assert code == 0;

    if (hadState) {
      Shell.runCommand(String.format("rsync -a %s %s", preinitSnapshotDir, targetDirPath));
      Shell.runCommand("rm -rf " + preinitSnapshotDir);
    }

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

    // The mount source dir holds a PRIMARY's own container state, but for a BACKUP it is filled
    // asynchronously by the primary's initContainerSync push, which may not have landed when the
    // backup runs postInitialization (the two race). Ensure it exists so the snapshot rsync below
    // cannot fail and abort backup init under -ea -- a backup's authoritative state is
    // (re)established by the primary's push plus the in-band statediffs regardless.
    Shell.runCommand("mkdir -p " + targetSourceDir);

    int removeTargetDirRetCode = Shell.runCommand("rm -rf " + targetDestDir);
    int removeDiffDirRetCode = Shell.runCommand("rm -rf " + targetDiffFile);
    // rsync (not `cp -a`): trailing-slash sources copy CONTENTS flat and portably;
    // `cp -a src/ dst/` nests under an extra dir when dst exists.
    int copySnapshotRetCode =
        Shell.runCommand(String.format("rsync -a %s %s", targetSourceDir, targetDestDir));
    // Soft-check (was an assert): a non-zero here must NOT abort backup init -- it just means the
    // snapshot is momentarily empty/partial, which the push + in-band diffs reconcile.
    if (removeTargetDirRetCode != 0 || removeDiffDirRetCode != 0 || copySnapshotRetCode != 0) {
      System.out.printf(
          "%s: postInitialization snapshot incomplete for %s e%d (rm=%d, rmDiff=%d, rsync=%d);"
              + " continuing -- backup state is (re)established via the primary push + in-band"
              + " statediffs%n",
          this.getClass().getSimpleName(),
          serviceName,
          placementEpoch,
          removeTargetDirRetCode,
          removeDiffDirRetCode,
          copySnapshotRetCode);
    }

    return true;
  }

  @Override
  public byte[] captureStateDiff(String serviceName, int placementEpoch) {
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

    return compressedStateDiff;
  }

  @Override
  public boolean applyStateDiff(String serviceName, int placementEpoch,
                                byte[] encodedState, int primaryEpoch,
                                String primaryID, int stateDiffCount) {
    // TODO: use stateDiffCount for named diff files when these recorders are updated
    // important location:
    // target dir   : /tmp/xdn/state/rsync/<nodeId>/mnt/<serviceName>/e<epoch>/
    // diff file    : /tmp/xdn/state/rsync/<nodeId>/diff/<serviceName>/e<epoch>.diff
    String targetDir =
        String.format("%s%s/e%d/", this.baseMountDirPath, serviceName, placementEpoch);
    String targetDiffFile =
        String.format("%s%s/e%d.diff", this.baseDiffDirPath, serviceName, placementEpoch);

    int retCode = Shell.runCommand("rm -rf " + targetDiffFile);
    assert retCode == 0;

    // decompress
    byte[] compressedStateDiff = encodedState;
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
    String targetDir = this.getTargetDirectory(serviceName, placementEpoch, null);
    int retCode = Shell.runCommand("rm -rf " + targetDir);
    assert retCode == 0;
    return true;
  }
}
