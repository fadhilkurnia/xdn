package edu.umass.cs.xdn.recorder;

import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.utils.Shell;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

public class FuselogStateDiffRecorder extends AbstractStateDiffRecorder {

  private static final String FUSELOG_BIN_PATH = "/usr/local/bin/fuselog";
  private static final String FUSELOG_APPLY_BIN_PATH = "/usr/local/bin/fuselog-apply";

  private static final String defaultWorkingBasePath = "/tmp/xdn/state/fuselog/";

  // the default working base directory is /tmp/xdn/state/fuselog/
  private static final String workingBasePath =
      Config.getGlobalString(ReconfigurationConfig.RC.XDN_FUSELOG_BASE_DIR);

  private final String baseMountDirPath;
  private final String baseSocketDirPath;
  private final String baseDiffDirPath;

  // Few important state and working directories are defined as below.
  //  In the primary replica, there is the mount directory where the fuselog filesystem is mounted.
  //  Because there can be multiple placement epochs for the same service, we put the epoch number
  //  in the path to differentiate them:
  //    /tmp/xdn/state/fuselog/<node-id>/
  //    /tmp/xdn/state/fuselog/<node-id>/mnt/
  //    /tmp/xdn/state/fuselog/<node-id>/mnt/<service-name>/e<epoch>/
  //
  //  Still in the primary replica, for each service and epoch, there is a socket file for the
  //  mounted filesystem to receive commands (e.g., capture stateDiff):
  //    /tmp/xdn/state/fuselog/<node-id>/sock/
  //    /tmp/xdn/state/fuselog/<node-id>/sock/<service-name>::e<epoch>.sock
  //
  //  In the backup replicas, there is no mounted filesystem, but there is diff directory to store
  //  the latest stateDiff file received from the primary replica, which will be applied to the
  //  target directory by the fuselog-apply program:
  //    /tmp/xdn/state/fuselog/<node-id>/diff/
  //    /tmp/xdn/state/fuselog/<node-id>/diff/<service-name>::e<epoch>.diff
  //
  //  TODO: introduce blob directory to decouple statediff transfer and order agreement.
  //  Using blob directory, the primary replica can write the captured stateDiff into a blob file
  //  and only returns the blob's digest in the captureStateDiff() method. Then the backup replicas
  //  can fetch the stateDiff blob file from the primary replica based on the digest and apply it to
  //  the target directory. This can avoid the extra overhead of transferring large stateDiff via
  //  the coordination layer (e.g., paxos) can make the stateDiff transfer more robust by leveraging
  //  existing blob transfer protocols (i.e., rsync). The blob directory is defined as below:
  //   /tmp/xdn/state/fuselog/<node-id>/blobs/<service-name>::e<epoch>::<digest>.blob

  // mapping service name and epoch into its fuselog filesystem socket
  private final Map<String, Map<Integer, SocketChannel>> serviceFsSocket;

  private final Logger logger = Logger.getLogger(FuselogStateDiffRecorder.class.getSimpleName());

  public FuselogStateDiffRecorder(String nodeID) {
    super(nodeID, workingBasePath + nodeID + "/");
    logger.log(
        Level.INFO,
        String.format(
            "%s:%s - initializing FUSE stateDiff recorder",
            nodeID, FuselogStateDiffRecorder.class.getSimpleName()));

    // Ensure that fuselog and fuselog-apply binaries exist.
    File fuselog = new File(FUSELOG_BIN_PATH);
    if (!fuselog.exists()) {
      String errMessage = "fuselog binary does not exist at " + FUSELOG_BIN_PATH;
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - %s", nodeID, FuselogStateDiffRecorder.class.getSimpleName(), errMessage));
      throw new RuntimeException(errMessage);
    }
    File fuselogApplicator = new File(FUSELOG_APPLY_BIN_PATH);
    if (!fuselogApplicator.exists()) {
      String errMessage = "fuselog-apply binary does not exist at " + FUSELOG_APPLY_BIN_PATH;
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - %s", nodeID, FuselogStateDiffRecorder.class.getSimpleName(), errMessage));
      throw new RuntimeException(errMessage);
    }

    // Create working mount dir, if not yet exist.
    // e.g., /tmp/xdn/state/fuselog/node1/mnt/
    this.baseMountDirPath = this.baseDirectoryPath + "mnt/";
    try {
      Files.createDirectories(Paths.get(this.baseMountDirPath));
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Failed to create mount directory: " + this.baseMountDirPath, e);
      throw new RuntimeException(e);
    }

    // Create socket dir, if not yet exist.
    // e.g., /tmp/xdn/state/fuselog/node1/sock/
    this.baseSocketDirPath = workingBasePath + nodeID + "/sock/";
    try {
      Files.createDirectories(Paths.get(this.baseSocketDirPath));
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Failed to create socket directory: " + this.baseSocketDirPath, e);
      throw new RuntimeException(e);
    }

    // Create diff dir, if not yet exist.
    // e.g., /tmp/xdn/state/fuselog/node1/diff/
    this.baseDiffDirPath = workingBasePath + nodeID + "/diff/";
    try {
      Files.createDirectories(Paths.get(this.baseDiffDirPath));
    } catch (IOException e) {
      logger.log(Level.SEVERE, "Failed to create diff directory: " + this.baseDiffDirPath, e);
      throw new RuntimeException(e);
    }

    // Initialize mapping between serviceName to the FS socket so we can send commands to the
    // filesystem to capture stateDiff later.
    this.serviceFsSocket = new ConcurrentHashMap<>();
  }

  @Override
  public String getTargetDirectory(String serviceName, int placementEpoch) {
    // Location: /tmp/xdn/state/fuselog/<node-id>/mnt/<service-name>/e<epoch>/
    return String.format("%s%s/e%d/", baseMountDirPath, serviceName, placementEpoch);
  }

  @Override
  public boolean preInitialization(String serviceName, int placementEpoch) {
    String targetDir = this.getTargetDirectory(serviceName, placementEpoch);
    String socketFile = baseSocketDirPath + serviceName + "::" + placementEpoch + ".sock";

    // Create target mnt dir, if not yet exist.
    // e.g., /tmp/xdn/state/fuselog/node1/mnt/service1/
    Shell.runCommand("sudo umount " + targetDir + " > /dev/null 2>&1");
    Shell.runCommand("rm -rf " + targetDir);
    int code = Shell.runCommand("mkdir -p " + targetDir);
    if (code != 0) {
      String errMessage =
          String.format(
              "failed to create target mount directory %s with exit code %d", targetDir, code);
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - %s",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), errMessage));
      throw new RuntimeException(errMessage);
    }

    // Initialize filesystem by mounting it on the mnt dir, preparing its socket file.
    // Note that the filesystem is intended to be mounted in the primary replica during
    // pre-initialization.
    assert targetDir.length() > 1 : "invalid target mount directory";
    assert targetDir.endsWith("/") : "target mount directory should end with '/'";
    // remove the trailing '/' at the end of targetDir
    String targetDirPath = targetDir.substring(0, targetDir.length() - 1);

    // Note: We need `allow_other` option so the containerized service can also
    // access the mounted filesystem. This requires `user_allow_other` to be set in
    // /etc/fuse.conf and the recorder to be run with root privilege.
    // We do not use the `allow_root` that is more restrictive than `allow_other`.
    String cmd = String.format("%s -o allow_other %s", FUSELOG_BIN_PATH, targetDirPath);

    Map<String, String> env = new HashMap<>();
    env.put("FUSELOG_SOCKET_FILE", socketFile);
    int exitCode = Shell.runCommand(cmd, false, env);
    if (exitCode != 0) {
      String errMessage =
          String.format("failed to mount filesystem at %s with exit code %d", targetDir, exitCode);
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - %s",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), errMessage));
      throw new RuntimeException(errMessage);
    }

    // Initialize socket client for the filesystem.
    SocketChannel socketChannel;
    UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(socketFile));
    try {
      socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
      boolean isConnEstablished = socketChannel.connect(address);
      if (!isConnEstablished) {
        logger.log(
            Level.SEVERE,
            String.format(
                "%s:%s - failed to connect to the filesystem socket at %s",
                this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), socketFile));
        return false;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Update the socket metadata.
    if (!serviceFsSocket.containsKey(serviceName)) {
      serviceFsSocket.put(serviceName, new ConcurrentHashMap<>());
    }
    serviceFsSocket.get(serviceName).put(placementEpoch, socketChannel);

    return true;
  }

  @Override
  public boolean postInitialization(String serviceName, int placementEpoch) {
    assert serviceName != null : "serviceName should not be null";
    assert placementEpoch >= 0 : "placementEpoch should be non-negative";
    // TODO: read initialization stateDiff and discard it
    return true;
  }

  @Override
  public String captureStateDiff(String serviceName, int placementEpoch) {
    assert serviceName != null : "serviceName should not be null";
    assert placementEpoch >= 0 : "placementEpoch should be non-negative";

    Map<Integer, SocketChannel> epochToChannelMap = serviceFsSocket.get(serviceName);
    assert epochToChannelMap != null : "unknown fs socket client for " + serviceName;
    SocketChannel socketChannel = epochToChannelMap.get(placementEpoch);
    assert socketChannel != null : "unknown fs socket client for " + serviceName;

    long startTime = System.nanoTime();

    // Send get command (g) to the filesystem
    try {
      logger.log(
          Level.FINEST,
          String.format(
              "%s:%s - sending FuselogFS command",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName()));
      socketChannel.write(ByteBuffer.wrap("g".getBytes()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Wait for response indicating the stateDiff size.
    ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
    sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
    sizeBuffer.clear();
    logger.log(
        Level.FINEST,
        String.format(
            "%s:%s - reading FuselogFS response",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName()));
    int numRead;
    try {
      numRead = socketChannel.read(sizeBuffer);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (numRead < 8) {
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - failed to read stateDiff size from filesystem socket, expected 8 bytes but"
                  + " got %d bytes",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), numRead));
      return null;
    }
    long stateDiffSize = sizeBuffer.getLong(0);
    logger.log(
        Level.FINER,
        String.format(
            "%s:%s - receiving stateDiff with size=%d bytes",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), stateDiffSize));
    assert stateDiffSize >= 0 : "invalid stateDiff size received from filesystem socket";

    // Read all the stateDiff based on the obtained size.
    ByteBuffer stateDiffBuffer = ByteBuffer.allocate((int) stateDiffSize);
    numRead = 0;
    try {
      while (numRead < stateDiffSize) {
        numRead += socketChannel.read(stateDiffBuffer);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Convert the stateDiff into String using Base64
    String stateDiff = Base64.getEncoder().encodeToString(stateDiffBuffer.array());
    logger.log(
        Level.FINER,
        String.format(
            "%s:%s - Base64-encoded stateDiff size=%d bytes",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), stateDiff.length()));
    logger.log(
        Level.FINEST,
        String.format(
            "%s:%s - encoded stateDiff: %s ",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), stateDiff));

    long endTime = System.nanoTime();
    long elapsedTime = endTime - startTime;
    double elapsedTimeMs = (double) elapsedTime / 1_000_000.0;
    logger.log(
        Level.FINER,
        String.format(
            "%s:%s - capturing stateDiff within %f ms",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), elapsedTimeMs));

    return stateDiff;
  }

  @Override
  public boolean applyStateDiff(String serviceName, int placementEpoch, String encodedState) {
    assert serviceName != null : "serviceName should not be null";
    assert placementEpoch >= 0 : "placementEpoch should be non-negative";
    assert encodedState != null : "encoded stateDiff should not be null";

    logger.log(
        Level.FINER,
        String.format(
            "%s:%s - applying stateDiff name=%s epoch=%d size=%d bytes",
            this.nodeID,
            FuselogStateDiffRecorder.class.getSimpleName(),
            serviceName,
            placementEpoch,
            encodedState.length()));
    logger.log(
        Level.FINEST,
        String.format(
            "%s:%s - applying stateDiff: %s",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), encodedState));

    String diffFile = this.baseDiffDirPath + serviceName + "::" + placementEpoch + ".diff";
    String targetDir = this.getTargetDirectory(serviceName, placementEpoch);

    // Store stateDiff into an external .diff file.
    byte[] stateDiff;
    try {
      FileOutputStream outputStream;
      outputStream = new FileOutputStream(diffFile);
      stateDiff = Base64.getDecoder().decode(encodedState);
      outputStream.write(stateDiff);
      outputStream.flush();
      outputStream.close();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Prepare the shell command to apply stateDiff.
    String cmd =
        String.format("%s %s --silent --statediff=%s", FUSELOG_APPLY_BIN_PATH, targetDir, diffFile);
    int exitCode = Shell.runCommand(cmd, true);
    if (exitCode != 0) {
      String errMessage =
          String.format(
              "failed to apply stateDiff for service %s epoch %d with exit code %d",
              serviceName, placementEpoch, exitCode);
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - %s",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), errMessage));
      throw new RuntimeException(errMessage);
    }

    return true;
  }

  @Override
  public boolean removeServiceRecorder(String serviceName, int placementEpoch) {
    assert serviceName != null : "serviceName should not be null";
    assert placementEpoch >= 0 : "placementEpoch should be non-negative";

    String targetDir = this.getTargetDirectory(serviceName, placementEpoch);
    int umountRetCode = Shell.runCommand("sudo umount " + targetDir + " > /dev/null 2>&1", false);
    if (umountRetCode != 0) {
      logger.log(
          Level.WARNING,
          String.format(
              "%s:%s - failed to unmount target directory %s with exit code %d",
              this.nodeID,
              FuselogStateDiffRecorder.class.getSimpleName(),
              targetDir,
              umountRetCode));
      return false;
    }

    int rmRetCode = Shell.runCommand("rm -rf " + targetDir, false);
    if (rmRetCode != 0) {
      logger.log(
          Level.WARNING,
          String.format(
              "%s:%s - failed to remove target directory %s with exit code %d",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), targetDir, rmRetCode));
      return false;
    }

    return true;
  }

  /**********************************************************************************************
   *                        Non-Deterministic Initialization Methods                            *
   *********************************************************************************************/

  @Override
  public void initContainerSync(
      String myNodeId,
      String serviceName,
      Map<String, InetAddress> ipAddresses,
      int placementEpoch,
      String sshKey) {

    // Obtain the backup nodes' IDs and their corresponding IP addresses.
    // The backup nodes are all the nodes except the current node.
    Set<String> backupNodes =
        ipAddresses.keySet().stream()
            .filter(node -> !node.equals(myNodeId.toString()))
            .map(String::toLowerCase)
            .collect(Collectors.toSet());
    logger.log(
        Level.INFO,
        String.format(
            "%s:%s - initializing container sync for service %s epoch %d with backup nodes %s (ip:"
                + " %s)",
            myNodeId,
            FuselogStateDiffRecorder.class.getSimpleName(),
            serviceName,
            placementEpoch,
            backupNodes,
            ipAddresses));

    // Prepare the backup replicas' target directory on each backup node, which is where we will
    // sync the data from current replica and also where fuselog-apply will apply the stateDiff.
    // e.g., backup-node1 => /tmp/xdn/state/fuselog/backup-node1/mnt/<serviceName>/e<epoch>/
    String currentReplica = this.baseDirectoryPath;
    Map<String, String> backupIdToTargetPaths = new HashMap<>();
    backupNodes.forEach(
        node ->
            backupIdToTargetPaths.put(
                node,
                String.format("%s%s/", FuselogStateDiffRecorder.defaultWorkingBasePath, node)));

    String mntDir = String.format("mnt/%s/", serviceName);
    String username = Shell.runCommandWithOutput("whoami").stdout.trim();

    // Copy data to other replicas
    Boolean allSyncSuccess = false;
    int count = 0;

    int numBackupReplicas = backupIdToTargetPaths.size();
    ExecutorService executor = Executors.newFixedThreadPool(numBackupReplicas);
    String sshOption =
        sshKey != null && !sshKey.trim().isEmpty() ? String.format("-e \"ssh -i %s\"", sshKey) : "";

    while (!allSyncSuccess) {
      if (++count > 10) {
        throw new RuntimeException("Failed running rsync after 10 iterations");
      }

      allSyncSuccess = true;
      List<Future<Boolean>> futures = new ArrayList<>();

      for (String key : backupIdToTargetPaths.keySet()) {
        final String replicaKey = key;
        final String targetReplica = backupIdToTargetPaths.get(replicaKey);
        futures.add(
            executor.submit(
                () ->
                    syncReplicaWithRsync(
                        currentReplica,
                        targetReplica,
                        ipAddresses.get(replicaKey).getHostAddress(),
                        mntDir,
                        username,
                        sshOption)));
      }

      for (Future<Boolean> future : futures) {
        try {
          if (!future.get()) {
            allSyncSuccess = false;
          }
        } catch (InterruptedException | ExecutionException e) {
          e.printStackTrace();
          allSyncSuccess = false;
        }
      }

      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      executor.shutdown();
    }

    Map<Integer, SocketChannel> epochToChannelMap = this.serviceFsSocket.get(serviceName);
    assert epochToChannelMap != null : "unknown fs socket client for " + serviceName;
    SocketChannel socketChannel = epochToChannelMap.get(placementEpoch);
    assert socketChannel != null : "unknown fs socket client for " + serviceName;

    // begin capturing statediff (c) in the filesystem
    try {
      System.out.println(">> clear stateDiffs...");
      socketChannel.write(ByteBuffer.wrap("c".getBytes()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean syncReplicaWithRsync(
      String currentReplica,
      String targetReplica,
      String hostAddr,
      String mntDir,
      String username,
      String sshOption) {
    int exitCode;
    if ("127.0.0.1".equals(hostAddr)) {
      exitCode =
          Shell.runCommand(
              String.format(
                  """
                          rsync -avz --delete --human-readable \
                          --include='mnt/' --include='%s' --include='%s***' \
                          --exclude='*' \
                          %s %s""",
                  mntDir, mntDir, currentReplica, targetReplica),
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
                  sshOption, mntDir, mntDir, currentReplica, username, hostAddr, targetReplica),
              true);
    }

    if (exitCode != 0) {
      System.out.println(String.format("Failed to sync %s to %s", currentReplica, targetReplica));
      return false;
    }

    return true;
  }
}
