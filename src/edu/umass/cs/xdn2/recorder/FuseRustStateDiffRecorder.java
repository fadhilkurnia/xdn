package edu.umass.cs.xdn2.recorder;

import edu.umass.cs.xdn2.utils.Shell;
import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
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

public class FuseRustStateDiffRecorder extends AbstractStateDiffRecorder {

  private static final String FUSERUST_BIN_PATH = "/usr/local/bin/fuserust";
  private static final String FUSERUST_APPLY_BIN_PATH = "/usr/local/bin/fuserust-apply";

  private static final String defaultWorkingBasePath = "/tmp/xdn/state/fuserust/";

  private final String baseMountDirPath;
  private final String baseSocketDirPath;
  private final String baseDiffDirPath;

  // important locations:
  // /tmp/xdn/state/fuserust/<node-id>/                                    the base directory
  // /tmp/xdn/state/fuserust/<node-id>/mnt/                                the mount directory
  // /tmp/xdn/state/fuserust/<node-id>/mnt/<service-name>/e<epoch>/        mount dir of specific
  // service
  // /tmp/xdn/state/fuserust/<node-id>/sock/                               the socket directory
  // /tmp/xdn/state/fuserust/<node-id>/sock/<service-name>::e<epoch>.sock  socket to fs of specific
  // service
  // /tmp/xdn/state/fuserust/<node-id>/diff/                               the stateDiff directory
  // /tmp/xdn/state/fuserust/<node-id>/diff/<service-name>::e<epoch>.diff  the stateDiff file

  // mapping service name and epoch into its fuserust filesystem socket
  public class SocketPair {
    private SocketChannel capture;
    private ServerSocketChannel applyChannel;

    public SocketPair() {
      this.capture = null;
      this.applyChannel = null;
    }

    public void setCaptureSocket(SocketChannel capture) {
      this.capture = capture;
    }

    public void setApplyChannel(ServerSocketChannel apply) {
      this.applyChannel = apply;
    }

    public SocketChannel getCaptureSocket() {
      return capture;
    }

    public ServerSocketChannel getApplyChannel() {
      return applyChannel;
    }
  }

  private final Map<String, Map<Integer, SocketPair>> serviceFsSocket;

  private final Logger logger = Logger.getLogger(FuseRustStateDiffRecorder.class.getSimpleName());

  public FuseRustStateDiffRecorder(String nodeID) {
    super(nodeID, defaultWorkingBasePath + nodeID + "/");
    logger.log(
        Level.FINE,
        String.format(
            "%s:%s - initializing FuseRust stateDiff recorder",
            nodeID, FuseRustStateDiffRecorder.class.getSimpleName()));

    // make sure that fuserust and fuserust-apply are exist
    File fuserust = new File(FUSERUST_BIN_PATH);
    if (!fuserust.exists()) {
      String errMessage = "fuserust binary does not exist at " + FUSERUST_BIN_PATH;
      System.out.println("ERROR: " + errMessage);
      throw new RuntimeException(errMessage);
    }
    File fuseRustApplicator = new File(FUSERUST_APPLY_BIN_PATH);
    if (!fuseRustApplicator.exists()) {
      String errMessage = "fuserust-apply binary does not exist at " + FUSERUST_APPLY_BIN_PATH;
      System.out.println("ERROR: " + errMessage);
      throw new RuntimeException(errMessage);
    }

    // create working mount dir, if not exist
    // e.g., /tmp/xdn/state/fuserust/node1/mnt/
    this.baseMountDirPath = this.baseDirectoryPath + "mnt/";
    try {
      Files.createDirectories(Paths.get(this.baseMountDirPath));
    } catch (IOException e) {
      System.err.println("ERROR: " + e);
      throw new RuntimeException(e);
    }

    // create socket dir, if not exist
    // e.g., /tmp/xdn/state/fuserust/node1/sock/
    this.baseSocketDirPath = defaultWorkingBasePath + nodeID + "/sock/";
    try {
      Files.createDirectories(Paths.get(this.baseSocketDirPath));
    } catch (IOException e) {
      System.err.println("ERROR: " + e);
      throw new RuntimeException(e);
    }

    // create diff dir, if not exist
    // e.g., /tmp/xdn/state/fuserust/node1/diff/
    this.baseDiffDirPath = defaultWorkingBasePath + nodeID + "/diff/";
    try {
      Files.createDirectories(Paths.get(this.baseDiffDirPath));
    } catch (IOException e) {
      System.err.println("ERROR: " + e);
      throw new RuntimeException(e);
    }

    // initialize mapping between serviceName to the FS socket
    this.serviceFsSocket = new ConcurrentHashMap<>();
  }

  @Override
  public boolean preInitialization(String serviceName, int placementEpoch) {
    if (this.serviceFsSocket.containsKey(serviceName)
        && this.serviceFsSocket.get(serviceName).containsKey((placementEpoch))) {

      logger.log(
          Level.WARNING,
          String.format(
              "%s:%s - socket data for %s:%d already exists. Skipping preInitialization.",
              this.nodeID,
              FuseRustStateDiffRecorder.class.getSimpleName(),
              serviceName,
              placementEpoch));

      return true;
    }

    // create target mnt dir, if not exist
    // e.g., /tmp/xdn/state/fuserust/node1/mnt/service1/
    String targetDirPath = this.getTargetDirectory(serviceName, placementEpoch, null);
    File targetDir = new File(targetDirPath);
    if (!targetDir.exists()) {
      logger.log(
          Level.FINEST,
          String.format(
              "%s:%s - Directory %s doesn't exist. Creating...",
              this.nodeID, FuseRustStateDiffRecorder.class.getSimpleName(), targetDirPath));

      String createDirCommand = String.format("mkdir -p %s", targetDirPath);
      int code = Shell.runCommand(createDirCommand);
      assert code == 0;
    } else {
      logger.log(
          Level.FINEST,
          String.format(
              "%s:%s - Directory %s exists. Unmounting fuserust (if exists) to allow initialization"
                  + " of new fuserust.",
              this.nodeID, FuseRustStateDiffRecorder.class.getSimpleName(), targetDirPath));
      Shell.runCommand("fusermount -u " + targetDirPath);
    }

    // Initialize apply stateDiff socket for fuserust-apply
    String applySocketFile =
        this.baseSocketDirPath + serviceName + "::" + placementEpoch + "::apply.sock";
    try {
      Files.deleteIfExists(Path.of(applySocketFile));
    } catch (IOException e) {
      throw new RuntimeException(
          String.format("Failed to delete old %s file: %s", applySocketFile, e));
    }

    UnixDomainSocketAddress applyAddress = UnixDomainSocketAddress.of(Path.of(applySocketFile));
    ServerSocketChannel applyServer;
    try {
      applyServer = ServerSocketChannel.open(StandardProtocolFamily.UNIX);
      applyServer.bind(applyAddress);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    this.serviceFsSocket.computeIfAbsent(serviceName, k -> new ConcurrentHashMap<>());
    SocketPair serviceSocket = new SocketPair();
    serviceSocket.setApplyChannel(applyServer);
    this.serviceFsSocket.get(serviceName).put(placementEpoch, serviceSocket);

    return true;
  }

  @Override
  public boolean postInitialization(String serviceName, int placementEpoch) {
    String targetDir = this.getTargetDirectory(serviceName, placementEpoch, null);
    String captureSocketFile = baseSocketDirPath + serviceName + "::" + placementEpoch + ".sock";

    // initialize file system in the mnt dir, with socket
    assert targetDir.length() > 1 : "invalid target mount directory";
    // remove the trailing '/' at the end of targetDir
    String targetDirPath = targetDir.substring(0, targetDir.length() - 1);
    String cmd = String.format("%s %s", FUSERUST_BIN_PATH, targetDirPath);

    Map<String, String> env = new HashMap<>();
    env.put("FUSELOG_SOCKET_FILE", captureSocketFile);
    env.put("ADAPTIVE_DEV_MODE", "false");
    env.put("FUSELOG_COMPRESSION", "true");
    env.put("FUSELOG_PRUNE", "true");
    env.put("ADAPTIVE_COMPRESSION", "false");
    env.put("WRITE_COALESCING", "true");
    env.put("RUST_LOG", "info");

    int exitCode = Shell.runCommand(cmd, true, env);
    assert exitCode == 0 : "failed to mount filesystem with exit code " + exitCode;

    // initialize capture stateDiff socket client for the filesystem
    SocketChannel captureChannel;
    UnixDomainSocketAddress captureAddress = UnixDomainSocketAddress.of(Path.of(captureSocketFile));
    try {
      captureChannel = SocketChannel.open(StandardProtocolFamily.UNIX);

      // Wait 0.5 seconds for fuselog to start
      try {
        Thread.sleep(500);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      boolean isConnEstablished = captureChannel.connect(captureAddress);
      if (!isConnEstablished) {
        System.err.println("failed to connect to the filesystem");
        return false;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // update the socket metadata
    this.serviceFsSocket.get(serviceName).get(placementEpoch).setCaptureSocket(captureChannel);

    // Run fuserust-apply in background
    String applySocketFile =
        this.baseSocketDirPath + serviceName + "::" + placementEpoch + "::apply.sock";
    String applyCmd =
        String.format(
            "%s %s --applySocket=%s", FUSERUST_APPLY_BIN_PATH, targetDir, applySocketFile);
    Shell.runCommandThread(applyCmd, false, null);
    return true;
  }

  @Override
  public byte[] captureStateDiff(String serviceName, int placementEpoch) {
    Map<Integer, SocketPair> epochToChannelMap = this.serviceFsSocket.get(serviceName);
    assert epochToChannelMap != null : "unknown fs socket client for " + serviceName;
    SocketChannel socketChannel = epochToChannelMap.get(placementEpoch).getCaptureSocket();
    assert socketChannel != null : "unknown fs socket client for " + serviceName;

    // send get command (g) to the filesystem
    try {
      logger.log(
          Level.FINEST,
          String.format(
              "%s:%s - sending FuselogFS command",
              this.nodeID, FuseRustStateDiffRecorder.class.getSimpleName()));
      socketChannel.write(ByteBuffer.wrap("g".getBytes()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // wait for response indicating the stateDiff size
    ByteBuffer sizeBuffer = ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN);
    try {
      while (sizeBuffer.hasRemaining()) {
        int bytesRead = socketChannel.read(sizeBuffer);
        if (bytesRead == -1) {
          throw new RuntimeException("Socket closed before all data could be read");
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    sizeBuffer.flip();
    long stateDiffSize = sizeBuffer.getLong();

    if (stateDiffSize <= 0) {
      System.err.println("No stateDiff to read.");
      return null;
    } else {
      logger.log(
          Level.FINEST,
          String.format(
              "%s:%s - stateDiff size = %d bytes (%.2f KB, %.2f MB)%n",
              this.nodeID,
              FuseRustStateDiffRecorder.class.getSimpleName(),
              stateDiffSize,
              stateDiffSize / 1024.0,
              stateDiffSize / (1024.0 * 1024)));
    }

    ByteBuffer stateDiffBuffer = ByteBuffer.allocate((int) stateDiffSize);
    try {
      while (stateDiffBuffer.hasRemaining()) {
        int bytesRead = socketChannel.read(stateDiffBuffer);
        if (bytesRead == -1) {
          throw new RuntimeException("Socket closed before all data could be read");
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    stateDiffBuffer.flip();

    byte[] resultBytes = new byte[stateDiffBuffer.remaining()];
    stateDiffBuffer.get(resultBytes);
    return resultBytes;
  }

  @Override
  public boolean applyStateDiff(String serviceName, int placementEpoch,
                                byte[] encodedState, String filename) {
    // TODO: use stateDiffCount for named diff files when these recorders are updated
    // TODO: directly apply stateDiff from the obtained byte[], not via
    //  the fuselog-apply program, which we currently use.

    logger.log(
        Level.FINER,
        String.format(
            "%s:%s - applying stateDiff name=%s epoch=%d size=%d bytes",
            this.nodeID,
            FuseRustStateDiffRecorder.class.getSimpleName(),
            serviceName,
            placementEpoch,
            encodedState.length));

    String applySocketFile =
        this.baseSocketDirPath + serviceName + "::" + placementEpoch + "::apply.sock";
    try (SocketChannel channel =
        SocketChannel.open(UnixDomainSocketAddress.of(Path.of(applySocketFile)))) {
      logger.log(
          Level.INFO,
          String.format(
              "%s:%s - connecting to %s",
              this.nodeID, FuseRustStateDiffRecorder.class.getSimpleName(), applySocketFile));

      ByteBuffer buffer = ByteBuffer.wrap(encodedState);
      while (buffer.hasRemaining()) {
        channel.write(buffer);
      }

      channel.shutdownOutput();

      ByteBuffer responseBuffer = ByteBuffer.allocate(1024);
      StringBuilder response = new StringBuilder();

      while (channel.read(responseBuffer) != -1) {
        responseBuffer.flip();
        response.append(StandardCharsets.UTF_8.decode(responseBuffer).toString());
        responseBuffer.clear();
      }

      logger.log(
          Level.INFO,
          String.format(
              "%s:%s - fuserust-apply responded with: %s",
              this.nodeID, FuseRustStateDiffRecorder.class.getSimpleName(), response.toString()));
    } catch (IOException e) {
      throw new RuntimeException("Fuserust error: " + e);
    }
    return true;
  }

  @Override
  public boolean removeServiceRecorder(String serviceName, int placementEpoch) {
    String targetDir = this.getTargetDirectory(serviceName, placementEpoch, null);
    int umountRetCode = Shell.runCommand("fusermount -u " + targetDir, false);
    int rmRetCode = Shell.runCommand("rm -rf " + targetDir, false);
    assert rmRetCode == 0;
    return true;
  }
}
