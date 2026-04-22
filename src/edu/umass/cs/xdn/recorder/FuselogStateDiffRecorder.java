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
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
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
    Shell.runCommand("sudo umount " + targetDir);
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
    env.put("FUSELOG_DAEMON_LOGS", "1");
    env.put("FUSELOG_COMPRESSION", "true");
    env.put("RUST_LOG", "info");
    // Coalescing reads old data before each write (two passes per write).
    // For heavy-write databases like MySQL, this can add overhead.
    // Disable with -DFUSELOG_DISABLE_COALESCING=true
    if (Boolean.parseBoolean(System.getProperty("FUSELOG_DISABLE_COALESCING", "false"))) {
      env.put("WRITE_COALESCING", "false");
    }
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

    // Clear the accumulated init state diff so that subsequent captureStateDiff calls
    // only capture incremental writes from user requests. Without this, the first
    // captureStateDiff after a service like MySQL initializes would try to transfer
    // all init writes (potentially hundreds of MB), blocking the PrimaryEpoch lock.
    Map<Integer, SocketChannel> epochToChannelMap = serviceFsSocket.get(serviceName);
    if (epochToChannelMap != null) {
      SocketChannel socketChannel = epochToChannelMap.get(placementEpoch);
      if (socketChannel != null) {
        drainStateDiff(socketChannel, serviceName, placementEpoch);
        logger.log(
            Level.INFO,
            String.format(
                "%s:%s - cleared init stateDiff for service=%s epoch=%d",
                this.nodeID,
                FuselogStateDiffRecorder.class.getSimpleName(),
                serviceName,
                placementEpoch));
      }
    }
    return true;
  }

  // Maximum plausible state diff for a single HTTP request (100 MB).
  // Values above this indicate protocol desynchronization (garbage size header).
  private static final long MAX_STATEDIFF_BYTES = 100L * 1024 * 1024;

  private static String toHexString(byte[] bytes) {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02X ", b));
    }
    return sb.toString().trim();
  }

  /**
   * Sends 'g' to fuselog and reads and discards the response, effectively clearing the accumulated
   * statediff log. Used during init to discard writes from service start-up.
   *
   * <p>The C++ fuselog protocol has no separate 'c' clear command; 'g' gets-and-clears atomically
   * (see send_gathered_statediffs in fuselogv2.cpp). Sending 'c' (or any unknown command) causes
   * the socket listener to exit, which is the root cause of ECONNREFUSED on all subsequent
   * captureStateDiff calls.
   */
  private void drainStateDiff(SocketChannel socketChannel, String serviceName, int placementEpoch) {
    try {
      socketChannel.write(ByteBuffer.wrap("g".getBytes()));
    } catch (IOException e) {
      logger.log(
          Level.WARNING,
          String.format(
              "%s:%s - failed to send drain command for service=%s epoch=%d: %s",
              this.nodeID,
              FuselogStateDiffRecorder.class.getSimpleName(),
              serviceName,
              placementEpoch,
              e.getMessage()));
      return;
    }

    // Read 8-byte little-endian size header.
    ByteBuffer sizeBuffer = ByteBuffer.allocate(8);
    sizeBuffer.order(ByteOrder.LITTLE_ENDIAN);
    try {
      int numRead = 0;
      while (numRead < 8) {
        int n = socketChannel.read(sizeBuffer);
        if (n < 0) {
          logger.log(
              Level.WARNING,
              String.format(
                  "%s:%s - socket closed reading drain size header for service=%s epoch=%d",
                  this.nodeID,
                  FuselogStateDiffRecorder.class.getSimpleName(),
                  serviceName,
                  placementEpoch));
          return;
        }
        numRead += n;
      }
    } catch (IOException e) {
      logger.log(
          Level.WARNING,
          String.format(
              "%s:%s - failed to read drain size header for service=%s epoch=%d: %s",
              this.nodeID,
              FuselogStateDiffRecorder.class.getSimpleName(),
              serviceName,
              placementEpoch,
              e.getMessage()));
      return;
    }

    long size = sizeBuffer.getLong(0);
    logger.log(
        Level.INFO,
        String.format(
            "%s:%s - draining %d bytes of init statediff for service=%s epoch=%d",
            this.nodeID,
            FuselogStateDiffRecorder.class.getSimpleName(),
            size,
            serviceName,
            placementEpoch));
    if (size <= 0) {
      return;
    }
    // Allow up to 1 GB — MySQL init can produce hundreds of MB.
    if (size > 1024L * 1024 * 1024) {
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - implausibly large drain size=%d for service=%s epoch=%d, aborting drain",
              this.nodeID,
              FuselogStateDiffRecorder.class.getSimpleName(),
              size,
              serviceName,
              placementEpoch));
      return;
    }

    // Read and discard payload in 64 KB chunks.
    ByteBuffer chunk = ByteBuffer.allocate(65536);
    long remaining = size;
    try {
      while (remaining > 0) {
        chunk.clear();
        if (remaining < chunk.capacity()) {
          chunk.limit((int) remaining);
        }
        int n = socketChannel.read(chunk);
        if (n < 0) {
          logger.log(
              Level.WARNING,
              String.format(
                  "%s:%s - socket closed while draining statediff for service=%s epoch=%d"
                      + " (%d/%d bytes drained)",
                  this.nodeID,
                  FuselogStateDiffRecorder.class.getSimpleName(),
                  serviceName,
                  placementEpoch,
                  size - remaining,
                  size));
          return;
        }
        remaining -= n;
      }
    } catch (IOException e) {
      logger.log(
          Level.WARNING,
          String.format(
              "%s:%s - error draining statediff for service=%s epoch=%d: %s",
              this.nodeID,
              FuselogStateDiffRecorder.class.getSimpleName(),
              serviceName,
              placementEpoch,
              e.getMessage()));
    }
  }

  /**
   * Closes the current socket channel for the given service/epoch and reconnects to the fuselog
   * Unix domain socket. This restores protocol synchronization when the socket stream has become
   * desynchronized (e.g., after a read timeout that left fuselog stuck in write_all).
   *
   * @return the new SocketChannel on success, or null on failure.
   */
  private SocketChannel reconnectSocket(String serviceName, int placementEpoch) {
    String socketFile = baseSocketDirPath + serviceName + "::" + placementEpoch + ".sock";
    Map<Integer, SocketChannel> epochToChannelMap = serviceFsSocket.get(serviceName);

    // Close the old channel — this sends EOF to fuselog, causing its handle_client loop to exit.
    if (epochToChannelMap != null) {
      SocketChannel old = epochToChannelMap.get(placementEpoch);
      if (old != null) {
        try {
          old.close();
        } catch (IOException ignored) {
        }
        epochToChannelMap.remove(placementEpoch);
      }
    }

    // Fuselog's start_listener polls accept() with a 100 ms sleep; give it time to cycle.
    try {
      Thread.sleep(300);
    } catch (InterruptedException ignored) {
      Thread.currentThread().interrupt();
    }

    // Retry connecting up to 10 times (2 seconds total).
    for (int attempt = 1; attempt <= 10; attempt++) {
      try {
        SocketChannel newChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
        UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(socketFile));
        boolean connected = newChannel.connect(address);
        if (connected) {
          if (epochToChannelMap != null) {
            epochToChannelMap.put(placementEpoch, newChannel);
          }
          logger.log(
              Level.INFO,
              String.format(
                  "%s:%s - reconnected to fuselog socket for service=%s epoch=%d (attempt %d)",
                  this.nodeID,
                  FuselogStateDiffRecorder.class.getSimpleName(),
                  serviceName,
                  placementEpoch,
                  attempt));
          return newChannel;
        }
        newChannel.close();
      } catch (IOException e) {
        logger.log(
            Level.WARNING,
            String.format(
                "%s:%s - reconnect attempt %d failed for service=%s epoch=%d: %s",
                this.nodeID,
                FuselogStateDiffRecorder.class.getSimpleName(),
                attempt,
                serviceName,
                placementEpoch,
                e.getMessage()));
      }
      try {
        Thread.sleep(200);
      } catch (InterruptedException ignored) {
        Thread.currentThread().interrupt();
      }
    }

    logger.log(
        Level.SEVERE,
        String.format(
            "%s:%s - failed to reconnect to fuselog socket for service=%s epoch=%d after 10"
                + " attempts",
            this.nodeID,
            FuselogStateDiffRecorder.class.getSimpleName(),
            serviceName,
            placementEpoch));
    return null;
  }

  @Override
  public byte[] captureStateDiff(String serviceName, int placementEpoch) {
    assert serviceName != null : "serviceName should not be null";
    assert placementEpoch >= 0 : "placementEpoch should be non-negative";

    Map<Integer, SocketChannel> epochToChannelMap = serviceFsSocket.get(serviceName);
    if (epochToChannelMap == null) {
      logger.log(
          Level.WARNING,
          String.format(
              "%s:%s - no socket map for service=%s, returning null",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), serviceName));
      return null;
    }
    SocketChannel socketChannel = epochToChannelMap.get(placementEpoch);
    if (socketChannel == null) {
      logger.log(
          Level.WARNING,
          String.format(
              "%s:%s - no socket channel for service=%s epoch=%d, returning null",
              this.nodeID,
              FuselogStateDiffRecorder.class.getSimpleName(),
              serviceName,
              placementEpoch));
      return null;
    }

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
    int numRead = 0;
    try {
      int n;
      while (numRead < 8) {
        n = socketChannel.read(sizeBuffer);
        if (n < 0) {
          logger.log(
              Level.SEVERE,
              String.format(
                  "%s:%s - filesystem socket closed while reading size header after %d bytes;"
                      + " reconnecting",
                  this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), numRead));
          reconnectSocket(serviceName, placementEpoch);
          return null;
        }
        numRead += n;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    long stateDiffSize = sizeBuffer.getLong(0);
    logger.log(
        Level.FINE,
        String.format(
            "%s:%s - receiving stateDiff with size=%d bytes",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), stateDiffSize));

    // Sanity-check the size. If it is negative or implausibly large, the socket is
    // desynchronized (leftover payload bytes from a previous incomplete read are being
    // misinterpreted as a size header).  Close and reconnect to restore the protocol.
    if (stateDiffSize < 0 || stateDiffSize > MAX_STATEDIFF_BYTES) {
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - garbage stateDiffSize=%d (raw LE bytes=[%s]); reconnecting socket",
              this.nodeID,
              FuselogStateDiffRecorder.class.getSimpleName(),
              stateDiffSize,
              toHexString(sizeBuffer.array())));
      reconnectSocket(serviceName, placementEpoch);
      return null;
    }

    // Read all the stateDiff based on the obtained size.
    // For small diffs (common case), use fast blocking reads on the Unix domain socket
    // to avoid the overhead of Selector.open() + configureBlocking per call.
    // For large diffs (>1MB), use a Selector-based read with timeout to detect fuselog stalls.
    final long LARGE_DIFF_THRESHOLD = 1024 * 1024; // 1 MB
    final long PAYLOAD_READ_TIMEOUT_MS = 5000;
    ByteBuffer stateDiffBuffer = ByteBuffer.allocate((int) stateDiffSize);
    numRead = 0;
    if (stateDiffSize <= LARGE_DIFF_THRESHOLD) {
      // Fast path: blocking reads — no Selector overhead.
      try {
        while (numRead < stateDiffSize) {
          int n = socketChannel.read(stateDiffBuffer);
          if (n < 0) {
            logger.log(
                Level.SEVERE,
                String.format(
                    "%s:%s - socket closed after reading %d/%d bytes",
                    this.nodeID,
                    FuselogStateDiffRecorder.class.getSimpleName(),
                    numRead,
                    stateDiffSize));
            return null;
          }
          numRead += n;
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } else {
      // Large diff path: Selector with timeout to detect fuselog stalls.
      try {
        socketChannel.configureBlocking(false);
        try (Selector selector = Selector.open()) {
          SelectionKey key = socketChannel.register(selector, SelectionKey.OP_READ);
          while (numRead < stateDiffSize) {
            int ready = selector.select(PAYLOAD_READ_TIMEOUT_MS);
            if (ready == 0) {
              logger.log(
                  Level.SEVERE,
                  String.format(
                      "%s:%s - timeout after %dms reading payload; received %d/%d bytes."
                          + " Reconnecting socket to restore protocol sync"
                          + " (fuselog may be stuck in write_all).",
                      this.nodeID,
                      FuselogStateDiffRecorder.class.getSimpleName(),
                      PAYLOAD_READ_TIMEOUT_MS,
                      numRead,
                      stateDiffSize));
              key.cancel();
              reconnectSocket(serviceName, placementEpoch);
              return null;
            }
            selector.selectedKeys().clear();
            int n = socketChannel.read(stateDiffBuffer);
            if (n < 0) {
              logger.log(
                  Level.SEVERE,
                  String.format(
                      "%s:%s - socket closed after reading %d/%d bytes",
                      this.nodeID,
                      FuselogStateDiffRecorder.class.getSimpleName(),
                      numRead,
                      stateDiffSize));
              key.cancel();
              return null;
            }
            numRead += n;
          }
          key.cancel();
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      } finally {
        try {
          socketChannel.configureBlocking(true);
        } catch (IOException e) {
          logger.log(
              Level.WARNING,
              String.format(
                  "%s:%s - failed to restore blocking mode: %s",
                  this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), e.getMessage()));
        }
      }
    }

    byte[] stateDiff = stateDiffBuffer.array();

    long endTime = System.nanoTime();
    long elapsedTime = endTime - startTime;
    double elapsedTimeMs = (double) elapsedTime / 1_000_000.0;
    logger.log(
        Level.INFO,
        String.format(
            "%s:%s - capturing stateDiff within %f ms, size=%d bytes",
            this.nodeID,
            FuselogStateDiffRecorder.class.getSimpleName(),
            elapsedTimeMs,
            stateDiff.length));

    return stateDiff;
  }

  @Override
  public boolean applyStateDiff(String serviceName, int placementEpoch, byte[] encodedState) {
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
            encodedState.length));

    String diffFile = this.baseDiffDirPath + serviceName + "::" + placementEpoch + ".diff";
    String targetDir = this.getTargetDirectory(serviceName, placementEpoch);

    // Store stateDiff into an external .diff file.
    try {
      FileOutputStream outputStream;
      outputStream = new FileOutputStream(diffFile);
      outputStream.write(encodedState);
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
    int umountRetCode = Shell.runCommand("sudo umount " + targetDir, true);
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
                node, String.format("%s%s/", FuselogStateDiffRecorder.workingBasePath, node)));

    String mntDir = String.format("mnt/%s/", serviceName);
    String username = Shell.runCommandWithOutput("whoami").stdout.trim();

    // Copy data to other replicas
    Boolean allSyncSuccess = false;
    int count = 0;

    int numBackupReplicas = backupIdToTargetPaths.size();
    ExecutorService executor = Executors.newFixedThreadPool(numBackupReplicas);

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
                        sshKey)));
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
    }

    executor.shutdown();

    Map<Integer, SocketChannel> epochToChannelMap = this.serviceFsSocket.get(serviceName);
    if (epochToChannelMap == null) {
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - no socket map for service=%s in initContainerSync",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), serviceName));
      return;
    }
    SocketChannel socketChannel = epochToChannelMap.get(placementEpoch);
    if (socketChannel == null) {
      logger.log(
          Level.SEVERE,
          String.format(
              "%s:%s - no socket channel for service=%s epoch=%d in initContainerSync",
              this.nodeID,
              FuselogStateDiffRecorder.class.getSimpleName(),
              serviceName,
              placementEpoch));
      return;
    }

    // Drain any statediff accumulated during rsync by fetching and discarding it.
    // The C++ fuselog 'g' command gets-and-clears atomically; there is no separate 'c' command.
    logger.log(
        Level.INFO,
        String.format(
            "%s:%s - clearing stateDiff log after rsync for service=%s epoch=%d",
            this.nodeID,
            FuselogStateDiffRecorder.class.getSimpleName(),
            serviceName,
            placementEpoch));
    drainStateDiff(socketChannel, serviceName, placementEpoch);
  }

  private boolean syncReplicaWithRsync(
      String currentReplica,
      String targetReplica,
      String hostAddr,
      String mntDir,
      String username,
      String sshKey) {
    int exitCode;
    // Use List<String> to pass args directly to ProcessBuilder without shell interpretation.
    // This avoids quoting issues and lets us include multi-word SSH options correctly.
    if ("127.0.0.1".equals(hostAddr)) {
      exitCode =
          Shell.runCommand(
              List.of(
                  "rsync",
                  "-avz",
                  "--delete",
                  "--human-readable",
                  "--omit-dir-times",
                  "--omit-link-times",
                  "--include=mnt/",
                  "--include=" + mntDir,
                  "--include=" + mntDir + "***",
                  "--exclude=*",
                  currentReplica,
                  targetReplica),
              true);
    } else {
      // Build SSH rsh command. StrictHostKeyChecking=no is required because the JVM runs as root
      // and root may not have remote hosts in its known_hosts file.
      String sshCmd =
          sshKey != null && !sshKey.trim().isEmpty()
              ? "ssh -i " + sshKey + " -o StrictHostKeyChecking=no"
              : "ssh -o StrictHostKeyChecking=no";
      exitCode =
          Shell.runCommand(
              List.of(
                  "rsync",
                  "-avz",
                  "--delete",
                  "--human-readable",
                  "--omit-dir-times",
                  "--omit-link-times",
                  "-e",
                  sshCmd,
                  "--include=mnt/",
                  "--include=" + mntDir,
                  "--include=" + mntDir + "***",
                  "--exclude=*",
                  currentReplica,
                  username + "@" + hostAddr + ":" + targetReplica),
              true);
    }

    if (exitCode != 0) {
      System.out.println(String.format("Failed to sync %s to %s", currentReplica, targetReplica));
      return false;
    }

    return true;
  }
}
