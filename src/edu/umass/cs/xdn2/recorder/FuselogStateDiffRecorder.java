package edu.umass.cs.xdn2.recorder;

import edu.umass.cs.xdn2.utils.Shell;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.StandardProtocolFamily;
import java.net.UnixDomainSocketAddress;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class FuselogStateDiffRecorder extends AbstractStateDiffRecorder {
  private static final String FUSELOG_BIN_PATH = "/usr/local/bin/fuselog";
  private static final String FUSELOG_APPLY_BIN_PATH = "/usr/local/bin/fuselog-apply";

  // /tmp/xdn/state/fuselog/<node-id>/<service-name>/e<epoch>/snp/
  // /tmp/xdn/state/fuselog/<node-id>/<service-name>/e<epoch>/cmtDiff/
  // /tmp/xdn/state/fuselog/<node-id>/<service-name>/e<epoch>/primary/
  // /tmp/xdn/state/fuselog/<node-id>/<service-name>/e<epoch>/backup1/
  // /tmp/xdn/state/fuselog/<node-id>/<service-name>/e<epoch>/backup2/
  // /tmp/xdn/state/fuselog/<node-id>/<service-name>/e<epoch>/fuselog.sock

  // mapping service name and epoch into its fuselog filesystem socket
  private final Map<String, Map<Integer, SocketChannel>> serviceFsSocket;

  private final Logger logger = Logger.getLogger(FuselogStateDiffRecorder.class.getSimpleName());

  public FuselogStateDiffRecorder(String nodeID, String basePath) {
    super(nodeID, basePath);
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

    // Initialize mapping between serviceName to the FS socket so we can send commands to the
    // filesystem to capture stateDiff later.
    this.serviceFsSocket = new ConcurrentHashMap<>();
  }

  @Override
  public boolean preInitialization(String serviceName, int placementEpoch) {
    // preInitialization is only called on the primary — mounts fuselog on primaryLive/.
    // snapshot/ is seeded by the caller (NonDeterministicService) via rsync before this runs.
    String primaryLiveDir = getTargetDirectory(serviceName, placementEpoch, LiveDirType.PRIMARY);
    String socketFile = getSocketPath(serviceName, placementEpoch);

    // Unmount any stale FUSE mount and recreate the primaryLive/ directory.
    Shell.runCommand("sudo umount " + primaryLiveDir);
    Shell.runCommand("rm -rf " + primaryLiveDir);
    int code = Shell.runCommand("mkdir -p " + primaryLiveDir);
    if (code != 0) {
      String errMessage = String.format(
              "failed to create %s directory with exit code %d",
              primaryLiveDir, code);
      logger.log(Level.SEVERE, String.format("%s:%s - %s",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), errMessage));
      throw new RuntimeException(errMessage);
    }

    // Mount fuselog on primaryLive/ — all writes are captured through the FUSE layer.
    assert primaryLiveDir.length() > 1 : "invalid primaryLive directory";
    assert primaryLiveDir.endsWith("/") : "primaryLive directory should end with '/'";
    String primaryLiveDirNoSlash = primaryLiveDir.substring(0, primaryLiveDir.length() - 1);
    String cmd = String.format("%s -o allow_other %s", FUSELOG_BIN_PATH, primaryLiveDirNoSlash);

    Map<String, String> env = new HashMap<>();
    env.put("FUSELOG_SOCKET_FILE", socketFile);
    if (Boolean.parseBoolean(System.getProperty("FUSELOG_DISABLE_COALESCING", "false"))) {
      env.put("WRITE_COALESCING", "false");
    }

    int exitCode = Shell.runCommand(cmd, false, env);
    if (exitCode != 0) {
      String errMessage = String.format(
              "failed to mount fuselog at %s with exit code %d", primaryLiveDir, exitCode);
      logger.log(Level.SEVERE, String.format("%s:%s - %s",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(), errMessage));
      throw new RuntimeException(errMessage);
    }

    // Connect to fuselog's Unix socket (retry since fuselog daemonizes asynchronously).
    UnixDomainSocketAddress address = UnixDomainSocketAddress.of(Path.of(socketFile));
    SocketChannel socketChannel = null;
    final int maxAttempts = 100;
    IOException lastError = null;
    for (int attempt = 1; attempt <= maxAttempts; attempt++) {
      try {
        socketChannel = SocketChannel.open(StandardProtocolFamily.UNIX);
        if (socketChannel.connect(address)) {
          lastError = null;
          break;
        }
      } catch (IOException e) {
        lastError = e;
        try { socketChannel.close(); } catch (IOException ignored) {}
        socketChannel = null;
      }
      try { Thread.sleep(100); } catch (InterruptedException ie) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(ie);
      }
    }
    if (socketChannel == null) {
      throw new RuntimeException(String.format(
              "%s:%s - failed to connect to fuselog socket at %s after %d attempts",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(),
              socketFile, maxAttempts), lastError);
    }

    if (!serviceFsSocket.containsKey(serviceName)) {
      serviceFsSocket.put(serviceName, new ConcurrentHashMap<>());
    }
    serviceFsSocket.get(serviceName).put(placementEpoch, socketChannel);

    logger.log(Level.INFO, String.format(
            "%s:%s - fuselog mounted on primaryLive/ for %s epoch %d",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(),
            serviceName, placementEpoch));
    return true;
  }

  @Override
  public boolean postInitialization(String serviceName, int placementEpoch) {
    // No-op: fuselog-apply is now invoked per-diff in applyStateDiff(),
    // not as a persistent daemon. No drain or setup needed here.
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
    String socketFile = getSocketPath(serviceName, placementEpoch);
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
  public boolean applyStateDiff(String serviceName, int placementEpoch,
                                byte[] encodedState, String filename) {
    assert serviceName != null : "serviceName should not be null";
    assert placementEpoch >= 0 : "placementEpoch should be non-negative";
    assert encodedState != null : "encoded stateDiff should not be null";

    String diffFile = getStateDiffDir(serviceName, placementEpoch) + filename;
    String snapshotDir = getSnapshotDir(serviceName, placementEpoch);

    logger.log(Level.INFO, String.format(
            "%s:%s - applying stateDiff service=%s filename=%s size=%d bytes",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(),
            serviceName, filename, encodedState.length));

    // Write diff bytes to stateDiff/<count>.diff
    try (FileOutputStream outputStream = new FileOutputStream(diffFile)) {
      outputStream.write(encodedState);
      outputStream.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    // Apply diff to snapshot/ synchronously via fuselog-apply
    // Apply diff to snapshot/ synchronously via fuselog-apply
    // Remove --silent temporarily for diagnosis
    String cmd = String.format("%s %s --statediff=%s",
            FUSELOG_APPLY_BIN_PATH, snapshotDir, diffFile);
    logger.log(Level.INFO, String.format(
            "%s:%s - running: %s (snapshotDir exists=%b, diffFile exists=%b, diffFile size=%d)",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(),
            cmd,
            new File(snapshotDir).exists(),
            new File(diffFile).exists(),
            new File(diffFile).length()));

    int exitCode = Shell.runCommand(cmd, true);

    logger.log(Level.WARNING, String.format(
            "%s:%s - fuselog-apply exit=%d for service=%s filename=%s",
            this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(),
            exitCode, serviceName, filename));

    if (exitCode != 0) {
      logger.log(Level.SEVERE, String.format(
              "%s:%s - failed to apply stateDiff for service=%s filename=%s",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(),
              serviceName, filename));
      return false;
    }

    return true;
  }

  @Override
  public boolean removeServiceRecorder(String serviceName, int placementEpoch) {
    assert serviceName != null : "serviceName should not be null";
    assert placementEpoch >= 0 : "placementEpoch should be non-negative";

    // Unmount primaryLive/ (FUSE mount) — close socket first
    Map<Integer, SocketChannel> epochMap = serviceFsSocket.get(serviceName);
    if (epochMap != null) {
      SocketChannel sc = epochMap.remove(placementEpoch);
      if (sc != null) {
        try { sc.close(); } catch (IOException ignored) {}
      }
      if (epochMap.isEmpty()) serviceFsSocket.remove(serviceName);
    }

    String primaryLiveDir = getTargetDirectory(serviceName, placementEpoch, LiveDirType.PRIMARY);
    int umountCode = Shell.runCommand("sudo umount " + primaryLiveDir, true);
    if (umountCode != 0) {
      logger.log(Level.WARNING, String.format(
              "%s:%s - failed to unmount primaryLive/ for %s epoch %d exit=%d",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(),
              serviceName, placementEpoch, umountCode));
    }

    // Delete the entire service/epoch directory tree
    String serviceEpochDir = getServiceBaseDir(serviceName, placementEpoch);
    int rmCode = Shell.runCommand("rm -rf " + serviceEpochDir, false);
    if (rmCode != 0) {
      logger.log(Level.WARNING, String.format(
              "%s:%s - failed to remove service epoch dir %s exit=%d",
              this.nodeID, FuselogStateDiffRecorder.class.getSimpleName(),
              serviceEpochDir, rmCode));
      return false;
    }

    return true;
  }

  // =========================================================================
  // Helpers
  // =========================================================================

  private String getSocketPath(String serviceName, int epoch) {
    return getServiceBaseDir(serviceName, epoch) + "fuselog.sock";
  }
}
