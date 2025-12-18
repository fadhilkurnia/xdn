package edu.umass.cs.xdn.eval;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A tiny Netty HTTP front-end on port 2300 that batches incoming requests before forwarding them to
 * a downstream HTTP service. After each flushed batch it triggers a docker checkpoint for the
 * provided container name. The goal is to provide a simple, reproducible baseline for CRIU
 * evaluations without pulling in the full XDN stack.
 *
 * <p>Usage: java -Djdk.httpclient.allowRestrictedHeaders=content-length \ -cp
 * jars/gigapaxos-1.0.10.jar:jars/gigapaxos-nio-src.jar:jars/nio-1.2.1.jar \
 * edu.umass.cs.xdn.eval.BaselineCriuReplica <targetPort> <containerName>
 *
 * <p>Notes: - Ensure that the target container is running with CRIU enabled and tcp-established
 * flag. - The replica listens on port 2300 by default and forwards to targetPort. - Ensure to
 * enable the experiment feature for docker. - We tested the CRIU-based checkpoint in r6615 and
 * xl170 machines in Cloudlab, we found error due to CRIU and CPU mismatch in c6620 machines with
 * "Can't set FPU registers" error.
 */
public class BaselineCriuReplica implements AutoCloseable {

  private static final Logger LOG = Logger.getLogger(BaselineCriuReplica.class.getName());

  private static final int MAX_CONTENT_LENGTH = 16 * 1024 * 1024;
  private static final int DEFAULT_BATCH_SIZE = Integer.getInteger("criu.batch.size", 8);
  private static final long BATCH_FLUSH_MS = Long.getLong("criu.batch.flush.ms", 25L);
  private static final int QUEUE_CAPACITY = Integer.getInteger("criu.queue.capacity", 1024);
  private static final int DEFAULT_LISTEN_PORT = Integer.getInteger("criu.listen.port", 2300);
  private static final boolean IS_SYNC_CHECKPOINT =
      Boolean.parseBoolean(System.getProperty("criu.checkpoint.sync", "true"));
  private static final List<String> DEFAULT_REPLICA_ADDRESSES =
      List.of("10.10.1.1", "10.10.1.2", "10.10.1.3");
  private static final String DEFAULT_SSH_KEY_PATH =
      System.getProperty("criu.rsync.ssh.key", "/users/fadhil/.ssh/cloudlab2");
  private static final String DEFAULT_SSH_USER =
      System.getProperty("criu.rsync.ssh.user", System.getProperty("user.name", ""));

  private final int listenPort;
  private final int forwardPort;
  private final String forwardHost;
  private final String containerName;
  private final List<String> replicaAddresses;
  private final BlockingQueue<PendingRequest> queue;
  private final ExecutorService batchExecutor;
  private final ExecutorService forwardExecutor;
  private final ExecutorService syncExecutor;
  private final AtomicBoolean running = new AtomicBoolean(false);
  private final HttpClient httpClient;
  private final String sshKeyPath = DEFAULT_SSH_KEY_PATH;
  private final String sshUser = DEFAULT_SSH_USER;

  private EventLoopGroup bossGroup;
  private EventLoopGroup workerGroup;
  private Channel serverChannel;

  public BaselineCriuReplica(int port, String containerName) {
    this(port, containerName, DEFAULT_REPLICA_ADDRESSES);
  }

  public BaselineCriuReplica(int port, String containerName, List<String> replicaAddresses) {
    this(
        DEFAULT_LISTEN_PORT,
        containerName,
        Integer.getInteger("criu.forward.port", port),
        System.getProperty("criu.forward.host", "127.0.0.1"),
        replicaAddresses);
  }

  public BaselineCriuReplica(
      int listenPort, String containerName, int forwardPort, String forwardHost) {
    this(listenPort, containerName, forwardPort, forwardHost, DEFAULT_REPLICA_ADDRESSES);
  }

  public BaselineCriuReplica(
      int listenPort,
      String containerName,
      int forwardPort,
      String forwardHost,
      List<String> replicaAddresses) {
    this.listenPort = listenPort;
    this.forwardPort = forwardPort;
    this.forwardHost = Objects.requireNonNull(forwardHost, "forwardHost");
    this.containerName = Objects.requireNonNull(containerName, "containerName");
    this.replicaAddresses = normalizeReplicaAddresses(replicaAddresses);
    this.queue = new ArrayBlockingQueue<>(QUEUE_CAPACITY);
    this.batchExecutor =
        Executors.newSingleThreadExecutor(
            r -> {
              Thread t = new Thread(r, "baseline-criu-worker");
              t.setDaemon(true);
              return t;
            });
    this.forwardExecutor =
        Executors.newFixedThreadPool(
            Math.max(2, DEFAULT_BATCH_SIZE),
            r -> {
              Thread t = new Thread(r, "baseline-criu-forwarder");
              t.setDaemon(true);
              return t;
            });
    this.syncExecutor =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "baseline-criu-rsync");
              t.setDaemon(true);
              return t;
            });
    this.httpClient =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5))
            .version(HttpClient.Version.HTTP_1_1)
            .build();
  }

  public static void main(String[] args) throws Exception {
    int port = 8080;
    String container;
    List<String> replicaAddresses = DEFAULT_REPLICA_ADDRESSES;

    if (args.length == 0) {
      System.err.println(
          "Usage: BaselineCriuReplica [port=8080] <containerName> [replica1 replica2 ...]");
      return;
    } else if (args.length == 1) {
      container = args[0];
    } else {
      port = Integer.parseInt(args[0]);
      container = args[1];
      if (args.length > 2) {
        replicaAddresses = new ArrayList<>(args.length - 2);
        for (int i = 2; i < args.length; i++) {
          replicaAddresses.add(args[i]);
        }
      }
    }

    if (!isLinuxWithDockerAndCriu()) {
      System.err.println("BaselineCriuReplica requires Linux with Docker and CRIU available");
      System.exit(1);
    }

    BaselineCriuReplica replica = new BaselineCriuReplica(port, container, replicaAddresses);
    Runtime.getRuntime().addShutdownHook(new Thread(replica::close));

    replica.start();
    replica.blockUntilShutdown();
  }

  private static boolean isLinuxWithDockerAndCriu() {
    String os = System.getProperty("os.name", "").toLowerCase();
    if (!os.contains("linux")) {
      return false;
    }
    if (!commandAvailable("docker", "--version")) {
      return false;
    }
    return commandAvailable("criu", "--version");
  }

  private static boolean commandAvailable(String... cmd) {
    try {
      Process proc = new ProcessBuilder(cmd).redirectErrorStream(true).start();
      if (!proc.waitFor(2, TimeUnit.SECONDS)) {
        proc.destroyForcibly();
        return false;
      }
      return proc.exitValue() == 0;
    } catch (IOException e) {
      return false;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      return false;
    }
  }

  private static List<String> normalizeReplicaAddresses(List<String> replicaAddresses) {
    if (replicaAddresses == null || replicaAddresses.isEmpty()) {
      return DEFAULT_REPLICA_ADDRESSES;
    }
    List<String> filtered = new ArrayList<>(replicaAddresses.size());
    for (String addr : replicaAddresses) {
      if (addr != null && !addr.isEmpty()) {
        filtered.add(addr);
      }
    }
    return filtered.isEmpty() ? DEFAULT_REPLICA_ADDRESSES : List.copyOf(filtered);
  }

  /** Starts Netty server and worker thread. */
  public void start() throws InterruptedException {
    if (!running.compareAndSet(false, true)) {
      return;
    }

    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup();

    ServerBootstrap bootstrap =
        new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childOption(ChannelOption.SO_KEEPALIVE, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childHandler(
                new ChannelInitializer<SocketChannel>() {
                  @Override
                  protected void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(new HttpServerCodec());
                    pipeline.addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH));
                    pipeline.addLast(new InboundHandler());
                  }
                });

    serverChannel = bootstrap.bind(listenPort).sync().channel();
    LOG.info(
        () ->
            "BaselineCriuReplica listening on port "
                + listenPort
                + " forwarding to "
                + forwardHost
                + ":"
                + forwardPort
                + " checkpointing container="
                + containerName);

    batchExecutor.submit(this::drainQueue);
  }

  public void blockUntilShutdown() throws InterruptedException {
    Channel channel = serverChannel;
    if (channel != null) {
      channel.closeFuture().sync();
    }
  }

  /** Worker loop that coalesces requests into a batch before forwarding. */
  private void drainQueue() {
    List<PendingRequest> batch = new ArrayList<>(DEFAULT_BATCH_SIZE);
    while (running.get()) {
      try {
        PendingRequest first = queue.poll(BATCH_FLUSH_MS, TimeUnit.MILLISECONDS);
        if (first == null) {
          continue;
        }
        batch.add(first);
        queue.drainTo(batch, DEFAULT_BATCH_SIZE - batch.size());

        forwardBatch(batch);
        batch.clear();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        LOG.log(Level.SEVERE, "Unexpected error while handling batch", e);
      }
    }

    // best-effort clean-up for any request still waiting in the queue
    queue.forEach(this::failFast);
    queue.clear();
  }

  /** Forwards batch synchronously then checkpoints the container. */
  private synchronized void forwardBatch(List<PendingRequest> batch) {
    List<CompletableFuture<Void>> futures = new ArrayList<>(batch.size());
    for (PendingRequest pending : batch) {
      futures.add(
          CompletableFuture.runAsync(
              () -> {
                try {
                  FullHttpResponse response = forward(pending.request);
                  HttpUtil.setKeepAlive(response, pending.keepAlive);
                  ChannelFuture writeFuture = pending.ctx.writeAndFlush(response);
                  if (!pending.keepAlive) {
                    writeFuture.addListener(ChannelFutureListener.CLOSE);
                  }
                } catch (Exception e) {
                  LOG.log(Level.WARNING, "Forwarding failed, sending 502 to client", e);
                  sendError(
                      pending.ctx,
                      HttpResponseStatus.BAD_GATEWAY,
                      "Forwarding failed: " + e.getMessage(),
                      pending.keepAlive);
                } finally {
                  ReferenceCountUtil.release(pending.request);
                }
              },
              forwardExecutor));
    }

    for (CompletableFuture<Void> future : futures) {
      try {
        future.join();
      } catch (CompletionException e) {
        // Exceptions are handled within the forwarding task.
      }
    }

    checkpointContainer();
  }

  /**
   * Perform a blocking forward to the configured upstream. If the upstream is the same host/port as
   * the listener, we avoid infinite loops by returning a simple echo response.
   */
  private FullHttpResponse forward(FullHttpRequest request)
      throws IOException, InterruptedException {
    if (isLoopbackForward()) {
      String payload = "Processed locally without forwarding";
      ByteBuf content = Unpooled.copiedBuffer(payload, StandardCharsets.UTF_8);
      DefaultFullHttpResponse response =
          new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, content);
      HttpUtil.setContentLength(response, content.readableBytes());
      response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
      return response;
    }

    URI uri = URI.create(String.format("http://%s:%d%s", forwardHost, forwardPort, request.uri()));
    HttpRequest.BodyPublisher bodyPublisher =
        request.content().isReadable()
            ? HttpRequest.BodyPublishers.ofByteArray(copyToBytes(request.content()))
            : HttpRequest.BodyPublishers.noBody();

    HttpRequest.Builder builder =
        HttpRequest.newBuilder(uri).method(request.method().name(), bodyPublisher);

    // Copy client headers to the outgoing request
    for (String name : request.headers().names()) {
      if (HttpHeaderNames.HOST.contentEqualsIgnoreCase(name)) {
        continue; // Host header will be set by HttpClient
      }
      for (String value : request.headers().getAll(name)) {
        builder.header(name, value);
      }
    }

    HttpResponse<byte[]> upstreamResponse =
        httpClient.send(builder.build(), HttpResponse.BodyHandlers.ofByteArray());

    ByteBuf content = Unpooled.wrappedBuffer(upstreamResponse.body());
    HttpResponseStatus status = HttpResponseStatus.valueOf(upstreamResponse.statusCode());
    DefaultFullHttpResponse response =
        new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);

    upstreamResponse
        .headers()
        .map()
        .forEach(
            (k, values) -> {
              for (String v : values) {
                response.headers().add(k, v);
              }
            });

    HttpUtil.setContentLength(response, content.readableBytes());
    return response;
  }

  private void checkpointContainer() {
    String checkpointName = "checkpoint-" + System.currentTimeMillis();
    boolean checkpointCreated = false;
    ProcessBuilder pb =
        new ProcessBuilder(
            "docker",
            "checkpoint",
            "create",
            "--leave-running=true",
            "--checkpoint-dir=/dev/shm/chk",
            containerName,
            checkpointName);
    try {
      Process process = pb.start();
      int exit = process.waitFor();
      if (exit != 0) {
        String errOutput;
        try (java.io.InputStream err = process.getErrorStream()) {
          errOutput = new String(err.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
        if (errOutput.isEmpty()) {
          LOG.warning("Docker checkpoint exited with " + exit + " for container " + containerName);
        } else {
          LOG.warning(
              "Docker checkpoint exited with "
                  + exit
                  + " for container "
                  + containerName
                  + " stderr: "
                  + errOutput);
        }
      } else {
        LOG.fine("Created checkpoint " + checkpointName + " for container " + containerName);
        checkpointCreated = true;
      }
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Failed to run docker checkpoint", e);
    }

    if (IS_SYNC_CHECKPOINT && checkpointCreated) {
      syncCheckpoint(checkpointName);
    }
  }

  private void syncCheckpoint(String checkpointName) {
    if (replicaAddresses.isEmpty()) {
      return;
    }

    int total = replicaAddresses.size();
    int majority = (total / 2) + 1;
    if (majority <= 1) {
      return;
    }

    String checkpointRootDir = "/dev/shm/chk";
    ExecutorCompletionService<Boolean> completionService =
        new ExecutorCompletionService<>(syncExecutor);
    int submitted = 0;

    for (int i = 0; i < replicaAddresses.size(); i++) {
      String replica = replicaAddresses.get(i);
      if (i == 0) {
        continue; // first address is self
      }
      completionService.submit(() -> rsyncCheckpoint(checkpointRootDir, checkpointName, replica));
      submitted++;
    }

    if (submitted == 0) {
      return;
    }

    int successes = 1; // local checkpoint counts toward majority
    for (int i = 0; i < submitted && successes < majority; i++) {
      try {
        Future<Boolean> future = completionService.take();
        if (Boolean.TRUE.equals(future.get())) {
          successes++;
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (ExecutionException e) {
        LOG.log(Level.WARNING, "Rsync to replica failed", e.getCause());
      }
    }

    if (successes >= majority) {
      LOG.fine(
          "Checkpoint "
              + checkpointName
              + " synced to majority of replicas ("
              + successes
              + "/"
              + total
              + ")");
    } else {
      LOG.warning(
          "Failed to sync checkpoint "
              + checkpointName
              + " to majority of replicas ("
              + successes
              + "/"
              + total
              + ")");
    }
  }

  private boolean rsyncCheckpoint(
      String checkpointRootDir, String checkpointName, String replicaHost) {
    String checkpointPath = checkpointRootDir + "/" + checkpointName;
    String sshKey = sshKeyPath;
    String remoteUserHost =
        (sshUser == null || sshUser.isBlank()) ? replicaHost : sshUser + "@" + replicaHost;
    List<String> cmd =
        List.of(
            "sudo",
            "rsync",
            "-avR",
            "--delete",
            "--rsync-path=sudo rsync",
            "-e",
            "ssh -o StrictHostKeyChecking=no -i " + sshKey,
            checkpointName,
            remoteUserHost + ":" + checkpointRootDir);
    String commandLine = String.join(" ", cmd);
    LOG.fine(() -> "Executing rsync command: " + commandLine);
    ProcessBuilder rsyncPb = new ProcessBuilder(cmd);
    rsyncPb.directory(new File(checkpointRootDir));
    rsyncPb.redirectErrorStream(true);
    long startNs = System.nanoTime();
    try {
      Process process = rsyncPb.start();
      byte[] output = process.getInputStream().readAllBytes();
      int exit = process.waitFor();
      if (exit == 0) {
        long durationMs = Duration.ofNanos(System.nanoTime() - startNs).toMillis();
        LOG.fine(
            () ->
                "Synced checkpoint "
                    + checkpointPath
                    + " to "
                    + replicaHost
                    + " in "
                    + durationMs
                    + "ms");
        return true;
      }
      String err = new String(output, StandardCharsets.UTF_8).trim();
      LOG.warning(
          "rsync to "
              + replicaHost
              + " failed with exit "
              + exit
              + (err.isEmpty() ? "" : " output: " + err));
    } catch (Exception e) {
      LOG.log(Level.WARNING, "rsync to replica " + replicaHost + " failed", e);
    }
    return false;
  }

  private void failFast(PendingRequest pending) {
    sendError(
        pending.ctx,
        HttpResponseStatus.SERVICE_UNAVAILABLE,
        "Replica shutting down",
        pending.keepAlive);
    ReferenceCountUtil.release(pending.request);
  }

  private boolean isLoopbackForward() {
    if (forwardPort != listenPort) {
      return false;
    }
    String hostLower = forwardHost.toLowerCase();
    return hostLower.equals("127.0.0.1") || hostLower.equals("localhost");
  }

  private static void sendError(
      ChannelHandlerContext ctx, HttpResponseStatus status, String message, boolean keepAlive) {
    ByteBuf content = Unpooled.copiedBuffer(message, StandardCharsets.UTF_8);
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
    HttpUtil.setContentLength(response, content.readableBytes());
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

    ChannelFuture writeFuture = ctx.writeAndFlush(response);
    if (!keepAlive) {
      writeFuture.addListener(ChannelFutureListener.CLOSE);
    }
  }

  private static byte[] copyToBytes(ByteBuf buf) {
    byte[] bytes = new byte[buf.readableBytes()];
    buf.getBytes(buf.readerIndex(), bytes);
    return bytes;
  }

  @Override
  public void close() {
    if (!running.compareAndSet(true, false)) {
      return;
    }

    if (serverChannel != null) {
      serverChannel.close().addListener(f -> LOG.fine("Server channel closed"));
    }
    if (bossGroup != null) {
      bossGroup.shutdownGracefully().syncUninterruptibly();
    }
    if (workerGroup != null) {
      workerGroup.shutdownGracefully().syncUninterruptibly();
    }
    batchExecutor.shutdownNow();
    forwardExecutor.shutdownNow();
    syncExecutor.shutdownNow();
  }

  /** Lightweight envelope used to hand off work from Netty threads to the batch worker. */
  private static final class PendingRequest {
    final ChannelHandlerContext ctx;
    final FullHttpRequest request;
    final boolean keepAlive;

    PendingRequest(ChannelHandlerContext ctx, FullHttpRequest request) {
      this.ctx = ctx;
      this.request = request;
      this.keepAlive = HttpUtil.isKeepAlive(request);
    }
  }

  /** Netty handler that accepts full HTTP requests and enqueues them for the worker. */
  private final class InboundHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
      FullHttpRequest retained = msg.retainedDuplicate();
      boolean accepted = queue.offer(new PendingRequest(ctx, retained));
      if (!accepted) {
        ReferenceCountUtil.release(retained);
        sendError(
            ctx,
            HttpResponseStatus.SERVICE_UNAVAILABLE,
            "Queue is full, please retry",
            HttpUtil.isKeepAlive(msg));
      }
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.log(Level.WARNING, "Request handling error", cause);
      if (ctx.channel().isActive()) {
        sendError(
            ctx,
            HttpResponseStatus.INTERNAL_SERVER_ERROR,
            "Internal error: " + cause.getMessage(),
            false);
      }
    }
  }
}
