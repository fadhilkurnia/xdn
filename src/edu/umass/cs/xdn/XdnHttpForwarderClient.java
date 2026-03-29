package edu.umass.cs.xdn;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Lightweight synchronous HTTP client backed by Netty with per-origin connection pooling. Each
 * origin (host + port) reuses a {@link FixedChannelPool} so requests of the same origin avoid
 * repeated TCP handshakes.
 *
 * <p>Supports HTTP/1.1 pipelining via {@link #executePipelined(String, int, List)} for batched
 * requests: all requests are written to a single channel before waiting for any response, reducing
 * round-trip overhead from N sequential forwards to a single pipelined exchange.
 */
public final class XdnHttpForwarderClient implements Closeable {

  private static final Logger LOG = Logger.getLogger(XdnHttpForwarderClient.class.getName());

  private static final int MAX_CONTENT_LENGTH = 16 * 1024 * 1024;
  private static final int DEFAULT_MAX_POOL_SIZE = 128;
  private static final String POOL_SIZE_PROP = "XDN_HTTP_MAX_POOL_SIZE";

  private static final AtomicLong REQUEST_COUNTER = new AtomicLong();
  private static final int LOG_SAMPLE_INTERVAL = 100;

  private final EventLoopGroup eventLoopGroup;
  private final boolean manageEventLoopGroup;
  private final ConcurrentMap<Origin, FixedChannelPool> pools = new ConcurrentHashMap<>();

  /** Creates a client backed by its own event loop group. */
  public XdnHttpForwarderClient() {
    this(new NioEventLoopGroup(0), true);
  }

  private XdnHttpForwarderClient(EventLoopGroup group, boolean manageGroup) {
    this.eventLoopGroup = Objects.requireNonNull(group, "eventLoopGroup");
    this.manageEventLoopGroup = manageGroup;
  }

  private static final int MAX_RETRIES = 1;

  /** When true, force Connection: keep-alive on all outgoing requests to prevent
   *  upstream containers (e.g. Apache) from closing pooled connections.
   *  Enable with -DHTTP_FORCE_KEEPALIVE=true */
  private static final boolean FORCE_KEEPALIVE =
      Boolean.parseBoolean(System.getProperty("HTTP_FORCE_KEEPALIVE", "false"));

  /**
   * Sends the given HTTP request and returns a detached response. The caller is responsible for
   * releasing the returned {@link FullHttpResponse} to avoid leaking pooled buffers.
   *
   * <p>Retries once on transient connection-pool errors (inactive channel, closed channel) which
   * happen when the remote server closes an idle keep-alive connection between the pool health
   * check and the actual write.
   */
  public FullHttpResponse execute(String host, int port, FullHttpRequest request) throws Exception {
    Objects.requireNonNull(host, "host");
    Objects.requireNonNull(request, "request");
    if (MAX_RETRIES <= 0) {
      return executeOnce(host, port, request);
    }
    // Extra retain so the request survives a failed attempt where writeAndFlush consumed
    // the ByteBuf content.  Released in the finally block below.
    ReferenceCountUtil.retain(request);
    Exception lastException = null;
    try {
      for (int attempt = 0; attempt <= MAX_RETRIES; attempt++) {
        try {
          return executeOnce(host, port, request);
        } catch (IllegalStateException e) {
          // Retry on transient pool errors (inactive/closed channels)
          String msg = e.getMessage();
          if (msg != null
              && (msg.contains("inactive")
                  || msg.contains("closed")
                  || msg.contains("in flight"))) {
            lastException = e;
            LOG.log(
                Level.WARNING, "HTTP fwd retry attempt {0}: {1}", new Object[] {attempt + 1, msg});
            continue;
          }
          throw e;
        } catch (java.nio.channels.ClosedChannelException e) {
          lastException = e;
          LOG.log(
              Level.WARNING,
              "HTTP fwd retry attempt {0}: ClosedChannelException",
              new Object[] {attempt + 1});
          continue;
        }
      }
      throw lastException;
    } finally {
      // Release the extra retain; skip if refCnt is already 0 (both attempts wrote)
      if (request.refCnt() > 0) {
        ReferenceCountUtil.release(request);
      }
    }
  }

  /**
   * Sends multiple HTTP requests over a single connection using HTTP/1.1 pipelining. All requests
   * are written to the channel before waiting for any response, reducing latency from N sequential
   * round-trips to approximately 1 round-trip + N × server processing time.
   *
   * <p>Responses are returned in the same order as the requests. The caller is responsible for
   * releasing each returned {@link FullHttpResponse}.
   *
   * <p>Falls back to sequential {@link #execute} if only one request is provided.
   *
   * @param host the target host (e.g., "127.0.0.1")
   * @param port the target port
   * @param requests the list of HTTP requests to pipeline
   * @return list of responses in the same order as requests
   */
  public List<FullHttpResponse> executePipelined(
      String host, int port, List<FullHttpRequest> requests) throws Exception {
    Objects.requireNonNull(host, "host");
    Objects.requireNonNull(requests, "requests");
    if (requests.isEmpty()) {
      return List.of();
    }
    // Single request: use the normal path (no pipelining overhead).
    if (requests.size() == 1) {
      return List.of(execute(host, port, requests.get(0)));
    }

    final int n = requests.size();
    final long t0 = System.nanoTime();

    Origin origin = new Origin(host, port);
    FixedChannelPool pool = poolFor(origin);

    // One CompletableFuture per request, completed in order by the handler.
    List<CompletableFuture<FullHttpResponse>> responseFutures = new ArrayList<>(n);
    for (int i = 0; i < n; i++) {
      responseFutures.add(new CompletableFuture<>());
    }

    // Retain all requests so they survive writeAndFlush.
    for (FullHttpRequest req : requests) {
      ReferenceCountUtil.retain(req);
    }

    pool.acquire()
        .addListener(
            (Future<Channel> acquireFuture) -> {
              if (!acquireFuture.isSuccess()) {
                Throwable cause = acquireFuture.cause();
                for (CompletableFuture<FullHttpResponse> f : responseFutures) {
                  f.completeExceptionally(cause);
                }
                return;
              }

              Channel channel = acquireFuture.getNow();
              if (channel == null || !channel.isActive()) {
                IllegalStateException ex =
                    new IllegalStateException("Acquired inactive channel for pipelining");
                for (CompletableFuture<FullHttpResponse> f : responseFutures) {
                  f.completeExceptionally(ex);
                }
                if (channel != null) {
                  releaseQuietly(pool, channel);
                }
                return;
              }

              ClientResponseHandler handler =
                  channel.pipeline().get(ClientResponseHandler.class);
              if (handler == null) {
                IllegalStateException ex =
                    new IllegalStateException("Missing response handler in pipeline");
                for (CompletableFuture<FullHttpResponse> f : responseFutures) {
                  f.completeExceptionally(ex);
                }
                releaseQuietly(pool, channel);
                return;
              }

              // Register all response futures in order before writing any request.
              // The handler will complete them in FIFO order as responses arrive.
              PipelineContext pctx = new PipelineContext(channel, pool, responseFutures);
              if (!handler.registerPipeline(pctx)) {
                IllegalStateException ex =
                    new IllegalStateException("Channel busy, cannot start pipelining");
                for (CompletableFuture<FullHttpResponse> f : responseFutures) {
                  f.completeExceptionally(ex);
                }
                releaseQuietly(pool, channel);
                return;
              }

              // Write all requests without waiting for responses (pipelining).
              for (int i = 0; i < n; i++) {
                FullHttpRequest req = requests.get(i);
                if (FORCE_KEEPALIVE) {
                  HttpUtil.setKeepAlive(req, true);
                }
                if (i < n - 1) {
                  channel.write(req);
                } else {
                  // Flush on the last write.
                  channel.writeAndFlush(req)
                      .addListener(
                          (ChannelFutureListener)
                              wf -> {
                                if (!wf.isSuccess()) {
                                  handler.failAll(wf.cause());
                                }
                              });
                }
              }
            });

    // Wait for all responses.
    List<FullHttpResponse> responses = new ArrayList<>(n);
    try {
      for (int i = 0; i < n; i++) {
        responses.add(responseFutures.get(i).get());
      }
    } catch (ExecutionException e) {
      // Release any already-received responses on failure.
      for (FullHttpResponse resp : responses) {
        ReferenceCountUtil.release(resp);
      }
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw new RuntimeException(cause);
    } finally {
      for (FullHttpRequest req : requests) {
        if (req.refCnt() > 0) {
          ReferenceCountUtil.release(req);
        }
      }
    }

    if (LOG.isLoggable(Level.FINE)) {
      double totalMs = (System.nanoTime() - t0) / 1_000_000.0;
      LOG.fine(
          String.format(
              "HTTP pipelined fwd: %d requests in %.2fms (%.2fms/req)",
              n, totalMs, totalMs / n));
    }

    return responses;
  }

  private FullHttpResponse executeOnce(String host, int port, FullHttpRequest request)
      throws Exception {
    final long t0 = System.nanoTime();
    final long reqNum = REQUEST_COUNTER.incrementAndGet();
    // ts[0] = acquire complete, ts[1] = write complete
    final long[] ts = new long[2];

    Origin origin = new Origin(host, port);
    FixedChannelPool pool = poolFor(origin);
    CompletableFuture<FullHttpResponse> responseFuture = new CompletableFuture<>();

    // Instead of copying the request, we bump up the reference count
    // and release it in the finally part, at the end of this method.
    ReferenceCountUtil.retain(request);
    final FullHttpRequest outbound = request;

    pool.acquire()
        .addListener(
            (Future<Channel> acquireFuture) -> {
              ts[0] = System.nanoTime();

              if (!acquireFuture.isSuccess()) {
                responseFuture.completeExceptionally(acquireFuture.cause());
                return;
              }

              Channel channel = acquireFuture.getNow();
              if (channel == null || !channel.isActive()) {
                // Channel was closed by the server (Connection: close) since
                // it was returned to the pool. Release it (to decrement pool
                // count) then close, and acquire a fresh connection.
                if (channel != null) {
                  releaseQuietly(pool, channel);
                  channel.close();
                }
                pool.acquire()
                    .addListener((Future<Channel> retryFuture) -> {
                      if (!retryFuture.isSuccess()) {
                        responseFuture.completeExceptionally(retryFuture.cause());
                        return;
                      }
                      Channel fresh = retryFuture.getNow();
                      if (fresh == null || !fresh.isActive()) {
                        responseFuture.completeExceptionally(
                            new IllegalStateException("Acquired inactive HTTP channel after retry"));
                        if (fresh != null) {
                          releaseQuietly(pool, fresh);
                          fresh.close();
                        }
                        return;
                      }
                      dispatchRequest(fresh, pool, outbound, responseFuture, ts);
                    });
                return;
              }

              dispatchRequest(channel, pool, outbound, responseFuture, ts);
            });

    try {
      FullHttpResponse response = responseFuture.get();
      if (reqNum % LOG_SAMPLE_INTERVAL == 0) {
        long tEnd = System.nanoTime();
        double totalMs = (tEnd - t0) / 1_000_000.0;
        double acqMs = (ts[0] - t0) / 1_000_000.0;
        double writeMs = (ts[1] - ts[0]) / 1_000_000.0;
        double respMs = (tEnd - ts[1]) / 1_000_000.0;
        LOG.info(
            String.format(
                "HTTP fwd: total=%.2fms acq=%.2fms write=%.2fms resp=%.2fms",
                totalMs, acqMs, writeMs, respMs));
      }
      return response;
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw new RuntimeException(cause);
    } finally {
      ReferenceCountUtil.release(request);
    }
  }

  /** Dispatch the HTTP request on an active channel. Used by both the initial
   *  acquire path and the retry-on-inactive path. */
  private void dispatchRequest(Channel channel, FixedChannelPool pool,
      FullHttpRequest outbound, CompletableFuture<FullHttpResponse> responseFuture, long[] ts) {
    ClientResponseHandler handler = channel.pipeline().get(ClientResponseHandler.class);
    if (handler == null) {
      responseFuture.completeExceptionally(
          new IllegalStateException("Missing response handler in pipeline"));
      releaseQuietly(pool, channel);
      return;
    }

    RequestContext context = new RequestContext(channel, pool, responseFuture);
    if (!handler.register(context)) {
      responseFuture.completeExceptionally(
          new IllegalStateException("Another request is already in flight"));
      releaseQuietly(pool, channel);
      return;
    }

    if (FORCE_KEEPALIVE) {
      HttpUtil.setKeepAlive(outbound, true);
    }

    // Re-set TCP_QUICKACK before each request. The Linux kernel resets
    // quickack mode after processing received data, so we must re-enable it
    // before each send to ensure the ACK for the server's response header
    // is sent immediately (avoiding the Nagle + delayed ACK 40ms penalty).
    PoolHandler.setQuickAck(channel);
    ChannelFuture writeFuture = channel.writeAndFlush(outbound);
    writeFuture.addListener(
        (ChannelFutureListener)
            wf -> {
              ts[1] = System.nanoTime();
              if (!wf.isSuccess()) {
                handler.fail(wf.cause());
              }
            });
  }

  /** Number of TCP connections to pre-create when a pool is first used.
   *  Set to 0 to disable pre-warming. */
  private static final int POOL_PREWARM_SIZE =
      Integer.getInteger("XDN_HTTP_POOL_PREWARM_SIZE", 8);

  private FixedChannelPool poolFor(Origin origin) {
    return pools.computeIfAbsent(origin, this::createAndWarmPool);
  }

  private FixedChannelPool createAndWarmPool(Origin origin) {
    Bootstrap bootstrap =
        new Bootstrap()
            .group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5_000)
            .remoteAddress(new InetSocketAddress(origin.host(), origin.port()));

    FixedChannelPool pool = new FixedChannelPool(bootstrap, new PoolHandler(), getMaxPoolSize());

    // Pre-warm: eagerly create TCP connections so the first real requests
    // don't pay the connect latency.
    if (POOL_PREWARM_SIZE > 0) {
      int n = Math.min(POOL_PREWARM_SIZE, getMaxPoolSize());
      List<Channel> warmChannels = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        try {
          Future<Channel> f = pool.acquire();
          Channel ch = f.await().getNow();
          if (ch != null && ch.isActive()) {
            warmChannels.add(ch);
          } else if (ch != null) {
            ch.close();
          }
        } catch (Exception e) {
          LOG.log(Level.FINE, "Pool pre-warm connection {0} failed: {1}",
              new Object[]{i, e.getMessage()});
          break;
        }
      }
      for (Channel ch : warmChannels) {
        pool.release(ch);
      }
      LOG.log(Level.INFO, "Pre-warmed {0} connections to {1}:{2}",
          new Object[]{warmChannels.size(), origin.host(), origin.port()});
    }

    return pool;
  }

  private int getMaxPoolSize() {
    String val = System.getProperty(POOL_SIZE_PROP);
    if (val != null) {
      try {
        return Integer.parseInt(val.trim());
      } catch (NumberFormatException ignored) {
      }
    }
    return DEFAULT_MAX_POOL_SIZE;
  }

  private static FullHttpRequest copyRequest(FullHttpRequest request) {
    ByteBuf content = request.content();
    ByteBuf copiedContent =
        content != null && content.isReadable()
            ? Unpooled.copiedBuffer(content)
            : Unpooled.EMPTY_BUFFER;

    DefaultFullHttpRequest copy =
        new DefaultFullHttpRequest(
            request.protocolVersion(), request.method(), request.uri(), copiedContent);
    copy.headers().set(request.headers());
    copy.trailingHeaders().set(request.trailingHeaders());
    if (copiedContent != Unpooled.EMPTY_BUFFER) {
      HttpUtil.setContentLength(copy, copiedContent.readableBytes());
    } else {
      copy.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
    }
    return copy;
  }

  private static FullHttpResponse copyResponse(FullHttpResponse response) {
    ByteBuf content = response.content();
    ByteBuf payload =
        (content == null || !content.isReadable()) ? Unpooled.EMPTY_BUFFER : content.copy();

    HttpHeaders headers = response.headers().copy();
    HttpHeaders trailing = response.trailingHeaders().copy();

    DefaultFullHttpResponse copy =
        new DefaultFullHttpResponse(
            response.protocolVersion(), response.status(), payload, headers, trailing);

    if (payload == Unpooled.EMPTY_BUFFER) {
      copy.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
    } else {
      HttpUtil.setContentLength(copy, payload.readableBytes());
    }

    return copy;
  }

  private static void releaseQuietly(FixedChannelPool pool, Channel channel) {
    try {
      pool.release(channel)
          .addListener(
              f -> {
                if (!f.isSuccess()) {
                  LOG.log(Level.WARNING, "Failed to return channel to pool", f.cause());
                  channel.close();
                }
              });
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "Failed to release channel back to pool", t);
      channel.close();
    }
  }

  @Override
  public void close() {
    pools
        .values()
        .forEach(
            pool -> {
              try {
                pool.close();
              } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Failed to close pool", e);
              }
            });
    pools.clear();

    if (manageEventLoopGroup) {
      eventLoopGroup.shutdownGracefully().syncUninterruptibly();
    }
  }

  private static final class PoolHandler implements ChannelPoolHandler {
    @Override
    public void channelReleased(Channel ch) {
      ClientResponseHandler handler = ch.pipeline().get(ClientResponseHandler.class);
      if (handler != null) {
        handler.clear();
      }
    }

    @Override
    public void channelAcquired(Channel ch) {
      setQuickAck(ch);
    }

    @Override
    public void channelCreated(Channel ch) {
      setQuickAck(ch);
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast(new HttpClientCodec());
      // Re-set TCP_QUICKACK on every channelRead BEFORE the aggregator
      // processes the data. This ensures the ACK for the response header
      // segment is sent immediately (the kernel resets TCP_QUICKACK after
      // each receive, so we must re-enable it before each ACK).
      pipeline.addLast(new io.netty.channel.ChannelInboundHandlerAdapter() {
        @Override
        public void channelRead(io.netty.channel.ChannelHandlerContext ctx,
                                Object msg) throws Exception {
          setQuickAck(ctx.channel());
          super.channelRead(ctx, msg);
        }
      });
      pipeline.addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH));
      pipeline.addLast(new ClientResponseHandler());
    }

    private static volatile boolean quickAckWarned = false;

    static void setQuickAck(Channel ch) {
      try {
        // Access protected javaChannel() via AbstractNioChannel
        java.lang.reflect.Method m =
            io.netty.channel.nio.AbstractNioChannel.class.getDeclaredMethod("javaChannel");
        m.setAccessible(true);
        java.nio.channels.SocketChannel sc =
            (java.nio.channels.SocketChannel) m.invoke(ch);
        // TCP_QUICKACK disables delayed ACK; must be re-set after each recv
        sc.setOption(jdk.net.ExtendedSocketOptions.TCP_QUICKACK, true);
      } catch (Exception e) {
        if (!quickAckWarned) {
          LOG.log(Level.WARNING, "Could not set TCP_QUICKACK: {0}", e.getMessage());
          quickAckWarned = true;
        }
      }
    }
  }

  /**
   * Channel handler that supports both single-request mode and pipelined mode. In pipelined mode,
   * incoming responses are matched to requests in FIFO order.
   */
  private static final class ClientResponseHandler
      extends SimpleChannelInboundHandler<FullHttpResponse> {

    // Single-request mode: exactly one in-flight request.
    private final Queue<RequestContext> singleQueue = new ConcurrentLinkedQueue<>();

    // Pipelined mode: multiple in-flight requests, completed in FIFO order.
    private volatile PipelineContext pipelineContext;

    ClientResponseHandler() {
      super(false);
    }

    /** Register a single request context (non-pipelined mode). */
    boolean register(RequestContext context) {
      if (pipelineContext != null) {
        return false; // Channel is in pipelined mode.
      }
      if (!singleQueue.isEmpty()) {
        return false; // Another request is already in flight.
      }
      singleQueue.add(context);
      return true;
    }

    /** Register a pipeline context for pipelined mode. */
    boolean registerPipeline(PipelineContext pctx) {
      if (pipelineContext != null || !singleQueue.isEmpty()) {
        return false; // Channel is busy.
      }
      this.pipelineContext = pctx;
      return true;
    }

    void fail(Throwable throwable) {
      RequestContext context = singleQueue.poll();
      if (context != null) {
        context.fail(throwable);
      }
    }

    void failAll(Throwable throwable) {
      PipelineContext pctx = this.pipelineContext;
      if (pctx != null) {
        pctx.failAll(throwable);
        this.pipelineContext = null;
      }
      RequestContext ctx;
      while ((ctx = singleQueue.poll()) != null) {
        ctx.fail(throwable);
      }
    }

    void clear() {
      singleQueue.clear();
      pipelineContext = null;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
      // Check pipelined mode first.
      PipelineContext pctx = this.pipelineContext;
      if (pctx != null) {
        boolean done = pctx.completeNext(msg);
        if (done) {
          this.pipelineContext = null;
        }
        return;
      }

      // Single-request mode.
      RequestContext context = singleQueue.poll();
      if (context == null) {
        ReferenceCountUtil.release(msg);
        LOG.warning("Received response with no context; dropping");
        return;
      }
      context.complete(msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      failAll(new IllegalStateException("Backend channel closed"));
      ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      failAll(cause);
      ctx.close();
    }
  }

  private record RequestContext(
      Channel channel, FixedChannelPool pool, CompletableFuture<FullHttpResponse> responseFuture) {

    void complete(FullHttpResponse response) {
      boolean delivered = responseFuture.complete(response);
      if (!delivered) {
        ReferenceCountUtil.release(response);
      }
      // If server sent Connection: close, close the channel AFTER releasing
      // it to the pool. Release decrements the pool's acquired count (so new
      // connections can be created), and the subsequent close invalidates the
      // channel so the pool won't hand it out again.
      if (!HttpUtil.isKeepAlive(response)) {
        release();
        channel.close();
        return;
      }
      release();
    }

    void fail(Throwable throwable) {
      if (!responseFuture.completeExceptionally(throwable)) {
        LOG.log(Level.WARNING, "Duplicate failure delivery", throwable);
      }
      release();
    }

    private void release() {
      try {
        pool.release(channel)
            .addListener(
                f -> {
                  if (!f.isSuccess()) {
                    LOG.log(Level.WARNING, "Failed to release channel", f.cause());
                    channel.close();
                  }
                });
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "Exception releasing channel to pool", t);
        channel.close();
      }
    }
  }

  /**
   * Tracks multiple in-flight pipelined requests on a single channel. Responses are completed in
   * FIFO order. The channel is released back to the pool only after all responses are received.
   */
  private static final class PipelineContext {
    private final Channel channel;
    private final FixedChannelPool pool;
    private final List<CompletableFuture<FullHttpResponse>> futures;
    private int nextIndex = 0;
    private boolean serverClosedConnection = false;

    PipelineContext(
        Channel channel,
        FixedChannelPool pool,
        List<CompletableFuture<FullHttpResponse>> futures) {
      this.channel = channel;
      this.pool = pool;
      this.futures = futures;
    }

    /** Complete the next pending future. Returns true if all futures are now complete. */
    boolean completeNext(FullHttpResponse response) {
      if (nextIndex >= futures.size()) {
        ReferenceCountUtil.release(response);
        LOG.warning("Pipelined response received but all futures already completed");
        return true;
      }
      // Track if server signalled Connection: close on any response.
      if (!HttpUtil.isKeepAlive(response)) {
        serverClosedConnection = true;
      }
      CompletableFuture<FullHttpResponse> future = futures.get(nextIndex++);
      if (!future.complete(response)) {
        ReferenceCountUtil.release(response);
      }
      boolean allDone = nextIndex >= futures.size();
      if (allDone) {
        // Always release to the pool first (to decrement acquired count),
        // then close if the server signalled Connection: close.
        releaseChannel();
        if (serverClosedConnection) {
          channel.close();
        }
      }
      return allDone;
    }

    void failAll(Throwable throwable) {
      for (int i = nextIndex; i < futures.size(); i++) {
        futures.get(i).completeExceptionally(throwable);
      }
      nextIndex = futures.size();
      releaseChannel();
    }

    private void releaseChannel() {
      try {
        pool.release(channel)
            .addListener(
                f -> {
                  if (!f.isSuccess()) {
                    LOG.log(Level.WARNING, "Failed to release pipelined channel", f.cause());
                    channel.close();
                  }
                });
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "Exception releasing pipelined channel", t);
        channel.close();
      }
    }
  }

  private record Origin(String host, int port) {}
}
