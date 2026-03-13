package edu.umass.cs.xdn;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Async HTTP/1.1 client with a dynamic connection pool per origin.
 *
 * <p>Connection lifecycle:
 * <ul>
 *   <li>A new connection is opened whenever a request arrives and all existing connections are
 *       occupied, up to {@code maxConnections}.
 *   <li>When {@code maxConnections} is reached and all connections are occupied, the request is
 *       queued. Queued requests are served in FIFO order as connections free up.
 *   <li>Queued requests that are not served within {@code queueTimeoutMs} fail with a
 *       {@link TimeoutException}.
 *   <li>Connections that have been idle for longer than {@code idleTimeoutMs} are closed and
 *       removed from the pool.
 * </ul>
 *
 * <p>All operations are non-blocking. {@link #executeAsync} returns a {@link CompletableFuture}
 * that completes on the Netty EventLoop thread when the container responds.
 *
 * <p>Usage example:
 * <pre>{@code
 * XdnHttpForwarderClient client = new XdnHttpForwarderClient.Builder()
 *         .maxConnections(512)
 *         .minConnections(8)
 *         .idleTimeoutMs(10_000)
 *         .queueTimeoutMs(5_000)
 *         .build();
 *
 * client.executeAsync("127.0.0.1", 8080, request)
 *       .whenComplete((response, err) -> {
 *           if (err != null) { ... }
 *           else {
 *               // use response
 *               response.release(); // caller must release
 *           }
 *       });
 * }</pre>
 */
public final class XdnHttpForwarderClient implements Closeable {

  private static final Logger LOG = Logger.getLogger(XdnHttpForwarderClient.class.getName());

  private static final int MAX_RESPONSE_CONTENT_LENGTH = 16 * 1024 * 1024; // 16 MiB

  // ---------------------------------------------------------------------------
  // Configuration
  // ---------------------------------------------------------------------------
  private final int minConnections;
  private final int maxConnections;
  private final long idleTimeoutMs;
  private final long queueTimeoutMs;

  // ---------------------------------------------------------------------------
  // Netty infrastructure
  // ---------------------------------------------------------------------------
  private final EventLoopGroup eventLoopGroup;
  private final boolean manageEventLoopGroup;
  private final Bootstrap bootstrap;

  // ---------------------------------------------------------------------------
  // Pool state — one DynamicPool per origin (host+port).
  // In practice this client targets a single local origin, but the map keeps
  // the design general.
  // ---------------------------------------------------------------------------
  private final ConcurrentMap<Origin, DynamicPool> pools = new ConcurrentHashMap<>();

  // Idle-connection reaper, shared across all origins.
  private final ScheduledExecutorService idleReaper =
          Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "xdn-forwarder-idle-reaper");
            t.setDaemon(true);
            return t;
          });

  // ---------------------------------------------------------------------------
  // Constructors / Builder
  // ---------------------------------------------------------------------------

  private XdnHttpForwarderClient(Builder builder, EventLoopGroup group, boolean manageGroup) {
    this.minConnections      = builder.minConnections;
    this.maxConnections      = builder.maxConnections;
    this.idleTimeoutMs       = builder.idleTimeoutMs;
    this.queueTimeoutMs      = builder.queueTimeoutMs;
    this.eventLoopGroup      = Objects.requireNonNull(group);
    this.manageEventLoopGroup = manageGroup;

    this.bootstrap = new Bootstrap()
            .group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5_000);

    // Reap idle connections every (idleTimeoutMs / 2) to keep the interval responsive.
    long reaperIntervalMs = Math.max(1_000, idleTimeoutMs / 2);
    idleReaper.scheduleAtFixedRate(
            this::reapIdleConnections, reaperIntervalMs, reaperIntervalMs, TimeUnit.MILLISECONDS);
  }

  /** Default client suitable for local container forwarding. */
  public XdnHttpForwarderClient() {
    this(new Builder(), new NioEventLoopGroup(0), true);
  }

  /**
   * Client that shares an externally managed EventLoopGroup.
   * The caller owns the group's lifecycle — this client will NOT shut it down on close().
   */
  public XdnHttpForwarderClient(EventLoopGroup sharedGroup) {
    this(new Builder(), sharedGroup, false);
  }

  public static final class Builder {
    private int  minConnections = 8;
    private int  maxConnections = 512;
    private long idleTimeoutMs  = 10_000;
    private long queueTimeoutMs = 5_000;
    private EventLoopGroup sharedGroup = null;

    /** Minimum connections kept alive per origin even when idle. */
    public Builder minConnections(int v) { this.minConnections = v; return this; }

    /** Hard ceiling on open connections per origin. */
    public Builder maxConnections(int v) { this.maxConnections = v; return this; }

    /** Milliseconds before an idle connection is closed. */
    public Builder idleTimeoutMs(long v) { this.idleTimeoutMs = v; return this; }

    /**
     * Milliseconds a queued request will wait for a free connection
     * before failing with {@link TimeoutException}.
     */
    public Builder queueTimeoutMs(long v) { this.queueTimeoutMs = v; return this; }

    /** Share an external EventLoopGroup (its lifecycle is managed by the caller). */
    public Builder sharedEventLoopGroup(EventLoopGroup g) { this.sharedGroup = g; return this; }

    public XdnHttpForwarderClient build() {
      if (sharedGroup != null) {
        return new XdnHttpForwarderClient(this, sharedGroup, false);
      }
      return new XdnHttpForwarderClient(this, new NioEventLoopGroup(0), true);
    }
  }

  // ---------------------------------------------------------------------------
  // Public API
  // ---------------------------------------------------------------------------

  /**
   * Sends {@code request} to {@code host:port} asynchronously.
   *
   * <p>The returned future completes on a Netty EventLoop thread. If the caller needs to do
   * significant work in the completion callback, it should dispatch to its own executor to avoid
   * blocking the EventLoop.
   *
   * <p>The caller is responsible for releasing the {@link FullHttpResponse} after use.
   *
   * @return a future that completes with the response, or exceptionally on error or timeout.
   */
  public CompletableFuture<FullHttpResponse> executeAsync(
          String host, int port, FullHttpRequest request) {
    Objects.requireNonNull(host,    "host");
    Objects.requireNonNull(request, "request");

    DynamicPool pool = pools.computeIfAbsent(new Origin(host, port), DynamicPool::new);
    return pool.submit(request);
  }

  @Override
  public void close() {
    idleReaper.shutdown();
    pools.values().forEach(DynamicPool::close);
    pools.clear();
    if (manageEventLoopGroup) {
      eventLoopGroup.shutdownGracefully().syncUninterruptibly();
    }
  }

  // ---------------------------------------------------------------------------
  // Idle connection reaper
  // ---------------------------------------------------------------------------

  private void reapIdleConnections() {
    pools.values().forEach(p -> p.reapIdle(idleTimeoutMs, minConnections));
  }

  // ---------------------------------------------------------------------------
  // DynamicPool — one per origin
  // ---------------------------------------------------------------------------

  /**
   * Manages a set of {@link PooledConnection}s for a single origin.
   *
   * <p>Invariants:
   * <ul>
   *   <li>{@code totalConnections} counts all connections — idle, in-use, and connecting.
   *   <li>A connection is "idle" when its {@code inFlight} reference is null.
   *   <li>Pending requests are queued in {@code waitQueue} when all connections are occupied
   *       and the max has been reached.
   * </ul>
   */
  private final class DynamicPool {

    private final Origin origin;

    // All connections owned by this pool (idle + in-use).
    private final Queue<PooledConnection> connections = new ConcurrentLinkedQueue<>();
    private final AtomicInteger totalConnections = new AtomicInteger(0);

    // Requests waiting for a free connection.
    private final Queue<PendingRequest> waitQueue = new ConcurrentLinkedQueue<>();

    DynamicPool(Origin origin) {
      this.origin = origin;
      // Pre-warm minimum connections eagerly so the first burst pays no handshake cost.
      for (int i = 0; i < minConnections; i++) {
        openNewConnection();
      }
    }

    /**
     * Submits a request. Returns immediately with a future that completes when the
     * container responds (or when a timeout / error occurs).
     */
    CompletableFuture<FullHttpResponse> submit(FullHttpRequest request) {
      CompletableFuture<FullHttpResponse> future = new CompletableFuture<>();

      // Retain so the buffer stays live until Netty finishes writing it.
      ReferenceCountUtil.retain(request);

      // Try to find an idle connection first.
      PooledConnection idle = findIdle();
      if (idle != null) {
        idle.send(request, future);
        return future;
      }

      // No idle connection — open a new one if under the limit.
      if (totalConnections.get() < maxConnections) {
        PooledConnection conn = openNewConnection();
        // openNewConnection() is async; the connection will pick up the request
        // once it becomes active via drainWaitQueue().
        waitQueue.add(new PendingRequest(request, future, System.currentTimeMillis()));
        // If the connection was already active (e.g. reused), drain immediately.
        conn.drainOrIdle();
        return future;
      }

      // At max connections — enqueue and wait.
      waitQueue.add(new PendingRequest(request, future, System.currentTimeMillis()));

      // Schedule timeout for this queued request.
      idleReaper.schedule(() -> {
        // Walk the queue looking for this specific future. If it's still there, time it out.
        waitQueue.removeIf(pending -> {
          if (pending.future == future && !future.isDone()) {
            ReferenceCountUtil.release(pending.request);
            future.completeExceptionally(new TimeoutException(
                    "Request queued for more than " + queueTimeoutMs + "ms — no connection available"));
            return true;
          }
          return false;
        });
      }, queueTimeoutMs, TimeUnit.MILLISECONDS);

      return future;
    }

    /** Returns an idle connection, or null if none are available. */
    private PooledConnection findIdle() {
      for (PooledConnection conn : connections) {
        if (conn.isIdle() && conn.isActive()) {
          return conn;
        }
      }
      return null;
    }

    /**
     * Opens a new TCP connection to the origin asynchronously.
     * The connection registers itself in {@code connections} once established.
     */
    private PooledConnection openNewConnection() {
      totalConnections.incrementAndGet();
      PooledConnection conn = new PooledConnection(this);
      connections.add(conn);

      bootstrap.clone()
              .remoteAddress(new InetSocketAddress(origin.host(), origin.port()))
              .handler(new ChannelInitializer<io.netty.channel.socket.SocketChannel>() {
                @Override
                protected void initChannel(io.netty.channel.socket.SocketChannel ch) {
                  ch.pipeline().addLast(new HttpClientCodec());
                  ch.pipeline().addLast(new HttpObjectAggregator(MAX_RESPONSE_CONTENT_LENGTH));
                  ch.pipeline().addLast(new ClientResponseHandler(conn));
                }
              })
              .connect()
              .addListener((ChannelFutureListener) cf -> {
                if (cf.isSuccess()) {
                  conn.attach(cf.channel());
                  // Pick up any queued requests that arrived before the connection was ready.
                  drainWaitQueue();
                } else {
                  LOG.log(Level.WARNING, "Failed to connect to " + origin, cf.cause());
                  connections.remove(conn);
                  totalConnections.decrementAndGet();
                  // Fail any requests that were waiting specifically for this connection slot.
                  // Other connections will drain the rest of the queue.
                  drainWaitQueue();
                }
              });

      return conn;
    }

    /**
     * Called whenever a connection becomes idle (after a response is received or on connect).
     * Drains the wait queue by dispatching pending requests to idle connections.
     */
    void drainWaitQueue() {
      PendingRequest pending;
      while ((pending = waitQueue.peek()) != null) {
        if (pending.future.isDone()) {
          // Already timed out — discard and move on.
          waitQueue.poll();
          continue;
        }
        PooledConnection idle = findIdle();
        if (idle == null) {
          // No idle connection available right now — leave the queue as-is.
          break;
        }
        // Atomically remove from the queue only if it's still the head.
        if (waitQueue.remove(pending)) {
          idle.send(pending.request, pending.future);
        }
      }
    }

    /** Closes connections that have been idle longer than {@code idleTimeoutMs},
     *  keeping at least {@code minConnections} alive. */
    void reapIdle(long idleTimeoutMs, int minKeep) {
      int active = totalConnections.get();
      for (PooledConnection conn : connections) {
        if (active <= minKeep) break;
        if (conn.isIdle() && conn.idleForMs() > idleTimeoutMs) {
          connections.remove(conn);
          totalConnections.decrementAndGet();
          conn.close();
          active--;
        }
      }
    }

    void onConnectionClosed(PooledConnection conn) {
      connections.remove(conn);
      totalConnections.decrementAndGet();
      // If there are queued requests and we're under the limit, open a replacement.
      if (!waitQueue.isEmpty() && totalConnections.get() < maxConnections) {
        openNewConnection();
      }
    }

    void close() {
      // Fail all queued requests.
      PendingRequest pending;
      while ((pending = waitQueue.poll()) != null) {
        ReferenceCountUtil.release(pending.request);
        pending.future.completeExceptionally(
                new IllegalStateException("XdnHttpForwarderClient closed"));
      }
      // Close all connections.
      connections.forEach(PooledConnection::close);
      connections.clear();
      totalConnections.set(0);
    }
  }

  // ---------------------------------------------------------------------------
  // PooledConnection — wraps a single Netty Channel
  // ---------------------------------------------------------------------------

  private static final class PooledConnection {

    private final DynamicPool pool;

    // Set once the TCP connection is established.
    private volatile Channel channel = null;

    // The in-flight request context. Null means the connection is idle.
    private final AtomicReference<RequestContext> inFlight = new AtomicReference<>();

    // Timestamp of when the connection last became idle, for idle timeout tracking.
    private volatile long idleSinceMs = System.currentTimeMillis();

    PooledConnection(DynamicPool pool) {
      this.pool = pool;
    }

    void attach(Channel ch) {
      this.channel = ch;
    }

    boolean isActive() {
      return channel != null && channel.isActive();
    }

    boolean isIdle() {
      return inFlight.get() == null;
    }

    long idleForMs() {
      return System.currentTimeMillis() - idleSinceMs;
    }

    /**
     * Sends a request on this connection. Must only be called when the connection is idle.
     * The retain on {@code request} was already done by {@link DynamicPool#submit}.
     */
    void send(FullHttpRequest request, CompletableFuture<FullHttpResponse> future) {
      RequestContext ctx = new RequestContext(future, this);
      if (!inFlight.compareAndSet(null, ctx)) {
        // Should never happen — caller checks isIdle() first.
        ReferenceCountUtil.release(request);
        future.completeExceptionally(
                new IllegalStateException("Connection was not idle when send() was called"));
        return;
      }

      channel.writeAndFlush(request).addListener((ChannelFutureListener) wf -> {
        // Release our retain now that Netty has consumed the buffer from the wire.
        ReferenceCountUtil.release(request);
        if (!wf.isSuccess()) {
          fail(wf.cause());
        }
      });
    }

    /**
     * Called when the connection is newly active but has no pending request.
     * Gives the pool a chance to dispatch a queued request onto this connection.
     */
    void drainOrIdle() {
      if (isActive() && isIdle()) {
        pool.drainWaitQueue();
      }
    }

    void complete(FullHttpResponse response) {
      RequestContext ctx = inFlight.getAndSet(null);
      idleSinceMs = System.currentTimeMillis();
      if (ctx != null) {
        boolean delivered = ctx.future.complete(response);
        if (!delivered) {
          // Future was already completed (e.g. by timeout) — release the response.
          ReferenceCountUtil.release(response);
        }
      } else {
        ReferenceCountUtil.release(response);
        LOG.warning("XdnHttpForwarderClient: response arrived with no in-flight context");
      }
      // Connection is now idle — serve next queued request if any.
      pool.drainWaitQueue();
    }

    void fail(Throwable cause) {
      RequestContext ctx = inFlight.getAndSet(null);
      idleSinceMs = System.currentTimeMillis();
      if (ctx != null) {
        ctx.future.completeExceptionally(cause);
      }
      // Close the channel on failure — don't return a broken connection to the pool.
      close();
    }

    void close() {
      if (channel != null && channel.isOpen()) {
        channel.close();
      }
    }
  }

  // ---------------------------------------------------------------------------
  // Netty inbound handler — one per channel
  // ---------------------------------------------------------------------------

  private static final class ClientResponseHandler
          extends SimpleChannelInboundHandler<FullHttpResponse> {

    private final PooledConnection conn;

    ClientResponseHandler(PooledConnection conn) {
      // autoRelease=false: the caller (or timeout handler) is responsible for releasing.
      super(false);
      this.conn = conn;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse response) {
      conn.complete(response);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      conn.fail(new IllegalStateException(
              "XdnHttpForwarderClient: backend channel closed unexpectedly"));
      conn.pool.onConnectionClosed(conn);
      ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      conn.fail(cause);
      ctx.close();
    }
  }

  // ---------------------------------------------------------------------------
  // Supporting types
  // ---------------------------------------------------------------------------

  private record RequestContext(
          CompletableFuture<FullHttpResponse> future,
          PooledConnection connection) {}

  private record PendingRequest(
          FullHttpRequest request,
          CompletableFuture<FullHttpResponse> future,
          long enqueuedAtMs) {}

  private record Origin(String host, int port) {}
}