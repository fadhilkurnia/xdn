package edu.umass.cs.xdn;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.Objects;
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
 *   <li>Idle connections are held in a {@link LinkedBlockingQueue}. Acquiring one is an atomic
 *       {@code poll()} — no two threads can acquire the same connection.
 *   <li>A new connection is opened whenever a request arrives and the idle queue is empty,
 *       up to {@code maxConnections}.
 *   <li>When {@code maxConnections} is reached and no idle connection is available, the request
 *       is queued. Queued requests are served in FIFO order as connections free up.
 *   <li>Queued requests that are not served within {@code queueTimeoutMs} fail with a
 *       {@link TimeoutException}.
 *   <li>Connections that have been idle for longer than {@code idleTimeoutMs} are closed,
 *       keeping at least {@code minConnections} alive.
 * </ul>
 *
 * <p>Reference counting contract:
 * <ul>
 *   <li>{@link #executeAsync} calls {@code retain()} on the request before any async work.
 *   <li>On the happy path, Netty's HTTP encoder releases the buffer when it finishes writing.
 *       The write listener does NOT call release() on success.
 *   <li>On the error path (write failure, queue timeout, pool closed), the holder calls
 *       {@code release()} explicitly since the encoder never ran.
 * </ul>
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
 *               // use response, then:
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
    private final Bootstrap bootstrapTemplate;

    // ---------------------------------------------------------------------------
    // Pool map — one DynamicPool per origin (host+port).
    // ---------------------------------------------------------------------------
    private final ConcurrentMap<Origin, DynamicPool> pools = new ConcurrentHashMap<>();

    // Shared scheduler for idle reaping and queue timeouts.
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "xdn-forwarder-scheduler");
                t.setDaemon(true);
                return t;
            });

    // ---------------------------------------------------------------------------
    // Constructors / Builder
    // ---------------------------------------------------------------------------

    private XdnHttpForwarderClient(Builder builder, EventLoopGroup group, boolean manageGroup) {
        this.minConnections       = builder.minConnections;
        this.maxConnections       = builder.maxConnections;
        this.idleTimeoutMs        = builder.idleTimeoutMs;
        this.queueTimeoutMs       = builder.queueTimeoutMs;
        this.eventLoopGroup       = Objects.requireNonNull(group, "eventLoopGroup");
        this.manageEventLoopGroup = manageGroup;

        this.bootstrapTemplate = new Bootstrap()
                .group(eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5_000);

        long reaperIntervalMs = Math.max(1_000, idleTimeoutMs / 2);
        scheduler.scheduleAtFixedRate(
                this::reapIdleConnections, reaperIntervalMs, reaperIntervalMs, TimeUnit.MILLISECONDS);
    }

    /** Default client with its own EventLoopGroup. Suitable for standalone use. */
    public XdnHttpForwarderClient() {
        this(new Builder(), new NioEventLoopGroup(0), true);
    }

    /**
     * Client that shares an externally managed EventLoopGroup.
     * The caller is responsible for the group's lifecycle — this client will NOT shut it down.
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
        public Builder minConnections(int v)  { this.minConnections = v; return this; }

        /** Hard ceiling on open connections per origin. */
        public Builder maxConnections(int v)  { this.maxConnections = v; return this; }

        /** Milliseconds of idle time before a connection is closed. */
        public Builder idleTimeoutMs(long v)  { this.idleTimeoutMs = v; return this; }

        /**
         * Milliseconds a queued request will wait for a free connection
         * before failing with {@link TimeoutException}.
         */
        public Builder queueTimeoutMs(long v) { this.queueTimeoutMs = v; return this; }

        /** Share an external EventLoopGroup (lifecycle managed by the caller). */
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
     * <p>Returns immediately. The returned future completes on a Netty EventLoop thread when the
     * container responds. For significant post-processing, dispatch to a separate executor to
     * avoid blocking the EventLoop.
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
        scheduler.shutdown();
        pools.values().forEach(DynamicPool::close);
        pools.clear();
        if (manageEventLoopGroup) {
            eventLoopGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    // ---------------------------------------------------------------------------
    // Idle reaper
    // ---------------------------------------------------------------------------

    private void reapIdleConnections() {
        pools.values().forEach(p -> p.reapIdle(idleTimeoutMs, minConnections));
    }

    // ---------------------------------------------------------------------------
    // DynamicPool
    // ---------------------------------------------------------------------------

    /**
     * Manages connections to a single origin.
     *
     * <p>Key invariants:
     * <ul>
     *   <li>{@code totalConnections} is incremented before a connection slot is used and
     *       decremented when a connection is permanently closed. It acts as a reservation
     *       counter — {@code getAndIncrement() < maxConnections} atomically reserves a slot.
     *   <li>{@code idleConnections} holds connections ready to accept a new request.
     *       {@code poll()} is the only way to acquire one — this is inherently race-free.
     *   <li>{@code allConnections} tracks every connection for lifecycle management (reaping,
     *       shutdown). It is never iterated for idle acquisition.
     * </ul>
     */
    private final class DynamicPool {

        private final Origin origin;

        // Idle connections available for immediate use. poll() is the atomic acquire operation.
        private final LinkedBlockingQueue<PooledConnection> idleConnections =
                new LinkedBlockingQueue<>();

        // All connections: idle + in-use + connecting. Used for reaping and shutdown only.
        private final ConcurrentLinkedQueue<PooledConnection> allConnections =
                new ConcurrentLinkedQueue<>();

        // Reservation counter. Incremented before opening, decremented on permanent close.
        private final AtomicInteger totalConnections = new AtomicInteger(0);

        // Requests waiting for a free connection.
        private final ConcurrentLinkedQueue<PendingRequest> waitQueue = new ConcurrentLinkedQueue<>();

        DynamicPool(Origin origin) {
            this.origin = origin;
            // Pre-warm minimum connections to avoid handshake cost on first request burst.
            for (int i = 0; i < minConnections; i++) {
                openNewConnection();
            }
        }

        /**
         * Submits a request. Returns immediately.
         *
         * <p>Acquires an idle connection atomically via {@code poll()}, opens a new one if under
         * the limit, or enqueues the request if at the limit.
         */
        CompletableFuture<FullHttpResponse> submit(FullHttpRequest request) {
            CompletableFuture<FullHttpResponse> future = new CompletableFuture<>();

            // Retain once here. Ownership transfers to the HTTP encoder on the happy path.
            // Any path that does not reach send() must call release() explicitly.
            ReferenceCountUtil.retain(request);

            // Fast path: grab an idle connection atomically — exactly one thread gets each slot.
            PooledConnection idle = idleConnections.poll();
            if (idle != null) {
                idle.send(request, future);
                return future;
            }

            // No idle connection — try to reserve a new connection slot atomically.
            // getAndIncrement() returns the pre-increment value, so < maxConnections means
            // this thread successfully reserved a slot.
            int slot = totalConnections.getAndIncrement();
            if (slot < maxConnections) {
                // Slot reserved — enqueue first, then open. The connection drains the queue on connect.
                waitQueue.add(new PendingRequest(request, future, System.currentTimeMillis()));
                openNewConnection();
                return future;
            }

            // At the limit — undo the reservation and queue with a timeout.
            totalConnections.decrementAndGet();
            waitQueue.add(new PendingRequest(request, future, System.currentTimeMillis()));
            scheduleQueueTimeout(request, future);
            return future;
        }

        /**
         * Schedules a timeout that fails the future and releases the buffer if no connection
         * becomes available within {@code queueTimeoutMs}.
         */
        private void scheduleQueueTimeout(FullHttpRequest request,
                                          CompletableFuture<FullHttpResponse> future) {
            scheduler.schedule(() ->
                            waitQueue.removeIf(pending -> {
                                if (pending.future == future && !future.isDone()) {
                                    // Never reached send() — release the retain from submit().
                                    ReferenceCountUtil.release(request);
                                    future.completeExceptionally(new TimeoutException(
                                            "Queued request timed out after " + queueTimeoutMs + "ms"));
                                    return true;
                                }
                                return false;
                            }),
                    queueTimeoutMs, TimeUnit.MILLISECONDS);
        }

        /**
         * Opens a new TCP connection asynchronously.
         * The slot in {@code totalConnections} must already be reserved by the caller.
         */
        private void openNewConnection() {
            PooledConnection conn = new PooledConnection(this);
            allConnections.add(conn);

            bootstrapTemplate.clone()
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
                            // Add to the idle queue before draining — the queued request can use us directly.
                            idleConnections.offer(conn);
                            drainWaitQueue();
                        } else {
                            LOG.log(Level.WARNING,
                                    "XdnHttpForwarderClient: failed to connect to " + origin, cf.cause());
                            allConnections.remove(conn);
                            totalConnections.decrementAndGet();
                            drainWaitQueue();
                        }
                    });
        }

        /**
         * Dispatches queued requests to idle connections.
         *
         * <p>Both {@code waitQueue.remove()} and {@code idleConnections.poll()} are atomic, so
         * no two threads can dispatch the same request to the same connection.
         */
        void drainWaitQueue() {
            while (true) {
                PendingRequest pending = waitQueue.peek();
                if (pending == null) break;

                // Discard already-completed (timed-out) entries without consuming an idle connection.
                if (pending.future.isDone()) {
                    waitQueue.poll();
                    continue;
                }

                // Atomically acquire an idle connection.
                PooledConnection idle = idleConnections.poll();
                if (idle == null) break; // No idle connection available right now.

                // Atomically remove the pending request.
                // If remove() returns false, the request was timed out between peek() and now.
                // Return the idle connection so it can serve a different request.
                if (waitQueue.remove(pending)) {
                    idle.send(pending.request, pending.future);
                } else {
                    idleConnections.offer(idle);
                }
            }
        }

        /**
         * Called when a backend channel closes unexpectedly.
         * Removes the connection and opens a replacement if there are queued requests.
         */
        void onConnectionClosed(PooledConnection conn) {
            allConnections.remove(conn);
            idleConnections.remove(conn); // no-op if already in-use
            totalConnections.decrementAndGet();

            // Open a replacement if there are queued requests and we have capacity.
            if (!waitQueue.isEmpty() && totalConnections.getAndIncrement() < maxConnections) {
                openNewConnection();
            } else {
                totalConnections.decrementAndGet();
            }
        }

        /**
         * Closes connections that have been idle longer than {@code idleTimeoutMs},
         * keeping at least {@code minKeep} alive.
         */
        void reapIdle(long idleTimeoutMs, int minKeep) {
            int active = totalConnections.get();
            Iterator<PooledConnection> it = idleConnections.iterator();
            while (it.hasNext() && active > minKeep) {
                PooledConnection conn = it.next();
                if (conn.idleForMs() > idleTimeoutMs) {
                    it.remove();
                    allConnections.remove(conn);
                    totalConnections.decrementAndGet();
                    conn.close();
                    active--;
                }
            }
        }

        /** Shuts down the pool, failing all queued requests and closing all connections. */
        void close() {
            PendingRequest pending;
            while ((pending = waitQueue.poll()) != null) {
                // Never reached send() — release the retain from submit().
                ReferenceCountUtil.release(pending.request);
                pending.future.completeExceptionally(
                        new IllegalStateException("XdnHttpForwarderClient closed"));
            }
            allConnections.forEach(PooledConnection::close);
            allConnections.clear();
            idleConnections.clear();
            totalConnections.set(0);
        }
    }

    // ---------------------------------------------------------------------------
    // PooledConnection
    // ---------------------------------------------------------------------------

    private static final class PooledConnection {

        private final DynamicPool pool;

        // Set once by attach() after TCP connect succeeds.
        private volatile Channel channel = null;

        // Non-null while a request is in flight. Null means idle.
        private final AtomicReference<RequestContext> inFlight = new AtomicReference<>();

        // Timestamp of when this connection last became idle, for reaper use.
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

        long idleForMs() {
            return System.currentTimeMillis() - idleSinceMs;
        }

        /**
         * Sends a request on this connection.
         *
         * <p>Refcount: the caller (submit) already called retain(). On success the encoder
         * releases the buffer. On write failure we release it explicitly here.
         */
        void send(FullHttpRequest request, CompletableFuture<FullHttpResponse> future) {
            RequestContext ctx = new RequestContext(future, this);
            if (!inFlight.compareAndSet(null, ctx)) {
                // Should never happen — connections are only obtained via idleConnections.poll().
                ReferenceCountUtil.release(request);
                future.completeExceptionally(
                        new IllegalStateException("BUG: send() called on a non-idle connection"));
                return;
            }

            channel.writeAndFlush(request).addListener((ChannelFutureListener) wf -> {
                if (!wf.isSuccess()) {
                    // Encoder never ran — we must release the retain from submit().
                    ReferenceCountUtil.release(request);
                    fail(wf.cause());
                }
                // On success: encoder already released the buffer. Do NOT call release() here.
            });
        }

        /**
         * Called by {@link ClientResponseHandler} when a response arrives.
         * Completes the future, returns to idle, and drains the wait queue.
         */
        void complete(FullHttpResponse response) {
            RequestContext ctx = inFlight.getAndSet(null);
            idleSinceMs = System.currentTimeMillis();

            if (ctx != null) {
                boolean delivered = ctx.future.complete(response);
                if (!delivered) {
                    // Future already completed (timed out) — we own the response now, release it.
                    ReferenceCountUtil.release(response);
                }
            } else {
                ReferenceCountUtil.release(response);
                LOG.warning("XdnHttpForwarderClient: response arrived with no in-flight context");
            }

            // Return to idle queue *before* draining so the next queued request can use us.
            pool.idleConnections.offer(this);
            pool.drainWaitQueue();
        }

        /**
         * Called on write failure or unexpected channel close.
         * Completes the future exceptionally and closes the channel.
         * A failed connection is never returned to the idle queue.
         */
        void fail(Throwable cause) {
            RequestContext ctx = inFlight.getAndSet(null);
            if (ctx != null) {
                ctx.future.completeExceptionally(cause);
            }
            close();
        }

        void close() {
            if (channel != null && channel.isOpen()) {
                channel.close();
            }
        }
    }

    // ---------------------------------------------------------------------------
    // Netty inbound handler — one instance per channel, always on the EventLoop thread
    // ---------------------------------------------------------------------------

    private static final class ClientResponseHandler
            extends SimpleChannelInboundHandler<FullHttpResponse> {

        private final PooledConnection conn;

        ClientResponseHandler(PooledConnection conn) {
            // autoRelease=false: response lifecycle is managed by the caller of executeAsync.
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
    // Value types
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