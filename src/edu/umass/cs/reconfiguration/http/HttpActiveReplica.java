package edu.umass.cs.reconfiguration.http;

import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ActiveReplicaFunctions;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.XdnGigapaxosApp;
import edu.umass.cs.xdn.XdnHttpForwarderClient;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.security.cert.CertificateException;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * HTTP front-end for an active replica.
 *
 * Design:
 * 1. HttpActiveReplica receives incoming HTTP requests via Netty.
 * 2. Each request is immediately forwarded to the Coordinator via handRequestToAppForHttp().
 * 3. The ChannelHandlerContext (i.e., the client connection) is stored in a pending-request map,
 *    keyed by request ID, so the Netty thread is never blocked.
 * 4. When the Coordinator finishes, it calls back via ExecutedCallback. The callback looks up
 *    the pending map by request ID, retrieves the ChannelHandlerContext, and writes the response.
 * 5. A per-request scheduled timeout removes stale entries from the map and sends a 504 response
 *    to the client if no response arrives within REQUEST_TIMEOUT_SECONDS.
 */
public class HttpActiveReplica {

    private static final Logger logger = Logger.getLogger(HttpActiveReplica.class.getName());

    public static final String XDN_HOST_DOMAIN = "xdnapp.com";

    private static final int DEFAULT_HTTP_PORT = 8080;
    private static final String DEFAULT_HTTP_ADDR = "0.0.0.0";
    private static final String HTTP_ADDR_ENV_KEY = "HTTPADDR";

    // How long (in seconds) to wait for a response before sending 504 to client.
    private static final int REQUEST_TIMEOUT_SECONDS = 10;

    // ---------------------------------------------------------------------------
    // Debug backdoor headers — for latency measurement and bottleneck isolation.
    // These headers are never expected in production traffic.
    //
    //  ___DDR   Dummy response: skip execution entirely, reply immediately.
    //           Measures the raw HTTP receive cost of HttpActiveReplica.
    //  ___DBC   Bypass coordination: forward directly to the docker container.
    //           Requires the container's exposed port in ___DBCP.
    //           Measures the cost of the coordination stack.
    //  ___DDE   Direct execute: call XdnGigapaxosApp.execute() directly,
    //           skipping the coordinator but still using the app.
    // ---------------------------------------------------------------------------
    static final String DBG_HDR_DUMMY_RESPONSE        = "___DDR";
    static final String DBG_HDR_BYPASS_COORDINATION   = "___DBC";
    static final String DBG_HDR_BYPASS_COORDINATION_PORT = "___DBCP";
    static final String DBG_HDR_BYPASS_COORDINATION_USE_JDK = "___DBCJ";
    static final String DBG_HDR_DIRECT_EXECUTE        = "___DDE";

    // Forwarder client for DBG_HDR_BYPASS_COORDINATION.
    static final XdnHttpForwarderClient debugHttpClient =
            new XdnHttpForwarderClient.Builder()
                    .minConnections(512)
                    .maxConnections(512)
                    .idleTimeoutMs(60_000)
                    .queueTimeoutMs(10_000)
                    .build();

    // New HttpClient-based forwarder
    private static final java.net.http.HttpClient debugHttpClientJdk =
            java.net.http.HttpClient.newBuilder()
                    .version(java.net.http.HttpClient.Version.HTTP_1_1)
                    .executor(Executors.newVirtualThreadPerTaskExecutor())
                    .connectTimeout(Duration.ofSeconds(5))
                    .build();

    // Set this reference before using DBG_HDR_DIRECT_EXECUTE.
    public static XdnGigapaxosApp debugAppReference = null;

    // ---------------------------------------------------------------------------
    // Pending request map: requestId -> PendingRequest.
    // Populated before handing off to the Coordinator; cleared on response or timeout.
    // ---------------------------------------------------------------------------
    private final ConcurrentHashMap<Long, PendingRequest> pendingRequests =
            new ConcurrentHashMap<>();

    private final ScheduledExecutorService timeoutScheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "har-timeout-scheduler");
                t.setDaemon(true);
                return t;
            });

    // ---------------------------------------------------------------------------

    public HttpActiveReplica(String nodeId,
                             ActiveReplicaFunctions arf,
                             InetSocketAddress sockAddr,
                             boolean ssl)
            throws CertificateException, SSLException, InterruptedException {

        assert nodeId != null : "Node ID cannot be null";

        // Configure SSL.
        final SslContext sslCtx;
        if (ssl) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(ssc.certificate(), ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        // Boss group: accepts connections.
        // Worker group: handles I/O. Size is tuned to CPU count for throughput.
        EventLoopGroup bossGroup  = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        System.out.println("workerGroup threads: " + ((NioEventLoopGroup)workerGroup).executorCount());

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    // Backlog: how many connections can queue while all workers are busy.
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    // AUTO_READ left true; flow control can be added later if needed.
                    .childOption(ChannelOption.AUTO_READ, true)
                    .childHandler(new HttpActiveReplicaInitializer(
                            nodeId, arf, sslCtx, pendingRequests, timeoutScheduler));

            if (sockAddr == null) {
                String addr = System.getProperty(HTTP_ADDR_ENV_KEY, DEFAULT_HTTP_ADDR);
                sockAddr = new InetSocketAddress(addr, DEFAULT_HTTP_PORT);
            }

            // Always listen on all interfaces so non-loopback clients can reach us.
            if (sockAddr.getAddress().isLoopbackAddress()) {
                sockAddr = new InetSocketAddress(DEFAULT_HTTP_ADDR, sockAddr.getPort());
            }

            if (Config.getGlobalBoolean(ReconfigurationConfig.RC.ENABLE_ACTIVE_REPLICA_HTTP_PORT_80)) {
                sockAddr = new InetSocketAddress(sockAddr.getAddress(), 80);
            }

            Channel channel = b.bind(sockAddr).sync().channel();
            logger.log(Level.INFO, "HttpActiveReplica is ready on {0}", sockAddr);
            channel.closeFuture().sync();

        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            timeoutScheduler.shutdown();
        }
    }

    // ---------------------------------------------------------------------------
    // PendingRequest: bundles everything needed to write a response once the
    // Coordinator calls back.
    // ---------------------------------------------------------------------------
    static final class PendingRequest {
        final ChannelHandlerContext ctx;
        final boolean isKeepAlive;
        final long startNs;

        PendingRequest(ChannelHandlerContext ctx, boolean isKeepAlive) {
            this.ctx       = ctx;
            this.isKeepAlive = isKeepAlive;
            this.startNs   = System.nanoTime();
        }
    }

    // ---------------------------------------------------------------------------
    // Channel initializer
    // ---------------------------------------------------------------------------
    private static class HttpActiveReplicaInitializer extends ChannelInitializer<SocketChannel> {

        private final String nodeId;
        private final ActiveReplicaFunctions arf;
        private final SslContext sslCtx;
        private final ConcurrentHashMap<Long, PendingRequest> pendingRequests;
        private final ScheduledExecutorService timeoutScheduler;

        HttpActiveReplicaInitializer(String nodeId,
                                     ActiveReplicaFunctions arf,
                                     SslContext sslCtx,
                                     ConcurrentHashMap<Long, PendingRequest> pendingRequests,
                                     ScheduledExecutorService timeoutScheduler) {
            this.nodeId           = nodeId;
            this.arf              = arf;
            this.sslCtx           = sslCtx;
            this.pendingRequests  = pendingRequests;
            this.timeoutScheduler = timeoutScheduler;
        }

        @Override
        protected void initChannel(SocketChannel ch) {
            ChannelPipeline p = ch.pipeline();

            if (sslCtx != null) {
                p.addLast(sslCtx.newHandler(ch.alloc()));
            }

            // Close connections that have been idle for 30 s (no incoming bytes).
            p.addLast(new ReadTimeoutHandler(30));

            // HTTP codec + aggregation into FullHttpRequest (max 1 MiB body).
            p.addLast(new HttpServerCodec());
            p.addLast(new HttpObjectAggregator(1 << 20));

            // Permissive CORS — tighten per-deployment if needed.
            CorsConfig corsConfig = CorsConfigBuilder.forAnyOrigin().allowNullOrigin().build();
            p.addLast(new CorsHandler(corsConfig));

            // Main business-logic handler.
            p.addLast(new HttpActiveReplicaHandler(
                    nodeId, arf, pendingRequests, timeoutScheduler, ch.remoteAddress()));
        }
    }

    // ---------------------------------------------------------------------------
    // Main inbound handler — one instance per channel (not sharable).
    // Extends SimpleChannelInboundHandler<FullHttpRequest> so Netty releases the
    // request ByteBuf automatically after channelRead0 returns.
    // ---------------------------------------------------------------------------
    private static class HttpActiveReplicaHandler
            extends SimpleChannelInboundHandler<FullHttpRequest> {

        private final String nodeId;
        private final ActiveReplicaFunctions arf;
        private final ConcurrentHashMap<Long, PendingRequest> pendingRequests;
        private final ScheduledExecutorService timeoutScheduler;
        private final InetSocketAddress senderAddr;

        HttpActiveReplicaHandler(String nodeId,
                                 ActiveReplicaFunctions arf,
                                 ConcurrentHashMap<Long, PendingRequest> pendingRequests,
                                 ScheduledExecutorService timeoutScheduler,
                                 InetSocketAddress senderAddr) {
            this.nodeId           = nodeId;
            this.arf              = arf;
            this.pendingRequests  = pendingRequests;
            this.timeoutScheduler = timeoutScheduler;
            this.senderAddr       = senderAddr;
        }

        // ------------------------------------------------------------------
        // Main dispatch
        // ------------------------------------------------------------------
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
            // Reject malformed requests immediately.
            if (!msg.decoderResult().isSuccess()) {
                sendResponse(ctx, BAD_REQUEST, "Malformed HTTP request", false);
                return;
            }

            boolean isKeepAlive = HttpUtil.isKeepAlive(msg);

            // 100-Continue handshake.
            if (HttpUtil.is100ContinueExpected(msg)) {
                ctx.writeAndFlush(new DefaultFullHttpResponse(HTTP_1_1, CONTINUE,
                        Unpooled.EMPTY_BUFFER));
            }

            // Route to XDN path or return 400 if service name cannot be inferred.
            if (!isXdnRequest(msg)) {
                sendResponse(ctx, BAD_REQUEST,
                        "Unrecognized request: missing XDN header or xdnapp.com host.", isKeepAlive);
                return;
            }

            // --- Debug shortcuts (never reached in normal production traffic) ---
            if (msg.headers().contains(DBG_HDR_DUMMY_RESPONSE)) {
                handleDummyResponse(ctx, isKeepAlive);
                return;
            }
            if (msg.headers().contains(DBG_HDR_DIRECT_EXECUTE)) {
                handleDirectExecute(ctx, msg, isKeepAlive);
                return;
            }
            if (msg.headers().contains(DBG_HDR_BYPASS_COORDINATION)) {
                handleBypassCoordination(ctx, msg, isKeepAlive);
                return;
            }
            // --- End debug shortcuts ---

            // Wrap into XdnHttpRequest. Body bytes are eagerly copied in the constructor
            // so the Netty ByteBuf can be released safely after this method returns.
            HttpContent content = new DefaultLastHttpContent(msg.content().retain());
            XdnHttpRequest xdnRequest = new XdnHttpRequest(msg, content);
            xdnRequest.stamp(XdnHttpRequest.TS_RECEIVED);
            registerPendingRequest(ctx, xdnRequest, isKeepAlive);

            // Hand off to the Coordinator. This returns immediately; the callback
            // (XdnExecutedCallback) will fire asynchronously on a Coordinator thread.
            ReplicableClientRequest gpRequest = ReplicableClientRequest.wrap(xdnRequest);
            gpRequest.setClientAddress(senderAddr);

            xdnRequest.stamp(XdnHttpRequest.TS_FLUSHED);
            boolean accepted = arf.handRequestToAppForHttp(gpRequest,
                    new XdnExecutedCallback(nodeId, xdnRequest.getRequestID(),
                            pendingRequests));

            if (!accepted) {
                // Coordinator rejected the request (e.g., unknown service name).
                // Cancel the pending entry and reply immediately.
                PendingRequest pending = pendingRequests.remove(xdnRequest.getRequestID());
                if (pending != null) {
                    sendResponse(ctx, NOT_FOUND,
                            "Unknown service: " + xdnRequest.getServiceName(), isKeepAlive);
                }
            } else {
                // Update demand stats for reconfigurator (mirrors existing behaviour).
                arf.updateDemandStatsFromHttp(xdnRequest, senderAddr.getAddress());
            }
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            String msg = cause.getMessage() != null
                    ? cause.getMessage() : cause.getClass().getSimpleName();

            // Connection-reset by peer is normal — do not log as an error.
            boolean isConnectionReset = (cause instanceof SocketException)
                    && msg.contains("Connection reset");
            if (!isConnectionReset) {
                logger.log(Level.WARNING,
                        String.format("%s:HttpActiveReplica - exceptionCaught: %s", nodeId, msg),
                        cause);
            }

            if (ctx.channel().isActive()) {
                boolean isUnknownService = msg.contains("Unknown coordinator for name=");
                HttpResponseStatus status = isUnknownService ? NOT_FOUND : INTERNAL_SERVER_ERROR;
                sendResponse(ctx, status, "Error: " + msg, false);
            } else {
                ctx.close();
            }
        }

        // ------------------------------------------------------------------
        // Pending-request lifecycle
        // ------------------------------------------------------------------

        /**
         * Registers the request in the pending map and schedules a timeout that fires
         * if no response arrives within REQUEST_TIMEOUT_SECONDS.
         */
        private void registerPendingRequest(ChannelHandlerContext ctx,
                                            XdnHttpRequest xdnRequest,
                                            boolean isKeepAlive) {
            long requestId = xdnRequest.getRequestID();
            pendingRequests.put(requestId, new PendingRequest(ctx, isKeepAlive));

            timeoutScheduler.schedule(() -> {
                PendingRequest timedOut = pendingRequests.remove(requestId);
                if (timedOut == null) {
                    // Response already arrived — nothing to do.
                    return;
                }
                logger.log(Level.WARNING,
                        "{0}:HttpActiveReplica - request {1} timed out after {2}s",
                        new Object[]{nodeId, requestId, REQUEST_TIMEOUT_SECONDS});
                // Schedule the write back on the channel's EventLoop thread.
                timedOut.ctx.executor().execute(() ->
                        sendResponse(timedOut.ctx, GATEWAY_TIMEOUT,
                                "Request timed out", timedOut.isKeepAlive));
            }, REQUEST_TIMEOUT_SECONDS, TimeUnit.SECONDS);
        }

        // ------------------------------------------------------------------
        // XDN request detection
        // ------------------------------------------------------------------

        private static boolean isXdnRequest(HttpRequest request) {
            String xdnHeader = request.headers().get("XDN");
            if (xdnHeader != null && !xdnHeader.isEmpty()) {
                return true;
            }
            String host = request.headers().get(HttpHeaderNames.HOST);
            if (host != null) {
                String bare = host.split(":")[0];
                if (bare.endsWith(XDN_HOST_DOMAIN)) {
                    return true;
                }
            }
            return false;
        }

        // ------------------------------------------------------------------
        // Response helpers
        // ------------------------------------------------------------------

        /**
         * Write a plain-text response and, if keep-alive is off, close the channel.
         * Must be called from the channel's EventLoop thread.
         */
        static void sendResponse(ChannelHandlerContext ctx,
                                 HttpResponseStatus status,
                                 String body,
                                 boolean isKeepAlive) {
            ByteBuf content = Unpooled.copiedBuffer(body, CharsetUtil.UTF_8);
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, status, content);
            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH,
                    response.content().readableBytes());
            if (isKeepAlive) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            ChannelFuture cf = ctx.writeAndFlush(response);
            if (!isKeepAlive) {
                cf.addListener(ChannelFutureListener.CLOSE);
            }
        }

        /**
         * Write an HttpResponse (as produced by the container) back to the client.
         * Must be called from the channel's EventLoop thread.
         */
        static void writeHttpResponse(ChannelHandlerContext ctx,
                                      HttpResponse httpResponse,
                                      boolean isKeepAlive,
                                      long startNs,
                                      String nodeId,
                                      long requestId) {
            if (httpResponse == null) {
                logger.log(Level.WARNING,
                        "{0}:HttpActiveReplica - null response for request {1}, sending 500",
                        new Object[]{nodeId, requestId});
                sendResponse(ctx, INTERNAL_SERVER_ERROR, "Empty response from service", isKeepAlive);
                return;
            }

            ChannelFuture cf = ctx.writeAndFlush(httpResponse);
            cf.addListener((ChannelFutureListener) future -> {
                if (!future.isSuccess() && !(future.cause() instanceof ClosedChannelException)) {
                    logger.log(Level.WARNING,
                            nodeId + ":HttpActiveReplica - write failed for request "
                                    + requestId + ": " + future.cause());
                }
                if (startNs > 0) {
                    long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
                    logger.log(Level.FINE,
                            "{0}:HttpActiveReplica - request {1} completed in {2}ms",
                            new Object[]{nodeId, requestId, elapsedMs});
                }
                if (!isKeepAlive) {
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                            .addListener(ChannelFutureListener.CLOSE);
                }
            });
        }

        // ------------------------------------------------------------------
        // Debug handlers
        // ------------------------------------------------------------------

        /** ___DDR: reply immediately without any execution. */
        private void handleDummyResponse(ChannelHandlerContext ctx, boolean isKeepAlive) {
            ByteBuf content = Unpooled.copiedBuffer("DEBUG-OK", CharsetUtil.UTF_8);
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, OK, content);
            response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH,
                    response.content().readableBytes());
            writeHttpResponse(ctx, response, isKeepAlive, System.nanoTime(), nodeId, -1L);
        }

        /** ___DDE: call XdnGigapaxosApp.execute() directly, bypassing the coordinator. */
        private void handleDirectExecute(ChannelHandlerContext ctx,
                                         FullHttpRequest msg,
                                         boolean isKeepAlive) {
            if (debugAppReference == null) {
                sendResponse(ctx, INTERNAL_SERVER_ERROR,
                        "[DEBUG] debugAppReference is not set", isKeepAlive);
                return;
            }
            msg.headers().remove(DBG_HDR_DIRECT_EXECUTE);
            HttpContent content = new DefaultLastHttpContent(msg.content().retain());
            XdnHttpRequest xdnRequest = new XdnHttpRequest(msg, content);
            long startNs = System.nanoTime();
            long requestId = xdnRequest.getRequestID();

            CompletableFuture.runAsync(() -> {
                boolean executed = debugAppReference.execute(xdnRequest);
                if (!executed || xdnRequest.getHttpResponse() == null) {
                    throw new RuntimeException("[DEBUG] Direct execute failed or returned null");
                }
            }).whenComplete((ignored, err) -> {
                if (!ctx.channel().isActive()) return;
                ctx.executor().execute(() -> {
                    if (err != null) {
                        sendResponse(ctx, INTERNAL_SERVER_ERROR,
                                err.getMessage(), isKeepAlive);
                        return;
                    }
                    writeHttpResponse(ctx, xdnRequest.getHttpResponse(),
                            isKeepAlive, startNs, nodeId, requestId);
                });
            });
        }

        /** ___DBC: forward directly to the docker container, bypassing coordination. */
        private void handleBypassCoordination(ChannelHandlerContext ctx,
                                              FullHttpRequest msg,
                                              boolean isKeepAlive) {
            String portStr = msg.headers().get(DBG_HDR_BYPASS_COORDINATION_PORT);
            if (portStr == null) {
                sendResponse(ctx, BAD_REQUEST,
                        "[DEBUG] Missing header: " + DBG_HDR_BYPASS_COORDINATION_PORT, isKeepAlive);
                return;
            }

            int targetPort;
            try {
                targetPort = Integer.parseInt(portStr);
            } catch (NumberFormatException e) {
                sendResponse(ctx, BAD_REQUEST,
                        "[DEBUG] Invalid port: " + portStr, isKeepAlive);
                return;
            }

            msg.headers().remove(DBG_HDR_BYPASS_COORDINATION);
            msg.headers().remove(DBG_HDR_BYPASS_COORDINATION_PORT);

            long startNs = System.nanoTime();

            if (msg.headers().contains(DBG_HDR_BYPASS_COORDINATION_USE_JDK)) {
                msg.headers().remove(DBG_HDR_BYPASS_COORDINATION_USE_JDK);
                handleBypassCoordinationJdk(ctx, msg, isKeepAlive, targetPort, startNs);
            } else {
                handleBypassCoordinationNetty(ctx, msg, isKeepAlive, targetPort, startNs);
            }
        }

        /** Netty-based bypass — uses XdnHttpForwarderClient connection pool. */
        private void handleBypassCoordinationNetty(ChannelHandlerContext ctx,
                                                   FullHttpRequest msg,
                                                   boolean isKeepAlive,
                                                   int targetPort,
                                                   long startNs) {

            debugHttpClient.executeAsync("127.0.0.1", targetPort, msg)
                    .whenComplete((response, err) -> {
                        if (!ctx.channel().isActive()) {
                            if (response != null) ReferenceCountUtil.release(response);
                            return;
                        }
                        ctx.executor().execute(() -> {
                            if (err != null) {
                                sendResponse(ctx, INTERNAL_SERVER_ERROR,
                                        err.getMessage(), isKeepAlive);
                                return;
                            }
                            writeHttpResponse(ctx, response, isKeepAlive, startNs, nodeId, -1L);
                        });
                    });
        }

        /** JDK HttpClient-based bypass — uses virtual threads, no manual pool management. */
        private void handleBypassCoordinationJdk(ChannelHandlerContext ctx,
                                                 FullHttpRequest msg,
                                                 boolean isKeepAlive,
                                                 int targetPort,
                                                 long startNs) {
            // Build the outbound JDK request from the Netty FullHttpRequest.
            java.net.http.HttpRequest outbound =
                    XdnGigapaxosApp.createOutboundHttpRequest(msg, msg, targetPort);

            debugHttpClientJdk
                    .sendAsync(outbound, java.net.http.HttpResponse.BodyHandlers.ofByteArray())
                    .whenComplete((response, err) -> {
                        if (!ctx.channel().isActive()) return;
                        ctx.executor().execute(() -> {
                            if (err != null) {
                                sendResponse(ctx, INTERNAL_SERVER_ERROR,
                                        err.getMessage(), isKeepAlive);
                                return;
                            }
                            writeHttpResponse(
                                    ctx,
                                    XdnGigapaxosApp.toNettyHttpResponse(response),
                                    isKeepAlive,
                                    startNs,
                                    nodeId,
                                    -1L);
                        });
                    });
        }
    }

    // ---------------------------------------------------------------------------
    // Callback — invoked by the Coordinator on a non-Netty thread.
    // ---------------------------------------------------------------------------

    /**
     * Called when the Coordinator (and Docker execution) complete for a request.
     * Looks up the pending ChannelHandlerContext by request ID and writes the response
     * back on the channel's own EventLoop thread.
     */
    private static class XdnExecutedCallback
            implements edu.umass.cs.gigapaxos.interfaces.ExecutedCallback {

        private final String nodeId;
        private final long requestId;
        private final ConcurrentHashMap<Long, PendingRequest> pendingRequests;

        XdnExecutedCallback(String nodeId,
                            long requestId,
                            ConcurrentHashMap<Long, PendingRequest> pendingRequests) {
            this.nodeId          = nodeId;
            this.requestId       = requestId;
            this.pendingRequests = pendingRequests;
        }

        @Override
        public void executed(Request response, boolean handled) {
            PendingRequest pending = pendingRequests.remove(requestId);
            if (pending == null) return;

            if (!handled || !(response instanceof XdnHttpRequest xdnRequest)) {
                pending.ctx.executor().execute(() ->
                        HttpActiveReplicaHandler.sendResponse(
                                pending.ctx, INTERNAL_SERVER_ERROR,
                                "Request not handled by coordinator", pending.isKeepAlive));
                return;
            }

            HttpResponse httpResponse = xdnRequest.getHttpResponse();

            // Captures end of coordination
            xdnRequest.stamp(XdnHttpRequest.TS_RESPONSE);
            xdnRequest.logLatencyTrace(1000); // sample 1 in 1000

            pending.ctx.executor().execute(() ->
                    HttpActiveReplicaHandler.writeHttpResponse(
                            pending.ctx,
                            httpResponse,
                            pending.isKeepAlive,
                            pending.startNs,
                            nodeId,
                            requestId));
        }
    }
}