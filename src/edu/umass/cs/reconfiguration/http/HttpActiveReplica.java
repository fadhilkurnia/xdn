package edu.umass.cs.reconfiguration.http;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.primarybackup.packets.ChangePrimaryPacket;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ActiveReplicaFunctions;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.XdnGigapaxosApp;
import edu.umass.cs.xdn.XdnHttpForwarderClient;
import edu.umass.cs.xdn.XdnHttpRequestBatcher;
import edu.umass.cs.xdn.request.XdnGetReplicaInfoRequest;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.handler.codec.http.cors.CorsConfig;
import io.netty.handler.codec.http.cors.CorsConfigBuilder;
import io.netty.handler.codec.http.cors.CorsHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.handler.timeout.ReadTimeoutException;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.util.CharsetUtil;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.MultithreadEventExecutorGroup;
import org.json.JSONException;
import org.json.JSONObject;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

// Implemented endpoint:
// - GET    /api/v2/services/{serviceName}/replica/info                 // get the replica metadata
// - any    with `XDN`, `coordinator-request`, and `node-id` header     // change this node to be primary in primary backup
//          this will be deprecated and moved into PUT /api/v2/services/{serviceName}/coordinator endpoint
//          in the reconfigurator.

/**
 * An HTTP front-end for an active replica that supports interaction
 * between a http client and this front-end.
 * To use this HTTP front-end, the underlying application use the request
 * type {@link HttpActiveReplicaRequest} or a type that extends {@link HttpActiveReplicaRequest}.
 * <p>
 * Loosely based on the HTTP Snoop server example from netty
 * documentation page:
 * https://github.com/netty/netty/blob/4.1/example/src/main/java/io/netty/example/http/snoop/HttpSnoopServerHandler.java
 * <p>
 * A similar implementation to {@link HttpReconfigurator}
 * <p>
 * Example command:
 * <p>
 * curl -X POST localhost -d '{NAME:"XDNApp0", QID:0, COORD: true, QVAL: "1", type: 400}' -H "Content-Type: application/json"
 * <p>
 * Or open your browser to interact with this http front end directly
 * <p>
 * Start ActiveReplica with HttpActiveReplica:
 * java -ea -cp jars/gigapaxos-1.0.08.jar -Djava.util.logging.config.file=conf/logging.properties \
 * -Dlog4j.configuration=conf/log4j.properties -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.trustStorePassword=qwerty \
 * -Djavax.net.ssl.keyStore=conf/keyStore.jks -Djavax.net.ssl.trustStore=conf/trustStore.jks \
 * -DgigapaxosConfig=conf/xdn.local.properties -DHTTPADDR=127.0.0.1 -Dcontainer=localhost:3000 \
 * edu.umass.cs.reconfiguration.ReconfigurableNode AR0
 * <p>
 * Start HttpActiveReplica alone:
 * java -ea -cp jars/gigapaxos-1.0.08.jar -DHTTPADDR=127.0.0.1 -Dcontainer=localhost:3000 \
 * edu.umass.cs.reconfiguration.http.HttpActiveReplica
 *
 * @author gaozy
 */
public class HttpActiveReplica {

    private static final Logger logger = Logger.getLogger(HttpActiveReplica.class.getName());

    private final static int DEFAULT_HTTP_PORT = 8080;

    private final static String DEFAULT_HTTP_ADDR = "0.0.0.0";

    private final static String HTTP_ADDR_ENV_KEY = "HTTPADDR";

    public final static String XDN_HOST_DOMAIN = "xdnapp.com";

    // FIXME: used to indicate whether a single outstanding request has been executed, might go wrong when there are multiple outstanding requests
    static boolean finished;

    private static final boolean isHttpFrontendBatchEnabled =
            Config.getGlobalBoolean(ReconfigurationConfig.RC.HTTP_AR_FRONTEND_BATCH_ENABLED);
    private static final long httpFrontendRequestTimeoutMs =
            Config.getGlobalLong(ReconfigurationConfig.RC.HTTP_AR_FRONTEND_REQUEST_TIMEOUT_MS);

    // Backdoor headers used for debugging purpose (e.g., latency measurement to identify
    // bottlenecks). We assume these headers are never used by any services.
    //  - DBG_HDR_DUMMY_RESPONSE        : Skip execution and send dummy response.
    //                                    Used to measure HTTP receiver stack of XDN.
    //  - DBG_HDR_BYPASS_COORDINATION   : Bypass replica coordinator, and directly forward the
    //                                    request into containerized service. This requires knowing
    //                                    the exposed port from the container in
    //                                    DBG_HDR_BYPASS_COORDINATION_PORT.
    //  - DBG_HDR_DIRECT_EXECUTE        : Directly call execute() in XdnGigapaxosApp, ignoring
    //                                    replica coordinator but still using the App.
    private static final String DBG_HDR_DUMMY_RESPONSE = "___DDR";
    private static final String DBG_HDR_BYPASS_COORDINATION = "___DBC";
    private static final String DBG_HDR_BYPASS_COORDINATION_PORT = "___DBCP";
    private static final String DBG_HDR_DIRECT_EXECUTE = "___DDE";
    private static final int DBG_NUM_FORWARDER_CLIENTS = 128;
    private static final XdnHttpForwarderClient[] debugHttpClients =
            new XdnHttpForwarderClient[DBG_NUM_FORWARDER_CLIENTS];
    
    // Direct reference to the app for debugging purpose, needed for DBG_HDR_DIRECT_EXECUTE.
    // In normal execution, the app should be interacted with through the replica coordinator,
    // and this reference should not be used.
    public static XdnGigapaxosApp debugAppReference = null;

    // Debug flag: bypass coordinator/PBM but still use the async response path.
    // Tests whether the bottleneck is the coordinator/PBM pipeline or the async mechanism.
    private static final boolean BYPASS_COORDINATOR =
            Boolean.getBoolean("PB_BYPASS_COORDINATOR");

    // ── Pipeline profiler (enable via -DXDN_PIPELINE_PROFILER=true) ──
    private static final boolean PIPELINE_PROFILER_ENABLED =
            Boolean.getBoolean("XDN_PIPELINE_PROFILER");
    private static final java.util.concurrent.atomic.AtomicLong pipelineProfilerCount =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong pipelineProfilerSumParse =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong pipelineProfilerSumQueue =
            new java.util.concurrent.atomic.AtomicLong();
    private static final java.util.concurrent.atomic.AtomicLong pipelineProfilerSumDispatch =
            new java.util.concurrent.atomic.AtomicLong();

    static {
        // Initialize HTTP clients for debugging purposes.
        for (int i = 0; i < DBG_NUM_FORWARDER_CLIENTS; i++) {
            debugHttpClients[i] = new XdnHttpForwarderClient();
        }
    }

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
            sslCtx = SslContextBuilder.forServer(ssc.certificate(),
                    ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        // Initialize request batcher, if enabled.
        XdnHttpRequestBatcher requestBatcher = isHttpFrontendBatchEnabled ? 
            new XdnHttpRequestBatcher(arf) : null;

        // Initializing boss and worker event loops.
        // The boss workers are accepting connections, which then will be passed to the child
        // worker group (i.e., `workerGroup`).
        int bossThreads = Config.getGlobalInt(ReconfigurationConfig.RC.HTTP_AR_BOSS_THREADS);
        int workerThreads = Config.getGlobalInt(ReconfigurationConfig.RC.HTTP_AR_WORKER_THREADS);
        EventLoopGroup bossGroup = bossThreads > 0
                ? new NioEventLoopGroup(bossThreads) : new NioEventLoopGroup();
        EventLoopGroup workerGroup = workerThreads > 0
                ? new NioEventLoopGroup(workerThreads) : new NioEventLoopGroup();

        // Dedicated executor group for dispatching requests.
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors());

        // Pool 1: Writes — each thread only runs for microseconds (async dispatch setup),
        // so we need far fewer threads than the old blocking model.
        int writePoolMultiplier = Config.getGlobalInt(
                ReconfigurationConfig.RC.HTTP_AR_WRITE_POOL_MULTIPLIER);
        int writePoolSize = writePoolMultiplier > 0
                ? threads * writePoolMultiplier : threads;
        DefaultEventExecutorGroup writePool =
                new DefaultEventExecutorGroup(writePoolSize);

        // Pool 2: Reads
        DefaultEventExecutorGroup readPool =
                new DefaultEventExecutorGroup((int) (threads * 0.4));

        logger.log(Level.INFO,
                "HttpActiveReplica pools: bossThreads={0} workerThreads={1} "
                        + "writePool={2} readPool={3}",
                new Object[]{
                        bossThreads > 0 ? bossThreads : "default(2*cores)",
                        workerThreads > 0 ? workerThreads : "default(2*cores)",
                        writePoolSize,
                        (int) (threads * 0.4)});

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.AUTO_READ, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(
                            new HttpActiveReplicaInitializer(
                                    nodeId, arf, readPool, writePool, requestBatcher, sslCtx)
                    );

            if (sockAddr == null) {
                String addr = DEFAULT_HTTP_ADDR;
                if (System.getProperty(HTTP_ADDR_ENV_KEY) != null) {
                    addr = System.getProperty(HTTP_ADDR_ENV_KEY);
                }
                sockAddr = new InetSocketAddress(addr, DEFAULT_HTTP_PORT);
            }

            // Note that we always use the DEFAULT_HTTP_ADDR (0.0.0.0) if the loop-back address
            // (e.g., 127.0.0.1 or localhost) is used so that we can listen to incoming HTTP
            // requests from all interfaces, including non-localhost ones.
            if (sockAddr.getAddress().isLoopbackAddress())
                sockAddr = new InetSocketAddress(DEFAULT_HTTP_ADDR, sockAddr.getPort());

            // Listen at port 80 if ENABLE_ACTIVE_REPLICA_HTTP_PORT_80 is true.
            if (Config.getGlobalBoolean(ReconfigurationConfig.RC.ENABLE_ACTIVE_REPLICA_HTTP_PORT_80)) {
                sockAddr = new InetSocketAddress(sockAddr.getAddress(), 80);
            }
            Channel channel = b.bind(sockAddr).sync().channel();

            logger.log(Level.INFO, "HttpActiveReplica is ready on {0}", new Object[]{sockAddr});

            channel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            readPool.shutdownGracefully();
            writePool.shutdownGracefully();
        }

    }


    private static class HttpActiveReplicaInitializer extends
            ChannelInitializer<SocketChannel> {

        private final String nodeId;
        private final ActiveReplicaFunctions arFunctions;
        private final MultithreadEventExecutorGroup readPool;
        private final MultithreadEventExecutorGroup writePool;
        private final XdnHttpRequestBatcher requestBatching;
        private final SslContext sslCtx;

        HttpActiveReplicaInitializer(String nodeId,
                                     final ActiveReplicaFunctions arf,
                                     final MultithreadEventExecutorGroup readPool,
                                     final MultithreadEventExecutorGroup writePool,
                                     final XdnHttpRequestBatcher requestBatching,
                                     SslContext sslCtx) {
            this.nodeId = nodeId;
            this.arFunctions = arf;
            this.readPool = readPool;
            this.writePool = writePool;
            this.requestBatching = requestBatching;
            this.sslCtx = sslCtx;
        }

        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline p = channel.pipeline();

            if (sslCtx != null)
                p.addLast(sslCtx.newHandler(channel.alloc()));

            // Timeout for truly stuck/abandoned connections.
            // Must be longer than the longest legitimate request processing time
            // (httpFrontendRequestTimeoutMs = 120s) because HTTP/1.1 clients send
            // no data while waiting for a response, and ReadTimeoutHandler fires
            // on absence of inbound data. A too-short value (e.g. 30s) kills
            // connections during normal PB replication under load.
            p.addLast(new ReadTimeoutHandler(180));

            // (de)serialize bytes to http
            p.addLast(new HttpServerCodec());

            // aggregate to FullHttpRequest
            p.addLast(new HttpObjectAggregator(1 << 20));

            // handle CORS
            CorsConfig corsConfig = CorsConfigBuilder.forAnyOrigin().build();
            p.addLast(new CorsHandler(corsConfig));

            // handle http request in separate executor group because execution
            // and coordination can take a long time
            p.addLast(new HttpActiveReplicaHandler(
                    nodeId,
                    arFunctions,
                    readPool,
                    writePool,
                    requestBatching,
                    channel.remoteAddress()));
        }
    }

    private static JSONObject getJSONObjectFromHttpContent(HttpContent httpContent) {
        ByteBuf content = httpContent.content();
        byte[] bytes;
        if (content.isReadable()) {
            bytes = new byte[content.readableBytes()];
            content.readBytes(bytes);
            logger.log(Level.FINE, "HttpContent: {0}", new Object[]{new String(bytes)});
        } else {
            return null;
        }

        try {
            return new JSONObject(new String(bytes));

        } catch (JSONException e) {
            return new JSONObject();
        }

    }

    /**
     * The json object must contain the following keys to be a valid request:
     * {@link HttpActiveReplicaRequest.Keys} NAME, QVAL
     * <p>
     * The other fields can be filled in with default values.
     */
    private static HttpActiveReplicaRequest getRequestFromJSONObject(JSONObject json) throws JSONException {
        if (!json.has(HttpActiveReplicaRequest.Keys.NAME.toString())) {
            throw new JSONException("missing key NAME");
        }
        if (!json.has(HttpActiveReplicaRequest.Keys.QVAL.toString())) {
            throw new JSONException("missing key QVAL");
        }

        String name = json.getString(HttpActiveReplicaRequest.Keys.NAME.toString());
        String qval = json.getString(HttpActiveReplicaRequest.Keys.QVAL.toString());

        // needsCoordination: default true
        boolean coord = !json.has(HttpActiveReplicaRequest.Keys.COORD.toString()) ||
                json.getBoolean(HttpActiveReplicaRequest.Keys.COORD.toString());

        int qid = (json.has(HttpActiveReplicaRequest.Keys.QID.toString()) ?
                json.getInt(HttpActiveReplicaRequest.Keys.QID.toString())
                : (int) (Math.random() * Integer.MAX_VALUE));

        int epoch = (json.has(HttpActiveReplicaRequest.Keys.EPOCH.toString())) ?
                json.getInt(HttpActiveReplicaRequest.Keys.EPOCH.toString())
                : 0;

        boolean stop = json.has(HttpActiveReplicaRequest.Keys.STOP.toString()) &&
                json.getBoolean(HttpActiveReplicaRequest.Keys.STOP.toString());


        return new HttpActiveReplicaRequest(HttpActiveReplicaPacketType.EXECUTE,
                name, qid, qval, coord, stop, epoch);
    }

    private static class HttpExecutedCallback implements ExecutedCallback {

        StringBuilder buf;
        final Object lock;
        // boolean finished;

        HttpExecutedCallback(StringBuilder buf, Object lock) {
            this.buf = buf;
            this.lock = lock;
        }

        @Override
        public void executed(Request response, boolean handled) {

            buf.append("RESPONSE:\n\r");
            buf.append(response);

            synchronized (lock) {
                finished = true;
                lock.notify();
            }
        }

    }

    // TODO: extend SimpleChannelInboundHandler<FullHttpRequest> instead.
    private static class HttpActiveReplicaHandler extends
            SimpleChannelInboundHandler<Object> {

        private static String nodeId = null;

        /** Single daemon thread for enforcing request timeouts without blocking writePool. */
        private static final ScheduledExecutorService timeoutScheduler =
                Executors.newSingleThreadScheduledExecutor(r -> {
                    Thread t = new Thread(r, "xdn-async-timeout");
                    t.setDaemon(true);
                    return t;
                });

        /**
         * Captures all state needed to write a response asynchronously from any thread
         * (callback, timeout, or channel-close listener).
         */
        private record ResponseContext(
                XdnHttpRequest httpRequest,
                ChannelHandlerContext ctx,
                boolean isKeepAlive,
                long startExecTimeNs,
                AtomicBoolean responded
        ) {}

        private final ActiveReplicaFunctions arFunctions;
        private final MultithreadEventExecutorGroup readPool;
        private final MultithreadEventExecutorGroup writePool;
        private final XdnHttpRequestBatcher requestBatching;
        private final InetSocketAddress senderAddr; // client's inet address

        private HttpRequest request;
        private HttpContent requestContent;

        /**
         * Returns true if the HTTP method is read-only according to the default
         * matcher defined in ServiceProperty.
         * It is also aligned with the safe methods defined in RFC 7231.
         * Reference: https://datatracker.ietf.org/doc/html/rfc7231#section-4.2.1
         *
         * This is used as a heuristic to classify requests into read vs. write pools,
         * so that write requests do not cause congestion that blocks read requests.
         */
        private static boolean isSafeMethod(HttpMethod method) {
            return method == HttpMethod.GET ||
                method == HttpMethod.HEAD ||
                method == HttpMethod.OPTIONS ||
                method == HttpMethod.TRACE;
        }

        /**
         * Thread-safe helper that writes an HTTP response (or error) to the Netty channel
         * from whichever thread fires first: the async callback, the timeout scheduler,
         * or the channel-close listener.  Exactly one caller succeeds via the AtomicBoolean
         * gate in ResponseContext.
         */
        private static void sendAsyncResponse(
                ResponseContext rctx,
                HttpResponse httpResponse,
                ScheduledFuture<?> timeoutTask,
                Throwable error) {
            if (!rctx.responded().compareAndSet(false, true)) {
                // Another path already handled this request (e.g., timeout fired first).
                // Release the response ByteBuf if provided, to prevent leaks.
                safeRelease(httpResponse);
                return;
            }

            // Cancel timeout if it hasn't fired yet
            if (timeoutTask != null) {
                timeoutTask.cancel(false);
            }



            XdnHttpRequest httpRequest = rctx.httpRequest();
            ChannelHandlerContext ctx = rctx.ctx();

            // Channel already closed — just release ByteBufs
            if (!ctx.channel().isActive()) {
                safeRelease(httpResponse);
                safeRelease(httpRequest.getHttpRequest());
                safeRelease(httpRequest.getHttpRequestContent());
                return;
            }

            // Write response directly — ctx.writeAndFlush() is thread-safe in Netty,
            // so we skip the event loop dispatch (ctx.executor().execute()) that added
            // unnecessary scheduling latency (~0.5ms per request).
            try {
                if (error != null) {
                    String msg = error.getMessage() != null
                            ? error.getMessage() : error.getClass().getSimpleName();
                    logger.log(Level.WARNING,
                            "Async request error: {0}", new Object[]{msg});
                    HttpResponseStatus status =
                            (error instanceof TimeoutException)
                                    ? SERVICE_UNAVAILABLE
                                    : INTERNAL_SERVER_ERROR;
                    HttpActiveReplicaHandler.sendStringResponse(
                            msg, status, false, ctx);
                } else {
                    if (edu.umass.cs.xdn.XdnGigapaxosApp.TIMING_HEADERS_ENABLED
                            && httpResponse != null) {
                        long callbackElapsedMs = (System.nanoTime() - rctx.startExecTimeNs()) / 1_000_000;
                        httpResponse.headers().set("X-XDN-Pipeline",
                                String.format("callback=%dms", callbackElapsedMs));
                    }
                    HttpActiveReplicaHandler.writeHttpResponse(
                            httpRequest.getRequestID(),
                            httpResponse,
                            ctx,
                            rctx.isKeepAlive(),
                            rctx.startExecTimeNs());
                }
            } finally {
                safeRelease(httpRequest.getHttpRequest());
                safeRelease(httpRequest.getHttpRequestContent());
            }
        }

        /**
         * Safely releases a reference-counted object, ignoring if already released.
         */
        private static void safeRelease(Object obj) {
            try {
                if (obj != null && ReferenceCountUtil.refCnt(obj) > 0) {
                    ReferenceCountUtil.release(obj);
                }
            } catch (Exception e) {
                // Already released or not ref-counted — ignore
            }
        }

        /**
         * Buffer that stores the response content
         */
        private final StringBuilder buf = new StringBuilder();

        HttpActiveReplicaHandler(String nodeId,
                                 ActiveReplicaFunctions arFunctions,
                                 MultithreadEventExecutorGroup readPool,
                                 MultithreadEventExecutorGroup writePool,
                                 XdnHttpRequestBatcher requestBatching,
                                 InetSocketAddress addr) {
            HttpActiveReplicaHandler.nodeId = nodeId;
            this.arFunctions = arFunctions;
            this.readPool = readPool;
            this.writePool = writePool;
            this.requestBatching = requestBatching;
            this.senderAddr = addr;
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg)
                throws Exception {

            // redirect handling to xdn, if either of these two conditions are met:
            // (1) the HttpRequest contains non-empty XDN header, or
            // (2) the HttpRequest contains Host header ending in "xdnapp.com".
            // Note that "Host" header is required since HTTP 1.1
            if (msg instanceof HttpRequest httpRequest) {
                boolean isXdnRequest = false;

                // Handle the first condition: contains XDN header
                String xdnHeader = httpRequest.headers().get("XDN");
                if (xdnHeader != null && !xdnHeader.isEmpty()) {
                    isXdnRequest = true;
                }

                // Handle the second condition: Host ending with "xdnapp.com"
                if (!isXdnRequest) {
                    String requestHost = httpRequest.headers().get(HttpHeaderNames.HOST);
                    if (requestHost != null) {
                        String[] hostPort = requestHost.split(":");
                        String host = hostPort[0];
                        if (host.endsWith(XDN_HOST_DOMAIN)) {
                            isXdnRequest = true;
                        }
                    }
                }

                if (isXdnRequest) {
                    handleReceivedXdnRequest(ctx, msg);
                    return;
                }
            }

            // The code below are for older Gigapaxos client. We keep it for backward compatibility,
            // but we are planning to deprecate that soon.
            // TODO: deprecate this.
            {
                // Request for GigaPaxos to coordinate
                HttpActiveReplicaRequest gRequest = null;

                // JSONObject to extract keys and values from http request
                JSONObject json = new JSONObject();

                //  This boolean is used to indicate whether the request has been retrieved.
                //  If request info is retrieved from HttpRequest, then don't bother to retrieve
                //  it from HttpContent. Otherwise, retrieve the info from HttpContent.
                //  If we still can't retrieve the info, then the request is a Malformed request.
                boolean retrieved = false;

                if (msg instanceof HttpRequest) {
                    HttpRequest httpRequest = this.request = (HttpRequest) msg;
                    buf.setLength(0);

                    if (HttpUtil.is100ContinueExpected(httpRequest)) {
                        send100Continue(ctx);
                    }

                    logger.log(Level.FINE,
                            "Http server received a request with HttpRequest: {0}",
                            new Object[]{httpRequest});

                    // Convert url query parameters into JSON key value pair
                    Map<String, List<String>> params =
                            (new QueryStringDecoder(httpRequest.uri())).parameters();
                    if (!params.isEmpty()) {
                        for (Entry<String, List<String>> p : params.entrySet()) {
                            String key = p.getKey();
                            List<String> vals = p.getValue();
                            for (String val : vals) {
                                // put the key-value pair into json
                                json.put(key.toUpperCase(), val);
                            }
                        }
                    }

                    if (json.length() > 0)
                        try {
                            gRequest = getRequestFromJSONObject(json);
                            logger.log(Level.INFO,
                                    "Http server retrieved an HttpActiveReplicaRequest " +
                                            "from HttpRequest: {0}", new Object[]{gRequest});
                            retrieved = true;
                        } catch (Exception e) {
                            // ignore and do nothing if this is a malformed request
                            e.printStackTrace();
                        }
                }

                if (msg instanceof HttpContent) {
                    if (!retrieved) {
                        HttpContent httpContent = (HttpContent) msg;
                        logger.log(Level.INFO,
                                "Http server received a request with HttpContent: {0}",
                                new Object[]{httpContent});
                        json = getJSONObjectFromHttpContent(httpContent);
                        if (json != null && json.length() > 0)
                            try {
                                gRequest = getRequestFromJSONObject(json);
                                retrieved = true;
                            } catch (Exception e) {
                                // TODO: A malformed request, we can send back the response here
                                e.printStackTrace();
                            }

                    }

                    if (msg instanceof LastHttpContent trailer) {
                        if (retrieved) {
                            logger.log(Level.INFO,
                                    "About to execute request: {0}", new Object[]{gRequest});
                            Object lock = new Object();
                            finished = false;
                            ExecutedCallback callback = new HttpExecutedCallback(buf, lock);

                            // execute GigaPaxos request here
                            if (arFunctions != null) {
                                logger.log(Level.FINE,
                                        "App {0} executes request: {1}",
                                        new Object[]{arFunctions, request});
                                boolean handled = arFunctions.handRequestToAppForHttp(
                                        (gRequest.needsCoordination())
                                                ? ReplicableClientRequest.wrap(gRequest) : gRequest,
                                        callback);

                                // This one below is inefficient, and probably incorrect.
                                while (!finished) {
                                    try {
                                        lock.wait(100);
                                    } catch (InterruptedException ignored) {

                                    }
                                }

                                //  If the request has been handled properly, then send demand
                                //  profile to RC. This logic follows the design of
                                //  (@link ActiveReplica}.
                                if (handled)
                                    arFunctions.updateDemandStatsFromHttp(
                                            gRequest, senderAddr.getAddress());
                            }

                        }

                        if (!trailer.trailingHeaders().isEmpty()) {
                            buf.append("\r\n");
                            for (CharSequence name : trailer.trailingHeaders()
                                    .names()) {
                                for (CharSequence value : trailer.trailingHeaders()
                                        .getAll(name)) {
                                    buf.append("TRAILING HEADER: ");
                                    buf.append(name).append(" = ").append(value)
                                            .append("\r\n");
                                }
                            }
                            buf.append("\r\n");
                        }
                        if (!writeResponse(trailer, ctx)) {
                            // If keep-alive is off, close the connection once the content is fully written.
                            ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
                        }
                    }

                }
            }
        }

        private void handleReceivedXdnRequest(ChannelHandlerContext ctx, Object msg) {

            if (msg instanceof HttpRequest) {
                this.request = (HttpRequest) msg;
                if (HttpUtil.is100ContinueExpected((HttpRequest) msg)) {
                    send100Continue(ctx);
                }
            }

            if (msg instanceof HttpContent content) {
                this.requestContent = new DefaultLastHttpContent(content.content());
            }

            if (msg instanceof LastHttpContent) {
                assert this.request != null;
                boolean isKeepAlive = HttpUtil.isKeepAlive(this.request);

                // Return http bad request response if service name is not specified
                String serviceName = XdnHttpRequest.inferServiceName(this.request);
                if (serviceName == null || serviceName.isEmpty()) {
                    HttpActiveReplicaHandler.sendBadRequestResponse(
                            "Unspecified service name." +
                                    "This can be cause because of a wrong Host or empty XDN header",
                            ctx
                    );
                    return;
                }
                System.err.printf(
                        "[XDN-DIAG] frontend-received localAddr=%s svc=%s uri=%s%n",
                        ctx.channel().localAddress(), serviceName, this.request.uri());

                // Check if this is a request to get replica info
                // GET /api/v2/services/{name}/replica/info
                String uri = this.request.uri();
                if (uri.startsWith("/api/v2/") &&
                        this.request.method() == HttpMethod.GET &&
                        uri.matches("/api/v2/services/[^/]+/replica/info")) {
                    // Extract service name from URI path
                    String[] parts = uri.split("/");
                    if (parts.length >= 5) {
                        String serviceNameFromPath = parts[4];
                        XdnGetReplicaInfoRequest xdnGetReplicaInfoRequest =
                                new XdnGetReplicaInfoRequest(serviceNameFromPath);
                        handleCoordinatorRequest(xdnGetReplicaInfoRequest, ctx);
                        return;
                    }
                    // If we can't extract service name, send bad request
                    HttpActiveReplicaHandler.sendBadRequestResponse(
                            "Invalid service name in URI path", ctx);
                    return;
                }

                // Handle debug requests, for measurement purposes.
                // Retain the netty objects because DBC and DDE execute off the event loop and
                // SimpleChannelInboundHandler will otherwise release them after this method returns.
                if (this.request.headers().contains(DBG_HDR_DUMMY_RESPONSE) ||
                        this.request.headers().contains(DBG_HDR_BYPASS_COORDINATION) ||
                        this.request.headers().contains(DBG_HDR_DIRECT_EXECUTE)) {
                    XdnHttpRequest debugRequest =
                            new XdnHttpRequest(this.request, this.requestContent);
                    ReferenceCountUtil.retain(debugRequest.getHttpRequest());
                    ReferenceCountUtil.retain(debugRequest.getHttpRequestContent());
                    this.request = null;
                    this.requestContent = null;
                    handleDebugRequests(debugRequest, ctx);
                    return;
                }

                // Instrumenting the request for latency measurement
                long startExecTimeNs = System.nanoTime();

                // For now, thread pool selection for each request is based on HTTP method heuristics,
                // where GET, HEAD, OPTIONS, and TRACE are considered read-only/safe methods.
                // TODO: Ideally, maybe, we should determine which pool this request should use based on
                // the behavior of the request instead of relying on HTTP method heuristics.
                MultithreadEventExecutorGroup selectedPool =
                        isSafeMethod(this.request.method()) ? this.readPool : this.writePool;

                XdnHttpRequest httpRequest =
                        new XdnHttpRequest(this.request, this.requestContent);
                // Retain the netty objects because we execute off the event loop and
                // SimpleChannelInboundHandler will otherwise release them after this method returns.
                ReferenceCountUtil.retain(httpRequest.getHttpRequest());
                ReferenceCountUtil.retain(httpRequest.getHttpRequestContent());
                this.request = null;
                this.requestContent = null;

                AtomicBoolean responded = new AtomicBoolean(false);
                ResponseContext rctx = new ResponseContext(
                        httpRequest, ctx, isKeepAlive, startExecTimeNs, responded);

                // Lazy timeout: only schedule if the request hasn't completed within
                // a short period. For most requests that complete in <100ms, this
                // avoids the cost of creating and cancelling a ScheduledFuture.
                ScheduledFuture<?> timeoutTask = null;
                if (httpFrontendRequestTimeoutMs > 0) {
                    timeoutTask = timeoutScheduler.schedule(() -> {
                        sendAsyncResponse(rctx, null, null,
                                new TimeoutException("Request timed out after "
                                        + httpFrontendRequestTimeoutMs + "ms"));
                    }, httpFrontendRequestTimeoutMs, TimeUnit.MILLISECONDS);
                }

                // If channel closes before completion, clean up
                final ScheduledFuture<?> capturedTimeout = timeoutTask;
                ctx.channel().closeFuture().addListener(cf -> {
                    if (responded.compareAndSet(false, true)) {
                        if (capturedTimeout != null) capturedTimeout.cancel(false);
                        safeRelease(httpRequest.getHttpRequest());
                        safeRelease(httpRequest.getHttpRequestContent());
                    }
                });

                // Lightweight dispatch — writePool thread returns in microseconds
                InetSocketAddress senderAddr = this.senderAddr;
                final long enqueueTimeNs = System.nanoTime();
                selectedPool.execute(() -> {
                    long dequeueTimeNs = System.nanoTime();
                    try {
                        if (isHttpFrontendBatchEnabled)
                            dispatchXdnHttpRequestViaBatching(
                                    httpRequest, senderAddr, rctx, capturedTimeout);
                        else
                            dispatchXdnHttpRequest(
                                    httpRequest, senderAddr, rctx, capturedTimeout);
                    } catch (Throwable t) {
                        sendAsyncResponse(rctx, null, capturedTimeout, t);
                    }
                    // ── Pipeline profiler ──
                    if (PIPELINE_PROFILER_ENABLED) {
                        long doneTimeNs = System.nanoTime();
                        long parseNs = enqueueTimeNs - startExecTimeNs;
                        long queueNs = dequeueTimeNs - enqueueTimeNs;
                        long dispatchNs = doneTimeNs - dequeueTimeNs;
                        pipelineProfilerSumParse.addAndGet(parseNs);
                        pipelineProfilerSumQueue.addAndGet(queueNs);
                        pipelineProfilerSumDispatch.addAndGet(dispatchNs);
                        long n = pipelineProfilerCount.incrementAndGet();
                        if (n % 1000 == 0) {
                            System.err.printf(
                                "[PIPELINE-PROFILER] n=%d avg parse=%.1fus queue=%.1fus dispatch=%.1fus total=%.1fus%n",
                                n,
                                pipelineProfilerSumParse.get() / 1000.0 / n,
                                pipelineProfilerSumQueue.get() / 1000.0 / n,
                                pipelineProfilerSumDispatch.get() / 1000.0 / n,
                                (pipelineProfilerSumParse.get() + pipelineProfilerSumQueue.get()
                                    + pipelineProfilerSumDispatch.get()) / 1000.0 / n);
                        }
                    }
                });
            }
        }

        /**
         * Non-blocking dispatch: hands the request to the replica coordinator via
         * callback. The writePool thread returns immediately after this call.
         */
        private void dispatchXdnHttpRequest(
                XdnHttpRequest httpRequest,
                InetSocketAddress clientInetSocketAddress,
                ResponseContext rctx,
                ScheduledFuture<?> timeoutTask) {

            // Debug bypass: execute directly via XdnGigapaxosApp, skip coordinator/PBM,
            // but still use the async response path (sendAsyncResponse).
            if (BYPASS_COORDINATOR && debugAppReference != null) {
                try {
                    boolean ok = debugAppReference.execute(httpRequest);
                    if (ok && httpRequest.getHttpResponse() != null) {
                        sendAsyncResponse(rctx, httpRequest.getHttpResponse(),
                                timeoutTask, null);
                    } else {
                        sendAsyncResponse(rctx, null, timeoutTask,
                                new RuntimeException("Bypass execute failed"));
                    }
                } catch (Throwable t) {
                    sendAsyncResponse(rctx, null, timeoutTask, t);
                }
                return;
            }

            // Create Gigapaxos' request, it is important to explicitly set the clientAddress,
            // otherwise, down the pipeline, the RequestPacket's equals method will return false
            // and our callback will not be called, leaving the client hanging
            // waiting for response.
            ReplicableClientRequest gpRequest = ReplicableClientRequest.wrap(httpRequest);
            gpRequest.setClientAddress(clientInetSocketAddress);

            this.arFunctions.handRequestToAppForHttp(gpRequest, (request, handled) -> {
                if (handled && request instanceof XdnHttpRequest xdnReq) {
                    sendAsyncResponse(rctx, xdnReq.getHttpResponse(), timeoutTask, null);
                } else if (handled) {
                    sendAsyncResponse(rctx, null, timeoutTask,
                            new RuntimeException(request == null
                                    ? "Request was handled but returned null"
                                    : "Unexpected response type: "
                                            + request.getClass().getSimpleName()));
                } else {
                    sendAsyncResponse(rctx, null, timeoutTask,
                            new RuntimeException("Request was not handled"));
                }
            });
        }

        /**
         * Non-blocking dispatch via batching: submits the request to the batcher which
         * fires the completion handler asynchronously. The writePool thread returns
         * immediately after this call.
         */
        private void dispatchXdnHttpRequestViaBatching(
                XdnHttpRequest httpRequest,
                InetSocketAddress clientInetSocketAddress,
                ResponseContext rctx,
                ScheduledFuture<?> timeoutTask) {
            requestBatching.submit(
                    httpRequest,
                    clientInetSocketAddress,
                    (completedRequest, error) -> {
                        if (error != null) {
                            sendAsyncResponse(rctx, null, timeoutTask, error);
                        } else {
                            sendAsyncResponse(
                                    rctx, completedRequest.getHttpResponse(), timeoutTask, null);
                        }
                    });
        }

        private void handleCoordinatorRequest(Request p, ChannelHandlerContext context) {
            assert p instanceof ChangePrimaryPacket || p instanceof XdnGetReplicaInfoRequest :
                    "Unexpected packet type of " + p.getClass().getSimpleName();

            if (p instanceof XdnGetReplicaInfoRequest) {
                arFunctions.handRequestToAppForHttp(p, (requestWithResponse, handled) -> {
                    assert requestWithResponse instanceof XdnGetReplicaInfoRequest;
                    assert handled : "Unhandled XdnGetReplicaInfoRequest";
                    XdnGetReplicaInfoRequest resp = (XdnGetReplicaInfoRequest) requestWithResponse;
                    if (resp.getErrorMessage() != null) {
                        int errCode = resp.getHttpErrorCode() == null
                                ? 500 : resp.getHttpErrorCode();
                        sendStringResponse(resp.getErrorMessage(),
                                HttpResponseStatus.valueOf(errCode),
                                false,
                                context);
                        return;
                    }
                    String responseString =
                            ((XdnGetReplicaInfoRequest) requestWithResponse).getJsonResponse();
                    sendStringResponse(responseString, OK, true, context);
                });
            }

            // TODO: cleanly handle this in Reconfigurator, instead of ActiveReplica
            if (p instanceof ChangePrimaryPacket) {
                arFunctions.handRequestToAppForHttp(p, (request, handled) -> {
                    sendStringResponse("OK\n", context, false);
                });
            }
        }

        private void handleDebugRequests(XdnHttpRequest httpRequest, ChannelHandlerContext ctx) {
            boolean isKeepAlive = HttpUtil.isKeepAlive(httpRequest.getHttpRequest());

            MultithreadEventExecutorGroup selectedPool =
                    isSafeMethod(httpRequest.getHttpRequest().method())
                            ? this.readPool
                            : this.writePool;
                            
            // Debug: send dummy response, the goal is to identify whether HttpActiveReplica
            //  is a bottleneck on receiving the incoming Http requests.
            if (httpRequest.getHttpRequest().headers().contains(DBG_HDR_DUMMY_RESPONSE)) {
                long startExecTimeNs = System.nanoTime();
                DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, OK,
                        Unpooled.copiedBuffer("DEBUG-OK", CharsetUtil.UTF_8));
                httpResponse.headers().setInt(
                        HttpHeaderNames.CONTENT_LENGTH,
                        httpResponse.content().readableBytes());
                HttpActiveReplicaHandler.writeHttpResponse(
                        httpRequest.getRequestID(),
                        httpResponse,
                        ctx,
                        isKeepAlive,
                        startExecTimeNs);
                ReferenceCountUtil.release(httpRequest.getHttpRequest());
                ReferenceCountUtil.release(httpRequest.getHttpRequestContent());
                return;
            }

            // Debug: directly execute the request using XdnGigapaxosApp, skip replica coordinator.
            if (httpRequest.getHttpRequest().headers().contains(DBG_HDR_DIRECT_EXECUTE)) {
                assert debugAppReference != null :
                        "XdnGigapaxosApp reference has not been set in HttpActiveReplica";
                long startExecTimeNs = System.nanoTime();

                httpRequest.getHttpRequest().headers().remove(DBG_HDR_DIRECT_EXECUTE);

                // Send and execute possibly long-running request in offload executor.
                Future<HttpResponse> execFuture =
                        selectedPool.submit(() -> {
                            boolean isExecuted = debugAppReference.execute(httpRequest);
                            assert isExecuted : "failed to executed HTTP request";
                            assert httpRequest.getHttpResponse() != null :
                                    "obtained null HTTP response";
                            return httpRequest.getHttpResponse();
                        });

                // If channel closes before completion, cancel the task
                ctx.channel().closeFuture().addListener(cf -> execFuture.cancel(true));

                // When done, switch back to the channel's EventLoop to write the response
                CompletableFuture.supplyAsync(() -> {
                            try {
                                return execFuture.get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }, selectedPool)
                        .whenComplete((httpResponse, err) -> {
                            // do nothing when the channel is inactive
                            if (!ctx.channel().isActive()) {
                                ReferenceCountUtil.release(httpRequest.getHttpRequest());
                                ReferenceCountUtil.release(httpRequest.getHttpRequestContent());
                                return;
                            }

                            // finally, handle the error or send the response
                            ctx.executor().execute(() -> {
                                try {
                                    // Error handling, send string message
                                    if (err != null) {
                                        System.out.println("Writing error response: " + err.getMessage());
                                        err.printStackTrace();
                                        HttpActiveReplicaHandler.sendStringResponse(
                                                err.getMessage(), INTERNAL_SERVER_ERROR, false, ctx);
                                        return;
                                    }

                                    // Success handling
                                    HttpActiveReplicaHandler.writeHttpResponse(
                                            httpRequest.getRequestID(),
                                            httpResponse,
                                            ctx,
                                            isKeepAlive,
                                            startExecTimeNs);
                                } finally {
                                    ReferenceCountUtil.release(httpRequest.getHttpRequest());
                                    ReferenceCountUtil.release(httpRequest.getHttpRequestContent());
                                }
                            });
                        });

                return;
            }

            // Debug: forward http request directly to the dockerized service. The goal is to
            //  identify the "rough" cost of coordination via the Gigapaxos/XDN stack.
            //  To make this work, please specify the docker's service port in the
            //  request header with DBG_HDR_BYPASS_COORDINATION (i.e., __DBCP).
            if (httpRequest.getHttpRequest().headers().contains(DBG_HDR_BYPASS_COORDINATION)) {
                int detectedTargetPort;

                String targetPortStr = httpRequest.getHttpRequest()
                        .headers().get(DBG_HDR_BYPASS_COORDINATION_PORT);
                if (targetPortStr != null) {
                    detectedTargetPort = Integer.parseInt(targetPortStr);
                } else {
                    detectedTargetPort = -1;
                }

                if (detectedTargetPort == -1) {
                    sendBadRequestResponse(
                            String.format(
                                    "[DEBUG] Expecting container port in the header with %s " +
                                            "as the key",
                                    DBG_HDR_BYPASS_COORDINATION_PORT),
                            ctx
                    );
                    return;
                }

                long startExecTimeNs = System.nanoTime();

                httpRequest.getHttpRequest().headers().remove(DBG_HDR_BYPASS_COORDINATION);
                httpRequest.getHttpRequest().headers().remove(DBG_HDR_BYPASS_COORDINATION_PORT);

                int forwarderClientIdx = Math.abs(ctx.channel().remoteAddress().hashCode() % DBG_NUM_FORWARDER_CLIENTS);
                XdnHttpForwarderClient debugHttpClient = debugHttpClients[forwarderClientIdx];

                // Send and execute possibly long-running request in offload executor.
                // Note: the caller (handleReceivedXdnRequest) already retains the request objects.
                Future<FullHttpResponse> execFuture = selectedPool.submit(() -> {
                    try {
                        return debugHttpClient.execute(
                                "127.0.0.1",
                                detectedTargetPort,
                                (FullHttpRequest) httpRequest.getHttpRequest());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                });

                // If channel closes before completion, cancel the task
                ctx.channel().closeFuture().addListener(cf -> execFuture.cancel(true));

                // When done, switch back to the channel's EventLoop to write the response
                CompletableFuture.supplyAsync(() -> {
                            try {
                                return execFuture.get();
                            } catch (InterruptedException | ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }, selectedPool)
                        .whenComplete((httpResponse, err) -> {
                            // do nothing when the channel is inactive
                            if (!ctx.channel().isActive()) {
                                ReferenceCountUtil.release(httpRequest.getHttpRequest());
                                ReferenceCountUtil.release(httpRequest.getHttpRequestContent());
                                return;
                            }

                            // finally, handle the error or send the response
                            ctx.executor().execute(() -> {
                                try {
                                    // Error handling, send string message
                                    if (err != null) {
                                        System.out.println("Writing error response: " + err.getMessage());
                                        err.printStackTrace();
                                        HttpActiveReplicaHandler.sendStringResponse(
                                                err.getMessage(), INTERNAL_SERVER_ERROR, false, ctx);
                                        return;
                                    }

                                    // Success handling
                                    HttpActiveReplicaHandler.writeHttpResponse(
                                            httpRequest.getRequestID(),
                                            httpResponse,
                                            ctx,
                                            isKeepAlive,
                                            startExecTimeNs);
                                } finally {
                                    ReferenceCountUtil.release(httpRequest.getHttpRequest());
                                    ReferenceCountUtil.release(httpRequest.getHttpRequestContent());
                                }
                            });
                        });
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            // ReadTimeoutException fires on idle keep-alive connections with no
            // pending request.  Just close silently — sending an error response
            // on an idle channel causes Go/curl clients to log "Unsolicited
            // response received on idle HTTP channel".
            if (cause instanceof ReadTimeoutException) {
                ctx.close();
                return;
            }

            String message = cause.getMessage();
            if (message == null) {
                message = cause.getClass().getSimpleName();
            }
            if (!(cause instanceof SocketException) ||
                    message.contains("Connection reset")) {
                System.out.println("HttpActiveReplicaHandler Error: " + message);
                cause.printStackTrace();
            }
            if (ctx.channel().isActive()) {
                byte[] bytes = ("Error: " + message).getBytes(CharsetUtil.UTF_8);
                boolean isUnknownServiceNameError =
                        message.contains("Unknown coordinator for name=");
                FullHttpResponse resp = new DefaultFullHttpResponse(
                        HTTP_1_1, isUnknownServiceNameError ? NOT_FOUND : INTERNAL_SERVER_ERROR,
                        Unpooled.wrappedBuffer(bytes));
                resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                resp.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
                ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
            } else {
                ctx.close();
            }
        }

        private static void sendBadRequestResponse(String message,
                                                   ChannelHandlerContext ctx) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, BAD_REQUEST,
                    Unpooled.copiedBuffer(message, CharsetUtil.UTF_8));

            // Add 'Content-Length' header only for a keep-alive connection.
            // Add keep alive header as per:
            // http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
            response.headers().setInt(
                    HttpHeaderNames.CONTENT_LENGTH,
                    response.content().readableBytes());
            response.headers().set(
                    HttpHeaderNames.CONNECTION,
                    HttpHeaderValues.KEEP_ALIVE);

            ChannelFuture cf = ctx.writeAndFlush(response);
            if (!cf.isSuccess()) {
                System.out.printf("%s:%s - sendBadRequestResponse, write failed: %s\n",
                        nodeId, HttpActiveReplica.class.getSimpleName(), cf.cause());
            }
        }

        private static void sendStringResponse(String message,
                                               HttpResponseStatus status,
                                               boolean isKeepAlive,
                                               ChannelHandlerContext ctx) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, status,
                    Unpooled.copiedBuffer(message, CharsetUtil.UTF_8));

            // Add 'Content-Length' header only for a keep-alive connection.
            // Add keep alive header as per:
            // http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
            if (isKeepAlive) {
                response.headers().setInt(
                        HttpHeaderNames.CONTENT_LENGTH,
                        response.content().readableBytes());
                response.headers().set(
                        HttpHeaderNames.CONNECTION,
                        HttpHeaderValues.KEEP_ALIVE);
            }

            ChannelFuture cf = ctx.writeAndFlush(response);
            if (!cf.isSuccess() && cf.cause() != null) {
                System.out.println("write failed: " + cf.cause());
                System.out.printf("%s:%s - sendStringResponse, write failed: %s, string:%s\n",
                        nodeId, HttpActiveReplica.class.getSimpleName(), cf.cause(), message);
            }

            // If keep-alive is off, close the connection once the content is fully written.
            if (!isKeepAlive) {
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }

        @Deprecated
        private static void sendStringResponse(String message,
                                               ChannelHandlerContext ctx,
                                               boolean isKeepAlive) {
            HttpActiveReplicaHandler.sendStringResponse(message, OK, isKeepAlive, ctx);
        }

        private static void writeHttpResponse(Long requestId, HttpResponse httpResponse,
                                              ChannelHandlerContext ctx,
                                              boolean isKeepAlive, long startProcessingTime) {
            assert requestId != null;
            if (httpResponse == null) {
                System.out.printf("%s:%s - ignoring empty HTTP response (id: %d)%n",
                        nodeId, HttpActiveReplica.class.getSimpleName(), requestId);
                logger.log(Level.WARNING,
                        String.format("%s:%s - ignoring empty HTTP response (id: %d)",
                                nodeId, HttpActiveReplica.class.getSimpleName(), requestId));
                return;
            }

            // Observability: logging post-execution time.
            String postExecTimestampHeaderKey = String.format("X-E-EXC-TS-%s", nodeId);
            String postExecTimestampStr = httpResponse.headers().get(postExecTimestampHeaderKey);
            if (postExecTimestampStr != null) {
                long postExecTimestamp = Long.parseLong(postExecTimestampStr);
                long postExecElapsedTime = System.nanoTime() - postExecTimestamp;
                logger.log(Level.FINE, "{0}:{1} - HTTP post-execution over {2}ms",
                        new Object[]{
                                nodeId.toLowerCase(),
                                HttpActiveReplica.class.getSimpleName(),
                                (postExecElapsedTime / 1_000_000.0)});
                httpResponse.headers().remove(postExecTimestampHeaderKey);
            }

            // Override the container's Connection header with the client's keep-alive
            // preference. Without this, containers that send "Connection: close" cause
            // the client to close TCP connections after every request, exhausting
            // ephemeral ports under load.
            if (isKeepAlive) {
                httpResponse.headers().set(HttpHeaderNames.CONNECTION,
                        HttpHeaderValues.KEEP_ALIVE);
            } else {
                httpResponse.headers().set(HttpHeaderNames.CONNECTION,
                        HttpHeaderValues.CLOSE);
            }

            // Guard: if the channel is already closed, release the response and bail out.
            // Without this, writeAndFlush on a closed channel causes the Netty encoder
            // to throw IllegalReferenceCountException when it tries to read the ByteBuf.
            if (!ctx.channel().isActive()) {
                safeRelease(httpResponse);
                return;
            }

            long preResponseWriteTsNs = System.nanoTime();
            ChannelFuture cf = ctx.writeAndFlush(httpResponse);
            cf.addListener((ChannelFutureListener) channelFuture -> {
                try {
                    if (!channelFuture.isSuccess()) {
                        if (channelFuture.cause() instanceof ClosedChannelException) {
                            return;
                        }
                        logger.log(Level.FINE,
                                "Writing response failed: " + channelFuture.cause());
                    }

                    if (startProcessingTime > 0) {
                        long now = System.nanoTime();
                        long writeDuration = now - preResponseWriteTsNs;
                        long elapsedOverallTime = now - startProcessingTime;
                        logger.log(Level.FINE,
                                "{0}:{1} - Overall HTTP execution within {2}ms (wrt={3}ms) [id: {4}]",
                                new Object[]{
                                        nodeId.toLowerCase(),
                                        HttpActiveReplica.class.getSimpleName(),
                                        (elapsedOverallTime / 1_000_000.0),
                                        (writeDuration / 1_000_000.0),
                                        String.valueOf(requestId)});
                    }

                    if (!isKeepAlive) {
                        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                                .addListener(ChannelFutureListener.CLOSE);
                    }
                } finally {
                    // writeAndFlush transfers ByteBuf ownership to Netty's pipeline,
                    // which releases it after encoding. No manual release needed here.
                }
            });
        }

        private boolean writeResponse(HttpObject currentObj, ChannelHandlerContext ctx) {
            // Decide whether to close the connection or not.
            boolean keepAlive = HttpUtil.isKeepAlive(request);
            // Build the response object.
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, currentObj.decoderResult().isSuccess() ? OK : BAD_REQUEST,
                    Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));

            response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

            if (keepAlive) {
                // Add 'Content-Length' header only for a keep-alive connection.
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                // Add keep alive header as per:
                // - http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }

            // Encode the cookie.
            String cookieString = request.headers().get(HttpHeaderNames.COOKIE);
            if (cookieString != null) {
                Set<Cookie> cookies = ServerCookieDecoder.STRICT.decode(cookieString);
                if (!cookies.isEmpty()) {
                    // Reset the cookies if necessary.
                    for (Cookie cookie : cookies) {
                        response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode(cookie));
                    }
                }
            } else {
                // Browser sent no cookie.  Add some.
                response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode("key1", "value1"));
                response.headers().add(HttpHeaderNames.SET_COOKIE, ServerCookieEncoder.STRICT.encode("key2", "value2"));
            }

            // Write the response.
            ctx.write(response);

            return keepAlive;
        }

        private static void send100Continue(ChannelHandlerContext ctx) {
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1, CONTINUE, Unpooled.EMPTY_BUFFER);
            ctx.write(response);
        }

    }

}
