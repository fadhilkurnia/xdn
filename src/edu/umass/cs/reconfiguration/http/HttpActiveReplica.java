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
    private static final XdnHttpForwarderClient debugHttpClient = new XdnHttpForwarderClient();
    public static XdnGigapaxosApp debugAppReference = null; // needed for DBG_HDR_DIRECT_EXECUTE

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

        // Initialize request batching
        XdnHttpRequestBatcher requestBatcher = new XdnHttpRequestBatcher(arf);

        // Initializing boss and worker event loops.
        // The boss workers are accepting connections, which then will be passed to the child
        // worker group (i.e., `workerGroup`).
        EventLoopGroup bossGroup = new NioEventLoopGroup();
        EventLoopGroup workerGroup = new NioEventLoopGroup();

        // Dedicated executor group for long-running application execution and coordination.
        int threads = Math.max(4, Runtime.getRuntime().availableProcessors());
        DefaultEventExecutorGroup executorGroup = new DefaultEventExecutorGroup(threads);

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.AUTO_READ, true)
                    .childOption(ChannelOption.TCP_NODELAY, true)
                    .childHandler(
                            new HttpActiveReplicaInitializer(
                                    nodeId, arf, executorGroup, requestBatcher, sslCtx)
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
        }

    }


    private static class HttpActiveReplicaInitializer extends
            ChannelInitializer<SocketChannel> {

        private final String nodeId;
        private final ActiveReplicaFunctions arFunctions;
        private final MultithreadEventExecutorGroup executorGroup;
        private final XdnHttpRequestBatcher requestBatching;
        private final SslContext sslCtx;

        HttpActiveReplicaInitializer(String nodeId,
                                     final ActiveReplicaFunctions arf,
                                     final MultithreadEventExecutorGroup executorGroup,
                                     final XdnHttpRequestBatcher requestBatching,
                                     SslContext sslCtx) {
            this.nodeId = nodeId;
            this.arFunctions = arf;
            this.executorGroup = executorGroup;
            this.requestBatching = requestBatching;
            this.sslCtx = sslCtx;
        }

        @Override
        protected void initChannel(SocketChannel channel) throws Exception {
            ChannelPipeline p = channel.pipeline();

            if (sslCtx != null)
                p.addLast(sslCtx.newHandler(channel.alloc()));

            // avoid stuck connections
            p.addLast(new ReadTimeoutHandler(30));

            // (de)serialize bytes to http
            p.addLast(new HttpServerCodec());

            // aggregate to FullHttpRequest
            p.addLast(new HttpObjectAggregator(1 << 20));

            // handle CORS
            CorsConfig corsConfig = CorsConfigBuilder.forAnyOrigin().build();
            p.addLast(new CorsHandler(corsConfig));

            // handle http request in separate executor group because execution
            // and coordination can take a long time
            p.addLast(executorGroup,
                    new HttpActiveReplicaHandler(
                            nodeId,
                            arFunctions,
                            executorGroup,
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
        private final ActiveReplicaFunctions arFunctions;
        private final ExecutorService offload;
        private final XdnHttpRequestBatcher requestBatching;
        private final InetSocketAddress senderAddr; // client's inet address

        private HttpRequest request;
        private HttpContent requestContent;
        /**
         * Buffer that stores the response content
         */
        private final StringBuilder buf = new StringBuilder();

        HttpActiveReplicaHandler(String nodeId,
                                 ActiveReplicaFunctions arFunctions,
                                 ExecutorService offload,
                                 XdnHttpRequestBatcher requestBatching,
                                 InetSocketAddress addr) {
            HttpActiveReplicaHandler.nodeId = nodeId;
            this.arFunctions = arFunctions;
            this.offload = offload;
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
                if (this.request.headers().contains(DBG_HDR_DUMMY_RESPONSE) ||
                        this.request.headers().contains(DBG_HDR_BYPASS_COORDINATION) ||
                        this.request.headers().contains(DBG_HDR_DIRECT_EXECUTE)) {
                    handleDebugRequests(new XdnHttpRequest(this.request, this.requestContent), ctx);
                    return;
                }

                // Instrumenting the request for latency measurement
                long startExecTimeNs = System.nanoTime();
                XdnHttpRequest httpRequest =
                        new XdnHttpRequest(this.request, this.requestContent);

                // Submit long-running request coordination and execution off the event loop
                Future<HttpResponse> execFuture;
                if (isHttpFrontendBatchEnabled) {
                    // TODO: put comments about why we are increasing the reference-count here.
                    //   By default, its handled, but not if we pass it into batching worker.
                    ReferenceCountUtil.retain(httpRequest.getHttpRequest());
                    ReferenceCountUtil.retain(httpRequest.getHttpRequestContent());
                    execFuture = offload.submit(
                            () -> executeXdnHttpRequestViaBatching(httpRequest, this.senderAddr));
                } else {
                    execFuture = offload.submit(() ->
                            executeXdnHttpRequest(httpRequest, this.senderAddr));
                }

                // If channel closes before completion, cancel the task
                ctx.channel().closeFuture().addListener(cf -> execFuture.cancel(true));

                // When done, switch back to the channel's EventLoop to write the response
                CompletableFuture.supplyAsync(() -> {
                            try {
                                // Add timeout to prevent indefinite blocking
                                return execFuture.get(10, java.util.concurrent.TimeUnit.SECONDS);
                            } catch (InterruptedException e) {
                                // Restore interrupt status
                                Thread.currentThread().interrupt();
                                throw new RuntimeException("Request execution interrupted", e);
                            } catch (java.util.concurrent.TimeoutException e) {
                                // Cancel the future if it times out
                                execFuture.cancel(true);
                                throw new RuntimeException(
                                        "Request execution timed out after 10s",
                                        e
                                );
                            } catch (ExecutionException e) {
                                throw new RuntimeException(
                                        "Request execution failed: " + e.getCause().getMessage(),
                                        e
                                );
                            }
                        }, offload)
                        .whenComplete((httpResponse, err) -> {
                            // Check channel state, do nothing if the channel is inactive
                            // (e.g., closed by client).
                            if (!ctx.channel().isActive()) {
                                if (err != null && !(err instanceof CancellationException)) {
                                    System.out.println(">>> HttpActiveReplica - original error: " +
                                            err.getMessage());
                                    err.printStackTrace();
                                }
                                return;
                            }

                            // Finally, handle the error or send the response
                            ctx.executor().execute(() -> {
                                try {
                                    // Error handling, send string message
                                    if (err != null) {
                                        System.out.println(
                                                "Writing error response: " + err.getMessage());
                                        err.printStackTrace();
                                        HttpActiveReplicaHandler.sendStringResponse(
                                                err.getMessage(),
                                                INTERNAL_SERVER_ERROR,
                                                false,
                                                ctx
                                        );
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
                                    if (isHttpFrontendBatchEnabled) {
                                        ReferenceCountUtil.release(httpRequest.getHttpRequest());
                                        ReferenceCountUtil.release(
                                                httpRequest.getHttpRequestContent());
                                    }
                                }
                            });
                        });
            }
        }

        // executeXdnHttpRequest synchronously executes the provided httpRequest. It converts
        // callback-based mechanism provided by Gigapaxos into blocking future.
        private HttpResponse executeXdnHttpRequest(XdnHttpRequest httpRequest,
                                                   InetSocketAddress clientInetSocketAddress) {
            // Create Gigapaxos' request, it is important to explicitly set the clientAddress,
            // otherwise, down the pipeline, the RequestPacket's equals method will return false
            // and our callback will not be called, leaving the client hanging
            // waiting for response.
            ReplicableClientRequest gpRequest = ReplicableClientRequest.wrap(httpRequest);
            gpRequest.setClientAddress(clientInetSocketAddress);

            // Convert callback-based execution into future so that we can execute it synchronously.
            CompletableFuture<Request> future = new CompletableFuture<>();
            this.arFunctions.handRequestToAppForHttp(gpRequest, (request, handled) -> {
                if (handled) {
                    if (request == null) {
                        future.completeExceptionally(
                                new RuntimeException("Request was handled but returned null"));
                    } else {
                        future.complete(request);
                    }
                } else {
                    future.completeExceptionally(new RuntimeException("Request was not handled"));
                }
            });

            // Wait until the future complete with timeout,
            // i.e., the httpRequest is already coordinated and executed.
            Request executedRequest;
            try {
                executedRequest = future.get(5, java.util.concurrent.TimeUnit.SECONDS);
                if (executedRequest == null) {
                    throw new RuntimeException("Executed request is null after future completion");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Request execution was interrupted", e);
            } catch (java.util.concurrent.TimeoutException e) {
                throw new RuntimeException("Request execution timed out after 5s", e);
            } catch (ExecutionException e) {
                throw new RuntimeException(
                        "Request execution failed: " + e.getCause().getMessage(), e);
            }

            // Validate the executed Http request
            if (!(executedRequest instanceof XdnHttpRequest xdnRequest)) {
                String exceptionMessage = "Unexpected executed request (" +
                        executedRequest.getClass().getSimpleName() +
                        "), it must be a " + XdnHttpRequest.class.getSimpleName();
                throw new RuntimeException(exceptionMessage);
            }

            // Get the response
            return xdnRequest.getHttpResponse();
        }

        private HttpResponse executeXdnHttpRequestViaBatching(
                XdnHttpRequest httpRequest, InetSocketAddress clientInetSocketAddress) {
            CompletableFuture<XdnHttpRequest> future = new CompletableFuture<>();
            requestBatching.submit(
                    httpRequest,
                    clientInetSocketAddress,
                    (completedRequest, error) -> {
                        // TODO: the batched request should consider the clientInetSocketAddress
                        if (error != null) {
                            System.out.println(
                                    "Error completing execution via batching: " +
                                            error.getMessage());
                            error.printStackTrace();
                            future.completeExceptionally(error);
                        } else {
                            future.complete(completedRequest);
                        }
                    });

            XdnHttpRequest executedRequest;
            try {
                executedRequest = future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            assert httpRequest.getRequestID() == executedRequest.getRequestID() :
                    "Mismatch between the received and the executed request";

            return executedRequest.getHttpResponse();
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
                        offload.submit(() -> {
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
                        }, offload)
                        .whenComplete((httpResponse, err) -> {
                            // do nothing when the channel is inactive
                            if (!ctx.channel().isActive()) return;

                            // finally, handle the error or send the response
                            ctx.executor().execute(() -> {
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

                // Send and execute possibly long-running request in offload executor.
                ReferenceCountUtil.retain(httpRequest.getHttpRequest());
                ReferenceCountUtil.retain(httpRequest.getHttpRequestContent());
                Future<FullHttpResponse> execFuture = offload.submit(() -> {
                    try {
                        httpRequest.getHttpRequestContent().retain();
                        return debugHttpClient.execute(
                                "127.0.0.1",
                                detectedTargetPort,
                                (FullHttpRequest) httpRequest.getHttpRequest());
                    } catch (IOException | InterruptedException e) {
                        throw new RuntimeException(e);
                    } finally {
                        ReferenceCountUtil.release(httpRequest.getHttpRequest());
                        ReferenceCountUtil.release(httpRequest.getHttpRequestContent());
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
                        }, offload)
                        .whenComplete((httpResponse, err) -> {
                            // release buffer of http request's content
                            httpRequest.getHttpRequestContent().content().release();

                            // do nothing when the channel is inactive
                            if (!ctx.channel().isActive()) return;

                            // finally, handle the error or send the response
                            ctx.executor().execute(() -> {
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
                            });
                        });
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (!(cause instanceof SocketException) ||
                    cause.getMessage().contains("Connection reset")) {
                System.out.println("HttpActiveReplicaHandler Error: " + cause.getMessage());
                cause.printStackTrace();
            }
            if (ctx.channel().isActive()) {
                byte[] bytes = ("Error: " + cause.getMessage()).getBytes(CharsetUtil.UTF_8);
                boolean isUnknownServiceNameError =
                        cause.getMessage().contains("Unknown coordinator for name=");
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

            long preResponseWriteTsNs = System.nanoTime();
            ChannelFuture cf = ctx.writeAndFlush(httpResponse);
            cf.addListener((ChannelFutureListener) channelFuture -> {
                try {
                    if (!channelFuture.isSuccess()) {
                        if (channelFuture.cause() instanceof ClosedChannelException) {
                            // do nothing if end-client is the one who close the connection.
                            return;
                        }
                        logger.log(Level.WARNING,
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

                    // If keep-alive is off, close the connection once the content is fully written.
                    if (!isKeepAlive) {
                        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                                .addListener(ChannelFutureListener.CLOSE);
                    }
                } finally {
                    // We should release the reference-counted httpResponse here.
                    // It is allocated in the XdnGigapaxosApp after forwarding the request
                    // into the containerized service.
                    // However, because this handler is a SimpleChannelInboundHandler, this
                    // handler already automatically release the httpResponse for us because
                    // of the ctx.writeAndFlush(..) above.
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
