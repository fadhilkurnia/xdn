package edu.umass.cs.reconfiguration.http;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.primarybackup.packets.ChangePrimaryPacket;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ActiveReplicaFunctions;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.utils.Config;
import edu.umass.cs.xdn.XdnGigapaxosApp;
import edu.umass.cs.xdn.XdnHttpRequestBatcher;
import edu.umass.cs.xdn.request.XdnGetProtocolRoleRequest;
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
import io.netty.util.concurrent.DefaultEventExecutorGroup;
import io.netty.util.concurrent.MultithreadEventExecutorGroup;
import org.json.JSONException;
import org.json.JSONObject;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.logging.Level;
import java.util.logging.Logger;

import static io.netty.handler.codec.http.HttpResponseStatus.*;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

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

    private static final boolean isDemandProfilerEnabled =
            Config.getGlobalBoolean(
                    ReconfigurationConfig.RC.HTTP_ACTIVE_REPLICA_ENABLE_DEMAND_PROFILER);

    // FIXME: used to indicate whether a single outstanding request has been executed, might go wrong when there are multiple outstanding requests
    static boolean finished;

    private static final boolean isHttpFrontendBatchEnabled = false;
    private final XdnHttpRequestBatcher batcher;

    // Flags to enable/disable debugging feature, specifically to identify bottleneck.
    // - isDebugBypassCoordination: directly send request to docker service, require
    //                              knowing the docker exposed port number,
    // - isDebugUseDummyResponse: skip execution in docker service.
    private static final boolean isDebugBypassCoordination = false;
    private static final boolean isDebugUseDummyResponse = false;
    private static final HttpClient debugHttpClient = HttpClient.newHttpClient();

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
        batcher = new edu.umass.cs.xdn.XdnHttpRequestBatcher(arf);

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
                                    nodeId, arf, executorGroup, batcher, sslCtx)
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
        private final edu.umass.cs.xdn.XdnHttpRequestBatcher requestBatching;
        private final SslContext sslCtx;

        HttpActiveReplicaInitializer(String nodeId,
                                     final ActiveReplicaFunctions arf,
                                     final MultithreadEventExecutorGroup executorGroup,
                                     final edu.umass.cs.xdn.XdnHttpRequestBatcher requestBatching,
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
        private final edu.umass.cs.xdn.XdnHttpRequestBatcher requestBatching;
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

            System.out.println(">>. receiving request ...");

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
                String requestHost = httpRequest.headers().get(HttpHeaderNames.HOST);
                if (requestHost != null) {
                    String[] hostPort = requestHost.split(":");
                    String host = hostPort[0];
                    if (host.endsWith(XDN_HOST_DOMAIN)) {
                        isXdnRequest = true;
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
                /**
                 * Request for GigaPaxos to coordinate
                 */
                HttpActiveReplicaRequest gRequest = null;
                /**
                 * JSONObject to extract keys and values from http request
                 */
                JSONObject json = new JSONObject();

                /**
                 * This boolean is used to indicate whether the request has been retrieved.
                 * If request info is retrieved from HttpRequest, then don't bother to retrieve it from
                 * HttpContent. Otherwise, retrieve the info from HttpContent.
                 * If we still can't retrieve the info, then the request is a Malformed request.
                 */
                boolean retrieved = false;

                if (msg instanceof HttpRequest) {
                    HttpRequest httpRequest = this.request = (HttpRequest) msg;
                    buf.setLength(0);

                    if (HttpUtil.is100ContinueExpected(httpRequest)) {
                        send100Continue(ctx);
                    }

                    logger.log(Level.FINE, "Http server received a request with HttpRequest: {0}", new Object[]{httpRequest});

                    // converting url query parameters into JSON key value pair
                    Map<String, List<String>> params = (new QueryStringDecoder(httpRequest.uri())).parameters();
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

                    if (json != null && json.length() > 0)
                        try {
                            gRequest = getRequestFromJSONObject(json);
                            logger.log(Level.INFO, "Http server retrieved an HttpActiveReplicaRequest from HttpRequest: {0}", new Object[]{gRequest});
                            retrieved = true;
                        } catch (Exception e) {
                            // ignore and do nothing if this is a malformed request
                            e.printStackTrace();
                        }
                }

                if (msg instanceof HttpContent) {
                    if (!retrieved) {
                        HttpContent httpContent = (HttpContent) msg;
                        logger.log(Level.INFO, "Http server received a request with HttpContent: {0}", new Object[]{httpContent});
                        if (httpContent != null) {
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

                    }

                    if (msg instanceof LastHttpContent) {
                        if (retrieved) {
                            logger.log(Level.INFO, "About to execute request: {0}", new Object[]{gRequest});
                            Object lock = new Object();
                            finished = false;
                            ExecutedCallback callback = new HttpExecutedCallback(buf, lock);

                            // execute GigaPaxos request here
                            if (arFunctions != null) {
                                logger.log(Level.FINE, "App {0} executes request: {1}", new Object[]{arFunctions, request});
                                boolean handled = arFunctions.handRequestToAppForHttp(
                                        (gRequest.needsCoordination()) ? ReplicableClientRequest.wrap(gRequest) : gRequest,
                                        callback);

                                synchronized (lock) {
                                    while (!finished) {
                                        try {
                                            lock.wait(100);
                                        } catch (InterruptedException e) {

                                        }
                                    }
                                }

                                /**
                                 *  If the request has been handled properly, then send demand profile to RC.
                                 *  This logic follows the design of (@link ActiveReplica}.
                                 */
                                if (handled)
                                    arFunctions.updateDemandStatsFromHttp(gRequest, senderAddr.getAddress());
                            }

                        }

                        LastHttpContent trailer = (LastHttpContent) msg;
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

            ByteBuf bodyRefCopy;
            if (msg instanceof HttpContent content) {
                bodyRefCopy = content.content().copy();
                this.requestContent = new DefaultLastHttpContent(bodyRefCopy);
            } else {
                bodyRefCopy = null;
            }

            if (msg instanceof LastHttpContent) {
                assert this.request != null;
                boolean isKeepAlive = HttpUtil.isKeepAlive(this.request);

                // return http bad request if service name is not specified
                String serviceName = XdnHttpRequest.inferServiceName(this.request);
                System.out.println(">> handling request for service name " + serviceName + " ...");
                if (serviceName == null || serviceName.isEmpty()) {
                    HttpActiveReplicaHandler.sendBadRequestResponse(
                            "Unspecified service name." +
                                    "This can be cause because of a wrong Host or empty XDN header",
                            ctx,
                            false);
                    return;
                }

                // FIXME: need to cleanly handle coordinator request. The two coordination requests
                //  handled here are:
                //   1. Request to set this node as the primary in PrimaryBackup.
                //   2. Request to get the protocol role (e.g., primary/backup).
                if (this.request.headers().get("coordinator-request") != null &&
                        this.request.headers().get("node-id") != null) {
                    String nodeID = this.request.headers().get("node-id");
                    ChangePrimaryPacket p = new ChangePrimaryPacket(serviceName, nodeID);
                    handleCoordinatorRequest(p, ctx);
                    return;
                }
                if (this.request.headers().get("XdnGetProtocolRoleRequest") != null) {
                    XdnGetProtocolRoleRequest xdnGetProtocolRoleRequest =
                            new XdnGetProtocolRoleRequest(serviceName);
                    handleCoordinatorRequest(xdnGetProtocolRoleRequest, ctx);
                    return;
                }

                if (isDebugUseDummyResponse || isDebugBypassCoordination) {
                    handleDebugRequests(new XdnHttpRequest(this.request, this.requestContent), ctx);
                    return;
                }

                // instrumenting the request for latency measurement
                long startExecTimeNs = System.nanoTime();
                XdnHttpRequest httpRequest =
                        new XdnHttpRequest(this.request, this.requestContent);

                // Submit long-running request coordination and execution off the event loop
                Future<HttpResponse> execFuture;
                if (isHttpFrontendBatchEnabled) {
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
                                var response = execFuture.get(10, java.util.concurrent.TimeUnit.SECONDS);
                                System.out.println(">>> HttpActiveReplica - async get " + response);
                                return response;
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt(); // Restore interrupt status
                                throw new RuntimeException("Request execution interrupted", e);
                            } catch (java.util.concurrent.TimeoutException e) {
                                execFuture.cancel(true); // Cancel the future if it times out
                                throw new RuntimeException("Request execution timed out after 10s", e);
                            } catch (ExecutionException e) {
                                throw new RuntimeException("Request execution failed", e);
                            }
                        }, offload)
                        .whenComplete((httpResponse, err) -> {
                            System.out.println(">>> HttpActiveReplica - whenComplete starting");
                            System.out.println(">>> HttpActiveReplica - response " + httpResponse);
                            // release buffer of http request's content
                            bodyRefCopy.release();

                            // do nothing when the channel is inactive
                            if (!ctx.channel().isActive()) {
                                System.out.println(">>> HttpActiveReplica - channel is inactive");
                                return;
                            }

                            // finally, handle the error or send the response
                            ctx.executor().execute(() -> {
                                // Error handling, send string message
                                if (err != null) {
                                    System.out.println("Writing error response: " + err.getMessage());
                                    err.printStackTrace();
                                    HttpActiveReplicaHandler.sendStringResponse(
                                            err.getMessage(), ctx, false);
                                    return;
                                }

                                // Success handling
                                long totalExecDuration = System.nanoTime() - startExecTimeNs;
                                HttpActiveReplicaHandler.writeHttpResponse(
                                        httpRequest.getRequestID(),
                                        httpResponse,
                                        ctx,
                                        isKeepAlive,
                                        totalExecDuration);

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
                System.out.println(">>> HttpActiveReplica - callback  response " + handled + " " + request);
                if (handled) {
                    future.complete(request);
                } else {
                    future.completeExceptionally(new Throwable("Request is failed to be handled"));
                }
            });

            // Wait until the future complete,
            // i.e., the httpRequest is already coordinated and executed.
            Request executedRequest;
            try {
                executedRequest = future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }

            // Validate the executed Http request
            if (!(executedRequest instanceof XdnHttpRequest xdnRequest)) {
                String exceptionMessage = "Unexpected executed request (" +
                        (executedRequest != null
                                ? executedRequest.getClass().getSimpleName() : "null") +
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

            return executedRequest.getHttpResponse();
        }

        // TODO: cleanly handle this
        private void handleCoordinatorRequest(Request p, ChannelHandlerContext context) {
            assert p instanceof ChangePrimaryPacket || p instanceof XdnGetProtocolRoleRequest :
                    "Unexpected packet type of " + p.getClass().getSimpleName();
            arFunctions.handRequestToAppForHttp(p, (request, handled) -> {
                String responseString = "OK\n";
                if (request instanceof XdnGetProtocolRoleRequest xdnGetProtocolRoleRequest) {
                    responseString = xdnGetProtocolRoleRequest.getJsonResponse();
                }
                sendStringResponse(responseString, context, false);
            });
        }

        private void handleDebugRequests(XdnHttpRequest httpRequest, ChannelHandlerContext ctx) {
            boolean isKeepAlive = HttpUtil.isKeepAlive(httpRequest.getHttpRequest());

            // Debug: send dummy response, the goal is to identify whether HttpActiveReplica
            //  is a bottleneck on receiving the incoming Http requests.
            if (isDebugUseDummyResponse) {
                httpRequest.getHttpRequestContent().content().release();
                DefaultFullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, OK,
                        Unpooled.copiedBuffer("DEBUG-OK", CharsetUtil.UTF_8));
                httpResponse.headers().setInt(
                        HttpHeaderNames.CONTENT_LENGTH,
                        httpResponse.content().readableBytes());
                HttpActiveReplicaHandler.writeHttpResponse(
                        httpRequest.getRequestID(),
                        httpResponse,
                        ctx, isKeepAlive, 0L);
                return;
            }

            // Debug: forward http request directly to the dockerized service. The goal is to
            //  identify the "rough" cost of coordination via the Gigapaxos/XDN stack.
            //  To make this work, please specify the docker's service port in the
            //  request header with XDN_DBG_SVC_PORT as the header name.
            if (isDebugBypassCoordination) {
                int detectedTargetPort = -1;

                String targetPortStr = httpRequest.getHttpRequest().
                        headers().get("XDN_DBG_SVC_PORT");
                if (targetPortStr != null) {
                    detectedTargetPort = Integer.parseInt(targetPortStr);
                }
                if (detectedTargetPort == -1) {
                    throw new IllegalStateException(
                            "Expecting port in the header with XDN_DBG_SVC_PORT as the key");
                }

                java.net.http.HttpRequest jdkHttpRequest =
                        XdnGigapaxosApp.createOutboundHttpRequest(
                                httpRequest.getHttpRequest(),
                                httpRequest.getHttpRequestContent(),
                                detectedTargetPort);

                java.net.http.HttpResponse<byte[]> response;
                try {
                    response = debugHttpClient.send(jdkHttpRequest,
                            java.net.http.HttpResponse.BodyHandlers.ofByteArray());
                } catch (IOException | InterruptedException e) {
                    throw new RuntimeException(e);
                }

                // Send and execute possibly long-running request in offload executor.
                Future<java.net.http.HttpResponse<byte[]>> execFuture = offload.submit(() -> {
                    try {
                        return debugHttpClient.send(jdkHttpRequest,
                                java.net.http.HttpResponse.BodyHandlers.ofByteArray());
                    } catch (IOException | InterruptedException e) {
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
                                            err.getMessage(), ctx, false);
                                    return;
                                }

                                // Success handling
                                HttpActiveReplicaHandler.writeHttpResponse(
                                        httpRequest.getRequestID(),
                                        XdnGigapaxosApp.toNettyHttpResponse(response),
                                        ctx, isKeepAlive, 0L);
                            });
                        });
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            System.out.println("HttpActiveReplicaHandler Error: " + cause.getMessage());
            cause.printStackTrace();
            if (ctx.channel().isActive()) {
                byte[] bytes = ("error: " + cause.getMessage()).getBytes(CharsetUtil.UTF_8);
                FullHttpResponse resp = new DefaultFullHttpResponse(
                        HTTP_1_1, INTERNAL_SERVER_ERROR, Unpooled.wrappedBuffer(bytes));
                resp.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
                resp.headers().setInt(HttpHeaderNames.CONTENT_LENGTH, bytes.length);
                ctx.writeAndFlush(resp).addListener(ChannelFutureListener.CLOSE);
            } else {
                ctx.close();
            }
        }

        private static void sendBadRequestResponse(String message, ChannelHandlerContext ctx, boolean isKeepAlive) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, BAD_REQUEST,
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
            if (!cf.isSuccess()) {
                System.out.println("write failed: " + cf.cause());
            }

            // If keep-alive is off, close the connection once the content is fully written.
            if (!isKeepAlive) {
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
        }

        private static void sendStringResponse(String message, ChannelHandlerContext ctx, boolean isKeepAlive) {
            FullHttpResponse response = new DefaultFullHttpResponse(
                    HTTP_1_1, OK,
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
            if (!cf.isSuccess()) {
                System.out.println("write failed: " + cf.cause());
            }

            // If keep-alive is off, close the connection once the content is fully written.
            if (!isKeepAlive) {
                ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(ChannelFutureListener.CLOSE);
            }
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
                                nodeId,
                                HttpActiveReplica.class.getSimpleName(),
                                (postExecElapsedTime / 1_000_000.0)});
                httpResponse.headers().remove(postExecTimestampHeaderKey);
            }

            if (isKeepAlive) {
                httpResponse.headers().set(
                        HttpHeaderNames.CONNECTION,
                        HttpHeaderValues.KEEP_ALIVE);
            }

            ChannelFuture cf = ctx.writeAndFlush(httpResponse);
            cf.addListener((ChannelFutureListener) channelFuture -> {
                if (!channelFuture.isSuccess()) {
                    logger.log(Level.WARNING,
                            "Writing response failed: " + channelFuture.cause());
                }

                // If keep-alive is off, close the connection once the content is fully written.
                if (!isKeepAlive) {
                    ctx.writeAndFlush(Unpooled.EMPTY_BUFFER)
                            .addListener(ChannelFutureListener.CLOSE);
                }
            });

            if (startProcessingTime > 0) {
                long elapsedTime = System.nanoTime() - startProcessingTime;
                logger.log(Level.FINE, "{0}:{1} - Overall HTTP execution within {2}ms (id: {3})",
                        new Object[]{
                                nodeId,
                                HttpActiveReplica.class.getSimpleName(),
                                (elapsedTime / 1_000_000.0),
                                String.valueOf(requestId)});
            }
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

        private record XdnHttpExecutedCallback(XdnHttpRequest request,
                                               ChannelHandlerContext ctx,
                                               ActiveReplicaFunctions arFunctions,
                                               long startProcessingTime)
                implements ExecutedCallback {
            @Override
            public void executed(Request executedRequest, boolean handled) {
                // Validates the executed Http request
                if (!(executedRequest instanceof XdnHttpRequest xdnRequest)) {
                    String exceptionMessage = "Unexpected executed request (" +
                            executedRequest.getClass().getSimpleName() +
                            "), it must be a " + XdnHttpRequest.class.getSimpleName();
                    throw new RuntimeException(exceptionMessage);
                }

                // Prepares the Http response, then send it back to client.
                HttpResponse httpResponse = xdnRequest.getHttpResponse();
                boolean isKeepAlive = HttpUtil.isKeepAlive(request.getHttpRequest());
                if (httpResponse != null) {
                    isKeepAlive = isKeepAlive && HttpUtil.isKeepAlive(httpResponse);
                }
                writeHttpResponse(xdnRequest.getRequestID(), httpResponse,
                        ctx, isKeepAlive, startProcessingTime);

                // Asynchronously sends statistics to the control plane (i.e., RC).
                if (isDemandProfilerEnabled) {
                    InetAddress clientInetAddress = null;
                    if (ctx.channel().remoteAddress() instanceof InetSocketAddress isa) {
                        clientInetAddress = isa.getAddress();
                    }
                    arFunctions.updateDemandStatsFromHttp(executedRequest, clientInetAddress);
                }
            }
        }
    }

    public static void main(String[] args) throws CertificateException, SSLException, InterruptedException {
        new HttpActiveReplica("node1", null, new InetSocketAddress(8080), false);
    }

}
