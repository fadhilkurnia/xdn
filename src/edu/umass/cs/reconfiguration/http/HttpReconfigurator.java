package edu.umass.cs.reconfiguration.http;

import edu.umass.cs.nio.JSONPacket;
import edu.umass.cs.reconfiguration.ReconfigurationConfig;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorFunctions;
import edu.umass.cs.reconfiguration.interfaces.ReconfiguratorRequest;
import edu.umass.cs.reconfiguration.reconfigurationpackets.*;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReconfigurationPacket.PacketType;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.DecoderResult;
import io.netty.handler.codec.http.*;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import io.netty.handler.codec.http.cookie.ServerCookieEncoder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.SelfSignedCertificate;
import io.netty.util.CharsetUtil;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import javax.net.ssl.SSLException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.cert.CertificateException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static io.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;
import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

/**
 * @author arun
 * <p>
 * An HTTP front-end for a reconfigurator that supports the create,
 * delete, and request active replicas operations.
 * <p>
 * Requests are encoded in URIs using
 * <p>
 * Loosely based on the HTTP Snoop server example from netty
 * documentation pages.
 * <p>
 *         TODO: implement better API with prefix of /api/services
 *          - GET 		/api/v2/services
 *          - GET 		/api/v2/services/{name}
 *          - POST 		/api/v2/services/{name}
 *          - GET 		/api/v2/services/{name}/placement
 *          - POST 		/api/v2/services/{name}/placement
 *          - DELETE 	/api/v2/services/{name}
 *          because currently everything is handled with GET request :(
 */
public class HttpReconfigurator {

    /**
     * String keys for URI keys.
     */
    public static enum HTTPKeys {

        /**
         * The request type that must be present in every request and must be
         * one of the ones specified in {@link HTTPRequestTypes}.
         */
        TYPE(JSONPacket.PACKET_TYPE),

        /**
         * The service name that must be present in every request.
         */
        NAME(BasicReconfigurationPacket.Keys.NAME.toString()),

        /**
         * The initial state used in name creation requests.
         */
        INITIAL_STATE(ClientReconfigurationPacket.Keys.INITIAL_STATE.toString()),

        /**
         * Request ID.
         */
        QID(RequestActiveReplicas.Keys.QID.toString()),

        ;
        /**
         *
         */
        public final String label;

        HTTPKeys(String label) {
            this.label = label;
        }
    }

    /**
     *
     */
    public static enum HTTPRequestTypes {

        /**
         *
         */
        CREATE(ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME),

        /**
         *
         */
        DELETE(ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME),

        /**
         *
         */
        REQ_ACTIVES(ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS),

        /**
         *
         */
        CHANGE_ACTIVES(
                ReconfigurationPacket.PacketType.RECONFIGURE_ACTIVE_NODE_CONFIG),

        /**
         *
         */
        CHANGE_RECONFIGURATORS(
                ReconfigurationPacket.PacketType.RECONFIGURE_RC_NODE_CONFIG),

        ;

        final ReconfigurationPacket.PacketType type;

        HTTPRequestTypes(ReconfigurationPacket.PacketType type) {
            this.type = type;
        }

        HTTPRequestTypes() {
            this(null);
        }
    }

    private static final Set<HttpReconfigurator> instances = new HashSet<HttpReconfigurator>();

    private static final Logger log = ReconfigurationConfig.getLogger();

    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    private final Channel channel;

    private final String rcf;

    /**
     * @param rcf
     * @param sockAddr
     * @param ssl
     * @throws CertificateException
     * @throws SSLException
     * @throws InterruptedException
     */
    public HttpReconfigurator(ReconfiguratorFunctions rcf,
                              InetSocketAddress sockAddr, boolean ssl)
            throws CertificateException, SSLException, InterruptedException {

        this.rcf = rcf == null ? "" : rcf.toString();

        // Configure SSL.
        final SslContext sslCtx;
        if (ssl) {
            SelfSignedCertificate ssc = new SelfSignedCertificate();
            sslCtx = SslContextBuilder.forServer(ssc.certificate(),
                    ssc.privateKey()).build();
        } else {
            sslCtx = null;
        }

        // Configure the server.
        bossGroup = new NioEventLoopGroup(1);
        workerGroup = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(
                            new HTTPReconfiguratorInitializer(sslCtx, rcf));

            // Note that we always use the DEFAULT_HTTP_ADDR (0.0.0.0) if the loop-back address
            // (e.g., 127.0.0.1 or localhost) is used so that we can listen incoming Http requests
            // from all interfaces, including non-localhost ones.
            if (sockAddr.getAddress().isLoopbackAddress()) {
                sockAddr = new InetSocketAddress("0.0.0.0", sockAddr.getPort());
            }

            // FIXME: a quick hack to make RC listens to all interface, enabling a nameserver
            //  to contact with the RC using localhost or 127.0.0.1.
            sockAddr = new InetSocketAddress("0.0.0.0", sockAddr.getPort());

            channel = b.bind(sockAddr).sync().channel();
            instances.add(this);
            log.log(Level.INFO, "{0} ready", new Object[]{this});
            System.out.println("HttpReconfigurator ready on " + sockAddr);

            channel.closeFuture().sync();
        } finally {
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public String toString() {
        return this.rcf + ":HTTP:" + this.channel.localAddress().toString();

    }

    /**
     * @return Local socket address.
     */
    public SocketAddress getListeningAddress() {
        return this.channel.localAddress();
    }

    private boolean closed = false;

    /**
     * Close server and workers gracefully.
     */
    public void close() {
        this.bossGroup.shutdownGracefully();
        this.workerGroup.shutdownGracefully();
        closed = true;
    }

    /**
     * @return True if this server was closed gracefully using {@link #close()}.
     */
    public boolean isClosed() {
        return this.closed;
    }

    /**
     * To close all instances.
     */
    public static void closeAll() {
        for (HttpReconfigurator instance :
            // convert to array to allow removal while iterating
                instances.toArray(new HttpReconfigurator[0]))
            try {
                instance.close();
                instances.remove(instance);
            } catch (Exception | Error e) {
                // ignore and try to close rest
            }
    }

    static class HTTPReconfiguratorInitializer extends
            ChannelInitializer<SocketChannel> {

        private final SslContext sslCtx;
        final ReconfiguratorFunctions rcFunctions;

        HTTPReconfiguratorInitializer(SslContext sslCtx,
                                      final ReconfiguratorFunctions rcFunctions) {
            this.sslCtx = sslCtx;
            this.rcFunctions = rcFunctions;
        }

        @Override
        protected void initChannel(SocketChannel ch) throws Exception {
            ChannelPipeline p = ch.pipeline();
            if (sslCtx != null)
                p.addLast(sslCtx.newHandler(ch.alloc()));

            p.addLast(new HttpRequestDecoder());

            // Uncomment if you don't want to handle HttpChunks.
            p.addLast(new HttpObjectAggregator(1048576));

            p.addLast(new HttpResponseEncoder());

            p.addLast(new HTTPReconfiguratorHandler(rcFunctions));

        }
    }

    private static final JSONObject toJSONObject(
            Map<String, List<String>> keyValues) throws JSONException {
        JSONObject json = new JSONObject();
        for (String key : keyValues.keySet())
            // replace with last in case key repeats
            json.put(getHTTPKey(key),
                    JSONObject.stringToValue(keyValues.get(key).get(0)));
        return json;
    }

    private static final String missingKeyMessage(String key) {
        return "Malformed request missing key " + key;
    }

    /**
     * {@link ClientReconfigurationPacket} types handled.
     */
    public static final ReconfigurationPacket.PacketType[] types = {
            ReconfigurationPacket.PacketType.CREATE_SERVICE_NAME,
            ReconfigurationPacket.PacketType.DELETE_SERVICE_NAME,
            ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS,

            ReconfigurationPacket.PacketType.RECONFIGURE_RC_NODE_CONFIG,
            ReconfigurationPacket.PacketType.RECONFIGURE_ACTIVE_NODE_CONFIG,};

    private static ReconfigurationPacket.PacketType getRequestType(Object val) {
        for (ReconfigurationPacket.PacketType type : types)
            if (val instanceof String && (
                    // string match
                    type.toString().toLowerCase()
                            .equals(val.toString().toLowerCase()) ||
                            // alias match
                            type.equals(HTTPRequestTypes.valueOf(val.toString()
                                    .toUpperCase()).type)))
                return type;
            else if (val instanceof Integer && type.getInt() == (Integer) val)
                return type;
        return null;
    }

    // for case-insensitive, alias-insensitive key match
    private static String getHTTPKey(JSONObject json, HTTPKeys matchKey)
            throws HTTPException {
        String[] keys = JSONObject.getNames(json);
        if (keys != null)
            for (String key : keys)
                if (key.toUpperCase().equals(matchKey.toString().toUpperCase())
                        || key.toUpperCase().equals(
                        matchKey.label.toUpperCase()))
                    return key;
        throw new HTTPException(
                ClientReconfigurationPacket.ResponseCodes.MALFORMED_REQUEST,
                missingKeyMessage(matchKey.toString()) + " in " + json);
    }

    private static String getHTTPKey(String formKey) {
        for (HTTPKeys key : HTTPKeys.values())
            if (key.toString().toUpperCase()
                    .equals(formKey.toString().toUpperCase())
                    || key.label.toUpperCase().equals(formKey.toUpperCase()))
                return key.label;
        return formKey;
    }

    private static final ReconfiguratorRequest toReconfiguratorRequest(
            JSONObject json, Channel channel) throws JSONException,
            HTTPException {

        ReconfigurationPacket.PacketType type = getRequestType(json.get(
                // must contain type key, else exception
                getHTTPKey(json, HTTPKeys.TYPE)));

        String name = json.getString(
                // must contain name key, else exception
                getHTTPKey(json, HTTPKeys.NAME));

        if (type == ReconfigurationPacket.PacketType.RECONFIGURE_ACTIVE_NODE_CONFIG
                || type == ReconfigurationPacket.PacketType.RECONFIGURE_RC_NODE_CONFIG)
            return toServerReconfigurationRequest(json, channel, type, name);

        long requestID = (type == ReconfigurationPacket.PacketType.REQUEST_ACTIVE_REPLICAS ? json
                .optLong(RequestActiveReplicas.Keys.QID.toString(), 0) : 0);

        // else must be ClientReconfigurationPacket; insert necessary fields
        json.put(HTTPKeys.TYPE.label, type.getInt())
                .put(HTTPKeys.NAME.label, name)
                // epoch can be always set to 0
                .put(BasicReconfigurationPacket.Keys.EPOCH.toString(), 0)
                // is_query is always true at this server
                .put(ClientReconfigurationPacket.Keys.IS_QUERY.toString(), true)
                // *some* creator needed for inter-reconfigurator forwarding
                .put(ClientReconfigurationPacket.Keys.CREATOR.toString(),
                        channel.remoteAddress())
                // myReceiver probably not necessary
                .put(ClientReconfigurationPacket.Keys.MY_RECEIVER.toString(),
                        channel.localAddress())
                // request ID
                .put(RequestActiveReplicas.Keys.QID.toString(), requestID);

        if (json.has(HTTPKeys.INITIAL_STATE.label)) {
            json.put(HTTPKeys.INITIAL_STATE.label, json.getString(HTTPKeys.INITIAL_STATE.label));
        }

        ClientReconfigurationPacket crp;
        try {
            crp = (ClientReconfigurationPacket) ReconfigurationPacket
                    .getReconfigurationPacket(json,
                            ClientReconfigurationPacket.unstringer);
        } catch (Exception e) {
            throw new HTTPException(
                    ClientReconfigurationPacket.ResponseCodes.MALFORMED_REQUEST,
                    "Unable to decode request: " + e.getMessage());
        }
        return crp;
    }

    private static final ReconfiguratorRequest toServerReconfigurationRequest(
            JSONObject json, Channel channel, PacketType type, String name) {
        throw new RuntimeException("Unimplemented");
    }

    static class HTTPReconfiguratorHandler extends
            SimpleChannelInboundHandler<Object> {

        private HttpRequest request;
        /**
         * Buffer that stores the response content
         */
        private final StringBuilder buf = new StringBuilder();
        final ReconfiguratorFunctions rcFunctions;

        public HTTPReconfiguratorHandler(ReconfiguratorFunctions rcFunctions) {
            this.rcFunctions = rcFunctions;
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (msg instanceof HttpRequest) {
                HttpRequest request = this.request = (HttpRequest) msg;
                buf.setLength(0);

                // All HTTP requests for Reconfiguration with '/api/v2/services' prefix
                // are being handled separately. We keep other requests (e.g. /?type=CREATE&name=..)
                // processed by the original handler for backward compatability (that is v1).
                if (this.isV2ApiRequest(request)) {
                    this.handleReconfigurationV2Request(ctx, msg);
                    return;
                }

                ReconfiguratorRequest crp = null;
                try {
                    JSONObject json = toJSONObject(new QueryStringDecoder(
                            request.uri()).parameters());
                    log.log(Level.INFO, "JSON converted from uri is {0}", new Object[]{json});
                    crp = toReconfiguratorRequest(json, ctx.channel());

                    if (rcFunctions != null) {
                        crp = (ReconfiguratorRequest) this.rcFunctions
                                .sendRequest(crp);
                    }
                    buf.append(crp.toString());

                } catch (JSONException | HTTPException e) {
                    //e.printStackTrace();
                    log.log(Level.INFO, "Incurred exception {0} while trying" +
                            " to parse message {1}", new Object[]{e, msg});
                    buf.append(crp != null ? crp.setFailed()
                            .setResponseMessage(e.getMessage()) : "");
                }
                buf.append("\r\n");
            }

            if (msg instanceof HttpContent) {
                HttpContent httpContent = (HttpContent) msg;

                ByteBuf content = httpContent.content();
                if (content.isReadable()) {
                    buf.append("CONTENT: ");
                    buf.append(content.toString(CharsetUtil.UTF_8));
                    buf.append("\r\n");
                    appendDecoderResult(buf, request);
                }

                if (msg instanceof LastHttpContent) {
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
                        // If keep-alive is off, close the connection once the
                        // content is fully written.
                        ctx.writeAndFlush(Unpooled.EMPTY_BUFFER).addListener(
                                ChannelFutureListener.CLOSE);
                    }
                }
            }
        }

        private static void appendDecoderResult(StringBuilder buf, HttpObject o) {
            DecoderResult result = o.decoderResult();
            if (result.isSuccess())
                return;

            buf.append(".. WITH DECODER FAILURE: ");
            buf.append(result.cause());
            buf.append("\r\n");
        }

        private boolean isV2ApiRequest(HttpRequest request) {
            return request.uri().startsWith("/api/v2/services");
        }

        private void handleReconfigurationV2Request(ChannelHandlerContext ctx, Object msg) {
            // Ignores non HTTP requests that is not self-contained.
            if (!(msg instanceof HttpRequest httpRequest && msg instanceof LastHttpContent)) {
                return;
            }

            HttpContent httpContent = (HttpContent) msg;
            assert this.rcFunctions != null : "Reconfigurator packets handler must be initialized";

            // parse the sender
            assert ctx.channel().remoteAddress() instanceof InetSocketAddress :
                    "Invalid request sender";
            InetSocketAddress senderAddress = (InetSocketAddress) ctx.channel().remoteAddress();

            // Parses and handles SetReplicaPlacementRequest
            Pattern pattern = Pattern.compile("^/api/v2/services/[a-zA-Z0-9_-]+/placement$");
            Matcher matcher = pattern.matcher(httpRequest.uri());
            if (httpRequest.method().equals(HttpMethod.POST) && matcher.matches()) {
                parseAndHandleHttpSetReplicaPlacementRequest(
                        ctx, senderAddress, httpRequest, httpContent);
                return;
            }

            // Parses and handles GetReplicaPlacementRequest
            if (httpRequest.method().equals(HttpMethod.GET) && matcher.matches()) {
                parseAndHandleHttpGetReplicaPlacementRequest(
                        ctx, senderAddress, httpRequest, httpContent);
                return;
            }

            // TODO: handle other kind of reconfigurator requests

            // Handles unknown v2 requests with BadRequestResponse (400).
            this.writeBadRequestResponse(ctx, "Unknown reconfiguration request.");
        }

        private void parseAndHandleHttpSetReplicaPlacementRequest(ChannelHandlerContext ctx,
                                                                  InetSocketAddress senderAddress,
                                                                  HttpRequest httpRequest,
                                                                  HttpContent httpContent) {
            SetReplicaPlacementRequest setReplicaPlacementReq =
                    this.parseSetReplicaPlacementRequest(
                            senderAddress, httpRequest, httpContent);
            if (setReplicaPlacementReq == null) {
                this.writeBadRequestResponse(
                        ctx, "Invalid format for set replica placement request.");
                return;
            }
            this.rcFunctions.sendRequest(setReplicaPlacementReq, response -> {
                assert response instanceof ClientReconfigurationPacket :
                        "Unexpected response type from Reconfigurator: " +
                                response.getClass().getSimpleName();
                ClientReconfigurationPacket crp = (ClientReconfigurationPacket) response;
                this.writeResponse(crp, ctx);
                return null;
            });
        }

        private void parseAndHandleHttpGetReplicaPlacementRequest(ChannelHandlerContext ctx,
                                                                  InetSocketAddress senderAddress,
                                                                  HttpRequest httpRequest,
                                                                  HttpContent httpContent) {
            GetReplicaPlacementRequest getReplicaPlacementReq =
                    this.parseGetReplicaPlacementRequest(
                            senderAddress, httpRequest, httpContent);
            this.rcFunctions.sendRequest(getReplicaPlacementReq, response -> {
                assert response instanceof ClientReconfigurationPacket :
                        "Unexpected response type from Reconfigurator: " +
                                response.getClass().getSimpleName();
                ClientReconfigurationPacket crp = (ClientReconfigurationPacket) response;
                this.writeResponse(crp, ctx);
                return null;
            });
        }

        // returns null if the content is invalid
        private SetReplicaPlacementRequest parseSetReplicaPlacementRequest(InetSocketAddress sender,
                                                                           HttpRequest request,
                                                                           HttpContent content) {
            assert sender != null;
            assert request != null;
            assert content != null;

            // Parse service name in the URI
            // example: /api/v2/services/{name}/placement
            String[] uriComponents = request.uri().split("/");
            assert uriComponents.length == 6 : "Invalid uri for set replica placement request";
            String serviceName = uriComponents[4];

            // Parse active names from the body
            // example: `{"NODES" : ["AR0", "AR2", "AR3"]}`
            // example: `{"NODES" : ["AR0", "AR2", "AR3"], "COORDINATOR": "AR0"}`
            String contentBody = content.content().toString(StandardCharsets.ISO_8859_1);
            Set<String> nodeIds = new HashSet<>();
            String coordinatorId = null;
            try {
                JSONObject contentJson = new JSONObject(contentBody);
                JSONArray nodeIdArray = contentJson.getJSONArray("NODES");
                for (int i = 0; i < nodeIdArray.length(); i++) {
                    String nodeId = nodeIdArray.getString(i);
                    nodeIds.add(nodeId);
                }
                coordinatorId = contentJson.has("COORDINATOR") ?
                        contentJson.getString("COORDINATOR") : null;
            } catch (JSONException e) {
                return null;
            }

            return new SetReplicaPlacementRequest(
                    sender, serviceName, nodeIds, coordinatorId);
        }

        private GetReplicaPlacementRequest parseGetReplicaPlacementRequest(InetSocketAddress sender,
                                                                           HttpRequest request,
                                                                           HttpContent content) {
            assert sender != null;
            assert request != null;
            assert content != null;

            // Parse service name in the URI
            // example: /api/v2/services/{name}/placement
            String[] uriComponents = request.uri().split("/");
            assert uriComponents.length == 6 : "Invalid uri for get replica placement request";
            String serviceName = uriComponents[4];

            return new GetReplicaPlacementRequest(sender, serviceName);
        }

        private void writeBadRequestResponse(ChannelHandlerContext ctx, String errMessage) {
            HttpResponse httpResponse = new DefaultFullHttpResponse(
                    HTTP_1_1,
                    BAD_REQUEST,
                    Unpooled.copiedBuffer(errMessage.getBytes(StandardCharsets.UTF_8)));
            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
            ctx.write(httpResponse);
        }

        private void writeResponse(ClientReconfigurationPacket response,
                                   ChannelHandlerContext ctx) {
            // TODO: convert ClientReconfigurationPacket into HttpResponse
            FullHttpResponse httpResponse = new DefaultFullHttpResponse(
                    HTTP_1_1,
                    response.isFailed()
                            ? HttpResponseStatus.INTERNAL_SERVER_ERROR
                            : HttpResponseStatus.OK,
                    Unpooled.copiedBuffer(response.toString().getBytes(StandardCharsets.UTF_8)));
            httpResponse.headers().set(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.TEXT_PLAIN);
            httpResponse.headers().set(HttpHeaderNames.CONTENT_LENGTH,
                    httpResponse.content().readableBytes());
            ctx.write(httpResponse);
            ctx.flush();
        }

        private boolean writeResponse(HttpObject currentObj,
                                      ChannelHandlerContext ctx) {
            // Decide whether to close the connection or not.
            boolean keepAlive = HttpUtil.isKeepAlive(request);
            // Build the response object.
            FullHttpResponse response = new DefaultFullHttpResponse(HTTP_1_1,
                    currentObj.decoderResult().isSuccess() ? OK : BAD_REQUEST,
                    Unpooled.copiedBuffer(buf.toString(), CharsetUtil.UTF_8));

            response.headers().set(HttpHeaderNames.CONTENT_TYPE,
                    "text/plain; charset=UTF-8");

            if (keepAlive) {
                // Add 'Content-Length' header only for a keep-alive connection.
                response.headers().setInt(HttpHeaderNames.CONTENT_LENGTH,
                        response.content().readableBytes());
                // Add keep alive header as per:
                // -
                // http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
                response.headers().set(HttpHeaderNames.CONNECTION,
                        HttpHeaderValues.KEEP_ALIVE);
            }

            // Encode the cookie.
            String cookieString = request.headers().get(HttpHeaderNames.COOKIE);
            if (cookieString != null) {
                Set<Cookie> cookies = ServerCookieDecoder.STRICT
                        .decode(cookieString);
                if (!cookies.isEmpty()) {
                    // Reset the cookies if necessary.
                    for (Cookie cookie : cookies) {
                        response.headers().add(HttpHeaderNames.SET_COOKIE,
                                ServerCookieEncoder.STRICT.encode(cookie));
                    }
                }
            } else {
                // Browser sent no cookie. Add some.
                response.headers().add(HttpHeaderNames.SET_COOKIE,
                        ServerCookieEncoder.STRICT.encode("key1", "value1"));
                response.headers().add(HttpHeaderNames.SET_COOKIE,
                        ServerCookieEncoder.STRICT.encode("key2", "value2"));
            }

            // Write the response.
            ctx.write(response);

            return keepAlive;
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }

    /**
     * @param args
     * @throws CertificateException
     * @throws SSLException
     * @throws InterruptedException
     */
    public static void main(String[] args) throws CertificateException,
            SSLException, InterruptedException {
        new HttpReconfigurator(null, new InetSocketAddress(8080), false);
    }

}
