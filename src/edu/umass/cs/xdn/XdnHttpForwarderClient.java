package edu.umass.cs.xdn;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.ReferenceCountUtil;

import java.io.Closeable;
import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A thin synchronous HTTP client implemented on top of Netty. This client caches the last
 * connected channel so that subsequent requests to the same host/port can reuse the connection.
 *
 * <p>Example usage:
 * <pre>{@code
 * EventLoopGroup group = new NioEventLoopGroup();
 * try (XdnHttpForwarderClient client = new XdnHttpForwarderClient(group)) {
 *     FullHttpRequest request = new DefaultFullHttpRequest(
 *             HttpVersion.HTTP_1_1,
 *             HttpMethod.GET,
 *             "/health");
 *     request.headers().set(HttpHeaderNames.HOST, "127.0.0.1");
 *     FullHttpResponse response = client.execute("127.0.0.1", 8080, request);
 *     System.out.println("status = " + response.status());
 *     // The returned response owns a pooled ByteBuf; release it after use to avoid leaks.
 *     response.release();
 * } finally {
 *     group.shutdownGracefully().syncUninterruptibly();
 * }
 * }</pre>
 *
 * TODO: This client is still buggy, it has reference counter error. We still need to fix this
 *  so that we can reuse faster Netty-based Http client, instead of converting Netty request
 *  into OpenJDK request.
 */
public class XdnHttpForwarderClient implements Closeable {

    private final EventLoopGroup eventLoopGroup;
    private final boolean manageEventLoopGroup;

    private final Bootstrap bootstrap;
    private volatile Channel currentChannel;
    private volatile String currentHost;
    private volatile int currentPort;

    /**
     * Creates a client with its own event loop group.
     */
    public XdnHttpForwarderClient() {
        this(new NioEventLoopGroup(0), true);
    }

    /**
     * Creates a client backed by the supplied event loop group.
     */
    public XdnHttpForwarderClient(EventLoopGroup eventLoopGroup) {
        this(eventLoopGroup, false);
    }

    private XdnHttpForwarderClient(EventLoopGroup group, boolean manageGroup) {
        this.eventLoopGroup = Objects.requireNonNull(group);
        this.manageEventLoopGroup = manageGroup;
        this.bootstrap = new Bootstrap()
                .group(this.eventLoopGroup)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5_000)
                .handler(new ChannelInitializer<>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline pipeline = ch.pipeline();
                        pipeline.addLast(new HttpClientCodec());
                        pipeline.addLast(new HttpObjectAggregator(1 << 20));
                        pipeline.addLast(new ClientResponseHandler());
                    }
                });
    }

    /**
     * Sends the provided HTTP request synchronously and returns a detached HTTP response.
     *
     * @param host    target host
     * @param port    target port
     * @param request netty {@link FullHttpRequest}; will not be modified
     * @return the HTTP response
     */
    public FullHttpResponse execute(String host, int port, FullHttpRequest request)
            throws Exception {
        Objects.requireNonNull(host, "host");
        Objects.requireNonNull(request, "request");

        Channel channel = ensureChannel(host, port);
        ClientResponseHandler handler = channel.pipeline().get(ClientResponseHandler.class);
        if (handler == null) {
            throw new IllegalStateException("Client pipeline missing response handler");
        }

        CompletableFuture<FullHttpResponse> responseFuture = new CompletableFuture<>();
        handler.register(responseFuture);

        FullHttpRequest outbound = request.copy();
        ChannelFuture writeFuture = channel.writeAndFlush(outbound);
        writeFuture.addListener(f -> {
            ReferenceCountUtil.release(outbound);
            if (!f.isSuccess()) {
                handler.failResponse(f.cause());
            }
        });

        try {
            return responseFuture.get();
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            throw new RuntimeException(cause);
        }
    }

    private Channel ensureChannel(String host, int port) throws InterruptedException {
        Channel existing = this.currentChannel;
        if (isChannelHealthy(existing, host, port)) {
            return existing;
        }

        synchronized (this) {
            existing = this.currentChannel;
            if (!isChannelHealthy(existing, host, port)) {
                if (existing != null) {
                    existing.close().syncUninterruptibly();
                    clearCachedChannel(existing);
                }

                Channel newChannel = bootstrap.connect(host, port).sync().channel();
                newChannel.closeFuture().addListener(f -> clearCachedChannel(newChannel));
                this.currentChannel = newChannel;
                this.currentHost = host;
                this.currentPort = port;
                return newChannel;
            }
            return existing;
        }
    }

    private boolean isChannelHealthy(Channel channel, String host, int port) {
        if (channel == null) {
            return false;
        }
        if (!channel.isActive() || !channel.isOpen()) {
            return false;
        }
        if (!host.equals(this.currentHost) || port != this.currentPort) {
            return false;
        }
        return channel.pipeline().get(ClientResponseHandler.class) != null;
    }

    private void clearCachedChannel(Channel channel) {
        synchronized (this) {
            if (this.currentChannel == channel) {
                this.currentChannel = null;
                this.currentHost = null;
                this.currentPort = 0;
            }
        }
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            if (this.currentChannel != null) {
                this.currentChannel.close().syncUninterruptibly();
                this.currentChannel = null;
            }
        }
        if (manageEventLoopGroup) {
            this.eventLoopGroup.shutdownGracefully().syncUninterruptibly();
        }
    }

    private static final class ClientResponseHandler extends SimpleChannelInboundHandler<FullHttpResponse> {

        private final AtomicReference<CompletableFuture<FullHttpResponse>> inFlight =
                new AtomicReference<>();

        void register(CompletableFuture<FullHttpResponse> future) {
            if (!inFlight.compareAndSet(null, future)) {
                future.completeExceptionally(
                        new IllegalStateException("Another request is already in flight"));
            }
        }

        void failResponse(Throwable throwable) {
            CompletableFuture<FullHttpResponse> future = inFlight.getAndSet(null);
            if (future != null) {
                future.completeExceptionally(throwable);
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
            CompletableFuture<FullHttpResponse> future = inFlight.getAndSet(null);
            if (future == null) {
                ReferenceCountUtil.release(msg);
                return;
            }

            FullHttpResponse detached = copyResponse(msg);
            ReferenceCountUtil.release(msg);
            future.complete(detached);
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            failResponse(cause);
            ctx.close();
        }

        private FullHttpResponse copyResponse(FullHttpResponse msg) {
            ByteBuf content = msg.content();
            ByteBuf copiedContent = content != null && content.isReadable()
                    ? Unpooled.copiedBuffer(content)
                    : Unpooled.EMPTY_BUFFER;

            DefaultFullHttpResponse response = new DefaultFullHttpResponse(
                    msg.protocolVersion(),
                    msg.status(),
                    copiedContent);

            response.headers().set(msg.headers());
            response.trailingHeaders().set(msg.trailingHeaders());
            if (copiedContent != Unpooled.EMPTY_BUFFER
                    && !response.headers().contains(HttpHeaderNames.CONTENT_LENGTH)) {
                HttpUtil.setContentLength(response, copiedContent.readableBytes());
            }
            return response;
        }
    }
}
