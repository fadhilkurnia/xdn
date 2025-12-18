package edu.umass.cs.xdn;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A lightweight debugging HTTP proxy built on Netty. The proxy buffers entire inbound requests
 * before forwarding them to origin servers and reuses persistent connections for identical {@code
 * Host} destinations via {@link FixedChannelPool}.
 *
 * <p>Running the proxy:
 *
 * <pre>{@code
 * # Compile the project first, then start the proxy on port 8080
 * java -cp build/classes/java/main edu.umass.cs.xdn.HttpDebugProxy 8080
 *
 * # Configure your browser/client to use http://127.0.0.1:8080 as its HTTP proxy.
 * }</pre>
 *
 * <p>The proxy is intended for local testing and debugging; it is <em>not</em> hardened for
 * production scenarios.
 */
public final class HttpDebugProxy implements Closeable {

  private static final Logger LOG = Logger.getLogger(HttpDebugProxy.class.getName());

  private static final int MAX_CONTENT_LENGTH = 16 * 1024 * 1024;
  private static final int DEFAULT_MAX_POOL_SIZE = 8;
  private static final AttributeKey<ProxyRequestContext> CONTEXT_KEY =
      AttributeKey.valueOf("httpDebugProxyCtx");

  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final EventLoopGroup clientGroup;
  private final ConcurrentMap<InetSocketAddress, FixedChannelPool> connectionPools =
      new ConcurrentHashMap<>();

  private volatile Channel serverChannel;

  public HttpDebugProxy() {
    this(new NioEventLoopGroup(), new NioEventLoopGroup(), new NioEventLoopGroup());
  }

  HttpDebugProxy(EventLoopGroup bossGroup, EventLoopGroup workerGroup, EventLoopGroup clientGroup) {
    this.bossGroup = Objects.requireNonNull(bossGroup, "bossGroup");
    this.workerGroup = Objects.requireNonNull(workerGroup, "workerGroup");
    this.clientGroup = Objects.requireNonNull(clientGroup, "clientGroup");
  }

  /**
   * Starts the proxy server and begins accepting client connections.
   *
   * @param listenPort local port used by the proxy
   */
  public void start(int listenPort) throws InterruptedException {
    if (serverChannel != null) {
      throw new IllegalStateException("proxy already started");
    }

    ServerBootstrap bootstrap =
        new ServerBootstrap()
            .group(bossGroup, workerGroup)
            .channel(NioServerSocketChannel.class)
            .childHandler(
                new ChannelInitializer<SocketChannel>() {
                  @Override
                  protected void initChannel(SocketChannel ch) {
                    ChannelPipeline pipeline = ch.pipeline();
                    pipeline.addLast(new HttpServerCodec());
                    pipeline.addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH));
                    pipeline.addLast(new ProxyFrontendHandler());
                  }
                })
            .childOption(ChannelOption.AUTO_READ, true)
            .childOption(ChannelOption.TCP_NODELAY, true)
            .childOption(ChannelOption.SO_KEEPALIVE, true);

    Channel channel = bootstrap.bind(listenPort).sync().channel();
    this.serverChannel = channel;
    LOG.info(() -> "HttpDebugProxy listening on port " + listenPort);
  }

  /** Blocks until the proxy channel is closed. */
  public void awaitTermination() throws InterruptedException {
    Channel channel = this.serverChannel;
    if (channel != null) {
      channel.closeFuture().sync();
    }
  }

  @Override
  public void close() {
    if (serverChannel != null) {
      serverChannel.close().syncUninterruptibly();
      serverChannel = null;
    }

    for (FixedChannelPool pool : connectionPools.values()) {
      try {
        pool.close();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "Failed to close connection pool", e);
      }
    }
    connectionPools.clear();

    bossGroup.shutdownGracefully().syncUninterruptibly();
    workerGroup.shutdownGracefully().syncUninterruptibly();
    clientGroup.shutdownGracefully().syncUninterruptibly();
  }

  private FixedChannelPool poolFor(InetSocketAddress address) {
    return connectionPools.computeIfAbsent(address, this::createPool);
  }

  private FixedChannelPool createPool(InetSocketAddress address) {
    Bootstrap bootstrap =
        new Bootstrap()
            .group(clientGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5_000)
            .remoteAddress(address);

    return new FixedChannelPool(bootstrap, new PoolHandler(), DEFAULT_MAX_POOL_SIZE);
  }

  private static Origin resolveOrigin(FullHttpRequest request) {
    String hostHeader = request.headers().get(HttpHeaderNames.HOST);
    if (hostHeader == null || hostHeader.isEmpty()) {
      throw new IllegalArgumentException("Missing Host header in request");
    }

    int port = 80;
    String trimmed = hostHeader.trim();
    String host;

    if (trimmed.startsWith("[")) {
      int closingIdx = trimmed.indexOf(']');
      if (closingIdx < 0) {
        throw new IllegalArgumentException("Invalid IPv6 host header: " + hostHeader);
      }
      host = trimmed.substring(1, closingIdx);
      if (closingIdx + 1 < trimmed.length() && trimmed.charAt(closingIdx + 1) == ':') {
        String portPart = trimmed.substring(closingIdx + 2);
        port = parsePort(portPart, hostHeader);
      }
    } else {
      int colonIdx = trimmed.lastIndexOf(':');
      if (colonIdx > 0 && trimmed.indexOf(':') == colonIdx) {
        port = parsePort(trimmed.substring(colonIdx + 1), hostHeader);
        host = trimmed.substring(0, colonIdx);
      } else {
        host = trimmed;
      }
    }

    if (host.isEmpty()) {
      throw new IllegalArgumentException("Empty host derived from Host header: " + hostHeader);
    }

    return new Origin(host, port);
  }

  private static int parsePort(String portPart, String originalHeader) {
    try {
      return Integer.parseInt(portPart);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid port in Host header: " + originalHeader, e);
    }
  }

  private static void sendError(Channel channel, HttpResponseStatus status, String message) {
    if (!channel.isActive()) {
      return;
    }
    String payload = message == null ? status.toString() : message;
    ByteBuf content =
        Unpooled.copiedBuffer(payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    FullHttpResponse response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, status, content);
    response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
    HttpUtil.setContentLength(response, content.readableBytes());
    channel.writeAndFlush(response).addListener(ChannelFutureListener.CLOSE);
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
    ByteBuf copiedContent =
        content != null && content.isReadable()
            ? Unpooled.copiedBuffer(content)
            : Unpooled.EMPTY_BUFFER;

    DefaultFullHttpResponse copy =
        new DefaultFullHttpResponse(response.protocolVersion(), response.status(), copiedContent);
    copy.headers().set(response.headers());
    copy.trailingHeaders().set(response.trailingHeaders());
    if (copiedContent != Unpooled.EMPTY_BUFFER) {
      HttpUtil.setContentLength(copy, copiedContent.readableBytes());
    } else {
      copy.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
    }
    return copy;
  }

  private final class ProxyFrontendHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest msg) {
      final Origin origin;
      try {
        origin = resolveOrigin(msg);
      } catch (IllegalArgumentException e) {
        sendError(ctx.channel(), HttpResponseStatus.BAD_REQUEST, e.getMessage());
        return;
      }

      boolean keepAlive = HttpUtil.isKeepAlive(msg);
      FullHttpRequest outboundRequest = copyRequest(msg);
      HttpUtil.setKeepAlive(outboundRequest, keepAlive);

      FixedChannelPool pool = poolFor(origin.asAddress());
      Future<Channel> acquireFuture = pool.acquire();
      acquireFuture.addListener(new BackendAcquireListener(ctx, outboundRequest, pool, keepAlive));
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      LOG.log(Level.WARNING, "Frontend pipeline exception", cause);
      ctx.close();
    }
  }

  private static final class BackendAcquireListener
      implements GenericFutureListener<Future<Channel>> {

    private final ChannelHandlerContext frontCtx;
    private final FullHttpRequest outboundRequest;
    private final FixedChannelPool pool;
    private final boolean keepAlive;

    BackendAcquireListener(
        ChannelHandlerContext frontCtx,
        FullHttpRequest outboundRequest,
        FixedChannelPool pool,
        boolean keepAlive) {
      this.frontCtx = frontCtx;
      this.outboundRequest = outboundRequest;
      this.pool = pool;
      this.keepAlive = keepAlive;
    }

    @Override
    public void operationComplete(Future<Channel> future) {
      if (!future.isSuccess()) {
        ReferenceCountUtil.release(outboundRequest);
        Throwable cause = future.cause();
        LOG.log(Level.WARNING, "Failed to acquire backend channel", cause);
        sendError(
            frontCtx.channel(),
            HttpResponseStatus.BAD_GATEWAY,
            "Failed to contact origin: " + cause.getMessage());
        return;
      }

      Channel outboundChannel = future.getNow();
      if (!frontCtx.channel().isActive()) {
        ReferenceCountUtil.release(outboundRequest);
        Future<?> releaseFuture = pool.release(outboundChannel);
        releaseFuture.addListener(
            f -> {
              if (!f.isSuccess()) {
                LOG.log(Level.WARNING, "Failed to return channel after cancel", f.cause());
                outboundChannel.close();
              }
            });
        return;
      }
      ProxyRequestContext context = new ProxyRequestContext(frontCtx.channel(), pool, keepAlive);
      outboundChannel.attr(CONTEXT_KEY).set(context);

      ChannelFuture writeFuture = outboundChannel.writeAndFlush(outboundRequest);
      writeFuture.addListener(
          f -> {
            ReferenceCountUtil.release(outboundRequest);
            if (!f.isSuccess()) {
              context.release(outboundChannel);
              LOG.log(Level.WARNING, "Backend write failed", f.cause());
              sendError(
                  frontCtx.channel(),
                  HttpResponseStatus.BAD_GATEWAY,
                  "Write to origin failed: " + f.cause().getMessage());
            }
          });
    }
  }

  private static final class ProxyBackendHandler
      extends SimpleChannelInboundHandler<FullHttpResponse> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
      ProxyRequestContext context = ctx.channel().attr(CONTEXT_KEY).getAndSet(null);
      if (context == null) {
        LOG.warning("Received response without context; dropping");
        return;
      }

      Channel inboundChannel = context.clientChannel();
      if (!inboundChannel.isActive()) {
        context.release(ctx.channel());
        return;
      }

      FullHttpResponse response = copyResponse(msg);
      HttpUtil.setKeepAlive(response, context.keepAlive());

      ChannelFuture writeFuture = inboundChannel.writeAndFlush(response);
      if (!context.keepAlive()) {
        writeFuture.addListener(ChannelFutureListener.CLOSE);
      }
      writeFuture.addListener(
          f -> {
            context.release(ctx.channel());
            if (!f.isSuccess()) {
              LOG.log(Level.WARNING, "Failed to write response to client", f.cause());
              inboundChannel.close();
            }
          });
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      ProxyRequestContext context = ctx.channel().attr(CONTEXT_KEY).getAndSet(null);
      if (context != null) {
        sendError(
            context.clientChannel(),
            HttpResponseStatus.BAD_GATEWAY,
            "Origin connection error: " + cause.getMessage());
        context.release(ctx.channel());
      }
      LOG.log(Level.WARNING, "Backend pipeline exception", cause);
      ctx.close();
    }
  }

  private final class PoolHandler implements io.netty.channel.pool.ChannelPoolHandler {
    @Override
    public void channelReleased(Channel ch) {
      ch.attr(CONTEXT_KEY).set(null);
    }

    @Override
    public void channelAcquired(Channel ch) {
      // No-op
    }

    @Override
    public void channelCreated(Channel ch) {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast(new HttpClientCodec());
      pipeline.addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH));
      pipeline.addLast(new ProxyBackendHandler());
    }
  }

  private static final class ProxyRequestContext {
    private final Channel clientChannel;
    private final FixedChannelPool pool;
    private final boolean keepAlive;

    private ProxyRequestContext(Channel clientChannel, FixedChannelPool pool, boolean keepAlive) {
      this.clientChannel = clientChannel;
      this.pool = pool;
      this.keepAlive = keepAlive;
    }

    Channel clientChannel() {
      return clientChannel;
    }

    boolean keepAlive() {
      return keepAlive;
    }

    void release(Channel backendChannel) {
      try {
        Future<?> releaseFuture = pool.release(backendChannel);
        releaseFuture.addListener(
            f -> {
              if (!f.isSuccess()) {
                LOG.log(Level.WARNING, "Failed to return channel to pool", f.cause());
                backendChannel.close();
              }
            });
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "Failed to release backend channel", t);
        backendChannel.close();
      }
    }
  }

  private record Origin(String host, int port) {
    InetSocketAddress asAddress() {
      return InetSocketAddress.createUnresolved(host, port);
    }
  }

  public static void main(String[] args) throws Exception {
    int listenPort = args.length > 0 ? Integer.parseInt(args[0]) : 8080;
    HttpDebugProxy proxy = new HttpDebugProxy();
    Runtime.getRuntime().addShutdownHook(new Thread(proxy::close));
    proxy.start(listenPort);
    proxy.awaitTermination();
  }
}
