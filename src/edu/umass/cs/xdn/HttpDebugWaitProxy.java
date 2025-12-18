package edu.umass.cs.xdn;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.AsciiString;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.EventExecutor;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * An HTTP proxy that intentionally delays request submission to and response delivery from an
 * upstream service. This is handy when debugging timeout logic because it simulates slow
 * application processing without touching the real service. The proxy expects each incoming request
 * to include the {@value #UPSTREAM_PORT_HEADER} header, which encodes the upstream port on {@code
 * localhost}. Both request and response delays are configured in milliseconds.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * # java -cp build/classes/java/main edu.umass.cs.xdn.HttpDebugWaitProxy <listenPort> [reqDelayMs] [respDelayMs]
 * # respDelayMs defaults to reqDelayMs when omitted.
 * }</pre>
 *
 * <p>The implementation reuses most of {@link HttpDebugProxy}'s architecture: requests and
 * responses are fully buffered, Netty's {@link FixedChannelPool} keeps connections hot, and all
 * delay injection happens on a dedicated executor so Netty IO threads stay responsive.
 */
public final class HttpDebugWaitProxy implements Closeable {

  private static final Logger LOG = Logger.getLogger(HttpDebugWaitProxy.class.getName());

  private static final int MAX_CONTENT_LENGTH = 16 * 1024 * 1024;
  private static final int DEFAULT_MAX_POOL_SIZE = 32;
  private static final String LOCALHOST = "127.0.0.1";
  private static final AsciiString UPSTREAM_PORT_HEADER = AsciiString.cached("x-upstream-port");
  private static final AttributeKey<ProxyRequestContext> CONTEXT_KEY =
      AttributeKey.valueOf("httpDebugWaitProxyCtx");

  private final EventLoopGroup bossGroup;
  private final EventLoopGroup workerGroup;
  private final EventLoopGroup clientGroup;
  private final ConcurrentMap<InetSocketAddress, FixedChannelPool> connectionPools =
      new ConcurrentHashMap<>();
  private final long requestDelayMillis;
  private final long responseDelayMillis;
  private final ExecutorService waitExecutor;

  private volatile Channel serverChannel;

  public HttpDebugWaitProxy() {
    this(0L, 0L);
  }

  public HttpDebugWaitProxy(long requestDelayMillis, long responseDelayMillis) {
    this(
        new NioEventLoopGroup(),
        new NioEventLoopGroup(),
        new NioEventLoopGroup(),
        requestDelayMillis,
        responseDelayMillis);
  }

  HttpDebugWaitProxy(
      EventLoopGroup bossGroup,
      EventLoopGroup workerGroup,
      EventLoopGroup clientGroup,
      long requestDelayMillis,
      long responseDelayMillis) {
    this.bossGroup = Objects.requireNonNull(bossGroup, "bossGroup");
    this.workerGroup = Objects.requireNonNull(workerGroup, "workerGroup");
    this.clientGroup = Objects.requireNonNull(clientGroup, "clientGroup");
    this.requestDelayMillis = Math.max(0L, requestDelayMillis);
    this.responseDelayMillis =
        responseDelayMillis >= 0 ? responseDelayMillis : this.requestDelayMillis;
    this.waitExecutor =
        Executors.newCachedThreadPool(
            r -> {
              Thread t = new Thread(r, "http-wait-proxy");
              t.setDaemon(true);
              return t;
            });
  }

  /**
   * Starts the proxy server, wiring Netty pipelines and beginning to accept clients.
   *
   * @param listenPort Port bound by the proxy.
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
    LOG.info(
        () ->
            "HttpDebugWaitProxy listening on port "
                + listenPort
                + " (requestDelay="
                + requestDelayMillis
                + "ms, responseDelay="
                + responseDelayMillis
                + "ms)");
  }

  /** Blocks until the proxy channel closes. */
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
    waitExecutor.shutdownNow();
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

  /**
   * Resolves the upstream address by reading {@value #UPSTREAM_PORT_HEADER}. The header is stripped
   * before forwarding the request.
   */
  private static Origin resolveOrigin(FullHttpRequest request) {
    String portHeader = request.headers().get(UPSTREAM_PORT_HEADER);
    if (portHeader == null || portHeader.isEmpty()) {
      throw new IllegalArgumentException("Missing header: " + UPSTREAM_PORT_HEADER);
    }
    int port = parsePort(portHeader);
    request.headers().remove(UPSTREAM_PORT_HEADER);
    return new Origin(LOCALHOST, port);
  }

  private static int parsePort(String value) {
    try {
      int port = Integer.parseInt(value.trim());
      if (port <= 0 || port > 65_535) {
        throw new NumberFormatException("out of range");
      }
      return port;
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid upstream port header: " + value, e);
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

  private void runAfterDelay(EventExecutor targetExecutor, long delayMillis, Runnable task) {
    if (delayMillis <= 0 || waitExecutor.isShutdown()) {
      targetExecutor.execute(task);
      return;
    }
    waitExecutor.execute(
        () -> {
          try {
            Thread.sleep(delayMillis);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
          }
          targetExecutor.execute(task);
        });
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

      runAfterDelay(
          ctx.executor(),
          requestDelayMillis,
          () -> dispatchToBackend(ctx, outboundRequest, origin, keepAlive));
    }

    private void dispatchToBackend(
        ChannelHandlerContext ctx,
        FullHttpRequest outboundRequest,
        Origin origin,
        boolean keepAlive) {
      if (!ctx.channel().isActive()) {
        ReferenceCountUtil.release(outboundRequest);
        return;
      }

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

  private final class BackendAcquireListener implements GenericFutureListener<Future<Channel>> {

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
      ProxyRequestContext context =
          new ProxyRequestContext(frontCtx.channel(), pool, keepAlive, responseDelayMillis);
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

  private final class ProxyBackendHandler extends SimpleChannelInboundHandler<HttpObject> {

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, HttpObject msg) {
      ProxyRequestContext context = ctx.channel().attr(CONTEXT_KEY).get();
      if (context == null) {
        LOG.warning("Received backend message without request context; dropping");
        return;
      }

      if (msg instanceof HttpResponse response) {
        if (isInformational(response)) {
          FullHttpResponse informational = toInformationalResponse(response);
          forwardResponse(ctx, context, informational, false);
          return;
        }
        context.beginResponseBuffer(ctx, response);
      }

      if (msg instanceof HttpContent content) {
        BackendResponseBuffer buffer = context.pendingResponseBuffer();
        if (buffer == null) {
          LOG.warning("Dropping HttpContent with no pending backend response");
          return;
        }
        if (!buffer.append(content)) {
          context.releasePendingResponse();
          handleBackendFailure(
              ctx, context, "Origin response exceeded " + MAX_CONTENT_LENGTH + " bytes");
          return;
        }
        if (msg instanceof LastHttpContent lastChunk) {
          FullHttpResponse aggregated = context.finishPendingResponse(lastChunk);
          if (aggregated != null) {
            forwardResponse(ctx, context, aggregated, true);
          }
        }
      }
    }

    private boolean isInformational(HttpResponse response) {
      HttpStatusClass statusClass = response.status().codeClass();
      return statusClass == HttpStatusClass.INFORMATIONAL
          && response.status().code() != HttpResponseStatus.SWITCHING_PROTOCOLS.code();
    }

    private FullHttpResponse toInformationalResponse(HttpResponse response) {
      ByteBuf payload =
          response instanceof FullHttpResponse full && full.content().isReadable()
              ? Unpooled.copiedBuffer(full.content())
              : Unpooled.EMPTY_BUFFER;
      DefaultFullHttpResponse copy =
          new DefaultFullHttpResponse(response.protocolVersion(), response.status(), payload);
      copy.headers().set(response.headers());
      if (payload.isReadable()) {
        HttpUtil.setContentLength(copy, payload.readableBytes());
      } else {
        copy.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
      }
      if (response instanceof FullHttpResponse full) {
        copy.trailingHeaders().set(full.trailingHeaders());
      }
      return copy;
    }

    private void forwardResponse(
        ChannelHandlerContext backendCtx,
        ProxyRequestContext context,
        FullHttpResponse response,
        boolean finalResponse) {
      Channel inboundChannel = context.clientChannel();
      if (!inboundChannel.isActive()) {
        ReferenceCountUtil.release(response);
        if (finalResponse) {
          context.releasePendingResponse();
          context.release(backendCtx.channel());
          backendCtx.channel().attr(CONTEXT_KEY).set(null);
        }
        return;
      }

      Runnable writeTask = () -> writeResponse(backendCtx, context, response, finalResponse);
      if (finalResponse && context.responseDelayMillis() > 0) {
        runAfterDelay(backendCtx.executor(), context.responseDelayMillis(), writeTask);
      } else {
        writeTask.run();
      }
    }

    private void writeResponse(
        ChannelHandlerContext backendCtx,
        ProxyRequestContext context,
        FullHttpResponse response,
        boolean finalResponse) {
      Channel inboundChannel = context.clientChannel();
      if (finalResponse) {
        HttpUtil.setKeepAlive(response, context.keepAlive());
      }

      ChannelFuture writeFuture = inboundChannel.writeAndFlush(response);
      if (finalResponse) {
        if (!context.keepAlive()) {
          writeFuture.addListener(ChannelFutureListener.CLOSE);
        }
        writeFuture.addListener(
            f -> {
              context.release(backendCtx.channel());
              backendCtx.channel().attr(CONTEXT_KEY).set(null);
              if (!f.isSuccess()) {
                LOG.log(Level.WARNING, "Failed to write response to client", f.cause());
                inboundChannel.close();
              }
            });
      } else {
        writeFuture.addListener(
            f -> {
              if (!f.isSuccess()) {
                LOG.log(
                    Level.WARNING, "Failed to write informational response to client", f.cause());
                inboundChannel.close();
              }
            });
      }
    }

    private void handleBackendFailure(
        ChannelHandlerContext ctx, ProxyRequestContext context, String message) {
      sendError(context.clientChannel(), HttpResponseStatus.BAD_GATEWAY, message);
      context.releasePendingResponse();
      context.release(ctx.channel());
      ctx.channel().attr(CONTEXT_KEY).set(null);
      ctx.close();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      ProxyRequestContext context = ctx.channel().attr(CONTEXT_KEY).getAndSet(null);
      if (context != null) {
        context.releasePendingResponse();
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
      assert ch.attr(CONTEXT_KEY).get() == null;
      // No-op
    }

    @Override
    public void channelCreated(Channel ch) {
      ChannelPipeline pipeline = ch.pipeline();
      pipeline.addLast(new HttpClientCodec());
      pipeline.addLast(new ProxyBackendHandler());
    }
  }

  private static final class ProxyRequestContext {
    private final Channel clientChannel;
    private final FixedChannelPool pool;
    private final boolean keepAlive;
    private final long responseDelayMillis;
    private BackendResponseBuffer pendingResponse;

    private ProxyRequestContext(
        Channel clientChannel, FixedChannelPool pool, boolean keepAlive, long responseDelayMillis) {
      this.clientChannel = clientChannel;
      this.pool = pool;
      this.keepAlive = keepAlive;
      this.responseDelayMillis = responseDelayMillis;
    }

    void beginResponseBuffer(ChannelHandlerContext backendCtx, HttpResponse response) {
      releasePendingResponse();
      this.pendingResponse = new BackendResponseBuffer(backendCtx.alloc(), response);
    }

    BackendResponseBuffer pendingResponseBuffer() {
      return pendingResponse;
    }

    FullHttpResponse finishPendingResponse(LastHttpContent lastChunk) {
      if (pendingResponse == null) {
        return null;
      }
      pendingResponse.addTrailers(lastChunk.trailingHeaders());
      FullHttpResponse response = pendingResponse.build();
      pendingResponse = null;
      return response;
    }

    void releasePendingResponse() {
      if (pendingResponse != null) {
        pendingResponse.release();
        pendingResponse = null;
      }
    }

    Channel clientChannel() {
      return clientChannel;
    }

    boolean keepAlive() {
      return keepAlive;
    }

    long responseDelayMillis() {
      return responseDelayMillis;
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

  private static final class BackendResponseBuffer {
    private final HttpVersion version;
    private final HttpResponseStatus status;
    private final HttpHeaders headers;
    private final HttpHeaders trailingHeaders = new DefaultHttpHeaders();
    private final ByteBufAllocator allocator;
    private ByteBuf payload;
    private long aggregatedBytes;

    BackendResponseBuffer(ByteBufAllocator allocator, HttpResponse response) {
      this.version = response.protocolVersion();
      this.status = response.status();
      this.headers = new DefaultHttpHeaders();
      this.headers.set(response.headers());
      this.allocator = allocator;
      long declaredLength = HttpUtil.getContentLength(response, -1L);
      if (declaredLength > 0 && declaredLength <= MAX_CONTENT_LENGTH) {
        this.payload = allocator.buffer((int) declaredLength);
      }
    }

    boolean append(HttpContent content) {
      ByteBuf chunk = content.content();
      if (!chunk.isReadable()) {
        return true;
      }
      int readable = chunk.readableBytes();
      long newSize = aggregatedBytes + readable;
      if (newSize > MAX_CONTENT_LENGTH) {
        return false;
      }
      aggregatedBytes = newSize;
      ensurePayload(readable);
      payload.writeBytes(chunk);
      return true;
    }

    void addTrailers(HttpHeaders trailers) {
      if (trailers != null && !trailers.isEmpty()) {
        trailingHeaders.set(trailers);
      }
    }

    FullHttpResponse build() {
      ByteBuf content = payload == null ? Unpooled.EMPTY_BUFFER : payload;
      payload = null;
      DefaultFullHttpResponse response = new DefaultFullHttpResponse(version, status, content);
      response.headers().set(headers);
      if (content.isReadable()) {
        HttpUtil.setContentLength(response, content.readableBytes());
      } else {
        response.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
      }
      if (!trailingHeaders.isEmpty()) {
        response.trailingHeaders().set(trailingHeaders);
      }
      return response;
    }

    void release() {
      if (payload != null) {
        payload.release();
        payload = null;
      }
    }

    private void ensurePayload(int minWritableBytes) {
      if (payload == null) {
        payload = allocator.buffer(Math.max(minWritableBytes, 256));
      } else {
        payload.ensureWritable(minWritableBytes);
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
    long requestDelay = args.length > 1 ? Long.parseLong(args[1]) : 0L;
    long responseDelay = args.length > 2 ? Long.parseLong(args[2]) : requestDelay;
    HttpDebugWaitProxy proxy = new HttpDebugWaitProxy(requestDelay, responseDelay);
    Runtime.getRuntime().addShutdownHook(new Thread(proxy::close));
    proxy.start(listenPort);
    proxy.awaitTermination();
  }
}
