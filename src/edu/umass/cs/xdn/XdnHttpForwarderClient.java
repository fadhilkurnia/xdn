package edu.umass.cs.xdn;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.pool.ChannelPoolHandler;
import io.netty.channel.pool.FixedChannelPool;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.util.AttributeKey;
import io.netty.util.ReferenceCountUtil;
import io.netty.util.concurrent.Future;
import java.io.Closeable;
import java.net.InetSocketAddress;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Lightweight synchronous HTTP client backed by Netty with per-origin connection pooling. Each
 * origin (host + port) reuses a {@link FixedChannelPool} so requests of the same origin avoid
 * repeated TCP handshakes.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * try (XdnHttpForwarderClient client = new XdnHttpForwarderClient()) {
 *     FullHttpRequest request = new DefaultFullHttpRequest(
 *             HttpVersion.HTTP_1_1, HttpMethod.GET, "/health");
 *     request.headers().set(HttpHeaderNames.HOST, "127.0.0.1");
 *     FullHttpResponse response = client.execute("127.0.0.1", 8080, request);
 *     System.out.println(response.status());
 *     response.release();
 * }
 * }</pre>
 */
public final class XdnHttpForwarderClient implements Closeable {

  private static final Logger LOG = Logger.getLogger(XdnHttpForwarderClient.class.getName());
  static final boolean PROFILE = Boolean.getBoolean("xdn.request.profiling");

  private static final int MAX_CONTENT_LENGTH = 16 * 1024 * 1024;
  private static final int DEFAULT_MAX_POOL_SIZE = 8;

  private final EventLoopGroup eventLoopGroup;
  private final boolean manageEventLoopGroup;
  private final ConcurrentMap<Origin, FixedChannelPool> pools = new ConcurrentHashMap<>();

  /** Creates a client backed by its own event loop group. */
  public XdnHttpForwarderClient() {
    this(new NioEventLoopGroup(0), true);
  }

  private XdnHttpForwarderClient(EventLoopGroup group, boolean manageGroup) {
    this.eventLoopGroup = Objects.requireNonNull(group, "eventLoopGroup");
    this.manageEventLoopGroup = manageGroup;
  }

  /**
   * Sends the given HTTP request and returns a detached response. The caller is responsible for
   * releasing the returned {@link FullHttpResponse} to avoid leaking pooled buffers.
   */
  public FullHttpResponse execute(String host, int port, FullHttpRequest request) throws Exception {
    Objects.requireNonNull(host, "host");
    Objects.requireNonNull(request, "request");

    long executeStartNs = PROFILE ? System.nanoTime() : 0;

    Origin origin = new Origin(host, port);
    FixedChannelPool pool = poolFor(origin);
    CompletableFuture<FullHttpResponse> responseFuture = new CompletableFuture<>();

    // Instead of copying the request, we bump up the reference count
    // and release it in the finally part, at the end of this method.
    ReferenceCountUtil.retain(request);
    final FullHttpRequest outbound = request;

    pool.acquire()
        .addListener(
            (Future<Channel> acquireFuture) -> {
              if (!acquireFuture.isSuccess()) {
                responseFuture.completeExceptionally(acquireFuture.cause());
                return;
              }

              long poolAcquiredNs = PROFILE ? System.nanoTime() : 0;

              Channel channel = acquireFuture.getNow();
              if (channel == null || !channel.isActive()) {
                responseFuture.completeExceptionally(
                    new IllegalStateException("Acquired inactive HTTP channel"));
                if (channel != null) {
                  releaseQuietly(pool, channel);
                }
                return;
              }

              ClientResponseHandler handler = channel.pipeline().get(ClientResponseHandler.class);
              if (handler == null) {
                responseFuture.completeExceptionally(
                    new IllegalStateException("Missing response handler in pipeline"));
                releaseQuietly(pool, channel);
                return;
              }

              RequestContext context =
                  new RequestContext(channel, pool, responseFuture, executeStartNs, poolAcquiredNs);
              if (!handler.register(context)) {
                responseFuture.completeExceptionally(
                    new IllegalStateException("Another request is already in flight"));
                releaseQuietly(pool, channel);
                return;
              }

              ChannelFuture writeFuture = channel.writeAndFlush(outbound);
              writeFuture.addListener(
                  (ChannelFutureListener)
                      wf -> {
                        if (wf.isSuccess()) {
                          if (PROFILE) {
                            context.setWriteDoneNs(System.nanoTime());
                          }
                        } else {
                          handler.fail(wf.cause());
                        }
                      });
            });

    try {
      return responseFuture.get();
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Exception) {
        throw (Exception) cause;
      }
      throw new RuntimeException(cause);
    } finally {
      ReferenceCountUtil.release(request);
    }
  }

  private FixedChannelPool poolFor(Origin origin) {
    return pools.computeIfAbsent(origin, this::createPool);
  }

  public void closePool(String host, int port) {
    FixedChannelPool pool = pools.remove(new Origin(host, port));
    if (pool != null) {
      try {
        pool.close();
      } catch (RuntimeException e) {
        LOG.log(Level.WARNING, "Failed to close pool for " + host + ":" + port, e);
      }
    }
  }

  private FixedChannelPool createPool(Origin origin) {
    Bootstrap bootstrap =
        new Bootstrap()
            .group(eventLoopGroup)
            .channel(NioSocketChannel.class)
            .option(ChannelOption.TCP_NODELAY, true)
            .option(ChannelOption.SO_KEEPALIVE, true)
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5_000)
            .remoteAddress(new InetSocketAddress(origin.host(), origin.port()));

    return new FixedChannelPool(bootstrap, new PoolHandler(), DEFAULT_MAX_POOL_SIZE);
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
    ByteBuf payload =
        (content == null || !content.isReadable()) ? Unpooled.EMPTY_BUFFER : content.copy();

    HttpHeaders headers = response.headers().copy();
    HttpHeaders trailing = response.trailingHeaders().copy();

    DefaultFullHttpResponse copy =
        new DefaultFullHttpResponse(
            response.protocolVersion(), response.status(), payload, headers, trailing);

    if (payload == Unpooled.EMPTY_BUFFER) {
      copy.headers().remove(HttpHeaderNames.CONTENT_LENGTH);
    } else {
      HttpUtil.setContentLength(copy, payload.readableBytes());
    }

    return copy;
  }

  private static void releaseQuietly(FixedChannelPool pool, Channel channel) {
    try {
      pool.release(channel)
          .addListener(
              f -> {
                if (!f.isSuccess()) {
                  LOG.log(Level.WARNING, "Failed to return channel to pool", f.cause());
                  channel.close();
                }
              });
    } catch (Throwable t) {
      LOG.log(Level.WARNING, "Failed to release channel back to pool", t);
      channel.close();
    }
  }

  @Override
  public void close() {
    pools
        .values()
        .forEach(
            pool -> {
              try {
                pool.close();
              } catch (RuntimeException e) {
                LOG.log(Level.WARNING, "Failed to close pool", e);
              }
            });
    pools.clear();

    if (manageEventLoopGroup) {
      eventLoopGroup.shutdownGracefully().syncUninterruptibly();
    }
  }

  private static final class PoolHandler implements ChannelPoolHandler {
    @Override
    public void channelReleased(Channel ch) {
      ClientResponseHandler handler = ch.pipeline().get(ClientResponseHandler.class);
      if (handler != null) {
        handler.clear();
      }
      if (PROFILE) {
        FirstByteTimestampHandler fbHandler = ch.pipeline().get(FirstByteTimestampHandler.class);
        if (fbHandler != null) {
          fbHandler.reset();
        }
      }
    }

    @Override
    public void channelAcquired(Channel ch) {
      // No-op
    }

    @Override
    public void channelCreated(Channel ch) {
      ChannelPipeline pipeline = ch.pipeline();
      if (PROFILE) {
        pipeline.addLast(new FirstByteTimestampHandler());
      }
      pipeline.addLast(new HttpClientCodec());
      pipeline.addLast(new HttpObjectAggregator(MAX_CONTENT_LENGTH));
      pipeline.addLast(new ClientResponseHandler());
    }
  }

  private static final class FirstByteTimestampHandler extends ChannelInboundHandlerAdapter {
    static final AttributeKey<Long> FIRST_BYTE_NS = AttributeKey.valueOf("firstByteNs");

    private boolean recorded;

    void reset() {
      recorded = false;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) {
      if (!recorded && msg instanceof ByteBuf) {
        recorded = true;
        ctx.channel().attr(FIRST_BYTE_NS).set(System.nanoTime());
      }
      ctx.fireChannelRead(msg);
    }
  }

  private static final class ClientResponseHandler
      extends SimpleChannelInboundHandler<FullHttpResponse> {

    private final AtomicReference<RequestContext> inFlight = new AtomicReference<>();

    ClientResponseHandler() {
      // We use autoRelease==false, enabling this Client's user to
      // manage the reference counted FullHttpResponse.
      super(false);
    }

    boolean register(RequestContext context) {
      return inFlight.compareAndSet(null, context);
    }

    void fail(Throwable throwable) {
      RequestContext context = inFlight.getAndSet(null);
      if (context != null) {
        context.fail(throwable);
      }
    }

    void clear() {
      inFlight.set(null);
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, FullHttpResponse msg) {
      long responseReceivedNs = PROFILE ? System.nanoTime() : 0;
      RequestContext context = inFlight.getAndSet(null);
      if (context == null) {
        ReferenceCountUtil.release(msg);
        LOG.warning("Received response with no context; dropping");
        return;
      }
      if (PROFILE) {
        Long firstByteNs = ctx.channel().attr(FirstByteTimestampHandler.FIRST_BYTE_NS).get();
        if (firstByteNs != null) {
          context.setFirstByteNs(firstByteNs);
        }
        context.setResponseReceivedNs(responseReceivedNs);
      }
      context.complete(msg);
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
      fail(new IllegalStateException("Backend channel closed"));
      ctx.fireChannelInactive();
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
      fail(cause);
      ctx.close();
    }
  }

  private static final class RequestContext {
    private final Channel channel;
    private final FixedChannelPool pool;
    private final CompletableFuture<FullHttpResponse> responseFuture;
    private final long executeStartNs;
    private final long poolAcquiredNs;
    private volatile long writeDoneNs;
    private volatile long firstByteNs;
    private volatile long responseReceivedNs;

    RequestContext(
        Channel channel,
        FixedChannelPool pool,
        CompletableFuture<FullHttpResponse> responseFuture,
        long executeStartNs,
        long poolAcquiredNs) {
      this.channel = channel;
      this.pool = pool;
      this.responseFuture = responseFuture;
      this.executeStartNs = executeStartNs;
      this.poolAcquiredNs = poolAcquiredNs;
    }

    void setWriteDoneNs(long ns) {
      this.writeDoneNs = ns;
    }

    void setFirstByteNs(long ns) {
      this.firstByteNs = ns;
    }

    void setResponseReceivedNs(long ns) {
      this.responseReceivedNs = ns;
    }

    void complete(FullHttpResponse response) {
      if (PROFILE) {
        long completeNs = System.nanoTime();

        LOG.log(
            Level.FINE,
            "[REQUEST_PROCESSING] phase=CONTAINER_RTT_BREAKDOWN poolAcquireUs={0} encodeWriteUs={1}"
                + " networkRttUs={2} decodeUs={3} completionUs={4} totalUs={5}",
            new Object[] {
              (poolAcquiredNs - executeStartNs) / 1_000.0,
              writeDoneNs > 0 ? (writeDoneNs - poolAcquiredNs) / 1_000.0 : -1,
              (writeDoneNs > 0 && firstByteNs > 0) ? (firstByteNs - writeDoneNs) / 1_000.0 : -1,
              (firstByteNs > 0 && responseReceivedNs > 0)
                  ? (responseReceivedNs - firstByteNs) / 1_000.0
                  : -1,
              responseReceivedNs > 0 ? (completeNs - responseReceivedNs) / 1_000.0 : -1,
              (completeNs - executeStartNs) / 1_000.0
            });
      }

      boolean delivered = responseFuture.complete(response);
      if (!delivered) {
        ReferenceCountUtil.release(response);
      }
      release();
    }

    void fail(Throwable throwable) {
      if (!responseFuture.completeExceptionally(throwable)) {
        LOG.log(Level.WARNING, "Duplicate failure delivery", throwable);
      }
      release();
    }

    private void release() {
      try {
        pool.release(channel)
            .addListener(
                f -> {
                  if (!f.isSuccess()) {
                    LOG.log(Level.WARNING, "Failed to release channel", f.cause());
                    channel.close();
                  }
                });
      } catch (Throwable t) {
        LOG.log(Level.WARNING, "Exception releasing channel to pool", t);
        channel.close();
      }
    }
  }

  private record Origin(String host, int port) {}
}
