import edu.umass.cs.xdn.XdnHttpForwarderClient;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpServerCodec;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ForwarderFrontend {
    public static void main(String[] args) throws Exception {
        int portListen = 3000;
        int portDocker = 8000;
        String containerName = "bookcatalog";
        boolean useProxy = true;
        boolean blocking = true;
        boolean sharedGroup = false;
        String logFile = "forwarder-timings.log";

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--p-listen"       -> portListen = Integer.parseInt(args[++i]);
                case "--containerName"  -> containerName = args[++i];
                case "--p-docker"       -> portDocker = Integer.parseInt(args[++i]);
                case "--proxy-mode"     -> useProxy = Boolean.valueOf(args[++i]);
                case "--blocking"       -> blocking = Boolean.valueOf(args[++i]);
                case "--shared-group"   -> sharedGroup = Boolean.valueOf(args[++i]);
                case "--log"            -> logFile = args[++i];
                default -> throw new IllegalArgumentException("Unknown args: " + args[i]);
            }
        }

        System.out.printf("portListen=%d, containerName=%s, portDocker=%d, useProxy=%s, blocking=%s, sharedGroup=%s, logFile=%s%n",
                portListen, containerName, portDocker, useProxy, blocking, sharedGroup, logFile);

        EventLoopGroup bossGroup = new NioEventLoopGroup(1);
        EventLoopGroup workerGroup = new NioEventLoopGroup();
        XdnHttpForwarderClient forwarder = sharedGroup
                ? new XdnHttpForwarderClient(workerGroup)
                : new XdnHttpForwarderClient();

        final ExecutorService blockingPool = blocking ? Executors.newFixedThreadPool(200) : null;
        final boolean isBlocking = blocking;
        final String dockerIp = getContainerIp(containerName, useProxy);
        final int dockerPort = getContainerPort(containerName, portDocker, useProxy);

        System.out.printf("Run with:%n docker container address=%s:%d%n useProxy=%s%n blocking=%s%n",
                dockerIp, dockerPort, useProxy, blocking);

        final TimingLogger timingLogger = new TimingLogger(logFile);
        final InnerTimingLogger innerTimingLogger = new InnerTimingLogger(logFile.replace(".log", "-inner.log"));

        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(bossGroup, workerGroup)
                    .channel(NioServerSocketChannel.class)
                    .childHandler(new ChannelInitializer<SocketChannel>() {
                        @Override
                        protected void initChannel(SocketChannel ch) {
                            ch.pipeline().addLast(new HttpServerCodec());
                            ch.pipeline().addLast(new HttpObjectAggregator(16 * 1024 * 1024));
                            ch.pipeline().addLast(isBlocking
                                    ? new BlockingForwardHandler(forwarder, dockerIp, dockerPort, blockingPool, timingLogger)
                                    : new NonBlockingForwardHandler(forwarder, dockerIp, dockerPort, timingLogger, innerTimingLogger));
                        }
                    });

            b.bind(portListen).sync().channel().closeFuture().sync();
        } finally {
            workerGroup.shutdownGracefully();
            bossGroup.shutdownGracefully();
            if (blockingPool != null) blockingPool.shutdown();
            innerTimingLogger.close();
            timingLogger.close();
            forwarder.close();
        }
    }

    private static String getContainerIp(String containerName, boolean useProxy) throws Exception {
        if (useProxy) return "127.0.0.1";

        ProcessBuilder pb = new ProcessBuilder("docker", "inspect", "-f",
                "{{range .NetworkSettings.Networks}}{{.IPAddress}}{{end}}",  containerName);
        pb.redirectErrorStream(true);
        Process proc = pb.start();

        String output;
        try (var reader = proc.inputReader()) {
            output = reader.readLine();
        }

        int exitCode = proc.waitFor();
        if (exitCode != 0 || output == null || output.isBlank()) {
            throw new IllegalStateException("Failed to get IP for container: " + containerName);
        }

        return output.trim();
    }

    private static int getContainerPort(String containerName, int port, boolean useProxy) throws Exception {
        if (useProxy) return port;

        // Find the container-internal port whose host binding matches portDocker.
        // Output looks like "80/tcp": we strip the "/tcp" suffix afterward.
        String template = String.format(
                "{{range $port, $bindings := .HostConfig.PortBindings}}"
                        + "{{range $bindings}}{{if eq .HostPort \"%d\"}}{{$port}}{{end}}{{end}}"
                        + "{{end}}",
                port);

        ProcessBuilder pb = new ProcessBuilder("docker", "inspect", "-f", template, containerName);
        pb.redirectErrorStream(true);
        Process proc = pb.start();

        String output;
        try (var reader = proc.inputReader()) {
            output = reader.readLine();
        }

        int exitCode = proc.waitFor();
        if (exitCode != 0 || output == null || output.isBlank()) {
            throw new IllegalStateException(
                    "Failed to resolve container port for host port " + port + " on: " + containerName);
        }

        String portStr = output.trim().split("/")[0];
        return Integer.parseInt(portStr);
    }

    static final class BlockingForwardHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private final XdnHttpForwarderClient forwarder;
        private final String dockerIp;
        private final int dockerPort;
        private final ExecutorService pool;
        private final TimingLogger timingLogger;

        BlockingForwardHandler(XdnHttpForwarderClient forwarder, String ip, int port, ExecutorService pool, TimingLogger timingLogger) {
            this.forwarder = forwarder;
            this.dockerIp = ip;
            this.dockerPort = port;
            this.pool = pool;
            this.timingLogger = timingLogger;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            // TimingLog when the request is received by ForwarderFrontend
            long tReceived = System.nanoTime();
            String reqId = req.headers().get("X-XDN-ReqId");
            if (reqId == null) reqId = "unknown";
            final String finalReqId = reqId;

            // Keep the request object alive even after it is handed to the pool (outside the netty threads)
            // Prevents channelRead0's return to clear the request object
            req.retain();
            // Create a handler to the workerGroup thread handling *this* request
            Channel channel = ctx.channel();

            pool.submit(() -> {
                // TimingLog before the request is sent to XdnHttpForwarderClient
                long tBeforeExecute = System.nanoTime();

                try {
                    FullHttpResponse res = this.forwarder.execute(this.dockerIp, this.dockerPort, req);

                    // TimingLog after the response is returned by XdnHttpForwarderClient
                    long tAfterExecute = System.nanoTime();
                    int statusCode = res.status().code();

                    // return response to the workerGroup thread that originally received *this* request
                    channel.eventLoop().execute(() -> {
                        ChannelFuture writeFuture = channel.writeAndFlush(res);

                        // TimingLog after the response is returned by ForwarderFrontend
                        writeFuture.addListener(f -> {
                            long tFlushed = System.nanoTime();
                            timingLogger.record(new RequestTiming(
                                    finalReqId, tReceived, tBeforeExecute, tAfterExecute, tFlushed, statusCode));
                        });
                    });
                } catch (Exception e) {
                    // TimingLog after the response is returned by XdnHttpForwarderClient (FAIL)
                    long tAfterExecute = System.nanoTime();
                    timingLogger.record(new RequestTiming(
                            finalReqId, tReceived, tBeforeExecute, tAfterExecute, -1, -1));

                    channel.eventLoop().execute(() -> channel.pipeline().fireExceptionCaught(e));
                }
                // no finally/release here: client.execute() already fully disposed of request object
            });
        }
    }

    static final class NonBlockingForwardHandler extends SimpleChannelInboundHandler<FullHttpRequest> {
        private final XdnHttpForwarderClient forwarder;
        private final String dockerIp;
        private final int dockerPort;
        private final TimingLogger timingLogger;
        private final InnerTimingLogger innerTimingLogger;

        NonBlockingForwardHandler(XdnHttpForwarderClient forwarder, String ip, int port, TimingLogger timingLogger, InnerTimingLogger innerTimingLogger) {
            this.forwarder = forwarder;
            this.dockerIp = ip;
            this.dockerPort = port;
            this.timingLogger = timingLogger;
            this.innerTimingLogger = innerTimingLogger;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest req) {
            long tReceived = System.nanoTime();
            String reqId = req.headers().get("X-XDN-ReqId");
            if (reqId == null) reqId = "unknown";
            final String finalReqId = reqId;

            // Keep the request object alive even after it is handed to the pool (outside the netty threads)
            req.retain();
            // Create a handler to the workerGroup thread handling *this* request
            Channel channel = ctx.channel();

            // TimingLog before the request is sent to XdnHttpForwarderClient
            long tBeforeExecute = System.nanoTime();
            forwarder.executeAsync(this.dockerIp, this.dockerPort, req)
                    .whenComplete((res, throwable) -> {
                        // TimingLog after the response is returned by XdnHttpForwarderClient
                        long tAfterExecute = System.nanoTime();

                        channel.eventLoop().execute(() -> {
                            if (throwable != null) {
                                timingLogger.record(new RequestTiming(
                                        finalReqId, tReceived, tBeforeExecute, tAfterExecute, -1, -1));
                                ctx.fireExceptionCaught(throwable);
                            } else {
                                int statusCode = res.status().code();
                                long tAcquire = Long.parseLong(res.headers().get("X-Fwd-Acquire-Nanos"));
                                long tWrite = Long.parseLong(res.headers().get("X-Fwd-Write-Nanos"));
                                long tRespRecv = Long.parseLong(res.headers().get("X-Fwd-RespRecv-Nanos"));
                                // strip before forwarding to the real client. These are diagnostic only
                                res.headers().remove("X-Fwd-Acquire-Nanos");
                                res.headers().remove("X-Fwd-Write-Nanos");
                                res.headers().remove("X-Fwd-RespRecv-Nanos");
                                innerTimingLogger.record(new InnerTiming(finalReqId, tBeforeExecute, tAcquire, tWrite, tRespRecv));

                                // Return response through the thread in workerGroup that handled *this* request
                                ChannelFuture writeFuture = channel.writeAndFlush(res);

                                // TimingLog after the response is returned by ForwarderFrontend
                                writeFuture.addListener(f -> {
                                    long tFlushed = System.nanoTime();
                                    timingLogger.record(new RequestTiming(
                                            finalReqId, tReceived, tBeforeExecute, tAfterExecute, tFlushed, statusCode));
                                });
                            }
                        });
                    });
        }
    }

    record RequestTiming(
            String reqId,
            long tReceivedMs,       //
            long tBeforeExecuteMs,  //
            long tAfterExecuteMs,   //
            long tFlushedMs,        //
            int statusCode
    ) {}

    record InnerTiming(
            String reqId,
            long tBeforeExecuteNanos,
            long tAcquireNanos,
            long tWriteNanos,
            long tRespRecvNanos
    ) {}

    static final class TimingLogger implements Closeable {
        private final BlockingQueue<RequestTiming> queue = new LinkedBlockingQueue<>();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Thread writerThread;

        TimingLogger(String filePath) throws IOException {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
            writer.write("reqId,tReceivedMs,tBeforeExecuteMs,tAfterExecuteMs,tFlushedMs,statusCode");
            writer.newLine();
            this.writerThread = new Thread(() -> drainLoop(writer), "timing-logger-writer");
            this.writerThread.start();
        }

        void record(RequestTiming t) {
            queue.offer(t);
        }

        private void drainLoop(BufferedWriter writer) {
            try {
                int sinceFlush = 0;
                while (running.get() || !queue.isEmpty()) {
                    RequestTiming t = queue.poll(200, TimeUnit.MILLISECONDS);
                    if (t == null) {
                        writer.flush();
                        sinceFlush = 0;
                        continue;
                    }
                    writer.write(String.join(",",
                            t.reqId(),
                            Long.toString((t.tReceivedMs())),
                            Long.toString((t.tBeforeExecuteMs())),
                            Long.toString((t.tAfterExecuteMs())),
                            Long.toString((t.tFlushedMs())),
                            Integer.toString(t.statusCode())));
                    writer.newLine();

                    if (++sinceFlush >= 200) {
                        writer.flush();
                        sinceFlush = 0;
                    }
                }

                writer.flush();
                writer.close();
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            running.set(false); // tells the loop to drain remaining items, then exit
            try {
                writerThread.join(); // waits for the background thread to finish writing/flushing
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    static final class InnerTimingLogger implements Closeable {
        private final BlockingQueue<InnerTiming> queue = new LinkedBlockingQueue<>();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Thread writerThread;

        InnerTimingLogger(String filePath) throws IOException {
            BufferedWriter writer = new BufferedWriter(new FileWriter(filePath));
            writer.write("reqId,tBeforeExecuteNanos,tAcquireNanos,tWriteNanos,tRespRecvNanos");
            writer.newLine();
            this.writerThread = new Thread(() -> drainLoop(writer), "inner-timing-logger-writer");
            this.writerThread.start();
        }

        void record(InnerTiming t) {
            queue.offer(t);
        }

        private void drainLoop(BufferedWriter writer) {
            try {
                while (running.get() || !queue.isEmpty()) {
                    InnerTiming t = queue.poll(200, TimeUnit.MILLISECONDS);
                    if (t == null) continue;
                    writer.write(String.join(",",
                            t.reqId(),
                            Long.toString(t.tBeforeExecuteNanos()),
                            Long.toString(t.tAcquireNanos()),
                            Long.toString(t.tWriteNanos()),
                            Long.toString(t.tRespRecvNanos())));
                    writer.newLine();
                }
                writer.flush();
                writer.close();
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void close() {
            running.set(false);
            try {
                writerThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
