package edu.umass.cs.xdn;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import edu.umass.cs.reconfiguration.interfaces.ActiveReplicaFunctions;
import edu.umass.cs.xdn.request.XdnHttpRequest;

import java.io.Closeable;
import java.net.InetSocketAddress;


public class XdnRingBufferBatcher implements Closeable {
    public static final int BUFFER_SIZE = 2048;
    private final Disruptor<XdnBatchEvent> disruptor;
    private final RingBuffer<XdnBatchEvent> ringBuffer;
    private final XdnBatchHandler batchHandler;

    public XdnRingBufferBatcher(ActiveReplicaFunctions arFunctions, int maxBatchSize) {
        // Initialize Disruptor with MULTI producer for multiple Netty worker threads
        this.disruptor = new Disruptor<>(
                XdnBatchEvent::new,
                BUFFER_SIZE,
                DaemonThreadFactory.INSTANCE,
                ProducerType.MULTI,
                new YieldingWaitStrategy()
        );

        this.batchHandler = new XdnBatchHandler(arFunctions, maxBatchSize);
        this.disruptor.handleEventsWith(this.batchHandler);
        this.ringBuffer = disruptor.start();
    }

    // Returns false if the ring buffer is full. Callers should send a 503 rather
    // than blocking — ringBuffer.next() would stall the Netty EventLoop thread,
    // preventing responses from being written and making latency unbounded.
    public boolean submit(XdnHttpRequest request, InetSocketAddress clientAddr,
                          RequestCompletionHandler handler) {
        long sequence;
        try {
            sequence = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e) {
            return false;
        }
        try {
            XdnBatchEvent event = ringBuffer.get(sequence);
            event.set(request, clientAddr, handler);
        } catch (Throwable t) {
            ringBuffer.get(sequence).clear();
            throw t;
        } finally {
            ringBuffer.publish(sequence);
        }
        return true;
    }

    @FunctionalInterface
    public interface RequestCompletionHandler {
        void onComplete(XdnHttpRequest request, Throwable error);
    }

    @Override
    public void close() {
        disruptor.shutdown();
    }
}