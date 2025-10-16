package edu.umass.cs.xdn;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.http.HttpActiveReplica;
import edu.umass.cs.reconfiguration.interfaces.ActiveReplicaFunctions;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;

import java.io.Closeable;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Batches {@link XdnHttpRequest}s before handing them over to {@link ActiveReplicaFunctions} for
 * execution. The batcher is composed of three single-threaded workers:
 * <ol>
 *   <li>A producer that drains the submission queue fed by {@link HttpActiveReplica} and pushes
 *   them into the batching queue.</li>
 *   <li>A batching worker that aggregates pending requests into {@link XdnHttpRequestBatch}
 *   instances and forwards them to the application by invoking
 *   {@link ActiveReplicaFunctions#handRequestToAppForHttp(Request, ExecutedCallback)}.</li>
 *   <li>A completion worker that receives finished batches and invokes the supplied
 *   {@link RequestCompletionHandler} so that {@link HttpActiveReplica} can respond to
 *   end-clients.</li>
 * </ol>
 */
public final class XdnHttpRequestBatcher implements Closeable {

    public static final int DEFAULT_MAX_BATCH_SIZE = 256;
    public static final Duration DEFAULT_MAX_BATCH_DELAY = Duration.ofMillis(2);

    private static final Logger LOG = Logger.getLogger(XdnHttpRequestBatcher.class.getName());

    private final ActiveReplicaFunctions arFunctions;
    private final int maxBatchSize;
    private final long maxBatchDelayNanos;

    private final BlockingQueue<BatchEntry> submissionQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<BatchEntry> batchingQueue = new LinkedBlockingQueue<>();
    private final BlockingQueue<BatchResult> completionQueue = new LinkedBlockingQueue<>();

    private final ExecutorService producerExecutor;
    private final ExecutorService batchingExecutor;
    private final ExecutorService completionExecutor;

    private final AtomicBoolean running = new AtomicBoolean(true);

    public XdnHttpRequestBatcher(ActiveReplicaFunctions arFunctions) {
        this(arFunctions, DEFAULT_MAX_BATCH_SIZE, DEFAULT_MAX_BATCH_DELAY);
    }

    public XdnHttpRequestBatcher(ActiveReplicaFunctions arFunctions,
                                 int maxBatchSize,
                                 Duration maxBatchDelay) {
        this.arFunctions = Objects.requireNonNull(arFunctions, "arFunctions");
        this.maxBatchSize = Math.max(1, maxBatchSize);
        this.maxBatchDelayNanos = Objects.requireNonNull(maxBatchDelay, "maxBatchDelay").toNanos();

        this.producerExecutor = Executors.newSingleThreadExecutor(namedThreadFactory("xdn-batch-producer"));
        this.batchingExecutor = Executors.newSingleThreadExecutor(namedThreadFactory("xdn-batch-builder"));
        this.completionExecutor = Executors.newSingleThreadExecutor(namedThreadFactory("xdn-batch-completion"));

        startProducerWorker();
        startBatchingWorker();
        startCompletionWorker();
    }

    public void submit(XdnHttpRequest request,
                       InetSocketAddress clientInetSocketAddress,
                       RequestCompletionHandler completionHandler) {
        Objects.requireNonNull(request, "request");
        Objects.requireNonNull(clientInetSocketAddress, "clientInetSocketAddress");
        Objects.requireNonNull(completionHandler, "completionHandler");
        if (!running.get()) {
            throw new IllegalStateException("Batcher is closed");
        }
        boolean isInserted = submissionQueue.offer(
                new BatchEntry(request, clientInetSocketAddress, completionHandler));
        assert isInserted : "Failed to submit request";
    }

    // ProducerWorker moves requests from submissionQueue into batchingQueue
    private void startProducerWorker() {
        producerExecutor.execute(() -> {
            try {
                while (running.get() || !submissionQueue.isEmpty()) {
                    BatchEntry entry = submissionQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (entry == null) {
                        continue;
                    }
                    batchingQueue.put(entry);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                LOG.log(Level.WARNING, "Producer worker encountered an error", t);
            }
        });
    }

    // BatchingWorker consumes multiple requests from batchingQueue, creates batch entry, and
    // passes the batch of request into ActiveReplica via dispatchBatch.
    private void startBatchingWorker() {
        batchingExecutor.execute(() -> {
            List<BatchEntry> buffer = new ArrayList<>(maxBatchSize);
            try {
                while (running.get() || !batchingQueue.isEmpty()) {
                    BatchEntry first = batchingQueue.poll(maxBatchDelayNanos, TimeUnit.NANOSECONDS);
                    if (first == null) {
                        continue;
                    }

                    // Keep write or unknown requests isolated to avoid unsafe batching.
                    if (isNotBehavioralReadOnly(first)) {
                        dispatchBatch(Collections.singletonList(first));
                        continue;
                    }

                    buffer.add(first);
                    while (buffer.size() < maxBatchSize) {
                        BatchEntry next = batchingQueue.poll();
                        if (next == null) {
                            break;
                        }
                        if (isNotBehavioralReadOnly(next)) {
                            dispatchBatch(new ArrayList<>(buffer));
                            buffer.clear();
                            dispatchBatch(Collections.singletonList(next));
                            break;
                        }
                        buffer.add(next);
                    }

                    dispatchBatch(new ArrayList<>(buffer));
                    buffer.clear();
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                LOG.log(Level.SEVERE, "Batching worker encountered an error", t);
            } finally {
                if (!buffer.isEmpty()) {
                    boolean isInserted = completionQueue.offer(
                            new BatchResult(new ArrayList<>(buffer),
                                    new IllegalStateException("Batcher shutting down")));
                    assert isInserted : "Failed to insert the BatchResult";
                    buffer.clear();
                }

                BatchEntry remaining;
                while ((remaining = batchingQueue.poll()) != null) {
                    boolean isInserted = completionQueue.offer(
                            new BatchResult(Collections.singletonList(remaining),
                                    new IllegalStateException("Batcher shutting down")));
                    assert isInserted : "Failed to insert the BatchResult";
                }
            }
        });
    }

    private static boolean isNotBehavioralReadOnly(BatchEntry entry) {
        return !entry.request.isReadOnlyRequest();
    }

    private void startCompletionWorker() {
        completionExecutor.execute(() -> {
            try {
                while (running.get() || !completionQueue.isEmpty()) {
                    BatchResult result = completionQueue.poll(100, TimeUnit.MILLISECONDS);
                    if (result == null) {
                        continue;
                    }
                    deliverResult(result);
                }
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            } catch (Throwable t) {
                LOG.log(Level.SEVERE, "Completion worker encountered an error", t);
            } finally {
                BatchResult leftover;
                while ((leftover = completionQueue.poll()) != null) {
                    deliverResult(leftover);
                }
            }
        });
    }

    private void dispatchBatch(List<BatchEntry> entries) {
        if (entries.isEmpty()) {
            return;
        }

        List<XdnHttpRequest> requests = new ArrayList<>(entries.size());
        for (BatchEntry entry : entries) {
            requests.add(entry.request);
        }

        InetSocketAddress firstClientInetSocketAddress =
                entries.getFirst().clientInetSocketAddress();

        XdnHttpRequestBatch batch;
        try {
            LOG.log(Level.FINE, "Batching with request size of " + entries.size());
            batch = new XdnHttpRequestBatch(requests);
        } catch (RuntimeException e) {
            boolean isInserted = completionQueue.offer(new BatchResult(entries, e));
            assert isInserted : "Failed to insert exception into the completion queue";
            return;
        }

        // Create Gigapaxos' request, it is important to explicitly set the clientAddress,
        // otherwise, down the pipeline, the RequestPacket's equals method will return false
        // and our callback will not be called, leaving the client hanging
        // waiting for response.
        ReplicableClientRequest gpRequest = ReplicableClientRequest.wrap(batch);
        gpRequest.setClientAddress(firstClientInetSocketAddress);

        BatchContext context = new BatchContext(entries);
        boolean accepted;
        try {
            accepted = arFunctions.handRequestToAppForHttp(
                    gpRequest, new BatchExecutedCallback(context));
        } catch (RuntimeException e) {
            boolean isInserted = completionQueue.offer(new BatchResult(entries, e));
            assert isInserted : "Failed to insert error into the completion queue";
            return;
        }

        if (!accepted) {
            boolean isInserted = completionQueue.offer(new BatchResult(entries,
                    new IllegalStateException("handRequestToAppForHttp returned false")));
            assert isInserted : "Failed to insert error into the completion queue";
        }
    }

    private void deliverResult(BatchResult result) {
        Throwable error = result.error;
        for (BatchEntry entry : result.entries) {
            try {
                entry.completionHandler.onComplete(entry.request, error);
            } catch (Throwable t) {
                LOG.log(Level.WARNING, "Request completion handler threw", t);
            }
        }
    }

    @Override
    public void close() {
        if (!running.compareAndSet(true, false)) {
            return;
        }

        producerExecutor.shutdownNow();
        batchingExecutor.shutdownNow();
        completionExecutor.shutdownNow();

        Throwable shutdownError = new IllegalStateException("Batcher closed");
        BatchEntry entry;
        while ((entry = submissionQueue.poll()) != null) {
            boolean isInserted = completionQueue.offer(
                    new BatchResult(Collections.singletonList(entry), shutdownError));
            assert isInserted : "Failed to insert closing error into the completion queue";
        }

        while ((entry = batchingQueue.poll()) != null) {
            boolean isInserted = completionQueue.offer(
                    new BatchResult(Collections.singletonList(entry), shutdownError));
            assert isInserted : "Failed to insert closing error into the completion queue";
        }

        flushCompletionQueue();
    }

    private void flushCompletionQueue() {
        BatchResult result;
        while ((result = completionQueue.poll()) != null) {
            deliverResult(result);
        }
    }

    private final class BatchExecutedCallback implements ExecutedCallback {
        private final BatchContext context;

        private BatchExecutedCallback(BatchContext context) {
            this.context = context;
        }

        @Override
        public void executed(Request executedRequestBatch, boolean handled) {
            Throwable error = null;

            // Validate that the response is correct and handled well.
            if (!(executedRequestBatch instanceof XdnHttpRequestBatch)) {
                error = new IllegalStateException("Unexpected request type "
                        + (executedRequestBatch == null ?
                        "null" : executedRequestBatch.getClass().getSimpleName()));
            } else if (!handled) {
                error = new IllegalStateException("Batch execution was not handled");
            }
            if (error != null) {
                boolean isInserted = completionQueue.offer(new BatchResult(context.entries, error));
                assert isInserted : "Failed to insert execution batch result into completion queue";
                return;
            }

            // Validate that the responses match the requests.
            XdnHttpRequestBatch executedXdnHttpRequestBatch =
                    (XdnHttpRequestBatch) executedRequestBatch;
            assert context.entries.size() == executedXdnHttpRequestBatch.size() :
                    "Unmatched size of request and response in a batch";
            for (int i = 0; i < executedXdnHttpRequestBatch.size(); i++) {
                long clientReqId = context.entries.get(i).request.getRequestID();
                long serverRespId = executedXdnHttpRequestBatch.
                        getRequestList().get(i).getRequestID();
                assert clientReqId == serverRespId :
                        String.format("Mismatch request-%d ID in the batch %d != %d",
                                i, clientReqId, serverRespId);
            }


            // Get the batch of response, pair each with the request.
            for (int i = 0; i < executedXdnHttpRequestBatch.size(); i++) {
                XdnHttpRequest clientReq = context.entries.get(i).request;
                XdnHttpRequest serverResp = executedXdnHttpRequestBatch.getRequestList().get(i);
                clientReq.setHttpResponse(serverResp.getHttpResponse());
            }

            boolean isInserted = completionQueue.offer(new BatchResult(context.entries, error));
            assert isInserted : "Failed to insert execution batch result into completion queue";
        }
    }

    private static ThreadFactory namedThreadFactory(String prefix) {
        AtomicInteger counter = new AtomicInteger();
        return r -> {
            Thread thread = new Thread(r);
            thread.setName(prefix + "-" + counter.incrementAndGet());
            thread.setDaemon(true);
            return thread;
        };
    }

    private record BatchEntry(XdnHttpRequest request,
                              InetSocketAddress clientInetSocketAddress,
                              RequestCompletionHandler completionHandler) {
    }

    private record BatchContext(List<BatchEntry> entries) {
    }

    private record BatchResult(List<BatchEntry> entries, Throwable error) {
    }

    @FunctionalInterface
    public interface RequestCompletionHandler {
        void onComplete(XdnHttpRequest request, Throwable error);
    }
}
