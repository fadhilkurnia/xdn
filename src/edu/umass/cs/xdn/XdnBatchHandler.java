package edu.umass.cs.xdn;

import com.lmax.disruptor.EventHandler;
import edu.umass.cs.eventual.LazyReplicaCoordinator;
import edu.umass.cs.reconfiguration.interfaces.ActiveReplicaFunctions;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.logging.Logger;

public class XdnBatchHandler implements EventHandler<XdnBatchEvent> {
    private final Logger logger = Logger.getLogger(XdnBatchHandler.class.getSimpleName());

    private final ActiveReplicaFunctions arFunctions;
    private final int maxBatchSize;

    // Separate semaphores for reads and writes so that writes are never queued
    // behind in-flight read batches and vice versa.
    //
    // Reads: higher concurrency because reads execute locally with no peer replication.
    // The consumer blocks after MAX_IN_FLIGHT_READ_BATCHES read batches are in-flight,
    // giving the ring buffer time to accumulate more requests for the next flush.
    //
    // Writes: lower concurrency because each write must replicate to all peers via
    // messenger.send(), which is synchronous and involves network I/O. Allowing too
    // many writes in-flight simultaneously would saturate the network and slow each
    // individual write further. A single in-flight write keeps the pipeline simple
    // and predictable without stalling the read path.
    private static final int MAX_IN_FLIGHT_READ_BATCHES = 4;
    private static final int MAX_IN_FLIGHT_WRITE_BATCHES = 1;
    private final Semaphore readPermits  = new Semaphore(MAX_IN_FLIGHT_READ_BATCHES);
    private final Semaphore writePermits = new Semaphore(MAX_IN_FLIGHT_WRITE_BATCHES);

    private long lastFlushNs = 0;
    private long totalBatches = 0;

    // Pre-allocated list, swapped (not copied) in flushBatch() to avoid per-flush allocation.
    private List<XdnBatchEvent> currentBatch;

    // The service name for making sure requests of different
    // XDN services aren't mixed in the same batch
    private String batchServiceName = null;

    public XdnBatchHandler(ActiveReplicaFunctions arFunctions, int maxBatchSize) {
        this.arFunctions = arFunctions;
        this.maxBatchSize = maxBatchSize;
        this.currentBatch = new ArrayList<>(maxBatchSize);
    }

    @Override
    public void onEvent(XdnBatchEvent event, long sequence, boolean endOfBatch) {
        // Puts any non read_only requests into its own batch
        // Sends the previous batch first, then the new batch
        if (!event.request.isReadOnlyRequest()) {
            if (!currentBatch.isEmpty()) flushBatch(false);
            batchServiceName = event.request.getServiceName();
            currentBatch.add(event);
            flushBatch(false);
            return;
        }

        // Only batch requests from the same service
        // If different service, send previous batch
        String nextServiceName = event.request.getServiceName();
        if (!currentBatch.isEmpty() && !Objects.equals(batchServiceName, nextServiceName)) {
            flushBatch(true);
        }

        // Add request to batch, store the service name of that request
        if (currentBatch.isEmpty()) batchServiceName = nextServiceName;
        currentBatch.add(event);

        // Send batch if it's full or the ring buffer consumer
        // has caught up to latest published sequence number
        if (endOfBatch || currentBatch.size() >= maxBatchSize) {
            flushBatch(true);
        }
    }

    private void flushBatch(boolean isReadBatch) {
        if (currentBatch.isEmpty()) {
            batchServiceName = null;
            return;
        }

        List<XdnBatchEvent> entriesForThisBatch = currentBatch;
        currentBatch = new ArrayList<>(maxBatchSize);
        batchServiceName = null;

        List<XdnHttpRequest> requests = new ArrayList<>(entriesForThisBatch.size());
        for (XdnBatchEvent e: entriesForThisBatch) {
            if (e.request != null) {
                requests.add(e.request);
            }
        }

        if (requests.isEmpty()) {
            for (XdnBatchEvent entry : entriesForThisBatch) entry.clear();
            return;
        }

        // Create GigaPaxos' request. It is important to explicitly set the clientAddress,
        // otherwise, down the pipeline, the RequestPacket's equals method will return false
        // and our callback will not be called, leaving the client hanging waiting for response.
        // We use the first non-null address for the GigaPaxos wrapper; individual per-client
        // responses are dispatched inside the callback via each entry's completionHandler.
        XdnHttpRequestBatch batch = new XdnHttpRequestBatch(requests);
        ReplicableClientRequest gpRequest = ReplicableClientRequest.wrap(batch);
        for (XdnBatchEvent entry : entriesForThisBatch) {
            if (entry.clientAddress != null) {
                gpRequest.setClientAddress(entry.clientAddress);
                break;
            }
        }

        // Acquire from the appropriate semaphore. Reads and writes never contend
        // with each other — a surge of in-flight reads cannot delay a write and
        // vice versa.
        Semaphore permits = isReadBatch ? readPermits : writePermits;
        try {
            permits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            for (XdnBatchEvent entry : entriesForThisBatch) entry.clear();
            return;
        }

        // Diagnostic logging: batch size and gap since last flush.
        // Log every 500 batches to avoid overwhelming output.
        long now = System.nanoTime();
        long gapMs = lastFlushNs == 0 ? 0 : (now - lastFlushNs) / 1_000_000;
        lastFlushNs = now;
        totalBatches++;
        if (totalBatches % 2 == 0) {
        logger.warning("Batch #" + totalBatches
                + " | " + (isReadBatch ? "READ" : "WRITE")
                + " | size=" + entriesForThisBatch.size()
                + " | gap=" + gapMs + "ms"
                + " | readPermits=" + readPermits.availablePermits()
                + " | writePermits=" + writePermits.availablePermits());
        }

        arFunctions.handRequestToAppForHttp(gpRequest, (executedRequestBatch, handled) -> {
            for (XdnBatchEvent entry : entriesForThisBatch) {
                if (entry.completionHandler != null) {
                    entry.completionHandler.onComplete(entry.request, null);
                }
                entry.clear();
            }
            permits.release();
        });
    }
}
