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
    private long lastFlushNs = 0;
    private long totalBatches = 0;

    private final ActiveReplicaFunctions arFunctions;
    private final int maxBatchSize;

    // Limits how many batches can be in-flight inside GigaPaxos simultaneously.
    // When the limit is reached, the Disruptor consumer blocks here instead of
    // submitting more work — this is the back-pressure point that lets requests
    // accumulate in the ring buffer so the next endOfBatch flush has a larger batch.
    private static final int MAX_IN_FLIGHT_BATCHES = 4;
    private final Semaphore inFlightPermits = new Semaphore(MAX_IN_FLIGHT_BATCHES);

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
            if (!currentBatch.isEmpty()) flushBatch();
            batchServiceName = event.request.getServiceName();
            currentBatch.add(event);
            flushBatch();
            return;
        }

        // Only batch requests from the same service
        // If different service, send previous batch
        String nextServiceName = event.request.getServiceName();
        if (!currentBatch.isEmpty() && !Objects.equals(batchServiceName, nextServiceName)) {
            flushBatch();
        }

        // Add request to batch, store the service name of that request
        if (currentBatch.isEmpty()) batchServiceName = nextServiceName;
        currentBatch.add(event);

        // Send batch if it's full or the ring buffer consumer
        // has caught up to latest published sequence number
        if (endOfBatch || currentBatch.size() >= maxBatchSize) {
            flushBatch();
        }
    }

    private void flushBatch() {
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
            // Nothing to send; clear slots and bail out
            for (XdnBatchEvent entry : entriesForThisBatch) {
                entry.clear();
            }
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

        // Block the Disruptor consumer until a slot is available.
        // This is the back-pressure point: when MAX_IN_FLIGHT_BATCHES are already
        // inside GigaPaxos, the consumer parks here and new requests pile up in the
        // ring buffer, so the next flush will have a larger batch.
        try {
            inFlightPermits.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            for (XdnBatchEvent entry : entriesForThisBatch) entry.clear();
            return;
        }

        // handRequestToAppForHttp returns immediately after handing the request to
        // GigaPaxos — it does not block for coordination. The consumer thread is
        // released as soon as GigaPaxos takes ownership, so it can drain the next
        // batch while this one is being coordinated in the background.
        // Diagnostic logging: batch size and gap since last flush.
        // Log every 500 batches to avoid overwhelming output.
        long now = System.nanoTime();
        long gapMs = lastFlushNs == 0 ? 0 : (now - lastFlushNs) / 1_000_000;
        lastFlushNs = now;
        totalBatches++;
        if (totalBatches % 500 == 0) {
            logger.warning("Batch #" + totalBatches
                    + " | size=" + entriesForThisBatch.size()
                    + " | gap=" + gapMs + "ms"
                    + " | permits=" + inFlightPermits.availablePermits());
        }

        arFunctions.handRequestToAppForHttp(gpRequest, (executedRequestBatch, handled) -> {
            for (XdnBatchEvent entry : entriesForThisBatch) {
                if (entry.completionHandler != null) {
                    entry.completionHandler.onComplete(entry.request, null);
                }
                entry.clear();
            }
            inFlightPermits.release();
        });
    }
}
