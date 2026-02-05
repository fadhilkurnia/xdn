package edu.umass.cs.xdn;

import com.lmax.disruptor.EventHandler;
import edu.umass.cs.reconfiguration.interfaces.ActiveReplicaFunctions;
import edu.umass.cs.reconfiguration.reconfigurationpackets.ReplicableClientRequest;
import edu.umass.cs.xdn.request.XdnHttpRequest;
import edu.umass.cs.xdn.request.XdnHttpRequestBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class XdnBatchHandler implements EventHandler<XdnBatchEvent> {
    private final List<XdnBatchEvent> currentBatch = new ArrayList<>();
    private final ActiveReplicaFunctions arFunctions;
    private final int maxBatchSize;

    // The service name for making sure requests of different
    // XDN services aren't mixed in the same batch
    private String batchServiceName = null;

    public XdnBatchHandler(ActiveReplicaFunctions arFunctions, int maxBatchSize) {
        this.arFunctions = arFunctions;
        this.maxBatchSize = maxBatchSize;
    }

    @Override
    public void onEvent(XdnBatchEvent event, long sequence, boolean endOfbatch) {
        // Puts any non read-only requests into it's own batch
        // Sends the previous batch first, then the new batch
        if (!event.request.isReadOnlyRequest()) {
            if (!currentBatch.isEmpty()) flushBatch();
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
        if (endOfbatch || currentBatch.size() >= maxBatchSize) {
            flushBatch();
        }
    }

    private void flushBatch() {
        if (currentBatch.isEmpty()) return;

        // Copy the requests out of the ring buffer
        List<XdnBatchEvent> entriesForThisBatch = new ArrayList<>();
        List<XdnHttpRequest> requests = new ArrayList<>(entriesForThisBatch.size());
        for (XdnBatchEvent e: entriesForThisBatch) requests.add(e.request);

        // Create Gigapaxos' request, it is important to explicitly set the clientAddress,
        // otherwise, down the pipeline, the RequestPacket's equals method will return false
        // and our callback will not be called, leaving the client hanging
        // waiting for response.
        XdnHttpRequestBatch batch = new XdnHttpRequestBatch(requests);
        ReplicableClientRequest gpRequest = ReplicableClientRequest.wrap(batch);
        gpRequest.setClientAddress(entriesForThisBatch.get(0).clientAddress);

        arFunctions.handRequestToAppForHttp(gpRequest, (executedRequestBatch, handled) -> {
            for (XdnBatchEvent entry: entriesForThisBatch) {
                entry.completionHandler.onComplete(entry.request, null);
                entry.clear();
            }
        });

        currentBatch.clear();
        batchServiceName = null;
    }
}
