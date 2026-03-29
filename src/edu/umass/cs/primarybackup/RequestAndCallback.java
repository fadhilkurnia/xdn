package edu.umass.cs.primarybackup;

import edu.umass.cs.gigapaxos.interfaces.ExecutedCallback;
import edu.umass.cs.primarybackup.packets.RequestPacket;

/**
 * Pairs a request with its callback, plus mutable pipeline timestamps
 * for latency instrumentation.
 */
public final class RequestAndCallback {
    private final RequestPacket requestPacket;
    private final ExecutedCallback callback;

    // Pipeline timestamps (nanoTime) for latency instrumentation.
    // Stage 1: PBM receives request
    private long pbmEntryNs;
    // Stage 2: batch worker picks up request from queue
    private long workerPickupNs;
    // Stage 3: execute() returns (WordPress response received)
    private long executeEndNs;
    // Stage 4: enqueued to doneQueue for capture thread
    private long doneQueueNs;
    // Stage 5: capture thread drains this request from doneQueue
    private long captureDrainNs;
    // Stage 6: captureStateDiff() completes
    private long captureDiffEndNs;
    // Stage 7: propose() called
    private long proposeNs;
    // Stage 8: Paxos commit callback fires
    private long commitCallbackNs;

    public RequestAndCallback(RequestPacket requestPacket, ExecutedCallback callback) {
        this.requestPacket = requestPacket;
        this.callback = callback;
    }

    public RequestPacket requestPacket() { return requestPacket; }
    public ExecutedCallback callback() { return callback; }

    public long pbmEntryNs() { return pbmEntryNs; }
    public long workerPickupNs() { return workerPickupNs; }
    public long executeEndNs() { return executeEndNs; }
    public long doneQueueNs() { return doneQueueNs; }
    public long captureDrainNs() { return captureDrainNs; }
    public long captureDiffEndNs() { return captureDiffEndNs; }
    public long proposeNs() { return proposeNs; }
    public long commitCallbackNs() { return commitCallbackNs; }

    public void setPbmEntryNs(long ns) { this.pbmEntryNs = ns; }
    public void setWorkerPickupNs(long ns) { this.workerPickupNs = ns; }
    public void setExecuteEndNs(long ns) { this.executeEndNs = ns; }
    public void setDoneQueueNs(long ns) { this.doneQueueNs = ns; }
    public void setCaptureDrainNs(long ns) { this.captureDrainNs = ns; }
    public void setCaptureDiffEndNs(long ns) { this.captureDiffEndNs = ns; }
    public void setProposeNs(long ns) { this.proposeNs = ns; }
    public void setCommitCallbackNs(long ns) { this.commitCallbackNs = ns; }

    /** Format a full pipeline timing breakdown string. Returns null if timestamps are missing. */
    public String formatPipelineTimingMs() {
        if (pbmEntryNs == 0) return null;
        return String.format(
                "queueWait=%.2f exec=%.2f captureWait=%.2f capture=%.2f propose=%.2f paxosCommit=%.2f total=%.2f batchSize=%d",
                (workerPickupNs - pbmEntryNs) / 1e6,
                (executeEndNs - workerPickupNs) / 1e6,
                (captureDrainNs - doneQueueNs) / 1e6,
                (captureDiffEndNs - captureDrainNs) / 1e6,
                (proposeNs > 0 ? (proposeNs - captureDiffEndNs) / 1e6 : 0),
                (commitCallbackNs > 0 && proposeNs > 0 ? (commitCallbackNs - proposeNs) / 1e6 : 0),
                (commitCallbackNs > 0 ? (commitCallbackNs - pbmEntryNs) / 1e6 : 0),
                0  // batch size set by caller
        );
    }
}
