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
    private long pbmEntryNs;
    private long workerPickupNs;
    private long executeEndNs;
    private long doneQueueNs;

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

    public void setPbmEntryNs(long ns) { this.pbmEntryNs = ns; }
    public void setWorkerPickupNs(long ns) { this.workerPickupNs = ns; }
    public void setExecuteEndNs(long ns) { this.executeEndNs = ns; }
    public void setDoneQueueNs(long ns) { this.doneQueueNs = ns; }
}
