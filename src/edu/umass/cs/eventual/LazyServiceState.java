package edu.umass.cs.eventual;

import edu.umass.cs.clientcentric.VectorTimestamp;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Mutable per-service protocol state for {@link LazyReplicaCoordinator}'s frontier-based
 * anti-entropy. All fields except the executors are guarded by {@link #lock}: read lock for
 * client reads, digest computation, and checkpoint capture (none of which mutate state); write
 * lock for client writes, write-after application, and checkpoint installation.
 *
 * <p>Vector clock invariant: {@code vc[self]} counts this replica's own client writes, each
 * retained in {@link #ownLog} until globally covered; {@code vc[j]} for a peer j counts
 * j-sourced writes reflected in local state, advanced contiguously by the write-after fast path
 * or in bulk by a checkpoint install.
 */
class LazyServiceState<NodeIDType> {

    final String serviceName;
    final int epoch;
    final Set<NodeIDType> nodes;

    final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    /** This replica's applied-write vector clock, keyed by nodeId string. */
    final VectorTimestamp vc;

    /**
     * This replica's own executed writes, encoded with {@code ClientRequest.toBytes()}; index i
     * holds the write with sequence number {@code firstSeq + i}. Pruned once globally covered.
     */
    final List<byte[]> ownLog = new ArrayList<>();

    /** Sequence number of {@code ownLog.get(0)}; when the log is empty, {@code vc[self] + 1}. */
    long firstSeq = 1;

    /** Component-wise max of every vector clock ever heard from each peer (pruning floor). */
    final Map<String, VectorTimestamp> peerVcFloor = new HashMap<>();

    /** The most recent sync report (clock and state digest) received from each peer. */
    final Map<String, PeerReport> latestReport = new HashMap<>();

    /** The last checkpoint shipped to each peer, for dedup/backoff. */
    final Map<String, ShipRecord> lastShip = new HashMap<>();

    /** Cached state digest, invalidated on every state mutation; null = must recompute. */
    volatile byte[] digestCache = null;

    /** Guards against concurrent checkpoint captures for this service. */
    final AtomicBoolean shipInFlight = new AtomicBoolean(false);

    /** Serializes checkpoint installs off the NIO demultiplexer thread. */
    final ExecutorService installExecutor;

    record PeerReport(VectorTimestamp vc, byte[] digest, long timeMs) {
    }

    record ShipRecord(VectorTimestamp vc, byte[] digest, long timeMs) {
    }

    LazyServiceState(String serviceName, int epoch, Set<NodeIDType> nodes) {
        this.serviceName = serviceName;
        this.epoch = epoch;
        this.nodes = nodes;
        List<String> nodeIds = new ArrayList<>();
        for (NodeIDType node : nodes) {
            nodeIds.add(node.toString());
        }
        this.vc = new VectorTimestamp(nodeIds);
        this.installExecutor = Executors.newSingleThreadExecutor(r -> {
            Thread t = new Thread(r, "lazy-install-" + serviceName);
            t.setDaemon(true);
            return t;
        });
    }

    void shutdown() {
        this.installExecutor.shutdownNow();
    }
}
