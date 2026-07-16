package edu.umass.cs.eventual.interfaces;

/**
 * CheckpointableApplication is implemented by applications whose whole service state can be
 * captured and restored as an opaque blob, enabling checkpoint-based anti-entropy in
 * {@link edu.umass.cs.eventual.LazyReplicaCoordinator}. The coordinator guarantees that no
 * request is being executed for the service while {@link #captureCheckpoint(String)} or
 * {@link #applyCheckpoint(String, byte[])} runs, so implementations only need to provide a
 * point-in-time snapshot of durable state (e.g., an archive of the service's state directory).
 *
 * <p>This is a sibling of {@code edu.umass.cs.primarybackup.interfaces.BackupableApplication},
 * which captures incremental statediffs; this interface captures the complete state.
 */
public interface CheckpointableApplication {

  /**
   * Captures the complete durable state of the given service as an opaque blob.
   *
   * @return the encoded checkpoint, or null on failure.
   */
  byte[] captureCheckpoint(String serviceName);

  /**
   * Replaces the service's durable state with the given checkpoint. Implementations must only
   * return after the service is ready to execute requests again (e.g., after a container
   * restart), otherwise subsequent request executions can be silently lost.
   *
   * @return true on success; false leaves the caller free to retry later.
   */
  boolean applyCheckpoint(String serviceName, byte[] checkpoint);

  /**
   * Returns a stable digest (e.g., sha256) of the service's current durable state, used to
   * detect divergence between replicas whose applied-write sets are equal. May return null if
   * the digest cannot be computed, which disables digest-based reconciliation.
   */
  byte[] getStateDigest(String serviceName);

  /**
   * Captures a checkpoint too large to ship inline and returns an out-of-band handle (e.g., a
   * {@code edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer} handle) that a peer can redeem
   * with {@link #applyCheckpointHandle(String, String)}. Returns null if unsupported.
   */
  default String captureCheckpointHandle(String serviceName) {
    return null;
  }

  /** Redeems a handle produced by {@link #captureCheckpointHandle(String)}. */
  default boolean applyCheckpointHandle(String serviceName, String handle) {
    return false;
  }
}
