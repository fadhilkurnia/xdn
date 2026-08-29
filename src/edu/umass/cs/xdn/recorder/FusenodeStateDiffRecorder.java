package edu.umass.cs.xdn.recorder;

/**
 * State-diff recorder backed by the low-level {@code fusenode} FUSE filesystem.
 *
 * <p>{@code fusenode} is an inode-based recorder that reuses the exact same socket protocol as the
 * high-level {@code fuselog} recorder and the same {@code fuselog-apply} apply binary (FLG3
 * format). It reads {@code FUSENODE_*} env with {@code FUSELOG_*} fallback, so the entire
 * launch/socket/capture/apply pipeline in {@link FuselogStateDiffRecorder} is reused unchanged --
 * only the mount binary and the writeback-cache default differ.
 *
 * <p>Writeback is default-ON here: the kernel buffers/defers container writes, and it engages
 * fusenode's hardlink-correct writeback path (correct under hardlink+truncate). Override at runtime
 * with {@code -DFUSELOG_WRITEBACK_CACHE=false} on the AR JVM.
 */
public class FusenodeStateDiffRecorder extends FuselogStateDiffRecorder {

  public FusenodeStateDiffRecorder(String nodeID) {
    super(
        nodeID,
        "/usr/local/bin/fusenode",
        "/usr/local/bin/fuselog-apply", /*writebackDefault*/
        true);
  }
}
