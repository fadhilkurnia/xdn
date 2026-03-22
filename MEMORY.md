# XDN Project Memory

## WordPress Primary-Backup Investigation (CloudLab)

### Root Cause Chain (Resolved Feb 2026)
The WordPress PB deployment was failing because captureStateDiff crashed after init:

1. **Garbage size header** at first `captureStateDiff` after init: stale payload bytes left in the socket buffer from a previous `send_statediff` response (bytes `79 0A 79 0A EF 8E CD 12` = 1,354,896,220,232,419,961). These stale bytes were misread as the 8-byte LE size header.
2. **Reconnect fails** (ECONNREFUSED): fuselog's `start_listener` loop in socket.rs breaks on ANY non-WouldBlock error from `accept()`, then drops `UnixListener`. Socket file exists but nobody listens.
3. **AssertionError loop**: After reconnect fails, epoch entry is removed from `serviceFsSocket` map but not re-added. All subsequent `captureStateDiff` calls hit `assert socketChannel != null` → `AssertionError` → batching worker crashes → no HTTP responses.

### Fixes Applied
- `FuselogStateDiffRecorder.java:366-369` — replaced asserts with null-return guards (WARNING logs)
- `FuselogStateDiffRecorder.java:708-711` — replaced asserts in `initContainerSync` with SEVERE logs + return
- `FuselogStateDiffRecorder.java` initContainerSync end — added `reconnectSocket` call AFTER sending 'c' to flush stale bytes before first `captureStateDiff`
- `XdnGigapaxosApp.java:1723` — added null check after `captureStateDiff` call (returns "null" prefix)
- `ServiceInstance.java:16` — made `initializationSucceed` `volatile` (JMM visibility fix)

### Fuselog Socket Protocol
- `'g'` → fuselog sends `[8-byte LE size][payload]`
- `'c'` → fuselog clears log, NO response
- `'m'` → checkpoint marker

### Known Remaining Issues
- ~~Primary detection via HTTP header~~ — **FIXED**: `detect_primary_via_docker()` SSHes each AR and runs `docker ps -q`; primary is the node with running containers (10.10.1.3 in 3-way CloudLab). Implemented in `investigate_wp_bottleneck.py`.
- ~~Fuselog 'c' command causes socket listener exit~~ — **FIXED**: C++ fuselog (`xdn-fs/cpp/fuselogv2.cpp`) only supports `'y'` and `'g'` commands. The `'c'` command was unknown and caused the listener to exit. Fixed by replacing all `'c'` calls with `drainStateDiff()` which uses `'g'` to get-and-discard. See C++ Fuselog Fix section below.

### C++ Fuselog Fix (Feb 2026)
The deployment uses **C++ fuselog** (`xdn-fs/cpp/fuselogv2.cpp`), NOT the Rust version.

**C++ fuselog socket protocol** (different from Rust!):
- `'y'` → fuselog ACKs with `"y\n"` (pre-execution barrier)
- `'g'` → fuselog serializes and sends `[8-byte LE size][payload]`, clears state, continues loop
- Any other byte (including `'c'`) → sets `is_waiting=false`, closes socket

**Fix in FuselogStateDiffRecorder.java**: Added `drainStateDiff()` helper that:
1. Sends `'g'`
2. Reads 8-byte LE size header
3. Reads and discards all payload bytes in 64KB chunks
4. Replaces all `write("c")` calls in `postInitialization` and `initContainerSync`

**Fix in `fuselogv2.cpp` `send_gathered_statediffs`**: The function held `sd_mutex` for the ENTIRE socket send (including sending 300MB init statediff), blocking all MySQL FUSE writes. Fixed by:
1. Under `sd_mutex`: snapshot global state into `local_filename_to_fid` and `local_statediffs` via `std::move` (O(1), leaves globals empty)
2. Release `sd_mutex` before any socket I/O
3. Serialize and send from local variables without holding mutex
File: `xdn-fs/cpp/fuselogv2.cpp` lines 371-373 (the move+unlock)

**Lock-free statediff list (Feb 2026)**: Replaced `sd_mutex` (held per write, N+1 acquisitions per MySQL FUSE callback) with:
- `fid_mutex` — protects `filename_to_fid` only (at most once per unique file path)
- `atomic<statediff_action*> statediff_head` — lock-free intrusive linked list via CAS `push_statediff()`
- `send_gathered_statediffs` atomically harvests via `exchange(nullptr)`, does O(n) two-pass pruning over linked list instead of O(n²) backward scan
- `fuselog_destroy()` drains remaining nodes at teardown
- Removed stale "single-threaded FUSE" warning comment
- **Actual results (run10, delay=50ms)**: Peak throughput 9.93 rps (vs 10.47 run9). NO measurable improvement. Execute=168ms at rate=10 (essentially same as run9's 161ms). The bottleneck is MySQL internal lock contention, not sd_mutex. sd_mutex hold time per write was microseconds — too short to matter vs 100ms+ MySQL execution. The lock-free change is still architecturally correct (O(1) harvest, O(n) pruning, no socket-I/O blocking FUSE threads) but doesn't move the throughput ceiling.

**No-coalesce experiment (Feb 2026, REJECTED)**: Tested `is_sd_coalesce = false` (skip pread + byte-diff, capture raw write buffer instead). Results vs baseline run9:
- Peak throughput: **9.89 rps vs 10.47 rps** — slightly *worse*
- Execute at rate=10: **167ms vs 161ms** — unchanged (confirms InnoDB contention, not pread)
- Capture at rate=10: 1.8ms vs 6.5ms — smaller, but only 1% of cycle time either way
- Statediff sizes: 290 KB vs 144 KB at rate=1; 61 KB vs 58 KB at rate=10 (larger without coalescing)
- **Conclusion**: pread on tmpfs is negligible. Bottleneck is MySQL InnoDB lock contention + Paxos serialization. `is_sd_coalesce` reverted to `true`. Results in `eval/results/bottleneck_no_coalesce/`.

**Control flags in `fuselogv2.cpp` (lines 75–78)**:
```cpp
const bool is_sd_capture  = true;
const bool is_sd_coalesce = true;   // pread + byte-diff; false = raw write buffer (tested, no benefit)
const bool is_sd_prune    = true;
const bool is_sd_compress = false;
```

## Throughput Ceiling + Request Batching (Feb 2026)

### Final results — run9 (direct to primary 10.10.1.3, per-rate warmup 15s, rates [1,5,10,12,15,20])
wp.newPost via XML-RPC, CloudLab 3-way, with batching:

| Rate | Actual tput | Avg latency | P50 | P95 |
|---:|---:|---:|---:|---:|
| 1 rps | 1.08 rps | 74.6ms | 66.2ms | 129.5ms |
| 5 rps | 5.24 rps | 97.8ms | 90.1ms | 172.3ms |
| **10 rps** | **10.47 rps (peak)** | 287.6ms | 285.1ms | 429.2ms |
| 12 rps | 6.92 rps ❌ | 588.6ms | 544.8ms | 1019.5ms |
| 15, 20 rps | SKIP (warmup 0 rps) | — | — | — |

**True steady-state capacity ≈ 10.5 rps**. Saturation knee between rate=10 and rate=12.

**Timing breakdown at peak (rate=10)**: execute=161ms, capture=6.5ms, cycle=167.5ms. Capture is only 4% of cycle — MySQL (execute) is the bottleneck, not statediff capture. pred_max=6.0 rps < actual=10.47 rps because batching combines ~5 reqs/cycle.

**StateDiff sizes (run9)**: median 143.7 KB at rate=1, 57.7 KB at rate=10, 175.6 KB at rate=12.

Run 8 → Run 9: peak improved from 9.79 → 10.47 rps; warmup reduced 30s→15s; skip guard prevents MySQL collapse.

Without batching (earlier): rate=10 had avg_latency 4511ms → batching reduces to **287ms** (15x improvement).

### Pipelining + Batching (Confirmed Feb 2026)
Paxos IS pipelined: `propose()` is async, next cycle starts immediately. `PB_BATCH_SIZE=32`, `BATCH_ACCUMULATION_MS=0`. `batchWorkerLoop` uses `queue.take()` then `queue.drainTo()`. `executeXdnBatchOfBatches()` combines all XdnHttpRequests for parallel HTTP + single captureStatediff + single Paxos. Experiments with 5ms delay and compression both REJECTED (each ~9.71 rps vs 10.47 rps baseline).

**All experiments confirmed**: newPost bottleneck is MySQL InnoDB lock contention (~160ms/cycle), not batching or capture.

### Investigation scripts
- `eval/investigate_wp_bottleneck.py` — cluster setup + load sweep. editPost workload via **REST API** (`/?rest_route=/wp/v2/posts/{id}`), rates `[1,100,...,7000]`, warmup 15s. Early termination: avg_ms > 3000 OR throughput < 80% of actual offered rate. Results in `eval/results/final7_pb/`.
- `eval/get_latency_at_rate.go` — Go load client. `-payloads-file FILE` for round-robin payloads; `-urls-file FILE` for round-robin per-request URLs (added for REST API editPost). Both use atomic counter.
- `eval/parse_pb_timing.py` — parses screen.log → timing table + 3 charts + report.txt.
- `eval/plot_load_latency.py` — 2 figures: load_latency.png + offered_actual_throughput.png. X-axis uses `actual_achieved_rate_rps` (not target rate from filename).
- Results in `eval/results/bottleneck/` (newPost run9 XML-RPC baseline), `eval/results/final7_pb/` (REST API, current XDN PB)

### REST API migration (Mar 2026)
All three benchmark scripts migrated from XML-RPC (`wp.editPost`) to WP REST API (`POST /?rest_route=/wp/v2/posts/{id}`).

**Why `/?rest_route=` not `/wp-json/wp/v2/posts`**: The Docker WordPress image ships with Apache `AllowOverride None`, so `.htaccess` rewrites don't work. `/?rest_route=` is WordPress's built-in query-string fallback handled by `index.php` directly — no mod_rewrite needed.

**Auth**: WP Application Passwords (WP 5.6+) over HTTP Basic Auth. WP disables App Passwords on non-HTTPS by default → fix: mu-plugin `add_filter('wp_is_application_passwords_available', '__return_true')` installed via `docker exec` (or `kubectl exec` for K8s/OpenEBS). App password created via `docker exec -i {cid} php` running PHP CLI. Stored in `_wpmod.ADMIN_APP_PASSWORD`.

**`enable_rest_auth_k8s()`** in `investigate_wp_openebs.py`: uses `kubectl exec -i deploy/wordpress` via SSH to K8S_CONTROL_PLANE (since WordPress runs as a K8s pod, not plain Docker).

**final7_pb peak**: ~470 rps (REST API editPost, XDN PB). Lower than XML-RPC results (~1300+ rps) because REST API validates Application Password on every request (extra DB lookup per request).

### editPost workload results (Feb 2026, confirmed InnoDB contention fix)
Switching from `wp.newPost` to `wp.editPost` (200 distinct post IDs, round-robin) eliminated AUTO_INCREMENT table-level mutex contention. Results in `eval/results/bottleneck_editpost/`.

| Rate | editPost tput | editPost p50 | execute avg |
|---:|---:|---:|---:|
| 10 | 10.05 rps | 25.4ms | 22.4ms |
| 50 | 49.90 rps | 31.5ms | 21.1ms |
| 75 | 76.57 rps | 34.7ms | 21.5ms |
| **100** | **100.77 rps ✓** | **35.3ms** | **21.5ms** |
| 150 | 3 rps ❌ | — | — (collapsed) |

**Saturation knee: 100→150 rps** (hard collapse). pred_max ≈ 43 rps; actual = 101 rps because batching yields ~2.3 reqs/cycle at peak. Queue overflow at 150 rps exhausts the batch capacity.

**Key comparison vs newPost run9**:
- execute: 161ms → **21ms** (7.5x faster)
- peak: 10.47 rps → **~100 rps** (~10x higher)
- saturation knee: 10→12 rps → **100→150 rps**
- p50 at peak: 285ms → **35ms**
- statediff size: 57.7 KB → **3–6 KB** at rate=10-100

**Remaining bottleneck** is the single `synchronized(currentEpoch)` batch worker thread in PrimaryBackupManager (cycle=21ms, single-threaded Paxos proposal serialization), not MySQL anymore.

Note: `plot_comparison.py` Phase 13 output shows swapped labels (cosmetic — data is correct).

### Opt 3 — Parallel Execute Workers (N=8, Feb 2026) — COMPLETE
Results in `eval/results/bottleneck_editpost_parallel8/`. Peak: **939 rps at offered=1000** (see final uniform sweep for definitive 1199 rps peak). Latency excellent ≤300 rps (p50=21-33ms), p99 rises at 500+.

**Implementation**:
- `N_PARALLEL_WORKERS = 8` in `PrimaryBackupManager.java`
- `ensureBatchWorker`: starts 8 virtual threads per service (all share one queue)
- `execute()` moved OUTSIDE `synchronized(currentEpoch)` in all 3 execute methods
- Epoch re-validation added after execute before captureStatediff
- **Critical bug fix**: `batchWorkerLoop` catches `Exception` (not just `InterruptedException`) → workers survive Netty `CancellationException` from `FixedChannelPool`. Workers log SEVERE + fail batch + continue. Without this, virtual threads died silently → requests hung → throughput collapsed.
- `plot_triple_comparison.py` DATASETS updated to use `bottleneck_editpost_parallel8` for XDN PB

## Replication Comparison (Feb 2026)

Full details in `memory/replication_comparison.md`.

### newPost baseline peaks
XDN PB ~10.5 rps | OpenEBS ~24 rps | Semi-sync ~15-17 rps (all bottlenecked by InnoDB AUTO_INCREMENT)

### Final uniform-rate sweep (Mar 1, 2026) — results in `eval/results/final_*/`
Rates: `[1,100,...,1200,1500]` — coarse sweep, 13 points.
**Peak**: XDN PB 1199 rps | OpenEBS 1199 rps | Semi-sync 995 rps

### Dense-rate sweep (Mar 1, 2026) — results in `eval/results/final2_*/`
Rates: `[1,100,...,1000,1050,1100,1150,1200,1300,1400,1500,1750,2000]` — 20 points, 50-rps steps around knees.
**Peak**: XDN PB **1364 rps** (rate=2000, still growing) | OpenEBS **1301 rps** (rate=1300) | Semi-sync **1099 rps** (rate=1100)
**Saturation knees**: XDN PB >2000 (not found) | OpenEBS **1300→1400** | Semi-sync **1100→1200**
**XDN PB latency advantage**: p50=24–35ms vs 79–100ms at 100–500 rps; above ~1000 rps all systems show bimodal p50 (~10ms vs 200ms+).
Plots: `eval/results/triple_comparison_latency.png` (p50-only, y-cap 1000ms), `triple_comparison_throughput.png`, `final_report.txt`

### Scripts
- `eval/investigate_wp_semisync.py` — Docker MySQL semi-sync. PRIMARY_HOST=10.10.1.1. Flags: `--skip-teardown`, `--skip-mysql`, `--skip-wp`, `--skip-install`, `--skip-seed`, `--rates COMMA_LIST`. Now uses REST API. Results dir: `final2_semisync`.
- `eval/investigate_wp_openebs.py` — K8s + OpenEBS Mayastor. Flags: `--skip-k8s`, `--skip-disk-prep`, `--skip-openebs`, `--skip-deploy`, `--skip-install`, `--skip-seed`, `--rates COMMA_LIST`. Now uses REST API + `enable_rest_auth_k8s()`. NOTE: `--skip-openebs` also skips DiskPool+StorageClass creation. Results dir: `final2_openebs`.
- `eval/plot_triple_comparison.py` — DATASETS uses `final2_pb`/`final2_openebs`/`final2_semisync`; p50-only latency plot, y-axis capped at 1000ms.
- See `memory/replication_comparison.md` for full table and OpenEBS setup fixes

### Infrastructure lessons
- `iptables -X` deletes Docker's DOCKER-FORWARD chain → `docker network create` fails silently (exit 0) → fix: `sudo systemctl restart docker`
- K8s `systemctl enable --now kubelet` can briefly drop node off network → added `_ssh_with_retry` (exit-255, 4 retries, 15s delay) in `install_on`
- After `kubeadm reset`, restart Docker to restore its iptables chains
- `diskpools.openebs.io` CRD may not be Established immediately after Helm install; `kubectl wait --timeout=300s` is the right gate
- **MySQL transient startup failure**: after mass `docker rm -f`, a new `docker run mysql:8.4.0` may silently exit before port 3306 opens (Docker networking edge case). Container disappears from `docker ps -a`. Fix: just retry — second attempt succeeds.

## NIO and Statediff Bottlenecks (Feb 2026, All RESOLVED)

All resolved. Key fixes:
1. **Async backup apply** (PrimaryBackupManager.java): per-service `newSingleThreadExecutor` for async `applyStateDiff()` (was blocking Paxos execution thread)
2. **NIO SELECT_TIMEOUT** (NIOTransport.java:547): use 1ms when `congested` map non-empty (was 2000ms → 34s drain for 6MB)
3. **NIO ZLIB inadvertently enabled** (CRITICAL): `setMaxPayloadSize(768MB)` left `compressionThreshold=4MB < MAX_PAYLOAD_SIZE` = compression ON. Fixed by: `if (compressionThreshold == MAX_PAYLOAD_SIZE) compressionThreshold = max(compressionThreshold, size)`
4. Assert crashes in `XdnGigapaxosApp.java`: replaced with null-return guards + WARNING log

### Key File Locations
- `src/edu/umass/cs/xdn/recorder/FuselogStateDiffRecorder.java` — fuselog socket manager
- `src/edu/umass/cs/xdn/XdnGigapaxosApp.java` — `captureStatediff` ~line 1700, `nonDeterministicInitialization` ~line 2459
- `src/edu/umass/cs/xdn/service/ServiceInstance.java` — `initializationSucceed` field
- `xdn-fs/rust/fuselog_core/src/socket.rs` — `start_listener`, `send_statediff`, `clear_statediff`
- `xdn-fs/rust/fuselog_core/src/lib.rs` — FUSE callbacks using `STATEDIFF_LOG.lock().unwrap()`
- `eval/investigate_wordpress_pb.py` — investigation script (run from eval/ dir)

### Deployment Pattern (CloudLab 3-way)
```bash
ant jar
for host in 10.10.1.1 10.10.1.2 10.10.1.3 10.10.1.4; do
  scp jars/gigapaxos-1.0.10.jar $host:/users/fadhil/xdn/jars/gigapaxos-1.0.10.jar &
done
wait
cd eval && python3 investigate_wordpress_pb.py
```
Config: `conf/gigapaxos.xdn.3way.cloudlab.properties`
ARs: 10.10.1.1-3, RC: 10.10.1.4, HTTP proxy port: 2300

## Feedback

- [Commutativity is resource-scoped](.claude/projects/-users-fadhil-xdn/memory/feedback_commutativity.md) — Two requests are only commutative if they target DIFFERENT resources (e.g., different book IDs)
- [Instrumentation logging level](.claude/projects/-users-fadhil-xdn/memory/feedback_instrumentation_logging.md) — Use INFO/FINE for instrumentation, never WARNING. Change logging.properties to see them.
