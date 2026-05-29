# WordPress Primary-Backup Throughput Investigation

This document records the full investigation into XDN Primary-Backup (PB) throughput
with WordPress on CloudLab. CLAUDE.md keeps a condensed summary; this file holds the
complete log (hypotheses eliminated, working fixes, and code changes).

## Setup
- Start the XDN cluster with `gpServer.sh`, using `conf/gigapaxos.xdn.wordpress-pb.cloudlab.properties`.
- Always `forceclear` the cluster when encountering an error.
- Change the logging level in the investigated class for better clarity.
- Deploy the service with the `xdn` command using `xdn-cli/examples/wordpress.yaml`.
- WordPress uses a primary-backup replica group: a single primary and several backups.
  Only the primary node runs the containerized service.

## Performance Baselines (CloudLab 3-way replication, WordPress XML-RPC editPost)
- **XDN PB**: ~1500 rps peak throughput (~1200 rps at low latency ≤16ms avg)
- **OpenEBS**: ~800 rps peak throughput
- **MySQL Semi-Sync Replication**: ~500 rps peak throughput

## PB Request Pipeline
```
Client → HTTP frontend (Netty) → writePool dispatch → PBReplicaCoordinator
  → PBM queue → PBM worker thread → XdnGigapaxosApp.execute(XdnHttpRequestBatch)
  → forwardHttpRequestBatchToContainerizedService → WordPress container (via FUSE)
  → doneQueue → capture thread → captureStateDiff → Paxos propose → commit callback
  → async response to client
```

## Key Benchmark Scripts
- `eval/investigate_wp_bottleneck.py` — Per-rate isolation benchmark (forceclear+redeploy between rate points)
- `eval/investigate_wordpress_pb.py` — Base module with cluster management, `GP_JVM_ARGS` tuning
- `eval/get_latency_at_rate.go` — Go load generator with Poisson arrivals
- Workload: XML-RPC `wp.editPost` editing 200 pre-seeded posts in round-robin

## PB Throughput Investigation (2026-03-11) — RESOLVED

**Goal**: XDN PB must beat OpenEBS (~800 rps) and MySQL semi-sync (~500 rps).

**Result**: XDN PB achieves ~1500 rps (peak) and ~1200 rps at low latency (≤16ms avg), beating OpenEBS by ~2x and semi-sync by ~3x.

### Root Cause

WordPress Apache sends `Connection: close` in HTTP responses. XDN's HTTP proxy (`HttpActiveReplica`) forwarded this header unchanged to the Go load-test client, causing TCP connection teardown after every request. Under sustained load, ephemeral ports accumulated in TIME_WAIT state (~18,608 observed), causing `"cannot assign requested address"` errors and a hard ~500 rps ceiling. This was not an XDN-internal bottleneck — it affected every code path equally (PBM, DDE, BYPASS_COORDINATOR).

### Working Fixes

1. **Connection header override** (root cause fix) — `HttpActiveReplica.java` lines 1251-1261:
   The proxy now overrides the container's `Connection` header with the client's keep-alive preference. Without this, containers that send `Connection: close` force the client to tear down TCP connections after every request.
   ```java
   if (isKeepAlive) {
       httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
   } else {
       httpResponse.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
   }
   ```

2. **ByteBuf copy() instead of retainedDuplicate()** — `XdnGigapaxosApp.java` line 2368:
   `retainedDuplicate()` shares refCnt with the original ByteBuf. When `XdnHttpForwarderClient` retries (MAX_RETRIES=1), each `executeOnce()` consumes net -1 from the shared refCnt, causing `IllegalReferenceCountException`. Fix: use `copy()` for independent refCnt.
   ```java
   ByteBuf copiedContent = content != null && content.isReadable()
       ? content.copy()       // was: content.retainedDuplicate()
       : Unpooled.EMPTY_BUFFER;
   ```

3. **Fully async dispatch** — `HttpActiveReplica.java`:
   Replaced blocking `future.get()` on writePool threads with a fully async pattern. Key components:
   - `ResponseContext` record (line 412): captures all state needed for async response
   - `timeoutScheduler` (line 401): single daemon thread for timeout enforcement
   - `sendAsyncResponse` helper (line 451): thread-safe response writing via `AtomicBoolean` gate (exactly-once semantics)
   - Channel close listener: releases ByteBufs if client disconnects before response
   - writePool threads now return in microseconds (just dispatch), making thread exhaustion impossible

### Hypotheses Eliminated (No Impact on Throughput)

All hypotheses below did NOT improve throughput beyond ~500 rps. The ~500 rps ceiling persisted because the real bottleneck was the `Connection: close` header causing ephemeral port exhaustion (see Root Cause above).

| # | Hypothesis | Test Method | Result |
|---|-----------|-------------|--------|
| 1 | WordPress/MySQL container is slow | `ab` direct container test at c=32 | 1237 rps through FUSE — NOT bottleneck |
| 2 | FUSE filesystem overhead | `ab` test goes through FUSE mount | Same 1237 rps — NOT bottleneck |
| 3 | HTTP forwarder pool size | Pool is 128 connections (`XDN_HTTP_MAX_POOL_SIZE=128`) | NOT bottleneck |
| 4 | Capture thread saturation | `doneQueueRemaining` avg=0, capacity 859 rps | NOT bottleneck |
| 5 | Too few PBM workers | 32 workers × 20ms/req = 1600 rps capacity, only ~10-13 active | NOT bottleneck |
| 6 | ForkJoinPool indirection | Fixed batch-of-1 fast path in `forwardHttpRequestBatchToContainerizedService` | No improvement |
| 7 | Excessive INFO logging | Changed 8 hot-path logs from INFO→FINE (~1875 lines/s saved) | No improvement |
| 8 | Frontend HTTP batcher | Disabled `HTTP_AR_FRONTEND_BATCH_ENABLED=false` | No improvement |
| 9 | Apache MaxRequestWorkers limit | Only 11 active workers at 500 rps (limit=150) | NOT bottleneck |
| 10 | Go client connection limits | `MaxIdleConnsPerHost=1024` | NOT bottleneck |
| 11 | Paxos consensus overhead | `PB_SKIP_REPLICATION=true` (bypass captureStateDiff + propose) | **Still 500 rps!** |
| 12 | Virtual thread pinning | Switched PBM workers from virtual→platform threads | **Still 500 rps!** |
| 13 | PBM coordinator pipeline overhead | `PB_BYPASS_COORDINATOR=true` (bypass PBM entirely, use async dispatch) | **Still ~489 rps at 1000 rps offered** |
| 14 | FUSELOG recorder overhead | Switched to RSYNC recorder (no FUSE) | **Still ~500 rps** |
| 15 | DDE bypass as sustained solution | `___DDE` header appeared to hit ~1000 rps in 15s tests, but sustained 60s runs also cap at ~500 rps | Same root cause (Connection: close) |

### Why DDE Appeared to Work (But Didn't on Sustained Load)

The `___DDE` debug header bypasses PBReplicaCoordinator entirely. In short-duration tests (15s), it appeared to achieve ~1000 rps because TIME_WAIT port accumulation hadn't saturated ephemeral ports yet. On sustained 60s tests, DDE also capped at ~500 rps — confirming the bottleneck was TCP connection management, not any XDN internal pipeline.

### Debug Flags
- `-DPB_SKIP_REPLICATION=true` — Capture thread skips captureStateDiff + Paxos propose, fires callbacks immediately
- `-DPB_N_PARALLEL_WORKERS=32` — Number of PBM worker threads
- `-DPB_CAPTURE_ACCUMULATION_MS=1` — Capture thread accumulation window
- `-DPB_BYPASS_COORDINATOR=true` — Bypasses PBM pipeline entirely, dispatches to app directly via async path
- `___DDE` HTTP header — Bypasses coordinator entirely, direct execute (legacy debug path)

### Code Changes Made During Investigation
- `HttpActiveReplica.java`: Connection header override (root cause fix); fully async dispatch (ResponseContext, sendAsyncResponse, timeoutScheduler); BYPASS_COORDINATOR debug flag
- `XdnGigapaxosApp.java`: ByteBuf `copy()` fix; fast-path for single-request batches (skip FJP)
- `PrimaryBackupManager.java`: `PB_SKIP_REPLICATION` bypass flag; changed 7 INFO logs to FINE; workers virtual→platform threads
- `FuselogStateDiffRecorder.java`: Changed "receiving stateDiff" log to FINE
