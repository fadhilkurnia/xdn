# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

XDN (eXtended Distribution Network) is a research system from UMass for replicating blackbox stateful services across geographic regions. It extends CDN capabilities to stateful applications using Paxos consensus (GigaPaxos), container orchestration, and multiple consistency models.

## System Requirements

- Linux 5.15+ (x86-64) or macOS (arm64)
- libfuse3 or rsync (for state synchronization)
- Java 21+, Ant 1.10.4+
- Docker 26+, accessible without `sudo`
- Go 1.20+ (for CLI/DNS)

## Build Commands

**Java (core platform):**
```bash
ant clean                        # Remove build artifacts
ant jar                          # Compile and create JAR files (gigapaxos + nio)
./bin/build_xdn_jar.sh           # Wrapper script (runs ant clean + ant jar)
```

**Go CLI — requires Go 1.20+:**
```bash
./bin/build_xdn_cli.sh           # Build CLI binaries (linux/amd64, darwin/arm64)
```

**Rust/C++ filesystem layer:**
```bash
./bin/build_xdn_fuselog.sh       # Build FUSE-based recorder filesystem
```

## Test Commands

```bash
ant xdn-full-tests               # Run all tests (regular + XDN scripted)
ant xdn-unit-tests               # Run only Xdn*Test classes (single JVM)
ant xdn-regular-unit-tests       # Run all tests EXCEPT Xdn*Test pattern
./bin/run_xdn_tests.sh           # Run XDN tests with per-method JVM isolation
./bin/run_xdn_tests.sh XdnFoo    # Run specific test class (pattern match)
VERBOSE=true ./bin/run_xdn_tests.sh  # Stream test output to stdout
ant runtest -Dtest=MyTest        # Run a specific JUnit 4/5 test by class name
ant test                         # Run legacy JUnit 4 reconfiguration tests
```

XDN scripted tests (`run_xdn_tests.sh`) run each test method in a **separate JVM** because each test that calls `XdnTestCluster.start()`/`.close()` needs full resource cleanup. Test output goes to `out/junit5-test-output/`.

**Test infrastructure:** `XdnTestCluster` provisions a local cluster (1 RC + 3 AR on loopback) for integration tests. It manages `/tmp/gigapaxos` and `/tmp/xdn` state directories. Key timeouts: port availability 30s, service readiness 90s, request 10s.

## Formatting and Linting

```bash
./bin/run_java_formatter.sh           # Format Java files in-place (Google Java Format)
./bin/run_java_formatter.sh --check   # Check formatting without modifying (CI mode)
```

Formatter scope: `src/edu/umass/cs/xdn/**/*.java` and `test/**/*.java`. Go code uses standard `gofmt`.

## Local Development

```bash
# Start local control plane (1 Reconfigurator) + 3 ActiveReplicas
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.local.properties start all

# Set control plane and deploy a service
export XDN_CONTROL_PLANE=localhost
xdn launch bookcatalog --image=fadhilkurnia/xdn-bookcatalog --state=/app/data/ --deterministic=true

# Access replicas (service name via XDN header)
curl http://localhost:2300/ -H "XDN: bookcatalog"

# Stop and clean up
sudo ./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.local.properties forceclear all
sudo rm -rf /tmp/gigapaxos /tmp/xdn
```

Local ports: Reconfigurator at :3000, ActiveReplicas at :2000-2002, HTTP proxy at :2300-2302.

## Architecture

### Languages
- **Java**: Core platform — GigaPaxos consensus, replication protocols, reconfiguration, XDN service management
- **Go**: CLI tool (`xdn-cli/`) using Cobra; DNS server (`xdn-dns/`) built on CoreDNS — each is a separate Go module
- **Rust/C++**: Filesystem layer (`xdn-fs/`) for state differential recording via FUSE

### Key Java Packages (`src/edu/umass/cs/`)
- `gigapaxos/` — Paxos consensus protocol implementation
- `reconfiguration/` — Control plane, replica management; entry point: `ReconfigurableNode.java`
- `xdn/` — Core XDN logic (see subpackages and key classes below)
- `nio/` — NIO networking framework
- `primarybackup/`, `chainreplication/`, `causal/`, `eventual/` — Replication protocol variants

### XDN Package Substructure (`src/edu/umass/cs/xdn/`)
- `docker/` — Container management (`DockerComposeManager`)
- `recorder/` — State diff strategies (`AbstractStateDiffRecorder` and four implementations)
- `request/` — HTTP request parsing (`XdnRequestParser`, `XdnHttpRequest`, `XdnHttpRequestBatch`)
- `service/` — Service metadata (`ServiceProperty`, `ServiceComponent`, `ConsistencyModel`)
- `utils/` — Shell execution helpers, hosts file editing
- `interfaces/behavior/` — Request behavior abstractions
- `proto/` — Protocol buffer classes
- `eval/` — Evaluation and experiment utilities

### Key Classes and Request Flow

**Core XDN classes:**
- `XdnGigapaxosApp` — Application layer implementing `Replicable`/`Reconfigurable`. Executes requests by forwarding HTTP to containerized services. Manages service instances per placement epoch, request caching (4096-entry LRU), and state diff recording.
- `XdnReplicaCoordinator` — Wraps multiple replication coordinators (Paxos, PrimaryBackup, ChainReplication, etc.) and routes each service's requests to the coordinator matching its consistency model.
- `DockerComposeManager` — Generates deterministic docker-compose YAML at `/tmp/xdn/compose/{nodeId}/{serviceName}/e{epoch}/`. Handles multi-container services with healthcheck dependencies.
- `ServiceProperty` — Service metadata: name, consistency model, determinism, state directory, components. Uses prefix conventions like `xdn:init:`, `xdn:checkpoint:`.
- `ServiceComponent` — Individual container config: image, ports, healthcheck, entry vs. stateful designation.
- `XdnRequestParser` — Parses raw HTTP into `XdnHttpRequest` or `XdnHttpRequestBatch`.

**Request processing pipeline:**
1. HTTP request arrives at ActiveReplica's HTTP frontend
2. Parsed into `XdnHttpRequest` (single) or `XdnHttpRequestBatch` (compressed batch)
3. `XdnReplicaCoordinator` routes to the appropriate protocol coordinator
4. Coordinator replicates via consensus, then calls `XdnGigapaxosApp.execute()`
5. `XdnHttpForwarderClient` (Netty-based, per-origin connection pool, max 8 connections) forwards to the Docker container
6. State diff captured after execution

### State Synchronization

Four state diff recorder strategies, configured via `XDN_PB_STATEDIFF_RECORDER_TYPE`:
- **RSYNC** — rsync-based incremental transfer
- **ZIP** — ZIP archive snapshots
- **FUSELOG** — Custom FUSE filesystem recording (Linux, C++)
- **FUSERUST** — Rust-based FUSE alternative (Linux)

State stored at `/tmp/xdn/{recorder-type}/state/{nodeId}/{serviceName}/e{epoch}/` (if default config is used). Checkpoint/restore lifecycle: preInitialization → container start → postInitialization → ongoing state diffs → final state capture on stop.

### Control/Data Plane
- **Reconfigurator** (`ReconfigurableNode`): Central coordination, replica placement decisions
- **ActiveReplica** (`XdnReplicaCoordinator` + `XdnGigapaxosApp`): Runs replicated service instances as Docker containers

### Consistency Models
Supported via `ConsistencyModel`: linearizability (default), causal, eventual, pram, and client-centric variants. Each maps to a coordinator in the corresponding protocol package.

### Configuration
- `gigapaxos.properties` — Main deployment config
- `conf/gigapaxos.local.properties` — Local development
- `conf/gigapaxos.cloudlab.properties` — CloudLab cluster deployment
- `testing.properties` — Test configuration (nodes, load, batch settings)

Key config properties: `APPLICATION`, `REPLICA_COORDINATOR_CLASS`, `XDN_PB_STATEDIFF_RECORDER_TYPE`, `HTTP_AR_FRONTEND_BATCH_ENABLED`, `NIO_MAX_PAYLOAD_SIZE` (default 128MB).

## CI Workflows (`.github/workflows/`)
- **ant-build-test.yml**: Build + run `xdn-full-tests` on push/PR to master/main (JDK 21, Docker, FUSE, rsync)
- **xdn-cli-ci.yml**: gofmt check + CLI binary build on changes to `xdn-cli/`
- **google-java-format.yml**: Formatting check on XDN Java file changes

## Conventions
- Java code follows Google Java Style (enforced by formatter)
- Service names: lowercase, no special characters
- Test naming: `Xdn*Test.java` for XDN-specific tests, `*Test.java` for general tests
- Commit messages: short, lowercase, imperative (e.g., `update xdn-cli`, `bugfix formatter`)
- Example services in `services/` — each is a standalone Docker app (bookcatalog, todo, chessapp, etc.)

## Investigating Primary Backup with Wordpress in Cloudlab

### Setup
- Start XDN cluster with gpServer.sh script, using the `conf/gigapaxos.xdn.wordpress-pb.cloudlab.properties` config.
- Always forceclear the cluster when encountering error.
- Change logging level in the investigated class for better clarity.
- Use xdn command with service description in `xdn-cli/examples/wordpress.yaml` to deploy the service.
- Wordpress uses primary-backup replica group, consisting of a single primary and several backups. Only the primary node that runs the containerized service.

### Performance Baselines (CloudLab 3-way replication, WordPress XML-RPC editPost)
- **XDN PB**: ~1500 rps peak throughput (~1200 rps at low latency ≤16ms avg)
- **OpenEBS**: ~800 rps peak throughput
- **MySQL Semi-Sync Replication**: ~500 rps peak throughput

### PB Request Pipeline
```
Client → HTTP frontend (Netty) → writePool dispatch → PBReplicaCoordinator
  → PBM queue → PBM worker thread → XdnGigapaxosApp.execute(XdnHttpRequestBatch)
  → forwardHttpRequestBatchToContainerizedService → WordPress container (via FUSE)
  → doneQueue → capture thread → captureStateDiff → Paxos propose → commit callback
  → async response to client
```

### Key Benchmark Scripts
- `eval/investigate_wp_bottleneck.py` — Per-rate isolation benchmark (forceclear+redeploy between rate points)
- `eval/investigate_wordpress_pb.py` — Base module with cluster management, `GP_JVM_ARGS` tuning
- `eval/get_latency_at_rate.go` — Go load generator with Poisson arrivals
- Workload: XML-RPC `wp.editPost` editing 200 pre-seeded posts in round-robin

### PB Throughput Investigation (2026-03-11) — RESOLVED

**Goal**: XDN PB must beat OpenEBS (~800 rps) and MySQL semi-sync (~500 rps).

**Result**: XDN PB achieves ~1500 rps (peak) and ~1200 rps at low latency (≤16ms avg), beating OpenEBS by ~2x and semi-sync by ~3x.

#### Root Cause

WordPress Apache sends `Connection: close` in HTTP responses. XDN's HTTP proxy (`HttpActiveReplica`) forwarded this header unchanged to the Go load-test client, causing TCP connection teardown after every request. Under sustained load, ephemeral ports accumulated in TIME_WAIT state (~18,608 observed), causing `"cannot assign requested address"` errors and a hard ~500 rps ceiling. This was not an XDN-internal bottleneck — it affected every code path equally (PBM, DDE, BYPASS_COORDINATOR).

#### Working Fixes

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

#### Hypotheses Eliminated (No Impact on Throughput)

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

#### Why DDE Appeared to Work (But Didn't on Sustained Load)

The `___DDE` debug header bypasses PBReplicaCoordinator entirely. In short-duration tests (15s), it appeared to achieve ~1000 rps because TIME_WAIT port accumulation hadn't saturated ephemeral ports yet. On sustained 60s tests, DDE also capped at ~500 rps — confirming the bottleneck was TCP connection management, not any XDN internal pipeline.

#### Debug Flags
- `-DPB_SKIP_REPLICATION=true` — Capture thread skips captureStateDiff + Paxos propose, fires callbacks immediately
- `-DPB_N_PARALLEL_WORKERS=32` — Number of PBM worker threads
- `-DPB_CAPTURE_ACCUMULATION_MS=1` — Capture thread accumulation window
- `-DPB_BYPASS_COORDINATOR=true` — Bypasses PBM pipeline entirely, dispatches to app directly via async path
- `___DDE` HTTP header — Bypasses coordinator entirely, direct execute (legacy debug path)

#### Code Changes Made During Investigation
- `HttpActiveReplica.java`: Connection header override (root cause fix); fully async dispatch (ResponseContext, sendAsyncResponse, timeoutScheduler); BYPASS_COORDINATOR debug flag
- `XdnGigapaxosApp.java`: ByteBuf `copy()` fix; fast-path for single-request batches (skip FJP)
- `PrimaryBackupManager.java`: `PB_SKIP_REPLICATION` bypass flag; changed 7 INFO logs to FINE; workers virtual→platform threads
- `FuselogStateDiffRecorder.java`: Changed "receiving stateDiff" log to FINE
