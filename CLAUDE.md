# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview
XDN is a research system from UMass for replicating blackbox 
stateful services across geographic regions. It extends CDN capabilities to 
stateful applications using Paxos consensus (GigaPaxos) & other coordination 
protocols, container orchestration, and multiple consistency models.

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

**Rust/C++ filesystem layer (Linux only):**
```bash
./bin/build_xdn_fuselog.sh       # Build both C++ (fuselog, fuselog-apply) and Rust (fuserust, fuserust-apply)
./bin/build_xdn_fuselog.sh cpp   # C++ only
./bin/build_xdn_fuselog.sh rust  # Rust only
./bin/build_xdn_fuselog.sh test  # Build and run C++ GoogleTest unit tests (needs libgtest-dev)
./bin/build_xdn_fuselog.sh bench # Build and run the compute_diff microbenchmark
```

> **Note**: compiled binaries in `bin/` (`xdn-darwin-arm64`, `xdn-linux-amd64`,
> `fuselog`, `fuselog-apply`, `fuserust`, `fuserust-apply`) are gitignored —
> developers build them locally via the scripts above. Only shell scripts
> and `bin/xdnd` (also a shell script) are checked in.

## Test Commands

```bash
ant xdn-full-tests               # Run all tests (regular + XDN scripted)
ant xdn-unit-tests               # Run only Xdn*Test classes (single JVM)
ant xdn-regular-unit-tests       # Run all tests EXCEPT Xdn*Test pattern
./bin/run_xdn_tests.sh           # Run XDN tests with per-method JVM isolation
./bin/run_xdn_tests.sh XdnFoo    # Run specific test class (pattern match)
VERBOSE=true ./bin/run_xdn_tests.sh  # Stream test output to stdout
ant runtest -Dtest=MyTest        # Run a specific JUnit 4/5 test by class name
ant test                         # End-to-end reconfiguration tests (TESTReconfigurationClient)

# Single-JVM multi-node Paxos with RSM-invariant assertion on every request:
bash test-gigapaxos/testPaxosMain/testPaxosMain.sh gigapaxos.properties \
    test-gigapaxos/testPaxosMain/testing.ci.properties   # CI-sized config (~30s)
bash test-gigapaxos/testPaxosMain/testPaxosMain.sh gigapaxos.properties \
    testing.properties                                   # Full-load config (~20m)
```

**Running a single test method from the CLI** (via JUnit ConsoleLauncher; useful when `ant runtest` is too coarse):
```bash
ant clean jar xdn-compile-tests
java -cp "lib/junit-platform-console-standalone-1.11.1.jar:build/classes:build/test-classes:lib/*" \
  org.junit.platform.console.ConsoleLauncher execute \
  --select-method edu.umass.cs.xdn.XdnGetReplicaInfoTest#testGetReplicaInfoSingleService \
  --details=verbose
```

XDN scripted tests (`run_xdn_tests.sh`) run each test method in a 
**separate JVM** because each test that calls 
`XdnTestCluster.start()`/`.close()` needs full resource cleanup. 
Test output goes to `out/junit5-test-output/`.

**fuselog tests (`xdn-fs/test/`, Linux only):** layered correctness harnesses
for the FUSE state-diff recorder, all driven by Python scripts that build on
the C++ `fuselog`/`fuselog-apply` binaries:
- **L1** — C++ GoogleTest unit tests (`build_xdn_fuselog.sh test`); pure C++,
  no FUSE mount.
- **L3** — `fuzz_differential.py`: random fs ops on a live mount vs. a plain-dir
  POSIX oracle, replay the statediff, assert `tree(A) == tree(B) == tree(C)`.
  Seed is printed at startup; reproduce a failure with `--seed N`.
- **L4** — `fuzz_concurrent.py` (disjoint subtrees) / `fuzz_concurrent_overlap.py`
  (shared path pool; ~5% known flake rate from a fid-reuse race).
- **L5** — `fuzz_db.py --db {sqlite,postgres,mysql,mariadb,mongodb}`: run a real
  DB on the mount, SIGKILL it, replay onto a fresh dir, assert committed
  transactions round-trip. Docker-based DBs need `sudo docker` and
  `user_allow_other` in `/etc/fuse.conf`.
Failure artifacts land in `/tmp/fuselog-fuzz-fail-<seed>/`. See
`xdn-fs/test/README.md` for details.

**Test infrastructure:** `XdnTestCluster` provisions a local cluster 
(1 RC + 3 AR on loopback) for integration tests. 
It manages `/tmp/gigapaxos` and `/tmp/xdn` state directories. 
Key timeouts: port availability 30s, service readiness 90s, request 10s.

## Formatting and Linting

```bash
./bin/run_java_formatter.sh           # Format Java files in-place (Google Java Format)
./bin/run_java_formatter.sh --check   # Check formatting without modifying (CI mode)
```

Formatter scope: `src/edu/umass/cs/xdn/**/*.java` and `test/**/*.java`. 
Go code uses standard `gofmt`.

## Local Development

```bash
# Start local control plane (1 Reconfigurator) + 3 ActiveReplicas
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.xdn.local.properties start all

# Set control plane and deploy a service
export XDN_CONTROL_PLANE=localhost
xdn launch bookcatalog --image=fadhilkurnia/xdn-bookcatalog --state=/app/data/ --deterministic=true

# Access replicas (service name via XDN header)
curl http://localhost:2300/ -H "XDN: bookcatalog"

# Alternative: add entries to /etc/hosts (bookcatalog.ar{0,1,2}.xdn.io → 127.0.0.1)
# and drop the XDN header: curl http://bookcatalog.ar0.xdn.io:2300/

# Stop and clean up
sudo ./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.xdn.local.properties forceclear all
sudo rm -rf /tmp/gigapaxos /tmp/xdn
```

Local ports: Reconfigurator at :3000, ActiveReplicas at :2000-2002, HTTP proxy at :2300-2302.

## CloudLab Cluster Deployment

Use the `bin/xdnd` shell script (driver-machine orchestrator) for multi-host deployment:

```bash
./bin/xdnd init-driver                                                          # prepare driver machine
./bin/xdnd dist-init -config=gigapaxos.properties -ssh-key=/ssh/key -username=u # prepare remote hosts
./bin/xdnd start-all -config=gigapaxos.properties -ssh-key=/ssh/key -username=u # launch all nodes
# optional: dist-init-observability for Prometheus/Grafana stack
```

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
- `protocoltask/` — Protocol task orchestration framework used by coordinators
- `primarybackup/`, `chainreplication/`, `causal/`, `eventual/`, `sequential/`, `pram/`, `clientcentric/`, `txn/` — Replication/consistency protocol variants (each maps to a `ConsistencyModel`)

### XDN Package Substructure (`src/edu/umass/cs/xdn/`)
- `docker/` — Container management (`DockerComposeManager`)
- `recorder/` — State diff strategies (`AbstractStateDiffRecorder` and four implementations)
- `request/` — HTTP request parsing (`XdnRequestParser`, `XdnHttpRequest`, `XdnHttpRequestBatch`) and internal request types (`XdnGetReplicaInfoRequest`, `XdnStopRequest`, `XDNHttpForwardRequest`, `XDNStatediffApplyRequest`)
- `service/` — Service metadata (`ServiceProperty`, `ServiceComponent`, `ConsistencyModel`, `DeploymentMode`, `ServiceInstance`, `RequestMatcher`)
- `cluster/` — Self-clustering ("StatefulSet-style") services: `StatefulClusterReplicaCoordinator`, `ClusterTopology`, `ClusterTopologyAware`, `SwarmOverlayManager` (see "Cluster-mode services" below)
- `utils/` — Shell execution helpers, hosts file editing
- `interfaces/behavior/` — Request behavior abstractions (commutative, key-commutative, etc.)
- `proto/` — Protocol buffer classes
- `eval/` — Evaluation and experiment utilities
- `experiment/` — Experiment harness code and ad-hoc clients (e.g. `XdnBookCatalogAppClient`)

### Top-level XDN helpers (in `src/edu/umass/cs/xdn/`)
- `XdnServiceProperties` — service-property helpers and defaults consumed elsewhere
- `XdnHttpRequestBatcher` — batches HTTP requests at the AR frontend when batching is enabled
- `XdnHttpForwarderClient` — Netty-based HTTP client that forwards parsed requests to the containerized service (per-origin pool, max 8 connections)
- `XdnGeoDemandProfiler`, `XdnReplicaPlacementProfile` — geo-demand tracking and replica placement policies
- `XdnServiceInitialStateValidator`, `XdnServiceNumReplicasExtractor` — launch-time validators/extractors
- `HttpDebugProxy`, `HttpDebugWaitProxy`, `HttpDebugIndirectProxy` — diagnostic HTTP proxies used in debugging/tests

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
4. Coordinator replicates via consensus or other protocols depending on the requested consistency model, then calls `XdnGigapaxosApp.execute()`
5. `XdnHttpForwarderClient` (Netty-based, per-origin connection pool, max 8 connections) forwards to the Docker container
6. State diff captured after execution (for non-deterministic service)

### State Synchronization

Four state diff recorder strategies, configured via `XDN_PB_STATEDIFF_RECORDER_TYPE`:
- **RSYNC** — rsync-based incremental transfer
- **ZIP** — ZIP archive snapshots
- **FUSELOG** — Custom FUSE filesystem recording (Linux, C++)
- **FUSERUST** — Rust-based FUSE alternative (Linux)

State stored at `/tmp/xdn/{recorderType}/state/{nodeId}/{serviceName}/e{epoch}/` 
(if default config is used). 
Checkpoint/restore lifecycle: preInitialization → container start → 
postInitialization → ongoing state diffs → final state capture on stop.

`rsync` recorder is preferred for local development & testing as it works both 
in Mac and Linux.
Real measurements and production should use `fuselog` or `fuserust`.

### Control/Data Plane
- **Reconfigurator** (`ReconfigurableNode`): Central coordination, replica placement decisions
- **ActiveReplica** (`XdnReplicaCoordinator` + `XdnGigapaxosApp`): Runs replicated service instances as Docker containers

### Consistency Models
Supported via `ConsistencyModel`: linearizability (default), sequential, causal,
eventual, pram, and client-centric variants. 
Each maps to a coordinator in the corresponding protocol package.

### Cluster-mode services (`xdn cluster launch`)
A second deployment shape alongside the regular blackbox-replicated services:
the container handles its own coordination/consensus, and XDN only provides
placement, stable identity, peer-discovery networking, and request routing.
Analogous to a Kubernetes StatefulSet. Selected via `"mode":"cluster"` in the
`xdn:init:<JSON>` initial state (`DeploymentMode.CLUSTER`).

**Coordinator.** `StatefulClusterReplicaCoordinator` (in
`edu.umass.cs.xdn.cluster`) is a **pass-through** coordinator —
`coordinateRequest` just calls `app.execute` on the local replica's container.
No Paxos, no broadcast. Selected as the very first branch in
`XdnReplicaCoordinator.inferCoordinatorByProperties` (before any
determinism/consistency checks, since both are meaningless for cluster mode).

**Identity.** Replicas get deterministic ordinals 0..N-1 by sorting the
`Set<NodeIDType> nodes` lexicographically. The coordinator pushes a
`ClusterTopology` (ordinal, size, phase) to `XdnGigapaxosApp` via the
`ClusterTopologyAware` interface before `restore()`. Each container is started
with `--hostname=replica-<ordinal> --network-alias=replica-<ordinal>` on a
swarm-wide overlay (`xdn-cluster-<svc>`), so peers find each other clusterwide
by name through Docker's embedded DNS.

**Env contract** injected into every cluster container:
`XDN_CLUSTER_ORDINAL`, `XDN_CLUSTER_SIZE`, `XDN_CLUSTER_SELF`,
`XDN_CLUSTER_PEERS`, `XDN_CLUSTER_PEER_PORT`, `XDN_CLUSTER_PHASE`
(`bootstrap` at epoch 0; `join` reserved for the not-yet-wired
reconfiguration path).

**Single-image vs. multi-component.** A cluster service can be one image
(e.g. etcd alone) or multiple components in a pod-style group (e.g.
bookcatalog frontend + a local rqlite sidecar per replica). For multi-
component, the **stateful** component is the cluster member (attached to the
overlay); the other components are **sidecars** started with `--network=
container:<cluster-member-name>` so they share its network namespace and
reach the cluster member at `127.0.0.1:<peer-port>`. The entry component's
port gets published to the host on the cluster member's `docker run` so
XDN's HTTP frontend can route to whichever sidecar is the entry.

**Docker Swarm overlay is required.** `bin/xdnd dist-init` now bootstraps a
swarm across all configured XDN hosts (all nodes join as managers).
`SwarmOverlayManager.ensureOverlay()` creates `xdn-cluster-<svc>` on-demand
(`-d overlay --attachable`); the call is idempotent and races between ARs
are treated as success.

**Statediff is bypassed** for cluster services — they handle their own
replication, so XDN-side state-tar capture is a no-op and FUSE/rsync are
skipped.

**Reference images** under `services/`:
- `services/etcd-cluster/` — single-image cluster (etcd), entrypoint maps
  `XDN_CLUSTER_*` → etcd's `--initial-cluster`, `--initial-cluster-state`, etc.
- `services/rqlite-cluster/` — single-image cluster (rqlite), same pattern.
- `services/bookcatalog-rqlite-cluster.yaml` — multi-component spec:
  `bookcatalog` (entry, talks to `127.0.0.1:4001`) + `rqlite` (stateful,
  cluster member on the overlay).

**Reconfiguration is deferred** (Component 6 in the design plan). The
`xdn:final:` cluster path in `XdnGigapaxosApp.createServiceInstance` throws
`UnsupportedOperationException`; cluster placements are effectively pinned
at epoch 0. The design intent is that image-specific add/remove-replica
hooks ship in the per-service YAML (e.g. `etcdctl member add` commands),
*not* as Java adapter classes — see the deleted-but-documented adapter
scaffolding history in commits `37b23c4f` → `79c90ace`.

**Helper script.** `bin/xdn-cluster-up.sh` brings up XDN across the
configured CloudLab hosts (1 RC + 3 ARs by default) and optionally launches
a demo cluster service:
```bash
bin/xdn-cluster-up.sh                  # just start XDN
bin/xdn-cluster-up.sh --launch-etcd
bin/xdn-cluster-up.sh --launch-bookcat
```

### Node Geolocation
Each node's `(lat, lon)` can be set in the gigapaxos properties file (see
`conf/gigapaxos.xdnlat.template.properties`) and is parsed via
`PaxosConfig` / `DefaultNodeConfig` into the `edu.umass.cs.nio.interfaces.Geolocation`
type. It flows through `ReconfigurableNodeConfig` and `Reconfigurator` into
`GetReplicaPlacementRequest`, and is surfaced by `xdn service info`.

### `xdn-cli` subcommands (`xdn-cli/cmd/`)
Cobra-based CLI. Top-level verbs include `launch`, `status`, `check`, and two command groups: `service` and `cluster`.
- `launch` — deploy a regular blackbox-replicated service. Flags: `--image`, `--state`, `--deterministic`, `--consistency`, `--num-replicas`, `--min-replicas`, `--max-replicas`, `--env`, `--port`, `--methods`, `-f file.yaml`.
- `service` — `info`, `destroy`, `move` (relocate; drives synchronous paxos leader change), `leader` (inspect/set the paxos leader).
- `cluster launch` — deploy a self-clustering service (`mode:cluster`). Flag-based for single-image (`--image --port --peer-port --state --num-replicas --env`) or `-f spec.yaml` for multi-component clusters. See "Cluster-mode services" above.

Mutating subcommands prompt for yes/no confirmation on stdin. The shared CREATE-request HTTP plumbing lives in `sendCreateRequest()` in `launch.go` and is reused by `cluster.go`.

### XDN-internal URL params
URL query parameters prefixed with `_xdn` (e.g. `_xdnsvc`) are consumed at the XDN/proxy layer and must be stripped from the request URI before it is forwarded to the containerized service. `_xdnsvc` provides the service name directly as a URL param and is used as a developer-experience alternative to setting the `XDN:` header.

### Configuration
- `gigapaxos.properties` — Main deployment config
- `conf/gigapaxos.xdn.local.properties` — Local development (1 RC + 3 AR on loopback)
- `conf/gigapaxos.xdn.local.geodemand.properties` — Local cluster with geo-located ARs; used by the geo-demand smoke CI to exercise demand-driven replica reconfiguration
- `conf/gigapaxos.cloudlab.properties` — CloudLab cluster deployment
- `conf/gigapaxos.xdn.cloudlab.local.{10,13}nodes.properties` — Multi-node CloudLab variants
- `conf/gigapaxos.xdn.3way.properties`, `conf/gigapaxos.xdn.3way.cloudlab.properties` — 3-way replication variants (local + cloudlab)
- `conf/gigapaxos.xdn.tpcc-java-pb.cloudlab.properties`, `conf/gigapaxos.xdn.wordpress-pb.cloudlab.properties` — App-specific primary-backup eval configs
- `conf/gigapaxos.xdn.cluster-launch.cloudlab.properties` — 4-host CloudLab config used by `bin/xdn-cluster-up.sh` (1 RC + 3 ARs, numeric node IDs)
- `testing.properties` — Test configuration (nodes, load, batch settings)

Key config properties: `APPLICATION`, `REPLICA_COORDINATOR_CLASS`, `XDN_PB_STATEDIFF_RECORDER_TYPE`, `HTTP_AR_FRONTEND_BATCH_ENABLED`, `NIO_MAX_PAYLOAD_SIZE` (default 128MB).

### Cluster Orchestration (`bin/xdnd`)
For multi-machine/CloudLab deployments, `bin/xdnd` drives remote setup and lifecycle over SSH: `xdnd init-driver` on the driver machine, then `xdnd dist-init -config=... -ssh-key=... -username=...` to initialize remotes, and `xdnd start-all ...` to start xdn instances fleet-wide. `xdnd dist-init-observability` is the optional observability bootstrap.

`xdnd dist-init` also bootstraps a Docker swarm across the hosts (all as managers) — required by cluster-mode services for the cross-host attachable overlay networks. `init_docker_swarm()` is idempotent.

For the cluster-launch demo specifically, `bin/xdn-cluster-up.sh` is a thinner helper that just starts ReconfigurableNode procs on the 4 configured hosts and optionally launches a demo service; it skips the dependency-install + image-build steps and assumes the hosts are already prepped.

### Conventions used by this codebase
- **Numeric GigaPaxos node IDs.** Configs use `reconfigurator.0=…`, `active.1=…`, `active.2=…`, `active.3=…` — *not* `RC0`/`AR0`/etc. String IDs cause `RequestPacket` to fall back to JSON-encoded packets (~2× wire bytes for Paxos-class protocols). The legacy assertion `requestPacket.getEntryReplica() > 0` at `PaxosInstanceStateMachine.java:1791` is **relaxed to `>= 0`** in this branch because node 0 (the reconfigurator under our numeric scheme) is a legitimate entry replica — see commit `64f00890`.

## CI Workflows (`.github/workflows/`)
- **ant-build-test.yml**: Parallelized XDN test suite on push/PR to master/main. Two job groups: (1) **unit-tests** — JDK-only (no Docker/FUSE/rsync), runs `ant xdn-regular-unit-tests` plus the Xdn*Test classes that don't drive `XdnTestCluster` (`XdnHttpRequestTest`, `XdnHttpRequestBatchTest`, `XdnGeoDemandProfilerTest`). (2) **xdn-integration** — matrix with one entry per `XdnTestCluster`-using class (`XdnEventualConsistencyBatchingTest`, `XdnGetReplicaInfoTest`, `XdnMultiServiceTest`, `XdnPerReplicaRequestTest`, `XdnSetCoordinatorNodeTest`, `XdnTaggedImageLaunchTest`), each on its own runner with full Docker/FUSE/rsync setup. Per-runner isolation is mandatory because `XdnTestCluster.java:44-49` hardcodes loopback ports (RC :3000, AR :2000-2002, proxy :2300-2302) and `/tmp/{gigapaxos,xdn}` — two clusters can't coexist on one host.
- **gigapaxos-correctness.yml**: Parallel matrix of four GigaPaxos correctness tests — `RequestPacketTest`, `E2ELatencyAwareRedirectorTest`, `ant test` (TESTReconfigurationClient end-to-end), and `TESTPaxosMain` (single-JVM multi-node Paxos with `assertRSMInvariant` enabled on every request). JDK-only, no Docker/FUSE/rsync — each job finishes in under a minute and they run concurrently.
- **fuselog-tests.yml**: layered fuselog correctness suite (L1 GoogleTest unit, L3 differential, L4 concurrent disjoint/overlap, L5 SQLite + Docker DB matrix). Path-filtered — runs only when `xdn-fs/**`, `bin/build_xdn_fuselog.sh`, or the workflow itself changes. Each job builds fuselog independently; an `all-fuselog-tests` aggregator gates branch protection.
- **xdn-cli-ci.yml**: gofmt check + CLI binary build on changes to `xdn-cli/`
- **google-java-format.yml**: Formatting check on XDN Java file changes
- **geo-demand-smoke.yml**: End-to-end smoke test for demand-driven replica reconfiguration — boots a local cluster from `conf/gigapaxos.xdn.local.geodemand.properties`, drives biased traffic via `eval/geo_demand_smoke.py`, and asserts the active set advances past `epoch=0` and contains the expected us-east-1 nodes (including leader)
- **test-report.yml**: JUnit test-result reporter (currently disabled — `if: false` — gated on the XDN test workflow)

## Conventions
- Java code follows Google Java Style (enforced by formatter)
- Service names: lowercase, no special characters
- Test naming: `Xdn*Test.java` for XDN-specific tests, `*Test.java` for general tests
- Commit messages: short, lowercase, imperative (e.g., `update xdn-cli`, `bugfix formatter`)
- Example services in `services/` — each is a standalone Docker app (bookcatalog, todo, chessapp, etc.)

## Evaluation Scripts (`eval/`)
Top-level `eval/` contains Python/Go benchmarking and experiment scripts (distinct from the Java `src/edu/umass/cs/xdn/eval/` subpackage). It covers load-latency sweeps for both active-replication and primary-backup variants across baselines (XDN, OpenEBS, rqlite, DRBD, CRIU, MySQL/Postgres/MongoDB sync), geo-distributed latency experiments, microbenchmarks (coordination granularity, optimization breakdown), failure-injection scenarios, and result aggregation/plotting. See `eval/README.md` for the full script index and typical workflows before running or modifying benchmarks.

## Live container migration PoC (`migration/`, `docs/live-migration-*.md`)
Standalone tooling for moving a running container from host A to host B with
in-memory state preserved across the move. **Not integrated with XDN yet**;
the artifacts are a standalone CLI that drives `podman container checkpoint`
→ `scp` → `podman container restore` end-to-end, optionally with one round
of pre-copy.

- `migration/xdn-migrate.sh` — single-shot migration.
- `migration/xdn-migrate-precopy.sh` — pre-dump + final-delta migration.
- `migration/counter-app/` — Go HTTP counter workload used to measure downtime.
- `migration/probe.sh` / `probe-redis.sh` — continuous-load probes that fail
  over from src to dst on first error and report client-visible downtime.
- `docs/live-migration-plan.md` — design proposal (analyzed Docker checkpoint,
  Podman+CRIU, runc+CRIU directly; recommended Podman).
- `docs/live-migration-notes.md` — running debug journal (every wart hit and
  the workaround).
- `docs/live-migration-results.md` — tabulated numbers: counter-app ~1.6 s
  downtime, Redis 104 MB → 2.59 s baseline / 1.92 s with pre-copy.

**Stack required:** `podman 3.4.4 --runtime runc` (Ubuntu's default `crun
0.17` has no checkpoint support; Docker's `start --checkpoint` is broken in
29.4) + `criu 4.2` from the OBS `devel:tools:criu` repo (Ubuntu's apt `criu
3.16` segfaults on kernel 5.15). All three of these calls out in
`docs/live-migration-notes.md` so the next person doesn't re-derive them.

**Restore-time is the remaining downtime floor** — podman re-walks the full
memory image on the destination regardless of pre-copy. CRIU's `--lazy-pages`
would collapse this but podman doesn't expose it.

## Further Reading (`docs/`)
- `docs/developer.md` — formatting, running tests (incl. ConsoleLauncher recipes), logging
- `docs/request-flow.md` — end-to-end request path through GigaPaxos
- `docs/HTTP-API.md` — HTTP API surface exposed by ActiveReplicas
- `docs/paxos-reconfiguration.md`, `docs/paxos-compaction.md` — GigaPaxos internals
- `docs/live-migration-plan.md`, `docs/live-migration-notes.md`, `docs/live-migration-results.md` — Live migration PoC (standalone; see "Live container migration PoC" section above)
