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
- **Python**: Top-level `eval/` (experiment harness, k6 load drivers, plot/analysis scripts) and `xdn-bw-trace/` (bpftrace-based bandwidth tracer + plotters). Note this is distinct from the Java `eval/` *subpackage* under `src/edu/umass/cs/xdn/eval/`.

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
- `services/mysql-cluster/` — self-clustering MySQL via Group Replication;
  the entrypoint maps `XDN_CLUSTER_*` onto GR (server_id, group seeds, bootstrap
  on ordinal 0). Unlike etcd/rqlite, MySQL has no one-flag clustering, so the
  entrypoint clears init GTIDs and wires the recovery channel itself.
- `services/bookcatalog-rqlite-cluster.yaml` — multi-component spec:
  `bookcatalog` (entry, talks to `127.0.0.1:4001`) + `rqlite` (stateful,
  cluster member on the overlay).
- `services/wordpress-mysql-cluster.yaml` — multi-component spec exercising the
  sidecar-netns path: `wordpress` (entry) + `mysql` (stateful GR member). Covered
  by `XdnWordPressClusterTest`.

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

### Client Geolocation and Geo-Demand Profiling
Clients can advertise their location via the `X-Client-Location: <lat>,<lon>`
HTTP header. `XdnGeoDemandProfiler` parses that header on the AR frontend and
accumulates per-region demand in a background worker; the demand distribution
feeds into `XdnReplicaPlacementProfile` so the Reconfigurator can re-place
replicas closer to where load is actually coming from. The geo-demand
end-to-end pipeline is exercised by `eval/geo_demand_smoke.py` (driven by
the `geo-demand-smoke.yml` CI job).

### `xdn-cli` subcommands (`xdn-cli/cmd/`)
Cobra-based CLI. Top-level verbs include `launch`, `status`, `check`, and two command groups: `service` and `cluster`.
- `launch` — deploy a regular blackbox-replicated service. Flags: `--image`, `--state`, `--deterministic`, `--consistency`, `--num-replicas`, `--min-replicas`, `--max-replicas`, `--env`, `--port`, `--methods`, `-f file.yaml`.
- `service` — `info`, `destroy`, `move` (relocate; drives synchronous paxos leader change), `leader` (inspect/set the paxos leader).
- `cluster launch` — deploy a self-clustering service (`mode:cluster`). Flag-based for single-image (`--image --port --peer-port --state --num-replicas --env`) or `-f spec.yaml` for multi-component clusters. See "Cluster-mode services" above.

Mutating subcommands prompt for yes/no confirmation on stdin. The shared CREATE-request HTTP plumbing lives in `sendCreateRequest()` in `launch.go` and is reused by `cluster.go`.

### XDN-internal URL params
URL query parameters prefixed with `_xdn` (e.g. `_xdnsvc`) are consumed at the XDN/proxy layer and must be stripped from the request URI before it is forwarded to the containerized service. `_xdnsvc` provides the service name directly as a URL param and is used as a developer-experience alternative to setting the `XDN:` header.

### Bandwidth/Latency Tracing (`xdn-bw-trace/`)
Per-service inter-replica + client⇄replica TCP bandwidth tracer built on bpftrace, with companion plotting scripts. `inter_replica_bw.bt` attaches kprobes on `tcp_sendmsg`/`tcp_cleanup_rbuf` and aggregates bytes by `(pid, local_port, peer_ip, peer_port)`. `trace_bw.py` discovers cluster topology from the RC HTTP API, drives a mixed read/write workload across replicas, and emits a CSV + `.meta.json` sidecar to `xdn-bw-trace/results/`. `plot_bw_graph.py` and `plot_lat_graph.py` render directed bandwidth graphs and analytic (Haversine + fiber-speed) latency graphs from the geolocation in `gigapaxos.properties`. Linux only; requires `bpftrace` ≥ 0.21 and `CAP_BPF` (run as root or passwordless sudo).

### Configuration
- `gigapaxos.properties` — Main deployment config
- `conf/gigapaxos.xdn.local.properties` — Local development (1 RC + 3 AR on loopback)
- `conf/gigapaxos.xdn.local.geodemand.properties` — Local cluster with geo-located ARs; used by the geo-demand smoke CI to exercise demand-driven replica reconfiguration
- `conf/gigapaxos.cloudlab.properties` — CloudLab cluster deployment
- `conf/gigapaxos.xdn.cloudlab.local.{10,13}nodes.properties` — Multi-node CloudLab variants
- `conf/gigapaxos.xdn.3way.properties`, `conf/gigapaxos.xdn.3way.cloudlab.properties` — 3-way replication variants (local + cloudlab)
- `conf/gigapaxos.xdn.tpcc-java-pb.cloudlab.properties`, `conf/gigapaxos.xdn.wordpress-pb.cloudlab.properties` — App-specific primary-backup eval configs
- `conf/gigapaxos.xdnlat.template.properties` — Template showing the `(lat, lon)` syntax for node geolocation
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

## Further Reading (`docs/`)
- `docs/developer.md` — formatting, running tests (incl. ConsoleLauncher recipes), logging
- `docs/request-flow.md` — end-to-end request path through GigaPaxos
- `docs/HTTP-API.md` — HTTP API surface exposed by ActiveReplicas
- `docs/paxos-reconfiguration.md`, `docs/paxos-compaction.md` — GigaPaxos internals
- `docs/wordpress-pb-investigation.md` — full WordPress primary-backup throughput investigation (see summary below)

## Investigating Primary Backup with WordPress on CloudLab

Full investigation log (hypotheses, fixes, code changes): [`docs/wordpress-pb-investigation.md`](docs/wordpress-pb-investigation.md).

**Setup:** Start the cluster with `gpServer.sh` using `conf/gigapaxos.xdn.wordpress-pb.cloudlab.properties`; deploy via `xdn` with `xdn-cli/examples/wordpress.yaml`. WordPress runs as a primary-backup group — only the primary runs the containerized service. Always `forceclear` after an error.

**Baselines (CloudLab 3-way, WordPress XML-RPC editPost):** XDN PB ~1500 rps peak (~1200 rps at ≤16ms avg) — beats OpenEBS (~800 rps) and MySQL semi-sync (~500 rps).

**PB request pipeline:**
```
Client → HTTP frontend (Netty) → writePool dispatch → PBReplicaCoordinator
  → PBM queue → PBM worker → XdnGigapaxosApp.execute(XdnHttpRequestBatch)
  → forwardHttpRequestBatchToContainerizedService → WordPress container (via FUSE)
  → doneQueue → capture thread → captureStateDiff → Paxos propose → commit callback
  → async response to client
```

**Benchmark scripts:** `eval/investigate_wp_bottleneck.py` (per-rate isolation), `eval/investigate_wordpress_pb.py` (cluster mgmt + `GP_JVM_ARGS`), `eval/get_latency_at_rate.go` (Poisson load gen).

**Resolved root cause (2026-03-11):** WordPress Apache sends `Connection: close`, which `HttpActiveReplica` forwarded to the client, tearing down TCP per request and exhausting ephemeral ports (TIME_WAIT) at a ~500 rps ceiling — not an XDN-internal bottleneck. Fixed by overriding the `Connection` header to honor the client's keep-alive preference (`HttpActiveReplica.java`), plus a `ByteBuf.copy()` refCnt fix (`XdnGigapaxosApp.java`) and fully async dispatch. See the docs file for the eliminated-hypotheses table.

**Debug flags:** `-DPB_SKIP_REPLICATION=true` (skip captureStateDiff + Paxos propose), `-DPB_N_PARALLEL_WORKERS=32`, `-DPB_CAPTURE_ACCUMULATION_MS=1`, `-DPB_BYPASS_COORDINATOR=true` (bypass PBM, dispatch directly via async path), `___DDE` HTTP header (bypass coordinator, legacy direct-execute path).
