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
ant test                         # Run legacy JUnit 4 reconfiguration tests
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
- `service/` — Service metadata (`ServiceProperty`, `ServiceComponent`, `ConsistencyModel`, `ServiceInstance`, `RequestMatcher`)
- `utils/` — Shell execution helpers, hosts file editing
- `interfaces/behavior/` — Request behavior abstractions (commutative, key-commutative, etc.)
- `proto/` — Protocol buffer classes
- `eval/` — Evaluation and experiment utilities
- `experiment/` — Experiment harness code and ad-hoc clients (e.g. `XdnBookCatalogAppClient`)

### Top-level XDN helpers (in `src/edu/umass/cs/xdn/`)
- `XdnServiceProperties` — service-property helpers and defaults consumed elsewhere
- `XdnHttpRequestBatcher` — batches HTTP requests at the AR frontend when batching is enabled
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

### `xdn-cli` subcommands (`xdn-cli/cmd/`)
Cobra-based CLI. Top-level verbs include `launch`, `status`, `check`, and the `service` command group. `service` covers: `info`, `destroy`, `move` (relocate a service to new replica hosts; drives synchronous paxos leader change), `leader` (inspect/set the paxos leader). `launch` accepts `--num-replicas`, `--min-replicas`, `--max-replicas` in addition to `--image`, `--state`, `--deterministic`, etc. Mutating subcommands prompt for yes/no confirmation on stdin.

### XDN-internal URL params
URL query parameters prefixed with `_xdn` (e.g. `_xdnsvc`) are consumed at the XDN/proxy layer and must be stripped from the request URI before it is forwarded to the containerized service. `_xdnsvc` provides the service name directly as a URL param and is used as a developer-experience alternative to setting the `XDN:` header.

### Configuration
- `gigapaxos.properties` — Main deployment config
- `conf/gigapaxos.xdn.local.properties` — Local development (1 RC + 3 AR on loopback)
- `conf/gigapaxos.cloudlab.properties` — CloudLab cluster deployment
- `conf/gigapaxos.xdn.cloudlab.local.{10,13}nodes.properties` — Multi-node CloudLab variants
- `conf/gigapaxos.xdn.3way.properties` — 3-way replication variant
- `testing.properties` — Test configuration (nodes, load, batch settings)

Key config properties: `APPLICATION`, `REPLICA_COORDINATOR_CLASS`, `XDN_PB_STATEDIFF_RECORDER_TYPE`, `HTTP_AR_FRONTEND_BATCH_ENABLED`, `NIO_MAX_PAYLOAD_SIZE` (default 128MB).

### Cluster Orchestration (`bin/xdnd`)
For multi-machine/CloudLab deployments, `bin/xdnd` drives remote setup and lifecycle over SSH: `xdnd init-driver` on the driver machine, then `xdnd dist-init -config=... -ssh-key=... -username=...` to initialize remotes, and `xdnd start-all ...` to start xdn instances fleet-wide. `xdnd dist-init-observability` is the optional observability bootstrap.

## CI Workflows (`.github/workflows/`)
- **ant-build-test.yml**: Build + run `xdn-full-tests` on push/PR to master/main (JDK 21, Docker, FUSE, rsync)
- **xdn-cli-ci.yml**: gofmt check + CLI binary build on changes to `xdn-cli/`
- **google-java-format.yml**: Formatting check on XDN Java file changes
- **test-report.yml**: JUnit test-result reporter (currently disabled — `if: false` — gated on the XDN test workflow)

## Conventions
- Java code follows Google Java Style (enforced by formatter)
- Service names: lowercase, no special characters
- Test naming: `Xdn*Test.java` for XDN-specific tests, `*Test.java` for general tests
- Commit messages: short, lowercase, imperative (e.g., `update xdn-cli`, `bugfix formatter`)
- Example services in `services/` — each is a standalone Docker app (bookcatalog, todo, chessapp, etc.)

## Further Reading (`docs/`)
- `docs/developer.md` — formatting, running tests (incl. ConsoleLauncher recipes), logging
- `docs/request-flow.md` — end-to-end request path through GigaPaxos
- `docs/HTTP-API.md` — HTTP API surface exposed by ActiveReplicas
- `docs/paxos-reconfiguration.md`, `docs/paxos-compaction.md` — GigaPaxos internals
