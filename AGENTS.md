# Repository Guidelines

## Project Structure & Module Organization
- `src/` houses the core Java sources (Gigapaxos/XDN), organized under `src/edu/umass/cs/...`.
- `test/` contains JUnit 4/5 tests; `test-gigapaxos/` holds legacy/integration test assets and scripts.
- `bin/` includes build and run scripts (e.g., `build_xdn_jar.sh`, `gpServer.sh`).
- `conf/` stores runtime properties (e.g., `gigapaxos.properties`, `gigapaxos.local.properties`).
- `xdn-cli/` is the Go-based CLI; `xdn-dns/` and `xdn-fs/` are supporting components.
- `services/` provides example stateful services; `docs/` contains documentation.
- Build outputs land in `build/` (classes) and `jars/` (artifacts).

## Build, Test, and Development Commands
- `./bin/build_xdn_jar.sh` compiles Java and produces jars in `jars/` (wraps `ant clean` + `ant jar`).
- `./bin/build_xdn_cli.sh` builds the `xdn` CLI binaries (requires Go).
- `ant jar` compiles and packages jars; `ant clean` removes `build/` and `jars/`.
- `ant test` runs the default integration test suite.
- `ant runtest -Dtest=MyTest` runs a specific JUnit 4/5 test by class name.
- `ant xdn-unit-tests` runs JUnit 5 unit tests that match `Xdn*Test`.
- `./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.local.properties start all` starts local RC/AR servers.

## Coding Style & Naming Conventions
- Java is the primary language; follow existing package layout under `edu.umass.cs`.
- Class names use `UpperCamelCase`; test classes typically end with `Test`.
- Format Java with Google Java Format via `bin/run_java_formatter.sh` (use `--check` in CI-style runs).
- Prefer small, focused commits and keep shell scripts POSIX/Bash-compatible.

## Testing Guidelines
- Tests live in `test/` and use both JUnit 4 and JUnit 5.
- Name new tests `*Test.java`; XDN-specific tests should follow `Xdn*Test` to be picked up by `ant xdn-unit-tests`.
- For new behavior, add unit tests and, when appropriate, integration coverage under `test-gigapaxos/` or scripted flows in `bin/run_xdn_tests.sh`.

## Commit & Pull Request Guidelines
- Commit history favors short, lowercase, imperative summaries (e.g., `update xdn-cli`, `bugfix formatter`).
- Keep subjects under ~72 characters and avoid noisy prefixes unless needed.
- No PR template is enforced; include a clear summary, the tests you ran (exact commands), and any config or doc changes.

## Architecture Overview
- Gigapaxos provides the replication/reconfiguration core; XDN layers service orchestration and a CLI on top.
- The control plane is the Reconfigurator (RC), and the data plane is the ActiveReplica (AR) set; both run from the Java jars in `jars/`.
- Client/service interactions typically flow through the `xdn` CLI (`xdn-cli/`) or via HTTP to AR frontends, with configuration in `conf/`.

## Configuration Notes
- Runtime behavior is driven by property files in `conf/`; prefer adding new settings there rather than hardcoding.
- When changing defaults, update the relevant `.properties` file and note it in the PR description.
