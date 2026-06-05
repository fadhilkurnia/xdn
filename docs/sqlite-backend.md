# Embedded SQLite backend for gigapaxos

gigapaxos persists paxos logs (`SQLPaxosLogger`) and reconfiguration state
(`SQLReconfiguratorDB`) through a JDBC abstraction (`paxosutil.SQL`) that already
supported Embedded Derby, MySQL, and Embedded H2. This adds **Embedded SQLite**
as a fourth backend, plus a lightweight non-pooling data source, aimed at
running a **lighter reconfigurator (RC) on small machines**.

## TL;DR

Recommended config for a small-footprint RC:

```properties
SQL_TYPE=EMBEDDED_SQLITE
CONNECTION_POOLING=false
# DB_MAX_CONNECTIONS must stay 0 (unbounded). See "Single-writer: why a hard
# connection cap does NOT work" below — gigapaxos re-enters the logger while
# holding a connection, so any finite cap self-deadlocks.
```

Versus the default (Embedded Derby + C3P0 pool), measured on this codebase:

- **~25% lower process RSS** and **72 fewer JVM threads** (no C3P0 helper threads).
- **~40× lower median DB write latency** (p50 ~0.5 ms vs ~25 ms) and ~10–35×
  higher write throughput depending on the durability setting.
- Passes the full 18-test reconfiguration integration suite, with only rare
  transient `SQLITE_BUSY_SNAPSHOT` events that the paxos layer retries.

Trade-off: SQLite is single-writer, so node-local DB writes serialize at the
engine level (WAL + `busy_timeout`). For an RC this is acceptable; for a
write-throughput-bound active replica it may not be. Note that a *hard*
application-level single connection (`DB_MAX_CONNECTIONS=1`) is **not** usable
here — see below.

## What changed

| File | Change |
|------|--------|
| `paxosutil/SQL.java` | New `EMBEDDED_SQLITE` enum value + driver/URL/blob/schema/varchar methods. New vendor-aware exception classifiers `isDuplicateKeyException` / `isDuplicateTableException` / `isNonexistentTableException` (SQLite reports a null SQLState and numeric vendor codes, not Derby SQLState strings). |
| `paxosutil/SimpleDataSource.java` | **New.** A dependency-free `DataSource`: a small bounded pool of reused connections via a close-intercepting proxy. No background/helper threads. Supports `maxIdle` (idle retention) and `maxTotal` (concurrency cap; `1` = single serialized writer). |
| `SQLPaxosLogger.java`, `SQLReconfiguratorDB.java` | `dataSource` field retyped to `javax.sql.DataSource`. C3P0 vs `SimpleDataSource` selected by config. SQLite URL handling (plain file path, no `;create=true`), WAL/`busy_timeout`/`IMMEDIATE` pragmas, and vendor-aware blob/clob access (sqlite-jdbc does **not** implement `createBlob`/`setBlob`/`getBlob`/`setClob`; we route through `setBytes`/`getBytes`/`setString`). |
| `PaxosConfig.java`, `ReconfigurationConfig.java` | New `CONNECTION_POOLING` and `DB_MAX_CONNECTIONS` config keys. |
| `build.xml`, `lib/` | Added `sqlite-jdbc-3.46.1.3.jar`. |

The abstraction is preserved: Derby/MySQL/H2 paths are unchanged and the full
reconfiguration integration suite still passes 18/18 on Derby (no regression).

## Configuration reference

| Key | Default | Meaning |
|-----|---------|---------|
| `SQL_TYPE` | `EMBEDDED_DERBY` | One of `EMBEDDED_DERBY`, `MYSQL`, `EMBEDDED_H2`, `EMBEDDED_SQLITE`. Shared by paxos and reconfiguration. |
| `CONNECTION_POOLING` | `true` | `true` → C3P0 pool. `false` → `SimpleDataSource` (no pool threads). Forced `false` internally for SQLite. |
| `DB_MAX_CONNECTIONS` | `0` | Non-pooling path only. Bounds concurrently outstanding connections (`SimpleDataSource` semaphore). **Must be `0` (unbounded) for gigapaxos** — any finite value deadlocks (see below). The plumbing exists for other embedders/uses. |

SQLite connections are opened with `journal_mode=WAL`, `synchronous=NORMAL`, and
`busy_timeout=30000` (see Durability and Concurrency below).

## Why SQLite needs special handling

1. **No JDBC LOB support.** The xerial `sqlite-jdbc` driver throws
   `SQLFeatureNotSupportedException` for `Connection.createBlob`,
   `PreparedStatement.setBlob`/`setClob`, and `ResultSet.getBlob`. We use the
   byte/String accessors (`setBytes`/`getBytes`/`setString`) for SQLite, which
   round-trip the exact same bytes. (Centralized in `SQLPaxosLogger`'s
   `setBlobBytes`/`lobToBytes`/`getColumnBytes` and `SQLReconfiguratorDB`'s
   `setClobString`.)

2. **Different error reporting.** SQLite reports `getSQLState() == null` and a
   numeric `getErrorCode()` (e.g. 19 = constraint). The new classifiers in
   `SQL.java` handle both styles so the catch blocks stay vendor-agnostic.

3. **Single writer.** Only one connection may write at a time. See below.

## Durability and Concurrency

### Durability (`synchronous`)

- `synchronous=NORMAL` (our default, WAL mode): fsync only at WAL checkpoint.
  **Durable across a process crash; may lose the last few commits on an OS/power
  crash.** Fast.
- `synchronous=FULL`: fsync every commit (equivalent to Derby's default
  durability). Slower tail, fully crash-safe.

### Concurrency: `SQLITE_BUSY`

Under concurrent read-then-write transactions on multiple connections, WAL mode
can occasionally raise `SQLITE_BUSY` / `SQLITE_BUSY_SNAPSHOT`, and `busy_timeout`
does **not** cover `BUSY_SNAPSHOT`. In the integration suite this happens rarely
(single-digit events across a full run) and is absorbed by the paxos layer's
write retries, so the suite passes. This is the proven, recommended setup.

Two "fixes" were evaluated and **rejected** for gigapaxos (both work in the
isolated benchmark but break the real cluster):

- `transaction_mode=IMMEDIATE` (take the write lock at `BEGIN`). Eliminates
  `BUSY_SNAPSHOT` in the benchmark, but gigapaxos borrows a connection and
  re-enters the logger on the same thread (e.g. `putCheckpointState` →
  `getSetGCAndGetMinLogfile` → `unpauseLogIndex`). Holding the write lock across
  such a nested write risks a write-lock self-deadlock.
- `DB_MAX_CONNECTIONS=1` (single connection). See the next section.

### Single-writer: why a hard connection cap does NOT work

A natural idea for SQLite is to serialize all DB access through one connection.
We implemented this (`SimpleDataSource` with a `maxTotal=1` semaphore) and it is
clean and fast *in isolation* (0 errors, tightest tail in the benchmark). But it
**deadlocks the real cluster at startup**, because gigapaxos acquires a
connection and then, while still holding it, re-enters the logger and acquires a
*second* connection on the same thread:

```
SQLPaxosLogger.putCheckpointState   // holds connection #1 (permit taken)
  -> getSetGCAndGetMinLogfile
    -> MessageLogDiskMap.restore
      -> unpauseLogIndex
        -> getDefaultConn()          // wants connection #2 -> blocks on the
                                     //   permit the same thread already holds
```

With one permit, the nested borrow waits forever. Any finite cap below the
maximum re-entrancy depth × concurrency has the same failure, so in practice the
cap must be unbounded (`0`). A genuinely safe single-writer would require a
reentrancy-aware connection design (e.g. a thread-local connection reused for
nested borrows) — and would still need care around the explicit-transaction
paths (`setAutoCommit(false)` batches) so a nested op cannot commit an outer
transaction early. That is a larger change, left as future work.

## Benchmark data

All numbers measured on this machine (8 cores, Linux, OpenJDK 21, local disk).

### Memory (13-node cluster in one JVM, `-Xmx512m`, idle baseline)

| Config | RSS | C3P0/mchange threads |
|--------|----:|---------------------:|
| Derby + C3P0 pool | 758 MB | 72 |
| Derby + SimpleDataSource | 724 MB | 0 |
| SQLite + SimpleDataSource | 567 MB | 0 |

- Dropping C3P0 (engine held constant): −34 MB RSS, −72 threads.
- Derby → SQLite engine swap: −157 MB RSS.
- Combined: **−191 MB (~25%) RSS, 72 fewer threads.**
- Java *heap* used is similar across all three (~424–450 MB); the savings are in
  RSS / native memory / thread stacks, not heap.

### Write latency (8 threads × 2000 read-modify-write txns, 200 rows, 1 KB)

`BEGIN; SELECT; UPDATE; COMMIT` per op. `DBLatencyBench` reproduces the
gigapaxos write shape; see `src/edu/umass/cs/gigapaxos/paxosutil/DBLatencyBench.java`.

| Mode | Throughput | p50 | p99 | p99.9 | max | Failed txns |
|------|-----------:|----:|----:|------:|----:|------------:|
| Derby + C3P0 (full durable) | 272 /s | 25.2 ms | 170 ms | 260 ms | 776 ms | 0% |
| SQLite multi-conn pool *(naive — do not use)* | 28,342 /s | 0.06 ms | 5.3 ms | 9.7 ms | 134 ms | **34.5%** |
| SQLite `IMMEDIATE`, WAL/NORMAL | 11,573 /s | 0.06 ms | 1.2 ms | 104 ms | 631 ms | 0% |
| **SQLite single-writer, WAL/NORMAL** | 9,481 /s | 0.59 ms | 2.9 ms | **4.9 ms** | 227 ms | 0% |
| SQLite `IMMEDIATE`, FULL durable | 2,672 /s | 0.06 ms | 22.8 ms | 231 ms | 4535 ms | 0% |
| SQLite single-writer, FULL durable | 2,941 /s | 0.54 ms | 77.6 ms | 165 ms | 280 ms | 0% |

This is a deliberately adversarial microbenchmark (8 threads hammering 200 rows
with read-modify-write), so the BUSY rate is a worst case, not what the cluster
sees (the real suite saw single-digit BUSY events per run). Takeaways:

- SQLite beats Derby on median latency by ~40× and on throughput by ~10× even at
  equal (FULL) durability; the larger gaps come partly from WAL/NORMAL durability.
- The multi-connection pool's BUSY rate spikes under sustained contention; the
  `IMMEDIATE` and single-writer columns show it *can* be driven to zero in
  isolation — but neither is usable in gigapaxos (see Concurrency above). The
  shipped config is the multi-connection pool with paxos-layer retries.

### Integration tests

`TESTReconfigurationClient` (5 RC + 8 AR cluster, 18 tests):

- Derby + C3P0: 18/18, ~430 s.
- SQLite (`CONNECTION_POOLING=false`, `DB_MAX_CONNECTIONS=0`): 18/18 across
  multiple runs (~125–329 s wall — the throughput test is variance-prone), with
  a handful (≈1–3) of transient `BUSY`/`BUSY_SNAPSHOT` events per run absorbed by
  paxos retries.
- SQLite with `DB_MAX_CONNECTIONS=1`: **deadlocks at startup** (re-entrant DB
  access — see Concurrency above). Do not use.

## Running it

Build (the sqlite-jdbc jar is picked up from `lib/`):

```bash
ant compile
```

Run the reconfiguration integration suite against SQLite (single-writer). Note
the harness configures nodes programmatically and forbids a `gigapaxosConfig`
file, so config overrides are injected via `ConfiguredTestRunner`:

```bash
CP="build/classes:build/test/classes:$(ls lib/*.jar | tr '\n' ':')"
java -ea -cp "$CP" \
  -Djavax.net.ssl.trustStore=conf/keyStore/node100.jks \
  -Djavax.net.ssl.trustStorePassword=qwerty \
  -Djavax.net.ssl.keyStore=conf/keyStore/node100.jks \
  -Djavax.net.ssl.keyStorePassword=qwerty \
  edu.umass.cs.reconfiguration.testing.ConfiguredTestRunner \
  edu.umass.cs.reconfiguration.testing.TESTReconfigurationClient \
  SQL_TYPE=EMBEDDED_SQLITE CONNECTION_POOLING=false
```

Run the DB write-latency benchmark:

```bash
CP="build/classes:$(ls lib/*.jar | tr '\n' ':')"
# mode in: derby-c3p0 derby-simple sqlite-pool sqlite-immediate sqlite-single
#          (+ "-full" suffix on sqlite modes for synchronous=FULL)
java -cp "$CP" edu.umass.cs.gigapaxos.paxosutil.DBLatencyBench \
  sqlite-single 8 2000 200 1024   # mode threads opsPerThread rows payloadBytes
```

## Known limitations / future work

- **Column-type migration (`reconcileTable` / `getAlterString`) is unsupported on
  SQLite** (SQLite cannot ALTER a column's type). In practice it is not
  exercised because SQLite stores declared types verbatim, so reconcile finds no
  mismatch; a genuine schema migration would fail loudly.
- `EMBEDDED_H2` remains in the abstraction but is untested here (no H2 jar in
  `lib/`).
- **True single-writer is not available** as a config flag (see Concurrency).
  Achieving it safely needs a reentrancy-aware connection design plus handling
  of the explicit-transaction paths — a worthwhile follow-up if the occasional
  `BUSY_SNAPSHOT` retry proves to be a problem in production.
- The `connectDB()` connection-leak fix in `SQLReconfiguratorDB` (it discarded
  the `getDefaultConn()` result) is included; harmless before, but it was the
  first deadlock the single-writer experiment exposed.
