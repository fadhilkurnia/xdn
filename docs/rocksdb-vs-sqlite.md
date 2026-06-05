# RocksDB vs SQLite for a small-memory, durable paxos RC

gigapaxos is a paxos-based system, so **durability is a safety requirement**: a
node that acknowledges (promises/accepts) a value must not forget it after a
crash, or paxos safety can be violated. The relevant comparison is therefore at
**equal, full fsync durability** — survives an OS/power crash, not just a process
crash.

- SQLite full durability: `SQLITE_SYNCHRONOUS=FULL` (WAL fsync on every commit).
- RocksDB full durability: `ROCKSDB_SYNC_WRITES=true` (WAL fsync on every write).

All numbers below are on the same machine/disk (OpenJDK 21, local disk). The
relaxed-durability numbers (SQLite `NORMAL`, RocksDB `sync=false`) are included
only for reference — they keep data only across a *process* crash, with a loss
window on an OS/power crash, so they are **not** safe for strict paxos durability.

## Summary / recommendation

For a durability-critical RC on a 0.5&ndash;1 GB machine **today, SQLite
(`synchronous=FULL`) is the better-balanced choice**: it is as light as RocksDB,
has predictable durable-write latency, and completes recovery-heavy workloads.
RocksDB ties on memory and is competitive (or better) on *batched* durable-write
latency, but has two issues at full durability: (1) very expensive *unbatched*
single durable writes, and (2) slow recovery reads. RocksDB becomes the stronger
choice if/when the logger's recovery-read path is optimized (see Future work),
and it is the clear winner if relaxed durability is ever acceptable.

## Memory (single RC node, `-Xmx128m`, RocksDB tuned for low memory)

| Backend | RSS | Min heap it runs at | Threads |
|---------|----:|--------------------:|--------:|
| Derby (default) | 191 MB | — | 65 |
| SQLite | 125 MB | ~48 MB | 56 |
| RocksDB | 124 MB | ~48 MB | 56 |

**Tie.** Both fit 0.5 GB with ~390 MB headroom. RocksDB's native memory is
bounded by a shared block cache + WriteBufferManager (`ROCKSDB_BLOCK_CACHE_MB`,
`ROCKSDB_MEMTABLE_BUDGET_MB`); SQLite is non-pooled (no C3P0 threads). Both are
~35% lighter than the Derby default.

## Latency — durable DB-layer writes (8 threads, 1 KB, full fsync)

Microbenchmark of **single** durable read-modify-write ops (no batching):

| Backend (full durability) | Throughput | p50 | p99 | p99.9 |
|---------------------------|-----------:|----:|----:|------:|
| SQLite `FULL` (single-writer) | 2941 ops/s | 0.54 ms | 77.6 ms | 165 ms |
| SQLite `FULL` (IMMEDIATE) | 2672 ops/s | 0.06 ms | 22.8 ms | 231 ms |
| **RocksDB `sync=true`** | **66 ops/s** | **83 ms** | 598 ms | 1066 ms |
| RocksDB `sync=false` *(relaxed, ref only)* | 74190 ops/s | 0.08 ms | 0.89 ms | 1.96 ms |

For **single, unbatched** durable writes RocksDB is dramatically slower — every
`put` triggers a full WAL fsync, and the per-op fsync cost dominates (~83 ms p50).
SQLite's WAL commit fsync is far cheaper here. (Relaxed RocksDB is the fastest of
all, but is not power-loss safe.)

## Latency — end-to-end reconfiguration (full durability)

The real paxos logger **batches** log writes (`logBatch` = one fsync per batch),
which is RocksDB's better case:

| Backend (full durability) | test04 create latency | 18-test suite |
|---------------------------|----------------------:|---------------|
| SQLite `FULL` | 982 ms | **18/18 pass, 109 s** |
| RocksDB `sync=true` | **566 ms** | 13/18, 0 failures, **timed out at 501 s** |

End-to-end, RocksDB's batched create latency is *lower* (566 vs 982 ms), but the
suite **does not finish**: it stalls in the recovery-heavy dynamic-membership
tests (active/RC add/delete), which repeatedly stop/start/recover many paxos
groups. SQLite finishes all 18 in 109 s. (Caveat: the RocksDB reconfigurator DB
writes async since it is paxos-backed, whereas SQLite `FULL` applies to both the
logger and the RC DB, so this end-to-end comparison slightly favors RocksDB.)

## Why RocksDB stalls on recovery

The RocksDB logger has no journal/log-index; recovery reads (`getLoggedMessages`,
one-pass `readNextMessage`, `getLoggedAccepts/Decisions`) prefix-scan and
deserialize the `messages` column family. Under the membership tests — which
recover many groups repeatedly — these scans dominate. This is a performance
characteristic, not a correctness bug (zero failures in every run).

## What each is good at

| | SQLite (`FULL`) | RocksDB (`sync=true`) |
|---|---|---|
| Memory | ~125 MB | ~125 MB |
| Single durable write | **fast** (2941 ops/s) | slow (66 ops/s) |
| Batched paxos log write | good | **good/better** |
| Recovery-heavy workload | **completes (109 s)** | stalls |
| Relaxed-durability speed | fast | **fastest** (74k ops/s) |
| Concurrency model | single writer (handled) | concurrent writers |

## Recommended configs

Durable RC, today (recommended):
```properties
SQL_TYPE=EMBEDDED_SQLITE
CONNECTION_POOLING=false
SQLITE_SYNCHRONOUS=FULL          # power-loss durable
# -Xmx96m   ->  ~125 MB RSS, fits 0.5 GB
```

RocksDB, durable (competitive on batched writes; recovery-read optimization
pending):
```properties
PAXOS_LOGGER=ROCKSDB
RECONFIGURATOR_DB=ROCKSDB
ROCKSDB_SYNC_WRITES=true          # power-loss durable
ROCKSDB_BLOCK_CACHE_MB=16
ROCKSDB_MEMTABLE_BUDGET_MB=8
```

## Future work (to make RocksDB the better durable choice)

1. **Optimize logger recovery reads** — cache per-paxosID message lists, or keep
   a compact in-memory log index so recovery does not re-scan/deserialize the
   `messages` CF. This is the main blocker.
2. **Group-commit unbatched writes** — coalesce single `log()`/checkpoint fsyncs
   so the per-write fsync cost is amortized (RocksDB supports group commit; the
   gigapaxos call pattern currently does not exploit it for single writes).
