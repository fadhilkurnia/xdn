# fuselog Layer 3 differential tester

End-to-end correctness check for the fuselog capture-and-replay pipeline.

## What it does

1. Mounts `fuselog` on `/tmp/xdn-fuselog/A`.
2. Performs a deterministic, seeded sequence of random fs operations
   (write/overwrite/extending-write/truncate/unlink/rename) on both
   `A` (the live fuselog mount) and `/tmp/xdn-fuselog/B` (a plain
   directory acting as a POSIX oracle).
3. Connects to fuselog's unix socket, sends `g`, and harvests the
   captured statediff payload.
4. Unmounts `A`, then runs `fuselog-apply` to replay the payload onto a
   fresh `/tmp/xdn-fuselog/C`.
5. Walks all three trees and asserts `tree(A) == tree(B) == tree(C)`
   (mode, size, sha256 per file).

Three different mismatches point to three different bugs:

| Mismatch | Implies |
|---|---|
| `A != B` | fuselog's FUSE handlers diverge from plain POSIX semantics |
| `A != C` | statediff capture is incomplete or corrupting data |
| `B != C` | independent confirmation; useful for localizing the above |

## Run

```bash
./bin/build_xdn_fuselog.sh cpp                          # build binaries first
./xdn-fs/test/fuzz_differential.py                      # 1000 ops, time-based seed
./xdn-fs/test/fuzz_differential.py --num-ops 100        # quick smoke
./xdn-fs/test/fuzz_differential.py --seed 12345         # replay a specific seed
./xdn-fs/test/fuzz_differential.py --keep-on-pass       # don't clean up dirs on success
```

The seed is printed prominently at the start of every run; use `--seed N`
to reproduce a failure.

## On failure

Artifacts are dumped to `/tmp/fuselog-fuzz-fail-<seed>/`:

- `ops.log` — every op in replay-friendly form
- `A.tar`, `B.tar`, `C.tar` — tree snapshots
- `statediff.bin` — raw socket payload
- `fuselog.log` — captured fuselog stdout/stderr

## Layer 5: real-database integration (`fuzz_db.py`)

A different oracle from the Layer 3/4 tests: instead of comparing the
filesystem trees byte-for-byte, run a real database on the fuselog mount,
SIGKILL it to simulate a primary crash, replay the captured statediff
onto a fresh dir, and start a replica DB from that dir. The replica
performs its own crash recovery on startup; the assertion is that the
replica observes exactly the transactions the primary `COMMIT`ted.

This is the realistic XDN failure model — the on-disk byte layout
doesn't need to be identical (DBs run background processes that mutate
on-disk state without changing logical state), only the durable
committed transactions need to round-trip.

Adapters (all use the same `fuzz_db.py` orchestrator):
| `--db` | Backend | Notes |
|---|---|---|
| `sqlite` | in-process `sqlite3` stdlib | WAL mode + `synchronous=FULL`; no docker |
| `postgres` | `postgres:16-alpine` in docker | needs `sudo docker` + `user_allow_other` |
| `mysql` | `mysql:8.4` in docker | `innodb_flush_log_at_trx_commit=1`; ~500 MB initial statediff |
| `mariadb` | `mariadb:11` in docker | inherits from MySQL adapter (same wire/SQL) |
| `mongodb` | `mongo:7` in docker | single-doc writes with `writeConcern: {w: "majority", j: true}` |

The docker-based adapters all need:
- `sudo docker` (passwordless preferred)
- `user_allow_other` uncommented in `/etc/fuse.conf` so the docker daemon
  can access the bind-mounted FUSE dir
- Container runs with `--user $(id -u):$(id -g)` so files inside the
  mount stay owned by the host user (no chown needed before/after)

Run:
```bash
./xdn-fs/test/fuzz_db.py --db sqlite                       # 200 txns, time-seed
./xdn-fs/test/fuzz_db.py --db sqlite --seed 42
./xdn-fs/test/fuzz_db.py --db postgres --num-txns 500
./xdn-fs/test/fuzz_db.py --db mysql --killmid 3.0          # kill mid-workload
./xdn-fs/test/fuzz_db.py --db mariadb
./xdn-fs/test/fuzz_db.py --db mongodb
```

`--killmid N` schedules a `SIGKILL` of the primary N seconds after the
workload starts (instead of after all transactions finish). This
exercises crash recovery on a partially-completed workload — the
realistic XDN failure scenario where the primary dies unexpectedly.

## Companion tests

- **`fuzz_concurrent.py`** — same harness but with N worker threads, each on
  a disjoint subtree (`t0/`, `t1/`, ...). Stresses `push_statediff` CAS and
  `fid_mutex` under contention; final state stays deterministic so
  `A == B == C` is still a valid assertion.
- **`fuzz_concurrent_overlap.py`** — N threads on the SAME shared path pool
  (overlapping ops). Drops the plain-dir oracle and only asserts `A == C`.
  Tolerates expected race errnos (`ENOENT`, `EEXIST`, ...) at op level.
  Writes use thread-owned byte offsets so concurrent threads never touch
  the same bytes (sidesteps the push-after-pwrite ordering race);
  truncate is excluded for the same reason. The test surfaces races on
  metadata, rename, link, and unlink, where it is correct.

  Note: this test is **probabilistic**, not deterministic. Even with a
  fixed RNG seed, OS scheduling determines which thread's `push_statediff`
  CAS lands first when their FUSE handlers complete in close succession.
  Hard-link aliasing (one inode under two names with concurrent writes
  through both) can also produce occasional divergence. Empirical pass
  rate at 4 threads × 200 ops is ~95% over 20 runs; failures land in
  `/tmp/fuselog-fuzz-fail-<seed>/` for inspection.

## Requirements

- Linux (FUSE)
- Python 3.9+ (stdlib only)
- `fusermount3` in `PATH`
- `fuselog` and `fuselog-apply` built and staged into `bin/`
