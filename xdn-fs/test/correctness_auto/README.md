# fuselog concurrency stress harness

Black-box, implementation-agnostic regression gate for the fid-table /
statediff-stack tearing class of race (see project discussion for the
full mechanism). Targets the *invariant*, not one specific historical
timing, so it should catch a coworker reintroducing a similar-but-not-
identical synchronization mistake, not just the exact original bug.

## Files

- `parser.py` — `WireFormatParser` interface + `FuselogV2Parser`, the one
  concrete parser today, verified byte-for-byte against `fuselog-apply.cpp`
  (not inferred). A future second implementation (different language,
  different wire format) gets its own parser class here; nothing else
  changes.
- `driver.py` — `Driver` interface + `FuselogV2Driver`, which starts/stops
  the real `fuselogv2` binary as a subprocess and reports back the
  writable (mounted) path and harvest socket path. A future second
  implementation gets its own driver class.
- `workload.py` — writer threads: real concurrent `open`/`pwrite` syscalls
  against the mounted path. Two populations: "hot" files (each thread
  exclusively owns a small slice, hammered continuously) and "cold" files
  (written rarely by one dedicated thread) — the latter specifically
  targets the condition that let a fid go permanently orphaned in the
  original bug (a rarely-written file, like a binlog header, going a full
  harvest cycle without being re-touched).
- `harvester.py` — implements the real socket protocol (connect, send
  `"g\n"`, read exactly one self-describing batch, close — **never** send
  anything not matching `"y"`/`"g"`, since the real server kills its
  entire listener on an unrecognized message) and runs the Tier-1 check
  after every batch.
- `replay_check.py` — Tier-2: replays accumulated raw batches through the
  real `fuselog-apply` binary and byte-compares the result against what
  the writer threads believe they wrote.
- `run_stress_test.py` — CLI entrypoint, wires everything together,
  produces a CI-friendly exit code.
- `test_harness_selftest.py` — validates the harness's own Tier-1 logic
  against a mock server and synthetic (including deliberately-corrupted)
  batches. No real binary needed. Run this whenever the harness itself
  changes.

## The two checks, and why both exist

- **Tier 1 (fast, continuous)**: for every harvested batch, every `fid`
  referenced by a statediff must resolve in that *same* batch's own file
  table. This is the direct, fast check for the exact bug class — no
  `fuselog-apply` invocation needed.
- **Tier 2 (slow, periodic + always at the end)**: replay accumulated
  batches through the *real* `fuselog-apply` binary, then byte-compare
  actual file content against expected. This is the true end-to-end
  check — it validates the real consumer and doesn't depend on Tier-1's
  own understanding of the protocol being correct.

## Usage

```sh
python3 run_stress_test.py \
    --fuselog-binary /path/to/fuselogv2 \
    --fuselog-apply-binary /path/to/fuselog-apply \
    --duration 15 --max-iterations 200000
```

Exit code `0` = pass, `1` = fail. On failure, the mount/backing/scratch
directories are preserved for inspection by default (pass
`--always-clean` to force cleanup regardless of outcome).

## Validating the harness before trusting it as a CI gate

1. Run `python3 test_harness_selftest.py` — should always pass, no real
   binary needed. Confirms the Tier-1 logic itself is sound.
2. Run the full harness against the **pre-fix** `fuselogv2` build — should
   reliably **fail** within the default duration/iteration budget.
3. Run the full harness against the **fixed** `fuselogv2` build — should
   reliably **pass**.

Only after both 2 and 3 are confirmed should this be wired into CI as an
actual gate — a regression test that's never been shown to regress-detect
isn't trustworthy yet.

## Known limitations / things to revisit

- `SocketReader` assumes uncompressed live batches (`FUSELOG_COMPRESSION`
  unset/false, the default) — it will raise a clear `ParseError` rather
  than silently mishandling a compressed live batch, but does not
  implement incremental zstd-frame streaming. Tier-2's file-based parsing
  path (via `parser.parse_batch`) does handle whole-file zstd, since a
  complete statediff file is fully in memory before parsing there.
- Reproduction is probabilistic, not deterministic — no synchronization
  points are injected into `fuselogv2` itself, per the decision to stay
  fully black-box across implementations. If the fixed build ever fails
  to reliably reproduce the bug against a *known-bad* build within the
  default budget, increase `--duration`/`--max-iterations` or
  `--num-hot-threads` before concluding the harness itself is broken —
  and re-run `test_harness_selftest.py` to rule out a harness regression
  specifically.
