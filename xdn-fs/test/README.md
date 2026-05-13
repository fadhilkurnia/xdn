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

## Requirements

- Linux (FUSE)
- Python 3.9+ (stdlib only)
- `fusermount3` in `PATH`
- `fuselog` and `fuselog-apply` built and staged into `bin/`
