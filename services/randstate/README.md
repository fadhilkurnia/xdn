# randstate — non-deterministic state service

A deliberately **non-deterministic** XDN service: on its first bootstrap it generates a random
`boot_id` and a burst of random rows under `/app/data`. It exists to demonstrate (and stress) the
primary-backup **non-deterministic initial-state synchronization** — the mechanism that makes all
replicas agree on the *primary's* random initial state instead of each generating its own.

If each replica bootstrapped independently they would diverge immediately; PB avoids that by having
only the primary bootstrap and syncing that exact state to the backups. A promoted backup (or a
restart) already has `boot_id`, so it **skips** bootstrap and serves the synced state.

State is a plain append-only log (not a DB) so the demo is not confounded by SQLite WAL recovery.
Pure Go (CGO disabled) → trivial multi-arch.

## Endpoints
| method | path | meaning |
|---|---|---|
| GET | `/state` | `{boot_id, rows, bytes, hash, ready}` |
| GET | `/hash` | sha256 of the whole state — the cross-replica fingerprint |
| GET | `/boot` | the `boot_id` |
| GET | `/ready` | 200 once bootstrap finished |
| GET | `/log` | raw state log (debug) |
| POST | `/write` | append one random row (test ongoing replication) |

## Env
- `BOOTSTRAP_ROWS` (default 200) — random rows written on first bootstrap.
- `BOOTSTRAP_ROW_BYTES` (default 8) — raw random bytes per row (hex-encoded on disk, so ~2×). Bump
  it for a **large initial state**, e.g. `ROWS=200 ROW_BYTES=262144` ≈ 100 MB. ⚠️ RECORDER ships the
  whole init state in-band through the JVM/paxos pipeline and **OOMs the small-heap (256 MB) ARs**
  well before ~64 MB; large initial state needs `RSYNC`. ~4 MB works fine under RECORDER.
- `ALWAYS_BOOTSTRAP` (default unset) — `true` re-randomizes on **every** start (drops the skip-guard).
  A promoted backup overwrites the synced state, so `boot_id` **changes** on every primary change —
  the negative control showing why the skip-guard, not init-sync, preserves a service's identity.
- `BOOTSTRAP_WRITE_MS` (default 0) — 0 = one fast burst (quiescent at capture). Set above the
  migrate-wait (~5s), e.g. `8000`, to make rows land *during* the init-sync window to stress the
  RSYNC seam.

## XDN service config
```json
{"name":"randstate","image":"fadhilkurnia/xdn-randstate","port":80,
 "state":"/app/data/","consistency":"linearizability","deterministic":false}
```

## Test the init-sync
1. Create the service → the primary bootstraps a random `boot_id`/`hash`.
2. Move the primary (or reconfigure the placement) to promote a backup.
3. The new primary's `/boot` and `/hash` must equal the original primary's — proving its random
   initial state was synchronized. With `XDN_PB_INIT_SYNC_MODE=RECORDER` this holds even with
   `BOOTSTRAP_WRITE_MS` overlapping the sync window (atomic capture); `RSYNC` can drop rows written
   mid-rsync.
