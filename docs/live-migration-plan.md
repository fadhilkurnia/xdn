# Live container migration — PoC plan

**Status:** draft, standalone PoC (no XDN integration in v1).
**Owner:** TBD.

## Why

XDN today moves a stateful blackbox replica by capturing the container's full
state to a tar, shipping it, and restarting on the destination
(`captureContainerizedServiceFinalState` → `restore` via `xdn:final:`). For
large state (multi-GB Postgres, hundreds of MB Redis) this is **minutes** of
unavailability per move. Live migration — checkpointing a running container's
memory and restoring it elsewhere — collapses that to seconds (or sub-seconds
with iterative pre-copy), and preserves things the tar pipeline drops:
in-memory caches, established TCP connections, ephemeral state.

This PoC is the standalone primitive. Folding it into XDN's reconfiguration
path is explicitly **out of scope** for v1.

## Goals (v1)

1. Move a running container from host A to host B preserving:
   - process tree + memory,
   - open file descriptors and bind-mounted state,
   - established TCP connections to external clients.
2. **Downtime ≤ 1 s** for a small (~10 MB working-set) toy app, **≤ 5 s** for
   Redis with ~100 MB of data, using iterative pre-copy.
3. A single CLI tool — `xdn-migrate` — that takes `--src-host`,
   `--src-container`, `--dst-host` and drives the whole pipeline.
4. Measurement: client-side downtime, total wall time, bytes transferred,
   broken down per pre-copy round.

## Non-goals (v1)

- Integration with `XdnGigapaxosApp` / `Reconfigurator`.
- Migration of GPU / SR-IOV / device-passthrough containers.
- Cross-architecture (amd64 only).
- Live migration of *cluster-mode* members (the work we just landed) — that
  needs a separate design pass because a cluster member's identity is its
  ordinal + overlay alias, not its container state.
- High availability of the migration controller; manual orchestration is fine.

## Approach analysis

| Option | Sketch | Pros | Cons |
|---|---|---|---|
| **A.** Docker `checkpoint`/`start --checkpoint` (experimental, CRIU under the hood) | What we'd reach for first because XDN already uses Docker. | Same tooling we already wire from `XdnGigapaxosApp`. | Has been "experimental" in Docker for years; routinely broken for overlay networks, named volumes, anything non-trivial. No pre-copy. |
| **B.** Podman `container migrate` | Same image format as Docker; explicit `--pre-checkpoint` support; well-maintained CRIU integration. | Best-supported CRIU stack in production today; can read Docker images unchanged. | Different runtime from rest of XDN — but that's *fine for the PoC*. |
| **C.** runc + CRIU directly | Lowest level: `runc checkpoint` / `runc restore`. | Maximum control; same path Docker/Podman use internally. | Most engineering; we'd rebuild orchestration that Podman already provides. |
| **D.** Application-level checkpoint (DB dumps + WAL replay) | Not really "live" migration. | Works without kernel-level CRIU. | Per-app effort; defeats the point of doing this in a blackbox-aware system. |

**Recommendation: B (Podman + CRIU).** Pre-copy is the lever that gets us from
"seconds of downtime" to "sub-second", and Podman is the only path where it
works out of the box today. Checkpoint files are the standard CRIU image
format, so when we later port to Docker (option A) or runc (C) for XDN
integration, the per-host pipeline stays the same.

## Architecture & data flow

```
[host-A]                                            [host-B]
container-X (running)
   │
   ├─ pre-dump #1  (full snapshot of dirty pages)  ──scp──▶ /var/lib/migrate/X/preN-1
   ├─ pre-dump #2  (only pages dirtied since #1)   ──scp──▶ /var/lib/migrate/X/preN-2
   ├─    …  (loop until dirty-page rate or wall-time threshold)
   ├─ pause source                                 ─┐
   ├─ rsync state dir bind-mount delta             ─┤  "downtime window"
   ├─ final dump  (last-mile dirty pages only)     ─┼─scp──▶ /var/lib/migrate/X/final
   │                                                │      ▼
   │                                                │   podman container restore --import=...
   │                                                │   container-X resumes with same PID,
   │                                                │   open FDs, TCP state
   ├─ stop + remove source ◀─ ack ─────────────────┘
   ▼
done; client experiences a brief pause but no connection reset.
```

Two transfers in the critical (downtime) path: the **final** CRIU dump (tens
of MB at best) and the **state-dir rsync delta**. Everything else happens
while the source is still running.

## Network identity preservation

The hardest part. Three knobs:

1. **Same IP on destination.** Easiest path: attach both containers (source
   and pre-staged destination) to the **same Docker Swarm overlay network**
   (the one we're already using for cluster-mode). On restore, CRIU sees the
   destination namespace owning the same IP, so TCP sockets restore cleanly.
   The PoC will require both hosts in the same swarm.
2. **conntrack on the destination.** CRIU restores socket state inside the
   container, but the host's connection tracking table won't have the flow.
   For TCP, the next packet from a peer will hit `ip_conntrack` clean —
   usually fine, but worth measuring.
3. **External LB or DNS flip.** Not in v1. Clients must point at the
   container's overlay IP directly during the PoC.

## PoC tool — `xdn-migrate`

Single Go binary (parallel to `xdn-cli/`), takes:

```
xdn-migrate \
    --src-host fadhil@10.10.1.2 \
    --src-container counter-app \
    --dst-host fadhil@10.10.1.3 \
    --pre-copy-rounds 3 \
    --pre-copy-interval 5s \
    --state-dir /var/lib/counter-app/ \
    --report /tmp/migration-report.json
```

Pipeline:

1. **Inspect source.** `podman inspect` to capture image, env, mounts,
   network attachments, ports.
2. **Stage destination.** `podman create` (not run) on dst-host with the same
   spec, attached to the same overlay net. Don't start it.
3. **Pre-copy loop.** `podman container checkpoint --pre-checkpoint
   --export=/tmp/pre-N.tar.gz`, then `scp` to dst. Track per-round size.
4. **Filesystem warm-up.** Concurrent `rsync` of the bind-mounted state dir
   (delta against previous round if any).
5. **Final dump.** `podman container checkpoint --with-previous --leave-stopped
   --export=/tmp/final.tar.gz`. Record source-pause timestamp `T_pause`.
6. **Ship & restore.** `scp` final + delta rsync of state dir;
   `podman container restore --import=/tmp/final.tar.gz` on dst-host. Record
   destination-running timestamp `T_resume`.
7. **Cleanup.** `podman rm -f` source (only after dst is verified running).
8. **Report.** Write `migration-report.json` with `downtime = T_resume -
   T_pause`, per-round bytes, total wall time.

## Test workloads

1. **counter-app** (~50 lines of Go). HTTP `/inc` increments an in-memory
   counter; `/get` returns it. Lets us measure downtime cleanly with a
   continuous-load client. Target: < 500 ms downtime.
2. **redis:7-alpine** with 100 MB of seeded keys. Tests that page-dirtying
   rate vs. pre-copy rounds gives diminishing returns. Target: < 5 s downtime.
3. **postgres:16-alpine** with a TPC-B-style load running. Real-world target;
   surfaces bind-mount data dir handling and fsync ordering.

## Measurement plan

A small client (`migrate-probe`) hammers the service end-to-end during
migration, records per-request `{ts_start, ts_end, status}`. Downtime =
longest window of consecutive failed/timeout requests within ±3 s of `T_pause`.
Run each workload N=5 times; report median + p99.

Also measure (from `migration-report.json`):
- Bytes transferred per pre-copy round (should monotonically decrease until a
  plateau).
- Total wall time (source-start-pre-copy → dst-running).
- Source CPU% during pre-copy (sanity check that pre-dump isn't pegging it).

## Risks & unknowns

| Risk | Mitigation |
|---|---|
| CRIU breaks for some kernel feature in our containers (overlayfs, certain syscalls) | Start with `counter-app` (minimal), grow workload complexity slowly; keep a known-good runc baseline. |
| TCP-state restoration fails when source IP isn't preserved | Require the same Swarm overlay for both hosts in v1; defer cross-overlay migration. |
| Bind-mount state-dir consistency between source's final-dump moment and dst restore | Two-phase rsync (warm-up while running + delta while paused); accept the rsync window as part of downtime. |
| CPU feature mismatch between hosts | CloudLab nodes are usually homogeneous; pin to `node1.xdn-r6615.*` cluster for v1. |
| Podman / CRIU version drift across the cluster | Pin both in `bin/xdnd dist-init` for the migration nodes; document required versions in this doc once verified. |

## Implementation sequencing

1. **Day 1.** Install CRIU + Podman on `10.10.1.2` and `10.10.1.3`; verify
   `podman container checkpoint`/`restore` works for `alpine sleep 9999` on a
   single host.
2. **Day 2.** A→B migration for `counter-app`, **no pre-copy**, measure
   baseline downtime.
3. **Day 3.** Add pre-copy loop; tune rounds + interval; measure downtime
   collapse on `counter-app`.
4. **Day 4.** Redis 100 MB. State dir handling (bind-mount → rsync).
5. **Day 5.** Postgres with TPC-B load. Address fsync window in the rsync
   step.
6. **Day 6.** Write up: `docs/live-migration-results.md` with the numbers and
   a 1-page table.

## Future XDN integration (sketch only — not v1)

When this works end-to-end as a standalone tool, the natural folding into XDN
is:

- A new `LiveMigrationStateDiffRecorder` (or a separate code path orthogonal
  to the recorder) wired into `XdnGigapaxosApp.captureContainerizedServiceFinalState`
  + `reviveContainerizedService`. Instead of tar-then-restart, drive the
  `xdn-migrate` pipeline.
- For the multi-component cluster work we just landed (`xdn cluster launch`),
  live-migrating a cluster member can preserve the overlay IP and the rqlite
  Raft membership without going through `etcdctl member add` — but this is
  delicate and needs its own analysis.
- `bin/xdnd dist-init` grows a CRIU + Podman install step (parallel to the
  existing Java / Docker / Go installs).

## Open questions to settle before starting

1. **Bind-mount state dir handling** during the final-dump window — rsync
   delta good enough, or do we need a shared FS (NFS / Ceph)? The redis and
   postgres workloads will answer this empirically.
2. **Same vs. different overlay IP** on destination — does our existing
   `xdn-cluster-*` overlay assign the same IP if a container with the same
   name is created on a different host? Needs a 5-minute experiment.
3. **podman ↔ docker coexistence** — both runtimes use `/var/lib/containers`
   trees that don't conflict, but socket/cgroup paths can; verify by running
   both on the same host before assuming.
