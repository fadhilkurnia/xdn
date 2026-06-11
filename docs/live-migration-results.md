# Live container migration — PoC results

Companion to [`live-migration-plan.md`](live-migration-plan.md) (the design)
and [`live-migration-notes.md`](live-migration-notes.md) (the running-debug
journal). This doc is the **tl;dr of what the PoC showed**.

## TL;DR

- A standalone CLI tool (`migration/xdn-migrate.sh` + `xdn-migrate-precopy.sh`)
  moves a running container from host A to host B on this CloudLab pod and
  preserves in-memory state across the move.
- For an idle Redis with 104 MB of data: **2.59 s downtime baseline → 1.92 s
  with one pre-copy round (−26 %)**.
- For a trivial in-memory counter: ~1.6 s either way; the workload is too
  small for pre-copy to help.
- The remaining downtime floor is dominated by **CRIU restore on the
  destination**, not network transfer. The next-frontier optimization is
  lazy-paging (`criu --lazy-pages`), which podman doesn't expose.
- Status: **viable as a v2 path for XDN reconfiguration**, but requires
  switching one runtime call site from Docker to Podman+runc (or going
  below podman to runc/CRIU directly). Not a drop-in replacement for the
  existing tar-state path; would coexist as a second statediff strategy.

## Setup

| | |
|---|---|
| Hosts | 10.10.1.2 (src), 10.10.1.3 (dst), 10 Gbps L2 link in same CloudLab pod |
| OS / kernel | Ubuntu 22.04.2 LTS, kernel 5.15.0-168-generic (identical) |
| CPU | x86_64 with `avx512f` (identical) |
| Runtime | podman 3.4.4 (apt), runc 1.3.5 (apt), CRIU 4.2 (OBS `devel:tools:criu`) |
| Network | each container on the host's podman default bridge; no overlay |

The CRIU on Ubuntu's archive (3.16) is too old for kernel 5.15 — it segfaults
during restore. The OBS repo's 4.2 is what works. Podman defaults to `crun
0.17` which lacks checkpoint support; force `--runtime runc`.

## Results

### Workload 1 — counter-app (Go HTTP server, atomic int64 + Go runtime, ~6 MB RSS)

| | baseline (1 shot) | with 1 pre-dump |
|---|---|---|
| critical-path archive | 180 KB | 83 KB |
| critical-path scp | 0.47 s | 0.31 s |
| critical-path checkpoint | 0.71 s | 0.79 s |
| critical-path restore | 0.73 s | 0.72 s |
| **client-measured downtime** | **1.66 s** | **1.61 s** |
| state preserved? | yes (last on src = 524, first on dst = 525) | yes |

Pre-copy didn't help: 50 ms savings. The workload has no memory to pay for
the pre-copy round. The fixed overhead is `criu dump (~0.5 s)` +
`criu restore (~0.7 s)` + scp of the per-process metadata (~80 KB) which all
exist whether or not you pre-copy.

### Workload 2 — Redis 7 with 104 MB of seeded keys (100 k × 1 KB)

| | baseline (1 shot) | with 1 pre-dump |
|---|---|---|
| pre-dump (NOT downtime) | — | 1.78 MB compressed / 1.02 s |
| critical-path archive | 1.78 MB | **54 KB** (33× smaller) |
| critical-path scp | 0.50 s | 0.30 s |
| critical-path checkpoint | 1.20 s | 0.79 s |
| critical-path restore | 1.03 s | 0.92 s |
| **client-measured downtime** | **2.59 s** | **1.92 s** (−26 %) |
| DBSIZE post-migration | 100 000 ✓ | 100 000 ✓ |

Pre-copy bought back 0.67 s. The win comes from the final dump being a delta
against the pre-dump — for an idle Redis (only the PING probe touching it),
the delta is essentially empty.

### Why isn't pre-copy a bigger win?

Restore time is still ~0.9 s regardless of pre-copy. That's because podman
3.4.4 doesn't expose CRIU's lazy-pages / restore-warm path: even though the
destination already has 110 MB of pre-dump pages, podman re-runs the whole
`criu restore` after the final dump arrives, walking the same pages again.

To collapse restore time you'd need CRIU's `--lazy-pages` — the destination
container starts paused with empty memory, faults pages in from a *still-
running* source on demand over a side channel. Real downtime drops to
"checkpoint final + 1 RTT to advertise restored". This is the canonical
"post-copy" technique and is what serverless platforms (Firecracker, gVisor)
use for fast warm starts.

## Knocked-down assumptions from the plan

- **"Docker checkpoint" approach (option A in the plan).** Confirmed dead:
  `docker checkpoint create` works, `docker start --checkpoint=...` always
  fails with `failed to upload checkpoint to containerd: content already
  exists` on Docker 29.4. Has been an open Docker bug for years. Podman+runc
  is the only realistic path.
- **"Iterative pre-copy with N rounds until dirty-page rate plateaus".**
  Hard collapse to **N=1** on podman 3.4.4 because `--with-previous` is
  rejected together with `-P`. To get multi-round chains we need either
  podman ≥ 4 or to bypass podman and orchestrate `criu pre-dump`/`dump`
  directly with parent-image-dir chaining.
- **TCP connection preservation.** Required `--tcp-established` on both
  checkpoint and restore. With podman's default bridge network the
  destination container gets a *different* IP from the source's, so the
  external client's TCP socket is broken anyway and must reconnect. The
  probe does this transparently with retry-on-error; "downtime" includes
  reconnect time. To actually preserve TCP connections cross-host we'd need
  IP preservation (CNI overlay shared across hosts, or float the IP).

## Knocked-down assumptions from the implementation

Things that bit us during the days, captured here so the next person
doesn't re-derive them:

1. **`apt install criu` ships 3.16 on Ubuntu 22.04**, which is too old for
   the 5.15 kernel and silently produces a `skip pagemap` + SEGV during
   restore. The OBS repo `devel:tools:criu` is required (`/etc/apt/sources.list.d/criu.list`).
2. **`podman container checkpoint`'s default `crun 0.17` runtime has no
   checkpoint support.** Need `--runtime runc` on every podman call.
3. **`docker save`/`load` image hashes don't match across hosts** if both
   hosts built independently. podman's `restore` embeds the source image ID
   and fails to look it up on the destination. Need `podman save | scp |
   podman load` to align IDs (or push to a shared registry).
4. **`scp` doesn't create the destination directory.** Many "No such file
   or directory" red herrings until I `mkdir -p` on dst first.
5. **podman archive uses `zstd`, not gzip,** despite the `.tar.gz`
   extension. `gunzip` confused me for a few minutes; `zstd -d` is correct.
6. **CRIU dumps full `pages-*.img` per process**, not de-duplicated against
   the image's resident pages. So a Redis with 104 MB RSS produces a 110 MB
   `pages-1.img` (compressed 60× by zstd because the data is hex). On the
   wire it's small; on disk during restore it's the full 110 MB again.

## Next steps if we want to push this further

In rough priority:

1. **Multi-round chained pre-copy.** Bypass podman, call CRIU directly with
   `pre-dump --prev-images-dir=<prev>` so we can iterate until the dirty-page
   rate plateaus. Expected payoff: marginal for idle workloads, real for
   read-heavy workloads where pages are read but not dirtied.
2. **Lazy-pages restore.** CRIU `--lazy-pages` + a page-fault server still
   running on the source. Expected payoff: collapses the restore leg of the
   downtime window from ~1 s to ~tens-of-ms.
3. **Preserve IP across hosts** via attaching the container to a Swarm-style
   overlay network (podman CNI plugins for flannel / VxLAN-bridged).
   Expected payoff: client TCP connections survive the move — no client
   reconnect.
4. **Postgres workload.** Day 5 in the plan; deferred because the redis
   numbers were already enough to justify v2 work. For postgres the bind-mount
   data-dir handling is the new variable — would need an rsync delta during
   the downtime window.
5. **XDN integration.** Sketch only at this stage: a new statediff strategy
   `LIVEMIGRATE` parallel to `RSYNC` / `FUSELOG`, called from
   `XdnGigapaxosApp.captureContainerizedServiceFinalState` /
   `reviveContainerizedService`. The catch is XDN uses Docker; either we
   wrap podman beside it (`docker info` + `podman info` coexist fine on a
   shared kernel) or we move the runtime to runc-via-podman across the board.

## Artifacts produced

```
docs/live-migration-plan.md          design proposal
docs/live-migration-notes.md         running debug journal
docs/live-migration-results.md       this file

migration/counter-app/main.go        Go HTTP counter workload
migration/counter-app/Dockerfile     ubuntu:22.04 base + counter binary
migration/xdn-migrate.sh             single-shot migration CLI
migration/xdn-migrate-precopy.sh     one-round pre-copy migration CLI
migration/probe.sh                   HTTP probe with failover
migration/probe-redis.sh             redis PING probe with failover
```
