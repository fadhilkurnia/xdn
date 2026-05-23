# Live migration PoC — running notes

Companion to `live-migration-plan.md`. Empirical findings as the PoC unfolds.

## Day 1 (2026-05-23)

### Environment

| | |
|---|---|
| Hosts | 10.10.1.2 (src), 10.10.1.3 (dst) on the CloudLab xdn-PG0 pod |
| OS | Ubuntu 22.04.2 LTS, kernel 5.15.0-168-generic |
| CPU | identical, with `avx512f` (CRIU image portable between them) |

### Installed

```
criu 4.2          (from devel:tools:criu OBS repo for 22.04 — Ubuntu's 3.16 doesn't work)
podman 3.4.4      (apt)
runc 1.3.5        (apt)  ← podman --runtime runc, because crun 0.17 has no checkpoint
```

### Three things that bit me, in order

1. **`apt install criu` on 22.04 ships 3.16, which segfaults during restore on
   kernel 5.15.** Symptom in `restore.log`:
   ```
   pie: N: skip pagemap   (repeated many times)
   Error: N killed by signal 11: Segmentation fault
   Restoring FAILED.
   ```
   Fix: add the OBS `devel:tools:criu` repo and upgrade to 4.2.

2. **Podman's default OCI runtime on 22.04 is `crun 0.17`, which is too old
   for `container checkpoint`.** Symptom:
   ```
   Error: configured runtime does not support checkpoint/restore
   ```
   Fix: pass `--runtime runc` (Ubuntu ships runc 1.3.5 which has CRIU
   integration). Don't bother trying to upgrade crun.

3. **Docker checkpoint is still broken on 29.4.3.** `docker checkpoint create`
   succeeds, but `docker start --checkpoint=...` fails with
   `failed to upload checkpoint to containerd: commit failed: content
   sha256:... already exists`. This has been an open Docker bug for years.
   Skip option A from the plan; podman+runc is the path.

### What works

Cross-host migration of `ubuntu:22.04 bash -c 'while true; do echo $i > /tmp/n; ...'`:

```
counter pre-cp  on 10.10.1.2 :  3
counter post-restore on 10.10.1.3 : 16     ← seconds of network transfer included
PID 1 (post-restore)  :  same bash command line, same uid
container status      :  Up 11 seconds
checkpoint archive    :  57 KB (no actual state in this toy)
```

The 13-step gap (3 → 16) is the time the bash loop kept running on .2 between
checkpoint and source removal, plus the time on .3 to scp and restore. The
loop's sleep is 1s, so the wall-clock migration window was ~13 seconds. The
*process state* survived; what we haven't measured yet is downtime visible to
an external client.

### Recipe that works (record so we don't re-derive it)

```bash
# source: 10.10.1.2
sudo podman --runtime runc run -d --name X <image> <cmd>
sudo podman --runtime runc container checkpoint X --export=/tmp/X.tar.gz
sudo chmod a+r /tmp/X.tar.gz
scp /tmp/X.tar.gz fadhil@10.10.1.3:/tmp/X.tar.gz

# destination: 10.10.1.3
sudo podman --runtime runc rm -f X 2>/dev/null   # idempotent
sudo podman --runtime runc container restore --import=/tmp/X.tar.gz
```

### Side-effect of Day 1

I enabled `experimental: true` in `/etc/docker/daemon.json` on 10.10.1.2 to
test Docker's checkpoint path. The `systemctl restart docker` that followed
killed the running XDN containers there (`etcd-demo.1`, `etcd-demo-2.1`,
`bookcat-db2.1`, `bookcat.1`, `bookcatalog.1`). They have `--restart
unless-stopped` so they'll come back when docker restarts them, but the etcd
and rqlite clusters may need a `bookcatalog`-sidecar restart again because of
the same startup-race issue we hit on first deploy. Mentioning so it's not a
surprise next time someone hits those services.

### Still open

- **TCP connection preservation across hosts.** Podman's default `podman`
  bridge gives a different IP per host (10.88.0.x). To survive an external
  client's TCP connection across the migration we'd need either same-IP-on-dst
  (CNI overlay shared across hosts: flannel/calico/swarm-overlay attachable to
  podman) or a floating IP. For Day 2 baseline we'll punt: use
  `--network=host` so container state migrates but the client is expected to
  reconnect; we measure the "neither host responds" window.
- **Overlay-IP-on-Swarm question.** Not yet probed; deferred until we need
  TCP preservation.

## Day 2 (2026-05-23) — counter-app baseline

`migration/counter-app/` (HTTP `/inc` + atomic counter, ubuntu:22.04 base,
~5 MB resident).

Pipeline driven by `migration/xdn-migrate.sh` (single-shot, no pre-copy).
Probe (`migration/probe.sh`) hammers `/inc` on the source, fails over to the
destination on first error, logs every request with `epoch.ns` timestamps.

### Result

```
checkpoint_sec:  0.707
scp_sec:         0.471
restore_sec:     0.728
cleanup_sec:     0.447
TOTAL:           2.352

client-measured downtime:  1.66 s
last counter on .2 = 524 ; first counter on .3 = 525   (delta = 1, state preserved)
failed probes during the window:  207 of 7252
```

### Three more things that bit me on day 2

1. **CRIU can't dump TCP sockets unless asked**. Without
   `--tcp-established` the checkpoint fails (silently in our script's first
   draft) when a client has any open keep-alive or in-flight requests against
   the container. `podman container checkpoint --tcp-established` fixes it;
   `restore` needs the matching flag.
2. **`scp` does not create the destination directory.** Cost me a confusing
   "No such file or directory" message that points at the wrong file. Now we
   `mkdir -p` on dst before scp.
3. **Per-host image builds produce different image IDs.** podman's restore
   embeds the source image ID in the checkpoint and fails on the destination
   with `parsing named reference "<64-hex>": invalid repository name`. Fix:
   one host builds the image, then `podman save | scp | podman load` to the
   other host so the layer blobs (and therefore IDs) match. For the future
   `xdn-migrate` tool this implies a one-time image-sync step before the
   first migration to any new host.

## Day 3 (2026-05-23) — pre-copy on counter-app

`migration/xdn-migrate-precopy.sh` adds one `podman container checkpoint -P`
round before the final checkpoint. Pre-dumps run while the container is still
serving requests, so they're outside the downtime window; only the final
delta ships during the pause.

### Result

| | bytes | wall sec |
|---|---|---|
| pre-dump 1 (background, NOT downtime) | 179 KB | 0.60 |
| final dump (downtime) | 83 KB ← half the 180 KB single-shot final | 0.79 |
| final scp (downtime) | 83 KB | 0.31 |
| restore (downtime) | — | 0.72 |
| **client-measured downtime** | | **1.61 s** |

For comparison, day-2 baseline (no pre-copy): **1.66 s** downtime.

### Honest finding: pre-copy bought us ~50 ms on counter-app.

The fixed `criu dump → criu restore` cost on these hosts is ≈ 1.5 s, dominated
by process-freeze + namespace dance + TCP-state restoration with
`--tcp-established`. Counter-app's actual state is a single `atomic.Int64`
plus the Go runtime working set — almost nothing to ship — so collapsing the
state transfer phase only saves on the order of disk I/O for 100 KB.

The interesting test for pre-copy is a workload whose memory IS the cost:
Redis with seeded keys (Day 4) or Postgres with a real working set (Day 5).
Expecting the gap to widen there.

### Roadblock to call out: podman 3.4.4 doesn't support chained pre-dumps.

The plan's "iterative pre-copy with N rounds until dirty-page rate plateaus"
collapses to a SINGLE pre-dump because podman 3.4.4 rejects
`--with-previous` together with `--pre-checkpoint`:

```
Error: --with-previous can not be used with --pre-checkpoint
```

This works in podman ≥ 4.0 (I checked the upstream changelog). For multi-round
pre-copy on Ubuntu 22.04 we'd need either to upgrade podman from kubic OBS
(`devel:kubic:libcontainers:stable`) or bypass podman and call CRIU's
`pre-dump` directly with parent-image-dir chaining. Deferring to v2.

