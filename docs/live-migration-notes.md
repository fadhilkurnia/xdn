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
