# Live replica migration (QEMU/KVM) — prototype

Move a running **bookcatalog + postgres** replica between two CloudLab machines
**without downtime**, using QEMU/KVM live migration. The replica runs inside a
KVM guest; live-migrating that guest moves the app *and* its postgres state from
one host to the other while clients keep hitting it.

## Idea

```
        clnode349 (10.10.1.1)                 clnode396 (10.10.1.2)
   ┌──────────────────────────┐          ┌──────────────────────────┐
   │  KVM guest  bookcat-replica  ──live migrate──▶  (same guest)    │
   │   ├ app  fadhilkurnia/xdn-bookcatalog (DB_TYPE=postgres)        │
   │   └ postgres:bookworm  (data in the guest disk)                 │
   │   eth: 10.10.1.100  (fixed IP+MAC, bridged onto the LAN)        │
   └────────────┬─────────────┘          └────────────┬─────────────┘
                └──────── experiment LAN 10.10.1.0/24 (xdnbr0) ───────┘
```

The guest keeps the **same IP/MAC** before and after migration because both
hosts bridge it onto the shared 10.10.1.0/24 experiment LAN. At the migration
cutover QEMU sends a gratuitous ARP, the L2 switch relearns the guest's MAC on
the new host's port, and traffic to `10.10.1.100` continues — the only blip is
the sub-second VM pause/resume. No shared storage: the disk is **block-migrated**
(`--copy-storage-all`) over the fast LAN.

## Files (run in order)

| Script | Where | What |
|--------|-------|------|
| `config.sh` | — | shared config (hosts, IPs, VM specs, images). Override via env. |
| `00-exchange-keys.sh` | **laptop** | one-time: root↔root SSH trust between the nodes for migration |
| `01-setup-host.sh` | **both hosts** | install KVM/libvirt, bridge the experiment NIC |
| `02-create-vm.sh` | **host0** (clnode349) | build + boot the bookcatalog+postgres guest |
| `03-migrate.sh` | host that holds the VM | live-migrate to the other node (run again to return) |
| `04-monitor.sh` | either host | hammer the service to measure downtime during migration |
| `05-verify.sh` | either host | where is the VM + prove postgres state survived |
| `09-teardown.sh` | either host | remove the VM from both nodes |

## Quick start

```bash
# 0) from your laptop (which can SSH to both nodes)
./00-exchange-keys.sh

# 1) on EACH node (ssh fadhil@clnode349… / …clnode396…), in this folder
./01-setup-host.sh

# 2) on clnode349 — boot the replica
./02-create-vm.sh
./05-verify.sh --write          # add a tagged book, confirm it serves

# 3) demo the migration
#    terminal A (on clnode396): watch for downtime
./04-monitor.sh
#    terminal B (on clnode349): move it
./03-migrate.sh                 # 10.10.1.1 -> 10.10.1.2, live

# 4) confirm it moved and kept its data, then send it back
./05-verify.sh                  # VM now on clnode396; the tagged book is still there
# (on clnode396) ./03-migrate.sh   # back to clnode349
```

`04-monitor.sh` prints `....X.X..recovered after ~0.4s outage` and a summary; a
healthy live migration shows a sub-second (often <1 s) blip or none at all.

## Requirements / assumptions

- Two CloudLab nodes on a **shared L2 experiment LAN** (`10.10.1.0/24` here),
  with KVM (`/dev/kvm`, AMD-V/VT-x) — both r6615 nodes qualify.
- **Identical CPUs** on both hosts (they are): the guest uses
  `--cpu host-passthrough`, which requires matching CPUs to migrate.
- `SSH_USER` has passwordless sudo (CloudLab default).
- Everything is tunable via env vars in `config.sh` (e.g. `VM_RAM_MB`,
  `VM_IP`, `EXP_IFACE`, `APP_IMAGE`).

## Notes & troubleshooting

- **Bridge is not persistent.** `01-setup-host.sh` builds `xdnbr0` with `ip`
  commands (not netplan), so re-run it after a reboot. SSH is on the *control*
  NIC, so bridging the experiment NIC never drops your session.
- **CloudLab L2 / MAC learning.** Bridging puts the guest's MAC on the
  experiment switch. A direct node-to-node experiment LAN normally allows this.
  If the guest can't reach `10.10.1.x` after `02-create-vm.sh`, the switch may be
  filtering the guest MAC — check with `ping 10.10.1.100` from the peer host.
- **First boot is slow** (~2–4 min): the guest pulls Docker + the postgres and
  bookcatalog images over the NAT NIC. The NAT NIC is only for setup; the service
  lives on the bridged `10.10.1.100`.
- **Disk size for block migration.** Each migration block-copies the
  `VM_DISK_GB` disk over the LAN; keep it small (default 12 G). For repeated
  migrations you can instead put the disk on shared storage (NFS over the LAN)
  and drop `--copy-storage-all` from `03-migrate.sh` for RAM-only migration.
- **Inspect**: `sudo virsh list --all`, `sudo virsh console bookcat-replica`,
  `sudo virsh domjobinfo bookcat-replica` (live migration progress).

## Relation to XDN

This prototypes the *mechanism* (move a live stateful replica between edge hosts
with no downtime) that XDN's demand-driven placement wants — here at the VM
granularity via QEMU live migration, decoupled from XDN's own coordinator. A
follow-up is wiring this to XDN reconfiguration so a replica relocation is a VM
migration instead of a stop/checkpoint/restore.
