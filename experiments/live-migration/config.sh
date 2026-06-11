#!/usr/bin/env bash
# Shared configuration + helpers for the QEMU/KVM live-replica-migration
# prototype. Source this from the other scripts:  source "$(dirname "$0")/config.sh"
#
# Every value can be overridden from the environment, e.g.
#   VM_RAM_MB=4096 ./02-create-vm.sh
set -euo pipefail

# --- The two CloudLab hosts (Clemson r6615) -------------------------------------
# Reachable over the public control net for SSH, and over the experiment LAN
# 10.10.1.0/24 for VM traffic + migration. Node names map to LAN IPs:
#   clnode349 = 10.10.1.1 (node0)   clnode396 = 10.10.1.2 (node1)
HOST0_DNS="${HOST0_DNS:-clnode328.clemson.cloudlab.us}"
HOST1_DNS="${HOST1_DNS:-clnode379.clemson.cloudlab.us}"
HOST0_IP="${HOST0_IP:-10.10.1.1}"
HOST1_IP="${HOST1_IP:-10.10.1.2}"
SSH_USER="${SSH_USER:-fadhil}"           # CloudLab login (has passwordless sudo)

# --- Host networking ------------------------------------------------------------
# The experiment NIC that carries 10.10.1.0/24; we move its IP onto a bridge so a
# VM can sit on the same L2 and keep its IP after migration. (Same NIC name on
# both r6615 nodes; auto-detected as a fallback.)
EXP_IFACE="${EXP_IFACE:-enp195s0np0}"
BRIDGE="${BRIDGE:-xdnbr0}"

# --- The migratable VM ----------------------------------------------------------
VM_NAME="${VM_NAME:-bookcat-replica}"
VM_VCPUS="${VM_VCPUS:-2}"
VM_RAM_MB="${VM_RAM_MB:-2048}"
VM_DISK_GB="${VM_DISK_GB:-12}"
VM_IP="${VM_IP:-10.10.1.100}"            # the replica's STABLE service IP
VM_CIDR="${VM_CIDR:-24}"
# Fixed MACs so the guest keeps a stable L2 identity across re-create/migrate.
VM_MAC_LAN="${VM_MAC_LAN:-52:54:00:ca:7a:10}"   # bridged onto 10.10.1.0/24
VM_MAC_NAT="${VM_MAC_NAT:-52:54:00:ca:7a:11}"   # libvirt NAT, internet for setup

# Where base image, VM disk and cloud-init seed live on each host.
WORKDIR="${WORKDIR:-/var/lib/libvirt/xdn-migration}"
UBUNTU_IMG_URL="${UBUNTU_IMG_URL:-https://cloud-images.ubuntu.com/jammy/current/jammy-server-cloudimg-amd64.img}"
# Golden base produced by 08-bake-base.sh (Ubuntu + docker + the service images
# pre-pulled, cleaned for re-templating). When present, create/migrate use it as
# the CoW backing, so the per-VM overlay is just the service's DATA delta -> much
# smaller, WAN-friendly migrations.
BAKED_BASE="${BAKED_BASE:-$WORKDIR/bookcat-base.qcow2}"

# Root SSH key used for host<->host live migration over the experiment LAN.
MIG_KEY="${MIG_KEY:-/root/.ssh/id_xdnmig}"

# --- The workload (bookcatalog + postgres) --------------------------------------
APP_IMAGE="${APP_IMAGE:-fadhilkurnia/xdn-bookcatalog}"   # supports DB_TYPE=postgres
# Pinned to 16: the floating `postgres:bookworm` tag now points to PG 18, which
# changed its data-dir layout and refuses to start on a /var/lib/postgresql/data
# volume mount (restart loop). 16 uses the classic layout the app expects.
PG_IMAGE="${PG_IMAGE:-postgres:16}"
SERVICE_PORT="${SERVICE_PORT:-8000}"     # VM port -> container :80
SERVICE_PATH="${SERVICE_PATH:-/api/books}"
SERVICE_URL="http://${VM_IP}:${SERVICE_PORT}${SERVICE_PATH}"

# --- Helpers (used by scripts that run ON a host) -------------------------------

# This host's experiment-LAN IPv4 (on the bridge once set up, else the raw NIC).
my_lan_ip() {
  ip -4 -o addr show "$BRIDGE" 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -1 \
    || true
}
my_lan_ip_any() {
  local ip; ip="$(my_lan_ip)"
  [ -n "$ip" ] && { echo "$ip"; return; }
  ip -4 -o addr show "$EXP_IFACE" 2>/dev/null | awk '{print $4}' | cut -d/ -f1 | head -1
}

# The OTHER node's experiment-LAN IP (the live-migration target by default).
peer_ip() {
  local me; me="$(my_lan_ip_any)"
  if [ "$me" = "$HOST0_IP" ]; then echo "$HOST1_IP"
  elif [ "$me" = "$HOST1_IP" ]; then echo "$HOST0_IP"
  else echo "" ; fi
}

say() { printf '\033[1;36m==>\033[0m %s\n' "$*"; }
die() { printf '\033[1;31mERROR:\033[0m %s\n' "$*" >&2; exit 1; }
