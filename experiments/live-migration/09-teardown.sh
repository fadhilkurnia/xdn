#!/usr/bin/env bash
# Remove the VM and its disks from BOTH hosts. The host bridge is left in place
# (harmless); pass --bridge to also tear it down and restore the raw NIC IP.
#
#   ./09-teardown.sh
#   ./09-teardown.sh --bridge
set -euo pipefail
source "$(dirname "$0")/config.sh"

destroy_here() {
  if sudo virsh dominfo "$VM_NAME" >/dev/null 2>&1; then
    say "removing VM '${VM_NAME}' on $(hostname)"
    sudo virsh destroy "$VM_NAME" >/dev/null 2>&1 || true
    sudo virsh undefine "$VM_NAME" --remove-all-storage >/dev/null 2>&1 || \
      sudo virsh undefine "$VM_NAME" >/dev/null 2>&1 || true
  fi
  sudo rm -f "$WORKDIR/${VM_NAME}.qcow2" "$WORKDIR/${VM_NAME}-seed.iso" 2>/dev/null || true
}

destroy_here
# Also clean the peer (best effort, over the migration key).
PEER="$(peer_ip)"
if [ -n "$PEER" ]; then
  say "cleaning peer ${PEER}"
  sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=5 "root@${PEER}" \
    "virsh destroy ${VM_NAME} 2>/dev/null; virsh undefine ${VM_NAME} --remove-all-storage 2>/dev/null; \
     rm -f ${WORKDIR}/${VM_NAME}.qcow2 ${WORKDIR}/${VM_NAME}-seed.iso 2>/dev/null; echo cleaned" 2>/dev/null || true
fi

if [ "${1:-}" = "--bridge" ]; then
  say "restoring ${EXP_IFACE} (removing bridge ${BRIDGE})"
  MYIP="$(my_lan_ip)"
  if [ -n "$MYIP" ] && ip link show "$BRIDGE" >/dev/null 2>&1; then
    sudo ip link set "$EXP_IFACE" nomaster || true
    sudo ip addr flush dev "$BRIDGE" || true
    sudo ip link del "$BRIDGE" || true
    sudo ip addr add "${MYIP}/${VM_CIDR}" dev "$EXP_IFACE" || true
  fi
fi
say "teardown done."
