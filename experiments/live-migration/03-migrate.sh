#!/usr/bin/env bash
# Run on the host that CURRENTLY holds the VM. Live-migrates it to the peer node
# over the experiment LAN, with no shared storage (block migration). Run it again
# to migrate back.
#
#   ./03-migrate.sh            # migrate to the other node (auto-detected)
#   ./03-migrate.sh 10.10.1.2  # or name the destination explicitly
set -euo pipefail
source "$(dirname "$0")/config.sh"

DEST_IP="${1:-$(peer_ip)}"
[ -n "$DEST_IP" ] || die "could not determine peer IP — pass it explicitly: ./03-migrate.sh <dest-ip>"

sudo virsh dominfo "$VM_NAME" >/dev/null 2>&1 || die "VM '$VM_NAME' is not on this host"

# --- prepare destination storage ------------------------------------------------
# Main writable disk (block-migrated) + its target dev (e.g. vda).
DISK="$(sudo virsh domblklist "$VM_NAME" --details | awk '$2=="disk"{print $4; exit}')"
MAINDEV="$(sudo virsh domblklist "$VM_NAME" --details | awk '$2=="disk"{print $3; exit}')"
[ -n "$DISK" ] && [ -n "$MAINDEV" ] || die "could not find the VM's main disk"
# -U: read metadata without taking a lock (the disk is in use by the running VM).
VSIZE="$(sudo qemu-img info -U "$DISK" | awk -F'[()]' '/virtual size/{print $2}' | awk '{print $1}')"
# If the disk is a CoW overlay, migrate only the delta (--copy-storage-inc): the
# shared base must exist on the dest, and we create a matching overlay there.
BACKING="$(sudo qemu-img info -U "$DISK" | sed -n 's/^backing file: \([^ ]*\).*/\1/p')"
if [ -n "$BACKING" ]; then
  ACTUAL_KB="$(sudo du -k "$DISK" | awk '{print $1}')"
  say "CoW disk: overlay ~${ACTUAL_KB} KB over base ${BACKING} -> delta-only migration"
  if ! sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "root@${DEST_IP}" "test -f '$BACKING'"; then
    say "base missing on dest -> copying it once (${BACKING})"
    sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "root@${DEST_IP}" "mkdir -p '$(dirname "$BACKING")'"
    sudo scp -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "$BACKING" "root@${DEST_IP}:${BACKING}" >/dev/null
  fi
  sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "root@${DEST_IP}" \
    "mkdir -p '$(dirname "$DISK")' && qemu-img create -f qcow2 -F qcow2 -b '$BACKING' '$DISK' ${VSIZE} >/dev/null && echo ok"
  COPYFLAG="--copy-storage-inc"
else
  say "standalone disk -> full block migration; pre-creating dest disk (${VSIZE} bytes)"
  sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "root@${DEST_IP}" \
    "mkdir -p '$(dirname "$DISK")' && qemu-img create -f qcow2 '$DISK' ${VSIZE} >/dev/null && echo ok"
  COPYFLAG="--copy-storage-all"
fi

# Read-only cdrom(s) (e.g. the cloud-init seed) are NOT block-copied, but libvirt
# still checks their source file exists on the destination — stage a copy.
for CD in $(sudo virsh domblklist "$VM_NAME" --details | awk '$2=="cdrom" && $4!="-"{print $4}'); do
  say "staging cdrom on ${DEST_IP}: ${CD}"
  sudo scp -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "$CD" "root@${DEST_IP}:${CD}" >/dev/null
done

# --- live migrate ---------------------------------------------------------------
say "live-migrating '${VM_NAME}'  $(my_lan_ip_any) -> ${DEST_IP}  (RAM + disk ${MAINDEV}, VM stays up)"
START="$(date +%s.%N)"
# Optional XBZRLE: compress re-sent dirty RAM pages (delta vs a page cache).
# Helps converge a write-heavy VM over a constrained link. XBZRLE=1 to enable.
COMP_OPTS=""
if [ "${XBZRLE:-0}" = "1" ]; then
  COMP_OPTS="--compressed --comp-methods xbzrle --comp-xbzrle-cache ${XBZRLE_CACHE:-536870912}"
  say "XBZRLE enabled (cache ${XBZRLE_CACHE:-536870912} bytes)"
fi

# --migrateuri pins the DATA stream to the experiment-LAN IP. Without it libvirt
# sends the bulk copy to the destination's primary hostname (the control net),
# bypassing any WAN shaping we put on the experiment NIC.
sudo virsh migrate \
  --live --persistent --undefinesource \
  ${COPYFLAG} --migrate-disks "$MAINDEV" \
  --migrateuri "tcp://${DEST_IP}" \
  ${COMP_OPTS} \
  --verbose --auto-converge \
  "$VM_NAME" "qemu+ssh://root@${DEST_IP}/system"
END="$(date +%s.%N)"

say "migration complete in $(echo "$END - $START" | bc)s — '${VM_NAME}' now runs on ${DEST_IP}"
# Detailed stats (incl. XBZRLE compression cache hits/overflow) live on the dest.
say "destination job stats:"
sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "root@${DEST_IP}" \
  "virsh domjobinfo ${VM_NAME} --completed" 2>/dev/null | sed 's/^/    /'
echo "    (run ./03-migrate.sh on ${DEST_IP} to send it back)"
