#!/usr/bin/env bash
# Bake a golden base image: Ubuntu + docker + the bookcatalog/postgres images
# pre-pulled, then cleaned for re-templating. VMs created from it (02-create-vm)
# overlay this base, so their migratable delta is just the service's DATA (tens
# of MB) instead of the whole OS+docker+images (~1.7 GB) -> far faster, WAN-
# friendly migrations.
#
# Run on ONE host (after 00/01). It boots a throwaway builder VM with internet
# (libvirt NAT), pulls the images, cleans the image, flattens it into
# $BAKED_BASE, and pre-seeds that to the peer.
set -euo pipefail
source "$(dirname "$0")/config.sh"

BUILDER="${BUILDER:-xdn-base-builder}"
STOCK="$WORKDIR/jammy-server-cloudimg-amd64.img"
WORKIMG="$WORKDIR/${BUILDER}.qcow2"
SEED="$WORKDIR/${BUILDER}-seed.iso"

ip link show "$BRIDGE" >/dev/null 2>&1 || die "run ./01-setup-host.sh first"
sudo mkdir -p "$WORKDIR"

[ -f "$STOCK" ] || { say "downloading stock cloud image"; sudo curl -fL --retry 3 -o "$STOCK" "$UBUNTU_IMG_URL"; }

say "creating builder overlay disk"
sudo virsh destroy "$BUILDER" 2>/dev/null || true
sudo virsh undefine "$BUILDER" 2>/dev/null || true
sudo rm -f "$WORKIMG"
sudo qemu-img create -f qcow2 -F qcow2 -b "$STOCK" "$WORKIMG" "${VM_DISK_GB}G" >/dev/null

GUEST_PUBKEY="$(sudo cat /root/.ssh/id_xdnmig.pub 2>/dev/null || true)"
say "writing builder cloud-init (install docker + pull ${PG_IMAGE}, ${APP_IMAGE})"
sudo tee "$WORKDIR/builder-user-data.yaml" >/dev/null <<EOF
#cloud-config
hostname: ${BUILDER}
ssh_pwauth: true
$( [ -n "$GUEST_PUBKEY" ] && printf 'ssh_authorized_keys:\n  - %s' "$GUEST_PUBKEY" )
runcmd:
  - [ sh, -c, "curl -fsSL https://get.docker.com | sh" ]
  - [ sh, -c, "docker pull ${PG_IMAGE} && docker pull ${APP_IMAGE}" ]
  - [ sh, -c, "touch /var/lib/xdn-baked" ]
EOF
sudo cloud-localds "$SEED" "$WORKDIR/builder-user-data.yaml"

say "booting builder (NAT NIC for internet)"
sudo virt-install --name "$BUILDER" --memory 2048 --vcpus 2 --cpu host-passthrough \
  --os-variant ubuntu22.04 \
  --disk "path=$WORKIMG,format=qcow2,bus=virtio" \
  --disk "path=$SEED,device=cdrom" \
  --network network=default,model=virtio \
  --graphics none --noautoconsole --import

say "waiting for builder NAT IP..."
NATIP=""
for i in $(seq 1 60); do
  NATIP="$(sudo virsh domifaddr "$BUILDER" 2>/dev/null | awk '/ipv4/{print $4}' | cut -d/ -f1 | head -1)"
  [ -n "$NATIP" ] && break
  sleep 3
done
[ -n "$NATIP" ] || die "builder did not get a NAT IP"

GSSH="sudo ssh -i ${MIG_KEY} -o StrictHostKeyChecking=accept-new -o ConnectTimeout=8 ubuntu@${NATIP}"
say "builder at ${NATIP} — waiting for docker install + image pulls (~3-6 min)"
for i in $(seq 1 150); do
  if $GSSH "test -f /var/lib/xdn-baked" 2>/dev/null; then say "images pulled"; break; fi
  sleep 5
done
$GSSH "test -f /var/lib/xdn-baked" 2>/dev/null || die "bake did not complete (images not pulled)"
$GSSH "sudo docker images --format '    {{.Repository}}:{{.Tag}} ({{.Size}})'"

say "cleaning image for re-templating (cloud-init/machine-id/ssh keys/apt, trim)"
$GSSH "sudo bash -c '
  cloud-init clean --logs --seed 2>/dev/null || true
  truncate -s0 /etc/machine-id; rm -f /var/lib/dbus/machine-id
  rm -f /etc/ssh/ssh_host_*; apt-get clean; rm -rf /var/lib/apt/lists/*
  rm -f /var/lib/xdn-baked; fstrim -av 2>/dev/null || true; sync'"

say "shutting down builder"
sudo virsh shutdown "$BUILDER"
for i in $(seq 1 45); do sudo virsh domstate "$BUILDER" 2>/dev/null | grep -q 'shut off' && break; sleep 2; done
sudo virsh domstate "$BUILDER" 2>/dev/null | grep -q 'shut off' || sudo virsh destroy "$BUILDER" 2>/dev/null || true

say "flattening + compressing into golden base ${BAKED_BASE}"
sudo rm -f "$BAKED_BASE"
sudo qemu-img convert -O qcow2 -c "$WORKIMG" "$BAKED_BASE"
sudo qemu-img info "$BAKED_BASE" | grep -E 'virtual size|disk size' | sed 's/^/    /'

say "removing builder"
sudo virsh undefine "$BUILDER" 2>/dev/null || true
sudo rm -f "$WORKIMG" "$SEED"

PEER="$(peer_ip)"
if [ -n "$PEER" ]; then
  say "pre-seeding golden base to peer ${PEER}"
  sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "root@${PEER}" "mkdir -p '$(dirname "$BAKED_BASE")'"
  sudo scp -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "$BAKED_BASE" "root@${PEER}:${BAKED_BASE}" >/dev/null
fi
say "done — ./02-create-vm.sh will now overlay the baked base (data-only delta)."
