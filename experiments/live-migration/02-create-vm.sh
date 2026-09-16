#!/usr/bin/env bash
# Run on the host where the replica should START (e.g. clnode349).
#
#   ./02-create-vm.sh
#
# Builds a KVM guest that runs the bookcatalog + postgres docker-compose, bridged
# onto 10.10.1.0/24 at VM_IP with a fixed MAC, plus a NAT NIC for first-boot
# internet. Boots it and waits for the service to answer.
set -euo pipefail
source "$(dirname "$0")/config.sh"

ip link show "$BRIDGE" >/dev/null 2>&1 || die "bridge $BRIDGE missing — run ./01-setup-host.sh first"
sudo mkdir -p "$WORKDIR"; cd "$WORKDIR"

DISK="$WORKDIR/${VM_NAME}.qcow2"
SEED="$WORKDIR/${VM_NAME}-seed.iso"

# Prefer the baked golden base (docker + images pre-pulled) so the overlay is just
# the data delta; otherwise fall back to the stock Ubuntu cloud image.
if sudo test -f "$BAKED_BASE"; then
  BASE="$BAKED_BASE"
  say "using baked base $BASE (docker + service images pre-pulled -> tiny delta)"
else
  BASE="$WORKDIR/jammy-server-cloudimg-amd64.img"
  if [ ! -f "$BASE" ]; then
    say "downloading Ubuntu 22.04 cloud image"
    sudo curl -fL --retry 3 -o "$BASE" "$UBUNTU_IMG_URL"
  fi
fi

say "creating a CoW overlay disk backed by the shared base image (delta-only migration)"
# The VM disk is a thin qcow2 OVERLAY on the read-only base image. The VM only
# writes its delta (docker, postgres data, ...) into the overlay; the big OS base
# is shared. Migration then ships just the overlay (see 03-migrate --copy-storage-inc).
sudo qemu-img create -f qcow2 -F qcow2 -b "$BASE" "$DISK" "${VM_DISK_GB}G" >/dev/null
# The backing file must exist at the SAME path on the destination for delta-only
# migration — pre-seed it on the peer now (over the LAN, before any WAN shaping).
PEER="$(peer_ip)"
if [ -n "$PEER" ]; then
  if sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "root@${PEER}" "test -f '$BASE'"; then
    say "base image already present on peer ${PEER}"
  else
    say "pre-seeding base image to peer ${PEER}:${BASE}"
    sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "root@${PEER}" "mkdir -p '$(dirname "$BASE")'"
    sudo scp -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "$BASE" "root@${PEER}:${BASE}" >/dev/null
  fi
fi

# SSH access into the guest (for debugging / reaching the replica directly).
# Use $GUEST_SSH_PUBKEY if set, else the host's migration pubkey if present.
GUEST_PUBKEY="${GUEST_SSH_PUBKEY:-}"
if [ -z "$GUEST_PUBKEY" ] && sudo test -f /root/.ssh/id_xdnmig.pub; then
  GUEST_PUBKEY="$(sudo cat /root/.ssh/id_xdnmig.pub)"
fi
SSH_KEYS_YAML=""
[ -n "$GUEST_PUBKEY" ] && SSH_KEYS_YAML=$'ssh_authorized_keys:\n  - '"$GUEST_PUBKEY"

# --- cloud-init: user-data (install docker, run the compose) --------------------
say "writing cloud-init (bookcatalog + postgres)"
sudo tee "$WORKDIR/user-data.yaml" >/dev/null <<EOF
#cloud-config
hostname: ${VM_NAME}
ssh_pwauth: true
password: xdnmigrate
chpasswd: { expire: false }
${SSH_KEYS_YAML}
write_files:
  - path: /opt/app/docker-compose.yaml
    permissions: '0644'
    content: |
      services:
        postgres:
          image: ${PG_IMAGE}
          environment:
            - POSTGRES_PASSWORD=root
            - POSTGRES_DB=books
          volumes:
            - pgdata:/var/lib/postgresql/data
          healthcheck:
            test: ["CMD-SHELL", "pg_isready -U postgres -d books"]
            interval: 3s
            timeout: 3s
            retries: 30
          restart: unless-stopped
        app:
          image: ${APP_IMAGE}
          ports:
            - "${SERVICE_PORT}:80"
          environment:
            - DB_TYPE=postgres
            - DB_HOST=postgres
          depends_on:
            postgres:
              condition: service_healthy
          restart: unless-stopped
      volumes:
        pgdata: {}
runcmd:
  # Install docker only if the base doesn't already have it (baked base does),
  # to keep the overlay delta minimal.
  - [ sh, -c, "command -v docker >/dev/null 2>&1 || curl -fsSL https://get.docker.com | sh" ]
  - [ sh, -c, "cd /opt/app && docker compose up -d --wait || docker compose up -d" ]
EOF

# --- cloud-init: network-config (static LAN NIC + DHCP NAT NIC, matched by MAC) --
sudo tee "$WORKDIR/network-config.yaml" >/dev/null <<EOF
version: 2
ethernets:
  lan:
    match: { macaddress: "${VM_MAC_LAN}" }
    set-name: lan
    addresses: [ "${VM_IP}/${VM_CIDR}" ]
    dhcp4: false
  nat:
    match: { macaddress: "${VM_MAC_NAT}" }
    set-name: nat
    dhcp4: true
EOF

sudo cloud-localds -N "$WORKDIR/network-config.yaml" "$SEED" "$WORKDIR/user-data.yaml"

say "defining + booting VM '${VM_NAME}' on $(hostname)"
sudo virt-install \
  --name "$VM_NAME" \
  --memory "$VM_RAM_MB" --vcpus "$VM_VCPUS" \
  --cpu host-passthrough \
  --os-variant ubuntu22.04 \
  --disk "path=${DISK},format=qcow2,bus=virtio" \
  --disk "path=${SEED},device=cdrom" \
  --network "bridge=${BRIDGE},model=virtio,mac=${VM_MAC_LAN}" \
  --network "network=default,model=virtio,mac=${VM_MAC_NAT}" \
  --graphics none --noautoconsole --import

say "VM defined. Waiting for the service at ${SERVICE_URL} (first boot pulls images, ~2-4 min)…"
for i in $(seq 1 90); do
  if curl -fsS --max-time 3 "$SERVICE_URL" >/dev/null 2>&1; then
    say "service is UP at ${SERVICE_URL}"
    curl -fsS "$SERVICE_URL"; echo
    exit 0
  fi
  sleep 5
done
echo "service did not answer yet — check 'sudo virsh console ${VM_NAME}' (login via cloud image) or"
echo "  ssh into the VM once up. Re-run ./05-verify.sh to poll."
