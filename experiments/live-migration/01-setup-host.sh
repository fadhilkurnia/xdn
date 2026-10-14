#!/usr/bin/env bash
# Run on BOTH hosts (clnode349 and clnode396). Idempotent.
#
#   sudo -v && ./01-setup-host.sh
#
# Installs KVM/libvirt, starts the libvirt NAT network (guest internet for
# first-boot setup), and bridges the experiment NIC so a VM can live on
# 10.10.1.0/24 and keep its IP across migration. SSH is on the *control* NIC, so
# reconfiguring the experiment NIC here never drops your session.
set -euo pipefail
source "$(dirname "$0")/config.sh"

say "installing KVM / libvirt / tools"
sudo apt-get update -y
sudo DEBIAN_FRONTEND=noninteractive apt-get install -y \
  qemu-kvm libvirt-daemon-system libvirt-clients virtinst \
  cloud-image-utils bridge-utils genisoimage curl bc

sudo systemctl enable --now libvirtd

say "ensuring libvirt default NAT network is up (guest internet for setup)"
sudo virsh net-info default >/dev/null 2>&1 || \
  sudo virsh net-define /usr/share/libvirt/networks/default.xml
sudo virsh net-autostart default >/dev/null 2>&1 || true
sudo virsh net-start default >/dev/null 2>&1 || true

sudo mkdir -p "$WORKDIR"

# --- Bridge the experiment NIC ---------------------------------------------------
if ip link show "$BRIDGE" >/dev/null 2>&1; then
  say "bridge $BRIDGE already exists ($(my_lan_ip)/${VM_CIDR}); skipping"
else
  MYIP="$(ip -4 -o addr show "$EXP_IFACE" 2>/dev/null | awk '{print $4}' | head -1)"
  [ -n "$MYIP" ] || die "no IPv4 on $EXP_IFACE — set EXP_IFACE to the 10.10.1.x NIC"
  say "moving $MYIP from $EXP_IFACE onto bridge $BRIDGE"
  sudo ip link add name "$BRIDGE" type bridge
  sudo ip link set "$BRIDGE" up
  sudo ip addr flush dev "$EXP_IFACE"
  sudo ip addr add "$MYIP" dev "$BRIDGE"
  sudo ip link set "$EXP_IFACE" up
  sudo ip link set "$EXP_IFACE" master "$BRIDGE"
  # Let bridged guest frames pass without the host's iptables mangling them.
  sudo modprobe br_netfilter 2>/dev/null || true
  sudo sysctl -qw net.bridge.bridge-nf-call-iptables=0 2>/dev/null || true
  sudo sysctl -qw net.bridge.bridge-nf-call-ip6tables=0 2>/dev/null || true
fi

# Open the libvirt migration ports on the experiment LAN (block migration uses
# the 49152-49215 range; libvirtd listens on 16509/16514). CloudLab nodes are
# behind the campus firewall on the control net; the experiment LAN is private.
sudo iptables -C INPUT -i "$BRIDGE" -p tcp --dport 49152:49215 -j ACCEPT 2>/dev/null || \
  sudo iptables -I INPUT -i "$BRIDGE" -p tcp --dport 49152:49215 -j ACCEPT || true

say "host $(hostname) ready: $(my_lan_ip)/${VM_CIDR} on $BRIDGE"
echo "    NOTE: the bridge is not persisted across reboot — re-run this script if you reboot."
