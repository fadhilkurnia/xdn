#!/usr/bin/env bash
# Run this ONCE, from your LAPTOP (which can SSH to both CloudLab hosts).
#
# Live migration needs root on the source host to SSH into root on the
# destination host. This bootstraps a shared root SSH key trust between the two
# nodes over the experiment LAN (10.10.1.0/24), so `virsh migrate
# qemu+ssh://root@<peer>/system` works passwordlessly in both directions.
#
#   ./00-exchange-keys.sh
#
# Assumes you can already `ssh <SSH_USER>@<host>` (your CloudLab key) and that
# the user has passwordless sudo (true on CloudLab).
set -euo pipefail
source "$(dirname "$0")/config.sh"

# SSH options for reaching the hosts from the laptop. Override SSH_KEY if needed.
SSH_KEY="${SSH_KEY:-$HOME/.ssh/cloudlab2}"
SSH_OPTS=(-o StrictHostKeyChecking=accept-new -o UserKnownHostsFile=/dev/null -o BatchMode=yes)
[ -f "$SSH_KEY" ] && SSH_OPTS+=(-i "$SSH_KEY")

# A throwaway keypair shared by both hosts' root users, just for migration.
TMP="$(mktemp -d)"; trap 'rm -rf "$TMP"' EXIT
ssh-keygen -t ed25519 -N "" -C "xdn-live-migration" -f "$TMP/id_xdnmig" >/dev/null
PUB="$(cat "$TMP/id_xdnmig.pub")"
PRIV="$(cat "$TMP/id_xdnmig")"

install_on() {
  local dns="$1"
  say "installing migration key on $dns"
  # Pipe the private key over stdin so it never lands on disk locally-as-arg.
  printf '%s\n' "$PRIV" | ssh "${SSH_OPTS[@]}" "${SSH_USER}@${dns}" \
    "sudo install -d -m700 /root/.ssh && \
     sudo tee ${MIG_KEY} >/dev/null && sudo chmod 600 ${MIG_KEY} && \
     echo '${PUB}' | sudo tee ${MIG_KEY}.pub >/dev/null && \
     ( ! sudo grep -qF 'xdn-live-migration' /root/.ssh/authorized_keys 2>/dev/null && \
         echo '${PUB}' | sudo tee -a /root/.ssh/authorized_keys >/dev/null || true ) && \
     printf 'Host ${HOST0_IP} ${HOST1_IP}\n  User root\n  IdentityFile ${MIG_KEY}\n  StrictHostKeyChecking accept-new\n  UserKnownHostsFile /root/.ssh/known_hosts\n' | sudo tee /root/.ssh/config.d_xdnmig >/dev/null && \
     sudo sh -c 'grep -q xdnmig /root/.ssh/config 2>/dev/null || cat /root/.ssh/config.d_xdnmig >> /root/.ssh/config' && \
     sudo chmod 600 /root/.ssh/config"
}

install_on "$HOST0_DNS"
install_on "$HOST1_DNS"

say "verifying root SSH ${HOST0_IP} <-> ${HOST1_IP} over the experiment LAN"
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST0_DNS}" \
  "sudo ssh -i ${MIG_KEY} -o StrictHostKeyChecking=accept-new root@${HOST1_IP} hostname" \
  && echo "  ${HOST0_IP} -> ${HOST1_IP} OK"
ssh "${SSH_OPTS[@]}" "${SSH_USER}@${HOST1_DNS}" \
  "sudo ssh -i ${MIG_KEY} -o StrictHostKeyChecking=accept-new root@${HOST0_IP} hostname" \
  && echo "  ${HOST1_IP} -> ${HOST0_IP} OK"

say "done — root-to-root SSH for migration is ready on both nodes."
