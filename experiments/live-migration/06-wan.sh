#!/usr/bin/env bash
# Emulate a WAN link on the experiment NIC of BOTH hosts, so live migration can
# be tested under latency + bandwidth limits. Shapes egress with tc/netem; the
# delay is per-direction, so RTT ≈ 2 × delay.
#
#   ./06-wan.sh on  [delay] [rate]   # default 25ms each-way (~50ms RTT), 1gbit
#   ./06-wan.sh off
#   ./06-wan.sh show
#
# Run it from the host that holds the VM (it also configures the peer over the
# migration SSH key). Affects all experiment-LAN traffic (migration + service),
# which is the point — both the copy and the clients see the WAN.
set -euo pipefail
source "$(dirname "$0")/config.sh"

ACTION="${1:-show}"
DELAY="${2:-25ms}"
RATE="${3:-1gbit}"
PEER="$(peer_ip)"

cmd_for() {
  case "$ACTION" in
    on)   echo "tc qdisc replace dev ${EXP_IFACE} root netem delay ${DELAY} rate ${RATE}";;
    off)  echo "tc qdisc del dev ${EXP_IFACE} root 2>/dev/null || true";;
    show) echo ":";;
    *)    die "usage: $0 {on|off|show} [delay] [rate]";;
  esac
}
C="$(cmd_for)"

echo "== $(hostname) ($(my_lan_ip_any)) =="
sudo sh -c "$C; tc qdisc show dev ${EXP_IFACE}"

if [ -n "$PEER" ]; then
  echo "== peer ${PEER} =="
  sudo ssh -i "$MIG_KEY" -o StrictHostKeyChecking=accept-new "root@${PEER}" \
    "$C; tc qdisc show dev ${EXP_IFACE}"
fi

if [ "$ACTION" = "on" ]; then
  rtt=$(( ${DELAY%ms} * 2 ))
  say "WAN emulated on ${EXP_IFACE} (both ends): ${DELAY} each-way (~${rtt}ms RTT), ${RATE} per direction"
elif [ "$ACTION" = "off" ]; then
  say "WAN emulation removed (line-rate LAN restored)"
fi
