#!/usr/bin/env bash
# Latency-optimization harness for one isolated XDN group. Runs ON the group's
# RC host (which is also AR1). Drives an end-to-end build -> deploy -> start ->
# launch -> measure cycle so an optimizer can score a code change on real
# cross-host replication latency.
#
# A group is defined by two env vars:
#   OPT_CONFIG   path (repo-relative) to the group's gigapaxos properties
#   OPT_NODES    space list of "ip:nid:httpport"; RC (nid 0) first, then ARs
# e.g. OPT_CONFIG=conf/gigapaxos.optA.utah.properties \
#      OPT_NODES="10.10.1.1:0:3300 10.10.1.1:1:2300 10.10.1.2:2:2300 10.10.1.3:3:2300"
#
# Subcommands:
#   build            ant jar + build_xdn_cli on this host
#   deploy           rsync ~/xdn to the group's OTHER hosts (jars+bin+conf)
#   start            kill/wipe/start RC + ARs across the group
#   launch <det>     launch bookcatalog (det=true Paxos / false primary-backup)
#   measure <svc> <dur>   sequential latency probe against the leader; prints JSON
#   destroy <svc>
#   cycle <det> <dur>     build+deploy+start+launch+measure in one shot
set -uo pipefail
cd "$(cd "$(dirname "$0")/.." && pwd)"
: "${OPT_CONFIG:?set OPT_CONFIG}"; : "${OPT_NODES:?set OPT_NODES}"
JF='-ea -Djavax.net.ssl.keyStorePassword=qwerty -Djavax.net.ssl.trustStorePassword=qwerty -Djavax.net.ssl.keyStore=conf/keyStore.jks -Djavax.net.ssl.trustStore=conf/trustStore.jks -Djava.util.logging.config.file=conf/logging.properties -Dlog4j.configuration=conf/log4j.properties -DgigapaxosConfig='"$OPT_CONFIG"' -Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.base/java.nio.channels.spi=ALL-UNNAMED'
export PATH="$PATH:/usr/local/go/bin"
rc_ip() { echo "$OPT_NODES" | tr ' ' '\n' | head -1 | cut -d: -f1; }
other_hosts() { echo "$OPT_NODES" | tr ' ' '\n' | cut -d: -f1 | sort -u | grep -v "^$(rc_ip)$"; }

cmd_build() { ant jar >/tmp/opt-build.log 2>&1 && bash bin/build_xdn_cli.sh >>/tmp/opt-build.log 2>&1 && echo build-ok || { echo build-FAIL; tail -5 /tmp/opt-build.log; return 1; }; }
cmd_deploy() {
  for h in $(other_hosts); do
    rsync -az --delete --exclude=.git --exclude=out --exclude=build ~/xdn/ "$h":xdn/ >/dev/null 2>&1 \
      && echo "deployed $h" || echo "deploy-FAIL $h"
  done
}
cmd_start() {
  for h in $(echo "$OPT_NODES" | tr ' ' '\n' | cut -d: -f1 | sort -u); do
    ssh -o BatchMode=yes "$h" 'pkill -9 -f "[R]econfigurableNode"; sleep 1; sudo rm -rf /tmp/gigapaxos /tmp/xdn' </dev/null 2>/dev/null
  done
  for triple in $OPT_NODES; do
    IFS=: read -r ip nid _ <<<"$triple"
    ssh -fn -o BatchMode=yes "$ip" "cd ~/xdn && mkdir -p logs && nohup java $JF -cp \$(ls jars/*.jar | tr '\n' ':') edu.umass.cs.reconfiguration.ReconfigurableNode $nid > logs/node-$nid.log 2> logs/node-$nid.err < /dev/null &" </dev/null
  done
  for triple in $OPT_NODES; do
    IFS=: read -r ip _ port <<<"$triple"
    for _ in $(seq 1 60); do timeout 1 bash -c "</dev/tcp/$ip/$port" 2>/dev/null && break; sleep 2; done
  done
  echo "started"
}
cmd_launch() {
  local det="${1:-true}" img="fadhilkurnia/xdn-bookcatalog"
  [ "$det" = false ] && img="fadhilkurnia/xdn-bookcatalog-nd"
  echo y | XDN_CONTROL_PLANE="$(rc_ip)" timeout 180 ./bin/xdn launch bookcatalog --image="$img" --state=/app/data/ --deterministic="$det" 2>&1 | grep -E "successfully|ERROR" | head -1
}
# Pin the leader/primary to AR1 (the AR co-located with the RC + driver) so the
# measured client->leader hop is always loopback and the number isolates
# server-side coordination+execution, not the variable driver->remote-leader
# network RTT. AR1's node id is the 2nd OPT_NODES entry.
cmd_pin() {
  local svc="${1:-bookcatalog}"
  local nid1; nid1=$(echo "$OPT_NODES" | tr ' ' '\n' | sed -n 2p | cut -d: -f2)
  echo y | XDN_CONTROL_PLANE="$(rc_ip)" timeout 60 ./bin/xdn service leader "$svc" "$nid1" 2>&1 | tail -1
  sleep 4
}
# Always measure against the driver-local AR1 frontend (127.0.0.1:2300) so the
# client hop is fixed across runs and groups.
cmd_measure() {
  local svc="${1:-bookcatalog}" dur="${2:-30}"
  python3 eval/opt_seq_latency.py --host 127.0.0.1 --port 2300 --service "$svc" --duration "$dur" --warmup 5
}
cmd_destroy() { XDN_CONTROL_PLANE="$(rc_ip)" ./bin/xdn service destroy "${1:-bookcatalog}" --yes 2>&1 | tail -1; }
cmd_cycle() {
  local det="${1:-true}" dur="${2:-30}"
  cmd_build || return 1
  cmd_deploy; cmd_start; cmd_launch "$det"; sleep 6; cmd_pin bookcatalog; cmd_measure bookcatalog "$dur"
}

sub="${1:-}"; shift || true
case "$sub" in
  build) cmd_build;; deploy) cmd_deploy;; start) cmd_start;;
  launch) cmd_launch "$@";; pin) cmd_pin "$@";; measure) cmd_measure "$@";; destroy) cmd_destroy "$@";;
  cycle) cmd_cycle "$@";;
  *) echo "usage: OPT_CONFIG=.. OPT_NODES=.. $0 {build|deploy|start|launch <det>|pin <svc>|measure <svc> <dur>|destroy <svc>|cycle <det> <dur>}"; exit 1;;
esac
