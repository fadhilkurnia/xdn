#!/bin/bash
# Maps the XDN_CLUSTER_* contract onto a Corfu server:
#   XDN_CLUSTER_SELF       replica-N overlay alias -> -a (bind + advertised
#                          address; must match the layout's endpoint strings)
#   XDN_CLUSTER_PEER_PORT  server port (layout endpoints are replica-N:PORT)
# Memory mode: no log path, no file-permission interaction with --user.
set -eu
: "${XDN_CLUSTER_SELF:?XDN_CLUSTER_SELF is required}"
PORT="${XDN_CLUSTER_PEER_PORT:-9000}"

# The stock JAVA_OPTS logs gc to /var/log/corfu and needs a prepared tmpdir;
# keep it minimal instead so nothing writes outside the container tmp.
export JAVA_OPTS="-XX:+UseG1GC -Djava.io.tmpdir=/image/corfu-server/temp"

echo "xdn-corfu: self=$XDN_CLUSTER_SELF port=$PORT (memory mode)"

# Self-clustering is the service's job: ordinal 0 installs the chain layout
# in the background once the members answer (retrying while they boot; an
# already-installed layout ends the loop). Frontends and XDN stay dumb.
if [ "${XDN_CLUSTER_ORDINAL:-}" = "0" ]; then
  (
    SIZE="${XDN_CLUSTER_SIZE:-3}"
    servers=""
    for i in $(seq 0 $((SIZE - 1))); do
      servers="${servers:+$servers,}\"replica-$i:$PORT\""
    done
    cat > /tmp/xdn-layout.json <<EOF
{"layoutServers":[$servers],"sequencers":[$servers],
 "segments":[{"replicationMode":"CHAIN_REPLICATION","start":0,"end":-1,
              "stripes":[{"logServers":[$servers]}]}],
 "unresponsiveServers":[],"epoch":0}
EOF
    for _ in $(seq 1 60); do
      out=$(/usr/share/corfu/bin/corfu_bootstrap_cluster \
              -l /tmp/xdn-layout.json --connection-timeout 5000 2>&1) || true
      case "$out" in
        *"installed successfully"*) echo "[xdn-corfu] layout bootstrapped"; break ;;
        *lreadyBootstrapped*|*already*) echo "[xdn-corfu] layout already installed"; break ;;
      esac
      sleep 5
    done
  ) &
fi

cd /app
exec java -cp "/app/*" $JAVA_OPTS \
    -Dlogback.configurationFile=/usr/share/corfu/conf/logback.prod.xml \
    org.corfudb.infrastructure.CorfuServer -m -a "$XDN_CLUSTER_SELF" "$PORT"
