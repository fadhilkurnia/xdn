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
cd /app
exec java -cp "/app/*" $JAVA_OPTS \
    -Dlogback.configurationFile=/usr/share/corfu/conf/logback.prod.xml \
    org.corfudb.infrastructure.CorfuServer -m -a "$XDN_CLUSTER_SELF" "$PORT"
