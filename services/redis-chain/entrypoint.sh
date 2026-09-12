#!/bin/sh
# Maps the XDN_CLUSTER_* contract onto chained Redis replication:
#   XDN_CLUSTER_ORDINAL    0 = chain head (master), N>0 replicates replica-(N-1)
#   XDN_CLUSTER_PEER_PORT  redis port used for replication (default 6379)
# Persistence is disabled: this image exists to exercise the chain-shaped
# coordination graph, not durability.
set -eu
: "${XDN_CLUSTER_ORDINAL:?XDN_CLUSTER_ORDINAL is required}"
PORT="${XDN_CLUSTER_PEER_PORT:-6379}"

ARGS="--port $PORT --bind 0.0.0.0 --protected-mode no --appendonly no --save ''"
if [ "$XDN_CLUSTER_ORDINAL" -gt 0 ]; then
  PRED="replica-$((XDN_CLUSTER_ORDINAL - 1))"
  # replica-read-only stays default (yes): reads at any hop, writes only at the head.
  ARGS="$ARGS --replicaof $PRED $PORT"
fi

# shellcheck disable=SC2086
exec redis-server $ARGS
