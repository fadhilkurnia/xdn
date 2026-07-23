#!/bin/bash
# Maps the XDN_CLUSTER_* contract onto a MongoDB replica set:
#   XDN_CLUSTER_ORDINAL    ordinal 0 initiates the replica set
#   XDN_CLUSTER_SIZE       member count; members addressed replica-0..N-1
#   XDN_CLUSTER_PEER_PORT  mongod port (peers and clients share it)
set -eu
: "${XDN_CLUSTER_ORDINAL:?XDN_CLUSTER_ORDINAL is required}"
: "${XDN_CLUSTER_SIZE:?XDN_CLUSTER_SIZE is required}"
PORT="${XDN_CLUSTER_PEER_PORT:-27017}"

if [ "$XDN_CLUSTER_ORDINAL" = "0" ]; then
  (
    members=""
    for i in $(seq 0 $((XDN_CLUSTER_SIZE - 1))); do
      members="${members}{_id:${i},host:\"replica-${i}:${PORT}\"},"
    done
    cfg="{_id:\"rs0\",members:[${members%,}]}"
    # Retry until every member's mongod is reachable and the initiate sticks;
    # simultaneous container starts make early attempts fail harmlessly.
    until mongosh --port "$PORT" --quiet --eval "rs.status().ok" >/dev/null 2>&1; do
      mongosh --port "$PORT" --quiet --eval "rs.initiate(${cfg})" >/dev/null 2>&1 || true
      sleep 3
    done
    echo "xdn-mongo: replica set initiated"
  ) &
fi

exec docker-entrypoint.sh mongod --replSet rs0 --bind_ip_all --port "$PORT"
