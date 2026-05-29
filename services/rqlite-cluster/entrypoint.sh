#!/bin/sh
# Maps the XDN_CLUSTER_* env contract (set by `xdn cluster launch`) onto rqlited's
# flags. Then exec's rqlited.
#
# Required env (provided by XdnGigapaxosApp.initContainerizedClusterService):
#   XDN_CLUSTER_ORDINAL   this replica's 0-based ordinal
#   XDN_CLUSTER_SIZE      total cluster size — used for -bootstrap-expect
#   XDN_CLUSTER_SELF      stable hostname (replica-N) — used for -node-id and adv addrs
#   XDN_CLUSTER_PEERS     comma-separated replica names
#   XDN_CLUSTER_PEER_PORT raft port (default 4002)
#   XDN_CLUSTER_PHASE     "bootstrap" on epoch 0, "join" when added later
set -eu

: "${XDN_CLUSTER_ORDINAL:?XDN_CLUSTER_ORDINAL is required}"
: "${XDN_CLUSTER_SIZE:?XDN_CLUSTER_SIZE is required}"
: "${XDN_CLUSTER_SELF:?XDN_CLUSTER_SELF is required}"
: "${XDN_CLUSTER_PEERS:?XDN_CLUSTER_PEERS is required}"

RAFT_PORT="${XDN_CLUSTER_PEER_PORT:-4002}"
HTTP_PORT="${RQLITE_HTTP_PORT:-4001}"
DATA_DIR="${RQLITE_DATA_DIR:-/rqlite-data}"

# Build join list: replica-0:4002,replica-1:4002,... (rqlite 8 wants the raft host:port
# here, not the HTTP one — node-to-node join goes over the Raft transport)
JOIN=""
old_ifs="$IFS"
IFS=,
for name in $XDN_CLUSTER_PEERS; do
  url="${name}:${RAFT_PORT}"
  if [ -z "$JOIN" ]; then
    JOIN="$url"
  else
    JOIN="${JOIN},${url}"
  fi
done
IFS="$old_ifs"

mkdir -p "$DATA_DIR"

# On bootstrap every node passes -bootstrap-expect <N> and the same -join URL list, so the
# first N nodes that reach each other form the cluster atomically. On join (reconfig) we
# drop -bootstrap-expect; the node finds an existing leader from the -join list.
case "${XDN_CLUSTER_PHASE:-bootstrap}" in
  join)
    exec rqlited \
      -node-id "$XDN_CLUSTER_SELF" \
      -http-addr "0.0.0.0:${HTTP_PORT}" \
      -raft-addr "0.0.0.0:${RAFT_PORT}" \
      -http-adv-addr "${XDN_CLUSTER_SELF}:${HTTP_PORT}" \
      -raft-adv-addr "${XDN_CLUSTER_SELF}:${RAFT_PORT}" \
      -join "$JOIN" \
      "$DATA_DIR"
    ;;
  *)
    exec rqlited \
      -node-id "$XDN_CLUSTER_SELF" \
      -http-addr "0.0.0.0:${HTTP_PORT}" \
      -raft-addr "0.0.0.0:${RAFT_PORT}" \
      -http-adv-addr "${XDN_CLUSTER_SELF}:${HTTP_PORT}" \
      -raft-adv-addr "${XDN_CLUSTER_SELF}:${RAFT_PORT}" \
      -bootstrap-expect "${XDN_CLUSTER_SIZE}" \
      -join "$JOIN" \
      "$DATA_DIR"
    ;;
esac
