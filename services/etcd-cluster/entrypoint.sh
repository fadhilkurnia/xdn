#!/bin/sh
# Maps the XDN_CLUSTER_* env contract (set by `xdn cluster launch`) onto etcd's
# native ETCD_* flags. Then exec's etcd.
#
# Required env (provided by XdnGigapaxosApp.initContainerizedClusterService):
#   XDN_CLUSTER_ORDINAL   this replica's 0-based ordinal
#   XDN_CLUSTER_SIZE      total cluster size
#   XDN_CLUSTER_SELF      stable hostname (replica-N)
#   XDN_CLUSTER_PEERS     comma-separated replica names (replica-0,..,replica-N-1)
#   XDN_CLUSTER_PEER_PORT etcd peer port (2380 by default)
#   XDN_CLUSTER_PHASE     "bootstrap" on epoch 0, "join" when added later
set -eu

: "${XDN_CLUSTER_ORDINAL:?XDN_CLUSTER_ORDINAL is required}"
: "${XDN_CLUSTER_SIZE:?XDN_CLUSTER_SIZE is required}"
: "${XDN_CLUSTER_SELF:?XDN_CLUSTER_SELF is required}"
: "${XDN_CLUSTER_PEERS:?XDN_CLUSTER_PEERS is required}"

PEER_PORT="${XDN_CLUSTER_PEER_PORT:-2380}"
CLIENT_PORT="${ETCD_CLIENT_PORT:-2379}"
DATA_DIR="${ETCD_DATA_DIR:-/etcd-data}"
TOKEN="${ETCD_INITIAL_CLUSTER_TOKEN:-xdn-etcd-cluster}"

# Build INITIAL_CLUSTER as "replica-0=http://replica-0:2380,replica-1=http://replica-1:2380,..."
INITIAL_CLUSTER=""
old_ifs="$IFS"
IFS=,
for name in $XDN_CLUSTER_PEERS; do
  entry="${name}=http://${name}:${PEER_PORT}"
  if [ -z "$INITIAL_CLUSTER" ]; then
    INITIAL_CLUSTER="$entry"
  else
    INITIAL_CLUSTER="${INITIAL_CLUSTER},${entry}"
  fi
done
IFS="$old_ifs"

case "${XDN_CLUSTER_PHASE:-bootstrap}" in
  join) CLUSTER_STATE=existing ;;
  *)    CLUSTER_STATE=new ;;
esac

mkdir -p "$DATA_DIR"

exec etcd \
  --name="$XDN_CLUSTER_SELF" \
  --data-dir="$DATA_DIR" \
  --initial-cluster-token="$TOKEN" \
  --initial-cluster="$INITIAL_CLUSTER" \
  --initial-cluster-state="$CLUSTER_STATE" \
  --initial-advertise-peer-urls="http://${XDN_CLUSTER_SELF}:${PEER_PORT}" \
  --listen-peer-urls="http://0.0.0.0:${PEER_PORT}" \
  --advertise-client-urls="http://${XDN_CLUSTER_SELF}:${CLIENT_PORT}" \
  --listen-client-urls="http://0.0.0.0:${CLIENT_PORT}"
