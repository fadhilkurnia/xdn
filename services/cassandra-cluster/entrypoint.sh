#!/bin/bash
# Maps the XDN_CLUSTER_* contract onto Cassandra:
#   XDN_CLUSTER_SELF       replica-N overlay alias -> listen/broadcast address
#   XDN_CLUSTER_PEERS      first peer (replica-0) -> gossip seed
#   XDN_CLUSTER_PEER_PORT  storage (gossip/streaming) port, default 7000
#
# The container's own hostname resolves to the bridge IP first (see the
# dual-homed networking notes), so the overlay address peers can dial must be
# resolved from the replica-N alias through Docker's embedded DNS.
set -eu
: "${XDN_CLUSTER_SELF:?XDN_CLUSTER_SELF is required}"
: "${XDN_CLUSTER_PEERS:?XDN_CLUSTER_PEERS is required}"

# The alias registers when this container attaches to the overlay; it resolves
# by the time the entrypoint runs (create -> connect -> start), but retry a few
# times to be safe.
resolve() {
  for _ in $(seq 1 30); do
    ip=$(getent hosts "$1" | awk '{print $1; exit}') && [ -n "$ip" ] && { echo "$ip"; return 0; }
    sleep 1
  done
  echo "failed to resolve $1" >&2
  return 1
}

SELF_IP=$(resolve "$XDN_CLUSTER_SELF")
SEED_NAME=$(echo "$XDN_CLUSTER_PEERS" | cut -d, -f1)
SEED_IP=$(resolve "$SEED_NAME")

export CASSANDRA_CLUSTER_NAME="xdn-cassandra"
export CASSANDRA_LISTEN_ADDRESS="$SELF_IP"
export CASSANDRA_BROADCAST_ADDRESS="$SELF_IP"
export CASSANDRA_RPC_ADDRESS="0.0.0.0"
export CASSANDRA_BROADCAST_RPC_ADDRESS="$SELF_IP"
export CASSANDRA_SEEDS="$SEED_IP"
# Single token keeps the ring layout deterministic and small-cluster friendly.
export CASSANDRA_NUM_TOKENS=1
export CASSANDRA_ENDPOINT_SNITCH="GossipingPropertyFileSnitch"
export CASSANDRA_DC="dc1"
export CASSANDRA_RACK="rack1"
# Modest heap so three nodes fit small hosts.
export MAX_HEAP_SIZE="${MAX_HEAP_SIZE:-1G}"
export HEAP_NEWSIZE="${HEAP_NEWSIZE:-200M}"

echo "xdn-cassandra: self=$XDN_CLUSTER_SELF ($SELF_IP) seed=$SEED_NAME ($SEED_IP)"
exec /usr/local/bin/docker-entrypoint.sh cassandra -f
