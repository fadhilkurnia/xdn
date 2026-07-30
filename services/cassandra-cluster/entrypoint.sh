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

# At larger cluster sizes, simultaneous non-seed bootstraps can collide
# ("other bootstrapping nodes detected"); stagger joins by ordinal once the
# cluster is bigger than a triple. No-op for the seed and at size <= 3.
ORD="${XDN_CLUSTER_ORDINAL:-0}"
SIZE="${XDN_CLUSTER_SIZE:-3}"
if [ "$ORD" != "0" ] && [ "$SIZE" -gt 3 ]; then
  STAGGER=$(( ORD * ${XDN_CASS_JOIN_STAGGER_S:-15} ))
  echo "xdn-cassandra: staggering join by ${STAGGER}s (ordinal $ORD of $SIZE)"
  sleep "$STAGGER"
fi

export CASSANDRA_CLUSTER_NAME="xdn-cassandra"
export CASSANDRA_LISTEN_ADDRESS="$SELF_IP"
export CASSANDRA_BROADCAST_ADDRESS="$SELF_IP"
export CASSANDRA_RPC_ADDRESS="0.0.0.0"
export CASSANDRA_BROADCAST_RPC_ADDRESS="$SELF_IP"
export CASSANDRA_SEEDS="$SEED_IP"
# Single token keeps the ring layout deterministic and small-cluster friendly.
export CASSANDRA_NUM_TOKENS=1

# Work on a conf tree WE own: /etc/cassandra in the stock image is a
# sticky-bit (1777) directory whose files belong to the cassandra user, and
# under a non-root --user the in-place sed renames (ours AND the stock
# entrypoint's) hit EPERM on hosts with strict sticky/rename policies. The
# stock docker-entrypoint.sh honors CASSANDRA_CONF, so a private copy makes
# every config edit host-independent.
export CASSANDRA_CONF=/tmp/xdn-cassconf
mkdir -p "$CASSANDRA_CONF"
cp -a /etc/cassandra/. "$CASSANDRA_CONF"/

# Deterministic, evenly spaced Murmur3 token per ordinal. With num_tokens=1
# each joiner otherwise picks ONE RANDOM token, and at larger cluster sizes
# two simultaneous joiners can collide ("Bootstrap Token collision"), which
# fails the bootstrap stream and wedges the member with CQL never opening.
# Fixed tokens make collisions impossible and the ring perfectly balanced.
# step ~= 2^63/SIZE; token_i = -(2^63-1) + (2i+1)*step stays in signed range.
STEP=$(( 9223372036854775807 / SIZE ))
TOKEN=$(( -9223372036854775807 + (2 * ORD + 1) * STEP ))
sed -ri "s/^# initial_token:.*/initial_token: ${TOKEN}/" "$CASSANDRA_CONF/cassandra.yaml"
echo "xdn-cassandra: ordinal $ORD/$SIZE initial_token=$TOKEN conf=$CASSANDRA_CONF"
export CASSANDRA_ENDPOINT_SNITCH="GossipingPropertyFileSnitch"
export CASSANDRA_DC="dc1"
export CASSANDRA_RACK="rack1"
# Modest heap so three nodes fit small hosts.
export MAX_HEAP_SIZE="${MAX_HEAP_SIZE:-1G}"
export HEAP_NEWSIZE="${HEAP_NEWSIZE:-200M}"

echo "xdn-cassandra: self=$XDN_CLUSTER_SELF ($SELF_IP) seed=$SEED_NAME ($SEED_IP)"
# Supervise IN-CONTAINER instead of exec'ing: on slow disks a joiner can hit
# cassandra's gossip-with-seeds startup deadline and exit; letting docker's
# restart policy revive the CONTAINER recreates the netns, stranding the
# netns-sharing sidecars (frontend, probe) in the dead namespace and churning
# the overlay IP peers have gossiped. Restarting only the PROCESS keeps the
# namespace and IP stable.
while true; do
  /usr/local/bin/docker-entrypoint.sh cassandra -f && exit 0
  echo "xdn-cassandra: cassandra exited ($?); retrying in 10s"
  sleep 10
done
