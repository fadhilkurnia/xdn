#!/usr/bin/env bash
# Standalone smoke test for the xdn-mysql-cluster image — validates that the MySQL Group
# Replication entrypoint forms a 3-node group WITHOUT the rest of the XDN stack (no swarm,
# no reconfigurator). This isolates the hardest part of XdnWordPressClusterTest so it can be
# iterated on in ~2 minutes locally instead of via CI.
#
# Usage:
#   ./services/mysql-cluster/smoke-test.sh           # run, report, then tear down
#   KEEP=1 ./services/mysql-cluster/smoke-test.sh    # leave containers up for inspection
#   WAIT=90 ./services/mysql-cluster/smoke-test.sh   # override the convergence wait (seconds)
#
# Exit code 0 means all 3 members reached ONLINE.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
IMAGE="xdn-mysql-cluster:test"
NETWORK="xdn-gr-smoke"
DATA_ROOT="/tmp/xdn-gr-smoke"
SIZE=3
PEER_PORT=33061
ROOT_PW="supersecret"
# WAIT is the total polling budget. Datadir --initialize alone can take ~40s per node, and 3
# init concurrently; then the real server starts, the group bootstraps, and joiners recover —
# so convergence can take several minutes on a modest machine. The poll exits as soon as the
# group is up, so a generous budget is harmless.
WAIT="${WAIT:-360}"

cleanup() {
  for i in $(seq 0 $((SIZE - 1))); do
    docker rm -f "replica-$i" >/dev/null 2>&1 || true
  done
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
  rm -rf "$DATA_ROOT" 2>/dev/null || true
}

# Always start from a clean slate; tear down on exit unless KEEP=1.
cleanup
if [ "${KEEP:-0}" != "1" ]; then
  trap cleanup EXIT
fi

echo "=== building $IMAGE ==="
docker build -t "$IMAGE" "$REPO_ROOT/services/mysql-cluster/"

echo "=== creating overlay-equivalent bridge network $NETWORK ==="
docker network create "$NETWORK" >/dev/null

echo "=== launching $SIZE members (mimicking XDN: non-root --user, bind-mounted datadir) ==="
for i in $(seq 0 $((SIZE - 1))); do
  mkdir -p "$DATA_ROOT/replica-$i"
  docker run -d \
    --name "replica-$i" \
    --hostname "replica-$i" \
    --network "$NETWORK" \
    --network-alias "replica-$i" \
    --user "$(id -u):$(id -g)" \
    -v /etc/passwd:/etc/passwd:ro \
    --mount "type=bind,source=$DATA_ROOT/replica-$i,target=/var/lib/mysql" \
    -e XDN_CLUSTER_ORDINAL="$i" \
    -e XDN_CLUSTER_SIZE="$SIZE" \
    -e XDN_CLUSTER_SELF="replica-$i" \
    -e XDN_CLUSTER_PEERS="replica-0,replica-1,replica-2" \
    -e XDN_CLUSTER_PEER_PORT="$PEER_PORT" \
    -e XDN_CLUSTER_PHASE=bootstrap \
    -e MYSQL_ROOT_PASSWORD="$ROOT_PW" \
    "$IMAGE" >/dev/null
  echo "  started replica-$i"
done

echo "=== polling up to ${WAIT}s for ${SIZE}x ONLINE (datadir init + bootstrap is slow) ==="
deadline=$(( $(date +%s) + WAIT ))
states=""
online=0
while [ "$(date +%s)" -lt "$deadline" ]; do
  states="$(docker exec replica-0 mysql -uroot -p"$ROOT_PW" -N -B \
    -e "SELECT MEMBER_HOST, MEMBER_STATE FROM performance_schema.replication_group_members" \
    2>/dev/null || true)"
  online="$(printf '%s\n' "$states" | grep -c ONLINE || true)"
  echo "  [$(date +%H:%M:%S)] ${online}/${SIZE} ONLINE"
  if [ "$online" = "$SIZE" ]; then break; fi
  sleep 10
done

echo "=== final member states ==="
echo "${states:-  (replica-0 not queryable)}"

echo "=== replica-0 log (tail) ==="
docker logs --tail 40 replica-0 2>&1 || true

echo
if [ "$online" = "$SIZE" ]; then
  echo "RESULT: PASS — all ${SIZE} members ONLINE"
  exit 0
else
  echo "RESULT: FAIL — only ${online:-0}/${SIZE} members ONLINE"
  echo "(inspect with: docker logs replica-1 ; docker logs replica-2)"
  [ "${KEEP:-0}" = "1" ] && echo "(containers left running; remove with: $0 then KEEP unset, or docker rm -f replica-0 replica-1 replica-2)"
  exit 1
fi
