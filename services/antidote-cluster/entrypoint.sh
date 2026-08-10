#!/bin/sh
# Maps the XDN_CLUSTER_* contract onto AntidoteDB. Each replica is an
# independent antidote DC; the Erlang node name is pinned to the overlay IP
# (resolved from the replica-N alias) because peer DCs must dial that
# address and the inter-DC descriptor derives from it. The container's own
# hostname resolves to the bridge IP first (see the dual-homed networking
# notes), so the alias must be resolved through embedded DNS.
set -eu
: "${XDN_CLUSTER_SELF:?XDN_CLUSTER_SELF is required}"

# The alias registers when this container attaches to the overlay; it
# resolves by the time the entrypoint runs, but retry a few times to be safe.
resolve() {
  for _ in $(seq 1 30); do
    ip=$(getent hosts "$1" | awk '{print $1; exit}') && [ -n "$ip" ] && { echo "$ip"; return 0; }
    sleep 1
  done
  echo "failed to resolve $1" >&2
  return 1
}

SELF_IP=$(resolve "$XDN_CLUSTER_SELF")
# The release uses -sname (short names): the host part must be dotless, so
# the replica-N alias itself is the node host — unique per member and
# resolvable through embedded DNS to the overlay IP from every container.
export NODE_NAME="antidote@${XDN_CLUSTER_SELF}"
echo "xdn-antidote: self=$XDN_CLUSTER_SELF ($SELF_IP) node=$NODE_NAME"

# Self-clustering is the service's job: ordinal 0 links the DCs in the
# background once every node is up (see xdnselflink.escript).
if [ "${XDN_CLUSTER_ORDINAL:-}" = "0" ]; then
  (/antidote/erts-*/bin/escript /xdnselflink.escript || true) &
fi

# The stock image's inline ENTRYPOINT references two vars it never defaults
# (empty expansions break the erl flag pairing); default everything here so
# the exec line below is well-formed.
ERLANG_DIST_PORT_MIN="${ERLANG_DIST_PORT_MIN:-9100}"
ERLANG_DIST_PORT_MAX="${ERLANG_DIST_PORT_MAX:-9100}"
ANTIDOTE_RECOVER_METADATA_ON_START="${ANTIDOTE_RECOVER_METADATA_ON_START:-${ANTIDOTE_META_DATA_ON_START:-true}}"

# Same command the stock image's inline ENTRYPOINT runs (overriding
# ENTRYPOINT discards it, so it is replicated verbatim; the remaining vars
# carry the image's ENV defaults).
exec /antidote/bin/antidote foreground \
    -riak_core handoff_port "${HANDOFF_PORT:-8099}" \
    -riak_core ring_creation_size "${RING_SIZE:-16}" \
    -antidote txn_cert "${ANTIDOTE_TXN_CERT:-true}" \
    -antidote txn_prot "${ANTIDOTE_TXN_PROT:-clocksi}" \
    -antidote recover_from_log "${ANTIDOTE_RECOVER_FROM_LOG:-true}" \
    -antidote recover_metadata_on_start "${ANTIDOTE_RECOVER_METADATA_ON_START}" \
    -antidote sync_log "${ANTIDOTE_SYNC_LOG:-false}" \
    -antidote enable_logging "${ANTIDOTE_ENABLE_LOGGING:-true}" \
    -antidote auto_start_read_servers "${ANTIDOTE_AUTO_START_READ_SERVERS:-true}" \
    -antidote logreader_port "${LOGREADER_PORT:-8085}" \
    -antidote pubsub_port "${PBSUB_PORT:-8086}" \
    -ranch pb_port "${PB_PORT:-8087}" \
    -antidote_stats metrics_port "${METRICS_PORT:-3001}" \
    -kernel logger_level "${DEBUG_LOGGER_LEVEL:-info}" \
    -kernel inet_dist_listen_min "${ERLANG_DIST_PORT_MIN}" \
    -kernel inet_dist_listen_max "${ERLANG_DIST_PORT_MAX}"
