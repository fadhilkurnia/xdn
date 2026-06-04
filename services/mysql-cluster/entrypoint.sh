#!/usr/bin/env bash
# Maps the XDN_CLUSTER_* env contract (set by `xdn cluster launch`) onto MySQL 8
# Group Replication. XDN provides identity + overlay networking (stable replica-N
# hostnames); MySQL handles its own (single-primary) consensus inside the group.
#
# Required env (provided by XdnGigapaxosApp.initContainerizedClusterService):
#   XDN_CLUSTER_ORDINAL   this replica's 0-based ordinal  -> server_id, bootstrap-or-join
#   XDN_CLUSTER_SIZE      total cluster size              (informational)
#   XDN_CLUSTER_SELF      stable hostname (replica-N)     -> report_host, local_address
#   XDN_CLUSTER_PEERS     comma-separated replica names   -> group_seeds
#   XDN_CLUSTER_PEER_PORT GR group-communication port (default 33061; NOT the 3306 client port)
#   XDN_CLUSTER_PHASE     "bootstrap" on epoch 0, "join" when added later
#   MYSQL_ROOT_PASSWORD   root password (also what the WordPress sidecar connects with)
#
# Optional:
#   XDN_MYSQL_GROUP_UUID, XDN_MYSQL_REPL_USER, XDN_MYSQL_REPL_PASSWORD, XDN_MYSQL_DATABASE
#
# Why the dance below (RESET MASTER, recovery user with sql_log_bin=0): the stock mysql
# init creates system objects under gtid_mode=ON, so each freshly-initialized node has a
# different GTID_EXECUTED. Group Replication refuses to let a joiner in if it carries
# transactions the group never saw. Clearing GTIDs on every node right before START makes
# them all start from an empty, compatible history; the group's data (the app database) is
# then created once on the primary and replicated out.
set -euo pipefail

: "${XDN_CLUSTER_ORDINAL:?XDN_CLUSTER_ORDINAL is required}"
: "${XDN_CLUSTER_SELF:?XDN_CLUSTER_SELF is required}"
: "${XDN_CLUSTER_PEERS:?XDN_CLUSTER_PEERS is required}"
: "${MYSQL_ROOT_PASSWORD:?MYSQL_ROOT_PASSWORD is required}"

PEER_PORT="${XDN_CLUSTER_PEER_PORT:-33061}"
PHASE="${XDN_CLUSTER_PHASE:-bootstrap}"
GROUP_UUID="${XDN_MYSQL_GROUP_UUID:-aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee}"
REPL_USER="${XDN_MYSQL_REPL_USER:-xdnrepl}"
REPL_PW="${XDN_MYSQL_REPL_PASSWORD:-xdnreplpass}"
APP_DB="${XDN_MYSQL_DATABASE:-wordpress}"
SERVER_ID="$(( XDN_CLUSTER_ORDINAL + 1 ))"

# group_seeds = "replica-0:33061,replica-1:33061,..."
SEEDS=""
old_ifs="$IFS"; IFS=,
for name in $XDN_CLUSTER_PEERS; do
  entry="${name}:${PEER_PORT}"
  SEEDS="${SEEDS:+$SEEDS,}$entry"
done
IFS="$old_ifs"

cat > /etc/mysql/conf.d/zz-xdn-group-replication.cnf <<EOF
[mysqld]
server_id                         = ${SERVER_ID}
gtid_mode                         = ON
enforce_gtid_consistency          = ON
binlog_checksum                   = NONE
log_bin                           = binlog
relay_log                         = relay-bin
log_replica_updates               = ON
binlog_format                     = ROW
report_host                       = ${XDN_CLUSTER_SELF}
plugin_load_add                         = group_replication.so
# The "loose-" prefix is essential: during the stock image's "mysqld --initialize" phase the
# group_replication plugin is not loaded yet, so without it these would be fatal "unknown
# variable" errors and mysqld would crash-loop. With it, they are warnings until the plugin
# loads on real startup.
loose-group_replication_group_name      = ${GROUP_UUID}
loose-group_replication_local_address   = ${XDN_CLUSTER_SELF}:${PEER_PORT}
loose-group_replication_group_seeds     = ${SEEDS}
loose-group_replication_start_on_boot   = OFF
loose-group_replication_bootstrap_group = OFF
# Let distributed recovery fetch the donor's public key so caching_sha2_password auth works
# over the (non-TLS) recovery connection. This is a system variable — it canNOT be set via
# CHANGE REPLICATION SOURCE on the recovery channel (that fails with ER 3139).
loose-group_replication_recovery_get_public_key = ON
# keep memory modest: up to N of these run on one CI runner alongside the JVM
innodb_buffer_pool_size           = 128M
performance_schema                = ON
EOF

echo "[xdn-mysql] ${XDN_CLUSTER_SELF} server_id=${SERVER_ID} phase=${PHASE} seeds=${SEEDS}"

# Hand off to the stock entrypoint to initialize the datadir + root password, in the
# background. We deliberately do NOT set MYSQL_DATABASE — the app DB is created once on the
# primary after the group is up so it replicates with a clean GTID.
docker-entrypoint.sh mysqld &
MYSQLD_PID=$!

# All admin SQL goes over TCP so we never accidentally talk to the stock entrypoint's
# temporary (skip-networking) init server — only the real server listens on 3306.
mysql_tcp() { mysql --protocol=tcp -h127.0.0.1 -uroot -p"${MYSQL_ROOT_PASSWORD}" -N -B -e "$1"; }

echo "[xdn-mysql] waiting for mysqld to accept TCP connections ..."
ready=0
for _ in $(seq 1 150); do
  if mysqladmin --protocol=tcp -h127.0.0.1 -uroot -p"${MYSQL_ROOT_PASSWORD}" ping >/dev/null 2>&1; then
    ready=1; break
  fi
  if ! kill -0 "$MYSQLD_PID" 2>/dev/null; then
    echo "[xdn-mysql] mysqld exited during startup" >&2; wait "$MYSQLD_PID"; exit 1
  fi
  sleep 2
done
[ "$ready" = 1 ] || { echo "[xdn-mysql] timed out waiting for mysqld" >&2; exit 1; }

# Skip re-configuring an already-grouped node (e.g. container restart).
state="$(mysql_tcp "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_HOST='${XDN_CLUSTER_SELF}'" 2>/dev/null || true)"
if [ "$state" != "ONLINE" ] && [ "$state" != "RECOVERING" ]; then
  echo "[xdn-mysql] configuring group replication recovery user + channel"
  # Recovery user is local to each node (sql_log_bin=0 -> no GTID), so creating it can't
  # desync GTID histories. The caching_sha2 public-key fetch needed for non-TLS recovery is
  # enabled via the group_replication_recovery_get_public_key system variable (see config above).
  mysql_tcp "SET SESSION sql_log_bin=0;
             CREATE USER IF NOT EXISTS '${REPL_USER}'@'%' IDENTIFIED BY '${REPL_PW}';
             GRANT REPLICATION SLAVE, BACKUP_ADMIN, GROUP_REPLICATION_STREAM ON *.* TO '${REPL_USER}'@'%';
             FLUSH PRIVILEGES;
             SET SESSION sql_log_bin=1;"
  mysql_tcp "CHANGE REPLICATION SOURCE TO SOURCE_USER='${REPL_USER}', SOURCE_PASSWORD='${REPL_PW}' FOR CHANNEL 'group_replication_recovery';"

  # Clear the GTIDs the stock init generated so every node starts from a compatible history.
  mysql_tcp "RESET MASTER;"

  if [ "${XDN_CLUSTER_ORDINAL}" = "0" ] && [ "${PHASE}" = "bootstrap" ]; then
    echo "[xdn-mysql] bootstrapping the group"
    mysql_tcp "SET GLOBAL group_replication_bootstrap_group=ON;
               START GROUP_REPLICATION;
               SET GLOBAL group_replication_bootstrap_group=OFF;"
    # Create the app database once, on the primary, so it replicates to the joiners.
    mysql_tcp "CREATE DATABASE IF NOT EXISTS \`${APP_DB}\`;"
  else
    echo "[xdn-mysql] joining the group (retrying until the seed is reachable)"
    joined=0
    for _ in $(seq 1 60); do
      if mysql_tcp "START GROUP_REPLICATION;" 2>/dev/null; then joined=1; break; fi
      sleep 3
    done
    [ "$joined" = 1 ] || echo "[xdn-mysql] WARNING: START GROUP_REPLICATION never succeeded" >&2
  fi
fi

# Report final membership state (best effort; informational).
for _ in $(seq 1 60); do
  st="$(mysql_tcp "SELECT MEMBER_STATE FROM performance_schema.replication_group_members WHERE MEMBER_HOST='${XDN_CLUSTER_SELF}'" 2>/dev/null || true)"
  if [ "$st" = "ONLINE" ]; then echo "[xdn-mysql] ${XDN_CLUSTER_SELF} is ONLINE"; break; fi
  sleep 2
done

# Keep mysqld in the foreground so the container lives as long as the server does.
wait "$MYSQLD_PID"
