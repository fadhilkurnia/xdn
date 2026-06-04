# xdn-mysql-cluster

A self-clustering MySQL image for XDN **cluster-mode** services. It wraps `mysql:8.0` and
translates the `XDN_CLUSTER_*` environment contract (set by `xdn cluster launch`) into a
single-primary **MySQL Group Replication** group.

Unlike etcd/rqlite, MySQL has no one-flag clustering, so the entrypoint does the assembly:

| `XDN_CLUSTER_*` | MySQL Group Replication |
|---|---|
| `ORDINAL` | `server_id = ORDINAL + 1`; ordinal 0 bootstraps the group |
| `SELF` (`replica-N`) | `report_host`, `group_replication_local_address` |
| `PEERS` | `group_replication_group_seeds` (`replica-0:33061,...`) |
| `PEER_PORT` (default 33061) | GR group-communication port (distinct from the 3306 client port) |
| `PHASE` | `bootstrap` → ordinal 0 bootstraps; otherwise members join an existing group |

## Bootstrap sequence (per node)

1. Write a GR config (`gtid_mode=ON`, `binlog`, `plugin_load_add=group_replication.so`,
   group name/seeds/local-address).
2. Start mysqld via the stock entrypoint (datadir init + `MYSQL_ROOT_PASSWORD`).
3. Create a node-local recovery user (`sql_log_bin=0`, so it generates no GTID) and point
   the `group_replication_recovery` channel at it with `GET_SOURCE_PUBLIC_KEY=1`.
4. `RESET MASTER` to drop the GTIDs the stock init produced — otherwise a joiner carries
   transactions the group never saw and GR rejects it.
5. Ordinal 0 (`phase=bootstrap`): bootstrap the group, then `CREATE DATABASE wordpress`
   once so it replicates out. Other ordinals: `START GROUP_REPLICATION`, retrying until the
   seed is reachable.

## Notes

- **Single-primary**: only the primary (ordinal 0 at bootstrap) accepts writes; secondaries
  are `super_read_only`. A client connecting to a secondary can read and connect, but not
  write — fine for a connectivity check, not for accepting new writes.
- `innodb_buffer_pool_size=128M` keeps memory modest so several members plus an app tier fit
  on one CI runner.
- Used by `XdnWordPressClusterTest` (multi-component cluster: this MySQL as the stateful
  member + a WordPress entry sidecar sharing its network namespace).

```bash
docker build -t xdn-mysql-cluster:test services/mysql-cluster/
```
