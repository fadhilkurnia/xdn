# xdn-rqlite-cluster

Reference cluster image for `xdn cluster launch`. Each replica runs a single
[rqlite](https://github.com/rqlite/rqlite) node (SQLite + Raft); the entrypoint translates
the `XDN_CLUSTER_*` contract injected by XDN into rqlite's native CLI flags
(`-node-id`, `-bootstrap-expect`, `-join`, `-raft-adv-addr`, ...). XDN provides identity
and overlay networking; rqlite handles its own Raft replication.

A SQL-speaking app like `bookcatalog` would treat the cluster as a single logical database
endpoint via XDN's HTTP frontend on the cluster's HTTP port (default 4001).

## Build
```bash
docker build -t xdn-rqlite-cluster:test services/rqlite-cluster/
```

## Launch (3 replicas)
```bash
docker swarm init                                # one-time, gives overlay nets
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.xdn.local.properties start all

export XDN_CONTROL_PLANE=localhost
xdn cluster launch bookcat-db \
    --image xdn-rqlite-cluster:test \
    --port 4001 --peer-port 4002 \
    --state /rqlite-data/ \
    --num-replicas 3
```

## Try it
```bash
# CREATE + INSERT through replica 0
curl -s http://localhost:2300/db/execute -H 'XDN: bookcat-db' \
     -d '["CREATE TABLE books (id INTEGER PRIMARY KEY, title TEXT)"]'
curl -s http://localhost:2300/db/execute -H 'XDN: bookcat-db' \
     -d '["INSERT INTO books(title) VALUES (\"Dune\")"]'

# SELECT through replica 2 — value must round-trip via Raft
curl -s http://localhost:2302/db/query?level=strong -H 'XDN: bookcat-db' \
     -d '["SELECT * FROM books"]'
```

## Env contract

| XDN variable             | Forwarded to rqlited as                              |
|--------------------------|-------------------------------------------------------|
| `XDN_CLUSTER_SELF`       | `-node-id`, `-http-adv-addr`, `-raft-adv-addr` host  |
| `XDN_CLUSTER_PEERS`      | `-join http://<name>:4001,...`                       |
| `XDN_CLUSTER_PEER_PORT`  | `-raft-addr` / `-raft-adv-addr` port (default 4002)  |
| `XDN_CLUSTER_SIZE`       | `-bootstrap-expect <N>` (bootstrap only)             |
| `XDN_CLUSTER_PHASE`      | bootstrap ⇒ pass `-bootstrap-expect`; join ⇒ omit it |
