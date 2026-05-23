# xdn-etcd-cluster

Reference image for `xdn cluster launch`. It wraps upstream
[`quay.io/coreos/etcd`](https://quay.io/repository/coreos/etcd) with an entrypoint that
translates the `XDN_CLUSTER_*` contract injected by XDN into etcd's native `ETCD_*` flags
(`ETCD_INITIAL_CLUSTER`, `ETCD_INITIAL_ADVERTISE_PEER_URLS`, etc.). etcd itself runs Raft;
XDN only places the replicas, gives each a stable `replica-N` identity on a Swarm overlay,
and routes client traffic to a replica's local v3 HTTP gateway.

## Build

```bash
docker build -t xdn-etcd-cluster:test services/etcd-cluster/
```

## Launch (3-node cluster, single host)

```bash
docker swarm init                              # one-time, gives us overlay networking
./bin/gpServer.sh -DgigapaxosConfig=conf/gigapaxos.xdn.local.properties start all

export XDN_CONTROL_PLANE=localhost
xdn cluster launch etcd-demo \
    --image xdn-etcd-cluster:test \
    --port 2379 --peer-port 2380 \
    --state /etcd-data/ \
    --num-replicas 3 \
    --adapter etcd

# Write through replica 0, read back via replica 1
curl -s http://localhost:2300/v3/kv/put -H 'XDN: etcd-demo' \
     -d '{"key":"Zm9v","value":"YmFy"}'
curl -s http://localhost:2301/v3/kv/range -H 'XDN: etcd-demo' \
     -d '{"key":"Zm9v"}'
```

## Env contract

| XDN variable             | Forwarded to etcd as                       |
|--------------------------|---------------------------------------------|
| `XDN_CLUSTER_SELF`       | `--name`, peer/client URL host             |
| `XDN_CLUSTER_PEERS`      | `--initial-cluster` (joined to `<name>=http://<name>:<peer_port>`) |
| `XDN_CLUSTER_PEER_PORT`  | peer URL port (default 2380)               |
| `XDN_CLUSTER_PHASE`      | `--initial-cluster-state` (`new`/`existing`)|
