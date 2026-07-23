# Cluster-mode coordination-signature datasets

Phased bandwidth measurements (idle 30s / write 60s / read 30s) of eight
self-replicating services under XDN cluster mode, captured by
`eval/measure_cluster_bw.py` from the per-service `bandwidth` section of the
replica-info endpoint (XdnBandwidthProfiler, tcp_info counters via the probe
sidecar). All runs: CloudLab Utah xl170 pod, 3 replicas on three physical
hosts (10.10.1.1-3), 2026-07-22/23. Render tables with
`eval/analyze_cluster_bw.py <file.json>`.

| file | system | measured wire signature |
|---|---|---|
| `bw-etcd2.json` | etcd (Raft) | leader star; follower-proxied writes; ReadIndex read bursts; 505 B/write |
| `bw-rqlite.json` | rqlite (Raft) | pure star, follower↔follower = 0; weak reads forwarded to leader; 820 B/write |
| `bw-mysql2.json` | MySQL Group Replication | full mesh: ~960 B/s idle heartbeat/pair, secondaries exchange certification under writes; reads free; 3,031 B/write |
| `bw-redis3.json` | Redis sub-replica chain | relay PATH: byte-identical store-and-forward per hop, no head↔tail edge; reads free |
| `bw-cass.json` | Cassandra (leaderless quorum) | flat gossip mesh; contact-point coordinator; QUORUM reads keep mesh elevated; 770 B/write |
| `bw-anti.json` | AntidoteDB (CRDT causal+) | symmetric idle mesh ~3.3 KB/s/pair; ONLY the writer's outbound edges swell (no acks back); reads free. Self-edges = in-container rpc driver, exclude |
| `bw-mongo2.json` | MongoDB replica set | idle heartbeat mesh ~1-1.6 KB/s/pair; write star atop it (oplog down, replSetUpdatePosition up), secondary↔secondary at baseline; majority reads free |
| `bw-corfu2.json` | CorfuDB (chain, client-driven) | replica↔replica frozen at ~180 B/s management noise under writes (client writes each log unit itself); reads served by the chain TAIL |

`superseded/` keeps earlier/aborted runs worth their diagnostics:
`bw-mongo.json` (write phase died: the primary had migrated off the
initiator), `bw-corfu.json` (zero ops: the readiness-gate heal restart wiped
the memory-mode layout after bootstrap), and first-iteration runs of etcd,
mysql, and redis (probe/classification fixes landed between them and the
canonical runs).
