# Cluster-mode coordination-signature datasets

Phased bandwidth measurements (idle 30s / write 60s / read 30s) of eight
self-replicating services under XDN cluster mode, captured by
`eval/measure_cluster_bw.py` from the per-service `bandwidth` section of the
replica-info endpoint (XdnBandwidthProfiler, tcp_info counters via the probe
sidecar). All runs: CloudLab Utah xl170, 3 replicas on three physical hosts
(10.10.1.1-3). Render tables with `eval/analyze_cluster_bw.py <file.json>`
(per-write coordination cost is reported net of the idle-phase baseline).

## `proxied/` — canonical (2026-07-24)

ALL load enters through XDN's HTTP proxy (`:2300`, `XDN:` header): etcd and
rqlite natively, the other six via the dumb HTTP KV shims
(`services/*-http/`, pod specs `services/*-http-cluster.yaml`). This is the
vantage point coordination inference requires — the proxy observes request
boundaries, so coordinated vs uncoordinated demand is attributable
per-request, and client demand (D^u) appears as `client` edges at exactly
the AR that served it. The measurement client probes which frontend accepts
writes (redis: the chain head only; mongo: the current primary only — the
same discovery native clients of those systems perform).

| file | system | measured wire signature (write phase) |
|---|---|---|
| `bw2-etcd.json` | etcd (Raft) | leader star; driven follower proxies every client write to the leader; 503 B/write |
| `bw2-rqlite.json` | rqlite (Raft) | pure star, follower↔follower = 0; 703 B/write |
| `bw2-mysql.json` | MySQL GR, MULTI-PRIMARY | writer broadcasts (XCom), non-writers exchange certification with each other (the f↔f mesh Raft lacks); reads free; 3,866 B/write over a 4.6 KB/s idle mesh |
| `bw2-redis.json` | Redis sub-replica chain | sequential relay PATH: head→mid ≈ mid→tail byte-identical, no head↔tail edge; 575 B/write |
| `bw2-corfu.json` | CorfuDB (client-driven chain) | embedded client fans out IN PARALLEL from the entry replica to all log units; peer↔peer stays at management noise; reads hit the chain TAIL; 511 B/write |
| `bw2-cass.json` | Cassandra (leaderless quorum) | local member coordinates each request, flat fan-out; QUORUM reads also cost coordination; 476 B/write over a 2.2 KB/s gossip mesh |
| `bw2-mongo.json` | MongoDB replica set | write star atop an all-pairs heartbeat mesh (oplog down, replSetUpdatePosition up, secondary↔secondary at baseline); majority reads local; 5,463 B/write |
| `bw2-anti.json` | AntidoteDB (CRDT causal+) | writes commit locally, ONLY the writer's outbound edges swell (no acks back); reads free; 3,585 B/write over an 18.7 KB/s idle mesh (heaviest idle of all) |

The redis vs corfu pair is the protocol-name-vs-wire-shape exhibit: both are
"chain replication", one relays sequentially through the members, the other
fans out from wherever the (embedded) client sits.

## `scaling/n5/`, `scaling/n7/` — cluster-size sweep (2026-07-27/30)

The proxied methodology at N=5 and N=7 (with `proxied/` as the N=3 points):
8 protocols x 3 sizes on CloudLab Mass rs620s, each replica on its own host
(7 ARs at N=7, RC co-located with AR1, driver outside the fleet). Same specs
launched at every size via `xdn cluster launch <name> -f <spec> -n <N>`.
Render growth curves with `eval/plot_scaling.py`. Headline
(baseline-corrected B/write at N=3 -> 5 -> 7): stars and chains grow O(N)
(etcd 503/1,087/1,571 at ~260 B per follower; redis 575/1,140/1,618 at
~280 B per hop; corfu 511/1,091/1,563; rqlite 703/1,458/2,048; cassandra
476/1,345/1,632 with RF=N), while certification/CRDT meshes grow
superlinearly (mysql-gr 3,866/10,138/15,792; mongo 5,463/10,818/18,285;
antidote 3,585/8,610/13,624) and their standing idle meshes track the
directed-pair count (mysql-gr 4.6 -> 14.9 -> 58.4 KB/s). Formation lessons
encoded in the service images along the way: cassandra needs deterministic
per-ordinal tokens (random single tokens collide at larger N), a private
conf tree (sticky-dir renames EPERM under a non-root user on some hosts),
in-container supervision (a crash-restarted member strands netns sidecars),
and no bootstrap streaming on fresh rings (simultaneous joiners race and
wedge).

## `native/` — first round (2026-07-22/23, older pod)

Same protocols driven through their native interfaces from mixed vantage
points (CLI clients at published ports, docker-exec loops, a non-member
overlay container). Peer edges (D^s) are directly comparable with
`proxied/`; client edges are NOT (vantage-dependent, and per-request
connections were undercounted by the poller before the forwarder keep-alive
fix). Kept for the cross-vantage comparison and because corfu's signature
differs structurally: with the client OUTSIDE the pods, its fan-out appears
as client edges on every member instead of inter-replica edges. MySQL here
is single-primary GR (the proxied round runs multi-primary for the
dumb-shim contract).

## `superseded/`

Earlier/aborted runs kept for their diagnostics: `bw-mongo.json` (write
phase died: the primary had migrated off the initiator), `bw-corfu.json`
(zero ops: the readiness-gate heal restart wiped the memory-mode layout),
and first-iteration etcd/mysql/redis runs from before probe and
classification fixes.
