# Quorum-size inference datasets

Latency-injection probes (`eval/infer_quorum_size.py`) that measure `q`, the
number of replicas on the critical path of a write (the leader/coordinator plus
the peers it waits for), for cluster-mode services. The bandwidth graph gives
the coordination shape and identifies the coordinator L beforehand; this probe
adds the timing dimension bytes cannot see — a waited-for and an unwaited-for
message look identical in a byte counter, but delaying the waited-for one shifts
client write latency. See `papers/xdn-paper/quorum-size-inference.md`.

Method: inject one-way `tc netem` delay (delta_ms) on `m` leader-outbound
overlay links via the member's bwprobe sidecar netns; client write latency
shifts iff `m > N - q`. Binary search over m finds the flip point
`m* = N - q + 1`, so `q = N - m* + 1`.

## `casskv-n3.json` — Cassandra QUORUM, N=3 (CloudLab Utah c6525, 2026-08-20)

First end-to-end validation run. Cassandra behind the uniform HTTP KV shim
(`services/cassandra-http`, local member pinned as coordinator), RF=3, QUORUM
reads/writes. delta=50ms, 120 writes/round after 20 warmup.

Result: **q = 2** in 3 rounds — coordinator + 1 peer.
- baseline (m=0): 4.71 ms
- boundary (m=2, both peers delayed): 55.06 ms -> SHIFTED (the +50ms positive
  control: the coordinator was waiting on a peer)
- bisect (m=1, one peer delayed): 4.51 ms -> not shifted (the fastest 1 of 2
  peers still acked; interchangeable-peer model holds)
- flip point m*=2 -> q = 3 - 2 + 1 = 2

Matches the QUORUM-at-RF-3 expectation exactly: a write commits once the
coordinator plus one peer acknowledge, so the critical set is 2 of the 3
replicas. Two prototype gaps found and worked around this run: the probe image
(`services/bw-probe`, alpine+iproute2-ss) ships no `tc` (installed at runtime),
and the probe-discovery grep assumed a `bwprobe.<service>` prefix rather than
the real `bwprobe.<clusterContainerName>` naming (fixed in the script).
