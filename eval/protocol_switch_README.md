# Dynamic protocol switch demo (Paxos -> primary-backup)

Prototype on branch `protocol-switch`. Switches a running deterministic
service from active replication (Paxos) to primary-backup at runtime via an
in-place placement update, to show lower latency under a large-request /
small-statediff workload (e.g. bookcatalog padded POSTs whose extra JSON
fields the app parses and drops).

## Mechanism (implemented)

- `ServiceProperty` carries an optional `replication` field
  (`active` | `primary-backup`); `XdnReplicaCoordinator.inferCoordinatorByProperties`
  honors it before the determinism/consistency inference.
- The RC placement endpoint accepts `"REPLICATION": "primary-backup"` in the
  `PUT /api/v2/services/{name}/placement` body; it rides the placement
  metadata (`REPLICATION_MODE` key) into the next epoch, where the AR stamps
  it on the property so it is sticky (round-trips through every future epoch
  final state, like the cluster ordinal map).
- The epoch's container state tar is protocol-agnostic, so a Paxos-epoch
  checkpoint restores cleanly into a primary-backup epoch. `XdnGigapaxosApp`
  now treats the PB nondeter-create/revive prefixes (not the determinism
  flag) as the signal to initialize the statediff recorder, so a switched
  deterministic service gets recorder setup and does not wipe its restored
  state.

## Trigger the switch

    curl -X PUT http://<rc>:3300/api/v2/services/bookcatalog/placement \
      -H 'Content-Type: application/json' \
      -d '{"NODES":["AR1","AR2","AR3"],"COORDINATOR":"AR1","REPLICATION":"primary-backup"}'

The COORDINATOR becomes the PB primary. Primary election converges in a few
seconds; requests during that window may fail (part of the story).

## Requirements for a fair latency measurement

- **Linux + FUSELOG/FUSERUST recorder** (`XDN_PB_STATEDIFF_RECORDER_TYPE=FUSELOG`).
  RSYNC spawns a subprocess per capture (~250ms floor on macOS loopback) that
  swamps the request-vs-diff saving; the switch mechanism is correct there but
  the latency win is invisible. Verified: a 200KB padded POST yields a
  106-byte rsync statediff (~2000:1), so the size asymmetry is real; only the
  capture cost is the blocker.
- **`RECONFIGURE_IN_PLACE=true`** (already set in `conf/gigapaxos.cloudlab.properties`).
  Without it a same-active-set placement update no-ops. Locally the switch
  fired only because the COORDINATOR change forced reconfiguration.
- Tune the primary's capture accumulation low: `-DPB_CAPTURE_ACCUMULATION_US=500`.

## Run

    # cluster up under Paxos, bookcatalog launched deterministic=true, then:
    python3 eval/protocol_switch_demo.py \
        --frontends 10.10.1.1:2300,10.10.1.2:2300,10.10.1.3:2300 \
        --rc 10.10.1.1:3300 --service bookcatalog \
        --nodes AR1,AR2,AR3 --primary AR1 \
        --pad-bytes 200000 --duration 90 --switch-at 45 \
        --rate 40 --workers 16 --out /tmp/psw-demo.json
    python3 eval/plot_protocol_switch.py -o psw.png /tmp/psw-demo.json

The harness is open-loop (worker pool) so the election window does not freeze
the timeline. The plot shows median latency over time with the switch marked.
Route all traffic to the primary's frontend with `--entry` (under PB only the
primary serves; hitting a backup frontend hangs).

## Result (Clemson r6525, fuselog, 2026-08-04)

`eval/datasets/protocol-switch/bookcatalog-1mb-bw100mbit.json`: median latency
**234ms (Paxos) -> 30ms (primary-backup)**, ~8x, after an in-place switch at
t=60s, 1MB incompressible requests, 3 replicas, inter-replica egress shaped to
100mbit. The Paxos-only baseline
(`bookcatalog-1mb-paxos-baseline.json`, 180s, no switch) is flat at ~230ms,
confirming this is a genuine steady-state protocol difference, not a warmup
artifact. The switch is timestamped at INITIATION; a ~15s primary-election
gap follows (no successful requests) before PB serves at ~30ms.

TWO METHODOLOGY BUGS were found and fixed to get an honest result (both
initially FAKED a win):
- **Compressible padding.** A `"x"*N` blob compresses to ~nothing, so the
  "1MB request" was ~1KB on the wire and never exercised the bandwidth
  mechanism (Paxos measured ~28ms == PB, the bandwidth shaping did nothing).
  The pad is now base64(os.urandom) -- incompressible -- so 1MB is really 1MB
  on the wire and Paxos pays ~230ms to replicate it. THIS is what makes the
  win real.
- **Warmup ramp masqueraded as the switch.** With the compressible pad,
  Paxos latency declined 52->28ms over ~90s (JIT/TCP), and a 30s warmup ended
  mid-ramp so the switch at t=60 coincided with the tail of the decline and
  looked causal. A Paxos-only baseline exposed this: Paxos alone reaches PB's
  latency with no switch at all. Fixed by incompressible padding (removes the
  ramp -- the real 230ms bandwidth cost dwarfs JIT/TCP noise) plus verifying
  against the flat baseline.

Three findings that shape the honest framing:

- **The latency win requires inter-replica BANDWIDTH to be the bottleneck.**
  On the bare Clemson LAN both protocols sit at ~10ms because replicating even
  1MB is nearly free. The result above shapes the ARs' egress with `tc tbf
  rate 100mbit` (pure token bucket, NO injected delay) so replicating the 1MB
  request to each backup costs real time while PB's tiny statediff does not.
  Only the inter-replica REPLICATION leg is shaped; client ingress (via the
  driver, unshaped) is fast and equal for both protocols. A latency-bound
  config (adding netem delay) makes Paxos ~= PB because both pay similar RTT
  rounds -- the request-vs-diff SIZE difference only shows when bandwidth is
  the constraint. tbf `latency 5000ms` (queue depth) is required so large
  flows queue rather than tail-drop (a small value collapses throughput via
  TCP retransmits -- an early 1MB run got 0 ok).
- **The bandwidth number is a stand-in for the ratio.** The win magnitude is
  (request - statediff) x (replicas-1) / bandwidth; 1MB/100mbit gives Paxos
  ~230ms of replication cost that PB avoids. 100mbit is a realistic per-vNIC /
  edge-uplink cap and 1MB a realistic document/media request; the same win
  follows from any combination with the same ratio. The request must be
  INCOMPRESSIBLE or the ratio is meaningless (see methodology bugs above).
- **Switch under LOW load only.** Hammering the service during the epoch
  transition wedges the primary election (in-flight requests never drain, no
  primary elected, no container); low load (a few req/s) transitions cleanly.
  Fine for latency measurement (low concurrency = cleaner per-request latency
  anyway) but switch-under-load robustness is a real follow-up (the
  under-tested PB epoch path).

## Workload realism

The 200KB padded POST (a `blob` field the app parses and drops) is a
stand-in that dials the request:statediff ratio; the mechanism is identical
to real large-request/small-diff patterns: a large document persisted as a
server-side projection (WordPress editPost: full HTML body >> DB delta;
image upload -> stored thumbnail), periodic full-catalog sync at low change
rate (ONIX feed ingestion: large batch of real records, few actually
changed), and idempotent re-writes / duplicate submissions (zero diff). A
paper-grade bookcatalog workload would batch-upsert records carrying real
`description` fields at a low change rate (archetype 2) so nothing is padded.
