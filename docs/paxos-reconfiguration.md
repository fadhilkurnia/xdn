# Replica-group reconfiguration & state transfer

A service's *placement* — the set of ActiveReplicas (ARs) that host it — is not
fixed. XDN's demand policy ([Geo-distributed demand](geo-demand.md)) periodically
recomputes where the replicas should live, and when the chosen set changes the
control plane **reconfigures** the service: it stops the current replica group,
moves the service's state to the new replicas, and starts the new group — all
without losing linearizability or dropping the service.

This page walks through one reconfiguration end-to-end: the steps, the packets on
the wire, and (importantly) how the service's *state* is carried from the old
epoch to the new one.

## Epochs

Every placement of a service is stamped with a monotonically increasing **epoch**
number. A reconfiguration advances the service from epoch `e` (active set `Aₑ`) to
epoch `e+1` (active set `Aₑ₊₁`). Epochs make the move atomic and idempotent: the
old epoch is *stopped* (no more writes), its final state is *sealed*, and the new
epoch is *started* from that state. A replica that appears in both `Aₑ` and `Aₑ₊₁`
still tears down its epoch-`e` instance and re-creates a fresh epoch-`e+1` instance
from the transferred state — there is no in-place "stay" fast-path.

## Actors

| Actor | Role |
| --- | --- |
| **RC** (`cp0`) | The Reconfigurator. Receives demand, decides the new placement, and drives the three-phase protocol. |
| **Old actives** `Aₑ` | The ARs hosting the service in epoch `e`. They seal the final state and then drop the old epoch. |
| **New actives** `Aₑ₊₁` | The ARs that will host epoch `e+1`. They fetch the previous epoch's final state and start fresh containers from it. |

The RC orchestrates the move with three chained barrier tasks —
`WaitAckStopEpoch` → `WaitAckStartEpoch` → `WaitAckDropEpoch`. The actual **state
bytes travel out-of-band, AR→AR**, not through the RC.

## Sequence diagram

```
 Client traffic          Reconfigurator           Old actives Aₑ          New actives Aₑ₊₁
 (X-Client-Location)        (cp0)                   {a, b, c}                {a, b, d}
        │                     │                         │                        │
        │  HTTP request       │                         │                        │
        ├──────────────────────────────────────────────▶│ served; demand sampled │
        │                     │                         │ (per grid cell)        │
        │                     │   DemandReport(grid)     │                        │
        │                     │◀────────────────────────┤  (throttled: ≤1 / 10s) │
        │                     │                          │                        │
        │   policy: weighted demand centroid → 3 closest actives → Aₑ₊₁={a,b,d}   │
        │                     │                          │                        │
        │                     │ RCRecordRequest(INTENT)  │   [paxos-logged among RCs]
        │                     ├──┐                       │                        │
        │                     │◀─┘                       │                        │
 ═══════╪═════════════════════╪═══ ① STOP  (WaitAckStopEpoch) ═════════════════════╪═══════
        │                     │      StopEpoch(e)        │                        │
        │                     ├─────────────────────────▶│                        │
        │                     │           exec XdnStopRequest through paxos:      │
        │                     │           • captureFinalState(e)  → state.tar     │
        │                     │           • stop the container                    │
        │                     │      AckStopEpoch        │                        │
        │                     │◀─────────────────────────┤  (no state in this ack)│
 ═══════╪═════════════════════╪═══ ② START (WaitAckStartEpoch) ════════════════════╪═══════
        │                     │            StartEpoch(e+1)                         │
        │                     ├───────────────────────────────────────────────────▶│
        │                     │                          │ RequestEpochFinalState(e)│
        │                     │                          │◀───────────────────────┤
        │                     │                          │ EpochFinalState(e, TAR) │  ◀── state transfer
        │                     │                          ├────────────────────────▶│
        │                     │                          │   restore("xdn:final:…")│
        │                     │                          │   → revive container    │
        │                     │            AckStartEpoch │                        │
        │                     │◀──────────────────────────────────────────────────┤
 ═══════╪═════════════════════╪═══ ③ COMPLETE / DROP (WaitAckDropEpoch) ═══════════╪═══════
        │                     ├──┐ RCRecordRequest(COMPLETE) [paxos-logged]        │
        │                     │◀─┘                       │                        │
        │                     │   DropEpochFinalState(e)  │                        │
        │                     ├─────────────────────────▶│                        │
        │                     │   AckDropEpochFinalState  │                        │
        │                     │◀─────────────────────────┤  (epoch e garbage-coll.)│
        │                     │                          │                        │
        ▼  traffic now routed to Aₑ₊₁={a,b,d} at epoch e+1                          ▼
```

## Step-by-step

**Trigger.** Each AR samples client demand per request (the `X-Client-Location`
header or the `_xdnloc` query param — the CORS-simple form browsers use — else the
client IP via GeoIP) and reports an aggregated grid to the RC at
most once every 10 s (`XdnGeoDemandProfiler`). On each report the RC runs the
placement policy: it computes the demand-weighted **centroid** of all cells and
picks the `N` ARs closest to it (`N` = the current group size). If that set differs
from the current one, it reconfigures.

**0 — Intent.** The RC records a `RECONFIGURATION_INTENT` for the service in its own
(paxos-replicated) reconfigurator group, so the decision survives an RC failure,
then spawns the stop task.

**① STOP** (`WaitAckStopEpoch`)
- RC → old actives: **`StopEpoch(e)`**.
- Each old AR coordinates a **stop request** through the service's paxos group so
  every replica stops at the *same* request slot (safety). Executing that stop runs
  XDN's `XdnStopRequest`, which **captures the epoch's final state** (tars the
  container's bind-mounted state directory) and then stops the container.
- Old AR → RC: **`AckStopEpoch`**. For a normal reconfiguration this ack carries
  **no** state (it only would for merge/split operations).

**② START** (`WaitAckStartEpoch`)
- RC → new actives: **`StartEpoch(e+1)`**. This packet names the previous epoch's
  group but does **not** contain the state.
- Because a previous epoch exists, each new AR spawns `WaitEpochFinalState` and
  pulls the state itself: new AR → an old AR **`RequestEpochFinalState(e)`**.
- Old AR → new AR: **`EpochFinalState(e, <bytes>)`** — **this is the state
  transfer.** The payload is the tar produced at stop time.
- The new AR **restores** that state (creating a fresh epoch-`e+1` container from
  it) and registers the new paxos instance.
- New AR → RC: **`AckStartEpoch`**.

**③ COMPLETE / DROP** (`WaitAckDropEpoch`)
- RC records `RECONFIGURATION_COMPLETE` (paxos-logged) — the move is now durable.
- RC → old actives: **`DropEpochFinalState(e)`**; old ARs garbage-collect the old
  epoch and reply **`AckDropEpochFinalState`**.

Each phase is idempotent and retried until a threshold of acks arrives, so the
protocol tolerates message loss and AR/RC restarts mid-reconfiguration.

## Packet reference

| Packet | From → To | Purpose |
| --- | --- | --- |
| `DemandReport` | AR → RC | Aggregated geo-demand grid (drives the policy). |
| `RCRecordRequest(INTENT / COMPLETE)` | RC → RC group | Durably record the decision / completion. |
| `StopEpoch(e)` | RC → old ARs | Stop epoch `e`; seal its final state. |
| `AckStopEpoch` | old AR → RC | Epoch `e` stopped (no state for plain reconfig). |
| `StartEpoch(e+1)` | RC → new ARs | Start epoch `e+1` (no state inside). |
| `RequestEpochFinalState(e)` | new AR → old AR | Ask for epoch `e`'s sealed state. |
| `EpochFinalState(e, bytes)` | old AR → new AR | **The state transfer.** |
| `DropEpochFinalState(e)` | RC → old ARs | GC the old epoch. |
| `AckDropEpochFinalState` | old AR → RC | Old epoch dropped. |

## What "state" is, and how it moves

For an XDN service the state is the contents of the declared **state directory**
(e.g. `/app/data/`), which is bind-mounted from the host into the container. State
transfer is a **directory snapshot**, carried as a string:

```
xdn:final:<epoch>::<serviceProperty>::<base64 tar of the state dir>
```

- **Seal (old AR).** `XdnGigapaxosApp.getFinalState(name, e)` tars the epoch's host
  mount directory and returns the `xdn:final:…` string (large snapshots are passed
  by URL instead of inline).
- **Apply (new AR).** `XdnGigapaxosApp.restore(name, "xdn:final:…")` decodes the
  tar and unpacks it into the new epoch's mount directory, then starts the
  container — which therefore boots with the previous epoch's data intact.

## It does **not** use the `checkpoint()` / `restore()` round-trip

GigaPaxos's generic `Replicable` contract transfers state with a
**`checkpoint(name)` → state → `restore(name, state)`** round-trip: paxos snapshots
an app via `checkpoint()` and re-installs it elsewhere via `restore()`. **XDN's
reconfiguration deliberately bypasses `checkpoint()`.**

- `XdnGigapaxosApp.checkpoint()` is intentionally **not** the state-transfer path —
  it returns a placeholder string and never snapshots the container volume.
- Instead, the **`Reconfigurable.getFinalState()`** method (the `xdn:final:` tar
  above) produces the state, and `restore()` consumes *that* payload — not a
  `checkpoint()` output.

Why does the framework take `getFinalState()` rather than the paxos checkpoint?
When an old AR serves `RequestEpochFinalState`, `ActiveReplica.getFinalStateContainer()`
returns paxos's internal checkpoint **only if** the app's coordinator is a
`PaxosReplicaCoordinator`. XDN's coordinator is **`XdnReplicaCoordinator`** (it
*wraps* paxos but is not a `PaxosReplicaCoordinator`), so the framework falls
through to **`appCoordinator.getFinalState()` → `XdnGigapaxosApp.getFinalState()`**
— the tar snapshot. So:

```
reconfiguration state transfer  =  getFinalState()  →  restore("xdn:final:…")
paxos checkpoint (checkpoint())  =  used only for paxos log truncation / recovery
```

!!! note "Implication for crash recovery"
    Because the paxos-level `checkpoint()` is a placeholder, **single-node crash
    recovery** from a paxos checkpoint is a separate path from reconfiguration and
    is not equivalent to it. Reconfiguration (group move) relies entirely on the
    `getFinalState()`/`restore()` tar mechanism described above.

## Where this lives in the code

| Concern | Class / method |
| --- | --- |
| Demand → new placement | `XdnGeoDemandProfiler.reconfigure` / `getNewActivesPlacement` |
| Three-phase orchestration | `Reconfigurator` + `WaitAckStopEpoch` → `WaitAckStartEpoch` → `WaitAckDropEpoch` |
| Stop + seal final state | `ActiveReplica.handleStopEpoch`; `XdnGigapaxosApp` `XdnStopRequest` → `captureContainerizedServiceFinalState` |
| Serve final state | `ActiveReplica.handleRequestEpochFinalState` → `getFinalStateContainer` → `XdnGigapaxosApp.getFinalState` |
| Fetch + apply on new AR | `ActiveReplica.handleStartEpoch` → `WaitEpochFinalState` → `XdnGigapaxosApp.restore` (`reviveContainerizedService`) |
