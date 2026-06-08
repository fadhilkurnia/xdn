# Flexible consistency in XDN

!!! tip
    A developer picks the intended consistency model with a single flag
    (`--consistency=...`), and XDN maps it to a replication protocol that upholds
    that guarantee. Advanced users can also plug in a custom protocol.

XDN decouples the **consistency model** (the guarantee a service wants) from the
**replication protocol** (the mechanism that enforces it). You declare the
former; XDN selects the latter.

!!! note
    Informally, a **consistency model** constrains what values a read may
    legally observe. A **replication protocol** (a.k.a. coordination or
    synchronization protocol) is what keeps every replica's state in agreement
    so those illegal observations never happen.

## Predefined consistency models

XDN ships nine consistency models, selected with `--consistency=<model>` at
launch (case-insensitive). Each maps to a concrete coordinator under the hood:

| Model | Guarantee (informal) | Protocol / coordinator |
|-------|----------------------|------------------------|
| `LINEARIZABLE` (alias `LINEARIZABILITY`) | Strongest: every request appears to take effect atomically at one instant, in real-time order. | All requests ordered by **Paxos** consensus (`PaxosReplicaCoordinator`). |
| `SEQUENTIAL` | All replicas see writes in one common order (not necessarily real-time). | Writes ordered by **Paxos**, read-only requests served **locally** (`AwReplicaCoordinator`). |
| `CAUSAL` | Causally related operations are seen in order; concurrent ones may differ. | Vector-clock causal protocol (`CausalReplicaCoordinator`). |
| `PRAM` (FIFO) | Writes from a given client are seen by everyone in that client's issue order. | Per-client FIFO protocol (`PramReplicaCoordinator`). |
| `EVENTUAL` | Replicas converge once updates stop; weakest guarantee. | Lazy/asynchronous propagation (`LazyReplicaCoordinator`). |
| `READ_YOUR_WRITES` | A client always sees its own prior writes. | Client-centric, vector-timestamp session protocol (`BayouReplicaCoordinator`). |
| `MONOTONIC_READS` | A client never sees state go backwards across reads. | Client-centric (`BayouReplicaCoordinator`). |
| `MONOTONIC_WRITES` | A client's writes are applied everywhere in issue order. | Client-centric (`BayouReplicaCoordinator`). |
| `WRITES_FOLLOW_READS` | A write that depends on a read lands after the state it read. | Client-centric (`BayouReplicaCoordinator`). |

The four **client-centric / session** models (`READ_YOUR_WRITES`,
`MONOTONIC_READS`, `MONOTONIC_WRITES`, `WRITES_FOLLOW_READS`) are all served by
one coordinator (`BayouReplicaCoordinator`). It tags each client session with a
**vector timestamp**: the client returns its timestamp on each request, and the
replica synchronizes just enough to satisfy the session guarantee before
responding.

### Determinism comes first

The consistency model only applies to services declared **deterministic** (see
[service properties](service-properties.md#determinism)). A **non-deterministic**
service — one that can produce different results from the same input — is always
replicated with **primary-backup** (`PrimaryBackupReplicaCoordinator`): the
primary executes the request and ships the resulting on-disk
[state diff](how-xdn-work.md) to the backups, giving a single, agreed order
without requiring the replicas to recompute identically.

!!! warning "The two flags work together"
    `--consistency` is ignored unless the service is also `--deterministic`. For
    example `--consistency=linearizable --deterministic=true` runs over Paxos,
    while the same command without `--deterministic` falls back to primary-backup.

### Operation behaviors can refine the choice

Some models carry constraints, so XDN also looks at each request's declared
[behavior](service-properties.md#operation-behaviors) (read-only, write-only,
read-modify-write, …):

- `CAUSAL` and `PRAM` need a reconciliation-free workload; if the service has
  **read-modify-write** operations, XDN falls back to `SEQUENTIAL` (which orders
  every write through Paxos and is always safe).
- `EVENTUAL` uses the lazy coordinator only when operations are monotonic /
  read-only / write-only; otherwise it also falls back to `SEQUENTIAL`.

The default if nothing is declared is `SEQUENTIAL` server-side (the `xdn` CLI
sends `linearizability` by default).

## Custom replication protocol

Beyond the predefined models, XDN's coordinator is pluggable. A protocol is a
subclass of `AbstractReplicaCoordinator` (implementing the `ReplicaCoordinator`
interface) that provides:

- `coordinateRequest(...)` — how a request is ordered/replicated before it executes;
- `createReplicaGroup(...)` / `deleteReplicaGroup(...)` / `getReplicaGroup(...)` — replica-group lifecycle.

`XdnReplicaCoordinator` is itself a thin wrapper that routes each service to one
of these coordinators (Paxos, primary-backup, client-centric, causal, PRAM,
lazy, …) based on the declared properties — a new protocol slots in the same
way.

!!! note
    Building a correct distributed protocol is genuinely hard 😁 — prefer a
    predefined model unless you have a specific reason to roll your own.
