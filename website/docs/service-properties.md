# Declaring service properties for XDN

!!! tip "Declarative replication"
    XDN embraces **declarative** replication: you declare *what* the service is
    and what guarantee you want — not *how* the replication happens. XDN uses
    those declarations to pick and run the right replication protocol.

You declare properties at launch with `xdn launch` flags or a `--file` YAML/JSON
spec (the same fields are also accepted by the control-plane HTTP API when
creating a service). For example:

```bash
xdn launch bookcatalog \
   --image=fadhilkurnia/xdn-bookcatalog \
   --port=80 \
   --consistency=linearizable \
   --deterministic=true \
   --state=/app/data/
```

## The properties you declare

| Property | Flag / field | Meaning |
|----------|--------------|---------|
| Name | `<name>` | Service name; becomes its DNS label (`name.<domain>`). |
| Image | `--image` / `image` (or `components` for multi-container) | The Docker image(s) of your blackbox service. |
| Port | `--port` / `port` | Port the container listens on for HTTP (default `80`). |
| State directory | `--state` / `state` | Absolute path (ending `/`) where the service persists state on disk. |
| Determinism | `--deterministic` / `deterministic` | Whether identical inputs yield identical results (default `false`). |
| Consistency | `--consistency` / `consistency` | The [consistency model](flexible-consistency.md) (CLI default `linearizability`). |
| Operation behaviors | `requests` matchers | Per-request classification (read-only, write-only, …); defaults inferred from HTTP method. |
| Replication factor | `--num-replicas`, `--min-replicas`, `--max-replicas` | Size constraints for the replica group. |
| Environment | `--env KEY=VALUE` / `environments` | Environment variables passed to the container. |

The three properties that most affect *how* a service is replicated are
**determinism**, **state location**, and **operation behaviors** (together with
the [consistency model](flexible-consistency.md)).

## Determinism

`--deterministic` (default `false`) is the master switch for protocol selection:

- **Deterministic** services can be replicated by re-executing the same ordered
  request stream on every replica, so XDN can use the protocol that matches your
  chosen [consistency model](flexible-consistency.md) (Paxos, client-centric,
  causal, …).
- **Non-deterministic** services are always replicated with **primary-backup**:
  one replica executes, and XDN ships the resulting on-disk state change to the
  others. This is the only coordinator that tolerates non-determinism, so the
  `--consistency` flag has no effect on a non-deterministic service.

## State on disk

`--state` declares the absolute directory where your service writes its
persistent state (e.g. `/app/data/`, or `database:/var/lib/mysql/` for a named
component in a multi-container service). XDN treats the service as a blackbox and
**captures changes to that directory transparently** — by default via a FUSE
filesystem mounted under the state path — so it can replicate writes and move a
replica's state to a new location without the service's cooperation. See the
[architecture page](how-xdn-work.md) for how the state diff is recorded and
applied.

## Operation behaviors

XDN classifies each incoming request by **behavior**, which lets it serve reads
cheaply and reason about which consistency protocols are safe. By default,
behaviors are inferred from the HTTP method:

| HTTP method | Default behavior |
|-------------|------------------|
| `GET`, `HEAD`, `OPTIONS`, `TRACE` | read-only |
| `PUT`, `DELETE` | write-only |
| `POST`, `PATCH` | read-modify-write |

You can override these by declaring `requests` matchers (path prefix + methods +
behavior). The behaviors XDN understands are `READ_ONLY`, `WRITE_ONLY`,
`READ_MODIFY_WRITE`, plus the optional hints below.

### Read-only vs. write operations

Separating **read-only** from write operations is what lets `SEQUENTIAL` (and
several other models) serve reads from the local replica while ordering writes
through consensus. A request marked read-only must not modify state.

### Nil-external operations

A **nil-external** operation produces no externally visible effect beyond its own
response (no side effects other observers depend on). XDN treats nil-external
requests as compatible with the weaker, reconciliation-free protocols (causal,
PRAM), so declaring them keeps those models eligible instead of forcing a
fallback to sequential.

### Monotonic operations

A **monotonic** operation only moves the state "forward" (e.g. set unions,
counters that only grow), so replicas converge without conflict resolution.
Declaring operations monotonic is what makes the `EVENTUAL` (lazy) protocol
eligible; otherwise XDN falls back to sequential consistency.

### Commutative operations

A **commutative** operation can be reordered with others without changing the
result. The property is declarable (`COMMUTATIVE`, and a per-key variant
`KEY_COMMUTATIVE`) and is intended to unlock order-relaxed coordination.

!!! note
    Determinism, state location, read/write classification, nil-external and
    monotonic hints are used today to select and constrain the replication
    protocol. The commutativity hints are declarable but not yet exploited for
    coordination — they are reserved for future optimizations.
