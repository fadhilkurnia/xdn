# Flexible consistency in XDN

!!! tip
    Developers can specify the intended consistency model, or even implement a custom
    replication protocol with the provided API.

XDN replicates a service under the consistency model the developer declares.
Each model below links to its [formal definition](consistency-definitions.md),
including the original definitions from the literature.

## Predefined consistency models

### Linearizable
Every request appears to take effect instantaneously, in one global order
consistent with real time: a read always reflects every previously
acknowledged write. The strongest model XDN offers, and the default.
[Formal definition.](consistency-definitions.md#linearizability)

### Sequential
All replicas apply writes in one agreed global order, but reads may observe a
stale prefix of it: linearizability without the real-time requirement.
[Formal definition.](consistency-definitions.md#sequential-consistency)

### Causal
Writes that may have influenced one another are applied in that order at every
replica; concurrent writes may be applied in different orders.
[Formal definition.](consistency-definitions.md#causal-consistency)

### PRAM
Each replica applies any single client's writes in the order they were issued;
writes from different sources may interleave differently at different replicas.
[Formal definition.](consistency-definitions.md#pram-fifo-consistency)

### Eventual
Replicas may serve any applied state; once writes stop arriving, all replicas
converge to the same state.
[Formal definition.](consistency-definitions.md#eventual-consistency)

### Read your writes
A session always observes its own earlier writes.
[Formal definition.](consistency-definitions.md#session-guarantees-client-centric)

### Writes follow reads
A session's write is ordered, everywhere, after the writes its earlier reads
observed.
[Formal definition.](consistency-definitions.md#session-guarantees-client-centric)

### Monotonic reads
A session never observes a state older than one it has already observed.
[Formal definition.](consistency-definitions.md#session-guarantees-client-centric)

### Monotonic writes
A session's writes are applied everywhere in the order the session issued them.
[Formal definition.](consistency-definitions.md#session-guarantees-client-centric)

## Custom replication protocol

!!! note
    Informally, a **consistency model** provides guarantees on what are valid values
    observable from the read requests.
    A **replication protocol** is the one that ensures the guarantee is satisfied and
    never broken by preventing invalid observations (i.e., upholding the safety
    property).

TBD

!!! note
    Replication protocol is commonly also referred to as a Coordination or
    Synchronization protocol. It is a class of Distributed Protocol that manages the
    state in all the replicas.
