# Client Centric Coordination Protocols

Assumption:
- The applications are okay with partial order.

Sources:
- Session Guarantees for Weakly Consistent Replicated Data
- Distributed Systems Textbook, Ch. 7 - Consistency and Replication.

System model:
- TODO


## Protocol for Monotonic Reads
**Informal Definition**:
Successive reads reflect a non-decreasing set of writes (i.e., reads cannot go backward).

**Definition**:
If a client reads the value of a data item `x`, any successive read operation on `x` by that client
will always return that same value or a more recent value.

Event-action protocol:
```
When receiving a WriteOnlyRequest W from client:
   increase our component in the current timestamp
   directly execute W locally
   send WriteAfterPacket to all replicas
   send the current timestamp as write-timestamp

When receiving a ReadOnlyRequest R from client:
    if R.timestamp <= myTimestamp:
        R.timestamp <- myTimestamp
        execute R
        response.read-timestamp <- the current timestamp
        send response to client
    else
        sync with our peers
        execute R
        response.read-timestamp <- the current timestamp
        send response to client
    
When receiving a SyncRequestPacket X:
    send SyncResponsePacket([request ....])

When receiving a SyncResponsePacket Y:
    for (Request R : Y.requests)
        if isAlreadyExecuted(R):
            no-op
        else:
            increase our component in the current timestamp
            execute R

When receiving a WriteAfterPacket Z from Server S:
    if Z.timestamp[S] <= myTimestamp[S]:
        no-op
    else if Z.timestamp[S] == myTimestamp[S]+1:
        execute Z.ops
        myTimestamp[S] <- Z.timestamp[S]
    else:
        send SyncRequestPacket to S
```

## Protocol for Monotonic Writes
**Informal Definition**:
Writes are propagated after writes that logically precede them.

**Definition**:
A write operation by a client on a data item `x` is completed before any successive write operation
on `x` by the same client.

Event-action protocol:
```
Sync before executing write locally
Write return the latest timestamp
```

## Protocol for Read Your Writes
**Informal Definition**:
Read operations reflect previous writes.

**Definition**:
The effect of a write operation by a client on data item `x` will always be seen by a successive 
read operation on `x` by the same client.

```
Sync before read
Write return the latest timestamp
```

## Protocol for Writes Follow Reads
**Informal Definition**:
Writes are propagated after reads on which they depend.

**Definition**:
A write operation by a process on a data item `x` following a previous read operation `x` by 
the same client is guaranteed to take place on the same or a more recent value of `x` that was read.

```
Sync before write
Read return the latest timestamp
```