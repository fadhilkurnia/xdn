# Causal Coordination Protocol

## System model
- We have an application that can accept read and write operations.
- The application is replicated into `N` replicas.
- Each replica stores its local directly-acyclic graph (DAG).
- Each vertex in the DAG contains (batch of) request.
- Each vertex in the DAG is uniquely identified by a vector timestamp.
- The used vector timestamps have `N` components, each containing a counter of
  updates happening in a node replica represented by that component.
  Example of vector timestamp: `<AR0:0, AR1:10, AR2:0>`.
- In the DAG, there is a specific root vertex with zero vector timestamps 
  (all components having counter value of zero). 

## Event-action protocol

```
When server i is receiving Write request W:
   leaves <- DAG.getLeafVertices()
   deps <- [leaf.timestamp for all leaf in leaves]
   ts <- max(dep.timestamp for all dep in deps)
   ts[i] <- ts[i] + 1
   execute W
   vertex <- new Vertex with ts as the ID, deps as the parent, and containing request W.
   send WriteForward{deps, ts, W} to all replicas
   wait for majority acknowledgements
   send response back to the client.
   
When receiving WriteForward{deps, ts, W} from S:
   wait until all of deps exist in our DAG
   execute W
   vertex <- new Vertex with ts as the ID, deps as the parent, and containing request W.
   send WriteAck{ts} to S

When receiving Read request R:
   execute R
   send response back to the client.

```

