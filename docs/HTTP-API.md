# Control-plane HTTP API

The XDN control plane (the Reconfigurator) exposes a small JSON HTTP API for
deploying, inspecting, and reconfiguring services. The dashboard and the `xdn`
CLI are both thin clients of this API; you can also call it directly.

- **Base URL** — `http(s)://<control-plane-host>:<port>`. The CLI talks to the
  clear-HTTP port (`:3300`); the browser dashboard uses the TLS port (`:3400`).
- **Format** — requests and responses are JSON; any origin is allowed (CORS).
- All endpoints live under the `/api/v2` prefix.

## Service lifecycle

| Method | Path | Purpose | Success |
| --- | --- | --- | --- |
| `POST` | `/api/v2/services/{name}` | Create (launch) a service. Body: `{"initial_state": "xdn:init:…"}` (the encoded service config). | `201` |
| `GET` | `/api/v2/services/{name}` | Get a service (its active replicas). | `200` |
| `DELETE` | `/api/v2/services/{name}` | Destroy a service. | `200` |

## Placement, coordinator & demand

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v2/services/{name}/placement` | Current placement: per-replica id, role, address, geolocation. |
| `PUT` | `/api/v2/services/{name}/placement` | Set the replica set (and optionally coordinator). Body: `{"NODES":[…],"COORDINATOR":"…"}`. |
| `PUT` | `/api/v2/services/{name}/coordinator` | Change the coordinator/leader. Body: `{"newCoordinatorNodeId":"…"}`. |
| `GET` | `/api/v2/services/{name}/demand` | Geo-demand heatmap cells: `[{"lat","lon","count"}]` (see [Geo-distributed demand](geo-demand.md)). |

## Cluster nodes (elasticity)

| Method | Path | Purpose | Success |
| --- | --- | --- | --- |
| `GET` | `/api/v2/nodes` | List node locations: candidates + active replicas (`[{"id","lat","lon","active"}]`). | `200` |
| `POST` | `/api/v2/nodes` | Add an active replica. Body: `{"id","host","port"}`. | `202` |
| `DELETE` | `/api/v2/nodes/{id}` | Remove an active replica. | `202` |

Node config changes are applied asynchronously (the cluster reconfigures and
transfers state); `202 Accepted` means the change was queued. Poll
`/api/v2/nodes` or a service's `/placement` for the result.

## Per-replica info (ActiveReplica)

Each ActiveReplica also serves its own live detail — queried directly from the
replica, not the control plane:

| Method | Path | Purpose |
| --- | --- | --- |
| `GET` | `/api/v2/services/{name}/replica/info` | This replica's role, protocol, epoch, consistency, and container status. |

The replica must know which service the request targets; pass `?_xdnsvc={name}`
or an `XDN: {name}` header (see [Service name inference](service-name-inference.md)).

## Status codes

`2xx` on success (`201` create, `202` async node change, `200` otherwise);
`400` malformed request; `404` unknown service; `409` create conflict; `5xx`
server error. Error responses carry a JSON body with a `RESPONSE_MESSAGE`.

## Deprecated: legacy `GET /?type=…`

Earlier versions did everything over `GET` with a query string —
`/?type=CREATE&name=…&initial_state=…`, `/?type=DELETE&name=…`,
`/?type=CHANGE_ACTIVES&…`. These still work for backward compatibility but are
**deprecated** (the server logs a warning) and will be removed; use the RESTful
endpoints above instead.
