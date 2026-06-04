# Service Name Inference

An ActiveReplica's HTTP frontend hosts many services at once, but an incoming
HTTP request is just a normal web request — it has no XDN-specific field. So
before the request can be routed to the right service (and coordinated), the
ActiveReplica must figure out **which service it is for**. This page describes
how that name is inferred.

## Where the name can come from

For each request the ActiveReplica looks in four places, in priority order, and
takes the first one that yields a non-empty name:

1. **`_xdnsvc` query parameter** — e.g. `http://host:port/path?_xdnsvc=my-service`.
   Useful for direct links and testing, where you can't control the Host header.
2. **`XDN` header** — e.g. `XDN: my-service`. Explicit and unambiguous; this is
   what clients and tools (e.g. the CLI) set.
3. **`XDN` cookie** — `Cookie: XDN=my-service`. Lets a browser keep talking to the
   same service across navigation without re-specifying it (see below).
4. **Host subdomain** — the left-most label of the hostname, e.g.
   `my-service.xdnapp.com` → `my-service`. This is the normal production path:
   each service gets its own subdomain, so an ordinary browser request to the
   service's URL just works.

If none of these yields a name, the request is rejected (the frontend can't know
where to route it).

## Cookie convenience for browsers

When a request names the service via `_xdnsvc`, the ActiveReplica responds with
`Set-Cookie: XDN=<service>`. Subsequent requests from that browser then carry the
service name automatically via the cookie (priority 3), so links and relative
asset paths keep working without the query parameter.

## Normalization

Once the name is inferred, the ActiveReplica:

- copies it into the **`XDN` header**, so the service name survives when the
  request is serialized and replicated to other replicas (e.g. Paxos followers),
  which re-run inference on the deserialized form; and
- **strips** the `_xdnsvc` query parameter from the URI, so the containerized
  service never sees XDN's routing internals.

## Where this lives in the code

| Concern | Location |
| --- | --- |
| Inference order + Host parsing | `XdnHttpRequest.inferServiceName` |
| Param / cookie names | `XdnHttpRequest.XDN_SVC_QUERY_PARAM`, `XDN` header/cookie |
| Cookie + header normalization | `XdnHttpRequest` (constructor / response path) |
