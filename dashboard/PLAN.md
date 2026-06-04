# XDN Dashboard — Implementation Plan

A lightweight, **stateless client-side** web dashboard for XDN, deployable via
GitHub Pages (e.g. under `xdn.cs.umass.edu`). It lets people:

- deploy / destroy a service,
- see where a service's replicas are placed (on a world map),
- see the client geo-demand distribution,
- see inter-replica communication (latency for v1; bandwidth later).

The dashboard is pure static HTML/CSS/JS (no build step, no server of its own).
All dynamic data comes from the XDN Reconfigurator (RC) control-plane HTTP API,
which the browser calls directly.

## Locked decisions

| Decision | Choice |
|---|---|
| How the CP API is reachable from an HTTPS page | **Native TLS on the RC** (Netty, wildcard `*.xdnapp.com` cert). To avoid breaking existing plaintext clients (coredns geo-DNS, xdn-cli, trace_bw all use `:3300`), TLS runs on the *separate* SSL port **`:3400`** (`HTTP_PORT_SSL_OFFSET`); `:3300` stays plaintext. |
| Auth on deploy/destroy | **Fully open** — research cluster; anyone can mutate (show a banner) |
| "Inter-replica bandwidth" in v1 | **Analytic latency only** (Haversine + fiber, client-side); live bandwidth deferred |

## Why backend changes are needed at all

The app is thin, but a browser on `https://xdn.cs.umass.edu` cannot use the
control plane as it stands today:

- **Mixed content** — GitHub Pages is HTTPS; the RC API is plaintext HTTP on
  `:3300`, so the browser blocks the calls. (The AR *data plane* has TLS; the
  *control plane* does not.) → add native TLS to the RC API.
- **CORS** — `HttpActiveReplica` sets `forAnyOrigin()`, but `HttpReconfigurator`
  (the RC) sets no CORS headers, so cross-origin calls are blocked. → add CORS.
- **Two datasets aren't served over HTTP yet** — the geo-demand grid and the
  list of all services. → add two small endpoints.

## API surface

Existing (used as-is by the Go CLI — confirmed call shapes):

- **Deploy:** `GET /?type=CREATE&name=<svc>&initial_state=xdn:init:<json>`
  where `<json>` = `{name,image,port,state,consistency,deterministic,...}`.
  *(Legacy GET-with-query-params; v1 will use it, but see the RESTful follow-up.)*
- **Destroy:** `GET /?type=DELETE&name=<svc>`. *(Same legacy GET shape.)*
- **Placement + geolocation:** `GET /api/v2/services/<svc>/placement` →
  `DATA.NODES[]` of `{ID, ROLE, HTTP_ADDRESS, METADATA, GEOLOCATION{LATITUDE,LONGITUDE}}`,
  plus `EPOCH`, `SERVICE_METADATA`. (`GEOLOCATION` is null if unconfigured.)
- Responses share base fields `{NAME, EPOCH, FAILED, RESPONSE_MESSAGE}` — check
  `FAILED` even on HTTP 200.

New (this project):

- **List services:** `GET /api/v2/services` → `[{name, epoch, num_replicas, ...}]`.
  Needed because a stateless dashboard otherwise has no way to enumerate services.
  (The v2 list endpoint is already a documented TODO in `HttpReconfigurator`.)
- **Geo-demand:** `GET /api/v2/services/<svc>/demand` → `[{lat, lon, count}]`,
  decoding the `XdnGeoDemandProfiler` sparse 1000×1000 grid (already persisted in
  the SQL `demand` table; surfaced via `SQLReconfiguratorDB.getDemandStats`).

## Architecture

```
 Browser  (xdn.cs.umass.edu — static JS, stateless)
   │   Leaflet world map · Chart.js · deploy/destroy forms
   │   CP endpoint from ?cp=<host:port> or config.js  (default cp.xdnapp.com:3400)
   │   polls every few seconds
   ▼  HTTPS + CORS
 XDN Reconfigurator API  (https://cp.xdnapp.com:3400 = TLS SSL port; :3300 plaintext stays)
   GET /api/v2/services                      (NEW)
   GET /api/v2/services/<svc>/placement      (exists)
   GET /api/v2/services/<svc>/demand         (NEW)
   GET /?type=CREATE… , GET /?type=DELETE…   (exists)
```

`cp.xdnapp.com` already resolves to the RC via coredns and is covered by the
`*.xdnapp.com` wildcard cert. TLS is served on the SSL port `:3400`
(`HTTP_PORT_SSL_OFFSET`), kept distinct from the plaintext `:3300` that coredns,
xdn-cli, and trace_bw use; the security group now allows both.

## Tech stack

- Vanilla HTML/CSS/JS, **no build step** (matches the lightweight requirement and
  the repo's no-JS-tooling reality).
- Leaflet (map) + leaflet.heat (demand heatmap) + Chart.js — all via CDN.
- Lives in a new top-level `dashboard/` directory.

## Phases

### Phase 0 — backend enablers (Java + Terraform) — unblocks everything
- [x] `HttpReconfigurator`: add Netty `CorsHandler(forAnyOrigin())` (mirror
      `HttpActiveReplica.java:429`).
- [x] `HttpReconfigurator`: native TLS — `ENABLE_RECONFIGURATOR_HTTPS` +
      `RECONFIGURATOR_TLS_CERT_CHAIN`/`_PRIVATE_KEY` props; Netty `SslContext`
      from the wildcard PEM (mirror the AR), with a self-signed fallback while the
      cert isn't on disk yet (the RC boots before the cert is issued).
- [x] `aws/main.tf`: RC instance profile + IAM `s3:GetObject` on the TLS bucket
      (`rc_tls` role); cert-store vars passed via literal keys (no dep cycle).
- [x] `aws/rc-userdata.tftpl`: pull `fullchain.pem`/`privkey.pem` from S3 +
      PKCS#8-convert + `xdn-tls-pull.timer` (mirror `ar-userdata.tftpl`); append
      the HTTPS flags. Services start first (coredns up → issuance), then pull.
- [x] **Verified on a live cluster (2026-06-03):** `:3400` serves the real
      `*.xdnapp.com` cert (cold-start self-signed → cert-pull restart works);
      OPTIONS preflight + GET both return `Access-Control-Allow-Origin: *`;
      `/api/v2/services/<svc>/placement` returns nodes over HTTPS; `:3300`
      plaintext + coredns DNS unaffected. RC AMI `ami-05421989023a115de`.
      Minor: the cold-boot `xdn-tls-pull` retry-loop vs timer can race once (cert
      still loads); and Netty CORS returns empty `Allow-Methods` on preflight —
      fine for GET, but set `allowedRequestMethods` when the RESTful POST/DELETE
      verbs land (below).

### Phase 1 — dashboard skeleton + deploy
- [~] ~~`GET /api/v2/services` (list)~~ **DESCOPED for v1** — customers inspect a
      service by its known name (entered in the UI / `?svc=`), so the auto-list
      endpoint is not needed. Kept as an optional future nicety (see follow-ups).
- [x] `dashboard/{index.html,app.js,style.css,config.js}`; CP endpoint config (`?cp=`).
- [x] inspect-a-service-by-name input, deploy form (CREATE), per-service destroy
      (DELETE), "open research cluster" banner, connection status, action log.
- [x] Placement view: replica table (id/role/http/geo/metadata) + Leaflet map with
      role-coloured markers + fitBounds. Map is now populated — per-AR
      `active.<id>.geolocation` added to the AWS config (illustrative US-spread
      coords; the cluster is physically single-region us-east-1). `/placement`
      returns lat/lon for all 3 nodes.
- [x] `.github/workflows/deploy-dashboard.yml` → `actions/deploy-pages`
      (publishes `dashboard/` to GitHub Pages on push; enable Pages → Source:
      GitHub Actions). Also serving live from the RC at `https://cp.xdnapp.com/`.
- [ ] Browser-verify the rendering end-to-end (now self-serviceable via the RC URL).

### Phase 2 — geo-demand
- [x] Enable geo-demand profiling (`DEMAND_PROFILE_TYPE=…XdnGeoDemandProfiler`) in the AWS config.
- [x] `GET /api/v2/services/<svc>/demand` → `[{lat,lon,count}]`. Read-only:
      `AbstractDemandProfile.getDemandGeoCells()` (default empty; overridden by
      `XdnGeoDemandProfiler`, no reset) ← aggregate profiler ← a `ReconfiguratorFunctions`
      method ← `HttpReconfigurator` route. No xdn import in core (clean layering).
- [x] Dashboard: `leaflet.heat` heatmap layer + 5s polling + toggle, over the placement map.
- [x] Deployed (RC AMI `ami-013338ed93b6d7521` + apply; ARs reuse their AMI with
      the new config) + generated biased demand + **verified live (2026-06-03):**
      `/demand` returns cells with CORS (SF 111 / NY 46 / Chicago 21 from biased
      traffic); RC stayed 0 OOM / 0 restarts.

### Phase 2.5 — cluster topology (all candidate locations + active replicas)
- [x] Backend: `GET /api/v2/nodes` → `[{id,lat,lon,active}]` (active replicas +
      configured-but-idle candidate locations), via static `candidate_geolocations`
      in `aws/main.tf` (cost $0 — config only, no running node).
- [x] Backend: runtime add/remove of ActiveReplicas (`CHANGE_ACTIVES`) +
      `XdnReplicaCoordinator` phantom-meta-group STOP-ack fix + `WaitAckStopEpoch`
      wedge alarm; verified add/remove on AWS (no wedge).
- [x] Dashboard: `topologyLayer` fetches `/api/v2/nodes` on connect; solid markers
      for active replicas, hollow/dashed for candidate locations, legend + toggle.
- [ ] Browser-verify the topology rendering end-to-end.

### Phase 3 — inter-replica edges (analytic latency)
- [ ] Dashboard: polylines between replica markers, labeled with Haversine+fiber
      latency (port `xdn-bw-trace/plot_lat_graph.py` math to JS). No backend.

### Phase 4 — live bandwidth (stretch, deferred)
- [ ] Implement `XdnBandwidthProfiler` (analog of `XdnGeoDemandProfiler`) +
      `GET /api/v2/services/<svc>/bandwidth`. Replaces the analytic edges with
      measured inter-replica bandwidth. Large; tracked separately.

## Later / follow-ups (not blocking v1)

- [ ] **RESTful `/api/v2/services`.** Today deploy/destroy go through the legacy
      `GET /?type=CREATE` / `GET /?type=DELETE` query-param API, and only GET
      endpoints exist under `/api/v2/services`. Add the proper verbs the code
      already TODOs in `HttpReconfigurator`:
      - `POST   /api/v2/services` (or `/api/v2/services/{name}`) — create/deploy
        (JSON body instead of `initial_state` query param),
      - `DELETE /api/v2/services/{name}` — destroy,
      - `GET    /api/v2/services/{name}` — single-service info.
      Then point both the dashboard and `xdn-cli` at these. Naturally pairs with
      the Phase 1 `GET /api/v2/services` list endpoint (same router); keep the
      legacy GET shapes working for back-compat during migration.

## Notes / risks

- **Open mutations:** per the locked decision, deploy/destroy are unauthenticated.
  Acceptable for a throwaway research cluster; the UI will say so plainly. Revisit
  (token-gated mutations) before pointing this at anything persistent.
- **TLS on the RC** adds cert distribution to the RC (it currently pulls nothing
  from S3); this is the one new piece of infra plumbing.
- **`GET /api/v2/services`** is net-new but small, and removes the need for the
  client to hold any state.
