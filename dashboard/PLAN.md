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
| How the CP API is reachable from an HTTPS page | **Native TLS on the RC `:3300`** (Netty, wildcard `*.xdnapp.com` cert) |
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
- **Destroy:** `GET /?type=DELETE&name=<svc>`.
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
   │   CP endpoint from ?cp=<host:port> or config.js  (default cp.xdnapp.com:3300)
   │   polls every few seconds
   ▼  HTTPS + CORS
 XDN Reconfigurator API  (https://cp.xdnapp.com:3300, TLS via *.xdnapp.com)
   GET /api/v2/services                      (NEW)
   GET /api/v2/services/<svc>/placement      (exists)
   GET /api/v2/services/<svc>/demand         (NEW)
   GET /?type=CREATE… , GET /?type=DELETE…   (exists)
```

`cp.xdnapp.com` already resolves to the RC via coredns and is covered by the
`*.xdnapp.com` wildcard cert; the security group already allows `:3300`.

## Tech stack

- Vanilla HTML/CSS/JS, **no build step** (matches the lightweight requirement and
  the repo's no-JS-tooling reality).
- Leaflet (map) + leaflet.heat (demand heatmap) + Chart.js — all via CDN.
- Lives in a new top-level `dashboard/` directory.

## Phases

### Phase 0 — backend enablers (Java + Terraform) — unblocks everything
- [ ] `HttpReconfigurator`: add Netty `CorsHandler(forAnyOrigin())` (mirror
      `HttpActiveReplica.java:429`).
- [ ] `HttpReconfigurator`: native TLS — `ENABLE_RECONFIGURATOR_HTTPS` +
      cert-chain/private-key path props; build Netty `SslContext` from the
      wildcard PEM (mirror the AR frontend's TLS init).
- [ ] `aws/main.tf`: RC instance profile + IAM `s3:GetObject` on the TLS bucket
      (mirror the `ar_acme` role's S3 statement).
- [ ] `aws/rc-userdata.tftpl`: pull `fullchain.pem`/`privkey.pem` from S3 and
      PKCS#8-convert (mirror `ar-userdata.tftpl`); write the HTTPS flags.
- [ ] Verify: browser `fetch` of `/placement` over `https://cp.xdnapp.com:3300`
      succeeds, including the CORS preflight (OPTIONS).

### Phase 1 — dashboard skeleton + list endpoint + deploy
- [ ] `HttpReconfigurator`: implement `GET /api/v2/services` (list).
- [ ] `dashboard/{index.html,app.js,style.css}`; CP endpoint config (`?cp=`).
- [ ] Service list (from the new list endpoint), deploy form (CREATE), destroy
      button (DELETE), "open research cluster" banner.
- [ ] Placement map: a marker per replica from `/placement`, colored by `ROLE`,
      `METADATA` popup.
- [ ] `.github/workflows/deploy-dashboard.yml` → `actions/deploy-pages`.

### Phase 2 — geo-demand
- [ ] `HttpReconfigurator`: `GET /api/v2/services/<svc>/demand` (decode sparse grid).
- [ ] Dashboard: Leaflet heatmap layer over the demand cells.

### Phase 3 — inter-replica edges (analytic latency)
- [ ] Dashboard: polylines between replica markers, labeled with Haversine+fiber
      latency (port `xdn-bw-trace/plot_lat_graph.py` math to JS). No backend.

### Phase 4 — live bandwidth (stretch, deferred)
- [ ] Implement `XdnBandwidthProfiler` (analog of `XdnGeoDemandProfiler`) +
      `GET /api/v2/services/<svc>/bandwidth`. Replaces the analytic edges with
      measured inter-replica bandwidth. Large; tracked separately.

## Notes / risks

- **Open mutations:** per the locked decision, deploy/destroy are unauthenticated.
  Acceptable for a throwaway research cluster; the UI will say so plainly. Revisit
  (token-gated mutations) before pointing this at anything persistent.
- **TLS on the RC** adds cert distribution to the RC (it currently pulls nothing
  from S3); this is the one new piece of infra plumbing.
- **`GET /api/v2/services`** is net-new but small, and removes the need for the
  client to hold any state.
