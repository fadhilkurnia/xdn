# Geo-distributed Demand

XDN places service replicas close to where requests actually come from. To do
that, every Active Replica (AR) continuously builds a coarse, anonymous map of
*where* its clients are, and rolls those maps up to the control plane. This page
describes that aggregation at a high level.

## Where a client's location comes from

For each request, the AR derives one approximate client location, in priority
order:

1. **`X-Client-Location` header** — a `lat,lon` the client (or an upstream proxy)
   declares explicitly.
2. **Source IP geolocation** — when no header is present, the client's IP
   (IPv4 or IPv6) is mapped to a city-level location using a local GeoLite2-City
   database.

Addresses that aren't real client locations — loopback, private/RFC1918,
link-local, multicast, IPv6 ULA, IPv4 CGNAT — are ignored, and any IP that can't
be resolved simply contributes nothing.

The geolocation lookup never touches the request hot path: each request only
enqueues its header or its source IP, and a background worker does the
(comparatively expensive) database lookup and grid update asynchronously.

## The demand grid

Each AR keeps demand in a fixed **1000 × 1000 latitude/longitude grid** covering
the whole Earth — about **0.2° per cell (~20 km)**. A client location increments
the one cell it falls in. The grid is stored sparsely, so only cells that have
actually seen traffic cost anything.

Each cell tracks **reads and writes separately** — a request is classified from
its declared behavior (`READ_ONLY` vs `WRITE_ONLY` / `READ_MODIFY_WRITE`, the
latter folding into *write* since it mutates state). This lets the heatmap and
analyses distinguish read-heavy from write-heavy regions.

No IP addresses, identities, or per-request data are retained — only per-cell
read/write request counts.

## Aggregating across replicas

```
client ──▶ AR (local 1000×1000 grid)
                  │  every ≥10s: snapshot + reset
                  ▼
            DemandReport ──▶ Reconfigurator (control plane)
                                  │ combine: sum per cell
                                  ▼
                         global demand map ──▶ placement + dashboard
```

- **Per AR:** roughly every 10 seconds an AR snapshots its grid into a
  `DemandReport` and **resets** its local grid, so each report carries only the
  demand *since the last report* (a delta). To keep reports small, only the
  busiest cells (top ~200) are sent, with their read/write counts.
- **At the Reconfigurator:** incoming reports are **combined by summing the
  read/write counts per cell**, producing a cluster-wide view of demand
  geo-distribution. The retention is governed by the config
  `XDN_DEMAND_WINDOW_MINUTES`: `-1` (default) keeps **cumulative** all-time demand;
  a positive `N` keeps only the **last `N` minutes** (a rolling window, so demand
  decays), which makes both the heatmap and placement reflect *current* load.

## What the aggregated demand is used for

- **Replica placement / elasticity** — the control plane compares the demand map
  against candidate edge locations to decide where a service's replicas should
  run, moving capacity toward the regions generating load.
- **Dashboard heatmap** — the same per-cell counts are exposed as
  `{lat, lon, read, write, count}` points (`count = read + write`) and rendered as
  the demand heatmap on the XDN dashboard, with a read/write/both selector.

## Where this lives in the code

| Concern | Location |
| --- | --- |
| Per-request grid + report/reset/combine | `edu.umass.cs.xdn.XdnGeoDemandProfiler` |
| IP → location (GeoLite2-City, caching, local-address guard) | `edu.umass.cs.xdn.GeoIpResolver` |
| Heatmap cells (`{lat, lon, read, write, count}`) | `XdnGeoDemandProfiler#getDemandGeoCells` |
| Read/write classification | `XdnHttpRequest#getBehaviors` (`RequestBehaviorType`) |
| Demand window config (`-1` cumulative / `N` minutes) | `ReconfigurationConfig.RC.XDN_DEMAND_WINDOW_MINUTES` |
