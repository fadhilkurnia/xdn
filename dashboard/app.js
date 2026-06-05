/* XDN Dashboard — stateless client. Talks directly to the XDN Reconfigurator's
 * TLS control-plane API. The address bar (?cp=, ?svc=) is the only state. */
"use strict";

// The XDN control-plane TLS API port is an internal detail; the UI only ever
// shows/accepts the host, and the dashboard assumes :3400 when building requests.
const PORT = 3400;
const bareHost = (s) =>
  String(s || "").replace(/^https?:\/\//, "").replace(/\/.*$/, "").replace(/:\d+$/, "").trim();

const DEFAULT_CP = bareHost(
  (window.XDN_DASHBOARD_CONFIG && window.XDN_DASHBOARD_CONFIG.defaultControlPlane) ||
  "cp.xdnapp.com"
);

const $ = (sel) => document.querySelector(sel);
const params = new URLSearchParams(location.search);

let controlPlane = bareHost(params.get("cp")) || DEFAULT_CP;
let currentSvc = params.get("svc") || null;
let map, markerLayer, heatLayers = [], lastDemandCells = [], demandTimer, topologyLayer;

// ---- URL state -------------------------------------------------------------
function syncUrl() {
  const p = new URLSearchParams();
  if (controlPlane && controlPlane !== DEFAULT_CP) p.set("cp", controlPlane);
  if (currentSvc) p.set("svc", currentSvc);
  const qs = p.toString();
  history.replaceState(null, "", qs ? `?${qs}` : location.pathname);
}

// ---- HTTP helpers ----------------------------------------------------------
const base = () => `https://${controlPlane}:${PORT}`;

async function api(path, opts = {}) {
  const resp = await fetch(base() + path, { mode: "cors", ...opts });
  const text = await resp.text();
  let body = null;
  try { body = text ? JSON.parse(text) : null; } catch (_) { /* non-JSON */ }
  return { ok: resp.ok, status: resp.status, body, text };
}


function log(msg, isError) {
  const el = $("#action-log");
  el.hidden = false;
  const stamp = new Date().toISOString().slice(11, 19);
  el.textContent = `[${stamp}] ${msg}\n` + el.textContent;
  el.style.color = isError ? "#fca5a5" : "#cbd5e1";
}

// ---- Connection ------------------------------------------------------------
function setConn(ok, text) {
  $("#conn-dot").className = "dot " + (ok ? "ok" : "bad");
  $("#footer-text").textContent = text;
  $("#cp-status").className = "status " + (ok ? "ok" : "bad");
  $("#cp-status").textContent = ok ? "connected" : "unreachable";
}

async function connect() {
  controlPlane = bareHost($("#cp").value) || DEFAULT_CP;
  syncUrl();
  setConn(false, "connecting…");
  try {
    // /api/v2/nodes doubles as the reachability probe and the topology source: a
    // successful response means the TLS control plane is reachable, and its body is
    // the cluster node list we draw on the map.
    const r = await api("/api/v2/nodes");
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    setConn(true, `connected to ${controlPlane}`);
    if (Array.isArray(r.body)) drawTopology(r.body);
    if (currentSvc) { $("#svc-name").value = currentSvc; inspect(currentSvc); }
  } catch (e) {
    setConn(false, `cannot reach ${controlPlane}`);
    log(
      `Connection failed: ${e.message}. Check the control-plane host is correct and ` +
      `reachable — the XDN Reconfigurator serves its HTTPS API on :3400.`,
      true
    );
  }
}

// ---- Deploy / destroy ------------------------------------------------------
async function deploy(form) {
  const f = new FormData(form);
  const cfg = {
    name: f.get("name").trim(),
    image: f.get("image").trim(),
    port: Number(f.get("port")) || 80,
    state: f.get("state") || "/",
    consistency: f.get("consistency"),
    deterministic: f.get("deterministic") === "on",
  };
  const initial = "xdn:init:" + JSON.stringify(cfg);
  log(`Deploying "${cfg.name}" (${cfg.image})…`);
  try {
    // RESTful create: POST /api/v2/services/{name} with the initial state in the body.
    const r = await api(`/api/v2/services/${encodeURIComponent(cfg.name)}`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ initial_state: initial }),
    });
    if (r.ok && !(r.body && r.body.FAILED)) {
      log(`Deployed "${cfg.name}". Reconfiguring replicas…`);
      setTimeout(() => inspect(cfg.name), 1500);
    } else {
      log(`Deploy failed: ${(r.body && r.body.RESPONSE_MESSAGE) || r.status}`, true);
    }
  } catch (e) {
    log(`Deploy error: ${e.message}`, true);
  }
}

async function destroy(name) {
  if (!confirm(`Destroy service "${name}"? This removes it from the cluster.`)) return;
  log(`Destroying "${name}"…`);
  try {
    // RESTful destroy: DELETE /api/v2/services/{name}.
    const r = await api(`/api/v2/services/${encodeURIComponent(name)}`, { method: "DELETE" });
    if (r.ok && !(r.body && r.body.FAILED)) {
      log(`Destroyed "${name}".`);
      if (currentSvc === name) { currentSvc = null; syncUrl(); clearPlacement(); }
    } else {
      log(`Destroy failed: ${(r.body && r.body.RESPONSE_MESSAGE) || r.status}`, true);
    }
  } catch (e) {
    log(`Destroy error: ${e.message}`, true);
  }
}

// ---- Placement view --------------------------------------------------------
async function inspect(name) {
  currentSvc = name;
  syncUrl();
  $("#svc-name").value = name;
  $("#destroy-btn").disabled = false;
  $("#placement-svc").textContent = `· ${name}`;
  const note = $("#placement-note");
  note.textContent = "Loading…";
  try {
    const r = await api(`/api/v2/services/${encodeURIComponent(name)}/placement`);
    if (!r.ok || !r.body) {
      note.textContent = `No placement for "${name}" (HTTP ${r.status}).`;
      renderReplicas([]); drawMarkers([]);
      return;
    }
    const nodes = (r.body.DATA && r.body.DATA.NODES) || [];
    const meta = (r.body.DATA && r.body.DATA.SERVICE_METADATA) || "";
    note.innerHTML =
      `epoch <b>${r.body.EPOCH ?? "?"}</b> · ${nodes.length} replica(s)` +
      (meta ? ` · <span class="mono">${esc(meta)}</span>` : "");
    renderReplicas(nodes);
    drawMarkers(nodes);
    startDemandPolling(name);
  } catch (e) {
    note.textContent = `Error loading placement: ${e.message}`;
  }
}

function clearPlacement() {
  $("#placement-svc").textContent = "";
  $("#placement-note").textContent = "";
  $("#demand-note").textContent = "";
  $("#svc-name").value = "";
  $("#destroy-btn").disabled = true;
  stopDemandPolling();
  renderReplicas([]); drawMarkers([]);
}

function roleIsLeader(role) {
  const r = String(role || "").toLowerCase();
  return r.includes("leader") || r.includes("coordinator") || r.includes("primary");
}

function rolePill(role) {
  return `<span class="pill ${roleIsLeader(role) ? "leader" : "replica"}">${esc(role || "replica")}</span>`;
}

// The placement (/placement) gives each replica's role + address; its live
// status (container health, epoch, consistency) comes from the replica itself.
// Like `xdn service info`, the client queries each replica's own /replica/info
// endpoint directly — the control plane stays out of it.
function renderReplicas(nodes) {
  const tbody = $("#replicas tbody");
  tbody.innerHTML = "";
  const name = currentSvc;
  for (const n of nodes) {
    const geo = n.GEOLOCATION
      ? `${n.GEOLOCATION.LATITUDE.toFixed(2)}, ${n.GEOLOCATION.LONGITUDE.toFixed(2)}`
      : "—";
    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td class="mono">${esc(n.ID)}</td>` +
      `<td class="role-cell">${rolePill(n.ROLE)}</td>` +
      `<td class="status-cell"><span class="muted">…</span></td>` +
      `<td class="mono">${replicaAddrCell(n, name)}</td>` +
      `<td class="mono">${geo}</td>`;
    tbody.appendChild(tr);
    if (name) updateReplicaDetail(tr, n, name);
    else tr.querySelector(".status-cell").innerHTML = `<span class="muted">—</span>`;
  }
}

// Fetch one replica's live detail and fill in its Role/Status cells. Failures
// (offline, mixed-content block, CORS) degrade to "unreachable" — same as the CLI.
async function updateReplicaDetail(tr, node, name) {
  const statusCell = tr.querySelector(".status-cell");
  const roleCell = tr.querySelector(".role-cell");
  const bases = replicaInfoBases(node);
  if (!bases.length) { statusCell.innerHTML = `<span class="muted">no address</span>`; return; }
  try {
    const info = await fetchReplicaInfo(bases, name);
    if (info.role) roleCell.innerHTML = rolePill(info.role); // live role wins
    const c = pickStatefulContainer(info);
    const status = (c && c.status) || "—";
    const tip = [
      info.protocol ? `protocol: ${info.protocol}` : "",
      info.epoch != null && info.epoch !== "?" ? `epoch: ${info.epoch}` : "",
      info.consistency && info.consistency !== "?" ? `consistency: ${info.consistency}` : "",
      c && c.image ? `image: ${c.image}` : "",
      c && c.createdAt && c.createdAt !== "?" ? `created: ${c.createdAt}` : "",
    ].filter(Boolean).join("\n");
    statusCell.innerHTML = `<span class="status ${statusClass(status)}">${esc(status)}</span>`;
    if (tip) statusCell.firstChild.setAttribute("title", tip);
  } catch (e) {
    statusCell.innerHTML =
      `<span class="status bad" title="${esc(e.message || e)}">unreachable</span>`;
  }
}

// Parse Java InetSocketAddress.toString() ("host/ip:port"; host may be empty,
// ip may be IPv6) into a browser base URL for the replica's clear HTTP frontend.
function parseSockAddr(s) {
  s = String(s || "");
  const lastColon = s.lastIndexOf(":");
  if (lastColon < 0) return null;
  const port = parseInt(s.slice(lastColon + 1), 10);
  if (!Number.isFinite(port)) return null;
  const hostPart = s.slice(0, lastColon);
  const slash = hostPart.indexOf("/");
  const host = slash >= 0 ? hostPart.slice(0, slash) : "";
  const ip = slash >= 0 ? hostPart.slice(slash + 1) : hostPart;
  if (!ip) return null;
  return { host: host.trim(), ip, port };
}

// Per-replica edge name. The control plane is <label>.<base-domain> (e.g.
// cp.xdnapp.com); each replica is reachable over a cert-valid name
// <nodeid>.edge.<base-domain> (covered by the *.edge.<base-domain> cert), which
// coredns resolves to that specific replica's IPv6. Returns null for a bare host.
function edgeBaseDomain() {
  const cp = String(controlPlane || "");
  // Edge names only make sense for a NAMED control plane like rc.<domain.tld>.
  // Bail on IP literals and apex/2-label hosts, which would otherwise yield a
  // bogus <id>.edge.<tld> (e.g. rc.xdnapp.com -> xdnapp.com, but 1.2.3.4 ->
  // 2.3.4); callers then fall back to the raw advertised address.
  if (cp.includes(":") || /^[0-9.]+$/.test(cp)) return null; // IPv6/IPv4 literal
  const parts = cp.split(".");
  return parts.length >= 3 ? parts.slice(1).join(".") : null;
}
function edgeHost(nodeId) {
  const base = edgeBaseDomain();
  return base ? `${nodeId}.edge.${base}` : null;
}

// HTTP-address cell: a clickable link that opens the service served BY THIS
// specific replica (https://<nodeid>.edge.<base>/?_xdnsvc=<svc>). Falls back to
// the raw advertised address when there's no edge name (bare-host control plane).
function replicaAddrCell(node, svc) {
  const host = edgeHost(node.ID);
  if (host && svc) {
    const url = `https://${host}/?_xdnsvc=${encodeURIComponent(svc)}`;
    return `<a href="${esc(url)}" target="_blank" rel="noopener" ` +
      `title="open ${esc(svc)} served by ${esc(node.ID)}">${esc(host)}</a>`;
  }
  return esc(node.HTTP_ADDRESS || node.ADDRESS || "");
}

// Ordered base URLs to reach a replica's frontend, most-preferred first. The
// cert-valid per-replica edge name (HTTPS) is tried first (works from a browser);
// the raw advertised address is kept as a fallback for non-edge deployments.
function replicaInfoBases(node) {
  const bases = [];
  const host = edgeHost(node.ID);
  if (host) bases.push(`https://${host}`); // cert-valid per-replica name
  const addr = parseSockAddr(node.HTTP_ADDRESS || node.ADDRESS);
  if (addr) {
    const ipH = addr.ip.includes(":") ? `[${addr.ip}]` : addr.ip; // bracket IPv6
    const authority = addr.host || ipH;
    bases.push(`https://${authority}`, `http://${authority}`);
    if (addr.port && addr.port !== 80 && addr.port !== 443) {
      bases.push(`http://${ipH}:${addr.port}`); // internal clear HTTP frontend port
    }
  }
  return bases;
}

// Try each base URL in order; return the first replica/info JSON that succeeds.
async function fetchReplicaInfo(bases, name) {
  let lastErr = new Error("no address");
  for (const base of bases) {
    try {
      return await fetchReplicaInfoOnce(base, name);
    } catch (e) {
      lastErr = e;
    }
  }
  throw lastErr;
}

async function fetchReplicaInfoOnce(base, name) {
  // Header-less GET so it stays a CORS "simple" request (no preflight); the AR
  // allows any origin. The service name rides _xdnsvc instead of the XDN header.
  const url =
    `${base}/api/v2/services/${encodeURIComponent(name)}/replica/info` +
    `?_xdnsvc=${encodeURIComponent(name)}`;
  const ctrl = new AbortController();
  const t = setTimeout(() => ctrl.abort(), 4000);
  try {
    const resp = await fetch(url, { mode: "cors", redirect: "follow", signal: ctrl.signal });
    if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
    return await resp.json();
  } finally {
    clearTimeout(t);
  }
}

// The containers[] entry whose name == statefulComponent, else the first one.
function pickStatefulContainer(info) {
  const cs = Array.isArray(info.containers) ? info.containers : [];
  if (!cs.length) return null;
  if (info.statefulComponent) {
    const m = cs.find((c) => c && c.name === info.statefulComponent);
    if (m) return m;
  }
  return cs[0];
}

function statusClass(s) {
  const v = String(s || "").toLowerCase();
  if (/up|running|healthy/.test(v)) return "ok";
  if (/exit|dead|stop|unhealthy|restart|created/.test(v)) return "bad";
  return "";
}

// ---- Map -------------------------------------------------------------------
function initMap() {
  map = L.map("map", { worldCopyJump: true }).setView([20, 0], 2);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "© OpenStreetMap", maxZoom: 18,
  }).addTo(map);
  // Cluster-topology layer (all candidate locations + active replicas) sits below
  // the per-service placement markers.
  topologyLayer = L.layerGroup().addTo(map);
  markerLayer = L.layerGroup().addTo(map);
}

function drawMarkers(nodes) {
  if (!markerLayer) return;
  markerLayer.clearLayers();
  const pts = [];
  for (const n of nodes) {
    if (!n.GEOLOCATION) continue;
    const { LATITUDE: lat, LONGITUDE: lon } = n.GEOLOCATION;
    const role = (n.ROLE || "").toLowerCase();
    const isLeader = role.includes("leader") || role.includes("coordinator");
    const color = isLeader ? getCSS("--leader") : getCSS("--replica");
    L.circleMarker([lat, lon], {
      radius: 8, color: "#fff", weight: 2, fillColor: color, fillOpacity: 0.9,
    })
      .bindTooltip(`${n.ID}${isLeader ? " (leader)" : ""}`, { permanent: false })
      .addTo(markerLayer);
    pts.push([lat, lon]);
  }
  const note = $("#placement-note");
  if (!pts.length && nodes.length) {
    note.innerHTML += ` · <span class="muted">no node geolocation configured (set ` +
      `<code>active.&lt;node&gt;.geolocation</code> to plot replicas)</span>`;
  } else if (pts.length) {
    map.fitBounds(pts, { padding: [40, 40], maxZoom: 6 });
  }
}

// ---- Cluster topology (all potential edge locations) -----------------------
// GET /api/v2/nodes -> [{id, lat, lon, active}] (fetched in connect()). Every node
// renders as a uniform small hollow marker; the active-vs-candidate state is not
// distinguished here. Solid markers are reserved for a deployed service's actual
// replica placement (drawMarkers).
function drawTopology(nodes) {
  if (!topologyLayer) return;
  topologyLayer.clearLayers();
  const pts = [];
  let n = 0;
  for (const node of nodes) {
    if (typeof node.lat !== "number" || typeof node.lon !== "number") continue;
    n++; pts.push([node.lat, node.lon]);
    // Every potential edge location renders as a small hollow marker, with no
    // active-vs-candidate distinction. Solid markers are reserved for a deployed
    // service's actual replica placement (drawMarkers).
    L.circleMarker([node.lat, node.lon], {
      radius: 4, color: getCSS("--muted"), weight: 1.5,
      fillColor: "#fff", fillOpacity: 0.25,
    }).bindTooltip(esc(node.id)).addTo(topologyLayer);
  }
  const count = $("#topo-count");
  if (count) count.textContent = ` · ${n} location${n === 1 ? "" : "s"}`;
  // Don't override the per-service fit when a service is being inspected.
  if (pts.length && !currentSvc) map.fitBounds(pts, { padding: [40, 40], maxZoom: 5 });
}

// ---- Demand heatmap --------------------------------------------------------
async function fetchDemand(name) {
  try {
    const r = await api(`/api/v2/services/${encodeURIComponent(name)}/demand`);
    const cells = r.ok && Array.isArray(r.body) ? r.body : [];
    lastDemandCells = cells;
    drawHeatmap(cells);
    const reads = cells.reduce((s, c) => s + (c.read || 0), 0);
    const writes = cells.reduce((s, c) => s + (c.write || 0), 0);
    $("#demand-note").textContent = cells.length
      ? `demand: ${reads + writes} req (${reads} read / ${writes} write) across ${cells.length} cell(s)`
      : "no demand yet — send requests with an X-Client-Location header";
  } catch (e) {
    $("#demand-note").textContent = `demand unavailable: ${e.message}`;
  }
}

function clearHeatLayers() {
  for (const l of heatLayers) if (map) map.removeLayer(l);
  heatLayers = [];
}

// Render demand as one or two heat layers per the read/write selector: reads use a cool (blue)
// gradient, writes a warm (red) gradient; "both" overlays them so hotspots of each kind show at a
// glance. Backward-compatible: falls back to total `count` when read/write fields are absent.
function drawHeatmap(cells) {
  if (!map || typeof L.heatLayer !== "function") return;
  clearHeatLayers();
  if (!cells.length || !$("#heat-toggle").checked) return;
  const kind = ($("#demand-kind") && $("#demand-kind").value) || "both";
  const READ_GRAD = { 0.2: "#60a5fa", 0.5: "#2563eb", 1: "#1e3a8a" };
  const WRITE_GRAD = { 0.2: "#f59e0b", 0.5: "#ef4444", 1: "#7f1d1d" };
  const weightFor = (c, k) =>
    k === "read" ? (c.read || 0)
    : k === "write" ? (c.write || 0)
    : (c.read != null || c.write != null) ? (c.read || 0) + (c.write || 0) : (c.count || 0);
  const addLayer = (k, grad) => {
    const pts = cells.map((c) => [c.lat, c.lon, weightFor(c, k)]).filter((p) => p[2] > 0);
    if (!pts.length) return;
    const max = Math.max(...pts.map((p) => p[2]));
    const layer = L.heatLayer(pts, { radius: 28, blur: 18, max, minOpacity: 0.3, gradient: grad });
    layer.addTo(map);
    heatLayers.push(layer);
  };
  if (kind === "read" || kind === "both") addLayer("read", READ_GRAD);
  if (kind === "write" || kind === "both") addLayer("write", WRITE_GRAD);
}

function startDemandPolling(name) {
  stopDemandPolling();
  fetchDemand(name);
  demandTimer = setInterval(() => fetchDemand(name), 5000);
}

function stopDemandPolling() {
  if (demandTimer) { clearInterval(demandTimer); demandTimer = null; }
  clearHeatLayers();
}

// ---- utils -----------------------------------------------------------------
const esc = (s) =>
  String(s ?? "").replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
const getCSS = (v) => getComputedStyle(document.documentElement).getPropertyValue(v).trim();

// ---- wire-up ---------------------------------------------------------------
window.addEventListener("DOMContentLoaded", () => {
  $("#banner").hidden = false;
  $("#banner").textContent =
    "⚠ Open research cluster — anyone with this page can deploy or destroy services.";
  $("#cp").value = controlPlane;
  if (currentSvc) $("#svc-name").value = currentSvc;
  initMap();
  const doInspect = () => { const n = $("#svc-name").value.trim(); if (n) inspect(n); };
  $("#connect").onclick = connect;
  $("#cp").addEventListener("keydown", (e) => { if (e.key === "Enter") connect(); });
  $("#inspect-btn").onclick = doInspect;
  $("#svc-name").addEventListener("keydown", (e) => { if (e.key === "Enter") doInspect(); });
  $("#destroy-btn").onclick = () => { if (currentSvc) destroy(currentSvc); };
  $("#heat-toggle").onchange = () => drawHeatmap(lastDemandCells);
  if ($("#demand-kind")) $("#demand-kind").onchange = () => drawHeatmap(lastDemandCells);
  $("#topo-toggle").onchange = () => {
    if (!topologyLayer || !map) return;
    if ($("#topo-toggle").checked) topologyLayer.addTo(map); else map.removeLayer(topologyLayer);
  };
  $("#deploy-form").addEventListener("submit", (e) => { e.preventDefault(); deploy(e.target); });
  connect();
});
