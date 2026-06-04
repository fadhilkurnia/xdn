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
let map, markerLayer, heatLayer, demandTimer, topologyLayer;

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

// Legacy GET-based create/delete (mirrors the Go CLI). REST verbs are a planned
// follow-up; until then deploy/destroy ride the /?type= query API.
function legacy(typeAndParams) {
  return api(`/?${typeAndParams}`);
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
  const q =
    `type=CREATE&name=${encodeURIComponent(cfg.name)}` +
    `&initial_state=${encodeURIComponent(initial)}`;
  log(`Deploying "${cfg.name}" (${cfg.image})…`);
  try {
    const r = await legacy(q);
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
    const r = await legacy(`type=DELETE&name=${encodeURIComponent(name)}`);
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
      `<td class="mono">${esc(n.HTTP_ADDRESS || n.ADDRESS || "")}</td>` +
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

// Ordered base URLs to reach a replica's HTTP frontend, most-preferred first.
// The AR terminates TLS on :443 (wildcard *.<domain> cert, :80 redirects), so we
// try HTTPS first and fall back to HTTP. A DNS hostname (when the placement
// carries one) is preferred over the raw IP since it can match the TLS cert;
// the internal clear-HTTP port is the last resort.
function replicaInfoBases(node) {
  const addr = parseSockAddr(node.HTTP_ADDRESS || node.ADDRESS);
  if (!addr) return [];
  const ipH = addr.ip.includes(":") ? `[${addr.ip}]` : addr.ip; // bracket IPv6
  const authority = addr.host || ipH; // hostname preferred (matches the cert)
  const bases = [`https://${authority}`, `http://${authority}`];
  if (addr.port && addr.port !== 80 && addr.port !== 443) {
    bases.push(`http://${ipH}:${addr.port}`); // internal clear HTTP frontend port
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
    drawHeatmap(cells);
    const total = cells.reduce((s, c) => s + (c.count || 0), 0);
    $("#demand-note").textContent = cells.length
      ? `demand: ${total} request(s) across ${cells.length} cell(s)`
      : "no demand yet — send requests with an X-Client-Location header";
  } catch (e) {
    $("#demand-note").textContent = `demand unavailable: ${e.message}`;
  }
}

function drawHeatmap(cells) {
  if (!map || typeof L.heatLayer !== "function") return;
  if (heatLayer) { map.removeLayer(heatLayer); heatLayer = null; }
  if (!cells.length) return;
  const maxCount = Math.max(...cells.map((c) => c.count || 1));
  const points = cells.map((c) => [c.lat, c.lon, c.count || 1]);
  heatLayer = L.heatLayer(points, { radius: 28, blur: 18, max: maxCount, minOpacity: 0.3 });
  if ($("#heat-toggle").checked) heatLayer.addTo(map);
}

function startDemandPolling(name) {
  stopDemandPolling();
  fetchDemand(name);
  demandTimer = setInterval(() => fetchDemand(name), 5000);
}

function stopDemandPolling() {
  if (demandTimer) { clearInterval(demandTimer); demandTimer = null; }
  if (heatLayer && map) { map.removeLayer(heatLayer); heatLayer = null; }
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
  $("#heat-toggle").onchange = () => {
    if (!heatLayer || !map) return;
    if ($("#heat-toggle").checked) heatLayer.addTo(map); else map.removeLayer(heatLayer);
  };
  $("#topo-toggle").onchange = () => {
    if (!topologyLayer || !map) return;
    if ($("#topo-toggle").checked) topologyLayer.addTo(map); else map.removeLayer(topologyLayer);
  };
  $("#deploy-form").addEventListener("submit", (e) => { e.preventDefault(); deploy(e.target); });
  connect();
});
