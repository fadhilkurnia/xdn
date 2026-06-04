/* XDN Dashboard — stateless client. Talks directly to the XDN Reconfigurator's
 * TLS control-plane API. The address bar (?cp=, ?svc=) is the only state. */
"use strict";

const DEFAULT_CP =
  (window.XDN_DASHBOARD_CONFIG && window.XDN_DASHBOARD_CONFIG.defaultControlPlane) ||
  "cp.xdnapp.com:3400";

const $ = (sel) => document.querySelector(sel);
const params = new URLSearchParams(location.search);

let controlPlane = params.get("cp") || DEFAULT_CP;
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
const base = () => `https://${controlPlane}`;

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
  controlPlane = $("#cp").value.trim() || DEFAULT_CP;
  syncUrl();
  setConn(false, "connecting…");
  try {
    // Any HTTP response means the TLS control plane is reachable. (We probe the
    // placement route for a throwaway name; the list endpoint is intentionally
    // not used — customers inspect a service by its known name.)
    await api("/api/v2/services/__probe__/placement");
    setConn(true, `connected to ${controlPlane}`);
    fetchTopology();
    if (currentSvc) { $("#svc-name").value = currentSvc; inspect(currentSvc); }
  } catch (e) {
    setConn(false, `cannot reach ${controlPlane}`);
    log(
      `Connection failed: ${e.message}. If using a Let's Encrypt staging cert, ` +
      `open https://${controlPlane}/api/v2/services in a new tab and accept the ` +
      `certificate, then Connect again.`,
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

function renderReplicas(nodes) {
  const tbody = $("#replicas tbody");
  tbody.innerHTML = "";
  for (const n of nodes) {
    const role = (n.ROLE || "").toLowerCase();
    const isLeader = role.includes("leader") || role.includes("coordinator");
    const geo = n.GEOLOCATION
      ? `${n.GEOLOCATION.LATITUDE.toFixed(2)}, ${n.GEOLOCATION.LONGITUDE.toFixed(2)}`
      : "—";
    const tr = document.createElement("tr");
    tr.innerHTML =
      `<td class="mono">${esc(n.ID)}</td>` +
      `<td><span class="pill ${isLeader ? "leader" : "replica"}">${esc(n.ROLE || "replica")}</span></td>` +
      `<td class="mono">${esc(n.HTTP_ADDRESS || n.ADDRESS || "")}</td>` +
      `<td class="mono">${geo}</td>` +
      `<td class="muted">${esc(n.METADATA || "")}</td>`;
    tbody.appendChild(tr);
  }
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

// ---- Cluster topology (all candidate locations + active replicas) ----------
// GET /api/v2/nodes -> [{id, lat, lon, active}]. Active replicas render as solid
// markers; configured-but-idle candidate locations render as hollow/dashed markers,
// so the map shows every potential AR site alongside where replicas actually run.
async function fetchTopology() {
  try {
    const r = await api("/api/v2/nodes");
    if (r.ok && Array.isArray(r.body)) drawTopology(r.body);
    else log(`Could not load cluster topology (HTTP ${r.status}).`, true);
  } catch (e) {
    log(`Could not load cluster topology: ${e.message}`, true);
  }
}

function drawTopology(nodes) {
  if (!topologyLayer) return;
  topologyLayer.clearLayers();
  const pts = [];
  let active = 0, candidate = 0;
  for (const n of nodes) {
    if (typeof n.lat !== "number" || typeof n.lon !== "number") continue;
    pts.push([n.lat, n.lon]);
    if (n.active) {
      active++;
      L.circleMarker([n.lat, n.lon], {
        radius: 8, color: "#fff", weight: 2,
        fillColor: getCSS("--replica"), fillOpacity: 0.9,
      }).bindTooltip(`AR ${esc(n.id)} — active replica`).addTo(topologyLayer);
    } else {
      candidate++;
      L.circleMarker([n.lat, n.lon], {
        radius: 6, color: getCSS("--muted"), weight: 2, dashArray: "3",
        fillColor: "#fff", fillOpacity: 0.15,
      }).bindTooltip(`${esc(n.id)} — candidate location`).addTo(topologyLayer);
    }
  }
  const count = $("#topo-count");
  if (count) count.textContent = ` · ${active} active, ${candidate} candidate`;
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
