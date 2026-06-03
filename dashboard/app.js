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
let map, markerLayer;

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
    // Any HTTP response (even 404) means the TLS control plane is reachable.
    await api("/api/v2/services");
    setConn(true, `connected to ${controlPlane}`);
    await refreshList();
    if (currentSvc) inspect(currentSvc);
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

// ---- Service list ----------------------------------------------------------
async function refreshList() {
  const tbody = $("#services tbody");
  const note = $("#list-note");
  try {
    const r = await api("/api/v2/services");
    if (r.ok && r.body && Array.isArray(r.body.SERVICES)) {
      note.textContent = "";
      renderServices(r.body.SERVICES);
      return;
    }
    // Endpoint not deployed yet (Phase 1 backend pending) → graceful fallback.
    tbody.innerHTML = "";
    note.innerHTML =
      `<code>GET /api/v2/services</code> not available yet — deploy a service ` +
      `below, or load <code>?svc=NAME</code> to inspect a known one.`;
  } catch (e) {
    note.textContent = `Could not list services: ${e.message}`;
  }
}

function renderServices(services) {
  const tbody = $("#services tbody");
  tbody.innerHTML = "";
  if (!services.length) {
    tbody.innerHTML = `<tr><td colspan="4" class="muted">No services deployed.</td></tr>`;
    return;
  }
  for (const s of services) {
    const name = s.NAME || s.name;
    const tr = document.createElement("tr");
    tr.className = "clickable" + (name === currentSvc ? " selected" : "");
    tr.innerHTML =
      `<td class="mono">${esc(name)}</td>` +
      `<td>${s.EPOCH ?? s.epoch ?? ""}</td>` +
      `<td>${s.NUM_REPLICAS ?? s.num_replicas ?? ""}</td>` +
      `<td></td>`;
    tr.querySelector("td:first-child").onclick = () => inspect(name);
    const del = document.createElement("button");
    del.className = "danger";
    del.textContent = "Destroy";
    del.onclick = (ev) => { ev.stopPropagation(); destroy(name); };
    tr.querySelector("td:last-child").appendChild(del);
    tbody.appendChild(tr);
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
      currentSvc = cfg.name;
      syncUrl();
      setTimeout(() => { refreshList(); inspect(cfg.name); }, 1500);
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
      refreshList();
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
  $("#placement-svc").textContent = `· ${name}`;
  document.querySelectorAll("#services tbody tr").forEach((tr) =>
    tr.classList.toggle("selected", tr.firstChild?.textContent === name));
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
  } catch (e) {
    note.textContent = `Error loading placement: ${e.message}`;
  }
}

function clearPlacement() {
  $("#placement-svc").textContent = "";
  $("#placement-note").textContent = "";
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
  initMap();
  $("#connect").onclick = connect;
  $("#refresh").onclick = refreshList;
  $("#cp").addEventListener("keydown", (e) => { if (e.key === "Enter") connect(); });
  $("#deploy-form").addEventListener("submit", (e) => { e.preventDefault(); deploy(e.target); });
  connect();
});
