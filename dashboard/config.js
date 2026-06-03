// Per-deployment defaults for the XDN dashboard. Edit this file (not app.js)
// when hosting the dashboard for a specific cluster. Everything here is just a
// default — the ?cp= URL param and the on-page input override it, so the page
// itself stays stateless.
window.XDN_DASHBOARD_CONFIG = {
  // Control-plane endpoint (host:port) of the XDN Reconfigurator's TLS API.
  defaultControlPlane: "cp.xdnapp.com:3400",
};
