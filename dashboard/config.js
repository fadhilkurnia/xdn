// Per-deployment defaults for the XDN dashboard. Edit this file (not app.js)
// when hosting the dashboard for a specific cluster. Everything here is just a
// default — the ?cp= URL param and the on-page input override it, so the page
// itself stays stateless.
window.XDN_DASHBOARD_CONFIG = {
  // Control-plane host of the XDN Reconfigurator. The TLS API port (:3400) is an
  // internal detail the dashboard assumes, so only the host is configured/shown.
  defaultControlPlane: "cp.xdnapp.com",
};
