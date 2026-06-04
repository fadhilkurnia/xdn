# Dashboard

The **XDN dashboard** is a lightweight, stateless web app for operating and
observing an XDN control plane from the browser — deploy and destroy services,
see where replicas are placed, watch the geo-demand heatmap, and view the full
cluster topology (active replicas + candidate locations).

[Open the dashboard :material-open-in-new:](/dashboard/){ .md-button .md-button--primary }

It talks directly to a Reconfigurator's TLS control-plane API (CORS-enabled), so
no server of its own is needed. Point it at your control plane with the
`?cp=host:port` URL parameter, or use the in-page **Connect** field.

!!! note "Live data needs a running control plane"
    The dashboard is a client; it only shows live data when it can reach a
    control plane (default `cp.xdnapp.com:3400`) whose TLS certificate your
    browser trusts. The homepage and documentation are fully static and always
    available.
