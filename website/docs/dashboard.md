---
title: Dashboard
hide:
  - navigation
  - toc
---

# Dashboard

Operate and observe an XDN control plane from the browser — deploy/destroy
services, see replica placement, the geo-demand heatmap, and the full cluster
topology. Point it at your control plane with the in-frame **Connect** field (or
[open it full-screen](app/)).

<style>
  /* Embed the dashboard SPA full-width under the site's header/footer/tabs. */
  .xdn-dash-frame {
    width: 100%;
    height: calc(100vh - 11rem);
    min-height: 720px;
    border: 1px solid var(--md-default-fg-color--lightest);
    border-radius: 6px;
  }
</style>

<iframe class="xdn-dash-frame" src="app/?embed=1" title="XDN dashboard"
        loading="lazy" referrerpolicy="no-referrer"></iframe>
