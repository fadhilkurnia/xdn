---
title: Dashboard
hide:
  - navigation
  - toc
---

# Dashboard

Use an existing XDN deployment from your browser. You can launch or destroy
services, see client geodistribution, and observe the inter-replica communication.
Alternatively, point this dashboard at your own XDN control plane host with the
Connect field below.

<style>
  /* Embed the dashboard SPA full-width under the site's header/footer/tabs. */
  .xdn-dash-frame {
    width: 100%;
    /* Tall enough to show the whole SPA (map + replica table + client emulator
       below it) without an inner scrollbar. */
    height: calc(100vh - 8rem);
    min-height: 1100px;
    border: 1px solid var(--md-default-fg-color--lightest);
    border-radius: 6px;
  }
</style>

<iframe class="xdn-dash-frame" src="app/?embed=1" title="XDN dashboard"
        loading="lazy" referrerpolicy="no-referrer"></iframe>
