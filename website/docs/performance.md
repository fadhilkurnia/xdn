---
title: Performance
hide:
  - navigation
  - toc
---

# Performance

Nightly performance measurements of XDN: low-load latency and max throughput
for Paxos active replication and primary-backup replication, with a per-stage
latency breakdown of the request flow. Measured by the
[nightly-perf workflow](https://github.com/fadhilkurnia/xdn/actions/workflows/nightly-perf.yml)
as an interleaved A/B of each night's HEAD against the last measured commit.

<style>
  /* Embed the perf trend page full-width under the site's header/footer/tabs,
     same treatment as the dashboard SPA. */
  .xdn-perf-frame {
    width: 100%;
    height: calc(100vh - 11rem);
    min-height: 720px;
    border: 1px solid var(--md-default-fg-color--lightest);
    border-radius: 6px;
  }
</style>

<iframe class="xdn-perf-frame" src="app/" title="XDN nightly performance"
        loading="lazy" referrerpolicy="no-referrer"></iframe>
