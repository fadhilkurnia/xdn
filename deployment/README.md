# XDN Deployment

Infrastructure for deploying XDN, organized by backend.

| Folder | Backend | Nodes | Lifetime |
|--------|---------|-------|----------|
| [`aws/`](aws/) | AWS (Terraform / EC2) | Managed cloud VMs | Persistent |
| [`cloudlab/`](cloudlab/) | [CloudLab](https://www.cloudlab.us/) (geni-lib) | Bare-metal, multi-site | Ephemeral (slivers expire) |

- **aws/** — Terraform configs (`useast1/` single-region and `multiregion/`)
  plus AMI builders. Persistent VMs with a stable RC Elastic IP and a wildcard
  `*.xdnapp.com` TLS cert. See [`aws/README.md`](aws/README.md).
- **cloudlab/** — geni-lib tooling to list, start, and tear down bare-metal raw
  PCs across CloudLab clusters (Utah, Clemson, Wisconsin, Apt, UMass). Real WAN
  topology, but slivers are time-limited. See [`cloudlab/README.md`](cloudlab/README.md).

Each subfolder has its own README with setup and usage.
