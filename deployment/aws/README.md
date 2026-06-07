# XDN on AWS

Terraform + AMI builders for deploying XDN to EC2, in two flavors.

| Folder | Scope |
|--------|-------|
| [`useast1/`](useast1/) | **Single-region** cluster — RC + ARs all in `us-east-1`. The original setup, and home to the shared AMI builders and cloud-init templates. |
| [`multiregion/`](multiregion/) | **Multi-region** — one RC plus ARs spread across several AWS regions for true geo-distribution. |

Start with `useast1/` (see its README for AMI build, HTTPS/wildcard-cert, and
cost controls). `multiregion/` layers a multi-region topology on top, **reusing
`useast1/`'s assets** rather than duplicating them:

- AMIs built by `useast1/create_rc_ami.sh` / `create_ar_ami.sh` (shared
  `ami_common.sh`); the AR AMI is copied into the other regions.
- launch-time cloud-init `useast1/rc-userdata.tftpl` / `ar-userdata.tftpl`,
  referenced from `multiregion/` as `../useast1/*.tftpl`.

Because of that reuse, keep `multiregion/` nested beside `useast1/` under `aws/`
(its Terraform `templatefile` paths point at `../useast1/`).
