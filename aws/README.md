# XDN on AWS

Terraform infrastructure and AMI builders for deploying XDN to EC2.

## Contents

| File | Purpose |
|------|---------|
| `main.tf` | VPC (dual-stack), subnets, security group, EC2 instances, RC Elastic IP, Route53 delegation of `xdnapp.com` to coredns, ACME DNS-01 delegation zone + AR IAM for HTTPS |
| `create_rc_ami.sh` | Build an AMI for the Reconfigurator (RC) + coredns nameserver |
| `create_ar_ami.sh` | Build an AMI for the ActiveReplica (AR) edge node |
| `ami_common.sh` | Shared AWS plumbing sourced by the two builders (not run directly) |

## Prerequisites

- AWS CLI configured (`aws sts get-caller-identity` works), region `us-east-1`
- An EC2 key pair `xdn-aws-key` with the private key at `~/.ssh/xdn-aws-key`
- Terraform `>= 1.x` with the AWS provider

## Build AMIs

Each script launches a temporary builder, compiles XDN, snapshots an AMI, then
terminates the builder. The new AMI id is printed and appended to
`last-built-amis.txt`.

```bash
./create_rc_ami.sh
./create_ar_ami.sh
```

Common overrides (see `ami_common.sh` for all): `AWS_REGION`, `REPO_BRANCH`,
`BUILDER_INSTANCE_TYPE`, `BUILDER_VOLUME_SIZE`, `SUBNET_ID`.

The AMIs are config-free: systemd units (`xdn-rc`, `xdn-dns`, `xdn-ar`, `caddy`)
stay inert until launch-time `user_data` writes `/opt/xdn/conf/gigapaxos.properties`
(and `node-id` / `Corefile` / `Caddyfile`). The AR AMI also bakes in a Caddy
build that bundles the Route53 DNS-01 provider. **Rebuild the AR AMI** to pick
up the Caddy layer.

## HTTPS (`https://<service>.xdnapp.com`)

Clients reach services over TLS, terminated by **Caddy on each AR** with a single
**wildcard `*.xdnapp.com`** certificate from Let's Encrypt; Caddy reverse-proxies
to the local XDN frontend (`127.0.0.1:2300`). The wildcard covers every service,
including ones created later, with no per-service config.

How issuance works (DNS-01, the only challenge that yields wildcards and the only
one robust to the cluster's geo-DNS):

1. `_acme-challenge.xdnapp.com` is a small **Route53 hosted zone** (`aws_route53_zone.acme`).
2. coredns (the `file` plugin, ahead of `xdn`) serves an **NS referral** for that
   name to Route53; everything else falls through to `xdn`'s geo-routing.
3. Each AR's Caddy writes the challenge TXT into that Route53 zone using its
   **EC2 instance profile** (`route53:ChangeResourceRecordSets`, scoped to the
   zone — no static keys). Let's Encrypt follows the referral to read it.

Relevant `main.tf` variables: `base_domain` (default `xdnapp.com`), `acme_email`,
and `acme_ca`. **While testing, set `acme_ca` to the LE staging endpoint**
(`https://acme-staging-v02.api.letsencrypt.org/directory`) to avoid the strict
production rate limits; the certs won't be browser-trusted but the full path is
exercised. `ENABLE_ACTIVE_REPLICA_HTTP_PORT_80` is set to `false` so the XDN
frontend uses its offset clear port (2300) and Caddy can own :80/:443.

Verify after `apply`:

```bash
dig +short NS _acme-challenge.xdnapp.com @<rc_elastic_ip>   # -> the Route53 NS (see acme_delegation_ns output)
xdn ... create bookcatalog ...                              # create a service
curl -v  https://bookcatalog.xdnapp.com/                    # valid LE chain
curl -sI http://bookcatalog.xdnapp.com/                     # 308/301 redirect to https
journalctl -u caddy -f                                      # (on an AR) watch issuance
```

## Deploy infrastructure

Plug the AMI ids into `main.tf`, then:

```bash
terraform init
terraform plan
terraform apply
```

## Notes

- Terraform state is git-ignored; do not commit `*.tfstate` (may contain secrets).
- The RC needs a stable IP (`aws_eip.rc`) because it serves DNS for `xdnapp.com`.
- Builders default to a 30 GB root volume; the base AMI's ~8 GB is too small.
- Caddy issues per-AR (each AR gets its own copy of the wildcard). Instance
  replacement re-issues; mind the LE rate limit (5 duplicate certs/week) when
  churning many ARs on the production CA — use the staging CA while iterating.
