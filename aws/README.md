# XDN on AWS

Terraform infrastructure and AMI builders for deploying XDN to EC2.

## Contents

| File | Purpose |
|------|---------|
| `main.tf` | VPC (dual-stack), subnets, security group, EC2 instances, RC Elastic IP, Route53 delegation of `xdnapp.com` to coredns, ACME DNS-01 delegation zone, the wildcard cert (Terraform `acme` provider) + its S3 store, and AR IAM |
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

Common overrides (see `ami_common.sh` for all): `ARCH`, `AWS_REGION`,
`REPO_BRANCH`, `BUILDER_INSTANCE_TYPE`, `BUILDER_VOLUME_SIZE`, `SUBNET_ID`.

### Architecture (x86-64 vs Graviton/arm64)

`ARCH` selects the AMI's CPU architecture (`amd64`, the default, or `arm64`).
It drives the base Ubuntu AMI, the Go toolchain download, and the builder
family — `coredns` and `docker-ce-cli` are compiled/installed **natively**, so
an `arm64` AMI is built on a Graviton builder (auto-defaults to `t4g.large`);
you cannot cross-compile them here. The Java jars are arch-neutral bytecode.

```bash
ARCH=arm64 ./create_rc_ami.sh    # Graviton RC AMI for t4g.* instances
```

The RC is a lean control-plane node (reconfigurator JVM + coredns; no Docker
daemon, no app containers, off the data path), so it's a good fit for the
smallest Graviton instance. `main.tf` exposes per-role sizing via
`rc_instance_type` (**default `t4g.micro`**, Graviton, ~$6/mo) and
`ar_instance_type` (default `t3.large`). The default `rc_ami` is the **arm64**
RC image; build/refresh it with `ARCH=arm64 ./create_rc_ami.sh` (clones
`fork/main`, which carries the IPv6 jar). The RC build also uses a smaller
16 GB root volume (vs the AR's 30 GB), matched by the RC's `root_block_device`,
so the AMI snapshot and each RC instance's gp3 volume are smaller and cheaper.

`t4g.micro` has only 1 GB RAM, so the RC JVM runs with a small (~256 MB) default
max heap. To keep that safe, `rc-userdata.tftpl` provisions a 1 GB **swapfile**
on boot, which absorbs boot/GC spikes (JVM + coredns + geo DB + OS) that would
otherwise risk an OOM-kill. If you want more headroom, bump to `t4g.small`
(2 GB) — same arm64 AMI, no rebuild:

```bash
terraform apply -var rc_instance_type=t4g.small
```

Avoid `t4g.nano` (0.5 GB) for the RC — even with swap, ~128 MB heap is too tight.
The ARs stay on `amd64` (`t3.large`) since they run Docker + heavy stateful
apps; only the RC AMI is built for `arm64`. (Note: an EBS *snapshot* is billed
by used blocks, not the provisioned volume size, so the 30→16 GB change mainly
trims each running RC's gp3 volume cost; the snapshot was already lean.)

The AMIs are config-free: systemd units (`xdn-rc`, `xdn-dns`, `xdn-ar`) stay
inert until launch-time `user_data` writes `/opt/xdn/conf/gigapaxos.properties`
(and `node-id` / `Corefile`). The AR's XDN jar must come from a branch that
includes the in-frontend TLS support, so build the AR AMI with
`REPO_BRANCH=<that branch>` (the frontend terminates HTTPS itself; see below).
At launch the AR also installs the AWS CLI and pulls the wildcard cert from S3.

## HTTPS (`https://<service>.xdnapp.com`)

The XDN ActiveReplica frontend **terminates TLS itself** on `:443` (Netty +
OpenSSL/BoringSSL), using a single **wildcard `*.xdnapp.com`** certificate — no
reverse-proxy hop. The wildcard covers every service, including ones created
later, with no per-service config. The frontend also runs a tiny listener on
`:80` that `308`-redirects to `https://`.

Relevant config:

- `main.tf` variables: `base_domain` (default `xdnapp.com`), `acme_email`
  (ACME account contact), `acme_ca` (ACME directory URL — see *Trust* below).
- Frontend flags (set per-AR in `ar-userdata.tftpl`, read by the patched
  `HttpActiveReplica`): `ENABLE_ACTIVE_REPLICA_HTTPS`, `ACTIVE_REPLICA_HTTPS_PORT`
  (443), `ACTIVE_REPLICA_TLS_CERT_CHAIN` / `ACTIVE_REPLICA_TLS_PRIVATE_KEY`
  (PEM paths under `/opt/xdn/conf/tls/`), `ENABLE_ACTIVE_REPLICA_HTTPS_REDIRECT`.

### Certificate lifecycle

The wildcard is **issued once by Terraform and reused** — instance replacement
never re-issues, so the Let's Encrypt rate limit is not consumed during normal
operation.

1. **Issue (once).** `terraform apply` runs the `acme_certificate.wildcard`
   resource (vancluever/acme provider), which obtains `*.<base_domain>` via an
   ACME **DNS-01** challenge using Terraform's own AWS credentials. The
   challenge `TXT` is written into the delegated `_acme-challenge.<base_domain>`
   Route53 zone (`aws_route53_zone.acme`); coredns is authoritative for the apex
   and **forwards** that subzone to Route53 (see `rc-userdata.tftpl` — the
   bundled coredns `file` plugin lacks `fallthrough` and `template` forces
   `AA=1`, so a `forward` block is used instead of an NS delegation). Let's
   Encrypt validates through that path. The cert + key are kept in **Terraform
   state** and written to a private, SSE-encrypted **S3 bucket**
   (`aws_s3_bucket.tls`, objects `wildcard/fullchain.pem` + `wildcard/privkey.pem`).
2. **Distribute (every boot).** Each AR has an instance profile granting
   `s3:GetObject` on that bucket. On launch (`ar-userdata.tftpl`) it installs the
   AWS CLI v2, pulls both PEMs to `/opt/xdn/conf/tls/`, and converts the key to
   **PKCS#8** (the acme provider may emit PKCS#1/SEC1, which Netty/JDK can't
   parse). The frontend then loads them and binds `:443`.
3. **Reuse (instance replacement).** Replacing an AR (or all of them) just
   re-pulls the **same** S3 object — **no ACME call, no rate-limit usage**. On
   `terraform apply`, `acme_certificate` is only refreshed (not re-created)
   unless it is within `min_days_remaining` (30 days) of expiry.
4. **Renew.** When inside the renewal window, `terraform apply` re-issues once
   and updates state + the S3 objects. Running ARs pick up the new cert via the
   baked **`xdn-tls-pull.timer`** (hourly): `xdn-tls-pull.sh` re-downloads, and
   on change restarts `xdn-ar` to reload the cert (Netty reads it at startup).
   So **renewal requires a `terraform apply`** (or AR replacement) to mint the
   new cert; the timer only propagates it.
5. **Trust (staging vs production).** `acme_ca` selects the CA:
   - **Staging** (`https://acme-staging-v02.api.letsencrypt.org/directory`) —
     **not browser-trusted**, but high rate limits; use it while iterating.
   - **Production** (`https://acme-v02.api.letsencrypt.org/directory`, the
     default) — browser-trusted. Limit: **5 certs per exact identifier set per
     week**. Because the cert is reused from S3, normal AR churn never hits this;
     only repeated *fresh* issuances do.
   Switching `acme_ca` changes the ACME server, so the next apply **re-issues**
   against the new CA (counts toward that CA's limits).

### Verify after `apply`

```bash
dig +short NS _acme-challenge.xdnapp.com @<rc_elastic_ip>   # delegated Route53 NS (see acme_delegation_ns output)
xdn launch demo --image=fadhilkurnia/xdn-bookcatalog --port=80 --consistency=linearizable --deterministic=true --state=/app/data/
curl -v  https://demo.xdnapp.com/api/books                  # served by Java on :443 (prod CA -> trusted)
curl -sI http://demo.xdnapp.com/api/books                   # 308 redirect to https
# on an AR:
sudo ss -tlnp | grep -E ':80 |:443 '                        # both owned by 'java'
sudo journalctl -u xdn-ar | grep -iE 'ready on|redirect ready|TLS provider'
```

## Deploy infrastructure

Plug the AMI ids into `main.tf`, then:

```bash
terraform init
terraform plan
terraform apply
```

## Notes

- Terraform state is git-ignored; do not commit `*.tfstate`. It now also holds
  the wildcard **private key** (the `acme_certificate`/`tls_private_key`
  resources) — keep state in a private/encrypted backend.
- The RC needs a stable IP (`aws_eip.rc`) because it serves DNS for `xdnapp.com`.
- Builders default to a 30 GB root volume; the base AMI's ~8 GB is too small.
- The wildcard is issued **once** and reused from S3, so AR replacement does not
  consume the LE rate limit (see *Certificate lifecycle*). Only re-issuance
  (renewal, or switching `acme_ca`) counts toward the limit — iterate on staging.
- The DNS-01 challenge runs from wherever `terraform apply` runs, using its AWS
  credentials (needs Route53 write on the `_acme-challenge` zone) and reaching
  coredns on the RC; on a fully cold bring-up where coredns isn't serving yet,
  the `acme_certificate` step may need a re-apply.
