# XDN on AWS

Terraform infrastructure and AMI builders for deploying XDN to EC2.

## Contents

| File | Purpose |
|------|---------|
| `main.tf` | VPC (dual-stack), subnets, security group, EC2 instances, RC Elastic IP, Route53 delegation of `xdnapp.com` to coredns |
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

The AMIs are config-free: systemd units (`xdn-rc`, `xdn-dns`, `xdn-ar`) stay
inert until launch-time `user_data` writes `/opt/xdn/conf/gigapaxos.properties`
(and `node-id` / `Corefile`).

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
