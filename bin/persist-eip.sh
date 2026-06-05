#!/usr/bin/env bash
#
# persist-eip.sh - ensure a PERSISTENT Elastic IP for the Reconfigurator (RC) exists,
# created OUTSIDE the terraform config (like bin/persist-cert.sh's TLS bucket) so it
# survives `terraform destroy`. The multi-region config reads it via a `data "aws_eip"`
# (tag Name=xdn-rc-persist-eip) and associates it to the RC on every apply, so the RC
# keeps the SAME public IP across redeploys -- cp.xdnapp.com's nameserver glue never
# changes and the dashboard never hits a DNS-propagation window.
#
# Idempotent: prints the existing EIP, or allocates + tags a new one. Run once per
# account/region before the first `terraform apply`.
#
#   AWS_REGION=us-east-1 bin/persist-eip.sh
#
set -euo pipefail

REGION="${AWS_REGION:-us-east-1}"
TAG_NAME="${RC_EIP_TAG:-xdn-rc-persist-eip}"

existing=$(aws ec2 describe-addresses --region "$REGION" \
  --filters "Name=tag:Name,Values=${TAG_NAME}" \
  --query 'Addresses[0].PublicIp' --output text 2>/dev/null || true)

if [[ -n "${existing}" && "${existing}" != "None" ]]; then
  echo "Persistent RC EIP already exists in ${REGION}: ${existing} (tag Name=${TAG_NAME})"
  exit 0
fi

ip=$(aws ec2 allocate-address --region "$REGION" --domain vpc \
  --tag-specifications "ResourceType=elastic-ip,Tags=[{Key=Name,Value=${TAG_NAME}},{Key=xdn-persist,Value=true}]" \
  --query 'PublicIp' --output text)

echo "Allocated persistent RC EIP in ${REGION}: ${ip} (tag Name=${TAG_NAME})"
echo "Point the domain's nameserver glue at this IP (terraform does this via data.aws_eip.rc)."
