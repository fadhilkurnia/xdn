#!/usr/bin/env bash
# Persist the freshly-issued wildcard TLS cert into the PERSISTENT S3 bucket, so
# every cluster destroy/redeploy reuses it WITHOUT re-issuing via ACME (Let's
# Encrypt enforces a hard 5-certs-per-week limit on the exact *.xdnapp.com set --
# burning it leaves the cluster with no browser-trusted cert until it resets).
#
# The persistent bucket (xdn-tls-persist-<account>) lives OUTSIDE the Terraform
# config, so `terraform destroy` never touches it. The RC + ARs always pull the
# cert from it (see local.persist_tls_bucket in deployment/aws/main.tf).
#
# Renewal workflow (~every 60 days, before the cert's notAfter):
#   1. cd aws && terraform apply -var issue_cert=true   # issues a fresh cert into
#                                                        # the transient cluster bucket
#   2. bin/persist-cert.sh                               # copy it into the persistent bucket
#   3. cd aws && terraform apply                         # issue_cert defaults false;
#                                                        # drops the transient ACME
#                                                        # resources (state-rm'd, no revoke)
set -euo pipefail
export AWS_REGION="${AWS_REGION:-us-east-1}" AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
cd "$(dirname "$0")/../aws"

acct="$(aws sts get-caller-identity --query Account --output text)"
persist="xdn-tls-persist-${acct}"
cluster="$(terraform state show 'aws_s3_bucket.tls[0]' 2>/dev/null | awk -F'"' '/ bucket +=/ {print $2; exit}')"

if [ -z "${cluster}" ]; then
  echo "error: no transient cluster cert bucket in state." >&2
  echo "       run 'cd aws && terraform apply -var issue_cert=true' first to issue a cert." >&2
  exit 1
fi

echo "cluster (transient) bucket: ${cluster}"
echo "persistent bucket:          ${persist}"
for f in wildcard/fullchain.pem wildcard/privkey.pem; do
  aws s3 cp "s3://${cluster}/${f}" "s3://${persist}/${f}" --quiet
  echo "  persisted ${f}"
done

echo
echo "Cert persisted. Verify issuer/expiry:"
aws s3 cp "s3://${persist}/wildcard/fullchain.pem" - --quiet | openssl x509 -noout -issuer -enddate -subject
echo
echo "Next: 'cd aws && terraform apply' (issue_cert defaults false) to drop the transient ACME resources without re-issuing."
