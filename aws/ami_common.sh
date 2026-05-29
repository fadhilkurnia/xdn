#!/usr/bin/env bash
# ami_common.sh - shared AWS plumbing for the XDN AMI builders.
#
# This file is sourced by create_rc_ami.sh and create_ar_ami.sh. It owns the
# generic "launch a builder, provision it, snapshot an AMI, terminate" flow.
# Each role-specific entrypoint sets a few variables and defines
# emit_provision_script() (the bash to run on the builder), then calls
# run_ami_build.
#
# Nothing here is XDN-role-specific except the shared provisioning emitter
# (emit_common_provision), which both roles reuse.

set -euo pipefail

# ---------------------------------------------------------------------------
# Configurable knobs (override via environment before running the entrypoint).
# ---------------------------------------------------------------------------
AWS_REGION="${AWS_REGION:-us-east-1}"
BASE_AMI="${BASE_AMI:-}"                       # empty -> resolve latest Ubuntu 24.04 amd64
BUILDER_INSTANCE_TYPE="${BUILDER_INSTANCE_TYPE:-t3.large}"
BUILDER_VOLUME_SIZE="${BUILDER_VOLUME_SIZE:-30}"   # GB, gp3 (8GB default is too small)
KEY_NAME="${KEY_NAME:-xdn-aws-key}"
SSH_KEY_PATH="${SSH_KEY_PATH:-$HOME/.ssh/xdn-aws-key}"
SUBNET_ID="${SUBNET_ID:-}"                     # empty -> first public subnet in default VPC
REPO_URL="${REPO_URL:-https://github.com/fadhilkurnia/xdn.git}"
REPO_BRANCH="${REPO_BRANCH:-main}"
GO_VERSION="${GO_VERSION:-1.23.2}"             # satisfies coredns (go.mod) AND xdn-cli
SSH_USER="${SSH_USER:-ubuntu}"

# State populated during the run (used by teardown).
BUILDER_ID=""
BUILDER_SG_ID=""
BUILDER_IP=""

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33m[warn]\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31m[err]\033[0m %s\n' "$*" >&2; exit 1; }

aws_ec2() { aws ec2 --region "$AWS_REGION" "$@"; }

# ---------------------------------------------------------------------------
# preflight - verify local tooling and credentials.
# ---------------------------------------------------------------------------
preflight() {
  log "Preflight checks"
  for bin in aws ssh; do
    command -v "$bin" >/dev/null 2>&1 || die "'$bin' not found in PATH"
  done
  aws sts get-caller-identity >/dev/null 2>&1 \
    || die "AWS credentials not configured (aws sts get-caller-identity failed)"
  [[ -f "$SSH_KEY_PATH" ]] || die "SSH private key not found: $SSH_KEY_PATH"
  aws_ec2 describe-key-pairs --key-names "$KEY_NAME" >/dev/null 2>&1 \
    || die "EC2 key pair '$KEY_NAME' not found in $AWS_REGION"
}

# ---------------------------------------------------------------------------
# resolve_base_ami - latest Ubuntu 24.04 amd64 via Canonical's public SSM param.
# ---------------------------------------------------------------------------
resolve_base_ami() {
  if [[ -n "$BASE_AMI" ]]; then
    log "Using provided base AMI: $BASE_AMI"
    return
  fi
  log "Resolving latest Ubuntu 24.04 amd64 AMI via SSM"
  BASE_AMI=$(aws ssm get-parameter --region "$AWS_REGION" \
    --name /aws/service/canonical/ubuntu/server/24.04/stable/current/amd64/hvm/ebs-gp3/ami-id \
    --query 'Parameter.Value' --output text)
  [[ "$BASE_AMI" == ami-* ]] || die "Failed to resolve base AMI (got: '$BASE_AMI')"
  log "Base AMI: $BASE_AMI"
}

# ---------------------------------------------------------------------------
# resolve_subnet - pick a public subnet in the default VPC if none was given.
# ---------------------------------------------------------------------------
resolve_subnet() {
  if [[ -n "$SUBNET_ID" ]]; then
    log "Using provided subnet: $SUBNET_ID"
    return
  fi
  log "Resolving a public subnet in the default VPC"
  local vpc_id
  vpc_id=$(aws_ec2 describe-vpcs --filters Name=isDefault,Values=true \
    --query 'Vpcs[0].VpcId' --output text)
  [[ "$vpc_id" == vpc-* ]] || die "No default VPC found; set SUBNET_ID explicitly"
  SUBNET_ID=$(aws_ec2 describe-subnets \
    --filters "Name=vpc-id,Values=$vpc_id" "Name=map-public-ip-on-launch,Values=true" \
    --query 'Subnets[0].SubnetId' --output text)
  [[ "$SUBNET_ID" == subnet-* ]] \
    || die "No public subnet in default VPC; set SUBNET_ID explicitly"
  VPC_ID="$vpc_id"
  log "Subnet: $SUBNET_ID (vpc $VPC_ID)"
}

# ---------------------------------------------------------------------------
# create_builder_sg - temp SG allowing SSH only from this machine's public IP.
# ---------------------------------------------------------------------------
create_builder_sg() {
  log "Creating temporary security group for the builder"
  local my_ip vpc_arg
  my_ip=$(curl -s --max-time 10 https://checkip.amazonaws.com || true)
  [[ -n "$my_ip" ]] || die "Could not determine this machine's public IP for SSH ingress"
  # Place the SG in the same VPC as the subnet.
  local vpc_id="${VPC_ID:-}"
  if [[ -z "$vpc_id" ]]; then
    vpc_id=$(aws_ec2 describe-subnets --subnet-ids "$SUBNET_ID" \
      --query 'Subnets[0].VpcId' --output text)
  fi
  vpc_arg=(--vpc-id "$vpc_id")
  BUILDER_SG_ID=$(aws_ec2 create-security-group \
    --group-name "xdn-ami-builder-${ROLE}-$$" \
    --description "Temporary SG for XDN ${ROLE} AMI builder" \
    "${vpc_arg[@]}" --query 'GroupId' --output text)
  aws_ec2 authorize-security-group-ingress --group-id "$BUILDER_SG_ID" \
    --protocol tcp --port 22 --cidr "${my_ip}/32" >/dev/null
  log "Security group: $BUILDER_SG_ID (SSH from ${my_ip}/32)"
}

# ---------------------------------------------------------------------------
# launch_builder - run a builder instance with an enlarged gp3 root volume.
# ---------------------------------------------------------------------------
launch_builder() {
  log "Launching builder ($BUILDER_INSTANCE_TYPE, ${BUILDER_VOLUME_SIZE}GB gp3)"
  local bdm
  bdm="[{\"DeviceName\":\"/dev/sda1\",\"Ebs\":{\"VolumeSize\":${BUILDER_VOLUME_SIZE},\"VolumeType\":\"gp3\",\"DeleteOnTermination\":true}}]"
  BUILDER_ID=$(aws_ec2 run-instances \
    --image-id "$BASE_AMI" \
    --instance-type "$BUILDER_INSTANCE_TYPE" \
    --key-name "$KEY_NAME" \
    --security-group-ids "$BUILDER_SG_ID" \
    --subnet-id "$SUBNET_ID" \
    --associate-public-ip-address \
    --block-device-mappings "$bdm" \
    --tag-specifications \
      "ResourceType=instance,Tags=[{Key=Name,Value=xdn-ami-builder-${ROLE}}]" \
    --query 'Instances[0].InstanceId' --output text)
  [[ "$BUILDER_ID" == i-* ]] || die "Failed to launch builder instance"
  log "Builder instance: $BUILDER_ID"
}

# ---------------------------------------------------------------------------
# wait_ssh - wait for running state, then for SSH to actually answer.
# ---------------------------------------------------------------------------
wait_ssh() {
  log "Waiting for instance to enter running state"
  aws_ec2 wait instance-running --instance-ids "$BUILDER_ID"
  BUILDER_IP=$(aws_ec2 describe-instances --instance-ids "$BUILDER_ID" \
    --query 'Reservations[0].Instances[0].PublicIpAddress' --output text)
  [[ -n "$BUILDER_IP" && "$BUILDER_IP" != "None" ]] || die "Builder has no public IP"
  log "Builder public IP: $BUILDER_IP"
  log "Waiting for SSH to respond"
  for _ in $(seq 1 60); do
    if ssh -i "$SSH_KEY_PATH" -o BatchMode=yes -o ConnectTimeout=5 \
        -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null \
        "${SSH_USER}@${BUILDER_IP}" true 2>/dev/null; then
      log "SSH is up"
      return
    fi
    sleep 5
  done
  die "SSH did not come up within ~5 minutes"
}

# ssh_remote <cmd...> - run an interactive-ish command on the builder.
ssh_remote() {
  ssh -i "$SSH_KEY_PATH" -o BatchMode=yes -o StrictHostKeyChecking=no \
    -o UserKnownHostsFile=/dev/null "${SSH_USER}@${BUILDER_IP}" "$@"
}

# run_remote_script - pipe the composed provisioning script to the builder and
# execute it with strict bash. Streams remote output to this terminal.
run_remote_script() {
  log "Provisioning the builder (this takes several minutes)"
  emit_provision_script | ssh_remote "cat > /tmp/provision.sh && bash /tmp/provision.sh"
}

# ---------------------------------------------------------------------------
# Shared provisioning emitters (printed locally, executed on the builder).
# GO_VERSION/REPO_URL/REPO_BRANCH are interpolated here so the remote script
# carries literal values; \$HOME etc. stay literal for remote expansion.
# ---------------------------------------------------------------------------
emit_header() {
  cat <<EOF
set -euo pipefail
export DEBIAN_FRONTEND=noninteractive
export PATH="\$HOME/.cargo/bin:/usr/local/go/bin:\$PATH"
echo ">> provisioning XDN ${ROLE} builder"
EOF
}

emit_common_provision() {
  cat <<EOF
# --- base dependencies (Java + Ant + Git) ---
sudo apt-get update
sudo apt-get install -y openjdk-21-jdk ant git rsync wget curl

# --- Go ${GO_VERSION} (NOT 1.22.4 from bin/xdnd; coredns needs 1.23+) ---
wget -q "https://go.dev/dl/go${GO_VERSION}.linux-amd64.tar.gz"
sudo rm -rf /usr/local/go
sudo tar -C /usr/local -xzf "go${GO_VERSION}.linux-amd64.tar.gz"
rm -f "go${GO_VERSION}.linux-amd64.tar.gz"

# --- clone source and build the jars ---
rm -rf "\$HOME/xdn"
git clone --depth 1 --branch "${REPO_BRANCH}" "${REPO_URL}" "\$HOME/xdn"
cd "\$HOME/xdn" && ./bin/build_xdn_jar.sh

# --- staging layout shared by both roles ---
sudo mkdir -p /opt/xdn/jars /opt/xdn/conf /opt/xdn/bin /opt/xdn/geo
sudo cp "\$HOME"/xdn/jars/*.jar /opt/xdn/jars/
sudo cp "\$HOME"/xdn/conf/keyStore.jks "\$HOME"/xdn/conf/trustStore.jks \\
        "\$HOME"/xdn/conf/logging.properties "\$HOME"/xdn/conf/log4j.properties \\
        /opt/xdn/conf/
EOF
}

# emit_prestage_helper - reboot-safe /tmp staging (geo DB + state dirs).
emit_prestage_helper() {
  cat <<'EOF'
# --- boot helper: recreate volatile /tmp dirs and stage the geo DB ---
sudo tee /opt/xdn/bin/xdn-prestage.sh >/dev/null <<'PRESTAGE'
#!/usr/bin/env bash
set -e
mkdir -p /tmp/geo /tmp/gigapaxos /tmp/xdn
if [ -f /opt/xdn/geo/geolocation_city_data.mmdb ] \
   && [ ! -f /tmp/geo/geolocation_city_data.mmdb ]; then
  cp /opt/xdn/geo/geolocation_city_data.mmdb /tmp/geo/
fi
PRESTAGE
sudo chmod +x /opt/xdn/bin/xdn-prestage.sh
EOF
}

# emit_finalize <unit-names> - reload + enable units (kept inert by
# ConditionPathExists until launch-time config arrives).
emit_finalize() {
  local units="$1"
  cat <<EOF
sudo systemctl daemon-reload
sudo systemctl enable ${units}
echo ">> provisioning complete for ${ROLE}"
EOF
}

# ---------------------------------------------------------------------------
# common_cleanup - shrink + de-identify the image before snapshotting.
# ---------------------------------------------------------------------------
common_cleanup() {
  log "Cleaning up the builder before imaging"
  ssh_remote 'bash -s' <<'EOF'
set -e
# stop anything we may have started; ignore if not present
sudo systemctl stop xdn-rc xdn-dns xdn-ar 2>/dev/null || true
# drop the source clone and build caches
rm -rf "$HOME/xdn" "$HOME/go" "$HOME/.cargo" "$HOME/.rustup" 2>/dev/null || true
go clean -cache -modcache 2>/dev/null || true
sudo apt-get clean
sudo rm -rf /var/lib/apt/lists/* /tmp/provision.sh
sudo find /var/log -type f -exec truncate -s0 {} + 2>/dev/null || true
# fresh identity for every instance launched from this AMI
sudo truncate -s0 /etc/machine-id
sudo rm -f /var/lib/dbus/machine-id
sudo rm -f /etc/ssh/ssh_host_*
sudo rm -f "$HOME/.bash_history"
EOF
}

# ---------------------------------------------------------------------------
# bake_image - stop the builder, create the AMI, wait for availability.
# ---------------------------------------------------------------------------
bake_image() {
  local ami_name="$1"
  common_cleanup
  log "Stopping builder before snapshot"
  aws_ec2 stop-instances --instance-ids "$BUILDER_ID" >/dev/null
  aws_ec2 wait instance-stopped --instance-ids "$BUILDER_ID"
  log "Creating AMI: $ami_name"
  AMI_ID=$(aws_ec2 create-image --instance-id "$BUILDER_ID" \
    --name "$ami_name" \
    --description "XDN ${ROLE} node ($REPO_BRANCH)" \
    --query 'ImageId' --output text)
  [[ "$AMI_ID" == ami-* ]] || die "create-image failed"
  log "Waiting for AMI $AMI_ID to become available"
  aws_ec2 wait image-available --image-ids "$AMI_ID"
  local script_dir
  script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
  printf '%s\t%s\t%s\n' "$ami_name" "$AMI_ID" "$AWS_REGION" \
    >> "$script_dir/last-built-amis.txt"
  log "AMI ready: $AMI_ID"
}

# ---------------------------------------------------------------------------
# teardown - terminate builder + delete temp SG. Idempotent; wired to traps.
# ---------------------------------------------------------------------------
teardown() {
  local rc=$?
  trap - EXIT ERR INT TERM
  if [[ -n "$BUILDER_ID" ]]; then
    log "Terminating builder $BUILDER_ID"
    aws_ec2 terminate-instances --instance-ids "$BUILDER_ID" >/dev/null 2>&1 || true
    aws_ec2 wait instance-terminated --instance-ids "$BUILDER_ID" 2>/dev/null || true
  fi
  if [[ -n "$BUILDER_SG_ID" ]]; then
    log "Deleting security group $BUILDER_SG_ID"
    # brief retry: ENI detachment can lag instance termination
    for _ in $(seq 1 12); do
      if aws_ec2 delete-security-group --group-id "$BUILDER_SG_ID" 2>/dev/null; then
        break
      fi
      sleep 5
    done
  fi
  if [[ $rc -ne 0 ]]; then
    warn "Build failed (exit $rc); cleaned up builder resources."
  fi
  exit $rc
}

# ---------------------------------------------------------------------------
# run_ami_build - top-level orchestration used by both entrypoints.
# Requires: ROLE, AMI_NAME_PREFIX set, emit_provision_script() defined.
# ---------------------------------------------------------------------------
run_ami_build() {
  : "${ROLE:?ROLE must be set by the entrypoint}"
  : "${AMI_NAME_PREFIX:?AMI_NAME_PREFIX must be set by the entrypoint}"
  local stamp ami_name
  stamp="$(date -u +%Y%m%d-%H%M%S)"   # captured once; stable for this run
  ami_name="${AMI_NAME_PREFIX}-${stamp}"

  preflight
  resolve_base_ami
  resolve_subnet
  trap teardown EXIT ERR INT TERM
  create_builder_sg
  launch_builder
  wait_ssh
  run_remote_script
  bake_image "$ami_name"

  log "Done. ${ROLE^^} AMI: $AMI_ID  (name: $ami_name, region: $AWS_REGION)"
  log "Plug this id into aws/main.tf for the ${ROLE^^} instance(s)."
}
