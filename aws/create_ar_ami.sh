#!/usr/bin/env bash
# create_ar_ami.sh - build an AWS AMI for the XDN ActiveReplica (AR) node.
#
# The AR role is heavy: Java 21 + gigapaxos jars + the xdn CLI + the FUSE
# recorders (fuselog/fuserust, C++/Rust) + Docker. The resulting AMI is
# config-free; per-deployment values (node IPs, unique node id AR0/AR1/...) are
# injected later via Terraform user_data. The baked systemd unit stays inert
# until that config lands.
#
# Usage:   ./create_ar_ami.sh
# Tunables: see ami_common.sh (AWS_REGION, BUILDER_INSTANCE_TYPE, REPO_BRANCH, ...).

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
# shellcheck source=ami_common.sh
source ./ami_common.sh

ROLE="ar"
AMI_NAME_PREFIX="${AMI_NAME_PREFIX:-xdn-ar}"

# emit_provision_script - full bash run on the builder for the AR role.
emit_provision_script() {
  emit_header
  emit_common_provision

  # --- AR-specific deps: FUSE/zstd toolchain, Docker CE, Rust ---
  cat <<'EOF'
# build toolchain for the FUSE recorders (bin/xdnd:428)
sudo apt-get install -y build-essential pkg-config libfuse3-dev libzstd-dev

# Docker CE (bin/xdnd:440-452)
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update
sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo groupadd -f docker

# Rust toolchain (bin/xdnd:466)
curl https://sh.rustup.rs -sSf | sh -s -- -y
export PATH="$HOME/.cargo/bin:/usr/local/go/bin:$PATH"

# --- build and stage AR artifacts ---
cd "$HOME/xdn"
./bin/build_xdn_cli.sh
./bin/build_xdn_fuselog.sh

# the xdn CLI is required
sudo cp bin/xdn /opt/xdn/bin/
sudo ln -sf /opt/xdn/bin/xdn /usr/local/bin/xdn

# Stage whichever FUSE recorder binaries this branch actually produced. The
# 'main' branch ships only the C++ fuselog; 'fadhil-optimized' also ships the
# Rust fuserust. The default recorder (FUSELOG) needs only the C++ pair.
for b in fuselog fuselog-apply fuserust fuserust-apply; do
  if [ -f "bin/$b" ]; then
    sudo cp "bin/$b" /opt/xdn/bin/
    sudo ln -sf "/opt/xdn/bin/$b" "/usr/local/bin/$b"
    echo "  staged $b"
  else
    echo "  note: bin/$b not produced by this branch; skipping"
  fi
done

# allow non-root mounts to expose files to other users (bin/xdnd:539)
sudo sed -i 's/#user_allow_other/user_allow_other/g' /etc/fuse.conf
EOF

  emit_prestage_helper
  emit_ar_unit
  emit_finalize "xdn-ar.service"
}

# emit_ar_unit - install the AR systemd unit. Runs as root for Docker access
# (matches the `sudo java` invocation in bin/gpServer.sh). Gated on both the
# config and a node id, since AR ids must be unique per instance (AR0/AR1/...)
# and are written by launch-time user_data.
emit_ar_unit() {
  cat <<'EOF'
sudo tee /etc/systemd/system/xdn-ar.service >/dev/null <<'UNIT'
[Unit]
Description=XDN ActiveReplica (edge server)
After=network-online.target docker.service
Wants=network-online.target
Requires=docker.service
ConditionPathExists=/opt/xdn/conf/gigapaxos.properties
ConditionPathExists=/opt/xdn/conf/node-id

[Service]
Type=simple
WorkingDirectory=/opt/xdn
EnvironmentFile=/opt/xdn/conf/node-id
ExecStartPre=/opt/xdn/bin/xdn-prestage.sh
ExecStart=/usr/bin/java -ea \
  -Djavax.net.ssl.keyStore=/opt/xdn/conf/keyStore.jks -Djavax.net.ssl.keyStorePassword=qwerty \
  -Djavax.net.ssl.trustStore=/opt/xdn/conf/trustStore.jks -Djavax.net.ssl.trustStorePassword=qwerty \
  -Djava.util.logging.config.file=/opt/xdn/conf/logging.properties \
  -Dlog4j.configuration=/opt/xdn/conf/log4j.properties \
  -DgigapaxosConfig=/opt/xdn/conf/gigapaxos.properties \
  -Djdk.httpclient.allowRestrictedHeaders=connection,content-length,host \
  -cp /opt/xdn/jars/* \
  edu.umass.cs.reconfiguration.ReconfigurableNode ${NODE_ID}
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
EOF
}

run_ami_build
