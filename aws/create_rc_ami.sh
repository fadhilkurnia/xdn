#!/usr/bin/env bash
# create_rc_ami.sh - build an AWS AMI for the XDN Reconfigurator (RC) node,
# which also hosts the coredns (xdn-dns) nameserver.
#
# The RC role is lean: Java 21 + gigapaxos jars + the coredns binary + the geo
# DB. No Docker, Rust, or FUSE. The resulting AMI is config-free; per-deployment
# values (node IPs, Corefile with the RC EIP) are injected later via Terraform
# user_data. Baked systemd units stay inert until that config lands.
#
# Usage:   ./create_rc_ami.sh                 # x86-64 (amd64) AMI, default
#          ARCH=arm64 ./create_rc_ami.sh      # Graviton (arm64) AMI for t4g.* RCs
#
# The RC is lean and off the data path, so it runs well on a small Graviton
# instance (t4g.small/medium, ~20% cheaper per GB than t3). ARCH=arm64 builds a
# native arm64 image on a Graviton builder (see ami_common.sh); point main.tf's
# rc_ami + rc_instance_type at it. coredns and docker-ce-cli compile/install
# natively, and the Java jars are arch-neutral, so nothing else changes.
#
# Tunables: see ami_common.sh (ARCH, AWS_REGION, BUILDER_INSTANCE_TYPE, REPO_BRANCH, ...).

set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

# The RC image is lean (Java jars + coredns + geo DB; no Docker daemon, Rust, or
# FUSE), so it needs far less disk than the AR build. Default the build volume to
# 16GB instead of the shared 30GB: the baked AMI's root snapshot is smaller and
# every RC instance launched from it gets a smaller (cheaper) gp3 volume. Must be
# >= the runtime RC volume in main.tf (root_block_device.volume_size, also 16) and
# leave headroom for the transient build (clone + Go module cache + jars ~7-8GB).
# A user-provided BUILDER_VOLUME_SIZE still wins (set before sourcing).
: "${BUILDER_VOLUME_SIZE:=16}"

# shellcheck source=ami_common.sh
source ./ami_common.sh

ROLE="rc"
AMI_NAME_PREFIX="${AMI_NAME_PREFIX:-xdn-rc}"

# emit_provision_script - full bash run on the builder for the RC role.
emit_provision_script() {
  emit_header
  emit_common_provision

  # --- RC needs the docker CLI (not the daemon): the reconfigurator validates a
  #     new service's image at CREATE time by shelling out to
  #     `docker image inspect` / `docker manifest inspect`
  #     (XdnServiceInitialStateValidator). Without it, every CREATE fails and
  #     services land with 0 replicas. Quoted heredoc so $(...) stays literal. ---
  cat <<'EOF'
sudo install -m 0755 -d /etc/apt/keyrings
sudo curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
sudo chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
  | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
sudo apt-get update
sudo apt-get install -y docker-ce-cli
EOF

  # --- RC-specific: build coredns and stage the nameserver assets ---
  cat <<EOF
# coredns: the upstream Makefile drives the build, so install 'make' (absent on
# the base AMI). Then write the .go-version the Makefile expects (it is missing
# in this vendored copy) so GOTOOLCHAIN resolves, and build the static binary.
sudo apt-get install -y make
cd "\$HOME/xdn/xdn-dns"
echo "${GO_VERSION}" > .go-version
make coredns
sudo cp "\$HOME/xdn/xdn-dns/coredns" /opt/xdn/coredns
# (the GeoLite2-City db is staged to /opt/xdn/geo by emit_common_provision)

# Baked default node id (numeric; the RC is node 0). user_data may overwrite
# /opt/xdn/conf/node-id at launch.
echo 'NODE_ID=0' | sudo tee /opt/xdn/conf/node-id >/dev/null

# Free port 53 for coredns: disable systemd-resolved's stub listener and keep
# local name resolution working via the real resolver.
sudo sed -i 's/#DNSStubListener=yes/DNSStubListener=no/' /etc/systemd/resolved.conf
sudo ln -sf /run/systemd/resolve/resolv.conf /etc/resolv.conf
EOF

  emit_prestage_helper
  emit_rc_units
  emit_finalize "xdn-rc.service xdn-dns.service"
}

# emit_rc_units - install the RC + DNS systemd units. Both run as root (so
# coredns can bind :53 without extra capabilities). ConditionPathExists keeps
# them inert until launch-time config exists.
emit_rc_units() {
  cat <<'EOF'
# --- xdn-rc.service (Reconfigurator) ---
sudo tee /etc/systemd/system/xdn-rc.service >/dev/null <<'UNIT'
[Unit]
Description=XDN Reconfigurator (control plane)
After=network-online.target
Wants=network-online.target
ConditionPathExists=/opt/xdn/conf/gigapaxos.properties

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

# --- xdn-dns.service (coredns nameserver) ---
sudo tee /etc/systemd/system/xdn-dns.service >/dev/null <<'UNIT'
[Unit]
Description=XDN coredns nameserver
After=xdn-rc.service network-online.target
Wants=network-online.target
ConditionPathExists=/opt/xdn/conf/Corefile

[Service]
Type=simple
WorkingDirectory=/opt/xdn
ExecStartPre=/opt/xdn/bin/xdn-prestage.sh
ExecStart=/opt/xdn/coredns -conf /opt/xdn/conf/Corefile
Restart=on-failure
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
EOF
}

run_ami_build
