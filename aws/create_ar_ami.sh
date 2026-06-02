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

# --- Caddy (HTTPS termination): a custom build that bundles the route53 DNS-01
#     provider, fetched from Caddy's download API (no Go/xcaddy needed here).
#     Per-deployment config (Caddyfile) is injected at launch by user_data. ---
sudo curl -fsSL -o /opt/xdn/bin/caddy \
  "https://caddyserver.com/api/download?os=linux&arch=amd64&p=github.com/caddy-dns/route53"
sudo chmod +x /opt/xdn/bin/caddy
sudo ln -sf /opt/xdn/bin/caddy /usr/local/bin/caddy
EOF

  emit_prestage_helper
  emit_ar_unit
  emit_caddy_unit
  emit_tls_sync_unit
  emit_finalize "xdn-ar.service caddy.service xdn-tls-sync.timer"
}

# emit_tls_sync_unit - install the cert-sync helper + a timer. Caddy issues and
# auto-renews the wildcard into its own storage; this copies the latest PEM to
# the stable path the XDN frontend loads (/opt/xdn/conf/tls/) and restarts the
# frontend when the cert changes (Netty loads the cert at startup). The CA dir
# name under Caddy's storage varies (staging vs prod), so we glob for it.
emit_tls_sync_unit() {
  cat <<'EOF'
sudo tee /opt/xdn/bin/xdn-tls-sync.sh >/dev/null <<'SYNC'
#!/usr/bin/env bash
set -euo pipefail
shopt -s nullglob
crt=""
for f in /var/lib/caddy/caddy/certificates/*/wildcard_.*/wildcard_.*.crt; do crt="$f"; done
if [ -z "$crt" ]; then echo "xdn-tls-sync: no wildcard cert issued yet" >&2; exit 1; fi
key="${crt%.crt}.key"
[ -f "$key" ] || { echo "xdn-tls-sync: missing key for $crt" >&2; exit 1; }
dst=/opt/xdn/conf/tls
install -d -m 0755 "$dst"
# Detect change against stashed raw sources (the published key is re-encoded, so
# it can't be cmp'd directly against Caddy's source key).
changed=0
cmp -s "$crt" "$dst/.src.crt" || changed=1
cmp -s "$key" "$dst/.src.key" || changed=1
if [ "$changed" = 1 ]; then
  install -m 0644 "$crt" "$dst/.src.crt"
  install -m 0600 "$key" "$dst/.src.key"
  install -m 0644 "$crt" "$dst/fullchain.pem"
  # Caddy emits the key as SEC1 ("EC PRIVATE KEY"); Netty/JDK only parse PKCS#8
  # ("PRIVATE KEY"). Convert so SslContextBuilder.forServer can load it.
  openssl pkcs8 -topk8 -nocrypt -in "$dst/.src.key" -out "$dst/privkey.pem"
  chmod 600 "$dst/privkey.pem"
  echo "xdn-tls-sync: updated $dst (PKCS#8) from $crt"
  # Frontend reads the cert at startup; restart it if already running so a
  # renewal is picked up (no-op during first-boot sync, before xdn-ar starts).
  systemctl try-restart xdn-ar 2>/dev/null || true
fi
SYNC
sudo chmod +x /opt/xdn/bin/xdn-tls-sync.sh

sudo tee /etc/systemd/system/xdn-tls-sync.service >/dev/null <<'UNIT'
[Unit]
Description=Sync Caddy-issued wildcard cert to the XDN frontend cert path
After=caddy.service
ConditionPathExists=/var/lib/caddy

[Service]
Type=oneshot
ExecStart=/opt/xdn/bin/xdn-tls-sync.sh
UNIT

sudo tee /etc/systemd/system/xdn-tls-sync.timer >/dev/null <<'UNIT'
[Unit]
Description=Periodically sync the renewed wildcard cert to the XDN frontend

[Timer]
OnBootSec=2min
OnUnitActiveSec=1h

[Install]
WantedBy=timers.target
UNIT
EOF
}

# emit_caddy_unit - install the Caddy systemd unit. Runs as root (matches the
# other XDN units) so it can bind :80/:443. ConditionPathExists keeps it inert
# until launch-time user_data writes the Caddyfile.
emit_caddy_unit() {
  cat <<'EOF'
sudo tee /etc/systemd/system/caddy.service >/dev/null <<'UNIT'
[Unit]
Description=Caddy (cert-only manager: ACME DNS-01 wildcard for *.<domain>)
After=network-online.target
Wants=network-online.target
ConditionPathExists=/opt/xdn/conf/Caddyfile

[Service]
Type=notify
EnvironmentFile=/opt/xdn/conf/caddy.env
ExecStart=/opt/xdn/bin/caddy run --environ --config /opt/xdn/conf/Caddyfile --adapter caddyfile
ExecReload=/opt/xdn/bin/caddy reload --config /opt/xdn/conf/Caddyfile --adapter caddyfile --force
Restart=on-failure
RestartSec=5
LimitNOFILE=1048576

[Install]
WantedBy=multi-user.target
UNIT
EOF
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
After=network-online.target docker.service caddy.service
Wants=network-online.target
Requires=docker.service
ConditionPathExists=/opt/xdn/conf/gigapaxos.properties
ConditionPathExists=/opt/xdn/conf/node-id
# The frontend terminates TLS itself: don't start until the wildcard PEM exists
# (Caddy issues it; xdn-tls-sync copies it here).
ConditionPathExists=/opt/xdn/conf/tls/fullchain.pem

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
