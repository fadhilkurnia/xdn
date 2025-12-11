#!/usr/bin/env bash
set -euo pipefail

# Ensure script runs with privileges needed for Docker daemon changes.
if [[ $EUID -ne 0 ]]; then
  echo "This script must run as root to configure Docker." >&2
  exit 1
fi

# Abort early if Docker is missing since the rest depends on it.
if ! command -v docker >/dev/null 2>&1; then
  echo "Docker is not installed. Install Docker before running this script." >&2
  exit 1
fi

export DOCKER_CLI_EXPERIMENTAL=enabled

# Enable Docker experimental features in daemon.json.
DAEMON_CONFIG="/etc/docker/daemon.json"
mkdir -p /etc/docker

python3 - <<'PY'
import json
from pathlib import Path

path = Path("/etc/docker/daemon.json")
data = {}
if path.exists():
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        raise SystemExit("Existing /etc/docker/daemon.json is not valid JSON; fix it before rerunning.")

if data.get("experimental") is True:
    raise SystemExit(0)

data["experimental"] = True
path.write_text(json.dumps(data, indent=2) + "\n")
PY

# Persist CLI experimental flag for login shells.
mkdir -p /etc/profile.d
cat >/etc/profile.d/docker-experimental.sh <<'EOF'
export DOCKER_CLI_EXPERIMENTAL=enabled
EOF

# Restart Docker to pick up experimental setting when possible.
if command -v systemctl >/dev/null 2>&1; then
  systemctl restart docker || true
elif command -v service >/dev/null 2>&1; then
  service docker restart || true
fi

# Install CRIU via available package manager if missing.
if ! command -v criu >/dev/null 2>&1; then
  if command -v apt-get >/dev/null 2>&1; then
    apt-get update -y
    apt-get install -y criu
  elif command -v yum >/dev/null 2>&1; then
    yum install -y criu
  elif command -v dnf >/dev/null 2>&1; then
    dnf install -y criu
  elif command -v zypper >/dev/null 2>&1; then
    zypper install -y criu
  else
    echo "criu is required but no supported package manager found." >&2
    exit 1
  fi
fi

# Ensure CRIU defaults allow checkpointing TCP connections.
mkdir -p /etc/criu
python3 - <<'PY'
from pathlib import Path

path = Path("/etc/criu/default.conf")
required = ("tcp-established", "tcp-close")

lines = path.read_text().splitlines() if path.exists() else []
existing = {line.strip() for line in lines}
changed = False

for opt in required:
    if opt not in existing:
        lines.append(opt)
        changed = True

if not changed and path.exists():
    raise SystemExit(0)

path.parent.mkdir(parents=True, exist_ok=True)
path.write_text("\n".join(lines).rstrip("\n") + "\n")
PY
