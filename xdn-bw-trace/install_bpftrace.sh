#!/usr/bin/env bash
# install_bpftrace.sh — install a recent enough bpftrace for inter_replica_bw.bt.
#
# The trace script uses builtins (e.g. `bswap`) that are only present in
# bpftrace ≥ 0.21, so the apt/jammy package (0.14) is too old. This script
# pulls the upstream static-bundle AppImage from
# github.com/bpftrace/bpftrace/releases and installs it under
# /opt/bpftrace-<version>, with a symlink at /usr/local/bin/bpftrace.
#
# Some hosts can't run the AppImage directly (the bundled FUSE auto-mount
# fails when its packaged util-linux's `mount` isn't where the AppImage
# expects it — that's what we hit on Ubuntu 22.04). On those hosts we
# extract the AppImage with `--appimage-extract` and use the inner AppRun
# instead. Both paths end up at the same /usr/local/bin/bpftrace symlink.
#
# Re-running the script is a no-op if the requested version is already
# installed; pass `--force` to reinstall.
#
# Usage:
#   xdn-bw-trace/install_bpftrace.sh                 # installs the pinned default
#   xdn-bw-trace/install_bpftrace.sh --version v0.25.1
#   BPFTRACE_VERSION=v0.25.1 xdn-bw-trace/install_bpftrace.sh
#   xdn-bw-trace/install_bpftrace.sh --force         # reinstall

set -euo pipefail

DEFAULT_VERSION="v0.25.1"
PREFIX_BASE="/opt"
SYMLINK="/usr/local/bin/bpftrace"

VERSION="${BPFTRACE_VERSION:-$DEFAULT_VERSION}"
FORCE=0

while (($#)); do
  case "$1" in
    --version)
      [[ $# -ge 2 ]] || { echo "error: --version requires an argument" >&2; exit 2; }
      VERSION="$2"; shift 2 ;;
    --version=*)
      VERSION="${1#--version=}"; shift ;;
    --force)
      FORCE=1; shift ;;
    -h|--help)
      sed -n '2,/^$/p' "$0" | sed 's/^# \{0,1\}//'
      exit 0 ;;
    *)
      echo "error: unknown argument '$1'" >&2; exit 2 ;;
  esac
done

# Normalize: accept "0.25.1" as well as "v0.25.1".
[[ "$VERSION" =~ ^v ]] || VERSION="v$VERSION"
VERSION_NUM="${VERSION#v}"

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "error: bpftrace runs on Linux only (host: $(uname -s))" >&2
  exit 1
fi

# Pick a sudo prefix: empty if we're already root, `sudo` otherwise.
if [[ "$(id -u)" -eq 0 ]]; then
  SUDO=""
else
  command -v sudo >/dev/null 2>&1 \
    || { echo "error: not root and 'sudo' not found" >&2; exit 1; }
  SUDO="sudo"
fi

PREFIX="${PREFIX_BASE}/bpftrace-${VERSION_NUM}"
URL="https://github.com/bpftrace/bpftrace/releases/download/${VERSION}/bpftrace"

# Probe: actually load a trivial bpftrace program. This forces the AppImage's
# squashfs to mount (and thus exercises the FUSE auto-mount path) and goes
# through the inner bpftrace binary, unlike `--version`/`--info` which the
# AppImage shim answers without mounting. We need root because bpftrace's
# CAP check fires before the mount. Returns 0 if the binary can run a script.
probe_bpftrace() {
  local bin="$1"
  $SUDO "$bin" -e 'BEGIN { exit(); }' >/dev/null 2>&1
}

# Skip work if the right version is already installed AND it actually runs.
# A bare AppImage on a FUSE-broken host will report the right --version but
# blow up on real scripts, so we re-probe before declaring "nothing to do".
if [[ "$FORCE" -eq 0 ]] && command -v bpftrace >/dev/null 2>&1; then
  EXISTING="$(bpftrace --version 2>/dev/null | head -n1 | awk '{print $NF}' || true)"
  if [[ "$EXISTING" == "$VERSION" ]] && probe_bpftrace "$(command -v bpftrace)"; then
    echo "bpftrace $VERSION already installed at $(command -v bpftrace) — nothing to do."
    echo "(pass --force to reinstall)"
    exit 0
  fi
  if [[ "$EXISTING" == "$VERSION" ]]; then
    echo "bpftrace $VERSION is on PATH but fails the probe — reinstalling."
    FORCE=1
  fi
fi

# If we already have an extracted install at $PREFIX that probes successfully,
# just (re-)point the symlink at it. This avoids re-downloading 164MB on every
# reinstall and tolerates a pre-existing /opt/bpftrace-<ver> from a previous
# manual install.
if [[ "$FORCE" -eq 0 && -x "${PREFIX}/AppRun" ]] \
   && probe_bpftrace "${PREFIX}/AppRun"; then
  echo "found working ${PREFIX}/AppRun — pointing ${SYMLINK} at it."
  $SUDO ln -sf "${PREFIX}/AppRun" "$SYMLINK"
  echo "installed: $(bpftrace --version 2>&1 | head -n1)  (via ${SYMLINK})"
  exit 0
fi

# Each bpftrace AppImage run stacks a tmpfs onto its `mountroot` directory
# and (on at least some kernels) doesn't tear it down on exit, so any path
# we want to delete may have one or more lingering tmpfs mounts under it.
# Try to unmount cleanly; -lf so a stuck mount still detaches.
unmount_under() {
  local root="$1"
  [[ -d "$root" ]] || return 0
  while read -r mp; do
    [[ -n "$mp" ]] && $SUDO umount -lf "$mp" 2>/dev/null || true
  done < <(mount | awk -v r="$root" '$3 ~ ("^" r) {print $3}' | sort -r)
}

safe_rm_rf() {
  local target="$1"
  unmount_under "$target"
  $SUDO rm -rf "$target"
}

WORKDIR="$(mktemp -d)"
# Extracted AppImages contain files owned by various build-side uids (root,
# nixbld, etc.) so a user-mode rm -rf can fail. Use sudo for cleanup, and
# unmount any tmpfs left behind by AppImage probes first.
trap 'safe_rm_rf "$WORKDIR"' EXIT

echo "downloading bpftrace ${VERSION} ..."
curl -fL --retry 3 -o "${WORKDIR}/bpftrace" "$URL"
chmod +x "${WORKDIR}/bpftrace"

# First try running the AppImage directly. On hosts where the AppImage's
# FUSE auto-mount works this is enough; we just install the binary.
echo "checking AppImage runs as-is ..."
if probe_bpftrace "${WORKDIR}/bpftrace"; then
  echo "AppImage runs natively — installing single binary."
  $SUDO install -m 0755 "${WORKDIR}/bpftrace" "$SYMLINK"
else
  echo "AppImage FUSE mount unavailable — falling back to --appimage-extract."
  pushd "$WORKDIR" >/dev/null
  ./bpftrace --appimage-extract >/dev/null
  popd >/dev/null

  if [[ ! -x "${WORKDIR}/squashfs-root/AppRun" ]]; then
    echo "error: AppImage extracted but AppRun missing in ${WORKDIR}/squashfs-root" >&2
    exit 1
  fi

  # Sanity: the extracted AppRun should actually run a script.
  if ! probe_bpftrace "${WORKDIR}/squashfs-root/AppRun"; then
    echo "error: extracted AppRun still fails to load a trivial bpftrace program" >&2
    exit 1
  fi

  # Replace any existing /opt/bpftrace-<ver> dir if --force was set.
  if [[ -d "$PREFIX" ]]; then
    if [[ "$FORCE" -eq 1 ]]; then
      safe_rm_rf "$PREFIX"
    else
      echo "error: $PREFIX already exists; pass --force to reinstall" >&2
      exit 1
    fi
  fi
  $SUDO mv "${WORKDIR}/squashfs-root" "$PREFIX"
  $SUDO ln -sf "${PREFIX}/AppRun" "$SYMLINK"
fi

# Final verification — should be on PATH at the symlink.
if ! INSTALLED="$(bpftrace --version 2>&1 | head -n1)"; then
  echo "error: bpftrace not runnable after install" >&2
  exit 1
fi
echo "installed: ${INSTALLED}  (via ${SYMLINK})"
