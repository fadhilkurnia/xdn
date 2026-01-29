#!/usr/bin/env python3
"""
Prepare nodes for DRBD by installing packages, loading the kernel module,
and wiping the backing disk used by DRBD resources.
"""

import argparse
import concurrent.futures
import shlex
import subprocess


def get_drbd_disk(node: str) -> str:
    """Detect available disk by finding the non-root NVMe disk."""
    root_disk = subprocess.run(
        ["ssh", node, "lsblk -no PKNAME $(findmnt -n -o SOURCE /)"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if root_disk == "nvme1n1":
        return "/dev/nvme0n1"
    return "/dev/nvme1n1"


def run_remote(node: str, command: str, capture_output: bool = False) -> subprocess.CompletedProcess:
    kwargs = {"check": True, "text": True}
    if capture_output:
        kwargs["capture_output"] = True
    return subprocess.run(["ssh", node, command], **kwargs)


def install_drbd(node: str) -> None:
    install_script = """
set -euo pipefail
if command -v apt-get >/dev/null 2>&1; then
  sudo apt-get update -y
  # Try drbd-dkms first, fallback to adding LINBIT PPA if unavailable
  if apt-cache show drbd-dkms >/dev/null 2>&1; then
    sudo apt-get install -y drbd-utils drbd-dkms
  else
    # Add LINBIT PPA for DRBD packages
    sudo apt-get install -y software-properties-common
    sudo add-apt-repository -y ppa:linbit/linbit-drbd9-stack
    sudo apt-get update -y
    sudo apt-get install -y drbd-utils drbd-dkms
  fi
elif command -v dnf >/dev/null 2>&1; then
  sudo dnf -y install drbd-utils kmod-drbd
elif command -v yum >/dev/null 2>&1; then
  sudo yum -y install drbd-utils kmod-drbd
elif command -v zypper >/dev/null 2>&1; then
  sudo zypper --non-interactive install drbd-utils drbd-kmp-default
else
  echo "No supported package manager found (apt/dnf/yum/zypper)." >&2
  exit 1
fi
command -v drbdadm >/dev/null 2>&1
"""
    run_remote(node, f"bash -lc {shlex.quote(install_script)}")


def prepare_kernel(node: str) -> None:
    commands = [
        "sudo modprobe drbd",
        "lsmod | grep -q '^drbd'",
        "echo drbd | sudo tee /etc/modules-load.d/drbd.conf >/dev/null",
        "sudo udevadm settle",
    ]
    for command in commands:
        run_remote(node, command)


def prepare_disk(node: str, device: str) -> None:
    if not device:
        raise ValueError("backing device is required for disk preparation")
    # First, handle LVM cleanup if the device is part of an LVM setup (common on CloudLab)
    lvm_cleanup_script = """
set -e
DEV={dev}
# Check if device is an LVM PV
if sudo pvs "$DEV" 2>/dev/null; then
    echo "Device $DEV is an LVM PV, cleaning up..."
    VG=$(sudo pvs --noheadings -o vg_name "$DEV" 2>/dev/null | tr -d ' ')
    if [ -n "$VG" ]; then
        echo "Found VG: $VG"
        # Unmount any filesystems from this VG
        for mp in $(sudo lvs --noheadings -o lv_path "$VG" 2>/dev/null | xargs -I{{}} findmnt -n -o TARGET {{}} 2>/dev/null || true); do
            echo "Unmounting $mp"
            sudo umount "$mp" || true
        done
        # Deactivate all LVs in the VG
        sudo lvchange -an "$VG" || true
        # Remove the PV from the VG
        sudo vgreduce "$VG" "$DEV" --force || sudo vgreduce --removemissing "$VG" --force || true
    fi
    # Remove the PV
    sudo pvremove "$DEV" --force --force || true
fi
""".format(dev=shlex.quote(device))
    run_remote(node, f"bash -c {shlex.quote(lvm_cleanup_script)}")

    commands = [
        f"test -b {shlex.quote(device)}",
        f"sudo lsblk -f {shlex.quote(device)}",
        (
            "for m in $(lsblk -n -o MOUNTPOINT {dev} | awk 'NF{{print}}'); do "
            "sudo umount $m || true; done"
        ).format(dev=shlex.quote(device)),
        (
            "for part in $(lsblk -n -o NAME {dev} | tail -n +2); do "
            "sudo wipefs -a /dev/$part || true; done"
        ).format(dev=shlex.quote(device)),
        f"sudo wipefs -a {shlex.quote(device)}",
        (
            "if command -v sgdisk >/dev/null 2>&1; then "
            "sudo sgdisk --zap-all {dev}; fi"
        ).format(dev=shlex.quote(device)),
        f"sudo dd if=/dev/zero of={shlex.quote(device)} bs=1M count=16 status=progress",
        f"sudo blockdev --rereadpt {shlex.quote(device)} || true",
    ]
    for command in commands:
        run_remote(node, command)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare nodes for DRBD by installing packages and wiping backing disks.",
    )
    parser.add_argument(
        "nodes",
        nargs="+",
        metavar="IP",
        help="IP addresses or hostnames of nodes to prepare.",
    )
    parser.add_argument(
        "--backing-device",
        default=None,
        help="Block device path to use for DRBD (e.g., /dev/sdb). Required unless --skip-disk-prep.",
    )
    parser.add_argument(
        "--skip-install",
        action="store_true",
        help="Skip package installation.",
    )
    parser.add_argument(
        "--skip-disk-prep",
        action="store_true",
        help="Skip destructive disk preparation steps.",
    )
    parser.add_argument(
        "--skip-kernel-prep",
        action="store_true",
        help="Skip loading the DRBD kernel module.",
    )
    parser.add_argument(
        "--auto-detect-disk",
        action="store_true",
        help="Automatically detect available disk on each node (selects non-root NVMe disk).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    if not args.skip_disk_prep and not args.backing_device and not args.auto_detect_disk:
        raise SystemExit("--backing-device or --auto-detect-disk is required unless --skip-disk-prep is set.")

    if not args.skip_install:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(args.nodes)) as executor:
            futures = [executor.submit(install_drbd, node) for node in args.nodes]
            for future in futures:
                future.result()

    if not args.skip_kernel_prep:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(args.nodes)) as executor:
            futures = [executor.submit(prepare_kernel, node) for node in args.nodes]
            for future in futures:
                future.result()

    if not args.skip_disk_prep:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(args.nodes)) as executor:
            futures = []
            for node in args.nodes:
                if args.auto_detect_disk:
                    device = get_drbd_disk(node)
                    print(f"Auto-detected disk for {node}: {device}")
                else:
                    device = args.backing_device
                futures.append(executor.submit(prepare_disk, node, device))
            for future in futures:
                future.result()


if __name__ == "__main__":
    main()
