#!/usr/bin/env python3
import argparse
import logging
import shlex
import subprocess
import sys
from pathlib import Path


INSTALL_STEPS = [
    "sudo swapoff -a",
    "sudo sed -i.bak '/\\sswap\\s/s/^/#/' /etc/fstab",
    "sudo apt-get update -y",
    "sudo apt-get install -y apt-transport-https ca-certificates curl gnupg lsb-release conntrack",
    "sudo install -m 0755 -d /etc/apt/keyrings",
    "curl -fsSL https://pkgs.k8s.io/core:/stable:/v1.31/deb/Release.key | "
    "sudo gpg --batch --yes --dearmor -o /etc/apt/keyrings/kubernetes-apt-keyring.gpg",
    'echo "deb [signed-by=/etc/apt/keyrings/kubernetes-apt-keyring.gpg] '
    'https://pkgs.k8s.io/core:/stable:/v1.31/deb/ /" | sudo tee /etc/apt/sources.list.d/kubernetes.list',
    "sudo apt-get update -y",
    "sudo apt-get install -y containerd kubelet kubeadm kubectl",
    "sudo mkdir -p /etc/containerd",
    'sudo bash -lc \'containerd config default | sed "s/SystemdCgroup = false/SystemdCgroup = true/" > /etc/containerd/config.toml\'',
    "sudo systemctl enable --now containerd",
    "sudo systemctl enable --now kubelet",
    "sudo systemctl restart containerd",
    "sudo systemctl restart kubelet",
]


def run(cmd, check=True, capture_output=False, text=True):
    logging.info("[CMD] %s", " ".join(cmd))
    return subprocess.run(cmd, check=check, capture_output=capture_output, text=text)


def ensure_ssh_config(host: str, user: str, key_path: Path) -> None:
    # Add per-host entry so ssh can connect non-interactively with the provided key.
    config_path = Path.home() / ".ssh" / "config"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    entry = (
        f"Host {host}\n"
        f"  HostName {host}\n"
        f"  User {user}\n"
        f"  IdentityFile {key_path}\n"
        "  StrictHostKeyChecking no\n\n"
    )
    existing = config_path.read_text() if config_path.exists() else ""
    if host not in existing:
        config_path.write_text(existing + entry)


def copy_ssh_key(host: str, user: str, key_path: Path) -> None:
    logging.info("Copying SSH key to %s", host)
    run(["ssh-copy-id", "-i", str(key_path), f"{user}@{host}"])


def run_remote(host: str, user: str, key_path: Path, command: str) -> subprocess.CompletedProcess:
    remote_cmd = f"bash -lc {shlex.quote(command)}"
    return run(["ssh", "-i", str(key_path), "-o", "StrictHostKeyChecking=no", f"{user}@{host}", remote_cmd])


def reset_control_plane() -> None:
    logging.info("Resetting any previous kubeadm installation on master")
    run(["sudo", "kubeadm", "reset", "-f"])
    run(["sudo", "rm", "-rf", "/etc/kubernetes", "/var/lib/etcd", "/var/lib/kubelet/pki", "/var/lib/kubelet/config.yaml"])


def reset_worker(host: str, user: str, key_path: Path) -> None:
    logging.info("Resetting any previous kubeadm installation on worker %s", host)
    run_remote(
        host,
        user,
        key_path,
        "sudo kubeadm reset -f && sudo rm -rf /etc/kubernetes /var/lib/kubelet/pki /var/lib/kubelet/config.yaml",
    )


def install_dependencies(host: str, user: str, key_path: Path, local: bool) -> None:
    logging.info("Installing Kubernetes dependencies on %s", "local master" if local else host)
    for step in INSTALL_STEPS:
        if local:
            run(["bash", "-lc", step])
        else:
            run_remote(host, user, key_path, step)


def kubeadm_init(master_ip: str, pod_network_cidr: str) -> None:
    logging.info("Initializing control plane on %s", master_ip)
    cmd = [
        "sudo",
        "kubeadm",
        "init",
        f"--apiserver-advertise-address={master_ip}",
        f"--pod-network-cidr={pod_network_cidr}",
    ]
    run(cmd)


def setup_kubeconfig() -> None:
    logging.info("Setting up kubeconfig on master")
    run(["bash", "-lc", "mkdir -p $HOME/.kube && sudo cp /etc/kubernetes/admin.conf $HOME/.kube/config"])
    run(["bash", "-lc", "sudo chown $(id -u):$(id -g) $HOME/.kube/config"])


def install_calico(calico_manifest: str) -> None:
    logging.info("Installing Calico from %s", calico_manifest)
    run(["kubectl", "apply", "-f", calico_manifest])


def fetch_join_command() -> str:
    logging.info("Fetching worker join command")
    proc = run(["sudo", "kubeadm", "token", "create", "--print-join-command"], capture_output=True)
    if proc.stdout:
        return proc.stdout.strip()
    print("Failed to retrieve join command", file=sys.stderr)
    sys.exit(1)


def join_workers(workers, user: str, key_path: Path, join_cmd: str) -> None:
    logging.info("Joining worker nodes")
    for host in workers:
        run_remote(host, user, key_path, f"sudo {join_cmd}")


def verify_cluster() -> None:
    logging.info("Verifying cluster state")
    run(["kubectl", "get", "nodes"])


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Initialize a Kubernetes cluster for evals.")
    parser.add_argument("--master", required=True, help="Master node IP (local host).")
    parser.add_argument("--nodes", nargs="+", required=True, help="Worker node IPs.")
    parser.add_argument("--ssh-key", required=True, help="Path to SSH private key.")
    parser.add_argument("--user", required=True, help="SSH username.")
    parser.add_argument(
        "--skip-ssh-setup",
        action="store_true",
        help="Skip configuring SSH and copying keys to worker nodes.",
    )
    parser.add_argument(
        "--skip-install-deps",
        action="store_true",
        help="Skip installing Kubernetes dependencies on master and workers.",
    )
    parser.add_argument(
        "--pod-network-cidr",
        default="192.168.0.0/16",
        help="Pod network CIDR for kubeadm init (default: 192.168.0.0/16).",
    )
    parser.add_argument(
        "--calico-manifest",
        default="https://raw.githubusercontent.com/projectcalico/calico/v3.25.0/manifests/calico.yaml",
        help="Calico manifest URL to install the pod network.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = parse_args()
    key_path = Path(args.ssh_key).expanduser()
    if not key_path.exists():
        print(f"SSH key not found: {key_path}", file=sys.stderr)
        sys.exit(1)

    if not args.skip_ssh_setup:
        for host in args.nodes:
            ensure_ssh_config(host, args.user, key_path)
            copy_ssh_key(host, args.user, key_path)

    if not args.skip_install_deps:
        install_dependencies(args.master, args.user, key_path, local=True)
        for host in args.nodes:
            install_dependencies(host, args.user, key_path, local=False)

    reset_control_plane()
    for host in args.nodes:
        reset_worker(host, args.user, key_path)
    kubeadm_init(args.master, args.pod_network_cidr)
    setup_kubeconfig()
    install_calico(args.calico_manifest)

    join_cmd = fetch_join_command()
    join_workers(args.nodes, args.user, key_path, join_cmd)
    verify_cluster()


if __name__ == "__main__":
    main()
