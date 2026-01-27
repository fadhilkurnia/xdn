#!/usr/bin/env python3
"""
Prepare nodes for OpenEBS by running setup commands over SSH 
and install OpenEBS Mayastor via Helm.
"""

# Steps to prepare the node for OpenEBS.
NODE_PREP_CMDS = [
    "modinfo nvme-tcp",
    "sudo modprobe nvme-tcp",               # enable nvme-tcp module
    "sudo sysctl -w vm.nr_hugepages=1024",  # allocate hugepages
    # ensure hugepages are allocated on (re)boot
    "echo 'vm.nr_hugepages=1024' | sudo tee -a /etc/sysctl.conf",
    "sudo sysctl --system",                 # reload sysctl settings
]

# Steps to shrink existing LVM mounted at /mydata to 650G so that
# we can use the /dev/nvme1n1 or /dev/nvme0n1 disk for OpenEBS Mayastore storage.
DISK_PREP_CMDS = [
    # unmount /mydata so we can use one whole disk for OpenEBS
    "if mountpoint -q /mydata; then sudo umount /mydata; fi",
    # check and resize filesystem & volume in /mydata to free up the disk space
    "sudo e2fsck -p -f $(ls -1d /dev/mapper/emulab-node*-bs | head -n 1)",
    "sudo resize2fs $(ls -1d /dev/mapper/emulab-node*-bs | head -n 1) 650G",
    "sudo lvreduce -y -f -L 650G $(ls -1d /dev/emulab/node*-bs | head -n 1) || [ $? -eq 5 ]",
    "sudo mount /mydata",
    # remove LVM setup on /dev/nvme1n1 to use it for OpenEBS
    "sudo pvmove {openebs_disk}",
    # remove the physical volume from the emulab volume group
    "sudo vgreduce --yes emulab {openebs_disk}",
    # remove the LVM label from the physical volume
    "sudo pvremove {openebs_disk}",
    # wipe any existing filesystem signatures from the disk
    "sudo wipefs -a {openebs_disk}",
    # zero out the beginning of the disk to remove any partition table
    "sudo dd if=/dev/zero of={openebs_disk} bs=1M count=100 status=progress",
]

import argparse
import concurrent.futures
import shutil
import subprocess
import time

def get_openebs_disk(node: str) -> str:
    root_disk = subprocess.run(
        [
            "ssh",
            node,
            "lsblk -no PKNAME $(findmnt -n -o SOURCE /)",
        ],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    if root_disk == "nvme1n1":
        return "/dev/nvme0n1"
    return "/dev/nvme1n1"


def get_kube_node_name(ssh_target: str) -> str:
    hostname_result = subprocess.run(
        ["ssh", ssh_target, "hostname"],
        check=False,
        capture_output=True,
        text=True,
    )
    hostname = hostname_result.stdout.strip()
    if not hostname:
        error_detail = hostname_result.stderr.strip()
        message = f"Unable to determine hostname for {ssh_target}."
        if error_detail:
            message = f"{message} {error_detail}"
        raise RuntimeError(message)
    return hostname


def run_node_prep(node: str) -> None:
    for command in NODE_PREP_CMDS:
        subprocess.run(["ssh", node, command], check=True)


def run_disk_prep(node: str) -> None:
    openebs_disk = get_openebs_disk(node)
    disk_prep_cmds = [cmd.format(openebs_disk=openebs_disk) for cmd in DISK_PREP_CMDS]
    for command in disk_prep_cmds:
        subprocess.run(["ssh", node, command], check=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Prepare nodes for OpenEBS by running setup commands over SSH.",
    )
    parser.add_argument(
        "nodes",
        nargs="+",
        metavar="IP",
        help="IP addresses or hostnames of nodes to prepare.",
    )
    parser.add_argument(
        "--skip-prep",
        action="store_true",
        help="Skip node and disk preparation steps.",
    )
    return parser.parse_args()


def ensure_helm_installed() -> None:
    if shutil.which("helm"):
        return
    install_script = "/tmp/get_helm.sh"
    subprocess.run(
        [
            "curl",
            "-fsSL",
            "https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3",
            "-o",
            install_script,
        ],
        check=True,
    )
    subprocess.run(["bash", install_script], check=True)


def restart_kubelet(node: str) -> None:
    subprocess.run(["ssh", node, "sudo systemctl restart kubelet"], check=True)


def wait_for_hugepages(kube_node_name: str, timeout_seconds: int = 300) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        result = subprocess.run(
            [
                "kubectl",
                "get",
                "node",
                kube_node_name,
                "-o",
                "jsonpath={.status.allocatable.hugepages-2Mi}",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
        value = result.stdout.strip()
        if value and value != "0":
            return
        time.sleep(5)
    raise RuntimeError(f"HugePages not reported for node {kube_node_name} within timeout.")


def install_openebs_mayastor(ssh_nodes: list[str]) -> None:
    # label all worker nodes with openebs.io/engine=mayastor to 
    # schedule mayastor pods on them
    kube_nodes = [get_kube_node_name(node) for node in ssh_nodes]
    print("Worker nodes:", kube_nodes)
    for node in kube_nodes:
        subprocess.run(
            [
                "kubectl",
                "label",
                "nodes",
                node,
                "openebs.io/engine=mayastor",
                "--overwrite",
            ],
            check=True,
        )

    for node in ssh_nodes:
        restart_kubelet(node)
    for node in kube_nodes:
        wait_for_hugepages(node)

    # prepare helm and add openebs repo
    ensure_helm_installed()
    subprocess.run(
        [
            "helm",
            "repo",
            "add",
            "openebs",
            "https://openebs.github.io/openebs",
            "--force-update",
        ],
        check=True,
    )
    subprocess.run(["helm", "repo", "update"], check=True)

    # install openebs mayastor via helm, if not already installed
    release_check = subprocess.run(
        ["helm", "status", "openebs", "--namespace", "openebs"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    if release_check.returncode != 0:
        subprocess.run(
            [
                "helm",
                "install",
                "openebs",
                "--namespace",
                "openebs",
                "openebs/openebs",
                "--create-namespace",
            ],
            check=True,
        )

    # Ensure Mayastor CRDs are registered before creating DiskPools.
    subprocess.run(
        [
            "kubectl",
            "wait",
            "--for=condition=Established",
            "crd/diskpools.openebs.io",
            "--timeout=300s",
        ],
        check=True,
    )

    # for each worker node, create disk pool using the openebs disk 
    # (/dev/nvme0n1 or /dev/nvme1n1)
    for ssh_target, kube_node_name in zip(ssh_nodes, kube_nodes):
        openebs_disk = get_openebs_disk(ssh_target)
        # determine the persistent disk ID (e.g., nvme-eui.01000000000000098ce38ee303b5dad5) 
        # using the nvme command, then use the id (e.g., aio:///dev/disk/by-id/nvme-...) in the DiskPool.
        disk_id_result = subprocess.run(
            [
                "ssh",
                ssh_target,
                    (
                        "ls -l /dev/disk/by-id/ "
                        "| grep nvme-eui "
                        "| grep {disk} "
                        "| awk '{{print $9; exit}}'"
                    ).format(disk=openebs_disk.split("/")[-1]),
            ],
            check=True,
            capture_output=True,
            text=True,
        )
        disk_by_id = disk_id_result.stdout.strip()
        if not disk_by_id:
            raise RuntimeError(f"Unable to determine disk ID for {ssh_target}.")
        openebs_disk_id = f"/dev/disk/by-id/{disk_by_id}"
        disk_pool_yaml = f"""
apiVersion: openebs.io/v1beta3
kind: DiskPool
metadata:
  name: disk-pool-{kube_node_name.replace('.', '-') }
  namespace: openebs
spec:
  node: {kube_node_name}
  disks: ["aio://{openebs_disk_id}"]
"""
        print("Creating DiskPool on node", kube_node_name)
        print(disk_pool_yaml)
        subprocess.run(
            [
                "kubectl",
                "apply",
                "-f",
                "-",
            ],
            input=disk_pool_yaml,
            text=True,
            check=True,
        )

    # create openebs mayastor storage class with three replicas, 
    # if not already present
    storage_class_yaml = """
apiVersion: storage.k8s.io/v1
kind: StorageClass
metadata:
  name: openebs-3-replica
parameters:
  protocol: nvmf
  repl: "3"
provisioner: io.openebs.csi-mayastor
reclaimPolicy: Delete
volumeBindingMode: Immediate
"""
    subprocess.run(
        [
            "kubectl",
            "apply",
            "-f",
            "-",
        ],
        input=storage_class_yaml,
        text=True,
        check=True,
    )

    # create pvc using the openebs-3-replica storage class,
    # if not already present
    pvc_yaml = """
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: openebs-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 4Gi
  storageClassName: openebs-3-replica
"""
    subprocess.run(
        [
            "kubectl",
            "apply",
            "-f",
            "-",
        ],
        input=pvc_yaml,
        text=True,
        check=True,
    )

    # verify by deploying a fio test pod that uses the pvc
    fio_pod_yaml = """
apiVersion: v1
kind: Pod
metadata:
  name: fio-test-pod
spec:
  nodeSelector:
    openebs.io/engine: mayastor
  containers:
    - name: fio
      image: nixery.dev/shell/fio
      args:
        - sleep
        - "1000000"
      volumeMounts:
        - mountPath: "/volume"
          name: openebs-volume
  volumes:
    - name: openebs-volume
      persistentVolumeClaim:
        claimName: openebs-pvc
  restartPolicy: Never
"""
    subprocess.run(
        [
            "kubectl",
            "apply",
            "-f",
            "-",
        ],
        input=fio_pod_yaml,
        text=True,
        check=True,
    )


def main() -> None:
    args = parse_args()

    if not args.skip_prep:
        # prepare the nodes
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(args.nodes),
        ) as executor:
            futures = [executor.submit(run_node_prep, node) for node in args.nodes]
            for future in futures:
                future.result()

        # prepare the disks
        with concurrent.futures.ThreadPoolExecutor(
            max_workers=len(args.nodes),
        ) as executor:
            futures = [executor.submit(run_disk_prep, node) for node in args.nodes]
            for future in futures:
                future.result()

    # install OpenEBS Mayastor
    install_openebs_mayastor(args.nodes)


if __name__ == "__main__":
    main()
