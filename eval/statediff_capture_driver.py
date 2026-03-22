#!/usr/bin/env python3
"""
Driver-side wrapper that stages the statediff experiment files to a worker node
and runs the worker script there over SSH.
"""

from __future__ import annotations

import argparse
import shlex
import subprocess
from pathlib import Path


ROOT = Path(__file__).resolve().parent.parent
REMOTE_HOST = "10.10.1.4"
REMOTE_DIR = "~/xdn-statediff-exp"


def run(cmd: str) -> None:
    print(f"$ {cmd}")
    subprocess.run(cmd, shell=True, check=True)


def stage() -> None:
    run(f"ssh {REMOTE_HOST} 'mkdir -p {REMOTE_DIR}'")
    run(
        "rsync -az "
        f"{shlex.quote(str(ROOT / 'eval' / 'statediff_capture_worker.py'))} "
        f"{shlex.quote(str(ROOT / 'services' / 'bookcatalog'))} "
        f"{shlex.quote(str(ROOT / 'services' / 'filewriter'))} "
        f"{shlex.quote(str(ROOT / 'xdn-fs' / 'cpp'))} "
        f"{REMOTE_HOST}:{REMOTE_DIR}/"
    )


def remote(cmd: str) -> None:
    run(f"ssh {REMOTE_HOST} 'cd {REMOTE_DIR} && {cmd}'")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest='cmd', required=True)

    p = sub.add_parser("prep")
    p.set_defaults(remote_cmd="python3 statediff_capture_worker.py prep")

    p = sub.add_parser("bookcatalog")
    p.add_argument("--requests", type=int, default=100)
    p.add_argument("--seed-books", type=int, default=200)

    p = sub.add_parser("bookcatalog-opt-size")
    p.add_argument("--requests", type=int, default=100)
    p.add_argument("--seed-books", type=int, default=200)

    p = sub.add_parser("filewriter")
    p.add_argument("--state-mb", type=int, default=1024)
    p.add_argument("--requests-per-size", type=int, default=20)

    p = sub.add_parser("filewriter-state-size")
    p.add_argument("--requests-per-state", type=int, default=20)
    p.add_argument("--write-size", type=int, default=4096)
    p.add_argument("--state-mbs", default="64,128,256,512,1024")

    args = parser.parse_args()
    stage()

    if args.cmd == "prep":
        remote("python3 statediff_capture_worker.py prep")
    elif args.cmd == "bookcatalog":
        remote(
            "python3 statediff_capture_worker.py bookcatalog "
            f"--requests {args.requests} --seed-books {args.seed_books}"
        )
    elif args.cmd == "bookcatalog-opt-size":
        remote(
            "python3 statediff_capture_worker.py bookcatalog-opt-size "
            f"--requests {args.requests} --seed-books {args.seed_books}"
        )
    elif args.cmd == "filewriter":
        remote(
            "python3 statediff_capture_worker.py filewriter "
            f"--state-mb {args.state_mb} --requests-per-size {args.requests_per_size}"
        )
    elif args.cmd == "filewriter-state-size":
        remote(
            "python3 statediff_capture_worker.py filewriter-state-size "
            f"--requests-per-state {args.requests_per_state} "
            f"--write-size {args.write_size} "
            f"--state-mbs {shlex.quote(args.state_mbs)}"
        )


if __name__ == "__main__":
    main()
