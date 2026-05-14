#!/usr/bin/env python3
"""Layer 5: real-database integration test for fuselog.

Mounts fuselog. Runs a real DB on top with a random transactional
workload. SIGKILLs the DB to simulate primary crash (no graceful
shutdown). Harvests the fuselog statediff. Replays it onto a fresh
directory. Starts a replica DB on the replayed dir, which performs
crash recovery. Asserts that every transaction the primary COMMITted
is present on the replica.

Run:
  ./xdn-fs/test/fuzz_db.py --db sqlite
  ./xdn-fs/test/fuzz_db.py --db sqlite --num-txns 1000 --seed 42
  ./xdn-fs/test/fuzz_db.py --db postgres
  ./xdn-fs/test/fuzz_db.py --db postgres --killmid 3.0

The --killmid flag schedules a SIGKILL of the primary N seconds after
the workload starts, instead of after all transactions finish. This
exercises crash recovery on a partially-completed workload.
"""

import argparse
import os
import random
import subprocess
import sys
import threading
import time

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from fuzz_differential import (  # noqa: E402
    APPLY_BIN, APPLY_DIR, BASE_DIR, FUSELOG_BIN, MOUNT_DIR, STATEDIFF_FILE,
    ensure_clean_dirs, harvest_statediff, log,
    start_fuselog, stop_fuselog,
)
from fuzz_db_sqlite import SqliteAdapter  # noqa: E402
from fuzz_db_postgres import PostgresAdapter  # noqa: E402
from fuzz_db_mysql import MysqlAdapter  # noqa: E402
from fuzz_db_mariadb import MariadbAdapter  # noqa: E402
from fuzz_db_mongodb import MongodbAdapter  # noqa: E402

ADAPTERS = {
    "sqlite": SqliteAdapter,
    "postgres": PostgresAdapter,
    "mysql": MysqlAdapter,
    "mariadb": MariadbAdapter,
    "mongodb": MongodbAdapter,
}

NAME_POOL = ["alice", "bob", "carol", "dave", "eve", "frank", "grace"]
ID_RANGE = 50  # IDs 0..49
PAYLOAD_SIZES = [16, 256, 1024, 4096]


def do_one_txn(adapter, rng, committed):
    """Execute one random transaction. Update `committed` only on a
    successful COMMIT. Returns a short tag for the op log."""
    op = rng.choice(["insert", "update", "delete"])

    if op == "insert":
        free = [i for i in range(ID_RANGE) if i not in committed]
        if not free:
            return ("skip", "insert", "full")
        chosen = rng.choice(free)
        name = rng.choice(NAME_POOL)
        value = rng.randint(0, 1000)
        payload = rng.randbytes(rng.choice(PAYLOAD_SIZES))
        if adapter.insert(chosen, name, value, payload):
            committed[chosen] = (chosen, name, value, payload)
            return ("ok", "insert", chosen)
        return ("fail", "insert", chosen)

    if op == "update":
        if not committed:
            return ("skip", "update", "empty")
        chosen = rng.choice(list(committed))
        name = rng.choice(NAME_POOL)
        value = rng.randint(0, 1000)
        payload = rng.randbytes(rng.choice(PAYLOAD_SIZES))
        if adapter.update(chosen, name, value, payload):
            committed[chosen] = (chosen, name, value, payload)
            return ("ok", "update", chosen)
        return ("fail", "update", chosen)

    # delete
    if not committed:
        return ("skip", "delete", "empty")
    chosen = rng.choice(list(committed))
    if adapter.delete(chosen):
        del committed[chosen]
        return ("ok", "delete", chosen)
    return ("fail", "delete", chosen)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", choices=ADAPTERS, required=True)
    parser.add_argument("--seed", type=int, default=None)
    parser.add_argument("--num-txns", type=int, default=200)
    parser.add_argument("--killmid", type=float, default=None,
                        help="seconds after workload start to SIGKILL the "
                             "primary (default: kill only after all txns)")
    args = parser.parse_args()

    if not FUSELOG_BIN.exists() or not APPLY_BIN.exists():
        log(f"error: binaries not found; "
            f"run ./bin/build_xdn_fuselog.sh cpp first")
        return 2

    seed = args.seed if args.seed is not None else int(time.time())
    log(f"==================================================")
    log(f" db        = {args.db}")
    log(f" seed      = {seed}")
    log(f" num_txns  = {args.num_txns}")
    log(f" killmid   = {args.killmid}")
    log(f" base_dir  = {BASE_DIR}")
    log(f"==================================================")

    rng = random.Random(seed)
    AdapterCls = ADAPTERS[args.db]

    ensure_clean_dirs()
    primary_data = MOUNT_DIR / args.db
    replica_data = APPLY_DIR / args.db

    fuselog_proc = None
    primary = None
    replica = None
    kill_timer = None

    try:
        # allow_other is needed when a docker container (started by root
        # via `sudo docker`) needs to access the bind-mounted FUSE dir.
        fuselog_proc = start_fuselog(allow_other=(args.db != "sqlite"))

        log(">> starting primary on A/")
        primary = AdapterCls(primary_data)
        primary.start()
        primary.init_schema()

        committed = {}
        if args.killmid is not None:
            kill_timer = threading.Timer(args.killmid, primary.kill)
            kill_timer.start()

        log(">> running workload")
        ok_count = 0
        fail_count = 0
        skip_count = 0
        for i in range(args.num_txns):
            tag, op, target = do_one_txn(primary, rng, committed)
            if tag == "ok":
                ok_count += 1
            elif tag == "fail":
                fail_count += 1
                # killmid: consecutive failures likely mean the DB is gone.
                if args.killmid is not None and fail_count > 5:
                    log(f"   stopping early at txn #{i}: DB unreachable")
                    break
            else:
                skip_count += 1

        if kill_timer is not None:
            kill_timer.cancel()
        log(f">> workload done: {ok_count} committed, "
            f"{fail_count} failed, {skip_count} skipped")
        log(f">> committed rows: {len(committed)}")

        log(">> SIGKILL primary")
        primary.kill()
        primary = None

        subprocess.run(["sync"], check=True)
        payload_bytes = harvest_statediff()
        with open(STATEDIFF_FILE, "wb") as f:
            f.write(payload_bytes)
        log(f">> statediff harvested: {len(payload_bytes)} bytes")

        stop_fuselog(fuselog_proc)
        fuselog_proc = None

        log(">> applying statediff to C/")
        env = os.environ.copy()
        env["FUSELOG_STATEDIFF_FILE"] = str(STATEDIFF_FILE)
        result = subprocess.run(
            [str(APPLY_BIN), str(APPLY_DIR) + "/"],
            env=env, capture_output=True, text=True,
        )
        if result.returncode != 0:
            log(f"fuselog-apply failed (rc={result.returncode}):")
            log(result.stdout)
            log(result.stderr)
            return 1

        log(">> starting replica on C/ (crash recovery on startup)")
        replica = AdapterCls(replica_data)
        replica.start()

        log(">> querying replica")
        replica_rows = replica.fetch_all()
        replica_state = {r[0]: r for r in replica_rows}

        log(f"--------------------------------------------------")
        log(f" primary committed : {len(committed)} rows")
        log(f" replica observed  : {len(replica_state)} rows")
        log(f"--------------------------------------------------")

        missing = sorted(i for i in committed if i not in replica_state)
        extra = sorted(i for i in replica_state if i not in committed)
        mismatched = sorted(
            i for i in committed
            if i in replica_state and committed[i] != replica_state[i]
        )

        if not (missing or extra or mismatched):
            log(f"PASS  seed={seed}")
            return 0

        log(f"FAIL  seed={seed}")
        if missing:
            log(f"  missing on replica ({len(missing)}): {missing[:10]}")
        if extra:
            log(f"  unexpected on replica ({len(extra)}): {extra[:10]}")
        if mismatched:
            log(f"  mismatched rows ({len(mismatched)}): {mismatched[:10]}")
            for i in mismatched[:3]:
                log(f"    id={i}:")
                log(f"      primary  = {committed[i]!r}")
                log(f"      replica  = {replica_state[i]!r}")
        return 1

    finally:
        if kill_timer is not None:
            try:
                kill_timer.cancel()
            except Exception:
                pass
        for ad in (primary, replica):
            if ad is not None:
                try:
                    ad.kill()
                except Exception:
                    pass
        stop_fuselog(fuselog_proc)


if __name__ == "__main__":
    sys.exit(main())
