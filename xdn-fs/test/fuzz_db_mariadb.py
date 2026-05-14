"""MariaDB adapter for fuzz_db.py.

mariadb:11 in docker; same wire protocol and SQL surface as MySQL so the
adapter mostly just subclasses MysqlAdapter with image + client name.

Two MariaDB-specific quirks remain in `start()`:
- After a crash the captured tc.log can have a stale magic header because
  MariaDB unlink+recreates it aggressively and fuselog's fid-reuse model
  doesn't disambiguate incarnations (see TODO in fuselogv2.cpp about
  fresh fids on recreate). We delete tc.log on the replica before start
  so MariaDB recreates it cleanly — safe for our single-engine workload.
- The mariadb:11 image ships only the `mariadb` client (no `mysql`)."""

import subprocess
import time
import uuid

from fuzz_db_mysql import MysqlAdapter


class MariadbAdapter(MysqlAdapter):
    name = "mariadb"
    image = "mariadb:11"
    client_cmd = "mariadb"

    def __init__(self, host_data_dir):
        super().__init__(host_data_dir)
        self.container = f"fuselog-mariadb-{uuid.uuid4().hex[:8]}"

    def start(self):
        import os
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # If the data dir already has innodb files we treat this as a
        # recovery start (i.e. the replica).
        is_recovery = (self.data_dir / "ibdata1").exists()
        if is_recovery:
            # MariaDB's tc.log gets recycled (unlink + recreate) more
            # aggressively than fuselog's fid-reuse model handles, so
            # the replayed tc.log can have a stale magic header.
            # MariaDB refuses recovery on a bad tc.log even with
            # `--tc-heuristic-recover=COMMIT`. Easiest workaround at
            # the test layer: delete the corrupt tc.log so MariaDB
            # creates a fresh one — safe because our single-engine
            # InnoDB workload doesn't have prepared cross-engine txns.
            # See TODO in fuselogv2.cpp about fresh fids on recreate.
            tc_log = self.data_dir / "tc.log"
            if tc_log.exists():
                tc_log.unlink()
        uid = os.getuid()
        gid = os.getgid()
        args = [
            "sudo", "docker", "run", "--rm", "-d",
            "--name", self.container,
            "--user", f"{uid}:{gid}",
            "-v", f"{self.data_dir}:{self.data_dir_in_container}",
            "-e", "MYSQL_ROOT_PASSWORD=test",
            "-e", "MYSQL_DATABASE=testdb",
            self.image,
            "--innodb-flush-log-at-trx-commit=1",
            "--sync-binlog=1",
        ]
        result = subprocess.run(args, capture_output=True, text=True)
        if result.returncode != 0:
            raise RuntimeError(
                f"docker run failed (rc={result.returncode}):\n"
                f"{result.stderr}"
            )

        for _ in range(360):
            chk = subprocess.run(
                ["sudo", "docker", "exec", self.container, self.client_cmd,
                 "-uroot", "-ptest", "--silent", "--skip-column-names",
                 "-e", "SELECT 1"],
                capture_output=True,
            )
            if chk.returncode == 0:
                return
            time.sleep(0.5)
        raise RuntimeError(
            f"mariadb in {self.container} did not become ready in 180s"
        )
