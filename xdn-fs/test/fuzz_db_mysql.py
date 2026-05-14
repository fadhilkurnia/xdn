"""MySQL adapter for fuzz_db.py.

Runs mysql:8.4 in docker with bind-mounted data dir on the fuselog
mount. Each transaction is one `docker exec mysql -e 'BEGIN; ...;
COMMIT;'`. 'Crash' is `docker kill -s KILL`.

Same shape as fuzz_db_postgres.py; differences are:
- /var/lib/mysql instead of /var/lib/postgresql/data
- mysqladmin ping for readiness instead of pg_isready
- HEX(...) / UNHEX(...) for BYTEA-equivalent payload encoding
- innodb_flush_log_at_trx_commit=1 enforced via my.cnf for durability
"""

import os
import subprocess
import time
import uuid
from pathlib import Path


class MysqlAdapter:
    name = "mysql"
    image = "mysql:8.4"
    data_dir_in_container = "/var/lib/mysql"
    client_cmd = "mysql"   # overridden by MariadbAdapter (image ships only `mariadb`)

    def __init__(self, host_data_dir: Path):
        self.data_dir = Path(host_data_dir)
        self.container = f"fuselog-mysql-{uuid.uuid4().hex[:8]}"

    def start(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        uid = os.getuid()
        gid = os.getgid()
        result = subprocess.run(
            [
                "sudo", "docker", "run", "--rm", "-d",
                "--name", self.container,
                "--user", f"{uid}:{gid}",
                "-v", f"{self.data_dir}:{self.data_dir_in_container}",
                "-e", "MYSQL_ROOT_PASSWORD=test",
                "-e", "MYSQL_DATABASE=testdb",
                self.image,
                # innodb_flush_log_at_trx_commit=1 is the default; pin it
                # explicitly so a captured statediff is guaranteed to
                # contain the WAL bytes of every committed transaction.
                "--innodb-flush-log-at-trx-commit=1",
                "--sync-binlog=1",
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"docker run failed (rc={result.returncode}):\n"
                f"{result.stderr}"
            )

        # `mysqladmin ping` succeeds before the entrypoint finishes
        # setting up the root password. Run an actual authenticated query
        # so we don't start the workload against a half-initialized DB.
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
            f"mysql in {self.container} did not become ready in 180s"
        )

    def init_schema(self):
        ok = self._mysql("testdb", """
            CREATE TABLE items (
                id      INT PRIMARY KEY,
                name    VARCHAR(64) NOT NULL,
                value   INT NOT NULL,
                payload VARBINARY(8192)
            ) ENGINE=InnoDB;
        """)
        if not ok:
            raise RuntimeError("init_schema failed (CREATE TABLE)")

    def kill(self):
        # `rm -f` is SIGKILL + remove. Guarantees the container is gone
        # before we proceed to harvest; plain `docker kill` has races
        # where the container can linger if the daemon is busy.
        subprocess.run(
            ["sudo", "docker", "rm", "-f", self.container],
            capture_output=True,
        )

    def insert(self, id_, name, value, payload):
        sql = (
            "BEGIN;\n"
            "INSERT INTO items (id, name, value, payload) VALUES "
            f"({int(id_)}, '{_quote(name)}', {int(value)}, "
            f"UNHEX('{payload.hex()}'));\n"
            "COMMIT;"
        )
        return self._mysql("testdb", sql)

    def update(self, id_, name, value, payload):
        sql = (
            "BEGIN;\n"
            f"UPDATE items SET name='{_quote(name)}', value={int(value)}, "
            f"payload=UNHEX('{payload.hex()}') "
            f"WHERE id={int(id_)};\n"
            "COMMIT;"
        )
        return self._mysql("testdb", sql)

    def delete(self, id_):
        sql = f"BEGIN;\nDELETE FROM items WHERE id={int(id_)};\nCOMMIT;"
        return self._mysql("testdb", sql)

    def fetch_all(self):
        # tabular output with HEX-encoded payload, pipe-delimited.
        sql = ("SELECT CONCAT_WS('|', id, name, value, HEX(payload)) "
               "FROM items ORDER BY id;")
        out = self._mysql_capture("testdb", sql)
        rows = []
        for line in out.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split("|", 3)
            if len(parts) != 4:
                continue
            rows.append((
                int(parts[0]),
                parts[1],
                int(parts[2]),
                bytes.fromhex(parts[3]) if parts[3] else b"",
            ))
        return rows

    def _mysql(self, db, sql):
        result = subprocess.run(
            ["sudo", "docker", "exec", "-i", self.container, self.client_cmd,
             "-uroot", "-ptest", "--silent", "--skip-column-names", db],
            input=sql, capture_output=True, text=True,
        )
        return result.returncode == 0

    def _mysql_capture(self, db, sql):
        result = subprocess.run(
            ["sudo", "docker", "exec", "-i", self.container, self.client_cmd,
             "-uroot", "-ptest", "--silent", "--skip-column-names", db],
            input=sql, capture_output=True, text=True,
        )
        return result.stdout


def _quote(s):
    return s.replace("'", "''").replace("\\", "\\\\")
