"""Postgres adapter for fuzz_db.py.

Runs postgres in a docker container with the data dir bind-mounted from
the fuselog mount. Each transaction is one `docker exec psql ... -c
'BEGIN; ... ; COMMIT;'` invocation. 'Crash' is `docker kill -s KILL`.

Requires `sudo docker` (passwordless preferred). The container runs as
the host user via `--user $(id -u):$(id -g)` so it has write access to
the fuselog-mounted data dir without needing `allow_other`."""

import os
import subprocess
import time
import uuid
from pathlib import Path


class PostgresAdapter:
    name = "postgres"
    image = "postgres:16-alpine"

    def __init__(self, host_data_dir: Path):
        self.data_dir = Path(host_data_dir)
        # Postgres's docker image needs PGDATA to be a subdir (it refuses
        # an empty mounted root in some configurations).
        self.pgdata_subdir = "pgdata"
        self.container = f"fuselog-pg-{uuid.uuid4().hex[:8]}"

    def start(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        uid = os.getuid()
        gid = os.getgid()
        result = subprocess.run(
            [
                "sudo", "docker", "run", "--rm", "-d",
                "--name", self.container,
                "--user", f"{uid}:{gid}",
                "-v", f"{self.data_dir}:/var/lib/postgresql/data",
                "-e", "POSTGRES_PASSWORD=test",
                "-e", "POSTGRES_HOST_AUTH_METHOD=trust",
                "-e", f"PGDATA=/var/lib/postgresql/data/{self.pgdata_subdir}",
                self.image,
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"docker run failed (rc={result.returncode}):\n"
                f"{result.stderr}"
            )

        # Wait up to 90s — first start includes initdb; replica start
        # includes WAL crash recovery; both can take a while.
        for _ in range(180):
            ready = subprocess.run(
                ["sudo", "docker", "exec", self.container,
                 "pg_isready", "-U", "postgres"],
                capture_output=True,
            )
            if ready.returncode == 0:
                return
            time.sleep(0.5)
        raise RuntimeError(
            f"postgres in {self.container} did not become ready in 90s; "
            f"check `sudo docker logs {self.container}`"
        )

    def init_schema(self):
        self._psql("postgres", "CREATE DATABASE testdb")
        self._psql("testdb", """
            CREATE TABLE items (
                id      INTEGER PRIMARY KEY,
                name    VARCHAR(64) NOT NULL,
                value   INTEGER NOT NULL,
                payload BYTEA
            )
        """)

    def kill(self):
        subprocess.run(
            ["sudo", "docker", "rm", "-f", self.container],
            capture_output=True,
        )

    def insert(self, id_, name, value, payload):
        sql = (
            "BEGIN;\n"
            "INSERT INTO items (id, name, value, payload) VALUES "
            f"({int(id_)}, '{_quote(name)}', {int(value)}, "
            f"decode('{payload.hex()}', 'hex'));\n"
            "COMMIT;"
        )
        return self._psql("testdb", sql)

    def update(self, id_, name, value, payload):
        sql = (
            "BEGIN;\n"
            f"UPDATE items SET name='{_quote(name)}', value={int(value)}, "
            f"payload=decode('{payload.hex()}', 'hex') "
            f"WHERE id={int(id_)};\n"
            "COMMIT;"
        )
        return self._psql("testdb", sql)

    def delete(self, id_):
        sql = f"BEGIN;\nDELETE FROM items WHERE id={int(id_)};\nCOMMIT;"
        return self._psql("testdb", sql)

    def fetch_all(self):
        # Pipe-separated for easy parsing; payload as hex.
        sql = ("SELECT id || '|' || name || '|' || value || '|' || "
               "encode(payload, 'hex') FROM items ORDER BY id;")
        out = self._psql_capture("testdb", sql)
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
                bytes.fromhex(parts[3]),
            ))
        return rows

    def _psql(self, db, sql):
        """Run SQL with ON_ERROR_STOP. Returns True on success."""
        result = subprocess.run(
            ["sudo", "docker", "exec", "-i", self.container, "psql",
             "-U", "postgres", "-d", db,
             "-v", "ON_ERROR_STOP=1", "-q"],
            input=sql, capture_output=True, text=True,
        )
        return result.returncode == 0

    def _psql_capture(self, db, sql):
        """Run a SELECT and return stdout (tuples-only, unaligned)."""
        result = subprocess.run(
            ["sudo", "docker", "exec", "-i", self.container, "psql",
             "-U", "postgres", "-d", db, "-t", "-A"],
            input=sql, capture_output=True, text=True,
        )
        return result.stdout


def _quote(s):
    """Single-quote escape for psql literal contexts.

    Workload names come from a fixed alphanumeric pool so doubling
    single-quotes is sufficient; nothing else needs escaping."""
    return s.replace("'", "''")
