"""SQLite adapter for fuzz_db.py.

In-process: 'crash' means abandoning the connection without a WAL
checkpoint. The .db / -wal / -shm files on the fuselog mount form the
durable state; opening the replica on the same files triggers automatic
WAL recovery on the next open.

Uses WAL mode + synchronous=FULL so each COMMIT fsyncs the WAL — this is
what makes fuselog's captured statediff contain the durable bytes."""

import sqlite3
from pathlib import Path


class SqliteAdapter:
    name = "sqlite"

    def __init__(self, host_data_dir: Path):
        self.data_dir = Path(host_data_dir)
        self.db_path = self.data_dir / "test.db"
        self.conn = None

    def start(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        # isolation_level=None means autocommit OFF in Python's sqlite3
        # binding sense, but transactions are managed explicitly via
        # BEGIN/COMMIT. Counter-intuitive name; this is correct.
        self.conn = sqlite3.connect(str(self.db_path), isolation_level=None,
                                    timeout=30)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")

    def init_schema(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS items (
                id      INTEGER PRIMARY KEY,
                name    TEXT NOT NULL,
                value   INTEGER NOT NULL,
                payload BLOB
            )
        """)

    def kill(self):
        # Drop the connection without checkpointing the WAL. The on-disk
        # state at this point is exactly what fuselog has captured.
        if self.conn is not None:
            try:
                self.conn.close()
            except sqlite3.Error:
                pass
            self.conn = None

    def insert(self, id_, name, value, payload):
        return self._txn(
            "INSERT INTO items (id, name, value, payload) VALUES (?, ?, ?, ?)",
            (id_, name, value, payload),
        )

    def update(self, id_, name, value, payload):
        return self._txn(
            "UPDATE items SET name=?, value=?, payload=? WHERE id=?",
            (name, value, payload, id_),
        )

    def delete(self, id_):
        return self._txn("DELETE FROM items WHERE id=?", (id_,))

    def fetch_all(self):
        cur = self.conn.execute(
            "SELECT id, name, value, payload FROM items ORDER BY id"
        )
        return [(r[0], r[1], r[2], bytes(r[3]) if r[3] is not None else b"")
                for r in cur.fetchall()]

    def _txn(self, sql, params):
        if self.conn is None:
            return False
        try:
            self.conn.execute("BEGIN")
            cur = self.conn.execute(sql, params)
            n = cur.rowcount
            self.conn.execute("COMMIT")
            # rowcount is -1 for INSERT in some drivers; treat that as ok.
            return n != 0
        except sqlite3.Error:
            try:
                self.conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            return False
