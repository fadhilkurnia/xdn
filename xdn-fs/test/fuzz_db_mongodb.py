"""MongoDB adapter for fuzz_db.py.

Runs mongo:7 in docker with /data/db bind-mounted to the fuselog mount.
Standalone (non-replica-set) — sufficient for single-document writes,
which are atomically durable when the WiredTiger journal is enabled
(default). Each insert/update/delete is its own implicit single-doc
transaction; durability comes from journal fsync on write.

Workload model maps to documents:
  {_id: int, name: str, value: int, payload: BinData(0, base64)}
"""

import base64
import json
import os
import subprocess
import time
import uuid
from pathlib import Path


class MongodbAdapter:
    name = "mongodb"
    image = "mongo:7"

    def __init__(self, host_data_dir: Path):
        self.data_dir = Path(host_data_dir)
        self.container = f"fuselog-mongo-{uuid.uuid4().hex[:8]}"

    def start(self):
        self.data_dir.mkdir(parents=True, exist_ok=True)
        uid = os.getuid()
        gid = os.getgid()
        result = subprocess.run(
            [
                "sudo", "docker", "run", "--rm", "-d",
                "--name", self.container,
                "--user", f"{uid}:{gid}",
                "-v", f"{self.data_dir}:/data/db",
                self.image,
                # journalCommitInterval=1 minimizes the window where a
                # durable-ack'd write could be lost on crash. With j:true
                # on the write itself this is belt-and-suspenders.
                "--journalCommitInterval", "1",
            ],
            capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(
                f"docker run failed (rc={result.returncode}):\n"
                f"{result.stderr}"
            )

        # mongosh is bundled in mongo:7. Wait until it can actually query.
        for _ in range(180):
            chk = subprocess.run(
                ["sudo", "docker", "exec", self.container, "mongosh",
                 "--quiet", "--eval", "db.runCommand({ping: 1})"],
                capture_output=True, text=True,
            )
            if chk.returncode == 0 and '"ok": 1' in chk.stdout.replace(' ', ''):
                return
            if chk.returncode == 0 and "ok: 1" in chk.stdout:
                return
            time.sleep(0.5)
        raise RuntimeError(
            f"mongo in {self.container} did not become ready in 90s"
        )

    def init_schema(self):
        # Mongo is schema-less; we just create a unique index on _id
        # (already implicit) and ensure the collection exists. A no-op
        # insert+delete creates the collection on disk.
        js = ('db.items.createIndex({_id: 1});')
        ok = self._mongo("test", js)
        if not ok:
            raise RuntimeError("init_schema failed")

    def kill(self):
        subprocess.run(
            ["sudo", "docker", "rm", "-f", self.container],
            capture_output=True,
        )

    def insert(self, id_, name, value, payload):
        b64 = base64.b64encode(payload).decode()
        js = (
            f"db.items.insertOne("
            f"{{_id: {int(id_)}, name: '{_quote(name)}', "
            f"value: {int(value)}, "
            f"payload: BinData(0, '{b64}')}}, "
            f"{{writeConcern: {{w: 'majority', j: true}}}});"
        )
        return self._mongo("test", js)

    def update(self, id_, name, value, payload):
        b64 = base64.b64encode(payload).decode()
        js = (
            f"db.items.updateOne("
            f"{{_id: {int(id_)}}}, "
            f"{{$set: {{name: '{_quote(name)}', value: {int(value)}, "
            f"payload: BinData(0, '{b64}')}}}}, "
            f"{{writeConcern: {{w: 'majority', j: true}}}});"
        )
        # updateOne returns matchedCount; we'd need to parse it for
        # true success. For our test, success = command didn't error.
        return self._mongo("test", js)

    def delete(self, id_):
        js = (
            f"db.items.deleteOne("
            f"{{_id: {int(id_)}}}, "
            f"{{writeConcern: {{w: 'majority', j: true}}}});"
        )
        return self._mongo("test", js)

    def fetch_all(self):
        # EJSON output is parseable per-line via json.loads.
        out = subprocess.run(
            ["sudo", "docker", "exec", self.container, "mongoexport",
             "--quiet", "--db", "test", "--collection", "items",
             "--sort", '{_id: 1}', "--jsonFormat", "canonical"],
            capture_output=True, text=True,
        )
        rows = []
        for line in out.stdout.splitlines():
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            id_ = int(doc["_id"]["$numberInt"]) if isinstance(doc["_id"], dict) \
                else int(doc["_id"])
            name = doc["name"]
            v = doc.get("value", 0)
            value = int(v["$numberInt"]) if isinstance(v, dict) else int(v)
            payload_field = doc.get("payload")
            if payload_field is None:
                payload = b""
            elif isinstance(payload_field, dict) and "$binary" in payload_field:
                payload = base64.b64decode(payload_field["$binary"]["base64"])
            else:
                payload = bytes(payload_field, "latin1")
            rows.append((id_, name, value, payload))
        return rows

    def _mongo(self, db, js):
        result = subprocess.run(
            ["sudo", "docker", "exec", "-i", self.container, "mongosh",
             "--quiet", "--norc", db],
            input=js, capture_output=True, text=True,
        )
        if result.returncode != 0:
            return False
        # mongosh exits 0 even on JS exceptions; check stderr-style output.
        out = result.stdout + result.stderr
        if "MongoServerError" in out or "uncaught exception" in out:
            return False
        return True


def _quote(s):
    return s.replace("'", "\\'").replace("\\", "\\\\")
