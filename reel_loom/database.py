from contextlib import contextmanager
import json
import sqlite3
from .schema import SCHEMA, SCHEMA_VERSION
from .errors import LoomError
from .values import canonical, now, identifier, digest


class Database:
    def __init__(self, root):
        self.path = root / "reel_loom.sqlite3"
        self.initialize()

    def connect(self):
        connection = sqlite3.connect(self.path, timeout=15)
        connection.row_factory = sqlite3.Row
        connection.execute("PRAGMA foreign_keys=ON")
        connection.execute("PRAGMA busy_timeout=15000")
        return connection

    def initialize(self):
        with self.connect() as db:
            db.execute("PRAGMA journal_mode=WAL")
            db.executescript(SCHEMA)
            db.execute("CREATE TABLE IF NOT EXISTS derivation_runs(id TEXT PRIMARY KEY, asset TEXT NOT NULL, kind TEXT NOT NULL, settings TEXT NOT NULL, state TEXT NOT NULL, output TEXT, created REAL NOT NULL)")
            row = db.execute("SELECT version FROM schema_info").fetchone()
            if row is None:
                db.execute("INSERT INTO schema_info VALUES(?)", (SCHEMA_VERSION,))
            elif row["version"] != SCHEMA_VERSION:
                raise RuntimeError("Unsupported database schema")
            self._resume_derivations(db)

    def begin_derivation(self, asset_id, kind, settings):
        job = identifier("derived")
        table = "analyses" if kind != "contact_sheet" else "artifacts"
        with self.write_unit() as db:
            row = db.execute(f"SELECT * FROM {table} WHERE asset=? AND kind=? ORDER BY created DESC,id DESC LIMIT 1",
                             (asset_id, kind)).fetchone()
            output = dict(row) if row else None
            if output:
                for field in ("settings", "result", "metadata"):
                    if field in output:
                        output[field] = json.loads(output[field])
            db.execute("INSERT INTO derivation_runs VALUES(?,?,?,?,?,?,?)",
                       (job, asset_id, kind, canonical(settings), "working", canonical(output) if output else None, now()))
            self.event(db, asset_id, "derivation_started", {"job": job, "kind": kind, "settings": settings})
        return job

    def finish_derivation(self, job, result):
        with self.write_unit() as db:
            db.execute("UPDATE derivation_runs SET state='done',output=? WHERE id=?", (canonical(result), job))
            self.event(db, result["asset"], "derivation_finished", {"job": job, "record": result})
        return result

    def fail_derivation(self, job):
        with self.write_unit() as db:
            row = db.execute("SELECT * FROM derivation_runs WHERE id=?", (job,)).fetchone()
            if row is None:
                return
            table = "analyses" if row["kind"] != "contact_sheet" else "artifacts"
            retained = [dict(item) for item in db.execute(f"SELECT * FROM {table} WHERE asset=? AND kind=?", (row["asset"], row["kind"]))]
            self.event(db, row["asset"], "derivation_abandoned", {"job": job, "records": retained})
            db.execute(f"DELETE FROM {table} WHERE asset=? AND kind=?", (row["asset"], row["kind"]))
            db.execute("UPDATE derivation_runs SET state='failed' WHERE id=?", (job,))

    def _resume_derivations(self, db):
        jobs = db.execute("SELECT * FROM derivation_runs WHERE state='working' AND output IS NOT NULL ORDER BY created,id").fetchall()
        for job in jobs:
            output = json.loads(job["output"])
            settings = json.loads(job["settings"])
            asset = db.execute("SELECT blob FROM assets WHERE id=?", (job["asset"],)).fetchone()
            if asset is None:
                continue
            if job["kind"] == "contact_sheet":
                metadata = dict(output["metadata"])
                metadata.update(settings)
                db.execute("INSERT OR IGNORE INTO artifacts VALUES(?,?,?,?,?,?,?)",
                           (job["id"], None, job["asset"], job["kind"], output["blob"], canonical(metadata), now()))
            else:
                fingerprint = digest({"blob": asset["blob"], "kind": job["kind"], "settings": settings, "algorithm": 1})
                db.execute("INSERT OR IGNORE INTO analyses VALUES(?,?,?,?,?,?,?)",
                           (job["id"], job["asset"], job["kind"], canonical(settings), canonical(output["result"]), fingerprint, now()))
            self.event(db, job["asset"], "derivation_recovered", {"job": job["id"], "previous": output["id"]})
            db.execute("UPDATE derivation_runs SET state='done' WHERE id=?", (job["id"],))

    @contextmanager
    def write_unit(self):
        db = self.connect()
        try:
            db.execute("BEGIN IMMEDIATE")
            yield db
            db.commit()
        except sqlite3.IntegrityError as exc:
            db.rollback()
            raise LoomError("integrity_conflict", "The operation conflicts with persisted references", 409) from exc
        except BaseException:
            db.rollback()
            raise
        finally:
            db.close()

    @contextmanager
    def read(self):
        db = self.connect()
        try:
            db.execute("BEGIN")
            yield db
        finally:
            db.rollback()
            db.close()

    def event(self, db, subject, kind, payload):
        db.execute("INSERT INTO events(subject,kind,payload,created) VALUES(?,?,?,?)",
                   (subject, kind, canonical(payload), now()))

    def events(self, subject, after=0, limit=100):
        with self.read() as db:
            rows = db.execute("SELECT * FROM events WHERE subject=? AND sequence>? ORDER BY sequence LIMIT ?",
                              (subject, after, limit)).fetchall()
            return [{**dict(row), "payload": json.loads(row["payload"])} for row in rows]

    def health(self):
        with self.read() as db:
            result = db.execute("PRAGMA quick_check").fetchone()[0]
            states = db.execute("SELECT state,COUNT(*) n FROM tickets GROUP BY state").fetchall()
            return {"database": result, "ticket_states": {r["state"]: r["n"] for r in states}}
