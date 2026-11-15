from contextlib import contextmanager
import json
import sqlite3
from .schema import SCHEMA, SCHEMA_VERSION
from .errors import LoomError, require
from .values import canonical, now, identifier

DERIVATION_KINDS = ("signal", "waveform", "loudness", "contact_sheet")
# Marker recorded in derivation_runs.output for runs that never produced anything.
# A finished run always carries the real canonicalized result; NULL alone cannot
# distinguish "never wrote output" from "interrupted while writing", so the
# explicit marker keeps unfinished work from ever being mistaken for a product.
FAILED_MARKER = {"loom_state": "failed", "reason": "interrupted_before_commit"}


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
        """Start a derivation attempt in its own transaction.

        The run only records what this attempt was asked to do; it never copies
        any pre-existing product, so a later failure cannot describe, or be
        recovered into, output belonging to a different (successful) attempt.
        """
        require(kind in DERIVATION_KINDS, "analysis_kind", "Unknown derivation kind")
        job = identifier("derived")
        with self.write_unit() as db:
            db.execute("INSERT INTO derivation_runs VALUES(?,?,?,?,?,?,?)",
                       (job, asset_id, kind, canonical(settings), "working", None, now()))
            self.event(db, asset_id, "derivation_started", {"job": job, "kind": kind, "settings": settings})
        return job

    def finish_derivation(self, job, result):
        """Publish a product and close the run atomically.

        The run row is matched on its working state, so a run already settled as
        failed can never overwrite a product published by another attempt.
        """
        with self.write_unit() as db:
            row = db.execute("SELECT * FROM derivation_runs WHERE id=?", (job,)).fetchone()
            require(row is not None, "derivation_missing", "Derivation run does not exist", 409)
            require(row["state"] == "working", "derivation_state",
                    "Only an active derivation can publish results", 409)
            db.execute("UPDATE derivation_runs SET state='done',output=? WHERE id=? AND state='working'",
                       (canonical(result), job))
            self.event(db, result["asset"], "derivation_finished", {"job": job, "record": result})
        return result

    def fail_derivation(self, job):
        """Abandon a failed attempt without touching anyone else's product.

        Only this job's own run is marked. Deleting rows by asset/kind would
        destroy results committed by a concurrent successful attempt, so the
        analyses/artifacts tables are left strictly untouched here.
        """
        with self.write_unit() as db:
            row = db.execute("SELECT * FROM derivation_runs WHERE id=? AND state='working'", (job,)).fetchone()
            if row is None:
                return
            db.execute("UPDATE derivation_runs SET state='failed',output=? WHERE id=?",
                       (canonical(FAILED_MARKER), job))
            self.event(db, row["asset"], "derivation_abandoned", {"job": job, "kind": row["kind"]})

    def _resume_derivations(self, db):
        """Settle runs interrupted by a process exit.

        A run that never reached its atomic publish step has no real product
        (output is NULL, or only a failure marker): mark it interrupted/failed so
        unfinished work can never be handed out as a finished result. Nothing is
        fabricated into analyses/artifacts; genuinely committed products live in
        their own tables and remain readable regardless of run state.
        """
        jobs = db.execute("SELECT * FROM derivation_runs WHERE state='working' ORDER BY created,id").fetchall()
        for job in jobs:
            output = json.loads(job["output"]) if job["output"] else None
            if not output or output == FAILED_MARKER:
                db.execute("UPDATE derivation_runs SET state='failed',output=? WHERE id=?",
                           (canonical(FAILED_MARKER), job["id"]))
                self.event(db, job["asset"], "derivation_interrupted",
                           {"job": job["id"], "kind": job["kind"]})
                continue
            # Legacy rows from older builds may already carry a published result.
            # Only accept output that references a product which really exists.
            table = "analyses" if job["kind"] != "contact_sheet" else "artifacts"
            product = db.execute(f"SELECT id FROM {table} WHERE id=?", (output["id"],)).fetchone()
            if product is None:
                db.execute("UPDATE derivation_runs SET state='failed',output=? WHERE id=?",
                           (canonical(FAILED_MARKER), job["id"]))
                self.event(db, job["asset"], "derivation_interrupted",
                           {"job": job["id"], "kind": job["kind"]})
                continue
            db.execute("UPDATE derivation_runs SET state='done' WHERE id=?", (job["id"],))
            self.event(db, job["asset"], "derivation_recovered",
                       {"job": job["id"], "record": output["id"]})

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
