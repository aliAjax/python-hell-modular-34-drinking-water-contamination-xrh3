import json
import sqlite3
from datetime import datetime, timezone

from . import domain
from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path):
        self.path = path

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self):
        conn = self.connect()
        try:
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    entity_type TEXT NOT NULL,
                    stable_key TEXT NOT NULL,
                    status TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    UNIQUE(entity_type, stable_key)
                );
                CREATE TABLE IF NOT EXISTS sources (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    observed_at TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(item_id, source_type, external_id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    action TEXT NOT NULL,
                    actor TEXT NOT NULL,
                    role TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                CREATE TABLE IF NOT EXISTS audit_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER,
                    event_type TEXT NOT NULL,
                    actor TEXT,
                    role TEXT,
                    payload TEXT NOT NULL,
                    previous_hash TEXT NOT NULL,
                    event_hash TEXT NOT NULL,
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    source_id TEXT NOT NULL,
                    contaminant TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    source_type TEXT NOT NULL,
                    external_id TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(source_id, contaminant, source_type, external_id),
                    FOREIGN KEY(item_id) REFERENCES items(id)
                );
                """
            )
        finally:
            conn.close()

    def _row_to_item(self, row):
        if row is None:
            return None
        result = dict(row)
        result["payload"] = json.loads(result["payload"])
        return result

    def _last_hash(self, conn, item_id):
        row = conn.execute(
            "SELECT event_hash FROM audit_events WHERE item_id IS ? ORDER BY id DESC LIMIT 1",
            (item_id,),
        ).fetchone()
        return row["event_hash"] if row else "GENESIS"

    def append_audit(self, conn, item_id, event_type, actor, role, payload):
        previous = self._last_hash(conn, item_id)
        event = {
            "item_id": item_id,
            "event_type": event_type,
            "actor": actor,
            "role": role,
            "payload": payload,
            "created_at": now_iso(),
        }
        event_hash = audit_hash(previous, event)
        conn.execute(
            "INSERT INTO audit_events(item_id,event_type,actor,role,payload,previous_hash,event_hash,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (item_id, event_type, actor, role, canonical_json(payload), previous, event_hash, event["created_at"]),
        )

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role, report):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                    (
                        entity_type,
                        stable_key,
                        initial_status,
                        1,
                        canonical_json(payload),
                        actor,
                        role,
                        now_iso(),
                        now_iso(),
                    ),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_item", "同一业务实体已经存在")
            item_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            report_id = self._insert_report(conn, item_id, report)
            payload["reports"] = [domain.report_summary(report, report_id)]
            conn.execute(
                "UPDATE items SET payload=? WHERE id=?",
                (canonical_json(payload), item_id),
            )
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key})
            self.append_audit(
                conn,
                item_id,
                "report_recorded",
                actor,
                role,
                {"report_id": report_id, "source_type": report["source_type"], "external_id": report["external_id"]},
            )
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def _insert_report(self, conn, item_id, report):
        report_payload = {
            "source_id": report["source_id"],
            "contaminant": report["contaminant"],
            "detected_at": report["detected_at"],
            "concentration": report["concentration"],
            "limit": report["limit"],
            "zone_ids": list(report["zone_ids"]),
            "population": report["population"],
            "complaints": report["complaints"],
            "source_type": report["source_type"],
            "external_id": report["external_id"],
        }
        conn.execute(
            "INSERT INTO reports(item_id,source_id,contaminant,detected_at,source_type,external_id,payload,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (
                item_id,
                report["source_id"],
                report["contaminant"],
                report["detected_at"],
                report["source_type"],
                report["external_id"],
                canonical_json(report_payload),
                now_iso(),
            ),
        )
        return conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]

    def find_merge_target(self, source_id, contaminant, detected_at):
        conn = self.connect()
        try:
            target_dt = datetime.fromisoformat(detected_at.replace("Z", "+00:00"))
            rows = conn.execute(
                "SELECT item_id, detected_at FROM reports WHERE source_id=? AND contaminant=?",
                (source_id, contaminant),
            ).fetchall()
            best_item_id = None
            best_diff = None
            for row in rows:
                report_dt = datetime.fromisoformat(row["detected_at"].replace("Z", "+00:00"))
                diff = abs((target_dt - report_dt).total_seconds())
                if diff < 1800 and (best_diff is None or diff < best_diff):
                    best_item_id = row["item_id"]
                    best_diff = diff
            if best_item_id is None:
                return None
            item = conn.execute("SELECT * FROM items WHERE id=?", (best_item_id,)).fetchone()
            if item is None or item["status"] == "cancelled":
                return None
            return self._row_to_item(item)
        finally:
            conn.close()

    def merge_report(self, item_id, report, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if row["status"] == "cancelled":
                raise ConflictError("item_cancelled", "事件已取消，不能并入上报")
            existing = conn.execute(
                "SELECT id FROM reports WHERE source_id=? AND contaminant=? AND source_type=? AND external_id=?",
                (report["source_id"], report["contaminant"], report["source_type"], report["external_id"]),
            ).fetchone()
            if existing:
                conn.execute("COMMIT")
                result = self._row_to_item(row)
                result["_duplicate"] = True
                return result
            report_id = self._insert_report(conn, item_id, report)
            payload = json.loads(row["payload"])
            new_zones = domain.merge_zones(payload, report["zone_ids"])
            payload.setdefault("reports", []).append(domain.report_summary(report, report_id))
            new_status = row["status"]
            if new_zones and new_status in {"disinfected", "sampled", "restored"}:
                new_status = "flushing"
            conn.execute(
                "UPDATE items SET status=?,version=version+1,payload=?,updated_at=? WHERE id=?",
                (new_status, canonical_json(payload), now_iso(), item_id),
            )
            self.append_audit(
                conn,
                item_id,
                "report_merged",
                actor,
                role,
                {
                    "report_id": report_id,
                    "source_type": report["source_type"],
                    "external_id": report["external_id"],
                    "new_zones": new_zones,
                },
            )
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            conn.execute("COMMIT")
            result = self._row_to_item(row)
            result["_merged"] = True
            result["_new_zones"] = new_zones
            return result
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def get_item(self, item_id):
        conn = self.connect()
        try:
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            return self._row_to_item(row)
        finally:
            conn.close()

    def list_items(self, status=None):
        conn = self.connect()
        try:
            if status:
                rows = conn.execute("SELECT * FROM items WHERE status=? ORDER BY id DESC", (status,)).fetchall()
            else:
                rows = conn.execute("SELECT * FROM items ORDER BY id DESC").fetchall()
            return [self._row_to_item(row) for row in rows]
        finally:
            conn.close()

    def add_source(self, item_id, source_type, external_id, payload, observed_at, actor, role):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            item = conn.execute("SELECT id FROM items WHERE id=?", (item_id,)).fetchone()
            if item is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            try:
                conn.execute(
                    "INSERT INTO sources(item_id,source_type,external_id,payload,observed_at,created_at) VALUES(?,?,?,?,?,?)",
                    (item_id, source_type, external_id, canonical_json(payload), observed_at, now_iso()),
                )
            except sqlite3.IntegrityError:
                raise ConflictError("duplicate_source", "同一来源记录已经提交")
            source_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            self.append_audit(
                conn,
                item_id,
                "source_recorded",
                actor,
                role,
                {"source_id": source_id, "source_type": source_type, "external_id": external_id},
            )
            conn.execute("COMMIT")
            return {"id": source_id, "item_id": item_id, "source_type": source_type, "external_id": external_id, "payload": payload, "observed_at": observed_at}
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_sources(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM sources WHERE item_id=? ORDER BY id DESC", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def apply_action(self, item_id, action, actor, role, new_status, new_payload, event_payload, expected_version=None):
        conn = self.connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            if row is None:
                raise NotFoundError("item_not_found", "业务实体不存在")
            if expected_version is not None and int(expected_version) != int(row["version"]):
                raise ConflictError(
                    "version_conflict",
                    "内容已被其他操作更新（当前版本 %s），请重新读取后再提交" % row["version"],
                )
            version = int(row["version"]) + 1
            conn.execute(
                "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
                (new_status, version, canonical_json(new_payload), now_iso(), item_id),
            )
            conn.execute(
                "INSERT INTO actions(item_id,action,actor,role,payload,created_at) VALUES(?,?,?,?,?,?)",
                (item_id, action, actor, role, canonical_json(event_payload), now_iso()),
            )
            self.append_audit(conn, item_id, action, actor, role, event_payload)
            conn.execute("COMMIT")
            return self.get_item(item_id)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def audit_trail(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute("SELECT * FROM audit_events WHERE item_id=? ORDER BY id", (item_id,)).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    def state_summary(self):
        conn = self.connect()
        try:
            counts = {}
            for row in conn.execute("SELECT status, COUNT(*) AS total FROM items GROUP BY status").fetchall():
                counts[row["status"]] = row["total"]
            return {"counts": counts, "items": self.list_items()}
        finally:
            conn.close()
