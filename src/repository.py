import json
import sqlite3
import uuid
from datetime import datetime, timezone

from .audit import audit_hash, canonical_json
from .domain import ConflictError, NotFoundError, DomainError
from .rules import (
    ENTITY_TYPE,
    INITIAL_STATUS,
    build_initial_payload,
    derive_status,
    merge_report,
    parse_iso,
    within_merge_window,
    describe_report,
)


def now_iso():
    return datetime.now(timezone.utc).isoformat()


class Repository:
    def __init__(self, path):
        self.path = path

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=30, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
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
                CREATE TABLE IF NOT EXISTS reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    item_id INTEGER NOT NULL,
                    fingerprint TEXT NOT NULL,
                    channel TEXT NOT NULL,
                    detected_at TEXT NOT NULL,
                    received_at TEXT NOT NULL,
                    payload TEXT NOT NULL,
                    created_by TEXT NOT NULL,
                    created_role TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    UNIQUE(fingerprint),
                    FOREIGN KEY(item_id) REFERENCES items(id)
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
                CREATE INDEX IF NOT EXISTS idx_reports_item ON reports(item_id);
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

    # ------------------------------------------------------------------
    # 上报接入（核心）：同水源/同污染物/检测时间相差不到半小时 -> 同一事件
    # ------------------------------------------------------------------

    def _find_merge_target(self, conn, report):
        rows = conn.execute(
            """
            SELECT i.* FROM items i
            JOIN reports r ON r.item_id = i.id
            WHERE i.entity_type = ?
              AND i.status != 'cancelled'
              AND json_extract(r.payload, '$.source_id') = ?
              AND json_extract(r.payload, '$.contaminant') = ?
            """,
            (ENTITY_TYPE, report["source_id"], report["contaminant"]),
        ).fetchall()
        candidates = []
        for row in rows:
            report_rows = conn.execute(
                "SELECT payload FROM reports WHERE item_id=?", (row["id"],)
            ).fetchall()
            best = None
            for rr in report_rows:
                stored = json.loads(rr["payload"])
                if within_merge_window(stored["detected_at"], report["detected_at"]):
                    delta = abs(parse_iso(stored["detected_at"]) - parse_iso(report["detected_at"]))
                    if best is None or delta < best:
                        best = delta
            if best is not None:
                candidates.append((best, int(row["id"]), row))
        if not candidates:
            return None
        # 时间差最近者优先；并列时并入最新事件
        candidates.sort(key=lambda entry: (entry[0], -entry[1]))
        return candidates[0][2]

    def _ingest_locked(self, conn, report, actor, role, received_at,
                       batched_index=None, prior_ingested=None):
        # 1) 全局幂等：重复提交不再计一次
        existing = conn.execute(
            "SELECT item_id FROM reports WHERE fingerprint=?", (report["fingerprint"],)
        ).fetchone()
        if existing is not None:
            row = conn.execute("SELECT * FROM items WHERE id=?", (existing["item_id"],)).fetchone()
            return {
                "outcome": "duplicate",
                "item": self._row_to_item(row),
                "new_zones": [],
                "fingerprint": report["fingerprint"],
            }

        # 2) 同批次内已写入的上报也参与合并窗口
        target_row = self._find_merge_target(conn, report)
        if target_row is None and prior_ingested:
            best = None
            best_item_id = None
            for entry in prior_ingested:
                ing = entry["report"]
                if ing["source_id"] == report["source_id"] and \
                        ing["contaminant"] == report["contaminant"] and \
                        within_merge_window(ing["detected_at"], report["detected_at"]):
                    delta = abs(parse_iso(ing["detected_at"]) - parse_iso(report["detected_at"]))
                    if best is None or delta < best:
                        best, best_item_id = delta, entry["item_id"]
            if best_item_id is not None:
                target_row = conn.execute("SELECT * FROM items WHERE id=?", (best_item_id,)).fetchone()

        stored_report = dict(report)
        stored_report["received_at"] = received_at

        if target_row is None:
            # 3) 新事件
            payload = build_initial_payload(report)
            ts = now_iso()
            stable_key = "pending:%s" % uuid.uuid4().hex  # 占位，插入 report 后用其 id 回填
            cur = conn.execute(
                "INSERT INTO items(entity_type,stable_key,status,version,payload,created_by,created_role,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (ENTITY_TYPE, stable_key, INITIAL_STATUS, 1, canonical_json(payload),
                 actor, role, ts, ts),
            )
            item_id = cur.lastrowid
            conn.execute(
                "INSERT INTO reports(item_id,fingerprint,channel,detected_at,received_at,payload,created_by,created_role,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (item_id, report["fingerprint"], report["channel"], report["detected_at"],
                 received_at, canonical_json(stored_report), actor, role, ts),
            )
            report_id = conn.execute("SELECT last_insert_rowid() AS id").fetchone()["id"]
            conn.execute(
                "UPDATE items SET stable_key=? WHERE id=?",
                ("report:%d" % report_id, item_id),
            )
            self.append_audit(conn, item_id, "report_received", actor, role, {
                "report_id": report_id,
                "channel": report["channel"],
                "outcome": "created",
                "detected_at": report["detected_at"],
                "received_at": received_at,
                "zones": report["zone_ids"],
            })
            row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
            return {
                "outcome": "created",
                "item": self._row_to_item(row),
                "report_id": report_id,
                "new_zones": list(report["zone_ids"]),
                "fingerprint": report["fingerprint"],
            }

        # 4) 并入已有事件（事件正在通知、冲洗、甚至已恢复时都可并入）
        item_id = target_row["id"]
        item = self._row_to_item(target_row)
        new_payload, new_zones = merge_report(item["payload"], report)
        new_payload["merged_report_ids"] = list(new_payload.get("merged_report_ids", []))
        new_status = derive_status(new_payload)
        ts = now_iso()
        cur = conn.execute(
            "INSERT INTO reports(item_id,fingerprint,channel,detected_at,received_at,payload,created_by,created_role,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (item_id, report["fingerprint"], report["channel"], report["detected_at"],
             received_at, canonical_json(stored_report), actor, role, ts),
        )
        report_id = cur.lastrowid
        new_payload["merged_report_ids"].append(report_id)
        version = int(target_row["version"]) + 1
        conn.execute(
            "UPDATE items SET status=?,version=?,payload=?,updated_at=? WHERE id=?",
            (new_status, version, canonical_json(new_payload), ts, item_id),
        )
        self.append_audit(conn, item_id, "report_merged", actor, role, {
            "report_id": report_id,
            "channel": report["channel"],
            "detected_at": report["detected_at"],
            "received_at": received_at,
            "new_zones": new_zones,
            "zones": report["zone_ids"],
            "new_status": new_status,
            "description": describe_report(report),
        })
        row = conn.execute("SELECT * FROM items WHERE id=?", (item_id,)).fetchone()
        return {
            "outcome": "merged",
            "item": self._row_to_item(row),
            "report_id": report_id,
            "new_zones": new_zones,
            "fingerprint": report["fingerprint"],
        }

    def ingest_report(self, report, actor, role, received_at=None):
        conn = self.connect()
        received_at = received_at or now_iso()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                result = self._ingest_locked(conn, report, actor, role, received_at)
            except sqlite3.IntegrityError as exc:
                # 并发下两个相同上报同时插入：输掉的一方按重复处理，不再计一次
                existing = conn.execute(
                    "SELECT item_id FROM reports WHERE fingerprint=?",
                    (report["fingerprint"],),
                ).fetchone()
                if existing is not None:
                    row = conn.execute("SELECT * FROM items WHERE id=?", (existing["item_id"],)).fetchone()
                    conn.execute("COMMIT")
                    return {
                        "outcome": "duplicate",
                        "item": self._row_to_item(row),
                        "new_zones": [],
                        "fingerprint": report["fingerprint"],
                    }
                raise ConflictError("duplicate_item", "同一业务实体已经存在") from exc
            conn.execute("COMMIT")
            return result
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def ingest_reports(self, reports, actor, role, received_at=None):
        """断网攒下的上报网络恢复后一起提交：按到达顺序逐条合并，迟到的也计入。"""
        conn = self.connect()
        received_at = received_at or now_iso()
        results = []
        try:
            conn.execute("BEGIN IMMEDIATE")
            prior = []
            for index, report in enumerate(reports):
                try:
                    result = self._ingest_locked(
                        conn, report, actor, role, received_at,
                        batched_index=index, prior_ingested=prior,
                    )
                except sqlite3.IntegrityError as exc:
                    existing = conn.execute(
                        "SELECT item_id FROM reports WHERE fingerprint=?",
                        (report["fingerprint"],),
                    ).fetchone()
                    if existing is not None:
                        row = conn.execute("SELECT * FROM items WHERE id=?", (existing["item_id"],)).fetchone()
                        result = {
                            "outcome": "duplicate",
                            "item": self._row_to_item(row),
                            "new_zones": [],
                            "fingerprint": report["fingerprint"],
                        }
                    else:
                        raise ConflictError("duplicate_item", "同一业务实体已经存在") from exc
                if result["outcome"] in ("created", "merged"):
                    prior.append({"report": report, "item_id": result["item"]["id"]})
                results.append(result)
            conn.execute("COMMIT")
            return results
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        finally:
            conn.close()

    def list_reports(self, item_id):
        conn = self.connect()
        try:
            rows = conn.execute(
                "SELECT id,item_id,fingerprint,channel,detected_at,received_at,payload,created_by,created_role,created_at FROM reports WHERE item_id=? ORDER BY detected_at, id",
                (item_id,),
            ).fetchall()
            result = []
            for row in rows:
                value = dict(row)
                value["payload"] = json.loads(value["payload"])
                result.append(value)
            return result
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # 兼容接口（保留旧用例与来源登记功能）
    # ------------------------------------------------------------------

    def create_item(self, entity_type, stable_key, initial_status, payload, actor, role):
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
            self.append_audit(conn, item_id, "created", actor, role, {"stable_key": stable_key})
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
                    "事件内容已被其他班组的提交更新（当前版本 %d，你提交时基于版本 %d），请重新读取最新内容后再提交"
                    % (int(row["version"]), int(expected_version)),
                    {
                        "current_version": int(row["version"]),
                        "submitted_version": int(expected_version),
                        "current_status": row["status"],
                    },
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
