import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, DomainError


def report(**overrides):
    base = {
        "source_id": "SRC-2",
        "contaminant": "bacteria",
        "detected_at": "2026-09-27T07:00:00+00:00",
        "concentration": 30,
        "limit": 10,
        "zone_ids": ["Z-3"],
        "population": 1000,
        "complaints": 2,
        "channel": "online_monitor",
    }
    base.update(overrides)
    return base


class FailureTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)
        self.payload = report()

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_duplicate_submission_idempotent_and_notification_dedup(self):
        result = self.service.ingest_report(self.payload, "a", "analyst")
        item = result["item"]
        self.assertEqual(result["outcome"], "created")

        # 同样内容再交一次：不再计一次，不新增事件、不涨版本
        again = self.service.ingest_report(self.payload, "a", "analyst")
        self.assertEqual(again["outcome"], "duplicate")
        self.assertEqual(again["item"]["id"], item["id"])
        self.assertEqual(again["item"]["version"], 1)
        self.assertEqual(len(self.service.get_item(item["id"])["reports"]), 1)

        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        item = self.service.act(item["id"], "advise", {"notice_id": "N-2", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        with self.assertRaises(DomainError) as context:
            self.service.act(item["id"], "advise", {"notice_id": "N-2", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        self.assertEqual(context.exception.code, "duplicate_notification")

    def test_permission_and_version_conflict_message(self):
        result = self.service.ingest_report(self.payload, "a", "analyst")
        item = result["item"]
        with self.assertRaises(DomainError) as context:
            self.service.act(item["id"], "verify", {"sample_count": 1}, "l", "lab", item["version"])
        self.assertEqual(context.exception.status, 403)

        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        stale_version = item["version"] - 1
        with self.assertRaises(ConflictError) as context:
            self.service.act(item["id"], "advise", {"notice_id": "N-3", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", stale_version)
        self.assertEqual(context.exception.code, "version_conflict")
        # 失败方必须能看清：内容已被更新、当前版本是多少
        self.assertIn("已被其他班组", str(context.exception))
        self.assertEqual(context.exception.details["current_version"], item["version"])
        self.assertEqual(context.exception.details["submitted_version"], stale_version)

    def test_two_crews_submit_disposal_concurrently_one_wins(self):
        result = self.service.ingest_report(report(zone_ids=["Z-3", "Z-4"]), "a", "analyst")
        item = result["item"]
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        item = self.service.act(item["id"], "advise", {"notice_id": "N-4", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        base_version = item["version"]

        # 班组 A、B 基于同一版本同时冲洗各自区域：一个放行，另一个被拒
        item_a = self.service.act(item["id"], "flush", {"zone_id": "Z-3"}, "crew-a", "field_operator", base_version)
        self.assertEqual(item_a["version"], base_version + 1)
        with self.assertRaises(ConflictError) as context:
            self.service.act(item["id"], "flush", {"zone_id": "Z-4"}, "crew-b", "field_operator", base_version)
        self.assertEqual(context.exception.details["current_version"], base_version + 1)

        # B 重新读取最新版本后重试：成功，且自己的处置内容没丢
        latest = self.service.get_item(item["id"])
        item_b = self.service.act(item["id"], "flush", {"zone_id": "Z-4"}, "crew-b", "field_operator", latest["version"])
        self.assertEqual(
            sorted(a["zone_id"] for a in item_b["payload"]["response_actions"] if a["type"] == "flush"),
            ["Z-3", "Z-4"],
        )


if __name__ == "__main__":
    unittest.main()
