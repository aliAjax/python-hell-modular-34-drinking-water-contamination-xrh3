import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service
from src.domain import ConflictError, DomainError


class MergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _report(self, source_id, contaminant, detected_at, zones, concentration=20, limit=10, **extra):
        payload = {
            "source_id": source_id,
            "contaminant": contaminant,
            "detected_at": detected_at,
            "concentration": concentration,
            "limit": limit,
            "zone_ids": zones,
            "population": 1000,
            "complaints": 1,
        }
        payload.update(extra)
        return payload

    def test_same_source_contaminant_within_half_hour_merges(self):
        first = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        second = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:10:00+00:00", ["Z-2"], source_type="lab", external_id="LAB-1"),
            "a", "analyst",
        )
        self.assertEqual(second["id"], first["id"])
        self.assertEqual(len(second["payload"]["reports"]), 2)
        self.assertEqual(second["payload"]["zone_ids"], ["Z-1", "Z-2"])
        detected = {r["detected_at"] for r in second["payload"]["reports"]}
        self.assertIn("2026-09-27T08:00:00+00:00", detected)
        self.assertIn("2026-09-27T08:10:00+00:00", detected)

    def test_duplicate_submission_does_not_create_event(self):
        first = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        again = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        self.assertEqual(again["id"], first["id"])
        self.assertEqual(len(again["payload"]["reports"]), 1)

    def test_different_source_or_contaminant_or_late_creates_separate_events(self):
        base = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        diff_source = self.service.create_item(
            self._report("SRC-2", "nitrate", "2026-09-27T08:05:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        diff_contaminant = self.service.create_item(
            self._report("SRC-1", "bacteria", "2026-09-27T08:05:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        late = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:31:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        self.assertNotEqual(diff_source["id"], base["id"])
        self.assertNotEqual(diff_contaminant["id"], base["id"])
        self.assertNotEqual(late["id"], base["id"])

    def test_merge_during_flushing_only_new_zones_treated(self):
        item = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        item = self.service.act(item["id"], "advise", {"notice_id": "N-1", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "f", "field_operator", item["version"])
        self.assertEqual(item["status"], "flushing")
        merged = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:15:00+00:00", ["Z-2"], source_type="hotline", external_id="HOT-1"),
            "a", "analyst",
        )
        self.assertEqual(merged["id"], item["id"])
        self.assertEqual(merged["payload"]["zones"]["Z-1"]["status"], "flushing")
        self.assertEqual(merged["payload"]["zones"]["Z-2"]["status"], "pending")
        item = merged
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-1", "completed": True}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-1", "zone_id": "Z-1", "concentration": 2}, "l", "lab", item["version"])
        self.assertEqual(item["payload"]["zones"]["Z-1"]["status"], "sampled")
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-2"}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-2", "completed": True}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-2", "zone_id": "Z-2", "concentration": 2}, "l", "lab", item["version"])
        item = self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "c", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")

    def test_recovered_zone_preserved_when_new_zone_merges(self):
        item = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        item = self.service.act(item["id"], "advise", {"notice_id": "N-1", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-1", "completed": True}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-1", "zone_id": "Z-1", "concentration": 2}, "l", "lab", item["version"])
        self.assertEqual(item["payload"]["zones"]["Z-1"]["status"], "sampled")
        merged = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:20:00+00:00", ["Z-3"], source_type="lab", external_id="LAB-2"),
            "a", "analyst",
        )
        self.assertEqual(merged["id"], item["id"])
        self.assertEqual(merged["payload"]["zones"]["Z-1"]["status"], "sampled")
        self.assertEqual(merged["payload"]["zones"]["Z-3"]["status"], "pending")
        self.assertEqual(merged["status"], "flushing")
        item = merged
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-3"}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-3", "completed": True}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-3", "zone_id": "Z-3", "concentration": 2}, "l", "lab", item["version"])
        item = self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "c", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")

    def test_restore_blocked_while_any_zone_not_cleared(self):
        item = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1", "Z-2"]),
            "a", "analyst",
        )
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        item = self.service.act(item["id"], "advise", {"notice_id": "N-1", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-1", "completed": True}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-1", "zone_id": "Z-1", "concentration": 2}, "l", "lab", item["version"])
        with self.assertRaises(DomainError) as context:
            self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "c", "coordinator", item["version"])
        self.assertEqual(context.exception.code, "zones_not_cleared")
        self.assertIn("Z-2", str(context.exception))

    def test_version_conflict_message_says_content_updated(self):
        item = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        with self.assertRaises(ConflictError) as context:
            self.service.act(item["id"], "advise", {"notice_id": "N-1", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"] - 1)
        self.assertIn("内容已被其他操作更新", str(context.exception))

    def test_late_report_merges_by_detection_time(self):
        item = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        late = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:05:00+00:00", ["Z-2"], source_type="hotline", external_id="HOT-2"),
            "a", "analyst",
        )
        self.assertEqual(late["id"], item["id"])
        self.assertEqual(len(late["payload"]["reports"]), 2)

    def test_late_report_after_restore_with_new_zone_reopens(self):
        item = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:00:00+00:00", ["Z-1"]),
            "a", "analyst",
        )
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        item = self.service.act(item["id"], "advise", {"notice_id": "N-1", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-1", "completed": True}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-1", "zone_id": "Z-1", "concentration": 2}, "l", "lab", item["version"])
        item = self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "c", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")
        late = self.service.create_item(
            self._report("SRC-1", "nitrate", "2026-09-27T08:08:00+00:00", ["Z-4"], source_type="hotline", external_id="HOT-3"),
            "a", "analyst",
        )
        self.assertEqual(late["id"], item["id"])
        self.assertEqual(len(late["payload"]["reports"]), 2)
        self.assertEqual(late["payload"]["zones"]["Z-1"]["status"], "sampled")
        self.assertEqual(late["payload"]["zones"]["Z-4"]["status"], "pending")
        self.assertEqual(late["status"], "flushing")


if __name__ == "__main__":
    unittest.main()
