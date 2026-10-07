import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service


def report(**overrides):
    base = {
        "source_id": "SRC-1",
        "contaminant": "nitrate",
        "detected_at": "2026-09-27T06:00:00+00:00",
        "concentration": 20,
        "limit": 10,
        "zone_ids": ["Z-1", "Z-2"],
        "population": 5000,
        "complaints": 4,
        "channel": "online_monitor",
    }
    base.update(overrides)
    return base


class WorkflowTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def _verify_and_notify(self, item):
        item = self.service.act(item["id"], "verify", {"sample_count": 2}, "analyst-1", "analyst", item["version"])
        self.assertEqual(item["payload"]["assessment"]["level"], "high")
        item = self.service.act(item["id"], "advise", {"notice_id": "N-1", "kind": "boil", "message": "煮沸"}, "disp-1", "dispatcher", item["version"])
        return item

    def _clear_zone(self, item, zone, sample_id, concentration=2):
        item = self.service.act(item["id"], "flush", {"zone_id": zone}, "field-1", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": zone, "completed": True}, "field-1", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": sample_id, "zone_id": zone, "concentration": concentration}, "lab-1", "lab", item["version"])
        return item

    def test_complete_water_response_workflow(self):
        result = self.service.ingest_report(report(), "analyst-1", "analyst")
        item = result["item"]
        item = self._verify_and_notify(item)
        item = self.service.act(item["id"], "switch_source", {"alternate_source_id": "ALT-1"}, "coord-1", "coordinator", item["version"])
        item = self._clear_zone(item, "Z-1", "S-1")
        # 只要有区域还在冲洗/未复检，事件不能恢复
        with self.assertRaises(Exception) as context:
            self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "coord-1", "coordinator", item["version"])
        self.assertEqual(context.exception.code, "zones_not_cleared")
        item = self._clear_zone(item, "Z-2", "S-2")
        item = self.service.act(item["id"], "restore", {"all_zones_cleared": True}, "coord-1", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")
        self.assertEqual(item["zone_summary"]["by_status"], {"restored": 2})
        self.assertGreaterEqual(len(item["audit"]), 11)

    def test_new_zone_only_flushes_itself_while_restored_zone_kept(self):
        result = self.service.ingest_report(report(zone_ids=["Z-1"]), "analyst-1", "analyst")
        item = result["item"]
        item = self._verify_and_notify(item)
        item = self._clear_zone(item, "Z-1", "S-1")
        item = self.service.act(item["id"], "restore", {}, "coord-1", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")

        # 事件恢复后，热线在同一窗口上报同一污染物，带来新区域 Z-2
        merged = self.service.ingest_report(
            report(channel="hotline", detected_at="2026-09-27T06:20:00+00:00",
                   zone_ids=["Z-2"], complaints=3),
            "hotline-1", "dispatcher",
        )
        self.assertEqual(merged["outcome"], "merged")
        item = self.service.get_item(merged["item"]["id"])
        self.assertEqual(item["payload"]["zones"]["Z-1"]["status"], "restored")
        self.assertEqual(item["payload"]["zones"]["Z-2"]["status"], "detected")
        self.assertNotEqual(item["status"], "restored")

        # 新区域重新走通知后的冲洗、消毒、复检；Z-1 保持已恢复
        item = self.service.act(item["id"], "advise", {"notice_id": "N-2", "kind": "do_not_drink", "message": "暂停饮用"}, "disp-1", "dispatcher", item["version"])
        item = self._clear_zone(item, "Z-2", "S-9")
        self.assertEqual(item["payload"]["zones"]["Z-1"]["status"], "restored")
        item = self.service.act(item["id"], "restore", {}, "coord-1", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")

    def test_merge_while_flushing_only_new_zone_needs_flush(self):
        result = self.service.ingest_report(report(zone_ids=["Z-1"]), "m", "analyst")
        item = result["item"]
        item = self._verify_and_notify(item)
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "field-1", "field_operator", item["version"])
        self.assertEqual(item["status"], "flushing")

        # 实验室复检在冲洗期间并入，带来 Z-2
        merged = self.service.ingest_report(
            report(channel="lab", detected_at="2026-09-27T06:25:00+00:00", zone_ids=["Z-2"]),
            "lab-1", "lab",
        )
        self.assertEqual(merged["outcome"], "merged")
        item = self.service.get_item(merged["item"]["id"])
        self.assertEqual(item["payload"]["zones"]["Z-1"]["status"], "flushing")
        self.assertEqual(item["payload"]["zones"]["Z-2"]["status"], "detected")
        # 仍有区域在冲洗 -> 事件显示 flushing，不是已恢复
        self.assertEqual(item["status"], "flushing")
        with self.assertRaises(Exception) as context:
            self.service.act(item["id"], "disinfect", {"zone_id": "Z-2", "completed": True}, "field-1", "field_operator", item["version"])
        self.assertEqual(context.exception.code, "zone_not_flushing")

    def test_event_not_restored_while_any_zone_pending(self):
        result = self.service.ingest_report(report(zone_ids=["Z-1", "Z-2", "Z-3"]), "m", "analyst")
        item = result["item"]
        item = self._verify_and_notify(item)
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "field-1", "field_operator", item["version"])
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-2"}, "field-1", "field_operator", item["version"])
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-3"}, "field-1", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-1", "completed": True}, "field-1", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-2", "completed": True}, "field-1", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-1", "zone_id": "Z-1", "concentration": 1}, "lab-1", "lab", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-2", "zone_id": "Z-2", "concentration": 1}, "lab-1", "lab", item["version"])
        self.assertEqual(item["status"], "sampled")
        self.assertEqual(item["zone_summary"]["by_status"], {"cleared": 2, "flushing": 1})
        with self.assertRaises(Exception) as context:
            self.service.act(item["id"], "restore", {}, "coord-1", "coordinator", item["version"])
        self.assertEqual(context.exception.code, "zones_not_cleared")
        self.assertEqual(context.exception.details["not_cleared_zones"], ["Z-3"])


    def test_failed_sample_sends_zone_back_to_flush_and_can_recover(self):
        result = self.service.ingest_report(report(zone_ids=["Z-1"]), "m", "analyst")
        item = result["item"]
        item = self._verify_and_notify(item)
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-1"}, "field-1", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-1", "completed": True}, "field-1", "field_operator", item["version"])
        # 首次复检不达标：区域退回冲洗，事件不能恢复
        item = self.service.act(item["id"], "sample", {"sample_id": "S-bad", "zone_id": "Z-1", "concentration": 30}, "lab-1", "lab", item["version"])
        self.assertEqual(item["payload"]["zones"]["Z-1"]["status"], "flushing")
        self.assertEqual(item["status"], "flushing")
        # 重新冲洗、消毒、复检通过后恢复（早前不合格结果不阻塞）
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-1", "completed": True}, "field-1", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-good", "zone_id": "Z-1", "concentration": 1}, "lab-1", "lab", item["version"])
        self.assertEqual(item["status"], "sampled")
        item = self.service.act(item["id"], "restore", {}, "coord-1", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")


if __name__ == "__main__":
    unittest.main()
