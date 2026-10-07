import os
import sys
import tempfile
import unittest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from src.repository import Repository
from src.service import Service


def report(**overrides):
    base = {
        "source_id": "SRC-9",
        "contaminant": "lead",
        "detected_at": "2026-10-01T08:00:00+00:00",
        "concentration": 15,
        "limit": 10,
        "zone_ids": ["Z-A"],
        "population": 800,
        "complaints": 1,
        "channel": "online_monitor",
    }
    base.update(overrides)
    return base


class MergeTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
        self.tmp.close()
        self.repo = Repository(self.tmp.name)
        self.repo.initialize()
        self.service = Service(self.repo)

    def tearDown(self):
        os.unlink(self.tmp.name)

    def test_three_channels_within_window_become_one_event_with_raw_reports(self):
        first = self.service.ingest_report(report(channel="online_monitor", zone_ids=["Z-A"]), "m", "analyst")
        self.assertEqual(first["outcome"], "created")
        event_id = first["item"]["id"]

        # 实验室复检，20 分钟后
        second = self.service.ingest_report(
            report(channel="lab", detected_at="2026-10-01T08:20:00+00:00", zone_ids=["Z-A", "Z-B"]),
            "lab-1", "lab",
        )
        self.assertEqual(second["outcome"], "merged")
        self.assertEqual(second["item"]["id"], event_id)
        self.assertEqual(second["new_zones"], ["Z-B"])

        # 热线，相对第一条 29 分钟
        third = self.service.ingest_report(
            report(channel="hotline", detected_at="2026-10-01T08:29:00+00:00",
                   zone_ids=["Z-C"], complaints=9, external_id=None),
            "hotline-1", "dispatcher",
        )
        self.assertEqual(third["outcome"], "merged")
        self.assertEqual(third["item"]["id"], event_id)

        item = self.service.get_item(event_id)
        self.assertEqual(len(self.service.state()["items"]), 1)
        # 原始上报和检测时间全部保留
        channels = sorted(entry["channel"] for entry in item["reports"])
        self.assertEqual(channels, ["hotline", "lab", "online_monitor"])
        detected = sorted(entry["detected_at"] for entry in item["reports"])
        self.assertEqual(detected, [
            "2026-10-01T08:00:00+00:00",
            "2026-10-01T08:20:00+00:00",
            "2026-10-01T08:29:00+00:00",
        ])
        # 区域覆盖补齐
        self.assertEqual(item["payload"]["zone_ids"], ["Z-A", "Z-B", "Z-C"])
        self.assertEqual(item["payload"]["complaints"], 11)
        kinds = [event["event_type"] for event in item["audit"]]
        self.assertEqual(kinds.count("report_merged"), 2)

    def test_outside_window_or_different_key_creates_separate_events(self):
        first = self.service.ingest_report(report(detected_at="2026-10-01T08:00:00+00:00"), "m", "analyst")
        # 相隔正好 30 分钟不算（要求“不到半小时”）
        on_edge = self.service.ingest_report(
            report(channel="lab", detected_at="2026-10-01T08:30:00+00:00"), "l", "lab"
        )
        self.assertEqual(on_edge["outcome"], "created")
        self.assertNotEqual(on_edge["item"]["id"], first["item"]["id"])

        # 不同污染物：新事件
        other = self.service.ingest_report(
            report(contaminant="arsenic", detected_at="2026-10-01T08:10:00+00:00"), "m", "analyst"
        )
        self.assertEqual(other["outcome"], "created")

        # 不同水源：新事件
        other_source = self.service.ingest_report(
            report(source_id="SRC-OTHER", detected_at="2026-10-01T08:10:00+00:00"), "m", "analyst"
        )
        self.assertEqual(other_source["outcome"], "created")

    def test_external_id_makes_resubmission_idempotent(self):
        payload = report(external_id="station-A-42")
        first = self.service.ingest_report(payload, "m", "analyst")
        again = self.service.ingest_report(payload, "m", "analyst")
        self.assertEqual(first["outcome"], "created")
        self.assertEqual(again["outcome"], "duplicate")
        self.assertEqual(again["item"]["version"], 1)
        item = self.service.get_item(first["item"]["id"])
        self.assertEqual(len(item["reports"]), 1)

    def test_offline_batch_merges_late_reports_in_order(self):
        # 监测站断网期间攒了 3 条，网络恢复后一起提交；顺序故意打乱，迟到的也要计入
        results = self.service.ingest_reports({"reports": [
            report(channel="hotline", detected_at="2026-10-01T09:25:00+00:00",
                   zone_ids=["Z-C"], complaints=6),
            report(channel="lab", detected_at="2026-10-01T09:05:00+00:00",
                   zone_ids=["Z-B"]),
            report(channel="online_monitor", detected_at="2026-10-01T08:50:00+00:00",
                   zone_ids=["Z-A"]),
        ]}, "m", "analyst")

        event_ids = {result["item"]["id"] for result in results}
        self.assertEqual(len(event_ids), 1)
        outcomes = [result["outcome"] for result in results]
        self.assertEqual(outcomes[0], "created")
        self.assertEqual(outcomes[1:], ["merged", "merged"])

        item = self.service.get_item(results[0]["item"]["id"])
        self.assertEqual(len(item["reports"]), 3)
        self.assertEqual(item["payload"]["zone_ids"], ["Z-C", "Z-B", "Z-A"])
        # 首/末检测时间按检测时间（而非到达时间）记录
        self.assertEqual(item["payload"]["first_detected_at"], "2026-10-01T08:50:00+00:00")
        self.assertEqual(item["payload"]["last_detected_at"], "2026-10-01T09:25:00+00:00")
        # received_at 相同：都是网络恢复时一起到的
        received = {entry["received_at"] for entry in item["reports"]}
        self.assertEqual(len(received), 1)

    def test_late_report_after_full_response_is_still_counted(self):
        first = self.service.ingest_report(report(zone_ids=["Z-A"]), "m", "analyst")
        item = first["item"]
        item = self.service.act(item["id"], "verify", {"sample_count": 1}, "a", "analyst", item["version"])
        item = self.service.act(item["id"], "advise", {"notice_id": "N-A", "kind": "boil", "message": "煮沸"}, "d", "dispatcher", item["version"])
        item = self.service.act(item["id"], "flush", {"zone_id": "Z-A"}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "disinfect", {"zone_id": "Z-A", "completed": True}, "f", "field_operator", item["version"])
        item = self.service.act(item["id"], "sample", {"sample_id": "S-A", "zone_id": "Z-A", "concentration": 1}, "l", "lab", item["version"])
        item = self.service.act(item["id"], "restore", {}, "c", "coordinator", item["version"])
        self.assertEqual(item["status"], "restored")

        # 断网迟到的上报，检测时间仍在窗口内，只带老区域 -> 计入但事件保持恢复
        late = self.service.ingest_report(
            report(channel="lab", detected_at="2026-10-01T08:20:00+00:00",
                   zone_ids=["Z-A"], concentration=8),
            "l", "lab",
        )
        self.assertEqual(late["outcome"], "merged")
        item = self.service.get_item(late["item"]["id"])
        self.assertEqual(item["status"], "restored")
        self.assertEqual(len(item["reports"]), 2)
        self.assertEqual(item["payload"]["zones"]["Z-A"]["status"], "restored")

    def test_batch_with_duplicate_only_counts_once(self):
        payload = report(external_id="dup-1")
        results = self.service.ingest_reports({"reports": [payload, dict(payload)]}, "m", "analyst")
        self.assertEqual([r["outcome"] for r in results], ["created", "duplicate"])
        item = self.service.get_item(results[0]["item"]["id"])
        self.assertEqual(len(item["reports"]), 1)


if __name__ == "__main__":
    unittest.main()
