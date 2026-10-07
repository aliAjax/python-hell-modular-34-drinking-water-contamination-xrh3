from datetime import datetime, timedelta, timezone

from .domain import DomainError, CHANNEL_LABELS

ENTITY_TYPE = "water_contamination"
INITIAL_STATUS = "detected"
INGEST_ROLES = {"analyst", "dispatcher", "lab"}
SOURCE_ROLES = {"analyst", "dispatcher", "field_operator", "lab"}
ACTION_ROLES = {
    "verify": {"analyst", "dispatcher"},
    "advise": {"coordinator", "dispatcher"},
    "switch_source": {"coordinator"},
    "flush": {"field_operator"},
    "disinfect": {"field_operator"},
    "sample": {"lab", "field_operator"},
    "restore": {"coordinator", "regulator"},
    "cancel": {"coordinator"},
}
ENFORCE_REGION = False
REGION_SENSITIVE_ACTIONS = set()
ACTION_REQUIRES_VERSION = {"advise", "switch_source", "flush", "disinfect", "sample", "restore", "cancel"}

# 同一水源同一污染物的两条上报，检测时间相差不到半小时即并入同一事件
MERGE_WINDOW = timedelta(minutes=30)

# 区域处置阶段顺序：检测发现 -> 冲洗中 -> 已消毒 -> 待恢复（复检通过）-> 已恢复
ZONE_PHASES = ["detected", "flushing", "disinfected", "cleared", "restored"]
# 通知发出后进入处置阶段；区域级操作只在处置阶段允许，具体到某一区域
# 能否执行，再看该区域自身的阶段。这样并入新区域后，已恢复区域不受影响，
# 新区域可独立走冲洗、消毒、复检，而事件状态由所有区域共同决定。
RESPONSE_STATUSES = {"advisory", "flushing", "disinfected", "sampled", "switched"}


def assess(payload):
    concentration = float(payload.get("concentration", 0))
    limit = max(float(payload.get("limit", 0.000001)), 0.000001)
    ratio = concentration / limit
    population = int(payload.get("population", 0))
    score = min(100.0, ratio * 35.0 + min(population / 1000.0, 40.0))
    if score >= 80:
        level = "critical"
    elif score >= 50:
        level = "high"
    elif score >= 20:
        level = "medium"
    else:
        level = "low"
    return {"score": round(score, 2), "level": level, "ratio": round(ratio, 3)}


def parse_iso(value):
    dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def within_merge_window(detected_at_a, detected_at_b):
    return abs(parse_iso(detected_at_a) - parse_iso(detected_at_b)) < MERGE_WINDOW


def now_iso():
    return datetime.now(timezone.utc).isoformat()


def _need_status(item, allowed):
    if item["status"] not in allowed:
        raise DomainError("invalid_state", "当前状态 %s 不允许执行该操作" % item["status"])


def _text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    return value.strip()


def build_initial_payload(report):
    """用第一条上报构造事件负载。"""
    zones = {zone: {"status": "detected", "history": []} for zone in report["zone_ids"]}
    return {
        "source_id": report["source_id"],
        "contaminant": report["contaminant"],
        # 事件级读数取所有上报中的最严重值
        "concentration": report["concentration"],
        "limit": report["limit"],
        "population": report["population"],
        "complaints": report["complaints"],
        "detected_at": report["detected_at"],
        "first_detected_at": report["detected_at"],
        "last_detected_at": report["detected_at"],
        "zones": zones,
        "zone_ids": list(report["zone_ids"]),
        "notifications": [],
        "response_actions": [],
        "sample_results": [],
        "verified": False,
        "merged": False,
        "merged_report_ids": [],
    }


def derive_status(payload):
    """根据事件级标记和各区域阶段推导事件状态。

    只要有一个区域还没恢复，事件就不能显示 restored；
    只要有区域进入了冲洗/消毒/复检阶段，事件就停在对应处置阶段
    （即使此时并入了仍处于 detected 的新区域）。
    """
    zones = payload.get("zones", {})
    statuses = [info["status"] for info in zones.values()]

    if statuses and all(status == "restored" for status in statuses):
        return "restored"

    if payload.get("cancelled"):
        return "cancelled"

    if any(status == "cleared" for status in statuses):
        return "sampled"
    if any(status == "disinfected" for status in statuses):
        return "disinfected"
    if any(status == "flushing" for status in statuses):
        return "flushing"
    if payload.get("alternate_source_id"):
        return "switched"
    if payload.get("notifications"):
        return "advisory"
    if payload.get("verified"):
        return "verified"
    return "detected"


def merge_report(payload, report):
    """把一条上报并入事件负载，返回 (新负载, 新区域列表)。

    原始上报由调用方整体存入 reports 表；这里同步事件级汇总字段。
    新区域以 detected 入列，已恢复的区域原样保留。
    """
    current = dict(payload)
    current["zones"] = {
        zone: {"status": info["status"], "history": list(info.get("history", []))}
        for zone, info in payload.get("zones", {}).items()
    }

    new_zones = [zone for zone in report["zone_ids"] if zone not in current["zones"]]
    merged_at = now_iso()
    for zone in new_zones:
        current["zones"][zone] = {
            "status": "detected",
            "history": [{"status": "detected", "at": merged_at,
                         "reason": "merged_report"}],
        }

    current["zone_ids"] = list(dict.fromkeys(
        list(payload.get("zone_ids", [])) + list(report["zone_ids"])
    ))
    current["complaints"] = int(current.get("complaints", 0)) + int(report["complaints"])
    current["population"] = max(int(current.get("population", 0)), int(report["population"]))
    if report["concentration"] / report["limit"] > \
            float(current.get("concentration", 0)) / max(float(current.get("limit", 0.000001)), 0.000001):
        current["concentration"] = report["concentration"]
        current["limit"] = report["limit"]
    if parse_iso(report["detected_at"]) < parse_iso(current["first_detected_at"]):
        current["first_detected_at"] = report["detected_at"]
    if parse_iso(report["detected_at"]) > parse_iso(current["last_detected_at"]):
        current["last_detected_at"] = report["detected_at"]

    current["verified"] = bool(current.get("verified"))
    if "assessment" in current and new_zones:
        current["assessment"] = assess(current)
    current["merged"] = True
    return current, new_zones


def _zone(current, payload):
    zone_id = _text(payload, "zone_id")
    zone = current.get("zones", {}).get(zone_id)
    if zone is None:
        raise DomainError("unknown_zone", "区域 %s 不在事件覆盖范围内" % zone_id, 404)
    return zone_id, zone


def apply_action(item, action, payload, actor, role):
    status = item["status"]
    current = dict(item["payload"])
    current["zones"] = {
        zone: {"status": info["status"], "history": list(info.get("history", []))}
        for zone, info in item["payload"].get("zones", {}).items()
    }

    if action == "verify":
        _need_status(item, {"detected", "verified"})
        sample_count = int(payload.get("sample_count", 0) or 0)
        if sample_count < 1:
            raise DomainError("sample_required", "需要至少一份复检样本", 409)
        current["assessment"] = assess(current)
        current["verification"] = {"sample_count": sample_count, "note": payload.get("note", "")}
        current["verified"] = True
        return "verified", current, {"assessment": current["assessment"], "verification": current["verification"]}

    if action == "advise":
        _need_status(item, {"verified", "advisory", "switched"})
        notice_id = _text(payload, "notice_id")
        notice = {
            "notice_id": notice_id,
            "kind": _text(payload, "kind"),
            "message": _text(payload, "message"),
        }
        notices = current.setdefault("notifications", [])
        if any(existing.get("notice_id") == notice_id for existing in notices):
            raise DomainError("duplicate_notification", "同一通知编号不能重复发送", 409)
        notices.append(notice)
        return derive_status(current), current, {"notice": notice}

    if action == "switch_source":
        _need_status(item, {"verified", "advisory", "flushing", "disinfected", "sampled", "switched"})
        alternate = _text(payload, "alternate_source_id")
        current["alternate_source_id"] = alternate
        return derive_status(current), current, {"alternate_source_id": alternate}

    if action == "flush":
        # 正在通知、切换水源、冲洗/消毒/复检阶段都允许对（新）区域启动冲洗
        if status not in RESPONSE_STATUSES:
            raise DomainError("invalid_state", "当前状态 %s 不允许执行该操作" % status)
        zone_id, zone = _zone(current, payload)
        if zone["status"] not in {"detected", "flushing"}:
            raise DomainError("zone_not_flushable", "区域 %s 当前阶段 %s，无需或不能冲洗" % (zone_id, zone["status"]), 409)
        zone["status"] = "flushing"
        zone["history"].append({"status": "flushing", "at": now_iso(), "actor": actor})
        current.setdefault("response_actions", []).append({"type": "flush", "zone_id": zone_id, "actor": actor})
        return "flushing", current, {"zone_id": zone_id, "type": "flush"}

    if action == "disinfect":
        if status not in RESPONSE_STATUSES:
            raise DomainError("invalid_state", "当前状态 %s 不允许执行该操作" % status)
        if not payload.get("completed"):
            raise DomainError("disinfection_incomplete", "消毒尚未完成", 409)
        zone_id, zone = _zone(current, payload)
        if zone["status"] != "flushing":
            raise DomainError("zone_not_flushing", "区域 %s 尚未完成冲洗，不能消毒" % zone_id, 409)
        zone["status"] = "disinfected"
        zone["history"].append({"status": "disinfected", "at": now_iso(), "actor": actor})
        current.setdefault("response_actions", []).append({"type": "disinfect", "zone_id": zone_id, "actor": actor})
        return derive_status(current), current, {"zone_id": zone_id, "type": "disinfect"}

    if action == "sample":
        if status not in RESPONSE_STATUSES:
            raise DomainError("invalid_state", "当前状态 %s 不允许执行该操作" % status)
        zone_id, zone = _zone(current, payload)
        if zone["status"] != "disinfected":
            raise DomainError("zone_not_disinfected", "区域 %s 尚未完成消毒，不能复检" % zone_id, 409)
        concentration = float(payload.get("concentration", 0))
        if concentration < 0:
            raise DomainError("invalid_concentration", "浓度不能为负数")
        sample_id = _text(payload, "sample_id")
        result = {"sample_id": sample_id, "zone_id": zone_id, "concentration": concentration}
        current.setdefault("sample_results", []).append(result)
        limit = max(float(current.get("limit", 0.000001)), 0.000001)
        if concentration <= limit:
            zone["status"] = "cleared"
        else:
            # 复检不达标：退回冲洗，重新走冲洗、消毒、复检
            zone["status"] = "flushing"
        zone["history"].append({"status": zone["status"], "at": now_iso(),
                                "sample_id": sample_id, "concentration": concentration})
        return derive_status(current), current, {"sample_result": result, "zone_status": zone["status"]}

    if action == "restore":
        _need_status(item, {"sampled"})
        zones = current.get("zones", {})
        pending = [zone_id for zone_id, info in zones.items() if info["status"] != "restored"]
        not_cleared = [zone_id for zone_id in pending if zones[zone_id]["status"] != "cleared"]
        if not_cleared:
            raise DomainError(
                "zones_not_cleared",
                "仍有区域未完成冲洗、消毒和复检，不能恢复：%s" % ", ".join(sorted(not_cleared)),
                409,
                {"pending_zones": pending, "not_cleared_zones": not_cleared},
            )
        limit = max(float(current.get("limit", 0.000001)), 0.000001)
        results = current.get("sample_results", [])
        if zones:
            # 以每个区域最新一次复检结果为准；早前不合格、复检通过的结果不阻塞恢复
            latest_by_zone = {}
            for result in results:
                latest_by_zone[result["zone_id"]] = float(result["concentration"])
            failing = sorted(
                zone_id for zone_id in pending
                if latest_by_zone.get(zone_id, float("inf")) > limit
            )
            if failing:
                raise DomainError(
                    "quality_not_met",
                    "以下区域最近一次复检结果仍超过限值：%s" % ", ".join(failing),
                    409,
                    {"failing_zones": failing},
                )
        elif not results or any(float(result["concentration"]) > limit for result in results):
            raise DomainError("quality_not_met", "复检结果未全部达到限值", 409)
        restored_at = now_iso()
        for zone_id in pending:
            zones[zone_id]["status"] = "restored"
            zones[zone_id]["history"].append({"status": "restored", "at": restored_at, "actor": actor})
        current["restoration"] = {"actor": actor, "note": payload.get("note", ""), "at": restored_at}
        return "restored", current, {"restoration": current["restoration"], "restored_zones": pending}

    if action == "cancel":
        _need_status(item, {"detected", "verified"})
        reason = _text(payload, "reason")
        current["cancellation"] = {"reason": reason, "actor": actor}
        current["cancelled"] = True
        return "cancelled", current, {"reason": reason}

    raise DomainError("unknown_action", "不支持的操作")


def describe_report(report):
    label = CHANNEL_LABELS.get(report["channel"], report["channel"])
    return "%s上报(检测时间 %s，区域 %s)" % (
        label, report["detected_at"], ",".join(report["zone_ids"])
    )
