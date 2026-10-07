from datetime import datetime


class DomainError(Exception):
    def __init__(self, code, message, status=400, details=None):
        super().__init__(message)
        self.code = code
        self.status = status
        self.details = details or {}


class ConflictError(DomainError):
    def __init__(self, code, message, details=None):
        super().__init__(code, message, 409, details)


class NotFoundError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 404)


REPORT_CHANNELS = {"online_monitor", "lab", "hotline"}
CHANNEL_LABELS = {
    "online_monitor": "在线监测",
    "lab": "实验室复检",
    "hotline": "热线",
}


def require_text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
    return value.strip()


def optional_text(payload, name):
    value = payload.get(name)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise DomainError("invalid_field", "%s 必须是非空字符串" % name)
    return value.strip()


def number(payload, name, minimum=None):
    value = payload.get(name)
    if isinstance(value, bool):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    try:
        value = float(value)
    except (TypeError, ValueError):
        raise DomainError("invalid_number", "%s 必须是数字" % name)
    if minimum is not None and value < minimum:
        raise DomainError("invalid_number", "%s 不能小于 %s" % (name, minimum))
    return value


def integer(payload, name, minimum=0):
    value = payload.get(name, 0) or 0
    if isinstance(value, bool):
        raise DomainError("invalid_integer", "%s 必须是整数" % name)
    try:
        value = int(value)
    except (TypeError, ValueError):
        raise DomainError("invalid_integer", "%s 必须是整数" % name)
    if value < minimum:
        raise DomainError("invalid_integer", "%s 不能小于 %s" % (name, minimum))
    return value


def parse_timestamp(payload, name):
    value = require_text(payload, name)
    validate_iso(value, name)
    return value


def optional_timestamp(payload, name):
    value = optional_text(payload, name)
    if value is not None:
        validate_iso(value, name)
    return value


def validate_iso(value, name="时间"):
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DomainError("invalid_timestamp", "%s 必须是 ISO 时间" % name)


def _zone_list(payload):
    zones = payload.get("zone_ids", [])
    if not isinstance(zones, list) or not zones:
        raise DomainError("zones_required", "至少需要一个受影响区域")
    cleaned = []
    for zone in zones:
        if not isinstance(zone, str) or not zone.strip():
            raise DomainError("invalid_zones", "区域编号必须是字符串列表")
        zone = zone.strip()
        if zone not in cleaned:
            cleaned.append(zone)
    return cleaned


def report_fingerprint(channel, external_id, source_id, contaminant, detected_at,
                       concentration, limit, zone_ids, population, complaints):
    # 有外部编号时以“渠道+外部编号”为幂等键；否则用全部上报内容指纹，
    # 保证同一内容重复提交只计一次、读数变化的新上报仍会计入。
    if external_id:
        return "%s|ext|%s" % (channel, external_id)
    return "|".join([
        channel,
        source_id,
        contaminant,
        detected_at,
        repr(float(concentration)),
        repr(float(limit)),
        ",".join(sorted(zone_ids)),
        str(int(population)),
        str(int(complaints)),
    ])


def normalize_report(payload):
    channel = (payload.get("channel") or "online_monitor")
    if not isinstance(channel, str) or not channel.strip():
        raise DomainError("invalid_channel", "上报渠道必须是非空字符串")
    channel = channel.strip()
    if channel not in REPORT_CHANNELS:
        raise DomainError(
            "invalid_channel",
            "上报渠道必须是 online_monitor / lab / hotline 之一",
        )
    source_id = require_text(payload, "source_id")
    contaminant = require_text(payload, "contaminant")
    detected_at = parse_timestamp(payload, "detected_at")
    concentration = number(payload, "concentration", 0)
    limit = number(payload, "limit", 0.000001)
    zone_ids = _zone_list(payload)
    population = integer(payload, "population", 0)
    complaints = integer(payload, "complaints", 0)
    external_id = optional_text(payload, "external_id")
    received_at = optional_timestamp(payload, "received_at")
    note = optional_text(payload, "note")
    fingerprint = report_fingerprint(
        channel, external_id, source_id, contaminant, detected_at,
        concentration, limit, zone_ids, population, complaints,
    )
    return {
        "channel": channel,
        "external_id": external_id,
        "source_id": source_id,
        "contaminant": contaminant,
        "detected_at": detected_at,
        "received_at": received_at,
        "concentration": concentration,
        "limit": limit,
        "zone_ids": zone_ids,
        "population": population,
        "complaints": complaints,
        "note": note,
        "fingerprint": fingerprint,
    }


# 兼容旧接口名：POST /api/items 即单条上报
normalize_create = normalize_report


def normalize_source(payload):
    source_type = require_text(payload, "source_type")
    external_id = require_text(payload, "external_id")
    observed_at = parse_timestamp(payload, "observed_at")
    result = {
        "source_type": source_type,
        "external_id": external_id,
        "observed_at": observed_at,
        "concentration": number(payload, "concentration", 0) if "concentration" in payload else None,
        "zone_id": payload.get("zone_id"),
        "note": payload.get("note", ""),
    }
    return result
