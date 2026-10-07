import hashlib
from datetime import datetime


class DomainError(Exception):
    def __init__(self, code, message, status=400):
        super().__init__(message)
        self.code = code
        self.status = status


class ConflictError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 409)


class NotFoundError(DomainError):
    def __init__(self, code, message):
        super().__init__(code, message, 404)


def require_text(payload, name):
    value = payload.get(name)
    if not isinstance(value, str) or not value.strip():
        raise DomainError("field_required", "%s 不能为空" % name)
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


def parse_timestamp(payload, name):
    value = require_text(payload, name)
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise DomainError("invalid_timestamp", "%s 必须是 ISO 时间" % name)
    return value


def _content_fingerprint(source_id, contaminant, detected_at, concentration, limit, zone_ids, population, complaints):
    key = "%s|%s|%s|%s|%s|%s|%s|%s" % (
        source_id,
        contaminant,
        detected_at,
        concentration,
        limit,
        ",".join(zone_ids),
        population,
        complaints,
    )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:32]


def normalize_create(payload):
    source_id = require_text(payload, "source_id")
    contaminant = require_text(payload, "contaminant")
    detected_at = parse_timestamp(payload, "detected_at")
    concentration = number(payload, "concentration", 0)
    limit = number(payload, "limit", 0.000001)
    zones = payload.get("zone_ids", [])
    if not isinstance(zones, list) or not zones:
        raise DomainError("zones_required", "至少需要一个受影响区域")
    if any(not isinstance(zone, str) or not zone.strip() for zone in zones):
        raise DomainError("invalid_zones", "区域编号必须是字符串列表")
    population = int(payload.get("population", 0) or 0)
    if population < 0:
        raise DomainError("invalid_population", "受影响人数不能为负数")
    complaints = int(payload.get("complaints", 0) or 0)
    zone_ids = [zone.strip() for zone in zones]
    source_type = require_text(payload, "source_type") if payload.get("source_type") else "online_monitoring"
    external_id = payload.get("external_id")
    if not isinstance(external_id, str) or not external_id.strip():
        external_id = _content_fingerprint(
            source_id, contaminant, detected_at, concentration, limit, zone_ids, population, complaints
        )
    else:
        external_id = external_id.strip()
    return {
        "source_id": source_id,
        "contaminant": contaminant,
        "detected_at": detected_at,
        "concentration": concentration,
        "limit": limit,
        "zone_ids": zone_ids,
        "population": population,
        "complaints": complaints,
        "source_type": source_type,
        "external_id": external_id,
    }


def build_initial_payload(normalized):
    zones = {zone_id: {"status": "pending"} for zone_id in normalized["zone_ids"]}
    return {
        "source_id": normalized["source_id"],
        "contaminant": normalized["contaminant"],
        "detected_at": normalized["detected_at"],
        "concentration": normalized["concentration"],
        "limit": normalized["limit"],
        "zone_ids": list(normalized["zone_ids"]),
        "zones": zones,
        "population": normalized["population"],
        "complaints": normalized["complaints"],
        "notifications": [],
        "response_actions": [],
        "sample_results": [],
        "reports": [],
    }


def report_summary(normalized, report_id):
    return {
        "report_id": report_id,
        "source_type": normalized["source_type"],
        "external_id": normalized["external_id"],
        "detected_at": normalized["detected_at"],
        "concentration": normalized["concentration"],
        "zone_ids": list(normalized["zone_ids"]),
    }


def merge_zones(payload, zone_ids):
    zones = payload.setdefault("zones", {})
    added = []
    for zone_id in zone_ids:
        if zone_id not in zones:
            zones[zone_id] = {"status": "pending"}
            added.append(zone_id)
    existing = payload.setdefault("zone_ids", [])
    for zone_id in added:
        if zone_id not in existing:
            existing.append(zone_id)
    return added


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
