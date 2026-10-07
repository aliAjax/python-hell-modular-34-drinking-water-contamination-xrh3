from . import domain, rules
from .domain import DomainError


class Service:
    def __init__(self, repository):
        self.repository = repository

    def ingest_report(self, payload, actor, role, region=None):
        """单条上报接入：自动建事件或按 30 分钟窗口并入，重复提交幂等。"""
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.INGEST_ROLES:
            raise DomainError("forbidden", "当前角色不能上报污染信息", 403)
        report = domain.normalize_report(payload)
        return self.repository.ingest_report(report, actor, role, report.get("received_at"))

    def ingest_reports(self, payload, actor, role, region=None):
        """监测站断网恢复后批量补传：逐条合并，迟到上报同样计入。"""
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.INGEST_ROLES:
            raise DomainError("forbidden", "当前角色不能上报污染信息", 403)
        entries = payload.get("reports") if isinstance(payload, dict) else None
        if not isinstance(entries, list) or not entries:
            raise DomainError("reports_required", "reports 必须是非空上报列表")
        reports = [domain.normalize_report(entry) for entry in entries]
        return self.repository.ingest_reports(reports, actor, role)

    # 旧接口名：POST /api/items 的语义即“提交一条上报”
    create_item = ingest_report

    def add_source(self, item_id, payload, actor, role, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        if role not in rules.SOURCE_ROLES:
            raise DomainError("forbidden", "当前角色不能提交来源记录", 403)
        self.repository.get_item(item_id)
        normalized = domain.normalize_source(payload)
        result = self.repository.add_source(
            item_id,
            normalized.pop("source_type"),
            normalized.pop("external_id"),
            normalized,
            normalized.pop("observed_at"),
            actor,
            role,
        )
        return result

    def act(self, item_id, action, payload, actor, role, expected_version=None, region=None):
        if not actor or not role:
            raise DomainError("identity_required", "需要用户身份和角色", 401)
        item = self.repository.get_item(item_id)
        allowed = rules.ACTION_ROLES.get(action, set())
        if role not in allowed:
            raise DomainError("forbidden", "当前角色不能执行该操作", 403)
        if rules.ENFORCE_REGION and action in rules.REGION_SENSITIVE_ACTIONS and region and role != "regulator":
            if item["payload"].get("region") != region:
                raise DomainError("region_mismatch", "不能处理其他区域的记录", 403)
        if action in rules.ACTION_REQUIRES_VERSION and expected_version is None:
            raise DomainError("expected_version_required", "该操作需要 expected_version", 400)
        new_status, new_payload, event_payload = rules.apply_action(item, action, payload, actor, role)
        self.repository.apply_action(
            item_id, action, actor, role, new_status, new_payload, event_payload, expected_version
        )
        return self.get_item(item_id)

    def get_item(self, item_id):
        item = self.repository.get_item(item_id)
        item["sources"] = self.repository.list_sources(item_id)
        item["reports"] = self.repository.list_reports(item_id)
        item["audit"] = self.repository.audit_trail(item_id)
        item["assessment"] = rules.assess(item["payload"])
        item["zone_summary"] = self._zone_summary(item["payload"])
        return item

    @staticmethod
    def _zone_summary(payload):
        zones = payload.get("zones", {})
        summary = {}
        for zone_id, info in zones.items():
            summary[info["status"]] = summary.get(info["status"], 0) + 1
        return {
            "total": len(zones),
            "by_status": summary,
            "pending_zones": sorted(
                zone_id for zone_id, info in zones.items() if info["status"] != "restored"
            ),
        }

    def list_items(self, status=None):
        items = self.repository.list_items(status)
        for item in items:
            item["zone_summary"] = self._zone_summary(item["payload"])
        return items

    def state(self):
        summary = self.repository.state_summary()
        for item in summary["items"]:
            item["zone_summary"] = self._zone_summary(item["payload"])
        return summary
