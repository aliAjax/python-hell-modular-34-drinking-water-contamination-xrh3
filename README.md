# 市政饮用水污染响应

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8334`。

- `app.py`：服务生命周期和依赖组装。
- `src/domain.py`：水源、污染物、区域、上报渠道校验与幂等指纹。
- `src/rules.py`：合并窗口、污染评分、通知去重、停水、切换水源、区域级冲洗/消毒/复检/恢复状态机。
- `src/repository.py`：SQLite、事务、重复保护、乐观版本、上报合并和审计链。
- `src/service.py`：身份、角色和用例编排。
- `src/http_api.py`：JSON 接口与静态首页。
- `src/audit.py`：审计哈希。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8334
python3 -m unittest discover -s tests -v
```

## 事件合并规则

在线监测、实验室复检、热线三个渠道（`channel=online_monitor|lab|hotline`）的上报走同一接入逻辑：

- **同一水源、同一污染物、检测时间相隔不到 30 分钟**的上报并入同一条事件（对事件内任一原始上报比对，取时间最近的事件；正好 30 分钟不算）。
- 每条原始上报完整保留：渠道、检测时间 `detected_at`、到达时间 `received_at`、读数、区域和上报人，在 `GET /api/items/<id>` 的 `reports` 中按检测时间排列。
- **重复提交不再计一次**：同一 `external_id`（渠道内唯一编号）或内容完全一致的上报返回 `outcome=duplicate`，不新增上报、不增加事件版本。
- 区域覆盖取并集；热线投诉数累加，事件级浓度/限值取最严重读数，`first/last_detected_at` 按检测时间记录。
- 断网攒下的上报可用 `POST /api/reports/batch` 在网络恢复后一个事务内批量补传，迟到上报同样计入，同一批次内也按 30 分钟窗口合并。

## 区域级处置

- 每个区域独立经历 `detected → flushing → disinfected → cleared → restored`；事件状态由所有区域共同推导。
- 事件正在通知或冲洗时并入新上报：**新区域只走冲洗、消毒、复检**，已恢复区域原样保留，不会被回退。
- 复检不达标，该区域退回 `flushing` 重新处置；恢复时以每个区域最新一次复检结果为准。
- **只要有区域还在冲洗（或未完成消毒、复检），事件就不能显示 `restored`**；`restore` 返回 `409 zones_not_cleared` 并在 `details` 中列出阻塞区域。
- 两个班组基于同一事件版本同时提交处置：版本相同者先到先得，另一个收到 `409 version_conflict`，消息明确说明“事件内容已被其他班组的提交更新”，并在 `details` 中给出 `current_version` / `submitted_version` / `current_status`，重新读取后再提交即可。

## 接口

`GET /health`、`GET /api/state`、`GET /api/items`、`POST /api/items`（单条上报：`created` 返回 201，`merged`/`duplicate` 返回 200）、`POST /api/reports/batch`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions`、`GET /api/items/<id>/audit`。处置类操作需要 `expected_version`（取 `GET` 响应中的 `version`）；身份通过 `X-User-Id`、`X-Role` 头传递。

内置规则不替代真实水质模型、法定通报渠道或供水控制系统的联锁。
