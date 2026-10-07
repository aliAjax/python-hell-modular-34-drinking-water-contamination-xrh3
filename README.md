# 市政饮用水污染响应

模块化纯 Python 3.9.6+ 标准库项目，默认端口 `8334`。

- `app.py`：服务生命周期和依赖组装。
- `src/domain.py`：水源、污染物、区域和来源校验，以及上报合并的初始载荷与区域归并。
- `src/rules.py`：污染评分、通知去重、停水、切换水源、按区域冲洗/消毒/复检、恢复状态机和恢复门禁。
- `src/repository.py`：SQLite、事务、重复保护、乐观版本、审计链，以及上报合并目标查找与并入。
- `src/service.py`：身份、角色和用例编排；同一水源、同一污染物、检测时间相隔不到半小时的上报并入同一条事件。
- `src/http_api.py`：JSON 接口与静态首页。
- `src/audit.py`：审计哈希。

```bash
python3 app.py --init --db ./data.db
python3 app.py --db ./data.db --port 8334
python3 -m unittest discover -s tests -v
```

接口包括 `GET /health`、`GET /api/state`、`POST /api/items`、`POST /api/items/<id>/sources`、`POST /api/items/<id>/actions` 和审计查询。测试覆盖完整响应流程、上报合并、重复提交、处置中并入新区域、恢复门禁、重复通知、复检阈值、权限和版本冲突。内置规则不替代真实水质模型、法定通报渠道或供水控制系统的联锁。
