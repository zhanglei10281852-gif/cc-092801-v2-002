# 红白喜事服务运营平台

这是一个面向婚庆公司、殡葬服务机构和现场调度人员的 Python 后端服务，用于管理服务套餐、家庭订单、现场执行队列、服务人员、结果版本和运营干预。服务保留登录、角色权限、会话、审计和配额等基础能力，所有业务状态与审计事件写入本地 SQLite 数据库，适合在单个应用容器中离线运行。

## 运行环境

- Python 3.11
- SQLite 3（由 Python 标准库提供）
- FastAPI 与 Uvicorn

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/ceremony-operations.db`，可复制 `.env.example` 并设置 `TOWNSHIP_DATABASE_PATH` 指向其他本地路径。

## 初始化与启动

```bash
python -m app.cli init-db
python -m app.cli check-db
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

服务订单运营接口使用 `/api/compute` 前缀，身份、角色、审计和系统接口分别位于 `/api/auth`、`/api/roles`、`/api/audit` 与 `/api/system`。

## 测试与编译检查

```bash
python -m pytest
python -m compileall -q app tests
```

本地冒烟命令：

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

## 目录结构

```text
app/compute/       任务模板、配额、提交、领取、回执和人工干预
app/api/            登录、角色、审计和系统管理接口
app/core/           时钟、安全、异常和分页能力
app/repositories/   SQLite 查询与事务封装
app/services/       身份、审计和后台任务服务
app/database.py     SQLite 连接、事务、表结构和权限初始化
tests/              领域、接口、调度和身份回归测试
tools/              本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL 和忙等待策略。提交、领取、回执和人工干预在即时事务中完成；租约、配额与结果版本使用可注入时钟，便于复现跨日和恢复边界。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。

## 取消与现场执行的收敛规则

服务订单（`compute_tasks`）的取消请求与现场回执按以下规则收敛，所有状态转移都在即时事务中完成，并同时写入 `compute_interventions` 快照与 `audit_events`（可通过 `/api/audit?resource_type=compute_task` 查询）：

- **尚未开始（`queued`）取消**：立即结束为 `cancelled`，费用为 0，并记录取消决定（谁、何时、原因）。
- **正在执行（`running`）取消**：转入 `cancel_requested`，记录取消决定；心跳不再续租并返回 409，要求现场停止；下一次 `complete` 或 `fail` 回执时收敛为 `cancelled`，计费止于回执时刻。回执内容以 `accepted=0` 的结果版本留痕，但不会把服务重新变成成功或失败。
- **迟到回执**：已终态（`cancelled`/`succeeded`/`failed`）的任务再收到完成或失败回执时，回执登记为 `accepted=0` 并产生 `receipt_ignored` 干预记录，订单状态与已结算费用不变。
- **租约兜底**：取消请求后现场始终不回执的，租约到期恢复时自动收敛为 `cancelled`，计费止于租约到期时刻（`recover_expired` 返回的 `cancelled` 列表）。
- **稳定结果**：重复取消回放当前状态（`cancellation.repeated=true`，不新增决定记录）；重复回执按（任务、工作者、尝试轮次、内容）指纹回放首次结论，回执 `outcome` 为 `applied`/`cancelled`/`replayed`/`ignored`。取消请求可携带可选 `request_key`，保证客户端重试不会产生第二条取消决定。
- **人工重试**：`retry` 会清空取消与计费标记，任务重新排队，旧决定仍保留在干预与审计记录中。

费用在完成、失败终结或取消收敛时结算：模板可配置 `base_fee`（分）、`unit_fee`（分/计费单位）与 `billing_unit_seconds`；未开始不计费。订单费用明细见 `GET /api/compute/task-details/{id}/billing`，任务详情同时返回 `billing`、`cancellation`、回执列表与干预时间线。旧的正常完成流程（排队 → 领取 → 成功/失败/重试）保持兼容。
