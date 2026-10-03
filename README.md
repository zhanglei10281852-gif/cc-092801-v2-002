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

## 取消与现场回执的收敛规则

家属或客服对服务订单发起取消后，取消请求与现场执行按以下规则收敛，保证费用不会被迟到回执重新激活：

- `queued`（尚未开始）：取消立即终态 `cancelled`，基础费与执行费全额 `waived`。
- `running`（正在执行）：取消进入 `cancel_requested`，记录决定人、原因与**计费停止时刻**；工作者下一次 complete/fail 回执时收敛为 `cancelled`，执行费只计算到取消请求时刻（按模板计费单位向上取整）。
- 终态任务（`cancelled`/`succeeded`/`failed`）收到迟到回执：登记为 `late_rejected` 并留痕，状态与费用不变，服务不会被重新激活。
- `cancel_requested` 任务的租约过期且现场始终未回执时，恢复任务按取消请求时刻结算并收敛为 `cancelled`。
- 重复取消、重复回执（回执 `idempotency_key`，或按工作者+载荷+执行轮次生成的自动键）以及人工重试都返回稳定结果；重试后属于新一轮结算（`settlement_seq`），排队等待时间不计入执行时长。
- 每次取消、回执、迟到拒绝、租约恢复和人工干预同时写入 `compute_interventions` 与 `audit_events`（含 actor、时间、前后状态），可通过 `/api/audit?resource_type=compute_task` 还原谁在何时做了决定。

模板创建时可声明计费字段（单位：分）：`base_fee_cents`、`unit_fee_cents`、`billing_unit_seconds`。

费用与订单查询接口：

```bash
# 单个订单：状态、费用明细（compute_charges）、结果版本、干预记录
curl -sS http://127.0.0.1:8432/api/compute/task-details/12
# 项目维度：汇总金额与逐单费用明细
curl -sS http://127.0.0.1:8432/api/compute/projects/demo/billing
```

取消与回执请求体均支持可选的 `idempotency_key`，响应中带 `cancellation.duplicate` 或 `receipt.disposition`（`accepted`/`cancelled`/`late_rejected`）指示本次结果的来源。

