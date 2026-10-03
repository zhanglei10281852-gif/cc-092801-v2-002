from __future__ import annotations

import hashlib
import json
import math
import sqlite3
from datetime import UTC, datetime, timedelta
from typing import Any, Callable

from app.compute.repository import ComputeRepository
from app.core.clock import Clock, SystemClock, from_storage, to_storage
from app.core.errors import ConflictError, NotFoundError, ValidationError
from app.database import get_connection, transaction
from app.repositories.audit import AuditRepository

TERMINAL_STATUSES = {"cancelled", "succeeded", "failed"}


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


class ComputeOperationsService:
    """管理服务订单（计算任务）的模板、配额、租约、回执、取消收敛与计费。

    取消与现场执行的收敛规则：
    - queued（尚未开始）收到取消：立即结束为 cancelled，不计费。
    - running（正在执行）收到取消：转入 cancel_requested，记录取消决定；
      不再续租，下一次 complete/fail 回执时收敛为 cancelled，并在回执时刻停止计费。
    - 已 cancelled 的任务收到迟到回执：回执留痕但不改变状态、不恢复计费。
    - 重复取消、重复回执返回稳定的业务结果；矛盾回执返回 409。
    """

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    def list_templates(self) -> list[dict[str, Any]]:
        return self.repository.active_templates()

    def create_template(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        self._validate_schema(payload["parameter_schema"], payload["default_parameters"])
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            if repository.template_by_code(payload["code"]):
                raise ConflictError("参数模板编码已存在")
            return repository.create_template(
                code=payload["code"], name=payload["name"], algorithm=payload["algorithm"],
                parameter_schema=payload["parameter_schema"], defaults=payload["default_parameters"],
                max_runtime_seconds=payload["max_runtime_seconds"], max_attempts=payload["max_attempts"],
                base_fee=payload.get("base_fee", 0), unit_fee=payload.get("unit_fee", 0),
                billing_unit_seconds=payload.get("billing_unit_seconds", 60),
                created_by=actor, now=now,
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    def submit(self, payload: dict[str, Any]) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            template = repository.template_by_code(payload["template_code"])
            if template is None or not template["active"]:
                raise NotFoundError("参数模板不存在或已经停用")
            parameters = self._validate_parameters(template, payload["parameters"])
            existing = repository.task_by_idempotency(payload["requested_by"], payload["idempotency_key"])
            parameter_digest = digest(parameters)
            if existing is not None:
                if existing["parameter_digest"] != parameter_digest:
                    raise ConflictError("同一幂等键对应了不同的计算参数")
                return self._decorate(connection, dict(repository.task_by_id(existing["id"])))
            self._check_quota(repository, payload["requested_by"], now_value)
            task = repository.create_task(
                template_id=template["id"], project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )
            return self._decorate(connection, task)

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return [
            self._decorate(self.connection, dict(row))
            for row in self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))
        ]

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = self._decorate(self.connection, dict(row))
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        result["receipts"] = self.repository.receipts(task_id)
        return result

    def claim(self, worker_id: str, capabilities: list[str], lease_seconds: int) -> dict[str, Any] | None:
        now_value = self.clock.now()
        now = to_storage(now_value)
        lease_until = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            candidate = repository.queued_candidate(capabilities, now)
            if candidate is None:
                return None
            cursor = connection.execute(
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=COALESCE(started_at,?),updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            task = dict(repository.task_by_id(candidate["id"]))
            self._audit(connection, actor=worker_id, action="compute.claim", task_id=task["id"],
                        before={"status": "queued"}, after={"status": "running", "lease_owner": worker_id})
            return self._decorate(connection, task)

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["status"] == "cancel_requested" and task["lease_owner"] == worker_id:
                # 取消请求先于心跳到达：明确通知现场停止，不再续租、不计新费。
                raise ConflictError("服务已被家属取消，请立即停止现场执行并回传回执")
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return self._decorate(connection, dict(ComputeRepository(connection).task_by_id(task_id)))

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any], request_key: str | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        body = {"result": result, "metrics": metrics}
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            # attempt 进入指纹：取消收敛后若人工重试并重新领取，新轮次允许相同结果；同一轮次的网络重试仍去重回放。
            receipt_digest = digest({"kind": "complete", "worker_id": worker_id, "attempt": int(task["attempt_count"]), **body})
            repeated = repository.receipt_by_digest(task_id, receipt_digest)
            if repeated is not None:
                # 重复回执（含网络重试）：回放首次处理后的稳定业务结果。
                return self._receipt_response(connection, task, receipt_digest, outcome="replayed", accepted=bool(repeated["accepted"]))
            if task["status"] in TERMINAL_STATUSES:
                # 迟到回执：留痕但绝不把已取消/已终态的服务重新变成有效。
                self._store_receipt(connection, task_id, "complete", worker_id, receipt_digest, False, body, now, request_key)
                self._intervention_row(connection, task_id, worker_id, "receipt_ignored",
                                       f"迟到完成回执：任务已处于 {task['status']}", dict(task), dict(task), now)
                self._audit(connection, actor=worker_id, action="compute.receipt.ignored", task_id=task_id,
                            before={"status": task["status"]}, after={"status": task["status"]},
                            metadata={"kind": "complete", "reason": "terminal"})
                return self._receipt_response(connection, task, receipt_digest, outcome="ignored")
            if task["status"] == "cancel_requested":
                if task["lease_owner"] != worker_id:
                    raise ConflictError("任务未由当前工作者持有")
                # 取消后第一次回执：在此刻收敛并停止计费；回执内容留痕但不作为有效结果。
                version = self._next_result_version(connection, task_id)
                connection.execute(
                    "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,receipt_digest,accepted,created_by,created_at) VALUES(?,?,?,?,?,?,0,?,?)",
                    (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest(body), receipt_digest, worker_id, now),
                )
                before = dict(task)
                self._settle_billing(connection, task, now_value)
                connection.execute(
                    "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,cancelled_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, now, now, task_id),
                )
                self._store_receipt(connection, task_id, "complete", worker_id, receipt_digest, True, body, now, request_key)
                after = dict(repository.task_by_id(task_id))
                self._intervention_row(connection, task_id, worker_id, "cancel_settle",
                                       "现场完成回执到达，按取消请求收敛", before, after, now)
                self._audit(connection, actor=worker_id, action="compute.cancel.settled", task_id=task_id,
                            before={"status": before["status"]}, after={"status": "cancelled"},
                            metadata={"receipt": "complete", "billing": json.loads(after["billing_detail_json"])})
                return self._receipt_response(connection, after, receipt_digest, outcome="cancelled" if after["status"] == "cancelled" else "applied")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            # 正常完成流程（保持旧版兼容）。
            version = self._next_result_version(connection, task_id)
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,receipt_digest,accepted,created_by,created_at) VALUES(?,?,?,?,?,?,1,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), digest(body), receipt_digest, worker_id, now),
            )
            self._settle_billing(connection, task, now_value)
            before = dict(task)
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, now, task_id),
            )
            self._store_receipt(connection, task_id, "complete", worker_id, receipt_digest, True, body, now, request_key)
            after = dict(repository.task_by_id(task_id))
            self._audit(connection, actor=worker_id, action="compute.complete", task_id=task_id,
                        before={"status": before["status"]}, after={"status": "succeeded", "result_version": version},
                        metadata={"billing": json.loads(after["billing_detail_json"])})
            return self._receipt_response(connection, after, receipt_digest, outcome="cancelled" if after["status"] == "cancelled" else "applied")

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool, request_key: str | None = None) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        body = {"error_code": error_code, "message": message, "retryable": retryable}
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            # attempt 进入指纹：不同重试轮次允许相同失败内容；同一轮次的网络重试仍去重回放。
            receipt_digest = digest({"kind": "fail", "worker_id": worker_id, "attempt": int(task["attempt_count"]), **body})
            repeated = repository.receipt_by_digest(task_id, receipt_digest)
            if repeated is not None:
                return self._receipt_response(connection, task, receipt_digest, outcome="replayed", accepted=bool(repeated["accepted"]))
            if task["status"] in TERMINAL_STATUSES:
                self._store_receipt(connection, task_id, "fail", worker_id, receipt_digest, False, body, now, request_key)
                self._intervention_row(connection, task_id, worker_id, "receipt_ignored",
                                       f"迟到失败回执：任务已处于 {task['status']}", dict(task), dict(task), now)
                self._audit(connection, actor=worker_id, action="compute.receipt.ignored", task_id=task_id,
                            before={"status": task["status"]}, after={"status": task["status"]},
                            metadata={"kind": "fail", "reason": "terminal"})
                return self._receipt_response(connection, task, receipt_digest, outcome="ignored")
            if task["status"] == "cancel_requested":
                if task["lease_owner"] != worker_id:
                    raise ConflictError("任务未由当前工作者持有")
                before = dict(task)
                self._settle_billing(connection, task, now_value)
                connection.execute(
                    "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,cancelled_at=?,last_error_code=?,last_error_message=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, now, error_code, message[:2000], now, task_id),
                )
                self._store_receipt(connection, task_id, "fail", worker_id, receipt_digest, True, body, now, request_key)
                after = dict(repository.task_by_id(task_id))
                self._intervention_row(connection, task_id, worker_id, "cancel_settle",
                                       "现场失败回执到达，按取消请求收敛", before, after, now)
                self._audit(connection, actor=worker_id, action="compute.cancel.settled", task_id=task_id,
                            before={"status": before["status"]}, after={"status": "cancelled"},
                            metadata={"receipt": "fail", "billing": json.loads(after["billing_detail_json"])})
                return self._receipt_response(connection, after, receipt_digest, outcome="cancelled" if after["status"] == "cancelled" else "applied")
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            status = "queued" if can_retry else "failed"
            delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1)) if can_retry else 0
            available = to_storage(now_value + timedelta(seconds=delay))
            before = dict(task)
            if can_retry:
                connection.execute(
                    "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, available, error_code, message[:2000], now, task_id),
                )
            else:
                self._settle_billing(connection, task, now_value)
                connection.execute(
                    "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, available, error_code, message[:2000], now, now, task_id),
                )
            self._store_receipt(connection, task_id, "fail", worker_id, receipt_digest, True, body, now, request_key)
            after = dict(repository.task_by_id(task_id))
            self._audit(connection, actor=worker_id, action="compute.fail", task_id=task_id,
                        before={"status": before["status"]}, after={"status": status, "retry": can_retry},
                        metadata={"error_code": error_code})
            return self._receipt_response(connection, after, receipt_digest, outcome="cancelled" if after["status"] == "cancelled" else "applied")

    def cancel(self, task_id: int, actor: str, reason: str, batch_key: str = "", request_key: str | None = None) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            # 重复取消：无论是否携带 request_key，都回放当前稳定结果，不覆盖原决定。
            if request_key:
                prior = connection.execute(
                    "SELECT id FROM compute_interventions WHERE task_id=? AND action='cancel' AND request_key=? ORDER BY id LIMIT 1",
                    (task_id, request_key),
                ).fetchone()
                if prior is not None:
                    return self._cancel_response(connection, dict(repository.task_by_id(task_id)), repeated=True)
            if task["status"] in {"cancel_requested", "cancelled"}:
                return self._cancel_response(connection, dict(task), repeated=True)
            before = dict(task)
            if task["status"] == "queued":
                # 尚未开始：立即结束，不计任何费用，同时记录取消决定。
                template = connection.execute("SELECT * FROM compute_templates WHERE id=?", (task["template_id"],)).fetchone()
                zero_detail = self._billing_detail(template, 0, now)
                connection.execute(
                    "UPDATE compute_tasks SET status='cancelled',finished_at=?,cancelled_at=?,"
                    "cancel_requested_at=?,cancel_requested_by=?,cancel_reason=?,"
                    "billing_stopped_at=?,billing_seconds=0,billing_amount=0,billing_detail_json=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, now, now, actor, reason, now, json.dumps(zero_detail, ensure_ascii=False, sort_keys=True), now, task_id),
                )
                action = "compute.cancelled"
                after_status = "cancelled"
            elif task["status"] == "running":
                # 正在执行：记录取消请求，等待下一次回执收敛；计费在回执前继续。
                connection.execute(
                    "UPDATE compute_tasks SET status='cancel_requested',cancel_requested_at=?,cancel_requested_by=?,cancel_reason=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, actor, reason, now, task_id),
                )
                action = "compute.cancel.requested"
                after_status = "cancel_requested"
            else:
                raise ConflictError("当前任务状态不允许取消")
            after_row = repository.task_by_id(task_id)
            after = dict(after_row)
            repository.add_intervention(task_id=task_id, actor=actor, action="cancel", reason=reason,
                                        before=before, after=after, batch_key=batch_key, now=now, request_key=request_key or "")
            self._audit(connection, actor=actor, action=action, task_id=task_id,
                        before={"status": before["status"]}, after={"status": after_status},
                        metadata={"reason": reason, "request_key": request_key or "", "batch_key": batch_key})
            return self._cancel_response(connection, after, repeated=False)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute(
                "UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,"
                "cancel_requested_at=NULL,cancel_requested_by='',cancel_reason='',cancelled_at=NULL,"
                "billing_stopped_at=NULL,billing_seconds=NULL,billing_amount=NULL,billing_detail_json='{}',"
                "updated_at=?,version=version+1 WHERE id=?",
                (chosen, now, now, task["id"]),
            )
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate, audit_action="compute.retry")

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate, audit_action="compute.priority")

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key)
                elif payload["operation"] == "retry":
                    value = self.retry(task_id, payload["actor"], payload["reason"], payload.get("priority"), batch_key)
                else:
                    value = self.set_priority(task_id, payload["actor"], payload["reason"], int(payload["priority"]), batch_key)
                succeeded.append({"task_id": task_id, "status": value["status"], "version": value["version"]})
            except (ConflictError, NotFoundError) as exc:
                failed.append({"task_id": task_id, "code": exc.code, "message": exc.message})
        return {"batch_key": batch_key, "succeeded": succeeded, "failed": failed}

    def recover_expired(self, actor: str = "recovery-worker") -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        recovered: list[int] = []
        exhausted: list[int] = []
        cancelled: list[int] = []
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            rows = connection.execute(
                "SELECT * FROM compute_tasks WHERE status IN ('running','cancel_requested') AND lease_expires_at<>'' AND lease_expires_at<? ORDER BY id",
                (now,),
            ).fetchall()
            for task in rows:
                before = dict(task)
                if task["status"] == "cancel_requested":
                    # 现场始终未回执：按租约到期时刻收敛取消，计费止于租约到期（最后一次心跳）。
                    lease_expired = from_storage(task["lease_expires_at"]) or now_value
                    self._settle_billing(connection, task, lease_expired)
                    connection.execute(
                        "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,cancelled_at=?,updated_at=?,version=version+1 WHERE id=?",
                        (task["lease_expires_at"], task["lease_expires_at"], now, task["id"]),
                    )
                    cancelled.append(int(task["id"]))
                    action = "lease_recovery"
                    reason = "取消请求后租约过期仍无回执，自动收敛"
                    audit_action = "compute.cancel.settled"
                    meta = {"receipt": "lease_expired"}
                elif int(task["attempt_count"]) < int(task["max_attempts"]):
                    status, finished_at = "queued", None
                    recovered.append(int(task["id"]))
                    connection.execute(
                        "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                        (status, now, finished_at, now, task["id"]),
                    )
                    action, reason, audit_action, meta = "lease_recovery", "租约过期自动恢复", "compute.lease_recovered", {"retry": True}
                else:
                    status, finished_at = "failed", now
                    exhausted.append(int(task["id"]))
                    self._settle_billing(connection, task, now_value)
                    connection.execute(
                        "UPDATE compute_tasks SET status=?,lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,updated_at=?,version=version+1 WHERE id=?",
                        (status, now, finished_at, now, task["id"]),
                    )
                    action, reason, audit_action, meta = "lease_recovery", "租约过期自动恢复", "compute.lease_recovered", {"retry": False}
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action=action, reason=reason, before=before, after=after, batch_key="", now=now)
                self._audit(connection, actor=actor, action=audit_action, task_id=task["id"],
                            before={"status": before["status"]}, after={"status": after["status"]}, metadata=meta)
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    # ------------------------------------------------------------------ 内部工具

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None], audit_action: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            before = dict(task)
            mutation(connection, task, now)
            after = dict(repository.task_by_id(task_id))
            repository.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key=batch_key, now=now)
            self._audit(connection, actor=actor, action=audit_action, task_id=task_id,
                        before={"status": before["status"]}, after={"status": after["status"]},
                        metadata={"reason": reason, "batch_key": batch_key})
            return self._decorate(connection, after)

    @staticmethod
    def _next_result_version(connection: sqlite3.Connection, task_id: int) -> int:
        return int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])

    @staticmethod
    def _store_receipt(connection: sqlite3.Connection, task_id: int, kind: str, worker_id: str, receipt_digest: str, accepted: bool, body: dict[str, Any], now: str, request_key: str | None) -> None:
        connection.execute(
            "INSERT INTO compute_receipts(task_id,kind,worker_id,receipt_digest,accepted,request_key,payload_json,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (task_id, kind, worker_id, receipt_digest, 1 if accepted else 0, request_key or "", json.dumps(body, ensure_ascii=False, sort_keys=True), now),
        )

    @staticmethod
    def _intervention_row(connection: sqlite3.Connection, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], now: str) -> None:
        connection.execute(
            "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,request_key,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (task_id, actor, action, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), "", "", now),
        )

    def _audit(self, connection: sqlite3.Connection, *, actor: str, action: str, task_id: int, before: dict[str, Any], after: dict[str, Any], metadata: dict[str, Any] | None = None) -> None:
        AuditRepository(connection).append(
            actor_user_id=None,
            actor_name=actor,
            action=action,
            resource_type="compute_task",
            resource_id=task_id,
            outcome="success",
            before=before,
            after=after,
            metadata=metadata or {},
            correlation_id=None,
            created_at=to_storage(self.clock.now()),
        )

    def _settle_billing(self, connection: sqlite3.Connection, task: sqlite3.Row, stopped_at: datetime) -> None:
        """按模板价格和实际执行时长结算费用，结果写回任务行。"""
        template = connection.execute("SELECT * FROM compute_templates WHERE id=?", (task["template_id"],)).fetchone()
        started = from_storage(task["started_at"])
        seconds = 0 if started is None else max(0, int((stopped_at - started).total_seconds()))
        stopped_text = to_storage(stopped_at)
        detail = self._billing_detail(template, seconds, stopped_text)
        connection.execute(
            "UPDATE compute_tasks SET billing_stopped_at=?,billing_seconds=?,billing_amount=?,billing_detail_json=? WHERE id=?",
            (stopped_text, seconds, detail["amount"], json.dumps(detail, ensure_ascii=False, sort_keys=True), task["id"]),
        )

    @staticmethod
    def _billing_detail(template: sqlite3.Row | dict[str, Any], seconds: int, stopped_at: str) -> dict[str, Any]:
        base_fee = int(template["base_fee"]) if template is not None else 0
        unit_fee = int(template["unit_fee"]) if template is not None else 0
        unit_seconds = int(template["billing_unit_seconds"]) if template is not None else 60
        unit_seconds = max(1, unit_seconds)
        units = math.ceil(seconds / unit_seconds) if seconds > 0 else 0
        amount = base_fee + units * unit_fee if seconds > 0 else 0
        return {
            "currency": "CNY",
            "base_fee": base_fee,
            "unit_fee": unit_fee,
            "billing_unit_seconds": unit_seconds,
            "billable_seconds": seconds,
            "units": units,
            "amount": amount,
            "stopped_at": stopped_at,
            "rule": "未开始不计费；执行中取消在下次回执（或租约到期）时停止计费",
        }

    def _decorate(self, connection: sqlite3.Connection, task: dict[str, Any]) -> dict[str, Any]:
        detail_json = task.get("billing_detail_json") or "{}"
        try:
            detail = json.loads(detail_json)
        except (TypeError, ValueError):
            detail = {}
        task["billing"] = {
            "status": "settled" if task.get("billing_amount") is not None else "open",
            "amount": task.get("billing_amount"),
            "billable_seconds": task.get("billing_seconds"),
            "stopped_at": task.get("billing_stopped_at"),
            "detail": detail,
        }
        task["cancellation"] = {
            "requested_at": task.get("cancel_requested_at"),
            "requested_by": task.get("cancel_requested_by") or None,
            "reason": task.get("cancel_reason") or None,
            "cancelled_at": task.get("cancelled_at"),
        }
        return task

    def _receipt_response(self, connection: sqlite3.Connection, task: sqlite3.Row | dict[str, Any], receipt_digest: str, *, outcome: str, accepted: bool | None = None) -> dict[str, Any]:
        result = self._decorate(connection, dict(task))
        if accepted is None:
            # applied/cancelled 表示首次确认生效；ignored 表示迟到留痕；replayed 另行传入 stored accepted。
            accepted = outcome in {"applied", "cancelled"}
        result["receipt"] = {
            "digest": receipt_digest,
            "outcome": outcome,
            "accepted": accepted,
            "repeated": outcome == "replayed",
        }
        return result

    def _cancel_response(self, connection: sqlite3.Connection, task: dict[str, Any], *, repeated: bool) -> dict[str, Any]:
        result = self._decorate(connection, task)
        result["cancellation"]["repeated"] = repeated
        return result

    def _check_quota(self, repository: ComputeRepository, requested_by: str, now: datetime) -> None:
        quota = repository.quota("user", requested_by)
        if quota is None:
            return
        states = repository.count_user_states(requested_by)
        if states.get("queued", 0) >= int(quota["max_queued"]):
            raise ConflictError("用户排队任务配额已用尽")
        if states.get("running", 0) >= int(quota["max_running"]):
            raise ConflictError("用户运行任务配额已用尽")
        day_start = to_storage(now.astimezone(UTC).replace(hour=0, minute=0, second=0, microsecond=0))
        if repository.count_user_submissions_since(requested_by, day_start) >= int(quota["daily_submissions"]):
            raise ConflictError("用户当日提交配额已用尽")

    @staticmethod
    def _validate_schema(schema: dict[str, dict[str, Any]], defaults: dict[str, Any]) -> None:
        if not schema:
            raise ValidationError("参数模板至少包含一个参数")
        allowed = {"integer", "number", "string", "boolean"}
        for name, rule in schema.items():
            if not name or not isinstance(rule, dict) or rule.get("type") not in allowed:
                raise ValidationError(f"参数 {name or '<empty>'} 的规则不合法")
        if set(defaults) - set(schema):
            raise ValidationError("默认值包含未声明参数")

    def _validate_parameters(self, template: sqlite3.Row, supplied: dict[str, Any]) -> dict[str, Any]:
        schema = json.loads(template["parameter_schema_json"])
        values = {**json.loads(template["default_parameters_json"]), **supplied}
        unknown = set(values) - set(schema)
        if unknown:
            raise ValidationError("包含模板未声明的参数", context={"parameters": sorted(unknown)})
        normalized: dict[str, Any] = {}
        for name, rule in schema.items():
            if name not in values:
                if rule.get("required"):
                    raise ValidationError(f"缺少必填参数：{name}")
                continue
            value = values[name]
            kind = rule["type"]
            valid = {"integer": isinstance(value, int) and not isinstance(value, bool), "number": isinstance(value, (int, float)) and not isinstance(value, bool), "string": isinstance(value, str), "boolean": isinstance(value, bool)}[kind]
            if not valid:
                raise ValidationError(f"参数 {name} 类型不正确")
            if rule.get("minimum") is not None and value < rule["minimum"]:
                raise ValidationError(f"参数 {name} 小于允许的最小值")
            if rule.get("maximum") is not None and value > rule["maximum"]:
                raise ValidationError(f"参数 {name} 大于允许的最大值")
            if rule.get("choices") and value not in rule["choices"]:
                raise ValidationError(f"参数 {name} 不在允许的选项中")
            normalized[name] = value
        return normalized
