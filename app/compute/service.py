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
from app.services.audit import AuditContext, AuditService


def digest(value: Any) -> str:
    text = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(text.encode()).hexdigest()


TERMINAL_STATUSES = {"cancelled", "succeeded", "failed"}


class ComputeOperationsService:
    """管理服务订单模板、配额、任务租约、回执、取消收敛、计费与人工干预。

    取消与现场回执的收敛规则：

    * ``queued``（尚未开始）收到取消：立即进入终态 ``cancelled``，费用全额 waived。
    * ``running``（正在执行）收到取消：进入 ``cancel_requested`` 并记录决定人、
      原因与计费停止时刻；下一次 complete/fail 回执收敛为 ``cancelled``，
      执行费只计算到取消请求时刻。
    * 已进入终态的任务收到迟到回执：登记为 ``late_rejected``，任务状态与费用不变，
      服务不会被重新激活；相同回执键的重复投递返回首次的稳定结果。
    """

    def __init__(self, connection: sqlite3.Connection | None = None, clock: Clock | None = None) -> None:
        self.connection = connection or get_connection()
        self.clock = clock or SystemClock()
        self.repository = ComputeRepository(self.connection)

    # ---- 模板与配额 ------------------------------------------------------

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
                created_by=actor, now=now,
                base_fee_cents=payload.get("base_fee_cents", 0),
                unit_fee_cents=payload.get("unit_fee_cents", 0),
                billing_unit_seconds=payload.get("billing_unit_seconds", 60),
            )

    def set_quota(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        now = to_storage(self.clock.now())
        with transaction(immediate=True) as connection:
            return ComputeRepository(connection).upsert_quota(actor=actor, now=now, **payload)

    # ---- 提交与查询 ------------------------------------------------------

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
                return dict(repository.task_by_id(existing["id"]))
            self._check_quota(repository, payload["requested_by"], now_value)
            return repository.create_task(
                template_row=template, project_code=payload["project_code"],
                requested_by=payload["requested_by"], parameters=parameters,
                parameter_digest=parameter_digest, priority=payload["priority"],
                idempotency_key=payload["idempotency_key"], max_attempts=template["max_attempts"], now=now,
            )

    def list_tasks(self, *, status: str | None = None, project_code: str | None = None, requested_by: str | None = None, limit: int = 100) -> list[dict[str, Any]]:
        return self.repository.list_tasks(status=status, project_code=project_code, requested_by=requested_by, limit=max(1, min(limit, 500)))

    def get_task(self, task_id: int) -> dict[str, Any]:
        row = self.repository.task_by_id(task_id)
        if row is None:
            raise NotFoundError("计算任务不存在")
        result = dict(row)
        result["results"] = self.repository.result_versions(task_id)
        result["interventions"] = self.repository.interventions(task_id)
        result["charges"] = self.repository.charges(task_id)
        return result

    def project_billing(self, project_code: str) -> dict[str, Any]:
        tasks = self.repository.list_tasks(status=None, project_code=project_code, requested_by=None, limit=500)
        return {
            "project_code": project_code,
            "summary": self.repository.project_summary(project_code),
            "tasks": [
                {
                    "id": task["id"],
                    "template_code": task["template_code"],
                    "status": task["status"],
                    "billable_seconds": task["billable_seconds"],
                    "charged_cents": task["charged_cents"],
                    "cancel_requested_at": task["cancel_requested_at"],
                    "cancellation_actor": task["cancellation_actor"],
                    "charges": self.repository.charges(task["id"]),
                }
                for task in tasks
            ],
            "charges": self.repository.project_charges(project_code),
        }

    # ---- 租约 ------------------------------------------------------------

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
                "UPDATE compute_tasks SET status='running',attempt_count=attempt_count+1,lease_owner=?,lease_expires_at=?,started_at=?,updated_at=?,version=version+1 WHERE id=? AND status='queued'",
                (worker_id, lease_until, now, now, candidate["id"]),
            )
            if cursor.rowcount != 1:
                return None
            return dict(repository.task_by_id(candidate["id"]))

    def heartbeat(self, task_id: int, worker_id: str, lease_seconds: int) -> dict[str, Any]:
        now_value = self.clock.now()
        now = to_storage(now_value)
        expires = to_storage(now_value + timedelta(seconds=lease_seconds))
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            if task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")
            if task["status"] == "cancel_requested":
                # 取消已在等待现场收敛：不续期租约，原样返回以便工作者尽快停止。
                return dict(task)
            if task["status"] != "running":
                raise ConflictError("任务未由当前工作者持有")
            cursor = connection.execute(
                "UPDATE compute_tasks SET lease_expires_at=?,updated_at=?,version=version+1 WHERE id=? AND status='running' AND lease_owner=?",
                (expires, now, task_id, worker_id),
            )
            if cursor.rowcount != 1:
                raise ConflictError("任务未由当前工作者持有")
            return dict(repository.task_by_id(task_id))

    # ---- 现场回执 --------------------------------------------------------

    def complete(self, task_id: int, worker_id: str, result: dict[str, Any], metrics: dict[str, Any], idempotency_key: str | None = None) -> dict[str, Any]:
        payload_digest = digest({"result": result, "metrics": metrics})
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            # 自动回执键按执行轮次（attempt）隔离：同一轮的重复投递返回稳定结果，
            # 不同轮次的真实重试不会被误判为重复。
            receipt_key = idempotency_key or "auto:complete:" + digest({"worker": worker_id, "payload": payload_digest, "attempt": task["attempt_count"]})
            cached = self._cached_receipt(repository, task_id, receipt_key)
            if cached is not None:
                return cached

            if task["status"] in TERMINAL_STATUSES:
                return self._reject_late_receipt(
                    connection, repository, task, receipt_key=receipt_key, kind="complete",
                    worker_id=worker_id, payload_digest=payload_digest, now=now,
                )
            if task["status"] == "cancel_requested":
                if task["lease_owner"] != worker_id:
                    raise ConflictError("任务未由当前工作者持有")
                return self._converge_cancel_on_receipt(
                    connection, repository, task, receipt_key=receipt_key, kind="complete",
                    worker_id=worker_id, payload_digest=payload_digest, now=now,
                )
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")

            version = int(connection.execute("SELECT COALESCE(MAX(version),0)+1 FROM compute_results WHERE task_id=?", (task_id,)).fetchone()[0])
            connection.execute(
                "INSERT INTO compute_results(task_id,version,result_json,metrics_json,result_digest,created_by,created_at) VALUES(?,?,?,?,?,?,?)",
                (task_id, version, json.dumps(result, ensure_ascii=False, sort_keys=True), json.dumps(metrics, ensure_ascii=False, sort_keys=True), payload_digest, worker_id, now),
            )
            billable = self._billable_seconds(task, now_value)
            charged = self._settle_charges(connection, task, billable=billable, now=now, created_by=worker_id, waive_all=False)
            connection.execute(
                "UPDATE compute_tasks SET status='succeeded',current_result_version=?,lease_owner='',lease_expires_at='',finished_at=?,billable_seconds=?,charged_cents=?,billed_at=?,updated_at=?,version=version+1 WHERE id=?",
                (version, now, billable, charged, now, now, task_id),
            )
            after = dict(repository.task_by_id(task_id))
            self._register_receipt(repository, task, after, receipt_key=receipt_key, kind="complete", worker_id=worker_id, payload_digest=payload_digest, disposition="accepted", now=now)
            self._audit(connection, worker_id, "compute.complete", task, after, now, metadata={"disposition": "accepted", "receipt_key": receipt_key, "billable_seconds": billable, "charged_cents": charged})
            return self._receipt_response(after, "accepted", receipt_key)

    def fail(self, task_id: int, worker_id: str, error_code: str, message: str, retryable: bool, idempotency_key: str | None = None) -> dict[str, Any]:
        payload_digest = digest({"error_code": error_code, "message": message, "retryable": retryable})
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")
            receipt_key = idempotency_key or "auto:fail:" + digest({"worker": worker_id, "payload": payload_digest, "attempt": task["attempt_count"]})
            cached = self._cached_receipt(repository, task_id, receipt_key)
            if cached is not None:
                return cached

            if task["status"] in TERMINAL_STATUSES:
                return self._reject_late_receipt(
                    connection, repository, task, receipt_key=receipt_key, kind="fail",
                    worker_id=worker_id, payload_digest=payload_digest, now=now,
                )
            if task["status"] == "cancel_requested":
                if task["lease_owner"] != worker_id:
                    raise ConflictError("任务未由当前工作者持有")
                return self._converge_cancel_on_receipt(
                    connection, repository, task, receipt_key=receipt_key, kind="fail",
                    worker_id=worker_id, payload_digest=payload_digest, now=now,
                )
            if task["status"] != "running" or task["lease_owner"] != worker_id:
                raise ConflictError("任务未由当前工作者持有")

            can_retry = retryable and int(task["attempt_count"]) < int(task["max_attempts"])
            if can_retry:
                status = "queued"
                delay = min(300, 2 ** max(0, int(task["attempt_count"]) - 1))
                available = to_storage(now_value + timedelta(seconds=delay))
                # 本轮执行时长累计到 billable_seconds，排队等待期间不计费；暂不出账。
                accumulated = self._billable_seconds(task, now_value)
                connection.execute(
                    "UPDATE compute_tasks SET status=?,available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,billable_seconds=?,updated_at=?,version=version+1 WHERE id=?",
                    (status, available, error_code, message[:2000], accumulated, now, task_id),
                )
            else:
                billable = self._billable_seconds(task, now_value)
                charged = self._settle_charges(connection, task, billable=billable, now=now, created_by=worker_id, waive_all=False)
                connection.execute(
                    "UPDATE compute_tasks SET status='failed',available_at=?,lease_owner='',lease_expires_at='',last_error_code=?,last_error_message=?,finished_at=?,billable_seconds=?,charged_cents=?,billed_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, error_code, message[:2000], now, billable, charged, now, now, task_id),
                )
            after = dict(repository.task_by_id(task_id))
            self._register_receipt(repository, task, after, receipt_key=receipt_key, kind="fail", worker_id=worker_id, payload_digest=payload_digest, disposition="accepted", now=now)
            self._audit(connection, worker_id, "compute.fail", task, after, now, metadata={"disposition": "accepted", "receipt_key": receipt_key, "retryable": retryable, "requeued": can_retry})
            return self._receipt_response(after, "accepted", receipt_key)

    # ---- 取消与人工干预 --------------------------------------------------

    def cancel(self, task_id: int, actor: str, reason: str, idempotency_key: str | None = None, batch_key: str = "") -> dict[str, Any]:
        request_key = idempotency_key or batch_key or None
        now_value = self.clock.now()
        now = to_storage(now_value)
        with transaction(immediate=True) as connection:
            repository = ComputeRepository(connection)
            task = repository.task_by_id(task_id)
            if task is None:
                raise NotFoundError("计算任务不存在")

            if request_key:
                existing = repository.intervention_by_request_key(task_id, "cancel", request_key)
                if existing is not None and int(json.loads(existing["after_json"]).get("settlement_seq", 0)) == int(task["settlement_seq"]):
                    return self._cancel_response(dict(repository.task_by_id(task_id)), request_key, duplicate=True)

            before = dict(task)
            duplicate = False
            if task["status"] == "queued":
                # 尚未开始：立即结束并免收全部费用。
                connection.execute(
                    "UPDATE compute_tasks SET status='cancelled',finished_at=?,cancellation_actor=?,cancellation_reason=?,cancel_requested_at=?,billed_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (now, actor, reason, now, now, now, task_id),
                )
                after_row = repository.task_by_id(task_id)
                self._settle_charges(connection, after_row, billable=0, now=now, created_by=actor, waive_all=True, reason=reason)
                after = dict(repository.task_by_id(task_id))
                self._record_intervention(repository, task_id, actor, "cancel", reason, before, after, now, request_key)
                self._audit(connection, actor, "compute.cancel", before, after, now, metadata={"phase": "queued", "reason": reason, "request_key": request_key})
            elif task["status"] == "running":
                # 正在执行：记录取消请求与计费停止时刻，等待下一次现场回执收敛。
                connection.execute(
                    "UPDATE compute_tasks SET status='cancel_requested',cancellation_actor=?,cancellation_reason=?,cancel_requested_at=?,updated_at=?,version=version+1 WHERE id=?",
                    (actor, reason, now, now, task_id),
                )
                after = dict(repository.task_by_id(task_id))
                self._record_intervention(repository, task_id, actor, "cancel", reason, before, after, now, request_key)
                self._audit(connection, actor, "compute.cancel", before, after, now, metadata={"phase": "running", "reason": reason, "request_key": request_key})
            elif task["status"] == "cancel_requested":
                # 重复取消：稳定幂等，不覆盖原决定人与决定时刻。
                duplicate = True
                after = dict(task)
                self._audit(connection, actor, "compute.cancel", before, after, now, metadata={"duplicate": True, "reason": reason, "request_key": request_key})
            elif task["status"] == "cancelled":
                duplicate = True
                after = dict(task)
                self._audit(connection, actor, "compute.cancel", before, after, now, metadata={"duplicate": True, "reason": reason, "request_key": request_key})
            else:
                raise ConflictError("当前任务状态不允许取消")
            return self._cancel_response(after, request_key, duplicate=duplicate)

    def retry(self, task_id: int, actor: str, reason: str, priority: int | None = None, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"failed", "cancelled"}:
                raise ConflictError("只有失败或已取消任务可以人工重试")
            chosen = task["priority"] if priority is None else priority
            connection.execute(
                "UPDATE compute_tasks SET status='queued',priority=?,available_at=?,lease_owner='',lease_expires_at='',finished_at=NULL,"
                "billable_seconds=0,charged_cents=0,billed_at='',cancellation_actor='',cancellation_reason='',cancel_requested_at='',"
                "settlement_seq=settlement_seq+1,updated_at=?,version=version+1 WHERE id=?",
                (chosen, now, now, task["id"]),
            )
        return self._intervene(task_id, actor, reason, "retry", batch_key, mutate)

    def set_priority(self, task_id: int, actor: str, reason: str, priority: int, batch_key: str = "") -> dict[str, Any]:
        def mutate(connection: sqlite3.Connection, task: sqlite3.Row, now: str) -> None:
            if task["status"] not in {"queued", "running"}:
                raise ConflictError("只有排队或运行中的任务可以调整优先级")
            connection.execute("UPDATE compute_tasks SET priority=?,updated_at=?,version=version+1 WHERE id=?", (priority, now, task["id"]))
        return self._intervene(task_id, actor, reason, "priority", batch_key, mutate)

    def batch_operation(self, payload: dict[str, Any]) -> dict[str, Any]:
        batch_key = digest({"actor": payload["actor"], "task_ids": payload["task_ids"], "operation": payload["operation"], "reason": payload["reason"]})
        succeeded: list[dict[str, Any]] = []
        failed: list[dict[str, Any]] = []
        for task_id in list(dict.fromkeys(payload["task_ids"])):
            try:
                if payload["operation"] == "cancel":
                    value = self.cancel(task_id, payload["actor"], payload["reason"], batch_key=batch_key)
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
                    # 现场始终未回执：按取消请求时刻停止计费并收敛为终态。
                    billable = self._billable_seconds(task, from_storage(task["cancel_requested_at"]))
                    charged = self._settle_charges(connection, task, billable=billable, now=now, created_by=actor, waive_all=False)
                    connection.execute(
                        "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,billable_seconds=?,charged_cents=?,billed_at=?,updated_at=?,version=version+1 WHERE id=?",
                        (now, billable, charged, now, now, task["id"]),
                    )
                    cancelled.append(int(task["id"]))
                elif int(task["attempt_count"]) < int(task["max_attempts"]):
                    accumulated = self._billable_seconds(task, from_storage(task["lease_expires_at"]) or now_value)
                    connection.execute(
                        "UPDATE compute_tasks SET status='queued',lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',billable_seconds=?,updated_at=?,version=version+1 WHERE id=?",
                        (now, accumulated, now, task["id"]),
                    )
                    recovered.append(int(task["id"]))
                else:
                    billable = self._billable_seconds(task, from_storage(task["lease_expires_at"]) or now_value)
                    charged = self._settle_charges(connection, task, billable=billable, now=now, created_by=actor, waive_all=False)
                    connection.execute(
                        "UPDATE compute_tasks SET status='failed',lease_owner='',lease_expires_at='',available_at=?,last_error_code='lease_expired',last_error_message='工作者租约已过期',finished_at=?,billable_seconds=?,charged_cents=?,billed_at=?,updated_at=?,version=version+1 WHERE id=?",
                        (now, now, billable, charged, now, now, task["id"]),
                    )
                    exhausted.append(int(task["id"]))
                after = dict(repository.task_by_id(task["id"]))
                repository.add_intervention(task_id=task["id"], actor=actor, action="lease_recovery", reason="租约过期自动恢复", before=before, after=after, batch_key="", now=now)
                self._audit(connection, actor, "compute.lease_recovery", before, after, now, metadata={"recovered": int(task["id"]) in recovered, "cancelled": int(task["id"]) in cancelled})
        return {"recovered": recovered, "exhausted": exhausted, "cancelled": cancelled}

    def summary(self) -> dict[str, Any]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks GROUP BY status ORDER BY status").fetchall()
        oldest = self.connection.execute("SELECT MIN(created_at) FROM compute_tasks WHERE status='queued'").fetchone()[0]
        return {"states": {row["status"]: row["amount"] for row in rows}, "oldest_queued_at": oldest, "templates": len(self.repository.active_templates())}

    # ---- 内部收敛与计费辅助 ----------------------------------------------

    def _intervene(self, task_id: int, actor: str, reason: str, action: str, batch_key: str, mutation: Callable[[sqlite3.Connection, sqlite3.Row, str], None]) -> dict[str, Any]:
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
            self._audit(connection, actor, f"compute.{action}", before, after, now, metadata={"reason": reason, "batch_key": batch_key or None})
            return after

    def _converge_cancel_on_receipt(self, connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, *, receipt_key: str, kind: str, worker_id: str, payload_digest: str, now: str) -> dict[str, Any]:
        """现场回执到达时，把 cancel_requested 收敛为 cancelled，计费截止到取消请求时刻。"""
        before = dict(task)
        stop_at = from_storage(task["cancel_requested_at"]) or self.clock.now()
        billable = self._billable_seconds(task, stop_at)
        charged = self._settle_charges(connection, task, billable=billable, now=now, created_by=worker_id, waive_all=False)
        connection.execute(
            "UPDATE compute_tasks SET status='cancelled',lease_owner='',lease_expires_at='',finished_at=?,billable_seconds=?,charged_cents=?,billed_at=?,updated_at=?,version=version+1 WHERE id=?",
            (now, billable, charged, now, now, task["id"]),
        )
        after = dict(repository.task_by_id(task["id"]))
        repository.add_intervention(
            task_id=task["id"], actor=worker_id, action="cancel_acknowledged",
            reason=f"现场{kind}回执确认执行取消（计费截止 {task['cancel_requested_at']}）",
            before=before, after=after, batch_key="", now=now,
        )
        self._register_receipt(repository, task, after, receipt_key=receipt_key, kind=kind, worker_id=worker_id, payload_digest=payload_digest, disposition="cancelled", now=now)
        self._audit(connection, worker_id, f"compute.{kind}", before, after, now,
                    metadata={"disposition": "cancelled", "receipt_key": receipt_key, "billable_seconds": billable, "charged_cents": charged,
                              "cancellation_actor": task["cancellation_actor"], "cancel_requested_at": task["cancel_requested_at"]})
        return self._receipt_response(after, "cancelled", receipt_key)

    def _reject_late_receipt(self, connection: sqlite3.Connection, repository: ComputeRepository, task: sqlite3.Row, *, receipt_key: str, kind: str, worker_id: str, payload_digest: str, now: str) -> dict[str, Any]:
        """迟到回执：任务已是终态，拒绝复活，只留痕并返回稳定结果。"""
        before = dict(task)
        repository.add_intervention(
            task_id=task["id"], actor=worker_id, action="late_receipt",
            reason=f"迟到的{kind}回执被拒绝，任务已处于 {task['status']} 终态",
            before=before, after=before, batch_key="", now=now,
        )
        response = self._receipt_response(before, "late_rejected", receipt_key)
        self._register_receipt(repository, task, before, receipt_key=receipt_key, kind=kind, worker_id=worker_id, payload_digest=payload_digest, disposition="late_rejected", now=now)
        self._audit(connection, worker_id, f"compute.{kind}", before, before, now,
                    outcome="denied", metadata={"disposition": "late_rejected", "receipt_key": receipt_key, "terminal_status": task["status"]})
        return response

    @staticmethod
    def _receipt_response(task: dict[str, Any], disposition: str, receipt_key: str) -> dict[str, Any]:
        response = dict(task)
        response["receipt"] = {"disposition": disposition, "idempotency_key": receipt_key}
        return response

    @staticmethod
    def _cancel_response(task: dict[str, Any], request_key: str | None, *, duplicate: bool) -> dict[str, Any]:
        response = dict(task)
        response["cancellation"] = {"duplicate": duplicate, "idempotency_key": request_key}
        return response

    @staticmethod
    def _cached_receipt(repository: ComputeRepository, task_id: int, receipt_key: str) -> dict[str, Any] | None:
        row = repository.receipt(task_id, receipt_key)
        if row is None:
            return None
        return json.loads(row["response_json"])

    @staticmethod
    def _register_receipt(repository: ComputeRepository, task: sqlite3.Row, response_task: dict[str, Any], *, receipt_key: str, kind: str, worker_id: str, payload_digest: str, disposition: str, now: str) -> None:
        response = ComputeOperationsService._receipt_response(response_task, disposition, receipt_key)
        repository.add_receipt(
            task_id=task["id"], receipt_key=receipt_key, kind=kind, worker_id=worker_id,
            payload_digest=payload_digest, disposition=disposition,
            resulting_status=response_task["status"],
            response_json=json.dumps(response, ensure_ascii=False, sort_keys=True),
            created_by=worker_id, now=now,
        )

    @staticmethod
    def _record_intervention(repository: ComputeRepository, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], now: str, request_key: str | None) -> None:
        repository.add_intervention_with_key(
            task_id=task_id, actor=actor, action=action, reason=reason,
            before=before, after=after, request_key=request_key, now=now,
        )

    @staticmethod
    def _billable_seconds(task: sqlite3.Row, stop_at: datetime) -> int:
        accumulated = int(task["billable_seconds"])
        started = from_storage(task["started_at"])
        if started is None:
            return accumulated
        elapsed = int((stop_at - started).total_seconds())
        return accumulated + max(0, elapsed)

    @staticmethod
    def _settle_charges(connection: sqlite3.Connection, task: sqlite3.Row, *, billable: int, now: str, created_by: str, waive_all: bool, reason: str = "") -> int:
        """写入本轮结算费用明细，返回应收总额（分）。"""
        seq = int(task["settlement_seq"])
        base = int(task["base_fee_cents"])
        unit = int(task["unit_fee_cents"])
        unit_seconds = max(1, int(task["billing_unit_seconds"]))
        if billable > 0:
            units = math.ceil(billable / unit_seconds)
            quantity = units * unit_seconds
        else:
            units, quantity = 0, 0
        execution = unit * units
        if waive_all:
            connection.execute(
                "INSERT INTO compute_charges(task_id,settlement_seq,fee_type,amount_cents,quantity_seconds,status,reason,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (task["id"], seq, "base", 0, 0, "waived", reason or "未开始即取消，免收基础费", created_by, now),
            )
            connection.execute(
                "INSERT INTO compute_charges(task_id,settlement_seq,fee_type,amount_cents,quantity_seconds,status,reason,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
                (task["id"], seq, "execution", 0, 0, "waived", reason or "未开始即取消，免收执行费", created_by, now),
            )
            return 0
        connection.execute(
            "INSERT INTO compute_charges(task_id,settlement_seq,fee_type,amount_cents,quantity_seconds,status,reason,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (task["id"], seq, "base", base, 0, "billed", "", created_by, now),
        )
        connection.execute(
            "INSERT INTO compute_charges(task_id,settlement_seq,fee_type,amount_cents,quantity_seconds,status,reason,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (task["id"], seq, "execution", execution, quantity, "billed", "", created_by, now),
        )
        return base + execution

    def _audit(self, connection: sqlite3.Connection, actor: str, action: str, before: dict[str, Any] | sqlite3.Row, after: dict[str, Any] | sqlite3.Row, now: str, *, metadata: dict[str, Any] | None = None, outcome: str = "success") -> None:
        before_data = dict(before)
        after_data = dict(after)
        AuditService(connection, self.clock).record(
            AuditContext(actor_user_id=None, actor_name=actor),
            action=action, resource_type="compute_task", resource_id=after_data.get("id", before_data.get("id")),
            outcome=outcome, before=before_data, after=after_data, metadata=metadata,
        )

    # ---- 校验与配额 ------------------------------------------------------

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
