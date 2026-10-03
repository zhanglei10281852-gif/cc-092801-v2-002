from __future__ import annotations

import json
import sqlite3
from typing import Any, Iterable


class ComputeRepository:
    """封装计算任务运营领域的 SQLite 读写。"""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def template_by_code(self, code: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE code=?", (code,)).fetchone()

    def template_by_id(self, template_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_templates WHERE id=?", (template_id,)).fetchone()

    def active_templates(self) -> list[dict[str, Any]]:
        rows = self.connection.execute("SELECT * FROM compute_templates WHERE active=1 ORDER BY code,version").fetchall()
        return [dict(row) for row in rows]

    def create_template(self, *, code: str, name: str, algorithm: str, parameter_schema: dict[str, Any], defaults: dict[str, Any], max_runtime_seconds: int, max_attempts: int, created_by: str, now: str, base_fee_cents: int = 0, unit_fee_cents: int = 0, billing_unit_seconds: int = 60) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_templates(code,name,algorithm,version,parameter_schema_json,default_parameters_json,max_runtime_seconds,max_attempts,base_fee_cents,unit_fee_cents,billing_unit_seconds,active,created_by,created_at,updated_at) VALUES(?,?,?,1,?,?,?,?,?,?,?,1,?,?,?)",
            (code, name, algorithm, json.dumps(parameter_schema, ensure_ascii=False, sort_keys=True), json.dumps(defaults, ensure_ascii=False, sort_keys=True), max_runtime_seconds, max_attempts, base_fee_cents, unit_fee_cents, billing_unit_seconds, created_by, now, now),
        )
        return dict(self.template_by_id(cursor.lastrowid))

    def quota(self, subject_type: str, subject_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_quotas WHERE subject_type=? AND subject_key=?", (subject_type, subject_key)).fetchone()

    def upsert_quota(self, *, subject_type: str, subject_key: str, max_queued: int, max_running: int, daily_submissions: int, actor: str, now: str) -> dict[str, Any]:
        self.connection.execute(
            "INSERT INTO compute_quotas(subject_type,subject_key,max_queued,max_running,daily_submissions,updated_by,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(subject_type,subject_key) DO UPDATE SET max_queued=excluded.max_queued,max_running=excluded.max_running,daily_submissions=excluded.daily_submissions,updated_by=excluded.updated_by,updated_at=excluded.updated_at",
            (subject_type, subject_key, max_queued, max_running, daily_submissions, actor, now, now),
        )
        return dict(self.quota(subject_type, subject_key))

    def count_user_states(self, requested_by: str) -> dict[str, int]:
        rows = self.connection.execute("SELECT status,COUNT(*) AS amount FROM compute_tasks WHERE requested_by=? GROUP BY status", (requested_by,)).fetchall()
        return {str(row["status"]): int(row["amount"]) for row in rows}

    def count_user_submissions_since(self, requested_by: str, since: str) -> int:
        return int(self.connection.execute("SELECT COUNT(*) FROM compute_tasks WHERE requested_by=? AND created_at>=?", (requested_by, since)).fetchone()[0])

    def task_by_id(self, task_id: int) -> sqlite3.Row | None:
        return self.connection.execute("SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.id=?", (task_id,)).fetchone()

    def task_by_idempotency(self, requested_by: str, key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_tasks WHERE requested_by=? AND idempotency_key=?", (requested_by, key)).fetchone()

    def create_task(self, *, template_row: sqlite3.Row, project_code: str, requested_by: str, parameters: dict[str, Any], parameter_digest: str, priority: int, idempotency_key: str, max_attempts: int, now: str) -> dict[str, Any]:
        cursor = self.connection.execute(
            "INSERT INTO compute_tasks(template_id,project_code,requested_by,parameters_json,parameter_digest,priority,idempotency_key,status,attempt_count,max_attempts,available_at,base_fee_cents,unit_fee_cents,billing_unit_seconds,created_at,updated_at) VALUES(?,?,?,?,?,?,?,'queued',0,?,?,?,?,?,?,?)",
            (template_row["id"], project_code, requested_by, json.dumps(parameters, ensure_ascii=False, sort_keys=True), parameter_digest, priority, idempotency_key, max_attempts, now, int(template_row["base_fee_cents"]), int(template_row["unit_fee_cents"]), int(template_row["billing_unit_seconds"]), now, now),
        )
        return dict(self.task_by_id(cursor.lastrowid))

    def queued_candidate(self, capabilities: Iterable[str], now: str) -> sqlite3.Row | None:
        capability_list = sorted(set(capabilities))
        params: list[Any] = [now]
        condition = ""
        if capability_list:
            placeholders = ",".join("?" for _ in capability_list)
            condition = f" AND tpl.algorithm IN ({placeholders})"
            params.extend(capability_list)
        return self.connection.execute(
            "SELECT t.*,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id WHERE t.status='queued' AND t.available_at<=?" + condition + " ORDER BY t.priority DESC,t.created_at ASC,t.id ASC LIMIT 1",
            params,
        ).fetchone()

    def result_versions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_results WHERE task_id=? ORDER BY version", (task_id,)).fetchall()]

    def interventions(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_interventions WHERE task_id=? ORDER BY id", (task_id,)).fetchall()]

    def charges(self, task_id: int) -> list[dict[str, Any]]:
        return [dict(row) for row in self.connection.execute("SELECT * FROM compute_charges WHERE task_id=? ORDER BY id", (task_id,)).fetchall()]

    def add_charge(self, *, task_id: int, fee_type: str, amount_cents: int, quantity_seconds: int, status: str, reason: str, created_by: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_charges(task_id,fee_type,amount_cents,quantity_seconds,status,reason,created_by,created_at) VALUES(?,?,?,?,?,?,?,?)",
            (task_id, fee_type, amount_cents, quantity_seconds, status, reason, created_by, now),
        )

    def receipt(self, task_id: int, receipt_key: str) -> sqlite3.Row | None:
        return self.connection.execute("SELECT * FROM compute_receipts WHERE task_id=? AND receipt_key=?", (task_id, receipt_key)).fetchone()

    def add_receipt(self, *, task_id: int, receipt_key: str, kind: str, worker_id: str, payload_digest: str, disposition: str, resulting_status: str, response_json: str, created_by: str, now: str) -> None:
        self.connection.execute(
            "INSERT INTO compute_receipts(task_id,receipt_key,kind,worker_id,payload_digest,disposition,resulting_status,response_json,created_by,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
            (task_id, receipt_key, kind, worker_id, payload_digest, disposition, resulting_status, response_json, created_by, now),
        )

    def project_summary(self, project_code: str) -> dict[str, Any]:
        row = self.connection.execute(
            "SELECT COUNT(*) AS task_count, COALESCE(SUM(charged_cents),0) AS charged_cents, COALESCE(SUM(billable_seconds),0) AS billable_seconds FROM compute_tasks WHERE project_code=?",
            (project_code,),
        ).fetchone()
        return {"task_count": int(row["task_count"]), "charged_cents": int(row["charged_cents"]), "billable_seconds": int(row["billable_seconds"])}

    def project_charges(self, project_code: str) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            "SELECT c.* FROM compute_charges c JOIN compute_tasks t ON t.id=c.task_id WHERE t.project_code=? ORDER BY c.id",
            (project_code,),
        ).fetchall()
        return [dict(row) for row in rows]

    def intervention_by_request_key(self, task_id: int, action: str, request_key: str) -> sqlite3.Row | None:
        return self.connection.execute(
            "SELECT * FROM compute_interventions WHERE task_id=? AND action=? AND request_key=?",
            (task_id, action, request_key),
        ).fetchone()

    def add_intervention(self, *, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], batch_key: str, now: str, request_key: str = "") -> None:
        self.connection.execute(
            "INSERT INTO compute_interventions(task_id,actor,action,reason,before_json,after_json,batch_key,request_key,created_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (task_id, actor, action, reason, json.dumps(before, ensure_ascii=False, sort_keys=True), json.dumps(after, ensure_ascii=False, sort_keys=True), batch_key, request_key, now),
        )

    def add_intervention_with_key(self, *, task_id: int, actor: str, action: str, reason: str, before: dict[str, Any], after: dict[str, Any], request_key: str | None, now: str) -> None:
        self.add_intervention(task_id=task_id, actor=actor, action=action, reason=reason, before=before, after=after, batch_key="", request_key=request_key or "", now=now)

    def list_tasks(self, *, status: str | None, project_code: str | None, requested_by: str | None, limit: int) -> list[dict[str, Any]]:
        clauses: list[str] = []
        values: list[Any] = []
        if status:
            clauses.append("t.status=?")
            values.append(status)
        if project_code:
            clauses.append("t.project_code=?")
            values.append(project_code)
        if requested_by:
            clauses.append("t.requested_by=?")
            values.append(requested_by)
        where = " WHERE " + " AND ".join(clauses) if clauses else ""
        values.append(limit)
        rows = self.connection.execute(
            "SELECT t.*,tpl.code AS template_code,tpl.algorithm AS template_algorithm FROM compute_tasks t JOIN compute_templates tpl ON tpl.id=t.template_id" + where + " ORDER BY t.priority DESC,t.created_at DESC,t.id DESC LIMIT ?",
            values,
        ).fetchall()
        return [dict(row) for row in rows]
