from __future__ import annotations

import json
from datetime import UTC, datetime

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection, init_db


FEE_TEMPLATE = {
    "code": "hearse-guide",
    "name": "出殡车与礼仪引导",
    "algorithm": "hearse-guide",
    "parameter_schema": {
        "distance_km": {"type": "integer", "required": True, "minimum": 1, "maximum": 200},
    },
    "default_parameters": {},
    "max_runtime_seconds": 3600,
    "max_attempts": 2,
    "base_fee_cents": 50000,       # 基础服务费 500 元
    "unit_fee_cents": 12000,       # 每分钟执行费 120 元
    "billing_unit_seconds": 60,
}


def payload(key: str, *, user: str = "family-1") -> dict:
    return {
        "template_code": "hearse-guide",
        "project_code": "funeral-2026",
        "requested_by": user,
        "parameters": {"distance_km": 20},
        "priority": 50,
        "idempotency_key": key,
    }


def make_service(client) -> tuple[ComputeOperationsService, FrozenClock]:
    init_db()
    clock = FrozenClock(datetime(2026, 10, 3, 1, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(FEE_TEMPLATE, "pricing-admin")
    return service, clock


def test_cancel_before_start_ends_immediately_and_waives_all_fees(client):
    service, clock = make_service(client)
    task = service.submit(payload("cancel-queued-001"))
    result = service.cancel(task["id"], "family-li", "家属临时取消出殡车", "cancel-key-0001")
    assert result["status"] == "cancelled"
    assert result["cancellation_actor"] == "family-li"
    assert result["cancel_requested_at"] == result["finished_at"]
    details = service.get_task(task["id"])
    assert details["charged_cents"] == 0
    assert details["billable_seconds"] == 0
    assert {item["status"] for item in details["charges"]} == {"waived"}
    assert all(item["amount_cents"] == 0 for item in details["charges"])


def test_running_cancel_stops_billing_at_request_on_next_receipt(client):
    service, clock = make_service(client)
    task = service.submit(payload("cancel-running-001"))
    claimed = service.claim("worker-on-road", ["hearse-guide"], 600)
    assert claimed["status"] == "running"

    clock.advance(seconds=30)
    cancelled = service.cancel(task["id"], "family-li", "家属路上取消礼仪引导", "cancel-key-0002")
    assert cancelled["status"] == "cancel_requested"
    assert cancelled["cancellation_actor"] == "family-li"
    assert cancelled["cancel_requested_at"]

    # 计费只到取消请求时刻：即使 90 秒后现场才回执，也按 30 秒结算。
    clock.advance(seconds=90)
    receipt = service.complete(task["id"], "worker-on-road", {"stopped": True}, {}, "receipt-0001")
    assert receipt["status"] == "cancelled"
    assert receipt["receipt"]["disposition"] == "cancelled"
    details = service.get_task(task["id"])
    assert details["billable_seconds"] == 30
    billed = {item["fee_type"]: item for item in details["charges"] if item["status"] == "billed"}
    assert billed["base"]["amount_cents"] == 50000
    assert billed["execution"]["amount_cents"] == 12000  # ceil(30/60)=1 个计费单位
    assert details["charged_cents"] == 62000


def test_late_complete_after_cancel_never_revives_service(client):
    service, clock = make_service(client)
    task = service.submit(payload("late-complete-001"))
    service.claim("worker-on-road", ["hearse-guide"], 600)
    clock.advance(seconds=10)
    service.cancel(task["id"], "family-li", "取消", "cancel-key-0003")
    clock.advance(seconds=20)
    first = service.complete(task["id"], "worker-on-road", {"ok": True}, {}, "receipt-0002")
    assert first["status"] == "cancelled"

    # 迟到的重复完成回执：状态保持 cancelled，费用不变。
    late = service.complete(task["id"], "worker-on-road", {"ok": True}, {}, "receipt-0002")
    assert late["status"] == "cancelled"
    assert late["receipt"]["disposition"] == "cancelled"
    details = service.get_task(task["id"])
    assert details["charged_cents"] == 62000
    assert [item["action"] for item in details["interventions"]].count("cancel_acknowledged") == 1

    # 另一张新的迟到完成回执同样不能复活。
    other = service.complete(task["id"], "worker-on-road", {"ok": False}, {}, "receipt-0003")
    assert other["status"] == "cancelled"
    assert other["receipt"]["disposition"] == "late_rejected"
    assert service.get_task(task["id"])["charged_cents"] == 62000


def test_late_fail_after_terminal_failure_is_rejected_and_stable(client):
    service, clock = make_service(client)
    task = service.submit(payload("late-fail-001"))
    service.claim("worker-a", ["hearse-guide"], 600)
    failed = service.fail(task["id"], "worker-a", "vehicle_breakdown", "车辆故障", False, "fail-key-0001")
    assert failed["status"] == "failed"

    duplicate = service.fail(task["id"], "worker-a", "vehicle_breakdown", "车辆故障", False, "fail-key-0001")
    assert duplicate["status"] == "failed"
    assert duplicate["receipt"]["disposition"] == "accepted"

    late = service.fail(task["id"], "worker-a", "late_error", "迟到的失败回执", False, "fail-key-0002")
    assert late["status"] == "failed"
    assert late["receipt"]["disposition"] == "late_rejected"
    details = service.get_task(task["id"])
    assert details["interventions"][-1]["action"] == "late_receipt"
    assert service.get_task(task["id"])["status"] == "failed"


def test_duplicate_cancel_returns_stable_result_without_overwriting_decision(client):
    service, clock = make_service(client)
    task = service.submit(payload("dup-cancel-001"))
    service.claim("worker-a", ["hearse-guide"], 600)
    clock.advance(seconds=5)
    first = service.cancel(task["id"], "family-li", "首次取消", "cancel-key-0004")
    assert first["status"] == "cancel_requested"
    assert first["cancellation"]["duplicate"] is False

    clock.advance(seconds=50)
    second = service.cancel(task["id"], "family-wang", "重复取消不应覆盖决定人", "cancel-key-0004")
    assert second["status"] == "cancel_requested"
    assert second["cancellation"]["duplicate"] is True
    assert second["cancellation_actor"] == "family-li"
    assert second["cancel_requested_at"] == first["cancel_requested_at"]

    # 无幂等键的重复取消同样稳定。
    third = service.cancel(task["id"], "family-wang", "再次重复取消")
    assert third["status"] == "cancel_requested"
    assert third["cancellation"]["duplicate"] is True


def test_failure_retry_receipt_key_is_stable_across_retries(client):
    service, clock = make_service(client)
    task = service.submit(payload("retry-receipt-001"))
    service.claim("worker-a", ["hearse-guide"], 600)
    clock.advance(seconds=5)
    first_fail = service.fail(task["id"], "worker-a", "transient", "临时故障", True, "fail-key-0010")
    assert first_fail["status"] == "queued"
    again = service.fail(task["id"], "worker-a", "transient", "临时故障", True, "fail-key-0010")
    assert again["status"] == "queued"
    assert again["receipt"]["disposition"] == "accepted"


def test_recover_expired_cancel_requested_converges_to_cancelled(client):
    service, clock = make_service(client)
    task = service.submit(payload("expired-cancel-001"))
    service.claim("worker-a", ["hearse-guide"], 10)
    clock.advance(seconds=5)
    service.cancel(task["id"], "family-li", "取消", "cancel-key-0005")
    clock.advance(seconds=20)
    result = service.recover_expired()
    assert task["id"] in result["cancelled"]
    details = service.get_task(task["id"])
    assert details["status"] == "cancelled"
    assert details["billable_seconds"] == 5
    assert details["charged_cents"] == 62000


def test_normal_completion_bills_full_duration_and_old_flow_compatible(client):
    service, clock = make_service(client)
    task = service.submit(payload("normal-complete-001"))
    service.claim("worker-a", ["hearse-guide"], 600)
    clock.advance(seconds=125)
    done = service.complete(task["id"], "worker-a", {"value": 1}, {})
    assert done["status"] == "succeeded"
    details = service.get_task(task["id"])
    assert details["current_result_version"] == 1
    assert details["billable_seconds"] == 125
    billed = {item["fee_type"]: item["amount_cents"] for item in details["charges"]}
    assert billed == {"base": 50000, "execution": 36000}  # ceil(125/60)=3
    assert details["charged_cents"] == 86000


def test_billing_accumulates_active_time_but_not_queue_wait(client):
    service, clock = make_service(client)
    task = service.submit(payload("retry-billing-001"))
    service.claim("worker-a", ["hearse-guide"], 600)
    clock.advance(seconds=40)
    # 第一轮失败并重排：累计 40 秒执行时长。
    requeued = service.fail(task["id"], "worker-a", "transient", "临时故障", True, "fail-key-0020")
    assert requeued["status"] == "queued"
    assert requeued["billable_seconds"] == 40
    # 排队等待 10 分钟不应计入执行时长。
    clock.advance(seconds=600)
    service.claim("worker-a", ["hearse-guide"], 600)
    clock.advance(seconds=30)
    done = service.complete(task["id"], "worker-a", {"ok": True}, {})
    details = service.get_task(task["id"])
    assert details["billable_seconds"] == 70
    assert details["charged_cents"] == 50000 + 12000 * 2  # ceil(70/60)=2 个计费单位


def test_project_billing_summary_reflects_only_settled_charges(client):
    service, clock = make_service(client)
    queued = service.submit(payload("billing-queued", user="family-a"))
    running = service.submit(payload("billing-running", user="family-b"))
    service.cancel(queued["id"], "family-li", "未开始取消", "cancel-key-0006")
    service.claim("worker-a", ["hearse-guide"], 600)
    # running 任务尚未结算，不应计入金额。
    billing = service.project_billing("funeral-2026")
    assert billing["summary"]["task_count"] == 2
    assert billing["summary"]["charged_cents"] == 0
    statuses = {item["id"]: item["status"] for item in billing["tasks"]}
    assert statuses[queued["id"]] == "cancelled"
    assert statuses[running["id"]] == "running"


def test_audit_events_record_who_decided_what_and_when(client):
    from app.database import close_connection

    close_connection()
    created = client.post(
        "/api/compute/templates?actor=pricing-admin",
        json=FEE_TEMPLATE,
    )
    assert created.status_code == 201, created.text
    task = client.post("/api/compute/tasks", json=payload("http-cancel-001")).json()
    cancel = client.post(
        f"/api/compute/tasks/{task['id']}/cancel",
        json={"actor": "clerk-zhang", "reason": "客服代为取消", "idempotency_key": "http-cancel-key-1"},
    )
    assert cancel.status_code == 200
    assert cancel.json()["status"] == "cancelled"

    # 重复取消返回稳定结果。
    repeat = client.post(
        f"/api/compute/tasks/{task['id']}/cancel",
        json={"actor": "clerk-zhang", "reason": "客服代为取消", "idempotency_key": "http-cancel-key-1"},
    )
    assert repeat.status_code == 200
    assert repeat.json()["cancellation"]["duplicate"] is True

    billing = client.get("/api/compute/projects/funeral-2026/billing")
    assert billing.status_code == 200
    assert billing.json()["summary"]["charged_cents"] == 0

    bootstrap = client.post("/api/auth/bootstrap", json={"username": "admin", "password": "Admin!23456", "client_label": "tests"})
    assert bootstrap.status_code == 201
    login = client.post("/api/auth/login", json={"username": "admin", "password": "Admin!23456", "client_label": "tests"})
    headers = {"Authorization": f"Bearer {login.json()['token']}"}
    audit = client.get("/api/audit?resource_type=compute_task&size=50", headers=headers)
    assert audit.status_code == 200, audit.text
    events = [item for item in audit.json()["data"] if item["resource_id"] == str(task["id"])]
    actions = [item["action"] for item in events]
    assert "compute.cancel" in actions
    decision = next(item for item in events if item["action"] == "compute.cancel" and item["actor_name"] == "clerk-zhang")
    assert decision["created_at"]
    assert json.loads(decision["after_json"])["status"] == "cancelled"
