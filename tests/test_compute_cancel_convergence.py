from __future__ import annotations

import json
from datetime import UTC, datetime

from app.compute.service import ComputeOperationsService
from app.core.clock import FrozenClock
from app.database import get_connection


TEMPLATE = {
    "code": "hearse-service",
    "name": "出殡车与礼仪引导",
    "algorithm": "hearse-service",
    "parameter_schema": {
        "distance_km": {"type": "number", "required": True, "minimum": 1, "maximum": 200},
    },
    "default_parameters": {},
    "max_runtime_seconds": 3600,
    "max_attempts": 1,
    "base_fee": 500,
    "unit_fee": 120,
    "billing_unit_seconds": 300,
}


def submit_payload(key: str, *, user: str = "family-1") -> dict:
    return {
        "template_code": "hearse-service",
        "project_code": "funeral-order-7",
        "requested_by": user,
        "parameters": {"distance_km": 12.5},
        "priority": 80,
        "idempotency_key": key,
    }


def create_template(client) -> None:
    response = client.post("/api/compute/templates?actor=service-admin", json=TEMPLATE)
    assert response.status_code == 201, response.text


def submit_and_claim(client, key: str, *, worker: str = "staff-on-road"):
    task = client.post("/api/compute/tasks", json=submit_payload(key)).json()
    claimed = client.post("/api/compute/tasks/claim", json={"worker_id": worker, "capabilities": ["hearse-service"], "lease_seconds": 600})
    assert claimed.status_code == 200 and claimed.json()["task"]["id"] == task["id"]
    return task


def clock_service(start: datetime | None = None) -> tuple[ComputeOperationsService, FrozenClock]:
    clock = FrozenClock(start or datetime(2026, 10, 3, 2, 0, tzinfo=UTC))
    service = ComputeOperationsService(get_connection(), clock)
    service.create_template(TEMPLATE, "service-admin")
    return service, clock


# ------------------------------------------------------------- 尚未开始：立即结束


def test_cancel_queued_service_ends_immediately_with_zero_fee(client):
    create_template(client)
    task = client.post("/api/compute/tasks", json=submit_payload("cancel-queued-1")).json()
    cancel = client.post(f"/api/compute/tasks/{task['id']}/cancel", json={"actor": "客服-小王", "reason": "家属临时取消出殡车"})
    assert cancel.status_code == 200
    body = cancel.json()
    assert body["status"] == "cancelled"
    assert body["billing"]["status"] == "settled"
    assert body["billing"]["amount"] == 0
    assert body["billing"]["billable_seconds"] == 0
    assert body["cancellation"]["requested_by"] == "客服-小王"
    assert body["cancellation"]["reason"] == "家属临时取消出殡车"
    billing = client.get(f"/api/compute/task-details/{task['id']}/billing").json()
    assert billing["status"] == "cancelled"
    assert billing["billing"]["amount"] == 0
    assert billing["billing"]["detail"]["rule"]


# ------------------------------------------------------------- 正在执行：记录取消，下次回执收敛


def test_running_cancel_is_recorded_and_settles_on_next_receipt(client):
    create_template(client)
    task = submit_and_claim(client, "cancel-running-1")

    first_cancel = client.post(f"/api/compute/tasks/{task['id']}/cancel", json={"actor": "客服-小王", "reason": "家属改期，停止礼仪引导"})
    assert first_cancel.status_code == 200
    assert first_cancel.json()["status"] == "cancel_requested"
    assert first_cancel.json()["cancellation"]["repeated"] is False

    # 心跳不再续租：现场被明确告知停止。
    heartbeat = client.post(
        f"/api/compute/tasks/{task['id']}/heartbeat",
        json={"worker_id": "staff-on-road", "capabilities": [], "lease_seconds": 600},
    )
    assert heartbeat.status_code == 409

    completion = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={"worker_id": "staff-on-road", "result": {"arrived": True}, "metrics": {"seconds": 420}},
    )
    assert completion.status_code == 200
    receipt = completion.json()
    assert receipt["status"] == "cancelled"
    assert receipt["receipt"]["outcome"] == "cancelled"
    assert receipt["receipt"]["accepted"] is True

    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    # 回执内容留痕但不作为有效结果版本。
    assert details["current_result_version"] is None
    assert details["results"][0]["accepted"] == 0
    actions = [item["action"] for item in details["interventions"]]
    assert actions == ["cancel", "cancel_settle"]


def test_running_cancel_billing_stops_at_receipt_time(client):
    service, clock = clock_service()
    task = service.submit(submit_payload("cancel-billing-1"))
    assert service.claim("staff-on-road", ["hearse-service"], 600)["id"] == task["id"]
    clock.advance(seconds=120)
    assert service.cancel(task["id"], "客服-小王", "家属取消")["status"] == "cancel_requested"
    # 取消后继续执行的时间仍然计费，直到工作人员回传失败回执。
    clock.advance(seconds=300)
    settled = service.fail(task["id"], "staff-on-road", "family_cancelled", "家属现场取消", False)
    assert settled["status"] == "cancelled"
    # 420 秒跨两个 300 秒计费单位：基础费 500 + 2 * 120。
    assert settled["billing"]["billable_seconds"] == 420
    assert settled["billing"]["amount"] == 500 + 2 * 120
    assert settled["billing"]["stopped_at"] == settled["cancellation"]["cancelled_at"]


# ------------------------------------------------------------- 迟到回执不能复活已取消服务


def test_late_receipts_after_cancel_can_not_revive_service(client):
    create_template(client)
    task = submit_and_claim(client, "late-receipt-1")
    client.post(f"/api/compute/tasks/{task['id']}/cancel", json={"actor": "客服-小王", "reason": "家属取消"})
    settle = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={"worker_id": "staff-on-road", "result": {"phase": "stop"}, "metrics": {}},
    )
    assert settle.json()["status"] == "cancelled"
    settled_amount = settle.json()["billing"]["amount"]

    late = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={"worker_id": "staff-on-road", "result": {"phase": "finished"}, "metrics": {"seconds": 1800}},
    )
    assert late.status_code == 200
    assert late.json()["status"] == "cancelled"
    assert late.json()["receipt"]["outcome"] == "ignored"
    assert late.json()["receipt"]["accepted"] is False
    assert late.json()["billing"]["amount"] == settled_amount

    late_fail = client.post(
        f"/api/compute/tasks/{task['id']}/fail",
        json={"worker_id": "staff-on-road", "error_code": "traffic", "message": "堵车", "retryable": True},
    )
    assert late_fail.status_code == 200
    assert late_fail.json()["status"] == "cancelled"
    assert late_fail.json()["receipt"]["outcome"] == "ignored"

    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert details["status"] == "cancelled"
    assert len(details["receipts"]) == 3
    assert [item["accepted"] for item in details["receipts"]] == [1, 0, 0]
    assert [item["action"] for item in details["interventions"]] == ["cancel", "cancel_settle", "receipt_ignored", "receipt_ignored"]


# ------------------------------------------------------------- 重复取消、重复回执的稳定结果


def test_duplicate_cancel_and_duplicate_receipt_are_stable(client):
    create_template(client)
    task = submit_and_claim(client, "duplicate-1")
    payload = {"actor": "客服-小王", "reason": "家属取消，请求键一致", "request_key": "cancel-key-0001"}
    first = client.post(f"/api/compute/tasks/{task['id']}/cancel", json=payload)
    second = client.post(f"/api/compute/tasks/{task['id']}/cancel", json=payload)
    assert first.status_code == second.status_code == 200
    assert first.json()["status"] == second.json()["status"] == "cancel_requested"
    assert second.json()["cancellation"]["repeated"] is True
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert [item["action"] for item in details["interventions"]] == ["cancel"]

    receipt_body = {"worker_id": "staff-on-road", "result": {"ok": True}, "metrics": {}}
    done_once = client.post(f"/api/compute/tasks/{task['id']}/complete", json=receipt_body)
    done_twice = client.post(f"/api/compute/tasks/{task['id']}/complete", json=receipt_body)
    assert done_once.status_code == done_twice.status_code == 200
    assert done_once.json()["receipt"]["outcome"] == "cancelled"
    assert done_twice.json()["receipt"]["outcome"] == "replayed"
    assert done_once.json()["billing"]["amount"] == done_twice.json()["billing"]["amount"]
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert len(details["receipts"]) == 1


def test_duplicate_cancel_without_request_key_is_also_stable(client):
    create_template(client)
    task = client.post("/api/compute/tasks", json=submit_payload("dup-cancel-queued")).json()
    payload = {"actor": "客服-小王", "reason": "家属取消"}
    first = client.post(f"/api/compute/tasks/{task['id']}/cancel", json=payload)
    second = client.post(f"/api/compute/tasks/{task['id']}/cancel", json=payload)
    assert first.json()["status"] == second.json()["status"] == "cancelled"
    assert second.json()["cancellation"]["repeated"] is True
    details = client.get(f"/api/compute/task-details/{task['id']}").json()
    assert len(details["interventions"]) == 1


# ------------------------------------------------------------- 矛盾回执


def test_contradictory_receipt_from_other_worker_is_conflict(client):
    create_template(client)
    task = submit_and_claim(client, "conflict-1")
    client.post(f"/api/compute/tasks/{task['id']}/cancel", json={"actor": "客服-小王", "reason": "家属取消"})
    other = client.post(
        f"/api/compute/tasks/{task['id']}/complete",
        json={"worker_id": "impostor", "result": {}, "metrics": {}},
    )
    assert other.status_code == 409


# ------------------------------------------------------------- 租约恢复收敛


def test_lease_recovery_settles_cancel_requested_at_lease_expiry(client):
    service, clock = clock_service()
    task = service.submit(submit_payload("recovery-cancel-1"))
    assert service.claim("staff-on-road", ["hearse-service"], 600)["id"] == task["id"]
    clock.advance(seconds=120)
    assert service.cancel(task["id"], "客服-小王", "家属取消")["status"] == "cancel_requested"
    clock.advance(seconds=601)
    result = service.recover_expired()
    assert result["cancelled"] == [task["id"]]
    details = service.get_task(task["id"])
    assert details["status"] == "cancelled"
    # 取消后不再心跳，租约在领取后 600 秒到期，计费止于租约到期。
    assert details["billing"]["billable_seconds"] == 600
    assert details["billing"]["amount"] == 500 + 2 * 120


# ------------------------------------------------------------- 旧的正常完成流程保持兼容


def test_normal_completion_flow_still_works_and_is_billed(client):
    service, clock = clock_service()
    task = service.submit(submit_payload("normal-complete-1"))
    assert service.claim("staff-on-road", ["hearse-service"], 600)["id"] == task["id"]
    clock.advance(seconds=305)
    done = service.complete(task["id"], "staff-on-road", {"value": 1}, {"seconds": 305})
    assert done["status"] == "succeeded"
    assert done["receipt"]["outcome"] == "applied"
    assert done["current_result_version"] == 1
    assert done["billing"]["billable_seconds"] == 305
    assert done["billing"]["amount"] == 500 + 2 * 120
    # 重复回放保持稳定。
    replay = service.complete(task["id"], "staff-on-road", {"value": 1}, {"seconds": 305})
    assert replay["status"] == "succeeded"
    assert replay["receipt"]["outcome"] == "replayed"


# ------------------------------------------------------------- 审计可还原谁在何时决定了什么


def test_audit_events_record_who_decided_what_and_when(client, admin):
    service, clock = clock_service()
    task = service.submit(submit_payload("audit-trail-1"))
    assert service.claim("staff-on-road", ["hearse-service"], 600)["id"] == task["id"]
    clock.advance(seconds=60)
    service.cancel(task["id"], "客服-小王", "家属电话取消")
    clock.advance(seconds=360)
    service.complete(task["id"], "staff-on-road", {"ok": True}, {})

    events = client.get("/api/audit?resource_type=compute_task&action=compute.cancel.requested&size=10", headers=admin["headers"])
    assert events.status_code == 200
    items = events.json()["data"]
    assert len(items) == 1
    event = items[0]
    assert event["actor_name"] == "客服-小王"
    assert str(event["resource_id"]) == str(task["id"])
    assert json.loads(event["before_json"])["status"] == "running"
    assert json.loads(event["after_json"])["status"] == "cancel_requested"
    assert event["created_at"]

    settled = client.get("/api/audit?resource_type=compute_task&action=compute.cancel.settled&size=10", headers=admin["headers"])
    assert settled.json()["total"] == 1
    settled_event = settled.json()["data"][0]
    assert settled_event["actor_name"] == "staff-on-road"
    metadata = json.loads(settled_event["metadata_json"])
    assert metadata["receipt"] == "complete"
    assert metadata["billing"]["billable_seconds"] == 420
    assert metadata["billing"]["amount"] == 500 + 2 * 120
