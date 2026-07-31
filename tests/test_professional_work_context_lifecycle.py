from fastapi.testclient import TestClient

from app.api import routes
from app.assistant_runtime.self_governance_runtime import (
    get_active_work_context_lifecycle,
    transition_active_work_context,
)
from app.main import app


client = TestClient(app)

TRANSITION = {
    "cycle_id": "EP-001",
    "completed_work_id": "VECTRA-SELF-GOVERNANCE-EP-001-INCREMENT-002",
    "expected_current_focus": "Professional Pipeline integration",
    "completion_evidence_id": "VECTRA-CONTROL-POINT-2026-07-31-001",
    "completion_verdict": "PASS",
    "next_focus": "VECTRA Laboratory restoration and architectural self-research",
    "next_recommended_step": (
        "Restore VECTRA Laboratory from the confirmed control point and perform "
        "full architectural self-research before selecting the next priority."
    ),
}


def test_transition_requires_product_owner_confirmation(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = transition_active_work_context(
        **TRANSITION,
        product_owner_confirmed=False,
    )
    readback = get_active_work_context_lifecycle()

    assert result["status"] == "HOLD"
    assert result["failure_reason"] == "product_owner_confirmation_required"
    assert result["read_only"] is True
    assert readback["active_work_context"]["current_focus"] == "Professional Pipeline integration"
    assert readback["transition_count"] == 0


def test_transition_completes_increment_without_closing_parent_cycle(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = transition_active_work_context(
        **TRANSITION,
        product_owner_confirmed=True,
    )
    readback = get_active_work_context_lifecycle()

    assert result["status"] == "PASS"
    assert result["verification_status"] == "PASS"
    assert result["readback_status"] == "PASS"
    assert result["transition_status"] == "COMPLETED"
    assert result["transition_reused"] is False
    assert result["parent_cycle_completed"] is False
    assert result["new_architectural_priority_selected"] is False

    active = readback["active_work_context"]
    assert active["cycle_id"] == "EP-001"
    assert active["status"] == "ACTIVE"
    assert active["current_focus"] == TRANSITION["next_focus"]
    assert active["next_recommended_step"] == TRANSITION["next_recommended_step"]
    assert active["last_completed_work"] == {
        "work_id": TRANSITION["completed_work_id"],
        "focus": TRANSITION["expected_current_focus"],
        "status": "COMPLETED",
        "verification_status": "PASS",
        "evidence_id": TRANSITION["completion_evidence_id"],
        "completed_at": result["transition"]["transitioned_at"],
    }
    assert readback["last_transition"]["completion_evidence_id"] == TRANSITION["completion_evidence_id"]
    assert readback["transition_count"] == 1


def test_repeating_same_transition_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    first = transition_active_work_context(
        **TRANSITION,
        product_owner_confirmed=True,
    )
    second = transition_active_work_context(
        **TRANSITION,
        product_owner_confirmed=True,
    )
    readback = get_active_work_context_lifecycle()

    assert first["status"] == "PASS"
    assert second["status"] == "PASS"
    assert second["transition_reused"] is True
    assert second["transition"]["transition_id"] == first["transition"]["transition_id"]
    assert readback["transition_count"] == 1


def test_transition_rejects_stale_focus_and_open_blockers(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    stale = dict(TRANSITION)
    stale["expected_current_focus"] = "Already completed focus"
    stale_result = transition_active_work_context(
        **stale,
        product_owner_confirmed=True,
    )

    assert stale_result["status"] == "HOLD"
    assert stale_result["failure_reason"] == "active_focus_mismatch"

    from app.assistant_runtime.self_governance_runtime import record_observation

    record_observation(
        observation_type="BLOCKER",
        title="Blocking Product Verification defect",
        subsystem="professional_runtime",
        criticality="CRITICAL",
    )
    blocked_result = transition_active_work_context(
        **TRANSITION,
        product_owner_confirmed=True,
    )

    assert blocked_result["status"] == "HOLD"
    assert blocked_result["failure_reason"] == "open_engineering_blockers"
    assert blocked_result["open_blockers"] == 1


def test_memory_action_contract_publishes_context_lifecycle_without_new_action_slot():
    schema = routes._laboratory_facade_openapi_schema()
    memory_operation = schema["paths"]["/vectra/laboratory/facade/memory"]["post"]
    request_schema = memory_operation["requestBody"]["content"]["application/json"]["schema"]
    operations = request_schema["properties"]["operation_type"]["enum"]
    operation_count = sum(len(methods) for methods in schema["paths"].values())

    assert "transition_active_work_context" in operations
    assert "get_active_work_context" in operations
    assert request_schema["properties"]["payload"]["properties"]["completion_verdict"]["enum"] == ["PASS"]
    assert operation_count == 29
    assert schema["x-vectra-gpt-actions-operation-limit"]["status"] == "PASS"
    assert schema["servers"] == [{"url": "https://bon-buasson-api.onrender.com"}]


def test_memory_facade_routes_transition_and_readback(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    transition_response = client.post(
        "/vectra/laboratory/facade/memory",
        json={
            "operation_type": "transition_active_work_context",
            "product_owner_approval": True,
            "payload": TRANSITION,
        },
    )
    transition_body = transition_response.json()

    assert transition_response.status_code == 200
    assert transition_body["operation_type"] == "transition_active_work_context"
    assert transition_body["runtime_service_called"] == "self_governance_runtime.transition_active_work_context"
    assert transition_body["result"]["status"] == "PASS"
    assert transition_body["result"]["readback_status"] == "PASS"

    readback_response = client.post(
        "/vectra/laboratory/facade/memory",
        json={"operation_type": "get_active_work_context", "payload": {}},
    )
    readback_body = readback_response.json()

    assert readback_response.status_code == 200
    assert readback_body["operation_type"] == "get_active_work_context"
    assert readback_body["runtime_service_called"] == "self_governance_runtime.get_active_work_context_lifecycle"
    assert readback_body["result"]["status"] == "PASS"
    assert readback_body["result"]["active_work_context"]["current_focus"] == TRANSITION["next_focus"]
    assert readback_body["result"]["transition_count"] == 1
