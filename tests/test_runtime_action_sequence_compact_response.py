import json

from app.api import routes
from app.assistant_runtime import runtime_action_sequence as sequence

SAFE_RESPONSE_LIMIT_BYTES = 16_000


def _configure(monkeypatch, tmp_path, *, fail_operation=None):
    monkeypatch.setattr(sequence, "STATE_PATH", tmp_path / "runtime_action_sequences.json")
    counters = {"capitalization": 0}

    def result(name, payload):
        if name == fail_operation:
            return {"status": "FAIL", "code": "CAPITALIZATION_FAILED", "message": "Controlled test failure"}
        return payload

    monkeypatch.setattr(sequence, "get_memory_overview", lambda **_: result("get_memory_overview", {"status": "ok", "verification_status": "PASS", "objects": [{"large": "x" * 5000}]}))
    monkeypatch.setattr(sequence, "list_memory_objects", lambda **_: result("list_memory_objects", {"status": "ok", "verification_status": "PASS", "objects": [{"content": "x" * 5000} for _ in range(5)]}))
    monkeypatch.setattr(sequence, "get_professional_knowledge", lambda *_: result("read_professional_knowledge", {"status": "ok", "verification_status": "PASS", "knowledge": [{"content": "x" * 5000} for _ in range(5)]}))

    def capitalize(payload):
        counters["capitalization"] += 1
        return result("capitalize_confirmed_knowledge", {
            "status": "PASS", "verification_status": "PASS",
            "capitalized_knowledge_ids": ["PK-COMPACT-001"],
            "created_object_ids": ["professional_memory:PK-COMPACT-001"],
            "full_payload": "x" * 10000,
        })

    monkeypatch.setattr(sequence, "find_memory_object_by_knowledge_id", lambda knowledge_id, **_: {"status": "ok", "readback_status": "PASS", "matches": [{"knowledge_id": knowledge_id, "object_id": f"professional_memory:{knowledge_id}"}]})
    monkeypatch.setattr(sequence, "get_memory_object", lambda object_id, **_: {"status": "ok", "verification_status": "PASS", "readback_status": "PASS", "object_id": object_id, "memory_object": {"object_id": object_id, "knowledge_id": object_id.split(":", 1)[-1], "content": "x" * 10000}})
    monkeypatch.setattr(sequence, "auto_capitalize_confirmed_knowledge", capitalize)
    monkeypatch.setattr(sequence, "readback_memory_object", lambda **_: result("verify_memory_object_readback", {
        "status": "ok", "verification_status": "PASS", "readback_status": "PASS",
        "memory_object": {"object_id": "professional_memory:PK-COMPACT-001", "knowledge_id": "PK-COMPACT-001", "content": "x" * 10000},
    }))
    monkeypatch.setattr(sequence, "verify_memory_repository_integrity", lambda **_: result("verify_memory_repository", {"status": "PASS", "verification_status": "PASS", "registry": "x" * 10000}))
    return counters


def _payload(mode=None):
    payload = {
        "sequence_id": "VECTRA-RESEARCH-CYCLE-CLOSURE-001",
        "program_type": "knowledge_capitalization",
        "domain": "bonboason",
        "product_owner_approval": True,
        "prepared_knowledge_package": {
            "professional_knowledge": [{"knowledge_id": "PK-COMPACT-001", "content": "confirmed"}],
            "business_knowledge": [],
        },
    }
    if mode is not None:
        payload["response_mode"] = mode
    return payload


def _size(value):
    return len(json.dumps(value, ensure_ascii=False, separators=(",", ":")).encode("utf-8"))


def test_complete_sequence_defaults_to_compact_and_is_under_safe_limit(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    result = sequence.execute_registered_action_sequence(_payload())
    assert result["response_mode"] == "compact"
    assert _size(result) < SAFE_RESPONSE_LIMIT_BYTES
    assert "steps" not in result
    assert "sequence" not in result
    assert result["steps_requested_count"] == 7
    assert result["steps_passed_count"] == 7
    assert result["readback_status"] == "PASS"
    assert result["memory_integrity_status"] == "PASS"


def test_compact_get_restores_bounded_state(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    sequence.execute_registered_action_sequence(_payload())
    result = sequence.get_registered_action_sequence({"sequence_id": _payload()["sequence_id"], "response_mode": "compact"})
    assert result["runtime_state_restored"] is True
    assert result["sequence"]["steps_completed_count"] == 7
    assert "steps_completed" not in result["sequence"]
    assert _size(result) < SAFE_RESPONSE_LIMIT_BYTES


def test_step_summary_has_only_bounded_per_step_results(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    sequence.execute_registered_action_sequence(_payload())
    result = sequence.get_registered_action_sequence({"sequence_id": _payload()["sequence_id"], "response_mode": "step_summary"})
    steps = result["sequence"]["steps"]
    assert len(steps) == 7
    assert all("runtime_result" not in item for item in steps)
    assert _size(result) < SAFE_RESPONSE_LIMIT_BYTES


def test_diagnostic_returns_full_saved_runtime_state(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    sequence.execute_registered_action_sequence(_payload())
    result = sequence.get_registered_action_sequence({"sequence_id": _payload()["sequence_id"], "response_mode": "diagnostic"})
    assert result["response_mode"] == "diagnostic"
    assert result["sequence"]["steps_completed"][1]["runtime_result"]["objects"]
    assert result["sequence"]["request_payload"]["prepared_knowledge_package"]


def test_failed_sequence_returns_compact_error(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path, fail_operation="capitalize_confirmed_knowledge")
    result = sequence.execute_registered_action_sequence(_payload())
    assert result["status"] == "FAIL"
    assert result["failed_step"] == {"step_number": 4, "operation_type": "capitalize_confirmed_knowledge"}
    assert result["error"]["code"] == "CAPITALIZATION_FAILED"
    assert "steps" not in result
    assert _size(result) < SAFE_RESPONSE_LIMIT_BYTES


def test_repeated_completed_sequence_id_does_not_duplicate_capitalization(monkeypatch, tmp_path):
    counters = _configure(monkeypatch, tmp_path)
    first = sequence.execute_registered_action_sequence(_payload())
    second = sequence.execute_registered_action_sequence(_payload())
    assert first["status"] == second["status"] == "PASS"
    assert second["sequence_reused"] is True
    assert counters["capitalization"] == 1


def test_openapi_publishes_response_modes_and_keeps_action_limit():
    schema = routes._laboratory_facade_openapi_schema()
    request_schema = schema["paths"]["/vectra/laboratory/facade/memory"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    response_mode = request_schema["properties"]["payload"]["properties"]["response_mode"]
    assert response_mode["default"] == "compact"
    assert set(response_mode["enum"]) == {"compact", "step_summary", "diagnostic"}
    assert routes._count_openapi_operations(schema) == 30
