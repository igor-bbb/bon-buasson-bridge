import json

from app.api import routes
from app.assistant_runtime import runtime_action_sequence as sequence


def _body(response):
    return json.loads(response.body.decode("utf-8"))


def _configure(monkeypatch, tmp_path):
    monkeypatch.setattr(sequence, "STATE_PATH", tmp_path / "runtime_action_sequences.json")
    calls = []

    def mark(name, result):
        def handler(*args, **kwargs):
            calls.append(name)
            return result() if callable(result) else dict(result)
        return handler

    monkeypatch.setattr(sequence, "get_memory_overview", mark("get_memory_overview", {"status": "ok", "verification_status": "PASS"}))
    monkeypatch.setattr(sequence, "list_memory_objects", mark("list_memory_objects", {"status": "ok", "objects": [], "verification_status": "PASS"}))
    monkeypatch.setattr(sequence, "get_professional_knowledge", mark("read_professional_knowledge", {"status": "ok", "knowledge": [], "verification_status": "PASS"}))
    monkeypatch.setattr(sequence, "auto_capitalize_confirmed_knowledge", mark("capitalize_confirmed_knowledge", {"status": "PASS", "final_status": "PASS"}))
    monkeypatch.setattr(sequence, "get_memory_object", mark("read_memory_object", {"status": "ok", "readback_status": "PASS", "verification_status": "PASS"}))

    def readback(*args, **kwargs):
        calls.append("verify_memory_object_readback" if kwargs.get("object_id") else "read_memory_object")
        return {
            "status": "ok",
            "readback_status": "PASS",
            "verification_status": "PASS",
            "memory_object": {"object_id": "professional_memory:PK-SEQ-001", "knowledge_id": "PK-SEQ-001"},
        }

    monkeypatch.setattr(sequence, "readback_memory_object", readback)
    monkeypatch.setattr(sequence, "verify_memory_repository_integrity", mark("verify_memory_repository", {"status": "PASS", "verification_status": "PASS"}))
    return calls


def _payload(steps=None):
    payload = {
        "sequence_id": "RAS-TEST-001",
        "product_owner_approval": True,
        "prepared_knowledge_package": {
            "source": "product_verification",
            "confirmation_level": "confirmed_by_product_owner",
            "professional_knowledge": [{"knowledge_id": "PK-SEQ-001", "title": "Sequence test", "status": "CONFIRMED_BY_PRODUCT_OWNER"}],
            "business_knowledge": [],
        },
    }
    if steps is not None:
        payload["steps"] = steps
    return payload


def test_executes_two_registered_actions_in_one_runtime_sequence(monkeypatch, tmp_path):
    calls = _configure(monkeypatch, tmp_path)
    result = sequence.execute_registered_action_sequence(_payload(["get_memory_overview", "list_memory_objects"]))
    assert result["status"] == "PASS"
    assert result["steps_completed_count"] == 2
    assert result["runtime_initialization_count"] == 1
    assert result["runtime_reinitialized_between_steps"] is False
    assert calls == ["get_memory_overview", "list_memory_objects"]


def test_executes_five_registered_actions_with_context_preserved(monkeypatch, tmp_path):
    calls = _configure(monkeypatch, tmp_path)
    steps = ["get_memory_overview", "list_memory_objects", "read_professional_knowledge", "capitalize_confirmed_knowledge", "read_memory_object"]
    result = sequence.execute_registered_action_sequence(_payload(steps))
    assert result["status"] == "PASS"
    assert result["steps_completed_count"] == 5
    assert result["scenario_context_preserved"] is True
    assert calls == steps


def test_executes_complete_product_verification_and_capitalization_sequence(monkeypatch, tmp_path):
    calls = _configure(monkeypatch, tmp_path)
    result = sequence.execute_registered_action_sequence(_payload())
    assert result["status"] == "PASS"
    assert result["verification_status"] == "PASS"
    assert result["sequence_status"] == "COMPLETED"
    assert result["steps_completed_count"] == 7
    assert result["transport_action_calls_required"] == 1
    assert [step["operation_type"] for step in result["steps"]] == sequence.DEFAULT_MEMORY_CAPITALIZATION_SEQUENCE
    assert calls == sequence.DEFAULT_MEMORY_CAPITALIZATION_SEQUENCE


def test_sequence_state_survives_separate_read_request(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    created = sequence.execute_registered_action_sequence(_payload(["get_memory_overview", "list_memory_objects"]))
    restored = sequence.get_registered_action_sequence({"sequence_id": created["sequence_id"]})
    assert restored["status"] == "PASS"
    assert restored["runtime_state_restored"] is True
    assert restored["sequence"]["status"] == "COMPLETED"
    assert restored["sequence"]["runtime_initialization_count"] == 1


def test_unknown_sequence_operation_is_rejected(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    result = sequence.execute_registered_action_sequence(_payload(["get_memory_overview", "not_registered"]))
    assert result["status"] == "FAIL"
    assert result["reason"] == "unsupported_registered_operation"


def test_public_memory_facade_executes_sequence_with_one_action(monkeypatch):
    monkeypatch.setattr(routes, "execute_vectra_registered_action_sequence", lambda payload: {
        "status": "PASS", "verification_status": "PASS", "sequence_id": "RAS-PUBLIC-001", "next_action": "Review"
    })
    body = _body(routes.vectra_laboratory_facade_memory({"operation_type": "execute_registered_action_sequence", "payload": {"steps": ["get_memory_overview", "list_memory_objects"]}}))
    assert body["status"] == "ok"
    assert body["runtime_service_called"] == "runtime_action_sequence.execute_registered_action_sequence"
    assert body["result"]["sequence_id"] == "RAS-PUBLIC-001"


def test_openapi_publishes_sequence_without_increasing_action_count():
    schema = routes._laboratory_facade_openapi_schema()
    request_schema = schema["paths"]["/vectra/laboratory/facade/memory"]["post"]["requestBody"]["content"]["application/json"]["schema"]
    operations = set(request_schema["properties"]["operation_type"]["enum"])
    assert {"execute_registered_action_sequence", "get_registered_action_sequence"} <= operations
    assert routes._count_openapi_operations(schema) == 30
