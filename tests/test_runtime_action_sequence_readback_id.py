import json

from app.api import routes
from app.assistant_runtime import runtime_action_sequence as sequence

SAFE_RESPONSE_LIMIT_BYTES = 16_000


def _payload(sequence_id="VECTRA-RESEARCH-CYCLE-CLOSURE-001", mode="compact"):
    return {
        "sequence_id": sequence_id,
        "program_type": "knowledge_capitalization",
        "domain": "bonboason",
        "product_owner_approval": True,
        "response_mode": mode,
        "prepared_knowledge_package": {
            "source": "confirmed_research",
            "professional_knowledge": [
                {"knowledge_id": "PK-NEW-001", "title": "First"},
                {"knowledge_id": "PK-NEW-002", "title": "Second"},
            ],
            "business_knowledge": [],
        },
    }


def _configure(monkeypatch, tmp_path):
    monkeypatch.setattr(sequence, "STATE_PATH", tmp_path / "runtime_action_sequences.json")
    calls = {"capitalize": 0, "get": [], "verify": []}
    mapping_enabled = {"value": True}

    monkeypatch.setattr(sequence, "get_memory_overview", lambda **_: {"status": "ok", "verification_status": "PASS"})
    monkeypatch.setattr(sequence, "list_memory_objects", lambda **_: {"status": "ok", "verification_status": "PASS", "objects": []})
    monkeypatch.setattr(sequence, "get_professional_knowledge", lambda *_: {"status": "ok", "verification_status": "PASS", "knowledge": []})

    def capitalize(_):
        calls["capitalize"] += 1
        return {
            "status": "ok",
            "verification_status": "PASS",
            "final_status": "CAPITALIZED",
            "created_knowledge_ids": ["PK-NEW-001", "PK-NEW-002"],
            "capitalized_knowledge_ids": ["PK-NEW-001", "PK-NEW-002"],
            "capitalization_reports": [{"knowledge_id": "PK-NEW-001"}, {"knowledge_id": "PK-NEW-002"}],
        }

    monkeypatch.setattr(sequence, "auto_capitalize_confirmed_knowledge", capitalize)

    def find(knowledge_id, **_):
        if not mapping_enabled["value"]:
            return {"status": "not_found", "readback_status": "FAIL", "matches": []}
        return {
            "status": "ok", "readback_status": "PASS",
            "matches": [{"knowledge_id": knowledge_id, "object_id": f"professional_memory:{knowledge_id}"}],
        }

    monkeypatch.setattr(sequence, "find_memory_object_by_knowledge_id", find)

    def get_object(object_id, **_):
        calls["get"].append(object_id)
        return {
            "status": "ok", "verification_status": "PASS", "readback_status": "PASS",
            "object_id": object_id,
            "memory_object": {"object_id": object_id, "knowledge_id": object_id.split(":", 1)[-1]},
        }

    monkeypatch.setattr(sequence, "get_memory_object", get_object)

    def verify(object_id=None, knowledge_id=None, **_):
        calls["verify"].append((object_id, knowledge_id))
        return {
            "status": "ok", "verification_status": "PASS", "readback_status": "PASS",
            "object_id": object_id,
            "memory_object": {"object_id": object_id, "knowledge_id": knowledge_id},
        }

    monkeypatch.setattr(sequence, "readback_memory_object", verify)
    monkeypatch.setattr(sequence, "verify_memory_repository_integrity", lambda **_: {"status": "PASS", "verification_status": "PASS"})
    return calls, mapping_enabled


def test_created_knowledge_id_is_resolved_and_handed_to_readback(monkeypatch, tmp_path):
    calls, _ = _configure(monkeypatch, tmp_path)
    result = sequence.execute_registered_action_sequence(_payload())
    assert result["status"] == "PASS"
    assert result["sequence_status"] == "COMPLETED"
    assert result["selected_readback_knowledge_id"] == "PK-NEW-001"
    assert result["selected_readback_object_id"] == "professional_memory:PK-NEW-001"
    assert result["capitalized_knowledge_ids"] == ["PK-NEW-001", "PK-NEW-002"]
    assert calls["get"] == ["professional_memory:PK-NEW-001"]
    assert calls["verify"] == [("professional_memory:PK-NEW-001", "PK-NEW-001")]


def test_selection_is_deterministic_and_full_mapping_is_saved(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    sequence.execute_registered_action_sequence(_payload(mode="diagnostic"))
    state = sequence.get_registered_action_sequence({"sequence_id": _payload()["sequence_id"], "response_mode": "diagnostic"})
    context = state["sequence"]["context"]
    assert context["created_knowledge_ids"] == ["PK-NEW-001", "PK-NEW-002"]
    assert context["selected_readback_knowledge_id"] == "PK-NEW-001"
    assert context["readback_target_selection_reason"] == "first_created"
    assert context["knowledge_object_mapping"]["PK-NEW-001"] == "professional_memory:PK-NEW-001"


def test_failed_sequence_resumes_from_read_step_without_recapitalization(monkeypatch, tmp_path):
    calls, mapping_enabled = _configure(monkeypatch, tmp_path)
    mapping_enabled["value"] = False
    first = sequence.execute_registered_action_sequence(_payload())
    assert first["status"] == "FAIL"
    assert first["failed_step"] == {"step_number": 5, "operation_type": "read_memory_object"}
    assert calls["capitalize"] == 1

    mapping_enabled["value"] = True
    resumed = sequence.execute_registered_action_sequence(_payload())
    assert resumed["status"] == "PASS"
    assert resumed["sequence_status"] == "COMPLETED"
    assert resumed["sequence_resumed"] is True
    assert resumed["capitalization_reexecuted"] is False
    assert calls["capitalize"] == 1
    assert resumed["runtime_initialization_count"] == 1


def test_missing_identifier_fails_at_capitalization_and_never_reads(monkeypatch, tmp_path):
    calls, _ = _configure(monkeypatch, tmp_path)
    payload = _payload("RAS-NO-ID")
    payload["prepared_knowledge_package"] = {"professional_knowledge": [], "business_knowledge": []}
    monkeypatch.setattr(sequence, "auto_capitalize_confirmed_knowledge", lambda _: {
        "status": "ok", "verification_status": "PASS", "final_status": "CAPITALIZED"
    })
    result = sequence.execute_registered_action_sequence(payload)
    assert result["status"] == "FAIL"
    assert result["failed_step"] == {"step_number": 4, "operation_type": "capitalize_confirmed_knowledge"}
    assert result["error"]["code"] == "capitalization_completed_without_readback_identifier"
    assert calls["get"] == []
    assert calls["verify"] == []


def test_step_summary_exposes_target_without_object_content(monkeypatch, tmp_path):
    _configure(monkeypatch, tmp_path)
    sequence.execute_registered_action_sequence(_payload())
    result = sequence.get_registered_action_sequence({"sequence_id": _payload()["sequence_id"], "response_mode": "step_summary"})
    read_step = next(step for step in result["sequence"]["steps"] if step["operation_type"] == "read_memory_object")
    assert read_step["result_summary"]["knowledge_id"] == "PK-NEW-001"
    assert read_step["result_summary"]["object_id"] == "professional_memory:PK-NEW-001"
    assert "memory_object" not in read_step["result_summary"]
    assert len(json.dumps(result, ensure_ascii=False).encode("utf-8")) < SAFE_RESPONSE_LIMIT_BYTES


def test_openapi_action_count_and_ep001_surface_are_unchanged():
    schema = routes._laboratory_facade_openapi_schema()
    assert routes._count_openapi_operations(schema) == 30
    assert schema["info"]["version"] == "VECTRA-RUNTIME-ACTION-SEQUENCE-READBACK-ID-001"
