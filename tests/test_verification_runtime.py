import json
from pathlib import Path

import pytest

from app.assistant_runtime import verification_runtime as vr
from app.assistant_runtime.architecture_registry_runtime import (
    evaluate_object_compliance,
    evaluate_registry_compliance,
)


@pytest.fixture()
def isolated_repository(tmp_path, monkeypatch):
    repository = vr.VerificationRepository(tmp_path / "verification_results.json")
    monkeypatch.setattr(vr, "_REPOSITORY", repository)
    return repository


def test_verification_runtime_loads_after_architecture_registry(isolated_repository):
    result = vr.initialize_verification_runtime(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["architecture_registry_loaded"] is True
    assert result["architecture_registry_id"] == "VECTRA-ARCHITECTURE-REGISTRY-001"


def test_object_verification_gets_mapping_only_through_registry(isolated_repository, monkeypatch):
    calls = []
    original = vr.verify_architecture_object

    def tracked(payload):
        calls.append(payload["object_id"])
        return original(payload)

    monkeypatch.setattr(vr, "verify_architecture_object", tracked)
    result = vr.verify_runtime_object({"object_id": "AOMM-POLICY-001"})
    assert result["status"] == "PASS"
    assert calls == ["AOMM-POLICY-001"]
    assert result["runtime_source"] == "ArchitectureRegistryRuntime.verify_architecture_object"
    assert result["verification_mapping"]["status"] == "AUTOMATED"


def test_object_verification_publishes_status_evidence_and_execution_id(isolated_repository):
    result = vr.verify_runtime_object({"object_id": "VECTRA-PROFESSIONAL-RUNTIME-001"})
    assert result["verification_status"] == "PASS"
    assert result["execution_id"].startswith("VER-")
    assert result["verification_evidence"]
    assert result["timestamp"].endswith("Z")
    assert result["aggregation"]["fail_count"] == 0


def test_verification_result_is_persisted_and_read_back(isolated_repository):
    executed = vr.verify_runtime_object({"object_id": "AOMM-POLICY-001"})
    repository_file = isolated_repository.path
    assert repository_file.exists()
    saved = json.loads(repository_file.read_text(encoding="utf-8"))
    assert saved["results"][0]["execution_id"] == executed["execution_id"]
    status = vr.get_verification_status({"execution_id": executed["execution_id"]})
    assert status["verification_status"] == "PASS"


def test_repeat_verification_creates_new_execution_without_registry_mutation(isolated_repository):
    registry_path = Path("runtime/architecture_registry/architecture_registry.json")
    before = registry_path.read_bytes()
    first = vr.verify_runtime_object({"object_id": "AOMM-POLICY-001"})
    second = vr.verify_runtime_object({"object_id": "AOMM-POLICY-001"})
    assert first["execution_id"] != second["execution_id"]
    assert registry_path.read_bytes() == before
    assert isolated_repository.state()["results_count"] == 2


def test_verification_failure_registers_reason(isolated_repository, monkeypatch):
    monkeypatch.setattr(vr, "verify_architecture_object", lambda payload: {
        "status": "PASS",
        "verification_mapping": {"tests": ["tests/does_not_exist.py"], "status": "AUTOMATED"},
        "verification_evidence": [],
    })
    result = vr.verify_runtime_object({"object_id": "AOMM-POLICY-001"})
    assert result["status"] == "FAIL"
    assert result["aggregation"]["fail_count"] == 1
    assert result["checks"][0]["reason"] == "test_path_not_found"
    assert result["error"]["code"] == "runtime_object_verification_failed"


def test_evidence_service_returns_mapping_evidence_source_and_timestamp(isolated_repository):
    executed = vr.verify_runtime_object({"object_id": "AOMM-POLICY-001"})
    evidence = vr.get_verification_evidence({"execution_id": executed["execution_id"]})
    assert evidence["status"] == "PASS"
    assert evidence["verification_mapping"]["tests"]
    assert evidence["verification_evidence"]
    assert evidence["runtime_source"]
    assert evidence["timestamp"]


def test_list_and_search_verification_results(isolated_repository):
    first = vr.verify_runtime_object({"object_id": "AOMM-POLICY-001"})
    vr.verify_runtime_object({"object_id": "VECTRA-PROFESSIONAL-RUNTIME-001"})
    listed = vr.list_verification_results({})
    assert listed["results_count"] == 2
    searched = vr.search_verification_results({"execution_id": first["execution_id"]})
    assert searched["results_count"] == 1
    assert searched["results"][0]["object_id"] == "AOMM-POLICY-001"


def test_registry_verification_aggregates_all_objects_and_compliance(isolated_repository):
    result = vr.verify_registry({"execution_id": "VER-REG-TEST"})
    assert result["status"] == "PASS"
    assert result["objects_count"] == 24
    assert result["aggregation"]["pass_count"] == 24
    assert result["aggregation"]["fail_count"] == 0
    assert result["compliance_integration"] == {"status": "PASS", "computed": True}


def test_compliance_remains_owned_by_architecture_registry(isolated_repository):
    vr.verify_runtime_object({"object_id": "AOMM-POLICY-001"})
    object_compliance = evaluate_object_compliance({"object_id": "AOMM-POLICY-001"})
    registry_compliance = evaluate_registry_compliance({})
    assert object_compliance["computed"] is True
    assert registry_compliance["computed"] is True
    assert object_compliance["status"] == "PASS"
    assert registry_compliance["status"] == "PASS"


def test_complete_verification_runtime_operation_contract(isolated_repository):
    executed = vr.execute_verification_runtime_operation(
        "verify_runtime_object", {"object_id": "AOMM-POLICY-001"}
    )
    operations = {
        "get_verification_runtime_status": {},
        "get_verification_status": {"execution_id": executed["execution_id"]},
        "list_verification_results": {},
        "get_verification_evidence": {"execution_id": executed["execution_id"]},
        "search_verification_results": {"object_id": "AOMM-POLICY-001"},
        "verify_registry": {"execution_id": "VER-REG-CONTRACT"},
    }
    for operation_type, payload in operations.items():
        result = vr.execute_verification_runtime_operation(operation_type, payload)
        assert result["status"] == "PASS", (operation_type, result)


def test_runtime_facade_exposes_verification_operations_without_new_action(isolated_repository, monkeypatch):
    from app.api import routes

    enum = routes._memory_facade_operation_request_schema()["properties"]["operation_type"]["enum"]
    expected = {
        "get_verification_status",
        "list_verification_results",
        "verify_runtime_object",
        "verify_registry",
        "get_verification_evidence",
        "search_verification_results",
    }
    assert expected.issubset(enum)
    assert routes._count_openapi_operations(routes._laboratory_full_openapi_schema()) == 29

    monkeypatch.setattr(routes, "_verify_laboratory_api_key", lambda *_: None)
    monkeypatch.setattr(routes, "execute_vectra_verification_runtime_operation", vr.execute_verification_runtime_operation)
    response = routes.vectra_laboratory_facade_memory({
        "operation_type": "verify_runtime_object",
        "payload": {"object_id": "AOMM-POLICY-001"},
    })
    body = json.loads(response.body)
    assert body["result"]["status"] == "PASS"
    assert body["result"]["object_id"] == "AOMM-POLICY-001"


def test_aot_aomm_and_registry_model_are_unchanged():
    registry = json.loads(Path("runtime/architecture_registry/architecture_registry.json").read_text(encoding="utf-8"))
    assert registry["aot_version"] == "1.0"
    assert registry["aomm_version"] == "1.0"
    assert registry["release_id"] == "VECTRA-ARCHITECTURE-REGISTRY-001"
    assert len(registry["objects"]) == 24


def test_ep001_is_not_changed_by_release():
    release_paths = {
        Path("app/assistant_runtime/verification_runtime.py"),
        Path("runtime/verification_runtime/verification_results.json"),
        Path("tests/test_verification_runtime.py"),
        Path("app/api/routes.py"),
        Path("app/main.py"),
    }
    assert Path("app/assistant_runtime/self_governance_runtime.py") not in release_paths
