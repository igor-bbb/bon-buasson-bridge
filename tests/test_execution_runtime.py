import json
from pathlib import Path

import pytest

from app.assistant_runtime import execution_runtime as er
from app.assistant_runtime import verification_runtime as vr


@pytest.fixture()
def isolated_repositories(tmp_path, monkeypatch):
    verification_repository = vr.VerificationRepository(tmp_path / "verification_results.json")
    execution_repository = er.ExecutionRepository(tmp_path / "execution_results.json")
    monkeypatch.setattr(vr, "_REPOSITORY", verification_repository)
    monkeypatch.setattr(er, "_REPOSITORY", execution_repository)
    monkeypatch.setattr(er, "get_latest_runtime_verification", vr.get_latest_runtime_verification)
    return verification_repository, execution_repository


def _verify(object_id="AOMM-POLICY-001"):
    result = vr.verify_runtime_object({"object_id": object_id})
    assert result["status"] == "PASS"
    return result


def test_execution_runtime_loads_after_verification_runtime(isolated_repositories):
    result = er.initialize_execution_runtime(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["load_order"] == "AFTER_VERIFICATION_RUNTIME"
    assert result["architecture_registry_loaded"] is True
    assert result["verification_runtime_loaded"] is True


def test_start_execution_gets_mapping_through_architecture_registry(isolated_repositories, monkeypatch):
    _verify()
    calls = []
    original = er.get_architecture_object

    def tracked(payload):
        calls.append(payload["object_id"])
        return original(payload)

    monkeypatch.setattr(er, "get_architecture_object", tracked)
    result = er.start_execution({"object_id": "AOMM-POLICY-001"})
    assert result["execution_status"] == "READY"
    assert calls == ["AOMM-POLICY-001"]
    assert result["runtime_source"] == "ArchitectureRegistryRuntime.get_architecture_object"
    assert result["execution_mapping"]["runtime_mapping"] == "Architecture Object Meta Model v1.0"
    assert result["execution_mapping"]["steps"][0]["path"] == "app/assistant_runtime/architecture_object_registry.py"


def test_execution_is_blocked_without_published_verification_pass(isolated_repositories):
    result = er.start_execution({"object_id": "AOMM-POLICY-001"})
    assert result["execution_status"] == "BLOCKED"
    assert result["verification_gate"]["status"] == "FAIL"
    assert result["error"]["code"] == "verification_pass_required"


def test_registered_execution_completes_deterministically(isolated_repositories):
    _verify()
    started = er.start_execution({"object_id": "AOMM-POLICY-001", "execution_id": "EXE-TEST"})
    result = er.run_execution({"execution_id": started["execution_id"]})
    assert result["execution_status"] == "COMPLETED"
    assert result["execution_result"]["status"] == "PASS"
    assert result["execution_result"]["steps_total"] == 1
    assert result["execution_result"]["steps_passed"] == 1
    assert result["execution_result"]["safe_termination"] is True


def test_run_execution_can_start_and_run_in_one_call(isolated_repositories):
    _verify()
    result = er.run_execution({"object_id": "AOMM-POLICY-001"})
    assert result["execution_status"] == "COMPLETED"
    assert result["execution_id"].startswith("EXE-")


def test_execution_status_is_published(isolated_repositories):
    _verify()
    result = er.run_execution({"object_id": "AOMM-POLICY-001"})
    status = er.get_execution_status({"execution_id": result["execution_id"]})
    assert status["status"] == "PASS"
    assert status["execution_status"] == "COMPLETED"
    assert status["verification_gate"]["verification_status"] == "PASS"


def test_execution_history_and_log_are_persisted(isolated_repositories):
    _verify()
    result = er.run_execution({"object_id": "AOMM-POLICY-001"})
    history = er.get_execution_history({"execution_id": result["execution_id"]})
    assert history["status"] == "PASS"
    assert [event["step_id"] for event in history["execution_history"]] == ["START", "RUN", "STEP-001", "FINISH"]
    assert history["execution_log"]
    saved = json.loads(isolated_repositories[1].path.read_text(encoding="utf-8"))
    assert saved["executions"][0]["execution_id"] == result["execution_id"]


def test_repeat_execution_creates_new_execution(isolated_repositories):
    _verify()
    first = er.run_execution({"object_id": "AOMM-POLICY-001"})
    second = er.run_execution({"execution_id": first["execution_id"], "repeat": True})
    assert second["execution_status"] == "COMPLETED"
    assert second["execution_id"] != first["execution_id"]
    assert second["repeat_of"] == first["execution_id"]
    assert isolated_repositories[1].state()["executions_count"] == 2


def test_execution_failure_stops_safely_and_registers_reason(isolated_repositories, monkeypatch):
    _verify()
    original = er.get_architecture_object

    def broken(payload):
        response = original(payload)
        response["object"]["implementation"]["paths"] = ["app/does_not_exist.py", "app/main.py"]
        return response

    monkeypatch.setattr(er, "get_architecture_object", broken)
    result = er.run_execution({"object_id": "AOMM-POLICY-001"})
    assert result["execution_status"] == "FAILED"
    assert result["execution_result"]["steps_executed"] == 1
    assert result["execution_result"]["safe_termination"] is True
    assert result["error"]["code"] == "execution_step_failed"


def test_search_execution_results(isolated_repositories):
    _verify()
    result = er.run_execution({"object_id": "AOMM-POLICY-001"})
    search = er.search_execution_results({"execution_id": result["execution_id"]})
    assert search["status"] == "PASS"
    assert search["results_count"] == 1
    assert search["results"][0]["execution_status"] == "COMPLETED"


def test_complete_execution_runtime_operation_contract(isolated_repositories):
    _verify()
    started = er.execute_execution_runtime_operation("start_execution", {"object_id": "AOMM-POLICY-001"})
    completed = er.execute_execution_runtime_operation("run_execution", {"execution_id": started["execution_id"]})
    operations = {
        "get_execution_runtime_status": {},
        "get_execution_status": {"execution_id": completed["execution_id"]},
        "get_execution_history": {"execution_id": completed["execution_id"]},
        "search_execution_results": {"object_id": "AOMM-POLICY-001"},
    }
    for operation_type, payload in operations.items():
        result = er.execute_execution_runtime_operation(operation_type, payload)
        assert result["status"] == "PASS", (operation_type, result)


def test_runtime_facade_exposes_execution_operations_without_new_action(isolated_repositories, monkeypatch):
    from app.api import routes

    enum = routes._memory_facade_operation_request_schema()["properties"]["operation_type"]["enum"]
    expected = {
        "get_execution_runtime_status",
        "start_execution",
        "run_execution",
        "get_execution_status",
        "get_execution_history",
        "search_execution_results",
    }
    assert expected.issubset(enum)
    assert routes._count_openapi_operations(routes._laboratory_full_openapi_schema()) == 29

    _verify()
    monkeypatch.setattr(routes, "_verify_laboratory_api_key", lambda *_: None)
    monkeypatch.setattr(routes, "execute_vectra_execution_runtime_operation", er.execute_execution_runtime_operation)
    response = routes.vectra_laboratory_facade_memory({
        "operation_type": "run_execution",
        "payload": {"object_id": "AOMM-POLICY-001"},
    })
    body = json.loads(response.body)
    assert body["result"]["execution_result"]["status"] == "PASS"
    assert body["result"]["execution_status"] == "COMPLETED"


def test_registry_verification_aot_and_aomm_are_not_modified(isolated_repositories):
    registry_path = Path("runtime/architecture_registry/architecture_registry.json")
    before = registry_path.read_bytes()
    _verify()
    er.run_execution({"object_id": "AOMM-POLICY-001"})
    assert registry_path.read_bytes() == before
    registry = json.loads(before)
    assert registry["aot_version"] == "1.0"
    assert registry["aomm_version"] == "1.0"
    assert registry["release_id"] == "VECTRA-ARCHITECTURE-REGISTRY-001"


def test_verification_runtime_source_is_not_modified_by_release():
    release_paths = {
        Path("app/assistant_runtime/execution_runtime.py"),
        Path("runtime/execution_runtime/execution_results.json"),
        Path("tests/test_execution_runtime.py"),
        Path("app/api/routes.py"),
        Path("app/main.py"),
    }
    assert Path("app/assistant_runtime/verification_runtime.py") not in release_paths
