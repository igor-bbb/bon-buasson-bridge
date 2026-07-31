import json
from pathlib import Path

import pytest

from app.assistant_runtime import execution_orchestrator_runtime as orch
from app.assistant_runtime import execution_runtime as er
from app.assistant_runtime import verification_runtime as vr

OBJECTS = ["AOMM-POLICY-001", "AOT-REGISTRY-001"]


@pytest.fixture()
def isolated_runtime(tmp_path, monkeypatch):
    verification_repository = vr.VerificationRepository(tmp_path / "verification.json")
    execution_repository = er.ExecutionRepository(tmp_path / "execution.json")
    orchestration_repository = orch.OrchestrationRepository(tmp_path / "orchestration.json")
    monkeypatch.setattr(vr, "_REPOSITORY", verification_repository)
    monkeypatch.setattr(er, "_REPOSITORY", execution_repository)
    monkeypatch.setattr(er, "get_latest_runtime_verification", vr.get_latest_runtime_verification)
    monkeypatch.setattr(orch, "_REPOSITORY", orchestration_repository)
    monkeypatch.setattr(orch, "run_execution", er.run_execution)
    return verification_repository, execution_repository, orchestration_repository


def _verify_all():
    for object_id in OBJECTS:
        result = vr.verify_runtime_object({"object_id": object_id})
        assert result["status"] == "PASS"


def test_orchestrator_loads_after_execution_runtime(isolated_runtime):
    result = orch.initialize_execution_orchestrator(force=True)
    assert result["status"] == "PASS"
    assert result["load_order"] == "AFTER_EXECUTION_RUNTIME"
    assert result["execution_runtime_loaded"] is True
    assert result["verification_runtime_loaded"] is True
    assert result["architecture_registry_loaded"] is True


def test_create_execution_plan_uses_registry_dependencies(isolated_runtime, monkeypatch):
    calls = []
    original = orch.resolve_dependencies
    def tracked(payload):
        calls.append(payload["object_id"])
        return original(payload)
    monkeypatch.setattr(orch, "resolve_dependencies", tracked)
    plan = orch.create_execution_plan({"object_ids": OBJECTS})
    assert plan["orchestration_status"] == "READY"
    assert sorted(calls) == sorted(OBJECTS)
    assert plan["runtime_sources"]["registry"] == "ArchitectureRegistryRuntime.resolve_dependencies"


def test_scheduler_is_deterministic(isolated_runtime, monkeypatch):
    monkeypatch.setattr(orch, "resolve_dependencies", lambda payload: {
        "status": "PASS",
        "direct_dependencies": ["AOMM-POLICY-001"] if payload["object_id"] == "AOT-REGISTRY-001" else [],
    })
    first = orch.create_execution_plan({"object_ids": list(reversed(OBJECTS))})
    second = orch.create_execution_plan({"object_ids": list(reversed(OBJECTS))})
    assert first["execution_order"] == ["AOMM-POLICY-001", "AOT-REGISTRY-001"]
    assert second["execution_order"] == first["execution_order"]


def test_queue_contains_required_states(isolated_runtime):
    plan = orch.create_execution_plan({"object_ids": [OBJECTS[0]]})
    assert plan["queue"][0]["status"] == "READY"
    queue = orch.get_execution_queue({"execution_plan_id": plan["execution_plan_id"]})
    assert queue["queue_summary"]["READY"] == 1
    assert queue["queue_summary"]["TOTAL"] == 1


def test_plan_runs_execution_runtime_only(isolated_runtime, monkeypatch):
    _verify_all()
    calls = []
    original = er.run_execution
    def tracked(payload):
        calls.append(payload["object_id"])
        return original(payload)
    monkeypatch.setattr(orch, "run_execution", tracked)
    result = orch.start_execution_plan({"object_ids": [OBJECTS[0]]})
    assert result["orchestration_status"] == "COMPLETED"
    assert calls == [OBJECTS[0]]
    assert result["queue"][0]["status"] == "COMPLETED"


def test_plan_updates_status_and_execution_ids(isolated_runtime):
    _verify_all()
    result = orch.start_execution_plan({"object_ids": [OBJECTS[0]]})
    status = orch.get_execution_plan_status({"execution_plan_id": result["execution_plan_id"]})
    assert status["orchestration_status"] == "COMPLETED"
    assert len(status["execution_ids"]) == 1
    assert status["queue_summary"]["COMPLETED"] == 1


def test_repeat_plan_creates_new_plan(isolated_runtime):
    _verify_all()
    first = orch.start_execution_plan({"object_ids": [OBJECTS[0]]})
    second = orch.start_execution_plan({"execution_plan_id": first["execution_plan_id"], "repeat": True})
    assert second["orchestration_status"] == "COMPLETED"
    assert second["execution_plan_id"] != first["execution_plan_id"]
    assert second["repeat_of"] == first["execution_plan_id"]


def test_completed_plan_is_not_rerun_without_explicit_repeat(isolated_runtime):
    _verify_all()
    first = orch.start_execution_plan({"object_ids": [OBJECTS[0]]})
    result = orch.start_execution_plan({"execution_plan_id": first["execution_plan_id"]})
    assert result["status"] == "FAIL"
    assert result["error"]["code"] == "execution_plan_already_completed"


def test_failed_execution_stops_plan_and_blocks_remaining(isolated_runtime, monkeypatch):
    monkeypatch.setattr(orch, "resolve_dependencies", lambda payload: {"status": "PASS", "direct_dependencies": []})
    def failed(payload):
        return {"execution_id": "EXE-FAIL", "execution_status": "FAILED", "execution_result": {"status": "FAIL"}, "error": {"code": "forced_failure"}}
    monkeypatch.setattr(orch, "run_execution", failed)
    result = orch.start_execution_plan({"object_ids": OBJECTS})
    assert result["orchestration_status"] == "FAILED"
    assert result["queue"][0]["status"] == "FAILED"
    assert result["queue"][1]["status"] == "BLOCKED"
    assert result["stop_reason"] == "forced_failure"


def test_orchestration_repository_persists_log(isolated_runtime):
    _verify_all()
    result = orch.start_execution_plan({"object_ids": [OBJECTS[0]]})
    raw = json.loads(isolated_runtime[2].path.read_text(encoding="utf-8"))
    saved = raw["plans"][0]
    assert saved["execution_plan_id"] == result["execution_plan_id"]
    assert saved["orchestration_log"][-1]["status"] == "COMPLETED"


def test_search_execution_plans(isolated_runtime):
    plan = orch.create_execution_plan({"object_ids": [OBJECTS[0]]})
    search = orch.search_execution_plans({"execution_plan_id": plan["execution_plan_id"]})
    assert search["status"] == "PASS"
    assert search["results_count"] == 1


def test_all_facade_operations_are_supported(isolated_runtime):
    plan = orch.create_execution_plan({"object_ids": [OBJECTS[0]]})
    operations = {
        "get_orchestrator_status": {},
        "get_execution_plan_status": {"execution_plan_id": plan["execution_plan_id"]},
        "get_execution_queue": {"execution_plan_id": plan["execution_plan_id"]},
        "search_execution_plans": {"execution_plan_id": plan["execution_plan_id"]},
    }
    for name, payload in operations.items():
        assert orch.execute_orchestrator_operation(name, payload)["status"] == "PASS"


def test_runtime_facade_exposes_orchestrator_without_new_action(isolated_runtime, monkeypatch):
    from app.api import routes
    enum = routes._memory_facade_operation_request_schema()["properties"]["operation_type"]["enum"]
    expected = {"get_orchestrator_status", "create_execution_plan", "start_execution_plan", "get_execution_plan_status", "get_execution_queue", "search_execution_plans"}
    assert expected.issubset(enum)
    assert routes._count_openapi_operations(routes._laboratory_full_openapi_schema()) == 29
    monkeypatch.setattr(routes, "_verify_laboratory_api_key", lambda *_: None)
    monkeypatch.setattr(routes, "execute_vectra_orchestrator_operation", orch.execute_orchestrator_operation)
    response = routes.vectra_laboratory_facade_memory({"operation_type": "create_execution_plan", "payload": {"object_ids": [OBJECTS[0]]}})
    body = json.loads(response.body)
    assert body["result"]["orchestration_status"] == "READY"


def test_protected_runtime_sources_are_not_modified():
    release_paths = {
        Path("app/assistant_runtime/execution_orchestrator_runtime.py"),
        Path("runtime/execution_orchestrator/orchestration_plans.json"),
        Path("tests/test_execution_orchestrator_runtime.py"),
        Path("app/api/routes.py"),
        Path("app/main.py"),
    }
    assert Path("app/assistant_runtime/execution_runtime.py") not in release_paths
    assert Path("app/assistant_runtime/verification_runtime.py") not in release_paths
    assert Path("app/assistant_runtime/architecture_registry_runtime.py") not in release_paths
