import json
from copy import deepcopy
from pathlib import Path

import pytest

from app.assistant_runtime import runtime_recovery as rr


@pytest.fixture()
def isolated_recovery(tmp_path, monkeypatch):
    repository = rr.RecoveryHistoryRepository(tmp_path / "history.json")
    monkeypatch.setattr(rr, "_REPOSITORY", repository)
    return repository


def blocked_readiness():
    return {
        "status": "PASS",
        "runtime_readiness": "BLOCKED",
        "runtime_health": "UNHEALTHY",
        "supervisor_evaluation_id": "SUP-BEFORE",
    }


def healthy_readiness():
    return {
        "status": "PASS",
        "runtime_readiness": "READY",
        "runtime_health": "HEALTHY",
        "supervisor_evaluation_id": "SUP-READY",
    }


def registered_procedure(payload):
    return {
        "status": "PASS",
        "procedure_set_id": "PROFESSIONAL-PROCEDURES-VECTRA-LABORATORY",
        "procedure_set_version": "1.2",
        "active_procedure": {
            "procedure_id": "handle_confirmed_blocker",
            "version": "1.2",
            "status": "ACTIVE",
            "lifecycle_status": "ACTIVE",
            "steps": ["confirm_blocker", "localize_cause", "prepare_minimal_solution"],
        },
    }


def registered_service(payload):
    return {
        "status": "PASS",
        "object": {
            "object_id": "VECTRA-SERVICE-RECOVERY-001",
            "implementation": {"paths": ["app/assistant_runtime/recovery.py"]},
        },
    }


def after_evaluation():
    return {
        "status": "PASS",
        "runtime_readiness": "READY",
        "runtime_health": "HEALTHY",
        "supervisor_evaluation_id": "SUP-AFTER",
        "evaluated_at": "2026-07-25T12:00:00Z",
    }


def configure_registered_sources(monkeypatch):
    monkeypatch.setattr(rr, "resolve_professional_procedure", registered_procedure)
    monkeypatch.setattr(rr, "get_architecture_object", registered_service)


def test_runtime_recovery_loads_after_supervisor(isolated_recovery, monkeypatch):
    monkeypatch.setattr(rr, "get_runtime_readiness", lambda payload: healthy_readiness())
    result = rr.initialize_runtime_recovery(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["load_order"] == "AFTER_RUNTIME_SUPERVISOR"
    assert result["supervisor_available"] is True


def test_registered_recovery_procedure_is_resolved(isolated_recovery, monkeypatch):
    configure_registered_sources(monkeypatch)
    result = rr.resolve_recovery_plan({"procedure_id": "handle_confirmed_blocker", "target_component": "Execution Runtime"})
    assert result["status"] == "PASS"
    plan = result["recovery_plan"]
    assert plan["procedure_source"]["procedure_id"] == "handle_confirmed_blocker"
    assert plan["procedure_source"]["architecture_object_id"] == "VECTRA-SERVICE-RECOVERY-001"
    assert plan["evidence"]["independent_recovery_catalog"] is False


def test_unregistered_recovery_procedure_is_rejected(isolated_recovery):
    result = rr.resolve_recovery_plan({"procedure_id": "invented_recovery"})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "recovery_procedure_not_registered"


def test_recovery_plan_is_deterministic(isolated_recovery, monkeypatch):
    configure_registered_sources(monkeypatch)
    payload = {"procedure_id": "handle_confirmed_blocker", "target_component": "Session Runtime"}
    first = rr.resolve_recovery_plan(deepcopy(payload))["recovery_plan"]
    second = rr.resolve_recovery_plan(deepcopy(payload))["recovery_plan"]
    assert first == second


def test_inactive_registered_procedure_is_rejected(isolated_recovery, monkeypatch):
    def inactive(payload):
        result = registered_procedure(payload)
        result["active_procedure"]["status"] = "INACTIVE"
        return result
    monkeypatch.setattr(rr, "resolve_professional_procedure", inactive)
    monkeypatch.setattr(rr, "get_architecture_object", registered_service)
    result = rr.resolve_recovery_plan({})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "registered_recovery_procedure_inactive"


def test_invalid_registry_runtime_mapping_is_rejected(isolated_recovery, monkeypatch):
    configure_registered_sources(monkeypatch)
    monkeypatch.setattr(rr, "get_architecture_object", lambda payload: {"status": "PASS", "object": {"object_id": rr.REGISTERED_RECOVERY_OBJECT_ID, "implementation": {"paths": ["other.py"]}}})
    result = rr.resolve_recovery_plan({})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "registered_recovery_runtime_mapping_invalid"


def test_recovery_cannot_start_when_runtime_is_ready(isolated_recovery, monkeypatch):
    monkeypatch.setattr(rr, "get_runtime_readiness", lambda payload: healthy_readiness())
    monkeypatch.setattr(rr, "get_runtime_health", lambda payload: healthy_readiness())
    result = rr.start_runtime_recovery({})
    assert result["status"] == "FAIL"
    assert result["failure_reason"] == "runtime_recovery_not_allowed"


def test_allowed_recovery_executes_and_saves_history(isolated_recovery, monkeypatch):
    configure_registered_sources(monkeypatch)
    monkeypatch.setattr(rr, "get_runtime_readiness", lambda payload: blocked_readiness())
    monkeypatch.setattr(rr, "get_runtime_health", lambda payload: blocked_readiness())
    monkeypatch.setattr(rr, "run_recovery_evolution", lambda payload: {"status": "ok", "checkpoint": {"checkpoint_id": "CP-1"}})
    monkeypatch.setattr(rr, "evaluate_runtime_supervisor", after_evaluation)
    result = rr.start_runtime_recovery({"target_component": "Execution Runtime", "reason": "confirmed failure"})
    assert result["status"] == "PASS"
    assert result["supervisor_reevaluated"] is True
    execution = result["recovery_execution"]
    assert execution["result"] == "COMPLETED"
    assert execution["supervisor_evaluation_before"]["runtime_readiness"] == "BLOCKED"
    assert execution["supervisor_evaluation_after"]["runtime_readiness"] == "READY"
    history = rr.get_runtime_recovery_history({})
    assert history["executions_count"] == 1


def test_recovery_history_is_atomically_persisted(isolated_recovery, monkeypatch):
    configure_registered_sources(monkeypatch)
    monkeypatch.setattr(rr, "get_runtime_readiness", lambda payload: blocked_readiness())
    monkeypatch.setattr(rr, "get_runtime_health", lambda payload: blocked_readiness())
    monkeypatch.setattr(rr, "run_recovery_evolution", lambda payload: {"status": "ok"})
    monkeypatch.setattr(rr, "evaluate_runtime_supervisor", after_evaluation)
    rr.start_runtime_recovery({"target_component": "Runtime"})
    raw = json.loads(isolated_recovery.path.read_text(encoding="utf-8"))
    assert len(raw["executions"]) == 1
    assert not isolated_recovery.path.with_suffix(".json.tmp").exists()


def test_search_runtime_recovery(isolated_recovery, monkeypatch):
    configure_registered_sources(monkeypatch)
    monkeypatch.setattr(rr, "get_runtime_readiness", lambda payload: blocked_readiness())
    monkeypatch.setattr(rr, "get_runtime_health", lambda payload: blocked_readiness())
    monkeypatch.setattr(rr, "run_recovery_evolution", lambda payload: {"status": "ok"})
    monkeypatch.setattr(rr, "evaluate_runtime_supervisor", after_evaluation)
    rr.start_runtime_recovery({"target_component": "Session Runtime"})
    result = rr.search_runtime_recovery({"target_component": "Session Runtime", "result": "COMPLETED"})
    assert result["results_count"] == 1


def test_all_recovery_facade_operations_are_supported(isolated_recovery, monkeypatch):
    from app.api import routes
    enum = routes._memory_facade_operation_request_schema()["properties"]["operation_type"]["enum"]
    expected = {
        "get_runtime_recovery_status", "start_runtime_recovery", "get_runtime_recovery_history",
        "search_runtime_recovery", "get_runtime_recovery_plan",
    }
    assert expected.issubset(enum)
    assert routes._count_openapi_operations(routes._laboratory_full_openapi_schema()) == 30
    configure_registered_sources(monkeypatch)
    monkeypatch.setattr(routes, "_verify_laboratory_api_key", lambda *_: None)
    monkeypatch.setattr(routes, "execute_vectra_runtime_recovery_operation", rr.execute_runtime_recovery_operation)
    response = routes.vectra_laboratory_facade_memory({"operation_type": "get_runtime_recovery_plan", "payload": {}})
    body = json.loads(response.body)
    assert body["result"]["status"] == "PASS"


def test_protected_runtime_components_are_not_release_files():
    release_paths = {
        Path("app/assistant_runtime/runtime_recovery.py"),
        Path("runtime/runtime_recovery/recovery_history.json"),
        Path("tests/test_runtime_recovery.py"),
        Path("app/api/routes.py"),
        Path("app/main.py"),
    }
    protected = {
        Path("app/assistant_runtime/runtime_supervisor.py"),
        Path("app/assistant_runtime/session_runtime.py"),
        Path("app/assistant_runtime/execution_orchestrator_runtime.py"),
        Path("app/assistant_runtime/execution_runtime.py"),
        Path("app/assistant_runtime/verification_runtime.py"),
        Path("app/assistant_runtime/architecture_registry_runtime.py"),
        Path("runtime/architecture_registry/architecture_registry.json"),
    }
    assert release_paths.isdisjoint(protected)
