import json
from copy import deepcopy
from pathlib import Path

import pytest

from app.assistant_runtime import runtime_supervisor as rs


@pytest.fixture()
def isolated_supervisor(tmp_path, monkeypatch):
    repository = rs.RuntimeEventRepository(tmp_path / "events.json")
    monkeypatch.setattr(rs, "_REPOSITORY", repository)
    return repository


def healthy_states():
    return [
        {
            "component": component,
            "loaded": True,
            "operational_status": "READY" if component != "Architecture Registry Runtime" else "PASS",
            "required_function_available": True,
            "dependencies_available": True,
            "blocking_error": None,
            "degradations": [],
            "status_source": f"test.{component}",
            "evidence": {"ok": True},
        }
        for component in rs.REQUIRED_COMPONENTS
    ]


def state(states, component):
    return next(item for item in states if item["component"] == component)


def evaluate(states):
    return rs.evaluate_runtime_state(states)


def test_supervisor_loads_after_session_runtime(isolated_supervisor):
    result = rs.initialize_runtime_supervisor(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["load_order"] == "AFTER_SESSION_RUNTIME"
    assert result["runtime_component"] == "Runtime Supervisor"


def test_collects_all_required_published_component_statuses(isolated_supervisor):
    states = rs.collect_component_states()
    assert [item["component"] for item in states] == sorted(rs.REQUIRED_COMPONENTS)
    assert all(item["status_source"] for item in states)


def test_normalization_is_deterministic():
    states = list(reversed(healthy_states()))
    normalized = rs._normalize_state_collection(states)
    assert [item["component"] for item in normalized] == sorted(rs.REQUIRED_COMPONENTS)


def test_ready_when_all_required_components_are_available():
    result = evaluate(healthy_states())
    assert result["runtime_health"] == "HEALTHY"
    assert result["runtime_readiness"] == "READY"
    assert result["blocking_conditions"] == []
    assert result["degraded_conditions"] == []


def test_optional_diagnostic_function_unavailable_is_degraded():
    states = healthy_states()
    state(states, "Runtime Supervisor")["degradations"] = [{
        "reason_code": "diagnostics_partially_unavailable",
        "description": "Extended diagnostics are unavailable",
        "affected_functions": ["extended_diagnostics"],
    }]
    result = evaluate(states)
    assert result["runtime_readiness"] == "DEGRADED"
    assert result["blocking_conditions"] == []
    assert result["degraded_conditions"]


def test_historical_event_search_unavailable_is_degraded():
    states = healthy_states()
    state(states, "Runtime Supervisor")["degradations"] = [{
        "reason_code": "historical_event_search_unavailable",
        "description": "Historical event search unavailable",
        "affected_functions": ["search_runtime_events"],
    }]
    assert evaluate(states)["runtime_readiness"] == "DEGRADED"


def test_registry_unavailable_is_blocked():
    states = healthy_states()
    target = state(states, "Architecture Registry Runtime")
    target["loaded"] = False
    target["operational_status"] = "UNAVAILABLE"
    result = evaluate(states)
    assert result["runtime_readiness"] == "BLOCKED"
    assert result["runtime_health"] == "UNHEALTHY"


def test_registry_missing_mandatory_object_is_blocked():
    states = healthy_states()
    state(states, "Architecture Registry Runtime")["required_function_available"] = False
    result = evaluate(states)
    assert result["runtime_readiness"] == "BLOCKED"
    assert any(item["reason_code"] == "required_function_unavailable" for item in result["blocking_conditions"])


def test_verification_blocking_error_is_blocked():
    states = healthy_states()
    state(states, "Verification Runtime")["blocking_error"] = {
        "code": "verification_failed",
        "message": "Mandatory verification failed",
        "affected_function": "verify_runtime_object",
    }
    assert evaluate(states)["runtime_readiness"] == "BLOCKED"


@pytest.mark.parametrize("component", [
    "Execution Runtime",
    "Execution Orchestrator Runtime",
    "Session Runtime",
])
def test_mandatory_runtime_component_unavailable_is_blocked(component):
    states = healthy_states()
    target = state(states, component)
    target["loaded"] = False
    target["operational_status"] = "UNAVAILABLE"
    assert evaluate(states)["runtime_readiness"] == "BLOCKED"


def test_supervisor_cannot_get_required_status_is_blocked():
    states = [item for item in healthy_states() if item["component"] != "Session Runtime"]
    result = evaluate(states)
    assert result["runtime_readiness"] == "BLOCKED"
    assert any(item["reason_code"] == "required_status_missing" for item in result["blocking_conditions"])


def test_unknown_status_is_blocked():
    states = healthy_states()
    state(states, "Execution Runtime")["operational_status"] = "MYSTERY"
    result = evaluate(states)
    assert result["runtime_readiness"] == "BLOCKED"
    assert any(item["reason_code"] == "required_status_unknown" for item in result["blocking_conditions"])


def test_missing_status_is_blocked():
    states = healthy_states()
    state(states, "Session Runtime")["operational_status"] = "MISSING"
    assert evaluate(states)["runtime_readiness"] == "BLOCKED"


def test_blocked_has_priority_over_degraded():
    states = healthy_states()
    state(states, "Runtime Supervisor")["degradations"] = [{"reason_code": "search_limited", "description": "Search limited", "affected_functions": ["search"]}]
    state(states, "Execution Runtime")["loaded"] = False
    result = evaluate(states)
    assert result["runtime_readiness"] == "BLOCKED"
    assert result["blocking_conditions"]
    assert result["degraded_conditions"]


def test_repeated_evaluation_has_identical_deterministic_result():
    states = healthy_states()
    first = evaluate(deepcopy(states))
    second = evaluate(deepcopy(states))
    assert first == second


def test_input_order_does_not_change_result():
    first = evaluate(healthy_states())
    second = evaluate(list(reversed(healthy_states())))
    assert first == second


def test_ready_lists_are_empty():
    result = evaluate(healthy_states())
    assert result["blocking_conditions"] == []
    assert result["degraded_conditions"] == []


def test_degraded_has_no_blocking_and_nonempty_degradation():
    states = healthy_states()
    state(states, "Runtime Supervisor")["degradations"] = [{"reason_code": "events_write_warning", "description": "Optional event write warning", "affected_functions": ["diagnostic_events"]}]
    result = evaluate(states)
    assert result["runtime_readiness"] == "DEGRADED"
    assert result["blocking_conditions"] == []
    assert len(result["degraded_conditions"]) == 1


def test_blocked_contains_at_least_one_blocking_condition():
    states = healthy_states()
    state(states, "Execution Runtime")["dependencies_available"] = False
    result = evaluate(states)
    assert result["runtime_readiness"] == "BLOCKED"
    assert len(result["blocking_conditions"]) >= 1


def test_blocking_error_is_never_downgraded():
    states = healthy_states()
    target = state(states, "Verification Runtime")
    target["operational_status"] = "DEGRADED"
    target["blocking_error"] = {"code": "mandatory_verification_unavailable", "message": "Verification blocked"}
    result = evaluate(states)
    assert result["runtime_readiness"] == "BLOCKED"


def test_every_result_has_reason_and_evidence():
    result = evaluate(healthy_states())
    assert result["readiness_reason"]
    assert len(result["evidence"]) == len(rs.REQUIRED_COMPONENTS)


def test_events_are_registered_atomically_and_searchable(isolated_supervisor):
    result = rs.evaluate_runtime_supervisor(healthy_states(), persist=True)
    events = rs.get_runtime_events({})
    assert result["runtime_readiness"] == "READY"
    assert events["events_count"] >= 1
    searched = rs.search_runtime_events({"event_type": "SUPERVISOR_EVALUATION_COMPLETED"})
    assert searched["results_count"] == 1
    raw = json.loads(isolated_supervisor.path.read_text(encoding="utf-8"))
    assert raw["last_evaluation"]["supervisor_evaluation_id"] == result["supervisor_evaluation_id"]


def test_diagnostics_are_read_only_and_publish_recommended_actions(isolated_supervisor, monkeypatch):
    states = healthy_states()
    state(states, "Execution Runtime")["loaded"] = False
    monkeypatch.setattr(rs, "collect_component_states", lambda: states)
    result = rs.get_runtime_diagnostics({})
    assert result["read_only"] is True
    assert result["runtime_readiness"] == "BLOCKED"
    assert result["recommended_actions"]


def test_all_supervisor_facade_operations_are_supported(isolated_supervisor, monkeypatch):
    from app.api import routes
    enum = routes._memory_facade_operation_request_schema()["properties"]["operation_type"]["enum"]
    expected = {
        "get_runtime_supervisor_status", "get_runtime_health", "get_runtime_readiness",
        "get_runtime_events", "search_runtime_events", "get_runtime_diagnostics",
    }
    assert expected.issubset(enum)
    assert routes._count_openapi_operations(routes._laboratory_full_openapi_schema()) == 30
    monkeypatch.setattr(routes, "_verify_laboratory_api_key", lambda *_: None)
    monkeypatch.setattr(routes, "execute_vectra_runtime_supervisor_operation", rs.execute_runtime_supervisor_operation)
    response = routes.vectra_laboratory_facade_memory({"operation_type": "get_runtime_readiness", "payload": {}})
    body = json.loads(response.body)
    assert body["result"]["runtime_readiness"] in {"READY", "DEGRADED", "BLOCKED"}


def test_protected_runtime_sources_are_not_release_files():
    release_paths = {
        Path("app/assistant_runtime/runtime_supervisor.py"),
        Path("runtime/runtime_supervisor/runtime_events.json"),
        Path("tests/test_runtime_supervisor.py"),
        Path("app/api/routes.py"),
        Path("app/main.py"),
    }
    protected = {
        Path("app/assistant_runtime/architecture_registry_runtime.py"),
        Path("app/assistant_runtime/verification_runtime.py"),
        Path("app/assistant_runtime/execution_runtime.py"),
        Path("app/assistant_runtime/execution_orchestrator_runtime.py"),
        Path("app/assistant_runtime/session_runtime.py"),
        Path("runtime/architecture_registry/architecture_registry.json"),
    }
    assert release_paths.isdisjoint(protected)
