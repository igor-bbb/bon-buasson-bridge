import json
from pathlib import Path

import pytest

from app.assistant_runtime import session_runtime as sr


@pytest.fixture()
def isolated_session_runtime(tmp_path, monkeypatch):
    repository = sr.SessionRepository(tmp_path / "sessions.json")
    monkeypatch.setattr(sr, "_REPOSITORY", repository)
    monkeypatch.setattr(sr, "_find_active_execution_plan_id", lambda: None)
    return repository


def test_session_runtime_loads_after_orchestrator(isolated_session_runtime):
    result = sr.initialize_session_runtime(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["load_order"] == "AFTER_EXECUTION_ORCHESTRATOR_RUNTIME"
    assert result["execution_orchestrator_loaded"] is True


def test_start_runtime_session_creates_identifier_and_state(isolated_session_runtime):
    result = sr.start_runtime_session({"session_state": {"workspace": "laboratory"}})
    assert result["status"] == "PASS"
    assert result["session_id"].startswith("SESSION-")
    assert result["session_status"] == "ACTIVE"
    assert result["session_state"]["workspace"] == "laboratory"
    assert result["created_at"]


def test_only_one_active_session_is_allowed(isolated_session_runtime):
    first = sr.start_runtime_session({})
    second = sr.start_runtime_session({})
    assert first["status"] == "PASS"
    assert second["status"] == "FAIL"
    assert second["error"]["code"] == "active_runtime_session_exists"
    assert second["active_session_id"] == first["session_id"]


def test_get_runtime_session_status_returns_active_session(isolated_session_runtime):
    created = sr.start_runtime_session({})
    status = sr.get_runtime_session_status({})
    assert status["status"] == "PASS"
    assert status["active_session"] is True
    assert status["session_id"] == created["session_id"]


def test_close_runtime_session_saves_state_and_reason(isolated_session_runtime):
    created = sr.start_runtime_session({"session_state": {"workspace": "laboratory"}})
    closed = sr.close_runtime_session({
        "session_id": created["session_id"],
        "reason": "work_complete",
        "session_state": {"business_domain": "Бон Буассон"},
    })
    assert closed["status"] == "PASS"
    assert closed["session_status"] == "CLOSED"
    assert closed["close_reason"] == "work_complete"
    assert closed["session_state"]["workspace"] == "laboratory"
    assert closed["session_state"]["business_domain"] == "Бон Буассон"
    assert closed["closed_at"]


def test_restore_runtime_session_restores_saved_state(isolated_session_runtime):
    created = sr.start_runtime_session({"session_state": {"professional_role": "Digital Business Analyst"}})
    sr.close_runtime_session({"session_id": created["session_id"], "reason": "pause"})
    restored = sr.restore_runtime_session({"session_id": created["session_id"], "reason": "continue"})
    assert restored["status"] == "PASS"
    assert restored["session_status"] == "RESTORED"
    assert restored["session_state"]["professional_role"] == "Digital Business Analyst"
    assert restored["restore_count"] == 1
    assert restored["restored_at"]


def test_restore_is_blocked_when_another_session_is_active(isolated_session_runtime):
    first = sr.start_runtime_session({})
    sr.close_runtime_session({"session_id": first["session_id"]})
    sr.start_runtime_session({})
    result = sr.restore_runtime_session({"session_id": first["session_id"]})
    assert result["status"] == "FAIL"
    assert result["error"]["code"] == "active_runtime_session_exists"


def test_repository_persists_lifecycle_log(isolated_session_runtime):
    created = sr.start_runtime_session({})
    sr.close_runtime_session({"session_id": created["session_id"], "reason": "done"})
    raw = json.loads(isolated_session_runtime.path.read_text(encoding="utf-8"))
    saved = raw["sessions"][0]
    assert [entry["event"] for entry in saved["lifecycle_log"]] == ["SESSION_CREATED", "SESSION_CLOSED"]
    assert raw["active_session_id"] is None


def test_list_and_search_runtime_sessions(isolated_session_runtime):
    created = sr.start_runtime_session({"session_state": {"workspace": "laboratory"}})
    sr.close_runtime_session({"session_id": created["session_id"]})
    listed = sr.list_runtime_sessions({"session_status": "CLOSED"})
    searched = sr.search_runtime_sessions({"query": "laboratory"})
    assert listed["sessions_count"] == 1
    assert searched["results_count"] == 1


def test_session_records_runtime_component_statuses(isolated_session_runtime):
    created = sr.start_runtime_session({})
    assert created["runtime_components"] == {
        "architecture_registry": "PASS",
        "verification_runtime": "PASS",
        "execution_runtime": "PASS",
        "execution_orchestrator_runtime": "PASS",
    }


def test_all_session_facade_operations_are_supported(isolated_session_runtime):
    created = sr.execute_session_runtime_operation("start_runtime_session", {})
    assert created["status"] == "PASS"
    session_id = created["session_id"]
    for name, payload in {
        "get_runtime_session_status": {"session_id": session_id},
        "list_runtime_sessions": {},
        "search_runtime_sessions": {"session_id": session_id},
        "close_runtime_session": {"session_id": session_id},
        "restore_runtime_session": {"session_id": session_id},
    }.items():
        assert sr.execute_session_runtime_operation(name, payload)["status"] == "PASS"


def test_runtime_facade_exposes_sessions_without_new_action(isolated_session_runtime, monkeypatch):
    from app.api import routes
    enum = routes._memory_facade_operation_request_schema()["properties"]["operation_type"]["enum"]
    expected = {
        "start_runtime_session",
        "restore_runtime_session",
        "get_runtime_session_status",
        "list_runtime_sessions",
        "search_runtime_sessions",
        "close_runtime_session",
    }
    assert expected.issubset(enum)
    assert routes._count_openapi_operations(routes._laboratory_full_openapi_schema()) == 29
    monkeypatch.setattr(routes, "_verify_laboratory_api_key", lambda *_: None)
    monkeypatch.setattr(routes, "execute_vectra_session_runtime_operation", sr.execute_session_runtime_operation)
    response = routes.vectra_laboratory_facade_memory({"operation_type": "start_runtime_session", "payload": {}})
    body = json.loads(response.body)
    assert body["result"]["session_status"] == "ACTIVE"


def test_protected_runtime_sources_are_not_modified():
    release_paths = {
        Path("app/assistant_runtime/session_runtime.py"),
        Path("runtime/session_runtime/runtime_sessions.json"),
        Path("tests/test_session_runtime.py"),
        Path("app/api/routes.py"),
        Path("app/main.py"),
    }
    assert Path("app/assistant_runtime/execution_orchestrator_runtime.py") not in release_paths
    assert Path("app/assistant_runtime/execution_runtime.py") not in release_paths
    assert Path("app/assistant_runtime/verification_runtime.py") not in release_paths
    assert Path("app/assistant_runtime/architecture_registry_runtime.py") not in release_paths
