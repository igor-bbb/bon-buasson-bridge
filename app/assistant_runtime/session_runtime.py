from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

from app.assistant_runtime.architecture_registry_runtime import initialize_architecture_registry_runtime
from app.assistant_runtime.verification_runtime import initialize_verification_runtime
from app.assistant_runtime.execution_runtime import initialize_execution_runtime
from app.assistant_runtime.execution_orchestrator_runtime import (
    initialize_execution_orchestrator,
    search_execution_plans,
)

RELEASE_ID = "VECTRA-SESSION-RUNTIME-001"
RESULTS_PATH = Path("runtime/session_runtime/runtime_sessions.json")
ACTIVE_STATUSES = {"ACTIVE", "RESTORED"}
CLOSED_STATUS = "CLOSED"


class SessionRuntimeError(RuntimeError):
    pass


class SessionRepository:
    def __init__(self, path: Path = RESULTS_PATH) -> None:
        self.path = path
        self._lock = RLock()
        self._data: dict[str, Any] | None = None

    def load(self, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            if self._data is not None and not force:
                return deepcopy(self._data)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if not self.path.exists():
                self._data = {
                    "repository_id": "VECTRA-SESSION-REPOSITORY-001",
                    "release_id": RELEASE_ID,
                    "schema_version": "1.0",
                    "sessions": [],
                    "active_session_id": None,
                    "updated_at": _now(),
                }
                self._persist()
            else:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(self._data.get("sessions"), list):
                    raise SessionRuntimeError("session_repository_invalid")
                self._repair_active_pointer()
            return deepcopy(self._data)

    def save(self, session: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            data = self._require_loaded()
            for index, current in enumerate(data["sessions"]):
                if current.get("session_id") == session.get("session_id"):
                    data["sessions"][index] = deepcopy(session)
                    break
            else:
                data["sessions"].append(deepcopy(session))
            data["active_session_id"] = (
                session["session_id"] if session.get("session_status") in ACTIVE_STATUSES else self._find_active_id(exclude=session.get("session_id"))
            )
            data["updated_at"] = _now()
            self._persist()
            return deepcopy(session)

    def get(self, session_id: str) -> dict[str, Any] | None:
        for session in self.sessions():
            if session.get("session_id") == session_id:
                return session
        return None

    def sessions(self) -> list[dict[str, Any]]:
        return deepcopy(self._require_loaded()["sessions"])

    def active(self) -> dict[str, Any] | None:
        data = self._require_loaded()
        active_id = data.get("active_session_id")
        if not active_id:
            return None
        return self.get(str(active_id))

    def state(self) -> dict[str, Any]:
        data = self._require_loaded()
        return {
            "repository_id": data["repository_id"],
            "sessions_count": len(data["sessions"]),
            "active_session_id": data.get("active_session_id"),
            "loaded": True,
        }

    def _require_loaded(self) -> dict[str, Any]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return self._data

    def _find_active_id(self, *, exclude: str | None = None) -> str | None:
        data = self._require_loaded()
        active = [
            item["session_id"]
            for item in data["sessions"]
            if item.get("session_id") != exclude and item.get("session_status") in ACTIVE_STATUSES
        ]
        return active[0] if active else None

    def _repair_active_pointer(self) -> None:
        assert self._data is not None
        active = [item for item in self._data["sessions"] if item.get("session_status") in ACTIVE_STATUSES]
        if len(active) > 1:
            raise SessionRuntimeError("multiple_active_runtime_sessions")
        self._data["active_session_id"] = active[0]["session_id"] if active else None

    def _persist(self) -> None:
        assert self._data is not None
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)


_REPOSITORY = SessionRepository()


def initialize_session_runtime(*, force: bool = False) -> dict[str, Any]:
    registry = initialize_architecture_registry_runtime(force=force)
    verification = initialize_verification_runtime(force=force)
    execution = initialize_execution_runtime(force=force)
    orchestrator = initialize_execution_orchestrator(force=force)
    repository = _REPOSITORY.load(force=force)
    return _pass(
        session_runtime_status="READY",
        runtime_component="Session Runtime",
        release_id=RELEASE_ID,
        loaded=True,
        load_order="AFTER_EXECUTION_ORCHESTRATOR_RUNTIME",
        architecture_registry_loaded=registry.get("loaded") is True,
        verification_runtime_loaded=verification.get("loaded") is True,
        execution_runtime_loaded=execution.get("loaded") is True,
        execution_orchestrator_loaded=orchestrator.get("loaded") is True,
        session_repository_id=repository["repository_id"],
        sessions_count=len(repository["sessions"]),
        active_session_id=repository.get("active_session_id"),
    )


def start_runtime_session(payload: dict[str, Any]) -> dict[str, Any]:
    active = _REPOSITORY.active()
    if active is not None:
        return _fail(
            "active_runtime_session_exists",
            "Only one professional Runtime session may be active",
            active_session_id=active["session_id"],
        )
    session_id = str(payload.get("session_id") or f"SESSION-{uuid4().hex[:12].upper()}")
    if _REPOSITORY.get(session_id) is not None:
        return _fail("runtime_session_already_exists", "session_id already exists", session_id=session_id)
    now = _now()
    state = _build_session_state(payload.get("session_state"))
    session = {
        "session_id": session_id,
        "session_status": "ACTIVE",
        "created_at": now,
        "restored_at": None,
        "closed_at": None,
        "session_state": state,
        "runtime_components": _runtime_component_statuses(),
        "active_execution_plan_id": state.get("active_execution_plan_id"),
        "lifecycle_log": [_event("SESSION_CREATED", "ACTIVE", payload.get("reason"))],
        "close_reason": None,
        "restore_count": 0,
        "restored_from_closed_state": False,
    }
    return _pass(**_REPOSITORY.save(session))


def restore_runtime_session(payload: dict[str, Any]) -> dict[str, Any]:
    active = _REPOSITORY.active()
    if active is not None:
        return _fail(
            "active_runtime_session_exists",
            "Close the active Runtime session before restoring another session",
            active_session_id=active["session_id"],
        )
    session = _get_session(payload)
    if session.get("status") == "FAIL":
        return session
    if session.get("session_status") != CLOSED_STATUS:
        return _fail("runtime_session_not_closed", "Only a closed Runtime session can be restored")
    session["session_status"] = "RESTORED"
    session["restored_at"] = _now()
    session["closed_at"] = None
    session["close_reason"] = None
    session["restore_count"] = int(session.get("restore_count") or 0) + 1
    session["restored_from_closed_state"] = True
    session["runtime_components"] = _runtime_component_statuses()
    session["lifecycle_log"].append(_event("SESSION_RESTORED", "RESTORED", payload.get("reason")))
    return _pass(**_REPOSITORY.save(session))


def get_runtime_session_status(payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id") or "").strip()
    session = _REPOSITORY.get(session_id) if session_id else _REPOSITORY.active()
    if session is None:
        return _pass(
            active_session=False,
            session_id=None,
            session_status="NO_ACTIVE_SESSION",
            runtime_components=_runtime_component_statuses(),
        )
    return _pass(active_session=session.get("session_status") in ACTIVE_STATUSES, **deepcopy(session))


def list_runtime_sessions(payload: dict[str, Any]) -> dict[str, Any]:
    sessions = _REPOSITORY.sessions()
    status_filter = str(payload.get("session_status") or "").strip().upper()
    if status_filter:
        sessions = [item for item in sessions if item.get("session_status") == status_filter]
    limit = _safe_limit(payload.get("limit"), default=100)
    sessions = sessions[-limit:]
    return _pass(
        sessions_count=len(sessions),
        active_session_id=_REPOSITORY.state().get("active_session_id"),
        sessions=sessions,
    )


def search_runtime_sessions(payload: dict[str, Any]) -> dict[str, Any]:
    criteria = {key: value for key, value in payload.items() if value not in (None, "", [])}
    results = [item for item in _REPOSITORY.sessions() if _matches_session(item, criteria)]
    return _pass(results_count=len(results), results=results, filters=criteria)


def close_runtime_session(payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id") or "").strip()
    session = _REPOSITORY.get(session_id) if session_id else _REPOSITORY.active()
    if session is None:
        return _fail("runtime_session_not_found", "Runtime session was not found")
    if session.get("session_status") not in ACTIVE_STATUSES:
        return _fail("runtime_session_not_active", "Only an active or restored Runtime session can be closed")
    final_state = payload.get("session_state")
    if isinstance(final_state, dict):
        merged = deepcopy(session.get("session_state") or {})
        merged.update(deepcopy(final_state))
        session["session_state"] = _build_session_state(merged)
    else:
        session["session_state"] = _build_session_state(session.get("session_state"))
    session["runtime_components"] = _runtime_component_statuses()
    session["active_execution_plan_id"] = session["session_state"].get("active_execution_plan_id")
    session["session_status"] = CLOSED_STATUS
    session["closed_at"] = _now()
    session["close_reason"] = str(payload.get("reason") or "normal_close")
    session["lifecycle_log"].append(_event("SESSION_CLOSED", CLOSED_STATUS, session["close_reason"]))
    return _pass(**_REPOSITORY.save(session))


def execute_session_runtime_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    operations = {
        "start_runtime_session": start_runtime_session,
        "restore_runtime_session": restore_runtime_session,
        "get_runtime_session_status": get_runtime_session_status,
        "list_runtime_sessions": list_runtime_sessions,
        "search_runtime_sessions": search_runtime_sessions,
        "close_runtime_session": close_runtime_session,
    }
    handler = operations.get(operation_type)
    if handler is None:
        return _fail("unsupported_session_runtime_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload)


def _build_session_state(provided: Any = None) -> dict[str, Any]:
    state = deepcopy(provided) if isinstance(provided, dict) else {}
    state.setdefault("workspace", None)
    state.setdefault("business_domain", None)
    state.setdefault("professional_role", None)
    state.setdefault("active_execution_plan_id", _find_active_execution_plan_id())
    state["captured_at"] = _now()
    return state


def _find_active_execution_plan_id() -> str | None:
    response = search_execution_plans({"orchestration_status": "RUNNING"})
    results = response.get("results") if isinstance(response, dict) else None
    if isinstance(results, list) and results:
        return results[-1].get("execution_plan_id")
    return None


def _runtime_component_statuses() -> dict[str, Any]:
    registry = initialize_architecture_registry_runtime()
    verification = initialize_verification_runtime()
    execution = initialize_execution_runtime()
    orchestrator = initialize_execution_orchestrator()
    return {
        "architecture_registry": registry.get("status"),
        "verification_runtime": verification.get("status"),
        "execution_runtime": execution.get("status"),
        "execution_orchestrator_runtime": orchestrator.get("status"),
    }


def _get_session(payload: dict[str, Any]) -> dict[str, Any]:
    session_id = str(payload.get("session_id") or "").strip()
    if not session_id:
        return _fail("session_id_required", "session_id is required")
    session = _REPOSITORY.get(session_id)
    return session if session is not None else _fail("runtime_session_not_found", "Runtime session was not found")


def _matches_session(session: dict[str, Any], criteria: dict[str, Any]) -> bool:
    for key, value in criteria.items():
        needle = str(value).casefold()
        if key in {"query", "text"}:
            haystack = json.dumps(session, ensure_ascii=False)
        else:
            haystack = str(session.get(key, ""))
        if needle not in haystack.casefold():
            return False
    return True


def _safe_limit(value: Any, *, default: int) -> int:
    try:
        return max(1, min(int(value), 500))
    except (TypeError, ValueError):
        return default


def _event(event: str, status: str, reason: Any = None) -> dict[str, Any]:
    return {"event": event, "status": status, "timestamp": _now(), "reason": reason}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pass(**payload: Any) -> dict[str, Any]:
    return {"status": "PASS", **payload, "error": None}


def _fail(code: str, message: str, **payload: Any) -> dict[str, Any]:
    return {"status": "FAIL", **payload, "error": {"code": code, "message": message}}
