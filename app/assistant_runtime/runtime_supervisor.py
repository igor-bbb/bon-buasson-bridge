from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Callable
from uuid import uuid4

from app.assistant_runtime.architecture_registry_runtime import (
    get_architecture_registry_runtime_status,
    list_architecture_objects,
)
from app.assistant_runtime.verification_runtime import initialize_verification_runtime
from app.assistant_runtime.execution_runtime import initialize_execution_runtime
from app.assistant_runtime.execution_orchestrator_runtime import initialize_execution_orchestrator
from app.assistant_runtime.session_runtime import initialize_session_runtime

RELEASE_ID = "VECTRA-RUNTIME-SUPERVISOR-001"
RESULTS_PATH = Path("runtime/runtime_supervisor/runtime_events.json")
REQUIRED_COMPONENTS = (
    "Architecture Registry Runtime",
    "Verification Runtime",
    "Execution Runtime",
    "Execution Orchestrator Runtime",
    "Session Runtime",
    "Runtime Supervisor",
)
FAILURE_STATUSES = {"BLOCKED", "FAILED", "FAIL", "UNAVAILABLE", "UNHEALTHY", "ERROR"}
SUCCESS_STATUSES = {"PASS", "READY", "HEALTHY", "ACTIVE", "RESTORED", "NO_ACTIVE_SESSION", "COMPLETED"}
DEGRADED_STATUSES = {"DEGRADED", "WARNING", "WARN", "PARTIAL"}


class RuntimeSupervisorError(RuntimeError):
    pass


class RuntimeEventRepository:
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
                    "repository_id": "VECTRA-RUNTIME-EVENT-REPOSITORY-001",
                    "release_id": RELEASE_ID,
                    "schema_version": "1.0",
                    "events": [],
                    "last_evaluation": None,
                    "updated_at": _now(),
                }
                self._persist()
            else:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(self._data.get("events"), list):
                    raise RuntimeSupervisorError("runtime_event_repository_invalid")
            return deepcopy(self._data)

    def append_many(self, events: list[dict[str, Any]], evaluation: dict[str, Any]) -> None:
        with self._lock:
            data = self._require_loaded()
            data["events"].extend(deepcopy(events))
            data["last_evaluation"] = deepcopy(evaluation)
            data["updated_at"] = _now()
            self._persist()

    def events(self) -> list[dict[str, Any]]:
        return deepcopy(self._require_loaded()["events"])

    def last_evaluation(self) -> dict[str, Any] | None:
        return deepcopy(self._require_loaded().get("last_evaluation"))

    def state(self) -> dict[str, Any]:
        data = self._require_loaded()
        return {
            "repository_id": data["repository_id"],
            "loaded": True,
            "events_count": len(data["events"]),
            "last_evaluation_id": (data.get("last_evaluation") or {}).get("supervisor_evaluation_id"),
        }

    def _require_loaded(self) -> dict[str, Any]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return self._data

    def _persist(self) -> None:
        assert self._data is not None
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, self.path)


_REPOSITORY = RuntimeEventRepository()


def _component_providers() -> dict[str, Callable[[], dict[str, Any]]]:
    return {
        "Architecture Registry Runtime": get_architecture_registry_runtime_status,
        "Verification Runtime": initialize_verification_runtime,
        "Execution Runtime": initialize_execution_runtime,
        "Execution Orchestrator Runtime": initialize_execution_orchestrator,
        "Session Runtime": initialize_session_runtime,
    }


def collect_component_states() -> list[dict[str, Any]]:
    states: list[dict[str, Any]] = []
    for component in sorted(_component_providers()):
        provider = _component_providers()[component]
        try:
            raw = provider()
            states.append(_normalize_component_state(component, raw, f"{provider.__module__}.{provider.__name__}"))
        except Exception as exc:  # status collection must publish a deterministic BLOCKED result
            states.append(_unavailable_component(component, "component_status_unavailable", str(exc)))
    states.append({
        "component": "Runtime Supervisor",
        "loaded": True,
        "operational_status": "READY",
        "required_function_available": True,
        "dependencies_available": True,
        "blocking_error": None,
        "degradations": [],
        "last_successful_evaluation": _now(),
        "status_source": "runtime_supervisor.self_status",
        "evidence": {"release_id": RELEASE_ID, "load_order": "AFTER_SESSION_RUNTIME"},
    })
    return sorted(states, key=lambda item: item["component"])


def initialize_runtime_supervisor(*, force: bool = False) -> dict[str, Any]:
    _REPOSITORY.load(force=force)
    evaluation = evaluate_runtime_supervisor()
    session_state = next((item for item in evaluation["component_states"] if item.get("component") == "Session Runtime"), {})
    connected = session_state.get("loaded") is True and session_state.get("dependencies_available") is True
    return _pass(
        runtime_component="Runtime Supervisor",
        release_id=RELEASE_ID,
        loaded=True,
        load_order="AFTER_SESSION_RUNTIME",
        supervisor_status="READY" if evaluation["status"] == "PASS" else "BLOCKED",
        connection_status="CONNECTED" if connected else "DISCONNECTED",
        session_runtime_loaded=session_state.get("loaded") is True,
        runtime_health=evaluation["runtime_health"],
        runtime_readiness=evaluation["runtime_readiness"],
        required_components=list(REQUIRED_COMPONENTS),
        component_states=evaluation["component_states"],
        event_repository=_REPOSITORY.state(),
        evaluated_at=evaluation["evaluated_at"],
    )


def evaluate_runtime_supervisor(component_states: list[dict[str, Any]] | None = None, *, persist: bool = True) -> dict[str, Any]:
    states = component_states if component_states is not None else collect_component_states()
    normalized = _normalize_state_collection(states)
    evaluation = evaluate_runtime_state(normalized)
    evaluation_id = f"SUP-{uuid4().hex[:12].upper()}"
    result = {
        "status": "PASS",
        "supervisor_evaluation_id": evaluation_id,
        "evaluated_at": _now(),
        "required_components": list(REQUIRED_COMPONENTS),
        "component_states": normalized,
        **evaluation,
    }
    if persist:
        events = _build_events(result, _REPOSITORY.last_evaluation())
        _REPOSITORY.append_many(events, result)
    return result


def evaluate_runtime_state(component_states: list[dict[str, Any]]) -> dict[str, Any]:
    states = _normalize_state_collection(component_states)
    blocking: list[dict[str, Any]] = []
    degraded: list[dict[str, Any]] = []
    evidence: list[dict[str, Any]] = []
    by_component = {item["component"]: item for item in states}

    for component in REQUIRED_COMPONENTS:
        state = by_component.get(component)
        if state is None:
            blocking.append(_blocking(component, "required_status_missing", "Required component did not publish a status", "published runtime status", "Restore the component status interface and repeat Supervisor evaluation."))
            continue
        evidence.append({
            "component": component,
            "loaded": state["loaded"],
            "operational_status": state["operational_status"],
            "required_function_available": state["required_function_available"],
            "dependencies_available": state["dependencies_available"],
            "status_source": state["status_source"],
        })
        status = state["operational_status"]
        if not state["loaded"]:
            blocking.append(_blocking(component, "required_component_not_loaded", "Required Runtime component is not loaded", "component loading", "Restore component startup and verify approved load order."))
        elif status in {"UNKNOWN", "MISSING", "INVALID"}:
            blocking.append(_blocking(component, "required_status_unknown", f"Unsupported or unknown status: {status}", "published runtime status", "Publish a supported deterministic component status."))
        elif status in FAILURE_STATUSES:
            blocking.append(_blocking(component, "component_blocking_status", f"Component published blocking status {status}", "mandatory runtime operation", "Resolve the component failure before resuming Runtime."))
        if not state["required_function_available"]:
            blocking.append(_blocking(component, "required_function_unavailable", "Mandatory Runtime function is unavailable", "mandatory runtime function", "Restore the mandatory function and repeat evaluation."))
        if not state["dependencies_available"]:
            blocking.append(_blocking(component, "required_dependency_unavailable", "Mandatory dependency is unavailable", "mandatory dependency", "Restore the dependency and repeat evaluation."))
        if state.get("blocking_error"):
            blocking.append(_blocking(component, str(state["blocking_error"].get("code") or "component_blocking_error"), str(state["blocking_error"].get("message") or "Component published a blocking error"), str(state["blocking_error"].get("affected_function") or "mandatory runtime operation"), str(state["blocking_error"].get("recommended_action") or "Resolve the blocking error and repeat evaluation.")))
        for condition in state.get("degradations", []):
            degraded.append(_degradation(component, str(condition.get("reason_code") or "component_degraded"), str(condition.get("description") or "Non-blocking degradation"), condition.get("affected_functions") or []))

    blocking = _dedupe_sort(blocking)
    degraded = _dedupe_sort(degraded)
    if blocking:
        readiness, health = "BLOCKED", "UNHEALTHY"
        reason = "BLOCKED: " + "; ".join(f"{item['component']}:{item['reason_code']}" for item in blocking)
    elif degraded:
        readiness, health = "DEGRADED", "DEGRADED"
        reason = "DEGRADED: " + "; ".join(f"{item['component']}:{item['reason_code']}" for item in degraded)
    else:
        readiness, health = "READY", "HEALTHY"
        reason = "READY: all required Runtime components and dependencies are available without degradation"
    return {
        "runtime_health": health,
        "runtime_readiness": readiness,
        "blocking_conditions": blocking,
        "degraded_conditions": degraded,
        "readiness_reason": reason,
        "evidence": sorted(evidence, key=lambda item: item["component"]),
    }


def get_runtime_supervisor_status(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return initialize_runtime_supervisor()


def get_runtime_health(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    result = evaluate_runtime_supervisor()
    return _pass(
        runtime_health=result["runtime_health"],
        runtime_readiness=result["runtime_readiness"],
        component_states=result["component_states"],
        evidence=result["evidence"],
        evaluated_at=result["evaluated_at"],
        supervisor_evaluation_id=result["supervisor_evaluation_id"],
    )


def get_runtime_readiness(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    result = evaluate_runtime_supervisor()
    return _pass(**result)


def get_runtime_events(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    events = _REPOSITORY.events()
    limit = _safe_limit(payload.get("limit"), 100)
    events = events[-limit:]
    return _pass(events_count=len(events), events=events, event_repository=_REPOSITORY.state())


def search_runtime_events(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    supported = {"component", "event_type", "reason_code", "blocking", "supervisor_evaluation_id"}
    filters = {key: value for key, value in payload.items() if key in supported and value not in (None, "")}
    results = []
    for event in _REPOSITORY.events():
        if all(event.get(key) == value for key, value in filters.items()):
            results.append(event)
    return _pass(results_count=len(results), results=results, filters=filters)


def get_runtime_diagnostics(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    result = evaluate_runtime_supervisor()
    affected = sorted({item["component"] for item in result["blocking_conditions"] + result["degraded_conditions"]})
    actions = [
        {"component": item["component"], "reason_code": item["reason_code"], "recommended_action": item["recommended_action"]}
        for item in result["blocking_conditions"]
    ]
    return _pass(
        runtime_health=result["runtime_health"],
        runtime_readiness=result["runtime_readiness"],
        readiness_reason=result["readiness_reason"],
        affected_components=affected,
        blocking_conditions=result["blocking_conditions"],
        degraded_conditions=result["degraded_conditions"],
        evidence=result["evidence"],
        recommended_actions=actions,
        read_only=True,
        evaluated_at=result["evaluated_at"],
    )


def execute_runtime_supervisor_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    operations = {
        "get_runtime_supervisor_status": get_runtime_supervisor_status,
        "get_runtime_health": get_runtime_health,
        "get_runtime_readiness": get_runtime_readiness,
        "get_runtime_events": get_runtime_events,
        "search_runtime_events": search_runtime_events,
        "get_runtime_diagnostics": get_runtime_diagnostics,
    }
    handler = operations.get(operation_type)
    if handler is None:
        return _fail("unsupported_runtime_supervisor_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload or {})


def _normalize_component_state(component: str, raw: dict[str, Any], source: str) -> dict[str, Any]:
    status = _extract_status(component, raw)
    required_function_available = True
    dependencies_available = True
    evidence: dict[str, Any] = {"published_status": deepcopy(raw)}
    if component == "Architecture Registry Runtime":
        objects = list_architecture_objects({})
        required_function_available = objects.get("status") == "PASS" and int(objects.get("objects_count") or 0) > 0
        evidence["mandatory_object_count"] = objects.get("objects_count")
        dependencies_available = raw.get("integrity_status") == "PASS"
    else:
        dependency_flags = [value for key, value in raw.items() if key.endswith("_loaded")]
        if dependency_flags:
            dependencies_available = all(value is True for value in dependency_flags)
    degradations = deepcopy(raw.get("degraded_conditions") or raw.get("degradations") or [])
    blocking_error = deepcopy(raw.get("blocking_error"))
    return {
        "component": component,
        "loaded": raw.get("loaded") is True,
        "operational_status": status,
        "required_function_available": required_function_available,
        "dependencies_available": dependencies_available,
        "blocking_error": blocking_error,
        "degradations": degradations if isinstance(degradations, list) else [],
        "last_successful_evaluation": _now() if raw.get("status") == "PASS" else None,
        "status_source": source,
        "evidence": evidence,
    }


def _extract_status(component: str, raw: dict[str, Any]) -> str:
    keys = {
        "Architecture Registry Runtime": ("verification_status", "status"),
        "Verification Runtime": ("verification_status", "status"),
        "Execution Runtime": ("execution_runtime_status", "status"),
        "Execution Orchestrator Runtime": ("orchestrator_status", "status"),
        "Session Runtime": ("session_runtime_status", "status"),
    }[component]
    for key in keys:
        value = raw.get(key)
        if value not in (None, ""):
            text = str(value).upper()
            if text in SUCCESS_STATUSES | FAILURE_STATUSES | DEGRADED_STATUSES:
                return text
            return "UNKNOWN"
    return "MISSING"


def _normalize_state_collection(states: list[dict[str, Any]]) -> list[dict[str, Any]]:
    normalized = []
    for raw in states:
        component = str(raw.get("component") or raw.get("runtime_component") or "").strip()
        if not component:
            continue
        status = str(raw.get("operational_status") or raw.get("status") or "MISSING").upper()
        if status not in SUCCESS_STATUSES | FAILURE_STATUSES | DEGRADED_STATUSES | {"UNKNOWN", "MISSING", "INVALID"}:
            status = "UNKNOWN"
        normalized.append({
            "component": component,
            "loaded": raw.get("loaded") is True,
            "operational_status": status,
            "required_function_available": raw.get("required_function_available") is not False,
            "dependencies_available": raw.get("dependencies_available") is not False,
            "blocking_error": deepcopy(raw.get("blocking_error")),
            "degradations": deepcopy(raw.get("degradations") or []),
            "last_successful_evaluation": raw.get("last_successful_evaluation"),
            "status_source": str(raw.get("status_source") or "provided_component_state"),
            "evidence": deepcopy(raw.get("evidence") or {}),
        })
    return sorted(normalized, key=lambda item: item["component"])


def _unavailable_component(component: str, code: str, message: str) -> dict[str, Any]:
    return {
        "component": component,
        "loaded": False,
        "operational_status": "UNAVAILABLE",
        "required_function_available": False,
        "dependencies_available": False,
        "blocking_error": {"code": code, "message": message},
        "degradations": [],
        "last_successful_evaluation": None,
        "status_source": "runtime_supervisor.collection_error",
        "evidence": {"error": message},
    }


def _blocking(component: str, reason_code: str, description: str, affected_function: str, recommended_action: str) -> dict[str, Any]:
    return {"component": component, "reason_code": reason_code, "description": description, "affected_function": affected_function, "blocking": True, "recommended_action": recommended_action}


def _degradation(component: str, reason_code: str, description: str, affected_functions: list[str]) -> dict[str, Any]:
    return {"component": component, "reason_code": reason_code, "description": description, "affected_functions": sorted(str(item) for item in affected_functions), "blocking": False}


def _dedupe_sort(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    unique: dict[tuple[str, str], dict[str, Any]] = {}
    for item in items:
        unique[(item["component"], item["reason_code"])] = item
    return [unique[key] for key in sorted(unique)]


def _build_events(current: dict[str, Any], previous: dict[str, Any] | None) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    evaluation_id = current["supervisor_evaluation_id"]
    if previous is None:
        events.append(_event("Runtime Supervisor", None, "READY", "SUPERVISOR_STARTED", "supervisor_started", False, current, evaluation_id))
    previous_states = {item["component"]: item["operational_status"] for item in (previous or {}).get("component_states", [])}
    for item in current["component_states"]:
        old = previous_states.get(item["component"])
        if old != item["operational_status"]:
            events.append(_event(item["component"], old, item["operational_status"], "COMPONENT_STATUS_CHANGED", "component_status_changed", item["operational_status"] in FAILURE_STATUSES, item, evaluation_id))
    for field, event_type in (("runtime_health", "RUNTIME_HEALTH_CHANGED"), ("runtime_readiness", "RUNTIME_READINESS_CHANGED")):
        old = (previous or {}).get(field)
        if old != current[field]:
            events.append(_event("Runtime Supervisor", old, current[field], event_type, field.lower() + "_changed", current[field] in {"UNHEALTHY", "BLOCKED"}, current, evaluation_id))
    for condition in current["blocking_conditions"]:
        events.append(_event(condition["component"], None, "BLOCKED", "BLOCKING_CONDITION_DETECTED", condition["reason_code"], True, condition, evaluation_id))
    for condition in current["degraded_conditions"]:
        events.append(_event(condition["component"], None, "DEGRADED", "DEGRADATION_DETECTED", condition["reason_code"], False, condition, evaluation_id))
    if previous and previous.get("runtime_readiness") in {"BLOCKED", "DEGRADED"} and current["runtime_readiness"] == "READY":
        events.append(_event("Runtime Supervisor", previous.get("runtime_readiness"), "READY", "RUNTIME_RECOVERED", "runtime_recovered", False, current, evaluation_id))
    events.append(_event("Runtime Supervisor", None, current["runtime_readiness"], "SUPERVISOR_EVALUATION_COMPLETED", "evaluation_completed", current["runtime_readiness"] == "BLOCKED", current, evaluation_id))
    return events


def _event(component: str, previous_status: Any, current_status: Any, event_type: str, reason_code: str, blocking: bool, evidence: Any, evaluation_id: str) -> dict[str, Any]:
    return {"event_id": f"EVT-{uuid4().hex[:12].upper()}", "timestamp": _now(), "component": component, "previous_status": previous_status, "current_status": current_status, "event_type": event_type, "reason_code": reason_code, "blocking": blocking, "diagnostic_message": f"{component}: {reason_code}", "evidence": deepcopy(evidence), "supervisor_evaluation_id": evaluation_id}


def _safe_limit(value: Any, default: int) -> int:
    try:
        return max(1, min(int(value), 1000))
    except (TypeError, ValueError):
        return default


def _pass(**fields: Any) -> dict[str, Any]:
    return {"status": "PASS", **fields, "error": None}


def _fail(code: str, message: str) -> dict[str, Any]:
    return {"status": "FAIL", "error": {"code": code, "message": message}}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()
