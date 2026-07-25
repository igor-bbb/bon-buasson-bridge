from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

from app.assistant_runtime.architecture_registry_runtime import (
    get_architecture_object,
    initialize_architecture_registry_runtime,
    resolve_dependencies,
)
from app.assistant_runtime.execution_runtime import (
    initialize_execution_runtime,
    run_execution,
)
from app.assistant_runtime.verification_runtime import initialize_verification_runtime

RELEASE_ID = "VECTRA-EXECUTION-ORCHESTRATOR-001"
RESULTS_PATH = Path("runtime/execution_orchestrator/orchestration_plans.json")
TERMINAL_STATES = {"COMPLETED", "FAILED", "BLOCKED"}
QUEUE_STATES = {"READY", "RUNNING", "COMPLETED", "FAILED", "BLOCKED"}


class OrchestrationRuntimeError(RuntimeError):
    pass


class OrchestrationRepository:
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
                    "repository_id": "VECTRA-ORCHESTRATION-REPOSITORY-001",
                    "release_id": RELEASE_ID,
                    "schema_version": "1.0",
                    "plans": [],
                    "updated_at": _now(),
                }
                self._persist()
            else:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(self._data.get("plans"), list):
                    raise OrchestrationRuntimeError("orchestration_repository_invalid")
            return deepcopy(self._data)

    def save(self, plan: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            data = self._require_loaded()
            for index, current in enumerate(data["plans"]):
                if current.get("execution_plan_id") == plan.get("execution_plan_id"):
                    data["plans"][index] = deepcopy(plan)
                    break
            else:
                data["plans"].append(deepcopy(plan))
            data["updated_at"] = _now()
            self._persist()
            return deepcopy(plan)

    def get(self, plan_id: str) -> dict[str, Any] | None:
        for plan in self.plans():
            if plan.get("execution_plan_id") == plan_id:
                return plan
        return None

    def plans(self) -> list[dict[str, Any]]:
        return deepcopy(self._require_loaded()["plans"])

    def state(self) -> dict[str, Any]:
        data = self._require_loaded()
        return {
            "repository_id": data["repository_id"],
            "plans_count": len(data["plans"]),
            "last_plan_id": data["plans"][-1]["execution_plan_id"] if data["plans"] else None,
            "loaded": True,
        }

    def _require_loaded(self) -> dict[str, Any]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return self._data

    def _persist(self) -> None:
        assert self._data is not None
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, self.path)


_REPOSITORY = OrchestrationRepository()


def initialize_execution_orchestrator(*, force: bool = False) -> dict[str, Any]:
    registry = initialize_architecture_registry_runtime(force=force)
    verification = initialize_verification_runtime(force=force)
    execution = initialize_execution_runtime(force=force)
    repository = _REPOSITORY.load(force=force)
    return {
        "status": "PASS",
        "orchestrator_status": "READY",
        "runtime_component": "Execution Orchestrator Runtime",
        "release_id": RELEASE_ID,
        "loaded": True,
        "load_order": "AFTER_EXECUTION_RUNTIME",
        "architecture_registry_loaded": registry.get("loaded") is True,
        "verification_runtime_loaded": verification.get("loaded") is True,
        "execution_runtime_loaded": execution.get("loaded") is True,
        "orchestration_repository_id": repository["repository_id"],
        "plans_count": len(repository["plans"]),
        "error": None,
    }


def create_execution_plan(payload: dict[str, Any]) -> dict[str, Any]:
    object_ids = _normalize_object_ids(payload)
    if not object_ids:
        return _fail("execution_object_ids_required", "object_ids must contain at least one registered architecture object")
    for object_id in object_ids:
        response = get_architecture_object({"object_id": object_id})
        if response.get("status") != "PASS":
            return response
    order, dependency_map, unresolved = _resolve_order(object_ids)
    if unresolved:
        return _fail("execution_dependencies_unresolved", "Execution dependencies contain a cycle or unresolved dependency", unresolved_dependencies=unresolved)
    plan_id = str(payload.get("execution_plan_id") or f"PLAN-{uuid4().hex[:12].upper()}")
    now = _now()
    queue = [
        {
            "queue_position": index,
            "object_id": object_id,
            "dependencies": dependency_map[object_id],
            "status": "READY" if not dependency_map[object_id] else "BLOCKED",
            "execution_id": None,
            "started_at": None,
            "finished_at": None,
            "result": None,
            "error": None,
        }
        for index, object_id in enumerate(order, start=1)
    ]
    plan = {
        "execution_plan_id": plan_id,
        "execution_ids": [],
        "requested_object_ids": object_ids,
        "execution_order": order,
        "dependency_map": dependency_map,
        "queue": queue,
        "orchestration_status": "READY",
        "orchestration_log": [_log("PLAN_CREATED", "READY")],
        "created_at": now,
        "started_at": None,
        "finished_at": None,
        "stop_reason": None,
        "repeat_of": payload.get("repeat_of"),
        "retention_policy": "KEEP_COMPLETED_ITEMS",
        "runtime_sources": {
            "registry": "ArchitectureRegistryRuntime.resolve_dependencies",
            "verification": "VerificationRuntime.published_results",
            "execution": "ExecutionRuntime.run_execution",
        },
    }
    return _REPOSITORY.save(plan)


def start_execution_plan(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("repeat") is True:
        source_id = str(payload.get("execution_plan_id") or "").strip()
        source = _REPOSITORY.get(source_id)
        if source is None:
            return _fail("execution_plan_not_found", "Execution plan was not found")
        created = create_execution_plan({"object_ids": source["requested_object_ids"], "repeat_of": source_id})
        if created.get("status") == "FAIL":
            return created
        payload = {"execution_plan_id": created["execution_plan_id"]}
    plan_id = str(payload.get("execution_plan_id") or "").strip()
    plan = _REPOSITORY.get(plan_id) if plan_id else None
    if plan is None:
        plan = create_execution_plan(payload)
        if plan.get("status") == "FAIL":
            return plan
    if plan["orchestration_status"] == "COMPLETED":
        return _fail("execution_plan_already_completed", "Completed plans require repeat=true")
    plan["orchestration_status"] = "RUNNING"
    plan["started_at"] = plan["started_at"] or _now()
    plan["orchestration_log"].append(_log("PLAN_STARTED", "RUNNING"))
    _REPOSITORY.save(plan)

    completed: set[str] = set()
    for item in plan["queue"]:
        if item["status"] == "COMPLETED":
            completed.add(item["object_id"])
            continue
        if not set(item["dependencies"]).issubset(completed):
            item["status"] = "BLOCKED"
            item["error"] = {"code": "dependency_not_completed", "message": "Required execution dependency is not completed"}
            plan["orchestration_status"] = "BLOCKED"
            plan["stop_reason"] = "dependency_not_completed"
            break
        item["status"] = "RUNNING"
        item["started_at"] = _now()
        plan["orchestration_log"].append(_log(f"QUEUE:{item['object_id']}", "RUNNING"))
        _REPOSITORY.save(plan)
        result = run_execution({"object_id": item["object_id"]})
        item["execution_id"] = result.get("execution_id")
        item["finished_at"] = _now()
        item["result"] = result.get("execution_result")
        item["error"] = result.get("error")
        item["status"] = "COMPLETED" if result.get("execution_status") == "COMPLETED" else "FAILED"
        if item["execution_id"]:
            plan["execution_ids"].append(item["execution_id"])
        plan["orchestration_log"].append(_log(f"QUEUE:{item['object_id']}", item["status"]))
        if item["status"] != "COMPLETED":
            plan["orchestration_status"] = "FAILED"
            plan["stop_reason"] = result.get("error", {}).get("code") or "execution_failed"
            for remaining in plan["queue"]:
                if remaining["status"] in {"READY", "BLOCKED"}:
                    remaining["status"] = "BLOCKED"
            break
        completed.add(item["object_id"])
    else:
        plan["orchestration_status"] = "COMPLETED"
        plan["finished_at"] = _now()
        plan["stop_reason"] = None
        plan["orchestration_log"].append(_log("PLAN_FINISHED", "COMPLETED"))
    return _REPOSITORY.save(plan)


def get_execution_plan_status(payload: dict[str, Any]) -> dict[str, Any]:
    plan = _get_plan(payload)
    if isinstance(plan, dict) and plan.get("status") == "FAIL":
        return plan
    return _pass(
        execution_plan_id=plan["execution_plan_id"],
        orchestration_status=plan["orchestration_status"],
        execution_order=plan["execution_order"],
        execution_ids=plan["execution_ids"],
        queue_summary=_queue_summary(plan["queue"]),
        stop_reason=plan["stop_reason"],
        repeat_of=plan["repeat_of"],
    )


def get_execution_queue(payload: dict[str, Any]) -> dict[str, Any]:
    plan = _get_plan(payload)
    if isinstance(plan, dict) and plan.get("status") == "FAIL":
        return plan
    return _pass(
        execution_plan_id=plan["execution_plan_id"],
        orchestration_status=plan["orchestration_status"],
        queue=deepcopy(plan["queue"]),
        queue_summary=_queue_summary(plan["queue"]),
        retention_policy=plan["retention_policy"],
    )


def search_execution_plans(payload: dict[str, Any]) -> dict[str, Any]:
    supported = {"execution_plan_id", "orchestration_status", "repeat_of", "object_id", "execution_id"}
    criteria = {key: value for key, value in payload.items() if key in supported and value not in (None, "")}
    if not criteria:
        return _fail("search_criteria_required", "At least one orchestration search criterion is required")
    results = []
    for plan in _REPOSITORY.plans():
        if _matches_plan(plan, criteria):
            results.append({
                "execution_plan_id": plan["execution_plan_id"],
                "orchestration_status": plan["orchestration_status"],
                "requested_object_ids": plan["requested_object_ids"],
                "execution_ids": plan["execution_ids"],
                "repeat_of": plan["repeat_of"],
                "created_at": plan["created_at"],
            })
    return _pass(results_count=len(results), results=results, filters=criteria)


def execute_orchestrator_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    operations = {
        "get_orchestrator_status": lambda _: initialize_execution_orchestrator(),
        "create_execution_plan": create_execution_plan,
        "start_execution_plan": start_execution_plan,
        "get_execution_plan_status": get_execution_plan_status,
        "get_execution_queue": get_execution_queue,
        "search_execution_plans": search_execution_plans,
    }
    handler = operations.get(operation_type)
    if handler is None:
        return _fail("unsupported_orchestrator_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload)


def _resolve_order(object_ids: list[str]) -> tuple[list[str], dict[str, list[str]], list[str]]:
    selected = set(object_ids)
    dependency_map: dict[str, list[str]] = {}
    for object_id in object_ids:
        response = resolve_dependencies({"object_id": object_id})
        if response.get("status") != "PASS":
            return [], {}, [object_id]
        dependency_map[object_id] = sorted(set(response.get("direct_dependencies", [])) & selected)
    indegree = {item: len(dependency_map[item]) for item in object_ids}
    ready = sorted([item for item, degree in indegree.items() if degree == 0])
    order: list[str] = []
    while ready:
        current = ready.pop(0)
        order.append(current)
        for target in sorted(object_ids):
            if current in dependency_map[target]:
                indegree[target] -= 1
                if indegree[target] == 0:
                    ready.append(target)
                    ready.sort()
    unresolved = sorted(set(object_ids) - set(order))
    return order, dependency_map, unresolved


def _normalize_object_ids(payload: dict[str, Any]) -> list[str]:
    raw = payload.get("object_ids") or payload.get("execution_object_ids") or []
    if isinstance(raw, str):
        raw = [item.strip() for item in raw.split(",")]
    return list(dict.fromkeys(str(item).strip() for item in raw if str(item).strip()))


def _get_plan(payload: dict[str, Any]) -> dict[str, Any]:
    plan_id = str(payload.get("execution_plan_id") or "").strip()
    if not plan_id:
        return _fail("execution_plan_id_required", "execution_plan_id is required")
    plan = _REPOSITORY.get(plan_id)
    return plan if plan is not None else _fail("execution_plan_not_found", "Execution plan was not found")


def _matches_plan(plan: dict[str, Any], criteria: dict[str, Any]) -> bool:
    for key, value in criteria.items():
        needle = str(value).casefold()
        if key == "object_id":
            haystack = " ".join(plan.get("requested_object_ids", []))
        elif key == "execution_id":
            haystack = " ".join(plan.get("execution_ids", []))
        else:
            haystack = str(plan.get(key, ""))
        if needle not in haystack.casefold():
            return False
    return True


def _queue_summary(queue: list[dict[str, Any]]) -> dict[str, int]:
    summary = {state: 0 for state in sorted(QUEUE_STATES)}
    for item in queue:
        summary[item["status"]] += 1
    summary["TOTAL"] = len(queue)
    return summary


def _log(event: str, status: str) -> dict[str, Any]:
    return {"timestamp": _now(), "event": event, "status": status}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _pass(**payload: Any) -> dict[str, Any]:
    return {"status": "PASS", **payload, "error": None}


def _fail(code: str, message: str, **payload: Any) -> dict[str, Any]:
    return {"status": "FAIL", **payload, "error": {"code": code, "message": message}}
