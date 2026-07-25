from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

from app.assistant_runtime.architecture_registry_runtime import get_architecture_object
from app.assistant_runtime.professional_procedures_runtime import resolve_professional_procedure
from app.assistant_runtime.recovery import run_recovery_evolution
from app.assistant_runtime.runtime_supervisor import (
    evaluate_runtime_supervisor,
    get_runtime_health,
    get_runtime_readiness,
)

RELEASE_ID = "VECTRA-RUNTIME-RECOVERY-001"
RESULTS_PATH = Path("runtime/runtime_recovery/recovery_history.json")
REGISTERED_RECOVERY_OBJECT_ID = "VECTRA-SERVICE-RECOVERY-001"
REGISTERED_RECOVERY_PROCEDURE_ID = "handle_confirmed_blocker"
ALLOWED_START_STATES = {"BLOCKED", "UNHEALTHY"}


class RuntimeRecoveryError(RuntimeError):
    pass


class RecoveryHistoryRepository:
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
                    "repository_id": "VECTRA-RUNTIME-RECOVERY-HISTORY-001",
                    "release_id": RELEASE_ID,
                    "schema_version": "1.0",
                    "executions": [],
                    "updated_at": _now(),
                }
                self._persist()
            else:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(self._data.get("executions"), list):
                    raise RuntimeRecoveryError("recovery_history_repository_invalid")
            return deepcopy(self._data)

    def append(self, execution: dict[str, Any]) -> None:
        with self._lock:
            data = self._require_loaded()
            data["executions"].append(deepcopy(execution))
            data["updated_at"] = _now()
            self._persist()

    def executions(self) -> list[dict[str, Any]]:
        return deepcopy(self._require_loaded()["executions"])

    def state(self) -> dict[str, Any]:
        data = self._require_loaded()
        return {
            "repository_id": data["repository_id"],
            "loaded": True,
            "executions_count": len(data["executions"]),
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


_REPOSITORY = RecoveryHistoryRepository()


def initialize_runtime_recovery(*, force: bool = False) -> dict[str, Any]:
    _REPOSITORY.load(force=force)
    supervisor = get_runtime_readiness({})
    return _pass(
        runtime_component="Runtime Recovery",
        release_id=RELEASE_ID,
        loaded=True,
        load_order="AFTER_RUNTIME_SUPERVISOR",
        recovery_status="READY",
        supervisor_available=supervisor.get("status") == "PASS",
        registered_procedure_source="Professional Procedures Runtime",
        registered_recovery_object_id=REGISTERED_RECOVERY_OBJECT_ID,
        history_repository=_REPOSITORY.state(),
        evaluated_at=_now(),
    )


def resolve_recovery_plan(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    requested = str(payload.get("procedure_id") or REGISTERED_RECOVERY_PROCEDURE_ID).strip()
    if requested != REGISTERED_RECOVERY_PROCEDURE_ID:
        return _fail("recovery_procedure_not_registered", f"Recovery procedure {requested} is not registered or allowed")

    procedure_result = resolve_professional_procedure({"procedure_id": requested})
    if procedure_result.get("status") != "PASS":
        return _fail("registered_recovery_procedure_unavailable", "Registered Professional Runtime procedure could not be resolved", evidence=procedure_result)
    procedure = procedure_result.get("active_procedure") or {}
    if procedure.get("status") != "ACTIVE" or procedure.get("lifecycle_status") != "ACTIVE":
        return _fail("registered_recovery_procedure_inactive", "Registered Recovery procedure is not active")

    architecture_result = get_architecture_object({"object_id": REGISTERED_RECOVERY_OBJECT_ID})
    if architecture_result.get("status") != "PASS":
        return _fail("registered_recovery_service_unavailable", "Registered Recovery service is not available through Architecture Registry", evidence=architecture_result)
    architecture_object = architecture_result.get("object") or {}
    implementation_paths = sorted((architecture_object.get("implementation") or {}).get("paths") or [])
    if "app/assistant_runtime/recovery.py" not in implementation_paths:
        return _fail("registered_recovery_runtime_mapping_invalid", "Registered Recovery service does not publish the approved Runtime implementation")

    target_component = str(payload.get("target_component") or "Runtime").strip() or "Runtime"
    allowed_steps = [str(step) for step in procedure.get("steps") or []]
    canonical = {
        "procedure_set_id": procedure_result.get("procedure_set_id"),
        "procedure_set_version": procedure_result.get("procedure_set_version"),
        "procedure_id": procedure.get("procedure_id"),
        "procedure_version": procedure.get("version"),
        "architecture_object_id": architecture_object.get("object_id"),
        "implementation_paths": implementation_paths,
        "target_component": target_component,
        "allowed_steps": allowed_steps,
        "expected_result": "SUPERVISOR_REEVALUATED",
    }
    digest = hashlib.sha256(json.dumps(canonical, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:16].upper()
    return _pass(
        recovery_plan={
            "recovery_plan_id": f"RECOVERY-PLAN-{digest}",
            "procedure_source": {
                "procedure_set_id": procedure_result.get("procedure_set_id"),
                "procedure_set_version": procedure_result.get("procedure_set_version"),
                "procedure_id": procedure.get("procedure_id"),
                "procedure_version": procedure.get("version"),
                "architecture_object_id": architecture_object.get("object_id"),
                "implementation_paths": implementation_paths,
            },
            "allowed_steps": allowed_steps,
            "target_component": target_component,
            "expected_result": "SUPERVISOR_REEVALUATED",
            "execution_status": "READY",
            "evidence": {
                "professional_procedure_resolution": "PASS",
                "architecture_registry_resolution": "PASS",
                "independent_recovery_catalog": False,
                "deterministic_digest": digest,
            },
        }
    )


def start_runtime_recovery(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    before_readiness = get_runtime_readiness({})
    before_health = get_runtime_health({})
    readiness = str(before_readiness.get("runtime_readiness") or "").upper()
    health = str(before_health.get("runtime_health") or "").upper()
    if readiness not in ALLOWED_START_STATES and health not in ALLOWED_START_STATES:
        return _fail(
            "runtime_recovery_not_allowed",
            "Runtime Recovery may start only for BLOCKED readiness or UNHEALTHY health",
            supervisor_evaluation_before={"runtime_readiness": readiness, "runtime_health": health},
        )

    plan_result = resolve_recovery_plan(payload)
    if plan_result.get("status") != "PASS":
        return plan_result
    plan = plan_result["recovery_plan"]
    started_at = _now()
    execution_id = f"RECOVERY-{uuid4().hex[:12].upper()}"

    recovery_result = run_recovery_evolution({
        "reason": str(payload.get("reason") or "runtime_recovery_from_confirmed_blocking_state"),
        "source": RELEASE_ID,
        "target_component": plan["target_component"],
        "recovery_plan_id": plan["recovery_plan_id"],
    })
    operation_passed = str(recovery_result.get("status") or "").lower() in {"ok", "pass"}

    after = evaluate_runtime_supervisor()
    execution = {
        "recovery_execution_id": execution_id,
        "recovery_plan_id": plan["recovery_plan_id"],
        "started_at": started_at,
        "completed_at": _now(),
        "target_component": plan["target_component"],
        "result": "COMPLETED" if operation_passed else "FAILED",
        "evidence": {
            "registered_procedure": plan["procedure_source"],
            "allowed_steps": plan["allowed_steps"],
            "runtime_recovery_result": recovery_result,
            "supervisor_reevaluation_status": after.get("status"),
        },
        "supervisor_evaluation_before": {
            "runtime_readiness": readiness,
            "runtime_health": health,
            "supervisor_evaluation_id": before_readiness.get("supervisor_evaluation_id"),
        },
        "supervisor_evaluation_after": {
            "runtime_readiness": after.get("runtime_readiness"),
            "runtime_health": after.get("runtime_health"),
            "supervisor_evaluation_id": after.get("supervisor_evaluation_id"),
            "evaluated_at": after.get("evaluated_at"),
        },
    }
    _REPOSITORY.append(execution)
    return _pass(
        recovery_execution=execution,
        recovery_plan=plan,
        supervisor_reevaluated=after.get("status") == "PASS",
    )


def get_runtime_recovery_status(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return initialize_runtime_recovery()


def get_runtime_recovery_plan(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return resolve_recovery_plan(payload)


def get_runtime_recovery_history(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    executions = _REPOSITORY.executions()
    limit = _safe_limit(payload.get("limit"), 100)
    executions = executions[-limit:]
    return _pass(executions_count=len(executions), executions=executions, history_repository=_REPOSITORY.state())


def search_runtime_recovery(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    supported = {"recovery_execution_id", "recovery_plan_id", "target_component", "result"}
    filters = {key: value for key, value in payload.items() if key in supported and value not in (None, "")}
    results = [item for item in _REPOSITORY.executions() if all(item.get(key) == value for key, value in filters.items())]
    return _pass(results_count=len(results), results=results, filters=filters)


def execute_runtime_recovery_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    operations = {
        "get_runtime_recovery_status": get_runtime_recovery_status,
        "start_runtime_recovery": start_runtime_recovery,
        "get_runtime_recovery_history": get_runtime_recovery_history,
        "search_runtime_recovery": search_runtime_recovery,
        "get_runtime_recovery_plan": get_runtime_recovery_plan,
    }
    handler = operations.get(operation_type)
    if handler is None:
        return _fail("unsupported_runtime_recovery_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload or {})


def _safe_limit(value: Any, default: int) -> int:
    try:
        return max(1, min(int(value), 1000))
    except (TypeError, ValueError):
        return default


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pass(**payload: Any) -> dict[str, Any]:
    return {"status": "PASS", **payload}


def _fail(code: str, message: str, **payload: Any) -> dict[str, Any]:
    return {"status": "FAIL", "failure_reason": code, "message": message, **payload}
