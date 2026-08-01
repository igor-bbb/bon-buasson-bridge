from __future__ import annotations

import hashlib
import importlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Callable
from uuid import uuid4

from app.assistant_runtime.architecture_registry_runtime import get_architecture_object
from app.assistant_runtime.professional_procedures_runtime import resolve_professional_procedure
from app.assistant_runtime.recovery import run_recovery_evolution
from app.assistant_runtime.runtime_supervisor import (
    evaluate_runtime_supervisor,
    get_runtime_health,
    get_runtime_readiness,
)

RELEASE_ID = "VECTRA-RUNTIME-ROOT-RECOVERY-001"
CONTRACT_VERSION = "runtime_root_recovery.v1"
RESULTS_PATH = Path("runtime/runtime_recovery/recovery_history.json")
REGISTERED_RECOVERY_OBJECT_ID = "VECTRA-SERVICE-RECOVERY-001"
REGISTERED_RECOVERY_PROCEDURE_ID = "handle_confirmed_blocker"
ALLOWED_START_STATES = {"BLOCKED", "UNHEALTHY"}

ROOT_COMPONENTS = (
    "Architecture Registry Runtime",
    "Verification Runtime",
    "Execution Runtime",
    "Execution Orchestrator Runtime",
    "Session Runtime",
    "Runtime Supervisor",
    "Runtime Recovery",
    "Runtime Capability Registry",
    "Runtime Dependency Graph",
    "Runtime Observability",
    "Runtime Health",
    "Runtime Snapshot",
)

_COMPONENT_INITIALIZERS: dict[str, tuple[str, str, str]] = {
    "Architecture Registry Runtime": ("app.assistant_runtime.architecture_registry_runtime", "initialize_architecture_registry_runtime", "RELOAD_FROM_ARCHITECTURE_REGISTRY"),
    "Verification Runtime": ("app.assistant_runtime.verification_runtime", "initialize_verification_runtime", "RELOAD_FROM_VERIFICATION_REPOSITORY"),
    "Execution Runtime": ("app.assistant_runtime.execution_runtime", "initialize_execution_runtime", "RELOAD_FROM_EXECUTION_REPOSITORY"),
    "Execution Orchestrator Runtime": ("app.assistant_runtime.execution_orchestrator_runtime", "initialize_execution_orchestrator", "RELOAD_FROM_ORCHESTRATION_REPOSITORY"),
    "Session Runtime": ("app.assistant_runtime.session_runtime", "initialize_session_runtime", "RELOAD_FROM_SESSION_REPOSITORY"),
    "Runtime Supervisor": ("app.assistant_runtime.runtime_supervisor", "initialize_runtime_supervisor", "REEVALUATE_PUBLISHED_RUNTIME_STATE"),
    "Runtime Recovery": (__name__, "initialize_runtime_recovery", "RELOAD_RECOVERY_HISTORY"),
    "Runtime Capability Registry": ("app.assistant_runtime.runtime_capability_registry", "initialize_runtime_capability_registry", "REBUILD_FROM_PUBLISHED_CAPABILITIES"),
    "Runtime Dependency Graph": ("app.assistant_runtime.runtime_dependency_graph", "initialize_runtime_dependency_graph", "REBUILD_FROM_PUBLISHED_DEPENDENCIES"),
    "Runtime Observability": ("app.assistant_runtime.runtime_observability", "initialize_runtime_observability", "REBUILD_FROM_PUBLISHED_RUNTIME_DATA"),
    "Runtime Health": ("app.assistant_runtime.runtime_health", "initialize_runtime_health", "REBUILD_FROM_APPROVED_HEALTH_FACTORS"),
    "Runtime Snapshot": ("app.assistant_runtime.observability", "create_startup_runtime_snapshot", "REBUILD_OFFICIAL_RUNTIME_SNAPSHOT"),
}


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
                    "contract": CONTRACT_VERSION,
                    "schema_version": "2.0",
                    "executions": [],
                    "updated_at": _now(),
                }
                self._persist()
            else:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(self._data.get("executions"), list):
                    raise RuntimeRecoveryError("recovery_history_repository_invalid")
                migrated = (
                    self._data.get("release_id") != RELEASE_ID
                    or self._data.get("contract") != CONTRACT_VERSION
                    or self._data.get("schema_version") != "2.0"
                )
                self._data["release_id"] = RELEASE_ID
                self._data["contract"] = CONTRACT_VERSION
                self._data["schema_version"] = "2.0"
                if migrated:
                    self._data["updated_at"] = _now()
                    self._persist()
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
            "release_id": data.get("release_id"),
            "contract": data.get("contract"),
            "schema_version": data.get("schema_version"),
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
        contract=CONTRACT_VERSION,
        loaded=True,
        load_order="AFTER_RUNTIME_SUPERVISOR",
        recovery_status="READY",
        connection_status="CONNECTED" if supervisor.get("status") == "PASS" else "DISCONNECTED",
        supervisor_available=supervisor.get("status") == "PASS",
        registered_procedure_source="Professional Procedures Runtime",
        registered_recovery_object_id=REGISTERED_RECOVERY_OBJECT_ID,
        history_repository=_REPOSITORY.state(),
        supported_root_components=list(ROOT_COMPONENTS),
        component_recovery_contracts={name: get_component_recovery_contract(name) for name in ROOT_COMPONENTS},
        evaluated_at=_now(),
    )


def get_component_recovery_contract(component: str) -> dict[str, Any]:
    """Publish availability without claiming that a recovery was executed."""
    normalized = _normalize_target_component(component)
    if normalized is None or normalized == "Runtime":
        return {
            "status": "UNSUPPORTED",
            "contract": CONTRACT_VERSION,
            "target_component": component,
        }
    _, _, strategy = _COMPONENT_INITIALIZERS[normalized]
    return {
        "status": "AVAILABLE",
        "contract": CONTRACT_VERSION,
        "release_id": RELEASE_ID,
        "target_component": normalized,
        "strategy": strategy,
        "execution_required_for_confirmation": True,
    }


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
    required_paths = {
        "app/assistant_runtime/recovery.py",
        "app/assistant_runtime/runtime_recovery.py",
    }
    if not required_paths.issubset(implementation_paths):
        return _fail("registered_recovery_runtime_mapping_invalid", "Registered Recovery service does not publish the approved Runtime implementation")

    requested_target = str(payload.get("target_component") or "Runtime").strip() or "Runtime"
    target_component = _normalize_target_component(requested_target)
    if target_component is None:
        return _fail(
            "runtime_recovery_target_not_supported",
            f"Runtime root {requested_target} is not supported",
            supported_root_components=list(ROOT_COMPONENTS),
        )
    recovery_sequence = _recovery_sequence(target_component)
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
        "expected_result": "TARGET_RECOVERED_AND_SUPERVISOR_READY",
        "recovery_sequence": recovery_sequence,
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
            "expected_result": "TARGET_RECOVERED_AND_SUPERVISOR_READY",
            "recovery_sequence": recovery_sequence,
            "technical_execution_contract": CONTRACT_VERSION,
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
    target_status_before = _read_target_status(plan["target_component"], before_readiness, before_health)

    checkpoint_result = run_recovery_evolution({
        "reason": str(payload.get("reason") or "runtime_recovery_from_confirmed_blocking_state"),
        "source": RELEASE_ID,
        "target_component": plan["target_component"],
        "recovery_plan_id": plan["recovery_plan_id"],
    })
    checkpoint_passed = str(checkpoint_result.get("status") or "").lower() in {"ok", "pass"}

    component_executions: list[dict[str, Any]] = []
    if checkpoint_passed:
        for component in plan["recovery_sequence"]:
            step = _recover_component(component)
            component_executions.append(step)
            if step["status"] != "PASS":
                break

    after = evaluate_runtime_supervisor()
    target_step = next((item for item in component_executions if item["runtime_component"] == plan["target_component"]), None)
    if plan["target_component"] == "Runtime":
        target_recovered = bool(component_executions) and len(component_executions) == len(plan["recovery_sequence"]) and all(item["status"] == "PASS" for item in component_executions)
    else:
        target_recovered = bool(target_step and target_step["status"] == "PASS")
    after_readiness = str(after.get("runtime_readiness") or "").upper()
    after_health = str(after.get("runtime_health") or "").upper()
    reevaluation_executed = bool(
        after.get("supervisor_evaluation_id")
        and after.get("evaluated_at")
        and after.get("supervisor_evaluation_id") != before_readiness.get("supervisor_evaluation_id")
    )
    supervisor_recovery_confirmed = after_readiness == "READY" and after_health == "HEALTHY"
    completed = checkpoint_passed and target_recovered and reevaluation_executed and supervisor_recovery_confirmed
    if not checkpoint_passed:
        failure_reason = "recovery_checkpoint_failed"
    elif not target_recovered:
        failure_reason = "target_component_not_recovered"
    elif not reevaluation_executed:
        failure_reason = "supervisor_reevaluation_not_confirmed"
    elif not supervisor_recovery_confirmed:
        failure_reason = "runtime_remains_blocked_after_recovery"
    else:
        failure_reason = None
    execution = {
        "recovery_execution_id": execution_id,
        "recovery_plan_id": plan["recovery_plan_id"],
        "started_at": started_at,
        "completed_at": _now(),
        "target_component": plan["target_component"],
        "contract": CONTRACT_VERSION,
        "result": "COMPLETED" if completed else "FAILED",
        "failure_reason": failure_reason,
        "target_recovered": target_recovered,
        "target_status_before": target_status_before,
        "target_status_after": (
            target_step.get("published_status")
            if target_step
            else {
                "status": "PASS" if target_recovered else "FAIL",
                "runtime_readiness": after_readiness,
                "runtime_health": after_health,
            }
        ),
        "evidence": {
            "registered_procedure": plan["procedure_source"],
            "allowed_steps": plan["allowed_steps"],
            "recovery_checkpoint_result": checkpoint_result,
            "component_recovery_executions": component_executions,
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
    response = {
        "recovery_execution": execution,
        "recovery_plan": plan,
        "supervisor_reevaluated": reevaluation_executed,
        "supervisor_reevaluation_executed": reevaluation_executed,
        "supervisor_recovery_confirmed": supervisor_recovery_confirmed,
    }
    if completed:
        return _pass(**response)
    return _fail(
        failure_reason or "runtime_recovery_failed",
        "Runtime root recovery did not restore the target and Runtime to READY / HEALTHY",
        **response,
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


def _normalize_target_component(value: str) -> str | None:
    requested = str(value or "").strip()
    if not requested or requested.lower() == "runtime":
        return "Runtime"
    by_lower = {name.lower(): name for name in ROOT_COMPONENTS}
    return by_lower.get(requested.lower())


def _recovery_sequence(target_component: str) -> list[str]:
    if target_component == "Runtime":
        return list(ROOT_COMPONENTS)
    start = ROOT_COMPONENTS.index(target_component)
    return list(ROOT_COMPONENTS[start:])


def _resolve_initializer(component: str) -> Callable[..., dict[str, Any]]:
    module_name, function_name, _ = _COMPONENT_INITIALIZERS[component]
    module = importlib.import_module(module_name)
    initializer = getattr(module, function_name)
    if not callable(initializer):
        raise RuntimeRecoveryError(f"recovery_initializer_not_callable:{component}")
    return initializer


def _recover_component(component: str) -> dict[str, Any]:
    _, _, strategy = _COMPONENT_INITIALIZERS[component]
    started_at = _now()
    try:
        initializer = _resolve_initializer(component)
        if component == "Runtime Snapshot":
            result = initializer()
            status = str(result.get("status") or "").upper()
            recovered = status in {"PASS", "OK", "DEGRADED"} and bool(result.get("snapshot_id") or result.get("result") or result.get("snapshot"))
        else:
            result = initializer(force=True)
            status = str(result.get("status") or "").upper()
            recovered = status == "PASS" and result.get("loaded") is True
        return {
            "runtime_component": component,
            "strategy": strategy,
            "status": "PASS" if recovered else "FAIL",
            "failure_reason": None if recovered else str(result.get("failure_reason") or "component_recovery_readback_failed"),
            "started_at": started_at,
            "completed_at": _now(),
            "published_status": _published_status_evidence(result),
        }
    except Exception as exc:
        return {
            "runtime_component": component,
            "strategy": strategy,
            "status": "FAIL",
            "failure_reason": "component_recovery_execution_failed",
            "started_at": started_at,
            "completed_at": _now(),
            "error": f"{type(exc).__name__}: {exc}",
        }


def _read_target_status(
    component: str,
    before_readiness: dict[str, Any],
    before_health: dict[str, Any],
) -> dict[str, Any]:
    if component == "Runtime":
        return {
            "status": before_readiness.get("status"),
            "runtime_readiness": before_readiness.get("runtime_readiness"),
            "runtime_health": before_health.get("runtime_health"),
            "supervisor_evaluation_id": before_readiness.get("supervisor_evaluation_id"),
        }
    try:
        if component == "Runtime Snapshot":
            module = importlib.import_module("app.assistant_runtime.observability")
            result = module.get_runtime_snapshot(refresh=False)
        else:
            result = _resolve_initializer(component)(force=False)
        return _published_status_evidence(result)
    except Exception as exc:
        return {
            "status": "FAIL",
            "failure_reason": "target_status_readback_failed",
            "error": f"{type(exc).__name__}: {exc}",
        }


def _published_status_evidence(result: dict[str, Any]) -> dict[str, Any]:
    fields = (
        "status",
        "runtime_component",
        "release_id",
        "contract",
        "loaded",
        "verification_status",
        "execution_status",
        "orchestrator_status",
        "session_runtime_status",
        "supervisor_status",
        "recovery_status",
        "registry_status",
        "graph_status",
        "observability_status",
        "health_status",
        "connection_status",
        "runtime_readiness",
        "runtime_health",
        "snapshot_id",
        "generated_at",
        "overall_status",
        "failure_reason",
        "error",
    )
    return {field: deepcopy(result.get(field)) for field in fields if field in result}


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
