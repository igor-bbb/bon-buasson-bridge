from __future__ import annotations

import ast
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
)
from app.assistant_runtime.verification_runtime import (
    get_latest_runtime_verification,
    initialize_verification_runtime,
)

RELEASE_ID = "VECTRA-EXECUTION-RUNTIME-001"
RESULTS_PATH = Path("runtime/execution_runtime/execution_results.json")


class ExecutionRuntimeError(RuntimeError):
    """Controlled Execution Runtime error."""


class ExecutionRepository:
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
                    "repository_id": "VECTRA-EXECUTION-REPOSITORY-001",
                    "release_id": RELEASE_ID,
                    "schema_version": "1.0",
                    "executions": [],
                    "updated_at": _now(),
                }
                self._persist()
            else:
                self._data = json.loads(self.path.read_text(encoding="utf-8"))
                self._validate(self._data)
            return deepcopy(self._data)

    def save(self, execution: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            data = self._require_loaded()
            execution_id = execution["execution_id"]
            for index, current in enumerate(data["executions"]):
                if current.get("execution_id") == execution_id:
                    data["executions"][index] = deepcopy(execution)
                    break
            else:
                data["executions"].append(deepcopy(execution))
            data["updated_at"] = _now()
            self._persist()
            return deepcopy(execution)

    def get(self, execution_id: str) -> dict[str, Any] | None:
        for execution in self.executions():
            if execution.get("execution_id") == execution_id:
                return execution
        return None

    def executions(self) -> list[dict[str, Any]]:
        return deepcopy(self._require_loaded()["executions"])

    def state(self) -> dict[str, Any]:
        data = self._require_loaded()
        executions = data["executions"]
        return {
            "repository_id": data["repository_id"],
            "executions_count": len(executions),
            "last_execution_id": executions[-1]["execution_id"] if executions else None,
            "loaded": True,
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

    @staticmethod
    def _validate(data: dict[str, Any]) -> None:
        if not isinstance(data.get("executions"), list):
            raise ExecutionRuntimeError("execution_results_invalid")


_REPOSITORY = ExecutionRepository()


def initialize_execution_runtime(*, force: bool = False) -> dict[str, Any]:
    registry = initialize_architecture_registry_runtime(force=force)
    verification = initialize_verification_runtime(force=force)
    repository = _REPOSITORY.load(force=force)
    return {
        "status": "PASS",
        "execution_status": "READY",
        "runtime_component": "Execution Runtime",
        "release_id": RELEASE_ID,
        "loaded": True,
        "load_order": "AFTER_VERIFICATION_RUNTIME",
        "architecture_registry_loaded": registry.get("loaded") is True,
        "architecture_registry_id": registry.get("registry_id"),
        "verification_runtime_loaded": verification.get("loaded") is True,
        "verification_repository_id": verification.get("verification_repository_id"),
        "execution_repository_id": repository["repository_id"],
        "executions_count": len(repository["executions"]),
        "error": None,
    }


def start_execution(payload: dict[str, Any]) -> dict[str, Any]:
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        return _fail("object_id_required", "object_id is required")
    registry_response = get_architecture_object({"object_id": object_id})
    if registry_response.get("status") != "PASS":
        return registry_response

    architecture_object = registry_response["object"]
    execution_mapping = _execution_mapping(architecture_object)
    if not execution_mapping["steps"]:
        return _fail("execution_mapping_unavailable", "Registered object has no executable implementation paths")

    latest_verification = get_latest_runtime_verification(object_id)
    verification_required = execution_mapping["verification_required"]
    verification_status = latest_verification.get("verification_status") if latest_verification else None
    ready = not verification_required or verification_status == "PASS"
    execution_id = str(payload.get("execution_id") or f"EXE-{uuid4().hex[:12].upper()}")
    now = _now()
    execution = {
        "execution_id": execution_id,
        "object_id": object_id,
        "execution_mapping": execution_mapping,
        "runtime_source": "ArchitectureRegistryRuntime.get_architecture_object",
        "timestamp": now,
        "started_at": now,
        "finished_at": None,
        "execution_status": "READY" if ready else "BLOCKED",
        "execution_result": None,
        "execution_history": [_event("START", "READY" if ready else "BLOCKED", None)],
        "execution_log": [
            "Execution Mapping resolved through Architecture Registry.",
            f"Verification gate: {'PASS' if ready else 'BLOCKED'}."
        ],
        "verification_gate": {
            "required": verification_required,
            "verification_execution_id": latest_verification.get("execution_id") if latest_verification else None,
            "verification_status": verification_status,
            "status": "PASS" if ready else "FAIL",
        },
        "repeat_of": payload.get("repeat_of"),
        "error": None if ready else {
            "code": "verification_pass_required",
            "message": "A published PASS result from Verification Runtime is required before execution.",
        },
    }
    return _REPOSITORY.save(execution)


def run_execution(payload: dict[str, Any]) -> dict[str, Any]:
    if payload.get("repeat") is True or payload.get("repeat_execution_id"):
        repeat_id = str(payload.get("repeat_execution_id") or payload.get("execution_id") or "").strip()
        return repeat_execution({"execution_id": repeat_id})
    execution_id = str(payload.get("execution_id") or "").strip()
    execution = _REPOSITORY.get(execution_id) if execution_id else None
    if execution is None:
        execution = start_execution(payload)
        if execution.get("status") == "FAIL":
            return execution
    if execution.get("execution_status") == "BLOCKED":
        return deepcopy(execution)
    if execution.get("execution_status") == "COMPLETED":
        return _fail("execution_already_completed", "Execution is already completed; start a repeated execution instead")

    execution["execution_status"] = "RUNNING"
    execution["execution_history"].append(_event("RUN", "RUNNING", None))
    execution["execution_log"].append("Deterministic execution started.")
    _REPOSITORY.save(execution)

    step_results: list[dict[str, Any]] = []
    final_status = "COMPLETED"
    error = None
    for step in execution["execution_mapping"]["steps"]:
        result = _execute_step(step)
        step_results.append(result)
        execution["execution_history"].append(_event(step["step_id"], result["status"], result.get("reason")))
        execution["execution_log"].append(f"{step['step_id']}: {result['status']}")
        if result["status"] == "FAIL":
            final_status = "FAILED"
            error = {
                "code": "execution_step_failed",
                "message": f"Execution stopped safely at {step['step_id']}: {result.get('reason')}",
            }
            break

    execution["execution_status"] = final_status
    execution["finished_at"] = _now()
    execution["execution_result"] = {
        "status": "PASS" if final_status == "COMPLETED" else "FAIL",
        "steps_total": len(execution["execution_mapping"]["steps"]),
        "steps_executed": len(step_results),
        "steps_passed": sum(item["status"] == "PASS" for item in step_results),
        "steps_failed": sum(item["status"] == "FAIL" for item in step_results),
        "step_results": step_results,
        "safe_termination": True,
    }
    execution["error"] = error
    execution["execution_history"].append(_event("FINISH", final_status, error and error["code"]))
    execution["execution_log"].append(f"Execution finished with status {final_status}.")
    return _REPOSITORY.save(execution)


def repeat_execution(payload: dict[str, Any]) -> dict[str, Any]:
    source_id = str(payload.get("execution_id") or "").strip()
    source = _REPOSITORY.get(source_id)
    if source is None:
        return _fail("execution_not_found", "Execution was not found")
    started = start_execution({"object_id": source["object_id"], "repeat_of": source_id})
    if started.get("execution_status") != "READY":
        return started
    return run_execution({"execution_id": started["execution_id"]})


def get_execution_status(payload: dict[str, Any]) -> dict[str, Any]:
    execution_id = str(payload.get("execution_id") or "").strip()
    execution = _REPOSITORY.get(execution_id)
    if execution is None:
        return _fail("execution_not_found", "Execution was not found")
    return _pass(
        execution_id=execution_id,
        object_id=execution["object_id"],
        execution_status=execution["execution_status"],
        started_at=execution["started_at"],
        finished_at=execution["finished_at"],
        execution_result=deepcopy(execution.get("execution_result")),
        verification_gate=deepcopy(execution.get("verification_gate")),
        repeat_of=execution.get("repeat_of"),
    )


def get_execution_history(payload: dict[str, Any]) -> dict[str, Any]:
    execution_id = str(payload.get("execution_id") or "").strip()
    execution = _REPOSITORY.get(execution_id)
    if execution is None:
        return _fail("execution_not_found", "Execution was not found")
    return _pass(
        execution_id=execution_id,
        object_id=execution["object_id"],
        execution_status=execution["execution_status"],
        execution_history=deepcopy(execution["execution_history"]),
        execution_log=deepcopy(execution["execution_log"]),
        started_at=execution["started_at"],
        finished_at=execution["finished_at"],
    )


def search_execution_results(payload: dict[str, Any]) -> dict[str, Any]:
    criteria = {key: value for key, value in payload.items() if key in {
        "execution_id", "object_id", "execution_status", "runtime_source", "repeat_of"
    } and value not in (None, "")}
    if not criteria:
        return _fail("search_criteria_required", "At least one execution search criterion is required")
    results = []
    for execution in _REPOSITORY.executions():
        if all(str(value).casefold() in str(execution.get(key, "")).casefold() for key, value in criteria.items()):
            results.append(_summary(execution))
    return _pass(results_count=len(results), results=results, filters=criteria)


def execute_execution_runtime_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    operations = {
        "get_execution_runtime_status": lambda _: initialize_execution_runtime(),
        "start_execution": start_execution,
        "run_execution": run_execution,
        "get_execution_status": get_execution_status,
        "get_execution_history": get_execution_history,
        "search_execution_results": search_execution_results,
    }
    handler = operations.get(operation_type)
    if handler is None:
        return _fail("unsupported_execution_runtime_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload)


def _execution_mapping(architecture_object: dict[str, Any]) -> dict[str, Any]:
    implementation = deepcopy(architecture_object.get("implementation") or {})
    verification = deepcopy(architecture_object.get("verification") or {})
    paths = [str(path) for path in implementation.get("paths") or []]
    steps = [
        {
            "step_id": f"STEP-{index:03d}",
            "operation": "VALIDATE_REGISTERED_IMPLEMENTATION",
            "path": path,
        }
        for index, path in enumerate(paths, start=1)
    ]
    return {
        "source_object_id": architecture_object["object_id"],
        "runtime_mapping": implementation.get("runtime_mapping"),
        "steps": steps,
        "verification_required": bool(verification.get("status")),
        "verification_mapping_status": verification.get("status"),
    }


def _execute_step(step: dict[str, Any]) -> dict[str, Any]:
    path = Path(step["path"])
    if not path.exists() or not path.is_file():
        return {**step, "status": "FAIL", "reason": "registered_implementation_path_not_found"}
    try:
        content = path.read_text(encoding="utf-8")
        if path.suffix == ".py":
            ast.parse(content, filename=str(path))
    except (OSError, UnicodeError, SyntaxError) as exc:
        return {**step, "status": "FAIL", "reason": f"registered_implementation_invalid:{type(exc).__name__}"}
    return {
        **step,
        "status": "PASS",
        "reason": None,
        "result": {
            "path_exists": True,
            "syntax_valid": True if path.suffix == ".py" else None,
            "bytes_read": len(content.encode("utf-8")),
        },
    }


def _event(step_id: str, status: str, reason: str | None) -> dict[str, Any]:
    return {"step_id": step_id, "status": status, "timestamp": _now(), "reason": reason}


def _summary(execution: dict[str, Any]) -> dict[str, Any]:
    return {
        "execution_id": execution.get("execution_id"),
        "object_id": execution.get("object_id"),
        "execution_status": execution.get("execution_status"),
        "timestamp": execution.get("timestamp"),
        "started_at": execution.get("started_at"),
        "finished_at": execution.get("finished_at"),
        "repeat_of": execution.get("repeat_of"),
    }


def _pass(**kwargs: Any) -> dict[str, Any]:
    return {"status": "PASS", **kwargs, "error": None}


def _fail(code: str, message: str) -> dict[str, Any]:
    return {"status": "FAIL", "execution_status": "FAILED", "error": {"code": code, "message": message}}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
