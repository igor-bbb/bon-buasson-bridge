"""VECTRA-RUNTIME-ACTION-SEQUENCE-COMPACT-RESPONSE-001.

Server-side execution of bounded, registered Runtime operation sequences.
This is an orchestration mechanism inside the existing Runtime and facade; it
is not a new GPT Action or a new repository architecture.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Iterable
import uuid

from app.assistant_runtime.durable_runtime_state import read_json_state, write_json_state
from app.assistant_runtime.memory_repository import (
    get_memory_object,
    get_memory_overview,
    list_memory_objects,
    readback_memory_object,
    verify_memory_repository_integrity,
)
from app.assistant_runtime.knowledge_capitalization import (
    auto_capitalize_confirmed_knowledge,
    get_professional_knowledge,
)

RELEASE_ID = "VECTRA-RUNTIME-ACTION-SEQUENCE-COMPACT-RESPONSE-001"

SUPPORTED_RESPONSE_MODES = {"compact", "step_summary", "diagnostic"}
DEFAULT_RESPONSE_MODE = "compact"


def _response_mode(payload: Dict[str, Any]) -> str:
    mode = str(payload.get("response_mode") or DEFAULT_RESPONSE_MODE).strip().lower()
    return mode if mode in SUPPORTED_RESPONSE_MODES else DEFAULT_RESPONSE_MODE


def _walk_values(value: Any) -> Iterable[tuple[str, Any]]:
    if isinstance(value, dict):
        for key, item in value.items():
            yield str(key), item
            yield from _walk_values(item)
    elif isinstance(value, list):
        for item in value:
            yield from _walk_values(item)


def _collect_ids(record: Dict[str, Any], keys: set[str]) -> List[str]:
    values: List[str] = []
    for key, value in _walk_values(record):
        if key not in keys:
            continue
        candidates = value if isinstance(value, list) else [value]
        for candidate in candidates:
            if isinstance(candidate, (str, int)) and str(candidate).strip():
                text = str(candidate).strip()
                if text not in values:
                    values.append(text)
    return values


def _step_summary(step: Dict[str, Any]) -> Dict[str, Any]:
    result = step.get("runtime_result") if isinstance(step.get("runtime_result"), dict) else {}
    operation_type = str(step.get("operation_type") or "")
    summary: Dict[str, Any] = {}
    if operation_type == "get_memory_overview":
        for key in ("objects_count", "total_objects", "mapping_errors_count", "integrity_errors_count"):
            if key in result:
                summary[key] = result[key]
    elif operation_type == "list_memory_objects":
        objects = result.get("objects") if isinstance(result.get("objects"), list) else []
        summary["objects_count"] = result.get("objects_count", len(objects))
    elif operation_type == "read_professional_knowledge":
        knowledge = result.get("knowledge")
        if isinstance(knowledge, list):
            summary["knowledge_count"] = len(knowledge)
        elif isinstance(knowledge, dict):
            summary["knowledge_count"] = 1
    elif operation_type == "capitalize_confirmed_knowledge":
        summary["capitalized_knowledge_ids"] = _collect_ids(result, {"knowledge_id", "capitalized_knowledge_ids"})
        summary["created_object_ids"] = _collect_ids(result, {"created_object_id", "created_object_ids", "object_id"})
    elif operation_type in {"read_memory_object", "verify_memory_object_readback"}:
        summary["readback_status"] = result.get("readback_status") or result.get("verification_status") or step.get("status")
        object_ids = _collect_ids(result, {"object_id"})
        if object_ids:
            summary["object_id"] = object_ids[0]
    elif operation_type == "verify_memory_repository":
        summary["memory_integrity_status"] = result.get("verification_status") or result.get("integrity_status") or step.get("status")
    return {
        "step_number": step.get("index"),
        "operation_type": operation_type,
        "status": step.get("status"),
        "verification_status": result.get("verification_status") or step.get("status"),
        "result_summary": summary,
    }


def _project_sequence(record: Dict[str, Any], response_mode: str, *, restored: bool = False) -> Dict[str, Any]:
    steps = record.get("steps_completed") if isinstance(record.get("steps_completed"), list) else []
    requested = record.get("steps_requested") if isinstance(record.get("steps_requested"), list) else []
    passed = sum(1 for step in steps if str(step.get("status") or "").upper() == "PASS")
    failed = sum(1 for step in steps if str(step.get("status") or "").upper() == "FAIL")
    failed_record = next((step for step in steps if str(step.get("status") or "").upper() == "FAIL"), None)
    completed = str(record.get("status") or "").upper() == "COMPLETED"
    capitalized_ids = _collect_ids(record.get("context", {}), {"knowledge_ids", "knowledge_id", "capitalized_knowledge_ids"})
    created_ids = _collect_ids(record, {"created_object_id", "created_object_ids"})
    updated_ids = _collect_ids(record, {"updated_object_id", "updated_object_ids"})
    if not created_ids:
        created_ids = _collect_ids(record.get("context", {}), {"object_id"})

    readback_step = next((s for s in reversed(steps) if s.get("operation_type") == "verify_memory_object_readback"), None)
    integrity_step = next((s for s in reversed(steps) if s.get("operation_type") == "verify_memory_repository"), None)
    readback_result = readback_step.get("runtime_result", {}) if isinstance(readback_step, dict) else {}
    integrity_result = integrity_step.get("runtime_result", {}) if isinstance(integrity_step, dict) else {}
    error_result = failed_record.get("runtime_result", {}) if isinstance(failed_record, dict) else {}
    error = None
    failed_step = None
    if failed_record:
        failed_step = {"step_number": failed_record.get("index"), "operation_type": failed_record.get("operation_type")}
        error = {
            "code": str(error_result.get("code") or error_result.get("reason") or "SEQUENCE_STEP_FAILED"),
            "message": str(error_result.get("message") or error_result.get("detail") or error_result.get("reason") or "Runtime sequence step failed"),
        }

    compact = {
        "status": "PASS" if completed else "FAIL",
        "verification_status": "PASS" if completed else "FAIL",
        "response_mode": response_mode,
        "sequence_id": record.get("sequence_id"),
        "sequence_status": record.get("status"),
        "program_type": record.get("program_type"),
        "steps_requested_count": len(requested),
        "steps_completed_count": len(steps),
        "steps_passed_count": passed,
        "steps_failed_count": failed,
        "runtime_initialization_count": record.get("runtime_initialization_count", 1),
        "runtime_reinitialized_between_steps": False,
        "scenario_context_preserved": True,
        "capitalized_knowledge_ids": capitalized_ids,
        "created_object_ids": created_ids,
        "updated_object_ids": updated_ids,
        "readback_status": readback_result.get("readback_status") or readback_result.get("verification_status") or ("PASS" if completed and readback_step else None),
        "memory_integrity_status": integrity_result.get("verification_status") or integrity_result.get("integrity_status") or ("PASS" if completed and integrity_step else None),
        "failed_step": failed_step,
        "error": error,
    }
    if restored:
        compact["runtime_state_restored"] = True
    if response_mode == "step_summary":
        compact["steps"] = [_step_summary(step) for step in steps]
    elif response_mode == "diagnostic":
        compact["sequence"] = deepcopy(record)
    return compact
STATE_PATH = Path(__file__).resolve().parents[2] / "runtime" / "action_sequences" / "runtime_action_sequences.json"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _load() -> Dict[str, Any]:
    value, _ = read_json_state(STATE_PATH, lambda: {"sequences": {}}, dict)
    value.setdefault("sequences", {})
    return value


def _save(value: Dict[str, Any]) -> Dict[str, Any]:
    return write_json_state(STATE_PATH, value)


def _status_ok(result: Dict[str, Any]) -> bool:
    return str(result.get("status") or "").upper() in {"OK", "PASS", "COMPLETED", "SUCCESS"} or str(result.get("verification_status") or "").upper() == "PASS"


def _knowledge_ids(payload: Dict[str, Any]) -> List[str]:
    package = payload.get("prepared_knowledge_package") if isinstance(payload.get("prepared_knowledge_package"), dict) else {}
    items = package.get("professional_knowledge") if isinstance(package.get("professional_knowledge"), list) else []
    return [str(item.get("knowledge_id")) for item in items if isinstance(item, dict) and item.get("knowledge_id")]


def _registry(payload: Dict[str, Any], context: Dict[str, Any]) -> Dict[str, Callable[[], Dict[str, Any]]]:
    domain = str(payload.get("domain") or "bonboason")
    limit = int(payload.get("limit") or 100)

    def capitalize() -> Dict[str, Any]:
        result = auto_capitalize_confirmed_knowledge(payload)
        context["capitalization"] = deepcopy(result)
        ids = _knowledge_ids(payload)
        context["knowledge_ids"] = ids
        return result

    def read_memory() -> Dict[str, Any]:
        object_id = str(payload.get("object_id") or context.get("object_id") or "")
        if object_id:
            return get_memory_object(object_id=object_id, domain=domain)
        ids = context.get("knowledge_ids") or _knowledge_ids(payload)
        if not ids:
            return {"status": "FAIL", "reason": "object_id_or_capitalized_knowledge_id_required"}
        result = readback_memory_object(knowledge_id=str(ids[0]), domain=domain)
        obj = result.get("memory_object") if isinstance(result, dict) else None
        if isinstance(obj, dict) and obj.get("object_id"):
            context["object_id"] = obj["object_id"]
        return result

    def verify_readback() -> Dict[str, Any]:
        object_id = str(payload.get("object_id") or context.get("object_id") or "")
        ids = context.get("knowledge_ids") or _knowledge_ids(payload)
        return readback_memory_object(
            object_id=object_id or None,
            knowledge_id=None if object_id else (str(ids[0]) if ids else None),
            domain=domain,
        )

    return {
        "get_memory_overview": lambda: get_memory_overview(domain=domain),
        "list_memory_objects": lambda: list_memory_objects(memory_space=payload.get("memory_space"), domain=domain, limit=limit),
        "read_professional_knowledge": lambda: get_professional_knowledge(payload.get("knowledge_id")),
        "capitalize_confirmed_knowledge": capitalize,
        "read_memory_object": read_memory,
        "verify_memory_object_readback": verify_readback,
        "verify_memory_repository": lambda: verify_memory_repository_integrity(domain=domain),
    }


DEFAULT_MEMORY_CAPITALIZATION_SEQUENCE = [
    "get_memory_overview",
    "list_memory_objects",
    "read_professional_knowledge",
    "capitalize_confirmed_knowledge",
    "read_memory_object",
    "verify_memory_object_readback",
    "verify_memory_repository",
]


def execute_registered_action_sequence(payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    response_mode = _response_mode(payload)
    sequence_id = str(payload.get("sequence_id") or f"RAS-{uuid.uuid4().hex[:12].upper()}")
    steps = payload.get("steps") if isinstance(payload.get("steps"), list) else DEFAULT_MEMORY_CAPITALIZATION_SEQUENCE
    steps = [str(step).strip() for step in steps if str(step).strip()]
    if not steps:
        return {"status": "FAIL", "release": RELEASE_ID, "reason": "steps_required", "response_mode": response_mode}

    existing = _load().get("sequences", {}).get(sequence_id)
    if isinstance(existing, dict) and str(existing.get("status") or "").upper() == "COMPLETED":
        result = _project_sequence(existing, response_mode, restored=True)
        result["sequence_reused"] = True
        return result

    context: Dict[str, Any] = {"sequence_id": sequence_id}
    registry = _registry(payload, context)
    unknown = [step for step in steps if step not in registry]
    if unknown:
        return {
            "status": "FAIL",
            "release": RELEASE_ID,
            "sequence_id": sequence_id,
            "response_mode": response_mode,
            "reason": "unsupported_registered_operation",
            "unsupported_operations": unknown,
            "registered_operations": sorted(registry),
        }

    store = _load()
    initialized_at = _now()
    record = {
        "sequence_id": sequence_id,
        "release": RELEASE_ID,
        "program_type": str(payload.get("program_type") or "knowledge_capitalization"),
        "status": "RUNNING",
        "initialized_at": initialized_at,
        "updated_at": initialized_at,
        "runtime_initialization_count": 1,
        "steps_requested": steps,
        "steps_completed": [],
        "context": deepcopy(context),
        "request_payload": deepcopy(payload),
    }
    store["sequences"][sequence_id] = record
    _save(store)

    results: List[Dict[str, Any]] = []
    for index, operation_type in enumerate(steps, start=1):
        result = registry[operation_type]()
        passed = _status_ok(result)
        step_result = {
            "index": index,
            "operation_type": operation_type,
            "status": "PASS" if passed else "FAIL",
            "runtime_result": deepcopy(result),
            "executed_at": _now(),
        }
        results.append(step_result)
        record["steps_completed"] = deepcopy(results)
        record["context"] = deepcopy(context)
        record["updated_at"] = _now()
        record["status"] = "RUNNING" if passed else "FAILED"
        store = _load()
        store["sequences"][sequence_id] = deepcopy(record)
        _save(store)
        if not passed:
            break

    completed = len(results) == len(steps) and all(item["status"] == "PASS" for item in results)
    record["status"] = "COMPLETED" if completed else "FAILED"
    record["completed_at"] = _now() if completed else None
    record["updated_at"] = _now()
    store = _load()
    store["sequences"][sequence_id] = deepcopy(record)
    _save(store)

    projected = _project_sequence(record, response_mode)
    projected["release"] = RELEASE_ID
    projected["transport_action_calls_required"] = 1
    projected["next_action"] = (
        "Laboratory may declare RESEARCH_CYCLE_CLOSED only after reviewing every PASS result."
        if completed else
        "Inspect the failed step and retry with the same sequence_id after correcting its input."
    )
    return projected


def get_registered_action_sequence(payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    response_mode = _response_mode(payload)
    sequence_id = str(payload.get("sequence_id") or "").strip()
    if not sequence_id:
        return {"status": "FAIL", "release": RELEASE_ID, "reason": "sequence_id_required", "response_mode": response_mode}
    store = _load()
    record = store.get("sequences", {}).get(sequence_id)
    if not isinstance(record, dict):
        return {
            "status": "NOT_FOUND",
            "verification_status": "FAIL",
            "release": RELEASE_ID,
            "response_mode": response_mode,
            "sequence_id": sequence_id,
            "runtime_state_restored": False,
            "sequence": None,
        }
    projected = _project_sequence(record, response_mode, restored=True)
    projected["release"] = RELEASE_ID
    if response_mode in {"compact", "step_summary"}:
        sequence_fields = {k: v for k, v in projected.items() if k not in {"status", "verification_status", "runtime_state_restored", "release", "response_mode"}}
        sequence_fields["status"] = projected.get("sequence_status")
        return {
            "status": projected["status"],
            "verification_status": projected["verification_status"],
            "release": RELEASE_ID,
            "response_mode": response_mode,
            "runtime_state_restored": True,
            "sequence": sequence_fields,
        }
    return projected

