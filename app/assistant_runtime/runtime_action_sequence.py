"""VECTRA-RUNTIME-ACTION-SEQUENCE-001.

Server-side execution of bounded, registered Runtime operation sequences.
This is an orchestration mechanism inside the existing Runtime and facade; it
is not a new GPT Action or a new repository architecture.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List
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

RELEASE_ID = "VECTRA-RUNTIME-ACTION-SEQUENCE-001"
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
    sequence_id = str(payload.get("sequence_id") or f"RAS-{uuid.uuid4().hex[:12].upper()}")
    steps = payload.get("steps") if isinstance(payload.get("steps"), list) else DEFAULT_MEMORY_CAPITALIZATION_SEQUENCE
    steps = [str(step).strip() for step in steps if str(step).strip()]
    if not steps:
        return {"status": "FAIL", "release": RELEASE_ID, "reason": "steps_required"}

    context: Dict[str, Any] = {"sequence_id": sequence_id}
    registry = _registry(payload, context)
    unknown = [step for step in steps if step not in registry]
    if unknown:
        return {
            "status": "FAIL",
            "release": RELEASE_ID,
            "sequence_id": sequence_id,
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
    persistence = _save(store)

    return {
        "status": "PASS" if completed else "FAIL",
        "verification_status": "PASS" if completed else "FAIL",
        "release": RELEASE_ID,
        "sequence_id": sequence_id,
        "program_type": record["program_type"],
        "sequence_status": record["status"],
        "steps_requested_count": len(steps),
        "steps_completed_count": len(results),
        "runtime_initialization_count": 1,
        "runtime_reinitialized_between_steps": False,
        "scenario_context_preserved": True,
        "transport_action_calls_required": 1,
        "steps": results,
        "state_persistence": persistence,
        "next_action": "Laboratory may declare RESEARCH_CYCLE_CLOSED only after reviewing every PASS result." if completed else "Inspect the failed step and retry with the same sequence_id after correcting its input.",
    }


def get_registered_action_sequence(payload: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    sequence_id = str(payload.get("sequence_id") or "").strip()
    if not sequence_id:
        return {"status": "FAIL", "release": RELEASE_ID, "reason": "sequence_id_required"}
    store = _load()
    record = store.get("sequences", {}).get(sequence_id)
    return {
        "status": "PASS" if isinstance(record, dict) else "NOT_FOUND",
        "release": RELEASE_ID,
        "sequence_id": sequence_id,
        "sequence": deepcopy(record) if isinstance(record, dict) else None,
        "runtime_state_restored": isinstance(record, dict),
    }
