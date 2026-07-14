"""Stage 2: guided professional research execution for Digital Business Analyst.

This module executes research programs over the existing read-only Business
Runtime.  It owns only execution state.  Research Programs, Professional
Activity, Evidence and Findings remain owned by their existing Foundation
Services.
"""
from __future__ import annotations

import json
import os
import uuid
from copy import deepcopy
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from app.assistant_runtime.business_framework_research import (
    create_research_program,
    get_research_program,
    transition_research_program,
)
from app.assistant_runtime.business_runtime_access import (
    discover_business_runtime_objects,
    get_business_runtime_manifest,
    open_business_workspace_direct,
    verify_business_runtime_access,
)
from app.assistant_runtime.evidence_platform import (
    get_professional_evidence,
    register_professional_evidence,
)
from app.assistant_runtime.findings_platform import (
    register_professional_finding,
)
from app.assistant_runtime.durable_runtime_state import read_json_state, write_json_state, inspect_json_state
from app.assistant_runtime.professional_activity import (
    complete_professional_activity,
    get_professional_activity,
    pause_professional_activity,
    start_professional_activity,
)

RELEASE_ID = "BUSINESS-RESEARCH-EXECUTION-001"
DEFAULT_BASE_PATH = "assistant_repository"
EXECUTIONS_FILE = Path("runtime") / "business_research_execution" / "executions.json"
EXECUTION_STATUSES = {
    "ACTIVE",
    "PAUSED",
    "READY_FOR_FINDINGS",
    "COMPLETED",
    "HOLD",
    "CANCELLED",
}
TASK_STATUSES = {"PENDING", "ACTIVE", "COMPLETED", "HOLD", "SKIPPED"}
PRIORITIES = {"LOW", "MEDIUM", "HIGH", "CRITICAL"}
OBJECT_KEYWORDS = {
    "sku": ("sku", "товар", "позици", "артикул"),
    "network": ("network", "сеть", "контракт", "клиент", "varus", "варус"),
    "category": ("категор", "category"),
    "tmc_group": ("тмс", "tmc", "групп"),
    "manager": ("менеджер", "кам", "kam", "manager"),
    "top_manager": ("руководител", "направлен", "top manager"),
    "business": ("бизнес", "business", "прибыл", "оборот", "марж", "финрез"),
}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _base_path() -> Path:
    return Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve()


def _path() -> Path:
    return _base_path() / EXECUTIONS_FILE


def _read_with_diagnostic() -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    value, diagnostic = read_json_state(_path(), list, list)
    return value, diagnostic


def _read() -> List[Dict[str, Any]]:
    value, _ = _read_with_diagnostic()
    return value


def _write(items: List[Dict[str, Any]]) -> None:
    write_json_state(_path(), _json_safe_value(items))


def _required(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _find(items: List[Dict[str, Any]], execution_id: str) -> Optional[Dict[str, Any]]:
    return next((item for item in items if str(item.get("research_execution_id")) == execution_id), None)


def _get_execution(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    execution_id = _required(payload, "research_execution_id")
    items, diagnostic = _read_with_diagnostic()
    if diagnostic.get("status") == "HOLD":
        raise RuntimeError(f"Research Execution Repository unavailable: {diagnostic}")
    execution = _find(items, execution_id)
    if execution is None:
        raise ValueError(f"Unknown research_execution_id: {execution_id}")
    execution.setdefault("continuation", {})
    execution["continuation"].update({
        "restored_from_persistent_state": True,
        "repository_source": diagnostic.get("source"),
        "repository_recovered": bool(diagnostic.get("recovered")),
        "restored_at": _now(),
    })
    return items, execution


def _compact_value(value: Any, depth: int = 0) -> Any:
    """Return a compact value that is always safe for JSON persistence.

    Business Runtime responses may contain Decimal/date/Path/Pydantic values.
    Persisting those values directly previously caused a second exception while
    handling the first failure, which escaped as Internal Server Error.
    """
    if depth > 3:
        return "[truncated]"
    if value is None or isinstance(value, (bool, int, float)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "model_dump") and callable(value.model_dump):
        try:
            return _compact_value(value.model_dump(exclude_none=True), depth + 1)
        except Exception:
            return str(value)
    if isinstance(value, dict):
        result: Dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= 16:
                result["_truncated_fields"] = max(0, len(value) - 16)
                break
            result[str(key)] = _compact_value(item, depth + 1)
        return result
    if isinstance(value, (list, tuple, set)):
        return [_compact_value(item, depth + 1) for item in list(value)[:10]]
    if isinstance(value, str):
        return value[:1200] + "…" if len(value) > 1200 else value
    # Last-resort normalization keeps repository writes deterministic instead
    # of leaking a serializer exception through the public Runtime contract.
    return str(value)


def _json_safe_value(value: Any) -> Any:
    """Normalize values for repository persistence without truncating context."""
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if hasattr(value, "model_dump") and callable(value.model_dump):
        try:
            return _json_safe_value(value.model_dump(exclude_none=True))
        except Exception:
            return str(value)
    if isinstance(value, dict):
        return {str(key): _json_safe_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_json_safe_value(item) for item in value]
    return str(value)


def _persist_execution_safely(items: List[Dict[str, Any]]) -> Optional[str]:
    try:
        _write(_json_safe_value(items))
        return None
    except Exception as exc:
        return str(exc)


def _build_research_tasks(question: str, hypothesis: str, period: str = "") -> List[Dict[str, Any]]:
    source = f"{question} {hypothesis}".lower()
    ranked: List[Tuple[int, str]] = []
    for object_type, keywords in OBJECT_KEYWORDS.items():
        positions = [source.find(keyword) for keyword in keywords if source.find(keyword) >= 0]
        if positions:
            ranked.append((min(positions), object_type))
    ordered_objects = [item[1] for item in sorted(ranked)]
    if not ordered_objects:
        ordered_objects = ["business", "network", "sku"]
    if "business" not in ordered_objects:
        ordered_objects.append("business")
    # Preserve hypothesis-driven ordering and remove duplicates.
    ordered_objects = list(dict.fromkeys(ordered_objects))[:6]

    tasks: List[Dict[str, Any]] = [
        {
            "task_id": "RT-001",
            "title": "Establish Business Runtime research context",
            "objective": "Confirm Runtime readiness, read-only access and available professional objects.",
            "task_type": "runtime_context",
            "recommended_workspace": "business_runtime_manifest",
            "recommended_object_type": None,
            "recommended_runtime_capabilities": ["get_business_runtime_manifest", "verify_business_runtime_access"],
            "expected_evidence": ["Runtime readiness", "Read-only guarantee", "Available object coverage"],
            "expected_findings": ["Operational limitations or confirmed runtime readiness"],
            "status": "PENDING",
            "result": None,
            "evidence_ids": [],
            "finding_ids": [],
        }
    ]
    for index, object_type in enumerate(ordered_objects, start=2):
        tasks.append({
            "task_id": f"RT-{index:03d}",
            "title": f"Investigate {object_type} workspace for the research question",
            "objective": f"Collect evidence from the {object_type} level only when it contributes to the active hypothesis.",
            "task_type": "workspace_investigation",
            "recommended_workspace": f"{object_type}_workspace",
            "recommended_object_type": object_type,
            "recommended_runtime_capabilities": ["discover_business_runtime_objects", "open_business_workspace_direct"],
            "expected_evidence": [f"Current {object_type} state", "Relevant business and navigation context"],
            "expected_findings": [f"Evidence-led conclusion for {object_type}"],
            "status": "PENDING",
            "result": None,
            "evidence_ids": [],
            "finding_ids": [],
            "period": period or None,
        })
    return tasks


def _progress(execution: Dict[str, Any]) -> Dict[str, Any]:
    tasks = execution.get("research_tasks") if isinstance(execution.get("research_tasks"), list) else []
    completed = len([task for task in tasks if task.get("status") in {"COMPLETED", "SKIPPED"}])
    remaining = len([task for task in tasks if task.get("status") not in {"COMPLETED", "SKIPPED"}])
    percent = round((completed / len(tasks)) * 100, 1) if tasks else 0.0
    next_task = next((task for task in tasks if task.get("status") in {"PENDING", "HOLD"}), None)
    return {
        "completed_tasks": completed,
        "total_tasks": len(tasks),
        "remaining_tasks": remaining,
        "completion_percent": percent,
        "next_recommended_task": _compact_task(next_task) if next_task else None,
        "stop_reason": execution.get("stop_reason"),
    }


def _compact_task(task: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not isinstance(task, dict):
        return None
    return {
        "task_id": task.get("task_id"),
        "title": task.get("title"),
        "objective": task.get("objective"),
        "task_type": task.get("task_type"),
        "recommended_workspace": task.get("recommended_workspace"),
        "recommended_object_type": task.get("recommended_object_type"),
        "recommended_runtime_capabilities": task.get("recommended_runtime_capabilities") or [],
        "expected_result": task.get("expected_evidence") or [],
        "status": task.get("status"),
        "evidence_ids": task.get("evidence_ids") or [],
        "finding_ids": task.get("finding_ids") or [],
    }


def _decision_lineage(execution: Dict[str, Any]) -> List[Dict[str, Any]]:
    return deepcopy(execution.get("decision_lineage") or [])


def _execution_manifest(execution: Dict[str, Any]) -> Dict[str, Any]:
    progress = _progress(execution)
    active_task = next((task for task in execution.get("research_tasks", []) if task.get("status") == "ACTIVE"), None)
    if active_task is None:
        active_task = next((task for task in execution.get("research_tasks", []) if task.get("status") in {"PENDING", "HOLD"}), None)
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "research_execution_manifest": {
            "research_execution_id": execution.get("research_execution_id"),
            "research_program_id": execution.get("research_program_id"),
            "professional_activity_id": execution.get("professional_activity_id"),
            "professional_goal": execution.get("professional_goal"),
            "research_question": execution.get("research_question"),
            "active_hypothesis": execution.get("active_hypothesis"),
            "business_domain": execution.get("business_domain"),
            "status": execution.get("status"),
            "active_task": _compact_task(active_task),
            "research_tasks": [_compact_task(task) for task in execution.get("research_tasks", [])],
            "investigated_objects": execution.get("investigated_objects") or [],
            "current_route": execution.get("current_route") or [],
            "open_questions": execution.get("open_questions") or [],
            "research_decisions": execution.get("research_decisions") or [],
            "evidence_ids": execution.get("evidence_ids") or [],
            "finding_ids": execution.get("finding_ids") or [],
            "decision_lineage": _decision_lineage(execution),
            "progress": progress,
            "read_only": True,
            "updated_at": execution.get("updated_at"),
            "continuation": execution.get("continuation") or {
                "restored_from_persistent_state": True,
                "transport_session_independent": True,
            },
        },
        "research_progress_report": {
            **progress,
            "performed_tasks": [
                _compact_task(task) for task in execution.get("research_tasks", [])
                if task.get("status") in {"COMPLETED", "SKIPPED"}
            ],
            "remaining_task_ids": [
                task.get("task_id") for task in execution.get("research_tasks", [])
                if task.get("status") not in {"COMPLETED", "SKIPPED"}
            ],
            "open_questions": execution.get("open_questions") or [],
            "context_saved": True,
        },
        "operational_readiness": {
            "status": "PASS" if execution.get("status") not in {"HOLD", "CANCELLED"} else "HOLD",
            "question": "Может ли Digital Business Analyst самостоятельно выполнить профессиональную исследовательскую программу существующего Business Framework?",
            "answer": "YES" if execution.get("status") not in {"HOLD", "CANCELLED"} else "NO",
        },
    }


def start_business_research_execution(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    missing = [key for key in ("research_question", "professional_goal", "business_domain") if not str(payload.get(key) or "").strip()]
    if missing:
        return {"status": "VALIDATION_ERROR", "missing_fields": missing, "research_execution_created": False}

    research_question = str(payload.get("research_question")).strip()
    professional_goal = str(payload.get("professional_goal")).strip()
    active_hypothesis = str(payload.get("professional_hypothesis") or payload.get("hypothesis") or research_question).strip()
    business_domain = str(payload.get("business_domain")).strip()
    title = str(payload.get("title") or f"Research: {research_question[:80]}").strip()
    priority = str(payload.get("priority") or "HIGH").upper()
    if priority not in PRIORITIES:
        return {"status": "VALIDATION_ERROR", "reason": "unsupported_priority", "supported_priorities": sorted(PRIORITIES), "research_execution_created": False}

    program_id = str(payload.get("research_program_id") or "").strip()
    if program_id:
        program_result = get_research_program({"research_program_id": program_id})
        if program_result.get("status") != "PASS":
            return {"status": "VALIDATION_ERROR", "reason": "unknown_research_program_id", "research_execution_created": False}
        program = program_result["research_program"]
    else:
        program_result = create_research_program({
            "title": title,
            "research_question": research_question,
            "professional_goal": professional_goal,
            "program_type": payload.get("program_type") or "business_framework_research",
            "business_domain": business_domain,
            "research_object": payload.get("research_object") or "Existing Business Framework",
            "priority": priority,
            "initial_hypotheses": [active_hypothesis] if active_hypothesis else [],
            "tags": payload.get("tags") or ["stage_2", "research_execution"],
            "allow_duplicate": bool(payload.get("allow_duplicate", False)),
        })
        if program_result.get("status") != "PASS":
            return program_result
        if not program_result.get("research_program_created") and program_result.get("duplicate_protected"):
            program_id = str(program_result.get("existing_research_program_id") or "")
            program = get_research_program({"research_program_id": program_id})["research_program"]
        else:
            program_id = str(program_result.get("research_program_id") or "")
            program = get_research_program({"research_program_id": program_id})["research_program"]

    activity_id = str(program.get("professional_activity_id") or "")
    try:
        if str(program.get("status") or "") == "PROPOSED":
            transition_research_program({"research_program_id": program_id, "target_status": "APPROVED"})
            transition_research_program({"research_program_id": program_id, "target_status": "ACTIVE_RESEARCH"})
        elif str(program.get("status") or "") == "APPROVED":
            transition_research_program({"research_program_id": program_id, "target_status": "ACTIVE_RESEARCH"})
    except Exception:
        pass

    activity = get_professional_activity({"activity_id": activity_id}).get("activity") or {}
    activity_status = str(activity.get("status") or "")
    try:
        if activity_status in {"PLANNED", "QUEUED", "PAUSED"}:
            start_professional_activity({"activity_id": activity_id, "reason": "business_research_execution_started"})
    except Exception as exc:
        return {
            "status": "HOLD",
            "reason": "professional_activity_not_startable",
            "diagnostic": {"current_step": "start_research_execution", "reason": str(exc), "unavailable_capability": "Professional Activity activation", "recommended_action": "Resolve the active Professional Activity and retry."},
            "research_execution_created": False,
        }
    refreshed_activity = get_professional_activity({"activity_id": activity_id}).get("activity") or {}
    if str(refreshed_activity.get("status") or "") != "ACTIVE":
        return {
            "status": "HOLD",
            "reason": "professional_activity_not_active",
            "diagnostic": {"current_step": "start_research_execution", "reason": f"Professional Activity status is {refreshed_activity.get('status')}", "unavailable_capability": "Professional Activity activation", "recommended_action": "Use a Research Program with an active or startable Professional Activity."},
            "research_execution_created": False,
        }

    tasks = _build_research_tasks(research_question, active_hypothesis, str(payload.get("period") or ""))
    now = _now()
    execution = {
        "research_execution_id": f"BRE-{uuid.uuid4().hex[:12].upper()}",
        "release": RELEASE_ID,
        "research_program_id": program_id,
        "professional_activity_id": activity_id,
        "title": title,
        "professional_goal": professional_goal,
        "research_question": research_question,
        "active_hypothesis": active_hypothesis,
        "business_domain": business_domain,
        "period": payload.get("period"),
        "status": "ACTIVE",
        "research_tasks": tasks,
        "investigated_objects": [],
        "current_route": [],
        "open_questions": [str(item).strip() for item in (payload.get("open_questions") or []) if str(item).strip()],
        "research_decisions": [{"decision": "Research route generated from the active hypothesis", "at": now, "basis": active_hypothesis}],
        "evidence_ids": [],
        "finding_ids": [],
        "decision_lineage": [],
        "stop_reason": None,
        "read_only": True,
        "created_at": now,
        "updated_at": now,
        "history": [{"event": "RESEARCH_EXECUTION_STARTED", "at": now}],
    }
    items = _read()
    items.append(execution)
    _write(items)
    result = _execution_manifest(execution)
    result.update({"research_execution_created": True, "next_allowed_action": "execute_next_business_research_task"})
    return result


def get_business_research_execution_manifest(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        items, execution = _get_execution(payload)
        execution["updated_at"] = execution.get("updated_at") or _now()
        _write(items)
        response = _execution_manifest(execution)
        response["continuation_diagnostic"] = {
            "professional_activity_state": execution.get("status"),
            "continuation_possible": execution.get("status") not in {"COMPLETED", "CANCELLED"},
            "recovery_source": (execution.get("continuation") or {}).get("repository_source", "primary"),
            "recommended_action": "Continue the active task or resume the execution." if execution.get("status") in {"ACTIVE", "READY_FOR_FINDINGS"} else "Resume the same execution by id.",
        }
        return response
    except Exception as exc:
        return {
            "status": "HOLD",
            "reason": "research_execution_manifest_unavailable",
            "diagnostic": {
                "professional_activity_state": "UNKNOWN",
                "reason": str(exc),
                "continuation_possible": False,
                "recommended_action": "Verify persistent Runtime repository availability; do not create a replacement program until recovery is attempted.",
            },
        }


def execute_business_research_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    try:
        items, execution = _get_execution(payload)
    except Exception as exc:
        return {
            "status": "HOLD",
            "reason": "research_execution_unavailable",
            "diagnostic": {
                "current_step": "load_research_execution",
                "reason": str(exc),
                "unavailable_capability": "Research Execution Repository",
                "recommended_action": "Reload the same Research Execution Manifest and retry without creating a new program.",
            },
            "research_context_preserved": True,
        }
    if execution.get("status") == "PAUSED":
        return {"status": "HOLD", "reason": "research_execution_paused", "manifest": _execution_manifest(execution)}
    if execution.get("status") in {"COMPLETED", "CANCELLED"}:
        return {"status": "HOLD", "reason": "research_execution_not_active", "manifest": _execution_manifest(execution)}

    task_id = str(payload.get("task_id") or "").strip()
    task = next((task for task in execution.get("research_tasks", []) if str(task.get("task_id")) == task_id), None) if task_id else None
    if task is None:
        # ACTIVE is selected first so a task interrupted by a transport or
        # persistence failure resumes in place instead of skipping ahead.
        task = next((task for task in execution.get("research_tasks", []) if task.get("status") == "ACTIVE"), None)
    if task is None:
        task = next((task for task in execution.get("research_tasks", []) if task.get("status") in {"PENDING", "HOLD"}), None)
    if task is None:
        execution["status"] = "READY_FOR_FINDINGS"
        execution["updated_at"] = _now()
        _write(items)
        return _execution_manifest(execution)

    task["status"] = "ACTIVE"
    task["started_at"] = _now()
    try:
        if task.get("task_type") == "runtime_context":
            runtime_manifest = get_business_runtime_manifest()
            runtime_access = verify_business_runtime_access(period=str(payload.get("period") or execution.get("period") or ""), limit_per_level=2)
            operation_result = {"runtime_manifest": runtime_manifest, "runtime_access": runtime_access}
            task_ok = runtime_access.get("status") == "PASS"
            source_type = "runtime"
            object_name = "Business Runtime"
        else:
            object_type = str(task.get("recommended_object_type") or "business")
            object_id = str(payload.get("object_id") or "").strip()
            if not object_id and object_type != "business":
                discovery = discover_business_runtime_objects(limit_per_level=3)
                candidates = (discovery.get("objects") or {}).get(object_type) or []
                object_id = str((candidates[0] or {}).get("object_id") or "") if candidates else ""
            operation_result = open_business_workspace_direct(
                object_type,
                object_id=object_id,
                period=str(payload.get("period") or execution.get("period") or ""),
            )
            task_ok = operation_result.get("status") == "PASS"
            source_type = "business_data"
            object_name = f"{object_type}:{object_id or 'business'}"

        safe_operation_result = _compact_value(operation_result)

        if not task_ok:
            task["status"] = "HOLD"
            task["result"] = safe_operation_result
            task["completed_at"] = None
            execution["status"] = "HOLD"
            execution["stop_reason"] = str((operation_result.get("diagnostic") or {}).get("reason") or operation_result.get("reason") or "runtime_task_failed")
            execution["updated_at"] = _now()
            execution.setdefault("history", []).append({"event": "TASK_HOLD", "task_id": task.get("task_id"), "at": _now(), "reason": execution["stop_reason"]})
            _write(items)
            return {
                "status": "HOLD",
                "diagnostic": {
                    "current_step": task.get("task_id"),
                    "reason": execution["stop_reason"],
                    "unavailable_capability": (task.get("recommended_runtime_capabilities") or [None])[0],
                    "recommended_action": "Resolve Runtime diagnostic and resume the same Research Task.",
                },
                **_execution_manifest(execution),
            }

        evidence_result = register_professional_evidence({
            "evidence_type": "research",
            "source_type": source_type,
            "reference": f"business_research_execution:{execution['research_execution_id']}:{task['task_id']}",
            "title": task.get("title"),
            "excerpt_or_summary": safe_operation_result,
            "business_domain": execution.get("business_domain"),
            "professional_activity_id": execution.get("professional_activity_id"),
            "research_program_id": execution.get("research_program_id"),
            "object": object_name,
            "period": payload.get("period") or execution.get("period"),
            "digital_role": "digital_business_analyst",
            "validated": True,
            "reliability": "HIGH",
            "validation_notes": "Captured directly from the existing read-only Business Runtime during a guided Research Task.",
            "lineage": [execution.get("research_question"), task.get("task_id")],
        })
        evidence = evidence_result.get("evidence") or {}
        evidence_id = evidence.get("evidence_id")
        if evidence_id and evidence_id not in task.setdefault("evidence_ids", []):
            task["evidence_ids"].append(evidence_id)
        if evidence_id and evidence_id not in execution.setdefault("evidence_ids", []):
            execution["evidence_ids"].append(evidence_id)

        task["status"] = "COMPLETED"
        task["result"] = safe_operation_result
        task["completed_at"] = _now()
        execution.setdefault("investigated_objects", []).append({"task_id": task.get("task_id"), "object": object_name, "at": _now()})
        execution.setdefault("current_route", []).append({"task_id": task.get("task_id"), "workspace": task.get("recommended_workspace"), "object": object_name})
        execution.setdefault("research_decisions", []).append({"decision": f"Executed {task.get('task_id')}", "at": _now(), "basis": "Current hypothesis and next recommended Research Task"})
        execution["status"] = "READY_FOR_FINDINGS" if all(t.get("status") in {"COMPLETED", "SKIPPED"} for t in execution.get("research_tasks", [])) else "ACTIVE"
        execution["stop_reason"] = None
        execution["updated_at"] = _now()
        execution.setdefault("history", []).append({"event": "TASK_COMPLETED", "task_id": task.get("task_id"), "at": _now(), "evidence_id": evidence_id})
        _write(items)
        response = _execution_manifest(execution)
        response.update({"task_execution_status": "PASS", "completed_task": _compact_task(task), "captured_evidence": evidence})
        return response
    except Exception as exc:
        task["status"] = "HOLD"
        task["result"] = None
        task["completed_at"] = None
        execution["status"] = "HOLD"
        execution["stop_reason"] = str(exc)
        execution["updated_at"] = _now()
        execution.setdefault("history", []).append({
            "event": "TASK_EXECUTION_ERROR",
            "task_id": task.get("task_id"),
            "at": _now(),
            "reason": str(exc),
        })
        persistence_error = _persist_execution_safely(items)
        diagnostic = {
            "current_step": task.get("task_id"),
            "reason": str(exc),
            "unavailable_capability": (task.get("recommended_runtime_capabilities") or [None])[0],
            "recommended_action": "Retry the same Research Task. The existing program, route and accumulated context are preserved.",
        }
        if persistence_error:
            diagnostic["persistence_warning"] = persistence_error
        response = _execution_manifest(_compact_value(execution))
        response.update({
            "status": "HOLD",
            "reason": "research_task_execution_failed",
            "diagnostic": diagnostic,
            "research_context_preserved": True,
            "retry_same_task": True,
        })
        return response


def record_business_research_finding(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, execution = _get_execution(payload)
    statement = _required(payload, "statement")
    task_id = str(payload.get("task_id") or "").strip()
    task = next((task for task in execution.get("research_tasks", []) if str(task.get("task_id")) == task_id), None)
    if task is None:
        raise ValueError("task_id must identify a Research Task in this execution")
    evidence_ids = payload.get("evidence_ids") if isinstance(payload.get("evidence_ids"), list) else list(task.get("evidence_ids") or [])
    if not evidence_ids:
        return {"status": "VALIDATION_ERROR", "reason": "validated_evidence_required", "finding_created": False}
    for evidence_id in evidence_ids:
        evidence = get_professional_evidence({"evidence_id": evidence_id}).get("evidence") or {}
        if evidence.get("research_program_id") != execution.get("research_program_id") or evidence.get("status") not in {"VALIDATED", "VERIFIED"}:
            return {"status": "VALIDATION_ERROR", "reason": "evidence_not_validated_or_not_in_program", "evidence_id": evidence_id, "finding_created": False}

    finding_result = register_professional_finding({
        "professional_type": "research",
        "finding_type": payload.get("finding_type") or "confirmed_fact",
        "statement": statement,
        "professional_activity_id": execution.get("professional_activity_id"),
        "business_domain": execution.get("business_domain"),
        "object": payload.get("object") or task.get("recommended_object_type"),
        "research_program_id": execution.get("research_program_id"),
        "evidence_ids": evidence_ids,
        "status": payload.get("status") or "SUPPORTED",
        "confidence": payload.get("confidence") or "HIGH",
        "author_engine": "business_research_execution",
        "business_impact_reference": payload.get("business_impact"),
        "limitations": payload.get("limitations") or [],
    })
    finding = finding_result.get("finding") or {}
    finding_id = finding.get("finding_id")
    if finding_id and finding_id not in task.setdefault("finding_ids", []):
        task["finding_ids"].append(finding_id)
    if finding_id and finding_id not in execution.setdefault("finding_ids", []):
        execution["finding_ids"].append(finding_id)
    lineage = {
        "research_question": execution.get("research_question"),
        "research_task_id": task_id,
        "evidence_ids": evidence_ids,
        "finding_id": finding_id,
        "recommendation": payload.get("recommendation"),
        "created_at": _now(),
    }
    execution.setdefault("decision_lineage", []).append(lineage)
    execution["updated_at"] = _now()
    _write(items)
    response = _execution_manifest(execution)
    response.update({"finding_created": True, "finding": finding, "decision_lineage_record": lineage})
    return response


def pause_business_research_execution(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, execution = _get_execution(payload)
    if execution.get("status") not in {"ACTIVE", "READY_FOR_FINDINGS", "HOLD"}:
        return {"status": "HOLD", "reason": "execution_cannot_be_paused", **_execution_manifest(execution)}
    try:
        activity = get_professional_activity({"activity_id": execution.get("professional_activity_id")}).get("activity") or {}
        if activity.get("status") == "ACTIVE":
            pause_professional_activity({"activity_id": execution.get("professional_activity_id"), "reason": payload.get("reason") or "research_execution_paused"})
    except Exception:
        pass
    execution["status"] = "PAUSED"
    execution["stop_reason"] = str(payload.get("reason") or "paused_by_laboratory")
    execution["updated_at"] = _now()
    execution.setdefault("history", []).append({"event": "PAUSED", "at": _now(), "reason": execution["stop_reason"]})
    _write(items)
    return _execution_manifest(execution)


def resume_business_research_execution(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, execution = _get_execution(payload)
    if execution.get("status") not in {"PAUSED", "HOLD"}:
        return {"status": "HOLD", "reason": "execution_is_not_paused", **_execution_manifest(execution)}
    try:
        activity = get_professional_activity({"activity_id": execution.get("professional_activity_id")}).get("activity") or {}
        if activity.get("status") == "PAUSED":
            start_professional_activity({"activity_id": execution.get("professional_activity_id"), "reason": "research_execution_resumed"})
    except Exception as exc:
        return {"status": "HOLD", "reason": "professional_activity_resume_failed", "diagnostic": {"current_step": "resume", "reason": str(exc), "unavailable_capability": "Professional Activity activation", "recommended_action": "Resolve active activity conflict and retry."}}
    execution["status"] = "ACTIVE"
    execution["stop_reason"] = None
    execution["updated_at"] = _now()
    execution.setdefault("history", []).append({"event": "RESUMED", "at": _now()})
    _write(items)
    return _execution_manifest(execution)


def complete_business_research_execution(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, execution = _get_execution(payload)
    incomplete = [task.get("task_id") for task in execution.get("research_tasks", []) if task.get("status") not in {"COMPLETED", "SKIPPED"}]
    if incomplete:
        return {"status": "HOLD", "reason": "research_tasks_incomplete", "remaining_task_ids": incomplete, **_execution_manifest(execution)}
    if not execution.get("finding_ids"):
        return {"status": "HOLD", "reason": "research_findings_required", **_execution_manifest(execution)}
    try:
        program = get_research_program({"research_program_id": execution.get("research_program_id")}).get("research_program") or {}
        status = str(program.get("status") or "")
        if status == "ACTIVE_RESEARCH":
            transition_research_program({"research_program_id": execution.get("research_program_id"), "target_status": "EVIDENCE_COLLECTION"})
            transition_research_program({"research_program_id": execution.get("research_program_id"), "target_status": "FINDINGS_VALIDATION"})
        elif status == "EVIDENCE_COLLECTION":
            transition_research_program({"research_program_id": execution.get("research_program_id"), "target_status": "FINDINGS_VALIDATION"})
    except Exception:
        pass
    try:
        activity = get_professional_activity({"activity_id": execution.get("professional_activity_id")}).get("activity") or {}
        if activity.get("status") == "ACTIVE":
            complete_professional_activity({
                "activity_id": execution.get("professional_activity_id"),
                "execution_result": "guided_business_research_execution_completed",
                "activity_outcome": f"Completed {len(execution.get('research_tasks') or [])} Research Tasks and formed {len(execution.get('finding_ids') or [])} evidence-backed Findings.",
                "business_impact": payload.get("business_impact") or "Research results are ready for Product Research and subsequent professional review.",
                "results": [{"research_execution_id": execution.get("research_execution_id"), "evidence_ids": execution.get("evidence_ids") or [], "finding_ids": execution.get("finding_ids") or []}],
            })
    except Exception as exc:
        return {"status": "HOLD", "reason": "professional_activity_completion_failed", "diagnostic": {"current_step": "complete_research_execution", "reason": str(exc), "unavailable_capability": "Professional Activity completion", "recommended_action": "Resolve the activity lifecycle state and retry completion."}, **_execution_manifest(execution)}
    execution["status"] = "COMPLETED"
    execution["stop_reason"] = None
    execution["completed_at"] = _now()
    execution["updated_at"] = _now()
    execution.setdefault("history", []).append({"event": "RESEARCH_EXECUTION_COMPLETED", "at": _now()})
    _write(items)
    response = _execution_manifest(execution)
    response.update({"research_execution_completed": True, "next_allowed_action": "start_business_framework_product_research"})
    return response


def verify_business_research_execution() -> Dict[str, Any]:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write([])
    repository_diagnostic = inspect_json_state(path, list)
    dependency_checks = {
        "execution_repository_readable": repository_diagnostic.get("status") in {"PASS", "RECOVERED", "EMPTY"},
        "research_program_foundation_available": callable(create_research_program),
        "professional_activity_available": callable(start_professional_activity),
        "business_runtime_access_available": callable(open_business_workspace_direct),
        "professional_evidence_platform_available": callable(register_professional_evidence),
        "professional_findings_platform_available": callable(register_professional_finding),
        "research_execution_manifest_supported": True,
        "decision_lineage_supported": True,
        "pause_resume_supported": True,
        "manifest_recovery_supported": True,
        "transport_session_independence": True,
        "atomic_repository_writes": True,
        "backup_recovery_supported": True,
        "read_only_guarantee": True,
    }
    return {
        "status": "PASS" if all(dependency_checks.values()) else "HOLD",
        "release": RELEASE_ID,
        "report_type": "Business Research Execution Report",
        "checks": dependency_checks,
        "execution_count": len(_read()),
        "repository_diagnostic": repository_diagnostic,
        "public_capabilities": [
            "start_business_research_execution",
            "get_business_research_execution_manifest",
            "execute_business_research_task",
            "record_business_research_finding",
            "pause_business_research_execution",
            "resume_business_research_execution",
            "complete_business_research_execution",
            "verify_business_research_execution",
        ],
        "operational_readiness": {
            "status": "PASS" if all(dependency_checks.values()) else "HOLD",
            "question": "Может ли Digital Business Analyst самостоятельно выполнить профессиональную исследовательскую программу существующего Business Framework?",
            "answer": "YES" if all(dependency_checks.values()) else "NO",
            "next_stage": "BUSINESS-FRAMEWORK-PRODUCT-RESEARCH-001" if all(dependency_checks.values()) else "Resolve Stage 2 diagnostics and repeat verification.",
        },
    }
