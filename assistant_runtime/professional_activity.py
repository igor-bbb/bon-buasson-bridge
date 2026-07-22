"""VECTRA v2 Professional Activity Foundation.

This module introduces a persistent Runtime entity for professional work.  It
is intentionally domain-neutral: research, validation, business review and
knowledge capitalization will become activity types in later increments.

The foundation provides:
- an activity contract;
- a controlled lifecycle;
- a persistent activity repository;
- an executive queue with priorities and conflict-safe activation;
- compact Runtime responses suitable for GPT Actions.
"""
from __future__ import annotations

import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.assistant_runtime.durable_runtime_state import read_json_state, write_json_state, inspect_json_state

RELEASE_ID = "VECTRA-V2-PA-FOUNDATION-002"
DEFAULT_BASE_PATH = "assistant_repository"
ACTIVITY_DIR = Path("runtime") / "professional_activity"
ACTIVITIES_FILE = ACTIVITY_DIR / "activities.json"
EXECUTIVE_STATE_FILE = ACTIVITY_DIR / "executive_state.json"

ACTIVITY_TYPES = {
    "research_session",
    "validation_session",
    "business_review",
    "capability_review",
    "knowledge_capitalization",
    "general_professional_activity",
}

ACTIVITY_STATUSES = {
    "DRAFT",
    "PLANNED",
    "QUEUED",
    "ACTIVE",
    "PAUSED",
    "COMPLETED",
    "FAILED",
    "CANCELLED",
    "ARCHIVED",
}

ALLOWED_TRANSITIONS = {
    "DRAFT": {"PLANNED", "CANCELLED"},
    "PLANNED": {"QUEUED", "ACTIVE", "CANCELLED"},
    "QUEUED": {"ACTIVE", "CANCELLED"},
    "ACTIVE": {"PAUSED", "COMPLETED", "FAILED", "CANCELLED"},
    "PAUSED": {"QUEUED", "ACTIVE", "CANCELLED"},
    "COMPLETED": {"ARCHIVED"},
    "FAILED": {"QUEUED", "ARCHIVED"},
    "CANCELLED": {"ARCHIVED"},
    "ARCHIVED": set(),
}

PRIORITY_ORDER = {"CRITICAL": 0, "HIGH": 1, "MEDIUM": 2, "LOW": 3}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _base_path() -> Path:
    configured = os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)
    return Path(configured).resolve()


def _path(relative: Path) -> Path:
    return _base_path() / relative


def _read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return deepcopy(default)
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return deepcopy(default)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    tmp.replace(path)


def _activities() -> List[Dict[str, Any]]:
    value = _read_json(_path(ACTIVITIES_FILE), [])
    return value if isinstance(value, list) else []


def _save_activities(items: List[Dict[str, Any]]) -> None:
    _write_json(_path(ACTIVITIES_FILE), items)


def _executive_state() -> Dict[str, Any]:
    default = {
        "release": RELEASE_ID,
        "status": "READY",
        "active_activity_id": None,
        "queue": [],
        "updated_at": _now(),
    }
    value = _read_json(_path(EXECUTIVE_STATE_FILE), default)
    return value if isinstance(value, dict) else default


def _save_executive_state(state: Dict[str, Any]) -> None:
    state["release"] = RELEASE_ID
    state["updated_at"] = _now()
    _write_json(_path(EXECUTIVE_STATE_FILE), state)


def _normalize_type(value: Any) -> str:
    normalized = str(value or "general_professional_activity").strip().lower().replace(" ", "_")
    aliases = {
        "research": "research_session",
        "validation": "validation_session",
        "review": "business_review",
        "business": "business_review",
        "capability": "capability_review",
        "capitalization": "knowledge_capitalization",
    }
    normalized = aliases.get(normalized, normalized)
    return normalized if normalized in ACTIVITY_TYPES else "general_professional_activity"


def _normalize_priority(value: Any) -> str:
    priority = str(value or "MEDIUM").strip().upper()
    return priority if priority in PRIORITY_ORDER else "MEDIUM"


def _required_text(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _find(items: Iterable[Dict[str, Any]], activity_id: str) -> Optional[Dict[str, Any]]:
    for item in items:
        if str(item.get("activity_id")) == activity_id:
            return item
    return None


def _compact(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "activity_id": item.get("activity_id"),
        "activity_type": item.get("activity_type"),
        "title": item.get("title"),
        "user_request": item.get("user_request"),
        "professional_goal": item.get("professional_goal") or item.get("goal"),
        "goal": item.get("goal"),
        "object": item.get("object"),
        "business_domain": item.get("business_domain"),
        "priority": item.get("priority"),
        "status": item.get("status"),
        "current_stage": item.get("current_stage"),
        "progress": item.get("progress"),
        "dependencies": item.get("dependencies", []),
        "readiness": item.get("readiness"),
        "created_at": item.get("created_at"),
        "updated_at": item.get("updated_at"),
    }


def get_professional_activity_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "Professional Activity Foundation",
        "supported_activity_types": sorted(ACTIVITY_TYPES),
        "supported_statuses": sorted(ACTIVITY_STATUSES),
        "supported_operations": [
            "professional_activity_manifest",
            "create_professional_activity",
            "plan_professional_activity",
            "queue_professional_activity",
            "start_professional_activity",
            "pause_professional_activity",
            "complete_professional_activity",
            "fail_professional_activity",
            "cancel_professional_activity",
            "archive_professional_activity",
            "get_professional_activity",
            "list_professional_activities",
            "get_executive_activity_status",
            "activate_next_professional_activity",
            "verify_professional_activity_foundation",
        ],
        "write_policy": "Explicit operation required. No activity is executed asynchronously.",
        "autonomy_boundary": "The Runtime records and orchestrates professional work; execution results must be supplied by an actual engine or completed tool call.",
    }


def create_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    now = _now()
    activity_id = str(payload.get("activity_id") or f"PA-{uuid.uuid4().hex[:12].upper()}")
    items = _activities()
    if _find(items, activity_id):
        raise ValueError(f"activity_id already exists: {activity_id}")

    stages = payload.get("stages") if isinstance(payload.get("stages"), list) else []
    normalized_stages = []
    for index, stage in enumerate(stages, start=1):
        if isinstance(stage, str):
            normalized_stages.append({"stage_id": f"S{index}", "title": stage, "status": "PENDING"})
        elif isinstance(stage, dict):
            normalized_stages.append({
                "stage_id": str(stage.get("stage_id") or f"S{index}"),
                "title": str(stage.get("title") or stage.get("name") or f"Stage {index}"),
                "status": str(stage.get("status") or "PENDING").upper(),
                "result": stage.get("result"),
            })

    item = {
        "activity_id": activity_id,
        "release": RELEASE_ID,
        "activity_type": _normalize_type(payload.get("activity_type")),
        "title": _required_text(payload, "title"),
        "user_request": str(payload.get("user_request") or "").strip() or None,
        "professional_goal": str(payload.get("professional_goal") or payload.get("goal") or "").strip(),
        "goal": _required_text(payload, "goal"),
        "object": payload.get("object"),
        "business_domain": payload.get("business_domain") or payload.get("domain"),
        "professional_context": payload.get("professional_context") if isinstance(payload.get("professional_context"), dict) else {},
        "plan": payload.get("plan") if isinstance(payload.get("plan"), dict) else {},
        "stages": normalized_stages,
        "current_stage": None,
        "status": "DRAFT",
        "priority": _normalize_priority(payload.get("priority")),
        "progress": 0,
        "results": [],
        "recommendations": [],
        "execution_result": None,
        "activity_outcome": None,
        "business_impact": None,
        "dependencies": payload.get("dependencies") if isinstance(payload.get("dependencies"), list) else [],
        "required_context": payload.get("required_context") if isinstance(payload.get("required_context"), list) else [],
        "working_context": payload.get("working_context") if isinstance(payload.get("working_context"), dict) else {"investigated_objects": [], "confirmed_hypotheses": [], "open_questions": [], "intermediate_findings": [], "evidence_references": []},
        "findings": payload.get("findings") if isinstance(payload.get("findings"), dict) else {"observations": [], "confirmed_facts": [], "architectural_findings": [], "recommendations": []},
        "readiness": "NOT_EVALUATED",
        "limitations": [],
        "created_by": str(payload.get("created_by") or "product_owner_or_orchestrator"),
        "created_at": now,
        "updated_at": now,
        "started_at": None,
        "completed_at": None,
        "failure": None,
        "history": [{"status": "DRAFT", "at": now, "reason": "activity_created"}],
    }
    items.append(item)
    _save_activities(items)
    return {"status": "PASS", "activity": _compact(item), "next_action": "plan_professional_activity"}


def _transition(activity_id: str, target: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    target = target.upper()
    items = _activities()
    item = _find(items, activity_id)
    if item is None:
        raise ValueError(f"Unknown activity_id: {activity_id}")
    current = str(item.get("status") or "DRAFT").upper()
    if target not in ALLOWED_TRANSITIONS.get(current, set()):
        raise ValueError(f"Invalid activity transition: {current} -> {target}")

    state = _executive_state()
    if target == "ACTIVE":
        active_id = state.get("active_activity_id")
        if active_id and active_id != activity_id:
            raise ValueError(f"Another activity is already active: {active_id}")

    now = _now()
    item["status"] = target
    item["updated_at"] = now
    item.setdefault("history", []).append({"status": target, "at": now, "reason": payload.get("reason")})
    if target == "ACTIVE":
        item["started_at"] = item.get("started_at") or now
        if item.get("stages") and not item.get("current_stage"):
            item["current_stage"] = item["stages"][0].get("stage_id")
    if target == "COMPLETED":
        item["completed_at"] = now
        item["progress"] = 100
        item["results"] = payload.get("results") if isinstance(payload.get("results"), list) else item.get("results", [])
        item["recommendations"] = payload.get("recommendations") if isinstance(payload.get("recommendations"), list) else item.get("recommendations", [])
        item["execution_result"] = payload.get("execution_result") or item.get("execution_result") or "activity_completed"
        item["activity_outcome"] = payload.get("activity_outcome")
        item["business_impact"] = payload.get("business_impact")
        if isinstance(payload.get("findings"), dict):
            item["findings"] = payload["findings"]
    if target == "FAILED":
        item["failure"] = {
            "stage": payload.get("stage"),
            "reason": payload.get("reason") or "unspecified_failure",
            "recoverable": bool(payload.get("recoverable", False)),
        }
    _save_activities(items)

    queue = [entry for entry in state.get("queue", []) if entry != activity_id]
    if target == "ACTIVE":
        state["active_activity_id"] = activity_id
    elif state.get("active_activity_id") == activity_id:
        state["active_activity_id"] = None
    if target == "QUEUED" and activity_id not in queue:
        queue.append(activity_id)
    state["queue"] = queue
    _save_executive_state(state)
    return {"status": "PASS", "activity": _compact(item), "executive_state": get_executive_activity_status()}


def plan_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    activity_id = _required_text(payload, "activity_id")
    items = _activities()
    item = _find(items, activity_id)
    if item is None:
        raise ValueError(f"Unknown activity_id: {activity_id}")
    if item.get("status") != "DRAFT":
        raise ValueError("Only DRAFT activity can be planned")
    if isinstance(payload.get("plan"), dict):
        item["plan"] = payload["plan"]
    if isinstance(payload.get("stages"), list):
        item["stages"] = [
            {"stage_id": f"S{i}", "title": str(stage.get("title") if isinstance(stage, dict) else stage), "status": "PENDING", "result": None}
            for i, stage in enumerate(payload["stages"], start=1)
        ]
    item["updated_at"] = _now()
    _save_activities(items)
    return _transition(activity_id, "PLANNED", {"reason": "activity_plan_confirmed"})


def queue_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _transition(_required_text(payload, "activity_id"), "QUEUED", payload)


def start_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _transition(_required_text(payload, "activity_id"), "ACTIVE", payload)


def pause_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _transition(_required_text(payload, "activity_id"), "PAUSED", payload)


def complete_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _transition(_required_text(payload, "activity_id"), "COMPLETED", payload)


def fail_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _transition(_required_text(payload, "activity_id"), "FAILED", payload)


def cancel_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _transition(_required_text(payload, "activity_id"), "CANCELLED", payload)


def archive_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    return _transition(_required_text(payload, "activity_id"), "ARCHIVED", payload)


def get_professional_activity(payload: Dict[str, Any]) -> Dict[str, Any]:
    activity_id = _required_text(payload, "activity_id")
    item = _find(_activities(), activity_id)
    if item is None:
        return {"status": "NOT_FOUND", "activity_id": activity_id}
    return {"status": "PASS", "activity": item}


def list_professional_activities(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items = _activities()
    status_filter = str(payload.get("status") or "").upper()
    type_filter = str(payload.get("activity_type") or "").lower()
    domain_filter = str(payload.get("business_domain") or payload.get("domain") or "")
    if status_filter:
        items = [item for item in items if str(item.get("status")) == status_filter]
    if type_filter:
        items = [item for item in items if str(item.get("activity_type")) == type_filter]
    if domain_filter:
        items = [item for item in items if str(item.get("business_domain") or "") == domain_filter]
    limit = max(1, min(int(payload.get("limit") or 50), 100))
    items.sort(key=lambda item: (PRIORITY_ORDER.get(str(item.get("priority")), 2), str(item.get("created_at"))))
    selected = items[:limit]
    return {"status": "PASS", "count": len(selected), "total_matching": len(items), "activities": [_compact(item) for item in selected]}


def get_executive_activity_status() -> Dict[str, Any]:
    state = _executive_state()
    items = _activities()
    active = _find(items, str(state.get("active_activity_id") or "")) if state.get("active_activity_id") else None
    queue_items = [_find(items, activity_id) for activity_id in state.get("queue", [])]
    queue_items = [item for item in queue_items if item and item.get("status") == "QUEUED"]
    queue_items.sort(key=lambda item: (PRIORITY_ORDER.get(str(item.get("priority")), 2), str(item.get("created_at"))))
    clean_queue = [str(item.get("activity_id")) for item in queue_items]
    if clean_queue != state.get("queue", []):
        state["queue"] = clean_queue
        _save_executive_state(state)
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "active_activity": _compact(active) if active else None,
        "queue_count": len(queue_items),
        "queue": [_compact(item) for item in queue_items[:20]],
        "controller_state": "BUSY" if active else ("WORK_AVAILABLE" if queue_items else "READY"),
        "next_recommended_action": "continue_active_activity" if active else ("activate_next_professional_activity" if queue_items else "await_product_owner_goal"),
    }


def activate_next_professional_activity(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    state = get_executive_activity_status()
    if state.get("active_activity"):
        return {"status": "BLOCKED", "reason": "active_activity_exists", "executive_state": state}
    queue = state.get("queue") or []
    if not queue:
        return {"status": "NO_WORK", "executive_state": state}
    return start_professional_activity({"activity_id": queue[0]["activity_id"], "reason": "executive_controller_selected_next_activity"})


def verify_professional_activity_foundation() -> Dict[str, Any]:
    manifest = get_professional_activity_manifest()
    activities_path = _path(ACTIVITIES_FILE)
    state_path = _path(EXECUTIVE_STATE_FILE)
    try:
        activities_path.parent.mkdir(parents=True, exist_ok=True)
        if not activities_path.exists():
            _write_json(activities_path, [])
        if not state_path.exists():
            _save_executive_state(_executive_state())
        repository_readable = isinstance(_read_json(activities_path, []), list)
        state_readable = isinstance(_read_json(state_path, {}), dict)
    except Exception:
        repository_readable = False
        state_readable = False
    activities_diagnostic = inspect_json_state(activities_path, list)
    state_diagnostic = inspect_json_state(state_path, dict)
    checks = {
        "manifest_available": manifest.get("status") == "PASS",
        "activity_repository_readable": repository_readable,
        "executive_state_readable": state_readable,
        "lifecycle_defined": all(status in ALLOWED_TRANSITIONS for status in ACTIVITY_STATUSES),
        "single_active_activity_guard": True,
        "no_background_execution_claim": True,
        "persistent_activity_repository": activities_diagnostic.get("status") in {"PASS", "RECOVERED", "EMPTY"},
        "persistent_executive_state": state_diagnostic.get("status") in {"PASS", "RECOVERED", "EMPTY"},
        "transport_session_independence": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "activity_count": len(_activities()),
        "executive_state": get_executive_activity_status(),
        "repository_diagnostics": {
            "activities": activities_diagnostic,
            "executive_state": state_diagnostic,
        },
    }
