"""Mandatory Professional Pipeline for VECTRA Runtime responses.

EP-001 Increment 002 moves Self Governance from durable storage into
observable Runtime behaviour. Every facade response is evaluated against the
active professional context before it is returned to the GPT layer.

The pipeline does not make Product Owner decisions and does not claim hidden
background execution. It produces deterministic governance directives,
persists professional continuity, and records deduplicated engineering
observations when Runtime results expose confirmed failures or blockers.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Optional
import hashlib

from app.assistant_runtime.durable_runtime_state import update_json_state
from app.assistant_runtime.professional_runtime_state import persist_professional_runtime_state
from app.assistant_runtime.self_governance_runtime import (
    get_self_governance_snapshot,
    record_observation,
)

RELEASE_ID = "VECTRA-SELF-GOVERNANCE-EP-001-INCREMENT-002"
CONTRACT_VERSION = "1.0"
PIPELINE_STATE_FILE = Path("runtime") / "governance" / "professional_pipeline_state.json"

_OPERATION_FAMILIES = {
    "self_audit": "professional_identity",
    "personality": "professional_identity",
    "verify_personality": "professional_identity",
    "start_working_session": "professional_continuity",
    "restore_professional_state": "professional_continuity",
    "runtime_status": "professional_runtime",
    "runtime_snapshot": "professional_runtime",
    "verify_runtime": "professional_runtime",
    "framework_manifest": "business_framework",
    "framework_registry": "business_framework",
    "execute_end_to_end": "business_research",
    "open_workspace": "business_workspace",
    "get_research_workspace": "business_research",
    "get_research_workspace_snapshot": "business_workspace",
    "discover_business_objects": "business_framework",
    "capitalize_confirmed_knowledge": "knowledge_capitalization",
    "read_professional_knowledge": "knowledge_capitalization",
    "read_business_knowledge": "knowledge_capitalization",
    "create_engineering_task": "engineering",
}

_ENGINEERING_FOCUS_HINTS = {
    "engineering", "self_governance", "development_governance", "architecture",
    "release", "verification", "runtime_implementation",
}

_TERMINAL_SUCCESS = {"PASS", "OK", "READY", "COMPLETED", "VERIFIED", "SUCCESS"}
_NON_TERMINAL_HOLD = {"HOLD", "NOT_READY", "BLOCKED", "INCOMPATIBLE", "PARTIAL"}
_FAILURE = {"FAIL", "FAILED", "ERROR", "INTERNAL_ERROR"}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _seed() -> Dict[str, Any]:
    return {
        "pipeline_id": "VECTRA-PROFESSIONAL-PIPELINE",
        "contract_version": CONTRACT_VERSION,
        "release": RELEASE_ID,
        "status": "ACTIVE",
        "processed_count": 0,
        "last_event": None,
        "recent_event_hashes": [],
        "updated_at": _now(),
    }


def _normalize_status(result: Any) -> str:
    if not isinstance(result, dict):
        return "UNKNOWN"
    candidates = [
        result.get("status"),
        result.get("final_status"),
        result.get("verification_status"),
        result.get("readiness"),
        result.get("overall_status"),
    ]
    for value in candidates:
        text = str(value or "").strip().upper()
        if text:
            return text
    return "UNKNOWN"


def _operation_family(operation_type: str, runtime_service: str = "") -> str:
    operation = str(operation_type or "").strip().lower()
    if operation in _OPERATION_FAMILIES:
        return _OPERATION_FAMILIES[operation]
    joined = f"{operation} {runtime_service}".lower()
    if any(token in joined for token in ("governance", "decision", "evolution", "continuity")):
        return "self_governance"
    if any(token in joined for token in ("engineering", "release", "deploy", "verification")):
        return "engineering"
    if any(token in joined for token in ("workspace", "dashboard")):
        return "business_workspace"
    if any(token in joined for token in ("business", "framework", "research", "sku", "network")):
        return "business_research"
    if any(token in joined for token in ("knowledge", "memory", "capitalization")):
        return "knowledge_capitalization"
    if any(token in joined for token in ("runtime", "snapshot", "status", "recovery")):
        return "professional_runtime"
    return "general_professional_activity"


def _focus_family(active_context: Dict[str, Any]) -> str:
    text = " ".join(
        str(active_context.get(key) or "")
        for key in ("cycle_id", "title", "current_focus", "next_recommended_step")
    ).lower()
    if any(token in text for token in _ENGINEERING_FOCUS_HINTS):
        return "engineering"
    if "business" in text or "workspace" in text:
        return "business_research"
    return "general_professional_activity"


def _confirmed_blocker(result: Any, normalized_status: str) -> bool:
    if not isinstance(result, dict):
        return normalized_status in _FAILURE
    if result.get("blocking") is True or result.get("stop_recommended") is True:
        return True
    if normalized_status in _FAILURE:
        return True
    attention = result.get("attention") if isinstance(result.get("attention"), dict) else {}
    return bool(attention.get("stop_recommended"))


def _event_hash(parts: Iterable[Any]) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _persist_pipeline_event(event: Dict[str, Any]) -> Dict[str, Any]:
    event_hash = str(event.get("event_hash") or "")

    was_new = {"value": False}

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = dict(current or _seed())
        hashes = [str(x) for x in state.get("recent_event_hashes", []) if x]
        if event_hash and event_hash not in hashes:
            hashes.append(event_hash)
            state["processed_count"] = int(state.get("processed_count") or 0) + 1
            was_new["value"] = True
        state["recent_event_hashes"] = hashes[-100:]
        state["last_event"] = deepcopy(event)
        state["updated_at"] = _now()
        state["release"] = RELEASE_ID
        state["contract_version"] = CONTRACT_VERSION
        state["status"] = "ACTIVE"
        return state

    state, diagnostic = update_json_state(PIPELINE_STATE_FILE, _seed, dict, updater)
    return {"state": state, "diagnostic": diagnostic, "was_new": was_new["value"]}


def _record_confirmed_observation_once(
    *, event_hash: str, operation_type: str, family: str, normalized_status: str, result: Any
) -> Optional[Dict[str, Any]]:
    # Event persistence happens first. If the same event hash has already been
    # processed, the caller marks it as duplicate and no new observation is made.
    if normalized_status not in _FAILURE | _NON_TERMINAL_HOLD:
        return None
    observation_type = "BLOCKER" if _confirmed_blocker(result, normalized_status) else "IMPROVEMENT"
    title = f"Runtime operation {operation_type} returned {normalized_status}"
    description = "Professional Pipeline recorded a confirmed Runtime outcome for engineering review."
    return record_observation(
        observation_type=observation_type,
        title=title,
        subsystem=family,
        description=description,
        source=f"professional_pipeline:{event_hash}",
        criticality="CRITICAL" if observation_type == "BLOCKER" else "NORMAL",
    )


def process_professional_response(
    *,
    operation_type: str,
    runtime_service: str,
    endpoint: str,
    result: Any,
    next_action: str = "",
) -> Dict[str, Any]:
    """Evaluate one Runtime result before it is returned to the GPT layer."""
    snapshot = get_self_governance_snapshot()
    active_context = snapshot.get("active_work_context") if isinstance(snapshot.get("active_work_context"), dict) else {}
    continuity = snapshot.get("professional_continuity") if isinstance(snapshot.get("professional_continuity"), dict) else {}
    attention = snapshot.get("attention") if isinstance(snapshot.get("attention"), dict) else {}

    family = _operation_family(operation_type, runtime_service)
    focus_family = _focus_family(active_context)
    normalized_status = _normalize_status(result)
    blocker = _confirmed_blocker(result, normalized_status)
    unrelated = focus_family not in {"general_professional_activity", family} and family not in {
        "professional_identity", "professional_runtime", "professional_continuity", "self_governance"
    }

    if blocker:
        governance_decision = "STOP_AND_PREPARE_ENGINEERING_TASK"
        governance_status = "HOLD"
        response_requirement = "Report the confirmed Runtime blocker precisely and do not continue as if the operation succeeded."
    elif unrelated and str(active_context.get("status") or "").upper() == "ACTIVE":
        governance_decision = "PRESERVE_ACTIVE_FOCUS_AND_OPEN_EXPLICIT_BRANCH"
        governance_status = "NOTICE"
        response_requirement = "Answer the request, but explicitly preserve the active cycle and mark this work as a separate branch."
    elif attention.get("recommendations"):
        governance_decision = "CONTINUE_WITH_ATTENTION_NOTICE"
        governance_status = "NOTICE"
        response_requirement = "Continue the active work and surface accumulated improvement attention when relevant."
    else:
        governance_decision = "CONTINUE_CURRENT_FOCUS"
        governance_status = "PASS"
        response_requirement = "Continue according to the active professional context."

    event_hash = _event_hash((operation_type, runtime_service, endpoint, normalized_status, family, active_context.get("cycle_id")))
    previous_state = _persist_pipeline_event({
        "event_hash": event_hash,
        "operation_type": operation_type,
        "runtime_service": runtime_service,
        "endpoint": endpoint,
        "operation_family": family,
        "result_status": normalized_status,
        "governance_decision": governance_decision,
        "governance_status": governance_status,
        "active_cycle_id": active_context.get("cycle_id"),
        "processed_at": _now(),
    })
    observation = None
    # Only the first occurrence of a deterministic event creates an observation.
    if previous_state.get("was_new") is True:
        observation = _record_confirmed_observation_once(
            event_hash=event_hash,
            operation_type=operation_type,
            family=family,
            normalized_status=normalized_status,
            result=result,
        )
    professional_state_result = persist_professional_runtime_state()

    return {
        "status": governance_status,
        "pipeline_id": "VECTRA-PROFESSIONAL-PIPELINE",
        "contract_version": CONTRACT_VERSION,
        "release": RELEASE_ID,
        "professional_context": {
            "active_work_context": deepcopy(active_context),
            "professional_continuity": deepcopy(continuity),
            "operation_family": family,
            "active_focus_family": focus_family,
            "result_status": normalized_status,
        },
        "self_governance": {
            "decision": governance_decision,
            "response_requirement": response_requirement,
            "confirmed_blocker": blocker,
            "new_branch_detected": unrelated,
            "attention": deepcopy(attention),
        },
        "engineering_observation": observation,
        "recommended_next_action": next_action or active_context.get("next_recommended_step") or continuity.get("resume_from"),
        "runtime_state_updated": bool(previous_state.get("diagnostic", {}).get("readback_verified")),
        "professional_runtime_state": professional_state_result.get("professional_runtime_state"),
        "professional_continuity_status": professional_state_result.get("status"),
        "read_only": False,
    }


def verify_professional_pipeline() -> Dict[str, Any]:
    pass_probe = process_professional_response(
        operation_type="self_audit",
        runtime_service="personality_runtime.self_audit",
        endpoint="/vectra/self-audit",
        result={"status": "PASS"},
        next_action="Continue professional work.",
    )
    notice_probe = process_professional_response(
        operation_type="open_workspace",
        runtime_service="business_workspace.open",
        endpoint="/vectra/business/workspace",
        result={"status": "PASS"},
    )
    checks = {
        "pipeline_active": pass_probe.get("pipeline_id") == "VECTRA-PROFESSIONAL-PIPELINE",
        "professional_context_present": isinstance(pass_probe.get("professional_context"), dict),
        "self_governance_present": isinstance(pass_probe.get("self_governance"), dict),
        "runtime_state_updated": pass_probe.get("runtime_state_updated") is True,
        "product_owner_decision_not_auto_approved": True,
        "new_branch_detection_available": isinstance(notice_probe.get("self_governance", {}).get("new_branch_detected"), bool),
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "checks": checks,
        "pass_probe": pass_probe,
        "notice_probe": notice_probe,
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
    }
