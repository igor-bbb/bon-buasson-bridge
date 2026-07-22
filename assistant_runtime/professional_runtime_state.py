"""Canonical Professional Runtime State for VECTRA.

EP-001 Final Integration creates one durable professional-state projection used
by Self Governance, Recovery, Self Audit and the Professional Pipeline. It does
not duplicate subsystem repositories: it composes their current confirmed
state into one canonical Runtime root and persists that root atomically.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from app.assistant_runtime.durable_runtime_state import (
    read_unified_runtime_state,
    update_unified_runtime_root,
)
from app.assistant_runtime.self_governance_runtime import get_self_governance_snapshot
from app.assistant_runtime.professional_business_model import build_professional_business_runtime_projection
from app.assistant_runtime.business_understanding_runtime import get_professional_understanding_state

RELEASE_ID = "VECTRA-SELF-GOVERNANCE-EP-001-FINAL"
CONTRACT_VERSION = "1.0"
STATE_ID = "VECTRA-PROFESSIONAL-RUNTIME-STATE"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _payload(root: Any) -> Dict[str, Any]:
    if not isinstance(root, dict):
        return {}
    payload = root.get("payload")
    return deepcopy(payload) if isinstance(payload, dict) else {}


def build_professional_runtime_state(
    *,
    active_business_domain: Optional[Dict[str, Any]] = None,
    professional_role: Optional[str] = None,
) -> Dict[str, Any]:
    unified, diagnostic = read_unified_runtime_state()
    governance = get_self_governance_snapshot()
    active_context = governance.get("active_work_context") if isinstance(governance, dict) else {}
    continuity = governance.get("professional_continuity") if isinstance(governance, dict) else {}
    decisions = governance.get("decisions") if isinstance(governance, dict) else []
    engineering_queue = governance.get("engineering_queue") if isinstance(governance, dict) else []
    evolution_backlog = governance.get("evolution_backlog") if isinstance(governance, dict) else []

    personality = _payload(unified.get("personality"))
    self_model = _payload(unified.get("self_model"))
    business_context = _payload(unified.get("business_context"))
    current_activity = _payload(unified.get("current_activity"))
    behaviour = _payload(unified.get("professional_behaviour"))

    domain = active_business_domain if isinstance(active_business_domain, dict) else {}
    if not domain:
        domain = self_model.get("active_business_domain") if isinstance(self_model.get("active_business_domain"), dict) else {}
    if not domain:
        domain = business_context.get("active_business_domain") if isinstance(business_context.get("active_business_domain"), dict) else {}

    role = str(professional_role or self_model.get("professional_role") or "vectra_laboratory").strip()
    domain_id = str(domain.get("domain_id") or "bon_buasson").strip().lower()
    professional_business_model = build_professional_business_runtime_projection(domain_id)
    professional_understanding_state = get_professional_understanding_state(domain_id)
    active_cycle = active_context if isinstance(active_context, dict) else {}
    open_decisions = [
        deepcopy(item) for item in (decisions or [])
        if isinstance(item, dict) and str(item.get("status") or "").upper() not in {"VERIFIED", "CAPITALIZED", "CLOSED", "REJECTED"}
    ]
    blockers = [
        deepcopy(item) for item in (engineering_queue or [])
        if isinstance(item, dict) and (
            str(item.get("criticality") or "").upper() == "CRITICAL"
            or str(item.get("status") or "").upper() in {"BLOCKED", "HOLD"}
        )
    ]

    next_action = (
        (continuity or {}).get("next_recommended_step")
        or active_cycle.get("next_recommended_step")
        or "continue_professional_work"
    )
    location_ready = bool(role)
    work_ready = bool(active_cycle.get("cycle_id") or current_activity)
    next_ready = bool(next_action)
    readiness = "PASS" if location_ready and work_ready and next_ready else "PARTIAL"

    return {
        "state_id": STATE_ID,
        "contract_version": CONTRACT_VERSION,
        "release": RELEASE_ID,
        "status": readiness,
        "professional_identity": {
            "identity_reference": self_model.get("identity_reference") or personality.get("personality_id"),
            "role": role,
            "workspace": self_model.get("current_workspace") or {},
        },
        "active_business_domain": deepcopy(domain),
        "professional_business_model": professional_business_model,
        "professional_understanding_state": professional_understanding_state,
        "active_work": {
            "engineering_cycle": deepcopy(active_cycle),
            "current_activity": deepcopy(current_activity),
            "current_focus": active_cycle.get("current_focus"),
        },
        "decision_state": {
            "open_count": len(open_decisions),
            "open_decisions": open_decisions,
        },
        "evolution_state": {
            "backlog_count": len(evolution_backlog or []),
            "items": deepcopy(evolution_backlog or []),
        },
        "engineering_state": {
            "queue_count": len(engineering_queue or []),
            "queue": deepcopy(engineering_queue or []),
            "blocker_count": len(blockers),
            "blockers": blockers,
        },
        "professional_continuity": deepcopy(continuity or {}),
        "professional_behaviour": deepcopy(behaviour),
        "recommended_next_action": next_action,
        "continuity_questions": {
            "where_am_i": role if location_ready else None,
            "what_am_i_working_on": active_cycle.get("title") or active_cycle.get("cycle_id") or current_activity.get("title"),
            "what_is_next": next_action,
        },
        "integrity": {
            "location_resolved": location_ready,
            "active_work_resolved": work_ready,
            "next_action_resolved": next_ready,
            "unified_runtime_read_status": diagnostic.get("status"),
            "professional_business_model_ready": professional_business_model.get("status") == "PASS",
            "professional_understanding_state_ready": professional_understanding_state.get("status") == "PASS",
        },
        "updated_at": _now(),
    }


def persist_professional_runtime_state(
    *,
    active_business_domain: Optional[Dict[str, Any]] = None,
    professional_role: Optional[str] = None,
) -> Dict[str, Any]:
    state = build_professional_runtime_state(
        active_business_domain=active_business_domain,
        professional_role=professional_role,
    )
    unified, diagnostic = update_unified_runtime_root(
        "professional_state",
        deepcopy(state),
        status="CONNECTED" if state.get("status") == "PASS" else "PARTIAL",
        source_of_truth="app.assistant_runtime.professional_runtime_state",
    )
    root = unified.get("professional_state") if isinstance(unified, dict) else {}
    readback = _payload(root)
    verified = readback.get("state_id") == STATE_ID
    return {
        "status": state.get("status") if verified else "HOLD",
        "professional_runtime_state": readback,
        "readback_verified": verified and bool(diagnostic.get("readback_verified")),
        "diagnostic": diagnostic,
        "read_only": False,
    }


def get_professional_runtime_state() -> Dict[str, Any]:
    unified, diagnostic = read_unified_runtime_state()
    root = unified.get("professional_state") if isinstance(unified, dict) else {}
    state = _payload(root)
    return {
        "status": state.get("status") or "NOT_READY",
        "professional_runtime_state": state,
        "diagnostic": diagnostic,
        "read_only": True,
    }


def restore_professional_continuity(
    *,
    active_business_domain: Optional[Dict[str, Any]] = None,
    professional_role: Optional[str] = None,
) -> Dict[str, Any]:
    persisted = persist_professional_runtime_state(
        active_business_domain=active_business_domain,
        professional_role=professional_role,
    )
    state = persisted.get("professional_runtime_state") or {}
    questions = state.get("continuity_questions") if isinstance(state, dict) else {}
    checks = {
        "where_am_i_resolved": bool((questions or {}).get("where_am_i")),
        "active_work_resolved": bool((questions or {}).get("what_am_i_working_on")),
        "next_step_resolved": bool((questions or {}).get("what_is_next")),
        "state_readback_verified": persisted.get("readback_verified") is True,
    }
    status = "PASS" if all(checks.values()) else "PARTIAL"
    return {
        "status": status,
        "recovery_type": "PROFESSIONAL_CONTINUITY_RECOVERY",
        "professional_runtime_state": state,
        "checks": checks,
        "recommended_next_action": state.get("recommended_next_action"),
        "chat_history_required": False,
        "release": RELEASE_ID,
        "read_only": False,
    }


def verify_professional_runtime_state() -> Dict[str, Any]:
    restored = restore_professional_continuity(professional_role="vectra_laboratory")
    return {
        "status": restored.get("status"),
        "checks": restored.get("checks"),
        "professional_runtime_state_id": (restored.get("professional_runtime_state") or {}).get("state_id"),
        "release": RELEASE_ID,
        "read_only": True,
    }
