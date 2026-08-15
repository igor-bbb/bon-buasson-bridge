"""Executable Self Governance for VECTRA.

This module turns accepted development rules into durable Runtime behaviour.
It keeps focus, decisions, improvements, blockers and professional continuity
outside chat history. Product decisions are never auto-approved: approval
requires explicit Product Owner confirmation.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
import uuid

from app.assistant_runtime.durable_runtime_state import (
    read_json_state,
    update_json_state,
    update_unified_runtime_root,
)

RELEASE_ID = "VECTRA-CORE-ONTOLOGY-001-INCREMENT-001"
WORK_CONTEXT_LIFECYCLE_RELEASE_ID = "VECTRA-PROFESSIONAL-WORK-CONTEXT-LIFECYCLE-001"
BLOCKER_GOVERNANCE_RELEASE_ID = "VECTRA-PROFESSIONAL-GOVERNANCE-APPROVED-EXECUTION-001"
CONTRACT_VERSION = "2.3"
STATE_FILE = Path("runtime") / "governance" / "self_governance_state.json"

VALID_OBSERVATION_TYPES = {"KNOWLEDGE", "IMPROVEMENT", "ARCHITECTURE_CHANGE", "BLOCKER"}
VALID_DECISION_STATUSES = {
    "CANDIDATE", "APPROVED", "DEFERRED", "REJECTED", "IN_IMPLEMENTATION",
    "IMPLEMENTED", "VERIFIED", "CAPITALIZED", "CLOSED",
}
VALID_FOCUS_STATUSES = {"ACTIVE", "PAUSED", "DEFERRED", "COMPLETED", "CANCELLED"}
ATTENTION_THRESHOLDS = {"RECOMMENDED": 5, "DESIRABLE": 7, "CRITICAL": 10}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _id(prefix: str) -> str:
    return f"{prefix}-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:8]}"


def _canonical_decisions() -> List[Dict[str, Any]]:
    return [
        {
            "decision_id": "CD-001",
            "title": "VECTRA is one continuous professional digital personality",
            "status": "IMPLEMENTED",
            "criticality": "CRITICAL",
            "owner": "professional_identity",
            "product_owner_confirmed": True,
            "verification": "Self Audit and session restoration",
        },
        {
            "decision_id": "CD-002",
            "title": "VECTRA is a digital organization with one identity and multiple professional roles",
            "status": "APPROVED",
            "criticality": "CRITICAL",
            "owner": "digital_organization",
            "product_owner_confirmed": True,
            "verification": "Role inheritance and task flow across digital colleagues",
        },
        {
            "decision_id": "CD-003",
            "title": "Profit and business development are the common business vector",
            "status": "APPROVED",
            "criticality": "CRITICAL",
            "owner": "canonical_product_model",
            "product_owner_confirmed": True,
            "verification": "Decision and recommendation alignment",
        },
        {
            "decision_id": "CD-004",
            "title": "SKU is the business atom; upper levels are aggregations",
            "status": "APPROVED",
            "criticality": "CRITICAL",
            "owner": "business_domain",
            "product_owner_confirmed": True,
            "verification": "Aggregation and investigation model verification",
        },
        {
            "decision_id": "CD-005",
            "title": "Working desktop is a professional briefing, not a dashboard",
            "status": "APPROVED",
            "criticality": "CRITICAL",
            "owner": "workspace",
            "product_owner_confirmed": True,
            "verification": "Workspace professional acceptance",
        },
        {
            "decision_id": "CD-006",
            "title": "External business environment is mandatory decision context",
            "status": "APPROVED",
            "criticality": "HIGH",
            "owner": "business_environment",
            "product_owner_confirmed": True,
            "verification": "External context appears in relevant workspace and dialogue scenarios",
        },
        {
            "decision_id": "CD-007",
            "title": "Business Domain restores passport, identity, operating and navigation models before data",
            "status": "APPROVED",
            "criticality": "CRITICAL",
            "owner": "business_domain",
            "product_owner_confirmed": True,
            "verification": "New-session Business Domain restoration",
        },
        {
            "decision_id": "CD-008",
            "title": "VECTRA governs accepted decisions, unfinished work and development focus",
            "status": "IN_IMPLEMENTATION",
            "criticality": "CRITICAL",
            "owner": "self_governance",
            "product_owner_confirmed": True,
            "verification": "Governance state persists and restores professional continuity",
        },
        {
            "decision_id": "CD-009",
            "title": "GPT model changes must not change VECTRA identity or obligations",
            "status": "APPROVED",
            "criticality": "HIGH",
            "owner": "platform_compatibility",
            "product_owner_confirmed": True,
            "verification": "Compatibility audit after model change",
        },
        {
            "decision_id": "CD-010",
            "title": "Product Owner is excluded from internal engineering decomposition",
            "status": "APPROVED",
            "criticality": "CRITICAL",
            "owner": "development_governance",
            "product_owner_confirmed": True,
            "verification": "Product Owner receives consolidated plan or real product choice only",
        },
        {
            "decision_id": "CD-011",
            "title": "Architecture decisions must be implemented in code; documents alone do not complete a cycle",
            "status": "IN_IMPLEMENTATION",
            "criticality": "CRITICAL",
            "owner": "development_governance",
            "product_owner_confirmed": True,
            "verification": "Runtime behaviour and professional acceptance",
        },
        {
            "decision_id": "CD-012",
            "title": "Critical ontology or architecture conflicts trigger a full implementation stop",
            "status": "IMPLEMENTED",
            "criticality": "CRITICAL",
            "owner": "core_ontology",
            "product_owner_confirmed": True,
            "verification": "Core Ontology architecture gate",
        },
        {
            "decision_id": "CD-013",
            "title": "VECTRA Core is business-independent; company knowledge enters only through adapters",
            "status": "IMPLEMENTED",
            "criticality": "CRITICAL",
            "owner": "core_ontology",
            "product_owner_confirmed": True,
            "verification": "Core Ontology business-independence verification",
        },
    ]


def _seed() -> Dict[str, Any]:
    now = _now()
    return {
        "governance_id": "VECTRA-SELF-GOVERNANCE",
        "version": CONTRACT_VERSION,
        "status": "ACTIVE",
        "active_work_context": {
            "cycle_id": "EP-001",
            "title": "Self Governance Engine",
            "status": "ACTIVE",
            "readiness_percent": 100,
            "current_focus": "Professional Pipeline integration",
            "started_at": now,
            "updated_at": now,
            "next_recommended_step": "Complete Increment 002 Professional Pipeline verification",
            "open_branches": [],
        },
        "work_context_history": [],
        "decisions": _canonical_decisions(),
        "observations": [],
        "evolution_backlog": [
            {
                "improvement_id": "UX-SELF-AUDIT-001",
                "title": "Group capabilities and make next action more professional",
                "subsystem": "self_audit",
                "status": "DEFERRED",
                "priority": "NORMAL",
                "blocking": False,
                "created_at": now,
            }
        ],
        "engineering_queue": [],
        "professional_continuity": {
            "last_cycle_id": "EP-001",
            "last_focus": "Self Governance Engine",
            "resume_from": "Professional Pipeline integration",
            "last_updated_at": now,
        },
        "policy": {
            "product_owner_approval_required": True,
            "automatic_product_decisions": False,
            "stop_only_for_blocker_or_critical_architecture_debt": True,
            "architecture_gate_required_before_implementation": True,
            "core_ontology_is_business_independent": True,
            "improvement_attention_thresholds": ATTENTION_THRESHOLDS,
            "new_major_cycle_requires_previous_status": ["COMPLETED", "DEFERRED", "REJECTED"],
        },
        "updated_at": now,
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
    }


def _merge_seed(current: Dict[str, Any]) -> Dict[str, Any]:
    state = dict(current or {})
    seed = _seed()
    for key in (
        "governance_id", "version", "status", "active_work_context", "work_context_history", "observations",
        "evolution_backlog", "engineering_queue", "professional_continuity", "policy",
    ):
        state.setdefault(key, deepcopy(seed[key]))
    existing = {
        str(item.get("decision_id")): item
        for item in state.get("decisions", [])
        if isinstance(item, dict) and item.get("decision_id")
    }
    for item in seed["decisions"]:
        existing.setdefault(item["decision_id"], deepcopy(item))
    state["decisions"] = list(existing.values())
    state["version"] = CONTRACT_VERSION
    state["updated_at"] = _now()
    state["release"] = RELEASE_ID
    state["contract_version"] = CONTRACT_VERSION
    return state


def _persist_root(state: Dict[str, Any]) -> Dict[str, Any]:
    unified, diagnostic = update_unified_runtime_root(
        "governance",
        deepcopy(state),
        status="CONNECTED",
        source_of_truth="app.assistant_runtime.self_governance_runtime",
    )
    return {
        "runtime_root_connected": (unified.get("governance") or {}).get("status") == "CONNECTED",
        "runtime_diagnostic": diagnostic,
    }


def initialize_self_governance_state() -> Dict[str, Any]:
    state, diagnostic = update_json_state(STATE_FILE, _seed, dict, _merge_seed)
    root = _persist_root(state)
    return {
        "status": "PASS" if diagnostic.get("readback_verified") and root["runtime_root_connected"] else "HOLD",
        "governance": deepcopy(state),
        "readback_verified": bool(diagnostic.get("readback_verified")),
        **root,
        "diagnostic": diagnostic,
        "read_only": False,
    }


def read_self_governance_state() -> Dict[str, Any]:
    state, diagnostic = read_json_state(STATE_FILE, _seed, dict)
    return {
        "status": diagnostic.get("status"),
        "governance": deepcopy(_merge_seed(state)),
        "diagnostic": diagnostic,
        "read_only": True,
    }


def set_active_work_context(
    *,
    cycle_id: str,
    title: str,
    focus: str,
    status: str = "ACTIVE",
    readiness_percent: Optional[int] = None,
    next_recommended_step: Optional[str] = None,
    product_owner_confirmed: bool = False,
) -> Dict[str, Any]:
    status = status.upper().strip()
    if status not in VALID_FOCUS_STATUSES:
        return {"status": "FAIL", "reason": "invalid_focus_status", "allowed": sorted(VALID_FOCUS_STATUSES)}

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = _merge_seed(current)
        previous = state.get("active_work_context") or {}
        switching_cycle = bool(previous.get("cycle_id") and previous.get("cycle_id") != cycle_id)
        previous_open = previous.get("status") not in {"COMPLETED", "DEFERRED", "CANCELLED"}
        if switching_cycle and previous_open and not product_owner_confirmed:
            state["focus_change_rejected"] = {
                "reason": "previous_cycle_not_closed",
                "previous_cycle_id": previous.get("cycle_id"),
                "requested_cycle_id": cycle_id,
                "at": _now(),
            }
            return state
        state["active_work_context"] = {
            "cycle_id": cycle_id,
            "title": title,
            "status": status,
            "readiness_percent": max(0, min(100, int(readiness_percent if readiness_percent is not None else 0))),
            "current_focus": focus,
            "started_at": previous.get("started_at") if previous.get("cycle_id") == cycle_id else _now(),
            "updated_at": _now(),
            "next_recommended_step": next_recommended_step,
            "open_branches": previous.get("open_branches", []) if previous.get("cycle_id") == cycle_id else [],
        }
        state.pop("focus_change_rejected", None)
        state["professional_continuity"] = {
            "last_cycle_id": cycle_id,
            "last_focus": focus,
            "resume_from": next_recommended_step or focus,
            "last_updated_at": _now(),
        }
        return state

    state, diagnostic = update_json_state(STATE_FILE, _seed, dict, updater)
    rejected = state.get("focus_change_rejected")
    _persist_root(state)
    return {
        "status": "HOLD" if rejected else "PASS",
        "active_work_context": deepcopy(state.get("active_work_context")),
        "focus_change_rejected": deepcopy(rejected),
        "readback_verified": bool(diagnostic.get("readback_verified")),
        "read_only": False,
    }


def transition_active_work_context(
    *,
    cycle_id: str,
    completed_work_id: str,
    expected_current_focus: str,
    completion_evidence_id: str,
    completion_verdict: str,
    next_focus: str,
    next_recommended_step: str,
    product_owner_confirmed: bool = False,
) -> Dict[str, Any]:
    """Complete the current work increment and move focus inside the same cycle.

    This command deliberately does not close the parent cycle and does not
    select a new architectural priority.  It records the Product Owner approved
    completion evidence, updates professional continuity atomically and returns
    a verified readback of the durable Governance state.
    """
    values = {
        "cycle_id": str(cycle_id or "").strip(),
        "completed_work_id": str(completed_work_id or "").strip(),
        "expected_current_focus": str(expected_current_focus or "").strip(),
        "completion_evidence_id": str(completion_evidence_id or "").strip(),
        "next_focus": str(next_focus or "").strip(),
        "next_recommended_step": str(next_recommended_step or "").strip(),
    }
    missing = sorted(key for key, value in values.items() if not value)
    if missing:
        return {
            "status": "HOLD",
            "verification_status": "FAIL",
            "readback_status": "NOT_RUN",
            "failure_reason": "required_transition_fields_missing",
            "missing_fields": missing,
            "release": WORK_CONTEXT_LIFECYCLE_RELEASE_ID,
            "read_only": True,
        }
    if not product_owner_confirmed:
        return {
            "status": "HOLD",
            "verification_status": "FAIL",
            "readback_status": "NOT_RUN",
            "failure_reason": "product_owner_confirmation_required",
            "release": WORK_CONTEXT_LIFECYCLE_RELEASE_ID,
            "read_only": True,
        }
    if str(completion_verdict or "").strip().upper() != "PASS":
        return {
            "status": "HOLD",
            "verification_status": "FAIL",
            "readback_status": "NOT_RUN",
            "failure_reason": "confirmed_pass_evidence_required",
            "received_completion_verdict": str(completion_verdict or "").strip().upper() or None,
            "release": WORK_CONTEXT_LIFECYCLE_RELEASE_ID,
            "read_only": True,
        }
    if values["next_focus"] == values["expected_current_focus"]:
        return {
            "status": "HOLD",
            "verification_status": "FAIL",
            "readback_status": "NOT_RUN",
            "failure_reason": "next_focus_must_differ_from_completed_focus",
            "release": WORK_CONTEXT_LIFECYCLE_RELEASE_ID,
            "read_only": True,
        }

    transition_key = "|".join(
        (
            values["cycle_id"],
            values["completed_work_id"],
            values["completion_evidence_id"],
        )
    )
    outcome: Dict[str, Any] = {}

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = _merge_seed(current)
        active = deepcopy(state.get("active_work_context") or {})
        history = [
            deepcopy(item)
            for item in state.get("work_context_history", [])
            if isinstance(item, dict)
        ]
        existing = next(
            (
                item
                for item in reversed(history)
                if str(item.get("transition_key") or "") == transition_key
            ),
            None,
        )
        if existing is not None:
            outcome.update(
                {
                    "mode": "idempotent_readback",
                    "transition": deepcopy(existing),
                }
            )
            return state

        if str(active.get("cycle_id") or "") != values["cycle_id"]:
            outcome.update(
                {
                    "mode": "rejected",
                    "failure_reason": "active_cycle_mismatch",
                    "actual_cycle_id": active.get("cycle_id"),
                }
            )
            return state
        if str(active.get("current_focus") or "") != values["expected_current_focus"]:
            outcome.update(
                {
                    "mode": "rejected",
                    "failure_reason": "active_focus_mismatch",
                    "actual_current_focus": active.get("current_focus"),
                }
            )
            return state

        attention = _attention_summary(state)
        if int(attention.get("open_blockers") or 0) > 0:
            outcome.update(
                {
                    "mode": "rejected",
                    "failure_reason": "open_engineering_blockers",
                    "open_blockers": int(attention.get("open_blockers") or 0),
                }
            )
            return state

        transitioned_at = _now()
        transition = {
            "transition_id": _id("WCTX"),
            "transition_key": transition_key,
            "cycle_id": values["cycle_id"],
            "completed_work_id": values["completed_work_id"],
            "completed_focus": values["expected_current_focus"],
            "completion_status": "COMPLETED",
            "completion_verdict": "PASS",
            "completion_evidence_id": values["completion_evidence_id"],
            "product_owner_confirmed": True,
            "next_focus": values["next_focus"],
            "next_recommended_step": values["next_recommended_step"],
            "new_architectural_priority_selected": False,
            "transitioned_at": transitioned_at,
        }
        history.append(transition)
        state["work_context_history"] = history[-100:]

        active.update(
            {
                "status": str(active.get("status") or "ACTIVE"),
                "focus_status": "ACTIVE",
                "readiness_percent": 0,
                "current_focus": values["next_focus"],
                "updated_at": transitioned_at,
                "next_recommended_step": values["next_recommended_step"],
                "last_completed_work": {
                    "work_id": values["completed_work_id"],
                    "focus": values["expected_current_focus"],
                    "status": "COMPLETED",
                    "verification_status": "PASS",
                    "evidence_id": values["completion_evidence_id"],
                    "completed_at": transitioned_at,
                },
            }
        )
        state["active_work_context"] = active
        state["professional_continuity"] = {
            "last_cycle_id": values["cycle_id"],
            "last_focus": values["next_focus"],
            "resume_from": values["next_recommended_step"],
            "last_completed_work_id": values["completed_work_id"],
            "completion_evidence_id": values["completion_evidence_id"],
            "last_updated_at": transitioned_at,
        }
        state["updated_at"] = transitioned_at
        outcome.update({"mode": "executed", "transition": deepcopy(transition)})
        return state

    state, diagnostic = update_json_state(STATE_FILE, _seed, dict, updater)
    _persist_root(state)
    active_readback = deepcopy(state.get("active_work_context") or {})

    if outcome.get("mode") == "rejected":
        return {
            "status": "HOLD",
            "verification_status": "FAIL",
            "readback_status": "PASS" if diagnostic.get("readback_verified") else "FAIL",
            "failure_reason": outcome.get("failure_reason"),
            "actual_cycle_id": outcome.get("actual_cycle_id"),
            "actual_current_focus": outcome.get("actual_current_focus"),
            "open_blockers": outcome.get("open_blockers"),
            "active_work_context": active_readback,
            "release": WORK_CONTEXT_LIFECYCLE_RELEASE_ID,
            "read_only": True,
        }

    transition = outcome.get("transition") if isinstance(outcome.get("transition"), dict) else {}
    readback_verified = bool(
        diagnostic.get("readback_verified")
        and active_readback.get("cycle_id") == values["cycle_id"]
        and active_readback.get("current_focus") == values["next_focus"]
        and (active_readback.get("last_completed_work") or {}).get("work_id")
        == values["completed_work_id"]
        and (active_readback.get("last_completed_work") or {}).get("evidence_id")
        == values["completion_evidence_id"]
    )
    return {
        "status": "PASS" if readback_verified else "HOLD",
        "verification_status": "PASS" if readback_verified else "FAIL",
        "readback_status": "PASS" if readback_verified else "FAIL",
        "transition_status": "COMPLETED" if readback_verified else "READBACK_FAILED",
        "transition_reused": outcome.get("mode") == "idempotent_readback",
        "transition": deepcopy(transition),
        "active_work_context": active_readback,
        "parent_cycle_completed": str(active_readback.get("status") or "").upper() == "COMPLETED",
        "new_architectural_priority_selected": False,
        "failure_reason": None if readback_verified else "work_context_readback_mismatch",
        "release": WORK_CONTEXT_LIFECYCLE_RELEASE_ID,
        "read_only": False,
    }


def get_active_work_context_lifecycle() -> Dict[str, Any]:
    """Read the active work context and its latest confirmed transition."""
    state_result = read_self_governance_state()
    state = state_result.get("governance") if isinstance(state_result.get("governance"), dict) else {}
    history = [
        deepcopy(item)
        for item in state.get("work_context_history", [])
        if isinstance(item, dict)
    ]
    active = deepcopy(state.get("active_work_context") or {})
    readback_verified = bool(
        state_result.get("status") in {"PASS", "RECOVERED"}
        and active.get("cycle_id")
    )
    return {
        "status": "PASS" if readback_verified else "HOLD",
        "verification_status": "PASS" if readback_verified else "FAIL",
        "readback_status": "PASS" if readback_verified else "FAIL",
        "active_work_context": active,
        "last_transition": history[-1] if history else None,
        "transition_count": len(history),
        "parent_cycle_completed": str(active.get("status") or "").upper() == "COMPLETED",
        "new_architectural_priority_selected": False,
        "failure_reason": None if readback_verified else "active_work_context_unavailable",
        "release": WORK_CONTEXT_LIFECYCLE_RELEASE_ID,
        "read_only": True,
    }


def record_observation(
    *,
    observation_type: str,
    title: str,
    subsystem: str,
    description: str = "",
    source: str = "professional_dialogue",
    criticality: str = "NORMAL",
) -> Dict[str, Any]:
    observation_type = observation_type.upper().strip()
    if observation_type not in VALID_OBSERVATION_TYPES:
        return {"status": "FAIL", "reason": "invalid_observation_type", "allowed": sorted(VALID_OBSERVATION_TYPES)}
    item = {
        "observation_id": _id("OBS"),
        "type": observation_type,
        "title": title,
        "description": description,
        "subsystem": subsystem,
        "source": source,
        "criticality": criticality.upper().strip(),
        "status": "OPEN",
        "created_at": _now(),
    }

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = _merge_seed(current)
        state.setdefault("observations", []).append(item)
        if observation_type == "IMPROVEMENT":
            state.setdefault("evolution_backlog", []).append({
                "improvement_id": _id("IMP"),
                "observation_id": item["observation_id"],
                "title": title,
                "description": description,
                "subsystem": subsystem,
                "status": "OPEN",
                "priority": criticality.upper().strip(),
                "blocking": False,
                "created_at": _now(),
            })
        elif observation_type in {"ARCHITECTURE_CHANGE", "BLOCKER"}:
            state.setdefault("engineering_queue", []).append({
                "engineering_item_id": _id("ENG"),
                "observation_id": item["observation_id"],
                "title": title,
                "description": description,
                "subsystem": subsystem,
                "status": "CANDIDATE",
                "criticality": "CRITICAL" if observation_type == "BLOCKER" else criticality.upper().strip(),
                "blocking": observation_type == "BLOCKER",
                "product_owner_confirmed": False,
                "created_at": _now(),
            })
        return state

    state, diagnostic = update_json_state(STATE_FILE, _seed, dict, updater)
    _persist_root(state)
    return {
        "status": "PASS",
        "observation": deepcopy(item),
        "attention": _attention_summary(state),
        "readback_verified": bool(diagnostic.get("readback_verified")),
        "read_only": False,
    }


def verify_runtime_operation_blockers(
    *,
    operation_type: str,
    runtime_service: str,
    endpoint: str,
    verification_status: str,
) -> Dict[str, Any]:
    """Close Professional Pipeline blockers disproved by a successful readback.

    This is an evidence transition, not a Product Owner decision: it only
    verifies open blocker candidates that were created automatically by the
    Professional Pipeline for the exact Runtime operation now returning a
    terminal success.
    """
    normalized_status = str(verification_status or "").strip().upper()
    if normalized_status not in {"PASS", "OK", "READY", "COMPLETED", "VERIFIED", "SUCCESS"}:
        return {
            "status": "NOT_APPLICABLE",
            "verified_blockers_count": 0,
            "verified_engineering_item_ids": [],
            "read_only": True,
        }

    operation = str(operation_type or "").strip()
    expected_title_prefix = f"Runtime operation {operation} returned "
    verified_items: List[Dict[str, Any]] = []

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = _merge_seed(current)
        observations = {
            str(item.get("observation_id")): item
            for item in state.get("observations", [])
            if isinstance(item, dict) and item.get("observation_id")
        }
        for item in state.get("engineering_queue", []):
            if not isinstance(item, dict):
                continue
            if item.get("blocking") is not True:
                continue
            if item.get("status") in {"IMPLEMENTED", "VERIFIED", "CLOSED", "REJECTED"}:
                continue
            if not str(item.get("title") or "").startswith(expected_title_prefix):
                continue
            observation = observations.get(str(item.get("observation_id") or ""))
            if not isinstance(observation, dict):
                continue
            if not str(observation.get("source") or "").startswith("professional_pipeline:"):
                continue

            evidence = {
                "operation_type": operation,
                "runtime_service": runtime_service,
                "endpoint": endpoint,
                "verification_status": normalized_status,
                "verified_at": _now(),
            }
            item["status"] = "VERIFIED"
            item["verification"] = deepcopy(evidence)
            item["updated_at"] = evidence["verified_at"]
            observation["status"] = "CLOSED"
            observation["closure_reason"] = "runtime_operation_successfully_reverified"
            observation["verification"] = deepcopy(evidence)
            observation["updated_at"] = evidence["verified_at"]
            verified_items.append(deepcopy(item))
        return state

    state, diagnostic = update_json_state(STATE_FILE, _seed, dict, updater)
    _persist_root(state)
    return {
        "status": "PASS",
        "verified_blockers_count": len(verified_items),
        "verified_engineering_item_ids": [
            item.get("engineering_item_id") for item in verified_items
        ],
        "attention": _attention_summary(state),
        "readback_verified": bool(diagnostic.get("readback_verified")),
        "read_only": False,
    }


def get_engineering_blockers(
    engineering_item_id: Optional[str] = None,
    *,
    include_resolved: bool = False,
) -> Dict[str, Any]:
    """Return Self Governance blocker candidates through a Product Owner view.

    Professional Pipeline creates durable ``ENG-*`` candidates when a Runtime
    failure is confirmed.  This read contract makes those candidates visible
    without requiring the Product Owner to inspect the internal Governance
    snapshot or infer a decision path from technical HOLD output.
    """
    requested_id = str(engineering_item_id or "").strip()
    state_result = read_self_governance_state()
    state = state_result.get("governance") if isinstance(state_result.get("governance"), dict) else {}
    terminal_statuses = {"IMPLEMENTED", "VERIFIED", "CLOSED", "REJECTED"}
    items = []
    for raw in state.get("engineering_queue", []):
        if not isinstance(raw, dict) or raw.get("blocking") is not True:
            continue
        item_id = str(raw.get("engineering_item_id") or "")
        if requested_id and item_id != requested_id:
            continue
        status = str(raw.get("status") or "CANDIDATE").upper()
        if not include_resolved and status in terminal_statuses:
            continue
        item = deepcopy(raw)
        item.setdefault("owner_decision", {"status": "PENDING"})
        item["product_owner_decision_required"] = bool(
            status not in terminal_statuses
            and (item.get("owner_decision") or {}).get("status") not in {"APPROVED", "REJECTED"}
        )
        items.append(item)

    attention = _attention_summary(state)
    found = bool(items) or not requested_id
    return {
        "status": "PASS" if found else "NOT_FOUND",
        "operation_type": "get_engineering_blockers",
        "engineering_item_id": requested_id or None,
        "blockers_count": len(items),
        "blockers": items,
        "open_blockers_count": int(attention.get("open_blockers") or 0),
        "hold_blockers_count": int(attention.get("hold_blockers") or 0),
        "approved_blockers_count": int(attention.get("approved_blockers") or 0),
        "engineering_hold": bool(attention.get("stop_recommended")),
        "product_owner_approval_required": bool(attention.get("product_owner_approval_required")),
        "protected_mutations_allowed": not bool(attention.get("stop_recommended")),
        "engineering_execution_scope": attention.get("engineering_execution_scope"),
        "approved_engineering_item_ids": deepcopy(attention.get("approved_engineering_item_ids") or []),
        "allowed_decisions": ["APPROVED", "REJECTED", "DEFERRED"],
        "failure_reason": None if found else "engineering_blocker_not_found",
        "release": BLOCKER_GOVERNANCE_RELEASE_ID,
        "read_only": True,
    }


def record_engineering_blocker_owner_decision(
    engineering_item_id: str,
    decision: str,
    *,
    product_owner_confirmed: bool = False,
    comment: str = "",
) -> Dict[str, Any]:
    """Record an explicit Product Owner decision for one ``ENG-*`` blocker.

    Approval creates and approves one linked Development Journal record while
    releasing ENGINEERING_HOLD for the explicitly approved engineering scope.
    The blocker remains open until Product Verification verifies or closes it.
    Rejection resolves only the selected blocker. Deferral records the decision
    but intentionally keeps the hold active.
    """
    item_id = str(engineering_item_id or "").strip()
    normalized_decision = str(decision or "").strip().upper()
    allowed = {"APPROVED", "REJECTED", "DEFERRED"}
    if not item_id:
        return {
            "status": "HOLD",
            "operation_type": "record_engineering_blocker_decision",
            "failure_reason": "engineering_item_id_required",
            "release": BLOCKER_GOVERNANCE_RELEASE_ID,
            "read_only": True,
        }
    if normalized_decision not in allowed:
        return {
            "status": "HOLD",
            "operation_type": "record_engineering_blocker_decision",
            "engineering_item_id": item_id,
            "failure_reason": "explicit_owner_decision_required",
            "allowed_decisions": sorted(allowed),
            "release": BLOCKER_GOVERNANCE_RELEASE_ID,
            "read_only": True,
        }
    if not product_owner_confirmed:
        return {
            "status": "HOLD",
            "operation_type": "record_engineering_blocker_decision",
            "engineering_item_id": item_id,
            "failure_reason": "product_owner_confirmation_required",
            "release": BLOCKER_GOVERNANCE_RELEASE_ID,
            "read_only": True,
        }

    state_result = read_self_governance_state()
    state = state_result.get("governance") if isinstance(state_result.get("governance"), dict) else {}
    current_item = next(
        (
            deepcopy(item)
            for item in state.get("engineering_queue", [])
            if isinstance(item, dict)
            and str(item.get("engineering_item_id") or "") == item_id
            and item.get("blocking") is True
        ),
        None,
    )
    if current_item is None:
        return {
            "status": "NOT_FOUND",
            "operation_type": "record_engineering_blocker_decision",
            "engineering_item_id": item_id,
            "failure_reason": "engineering_blocker_not_found",
            "release": BLOCKER_GOVERNANCE_RELEASE_ID,
            "read_only": True,
        }

    prior_decision = str((current_item.get("owner_decision") or {}).get("status") or "").upper()
    if prior_decision == normalized_decision and current_item.get("product_owner_confirmed") is True:
        attention = _attention_summary(state)
        return {
            "status": "PASS",
            "operation_type": "record_engineering_blocker_decision",
            "engineering_item_id": item_id,
            "decision": deepcopy(current_item.get("owner_decision") or {}),
            "development_record_id": current_item.get("development_record_id"),
            "decision_reused": True,
            "engineering_hold": bool(attention.get("stop_recommended")),
            "open_blockers_count": int(attention.get("open_blockers") or 0),
            "hold_blockers_count": int(attention.get("hold_blockers") or 0),
            "protected_mutations_allowed": not bool(attention.get("stop_recommended")),
            "engineering_execution_scope": attention.get("engineering_execution_scope"),
            "failure_reason": None,
            "release": BLOCKER_GOVERNANCE_RELEASE_ID,
            "read_only": False,
        }
    if prior_decision in {"APPROVED", "REJECTED"}:
        return {
            "status": "HOLD",
            "operation_type": "record_engineering_blocker_decision",
            "engineering_item_id": item_id,
            "failure_reason": "owner_decision_already_recorded",
            "existing_decision": prior_decision,
            "release": BLOCKER_GOVERNANCE_RELEASE_ID,
            "read_only": True,
        }

    development_record_id = current_item.get("development_record_id")
    if normalized_decision == "APPROVED" and not development_record_id:
        # Local import avoids coupling Development Journal startup to the
        # Governance module while keeping the approved transition executable.
        from app.development_journal import create_development_request, record_owner_decision

        created = create_development_request(
            {
                "event_type": "self_governance_blocker",
                "component": str(current_item.get("subsystem") or "self_governance"),
                "confirmed_gap": str(current_item.get("title") or "Confirmed Runtime blocker."),
                "evidence_summary": str(
                    current_item.get("description")
                    or "Professional Pipeline created a durable blocker candidate."
                ),
                "proposal": "Prepare one bounded engineering increment and verify it through Product Verification.",
                "priority": "P1",
                "runtime_context": {
                    "source_environment": "Self Governance",
                    "engineering_item_id": item_id,
                    "observation_id": current_item.get("observation_id"),
                    "governance_source": "professional_pipeline",
                },
                "session_id": f"self-governance-{item_id}",
                "source_environment": "Self Governance",
            }
        )
        development_record_id = created.get("record_id")
        if created.get("status") != "ok" or not development_record_id:
            return {
                "status": "HOLD",
                "operation_type": "record_engineering_blocker_decision",
                "engineering_item_id": item_id,
                "failure_reason": "development_record_creation_failed",
                "journal_result": created,
                "release": BLOCKER_GOVERNANCE_RELEASE_ID,
                "read_only": True,
            }
        approved = record_owner_decision(
            str(development_record_id),
            {
                "decision": "APPROVED",
                "product_owner_approval": True,
                "comment": comment or f"Approved from Self Governance blocker {item_id}.",
            },
        )
        if approved.get("status") != "ok":
            return {
                "status": "HOLD",
                "operation_type": "record_engineering_blocker_decision",
                "engineering_item_id": item_id,
                "development_record_id": development_record_id,
                "failure_reason": "development_record_owner_decision_failed",
                "journal_result": approved,
                "release": BLOCKER_GOVERNANCE_RELEASE_ID,
                "read_only": True,
            }

    decision_record = {
        "status": normalized_decision,
        "actor": "Product Owner",
        "comment": str(comment or ""),
        "decided_at": _now(),
    }
    updated: Dict[str, Any] = {}

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        merged = _merge_seed(current)
        for item in merged.get("engineering_queue", []):
            if not isinstance(item, dict) or str(item.get("engineering_item_id") or "") != item_id:
                continue
            item["status"] = normalized_decision
            item["owner_decision"] = deepcopy(decision_record)
            item["product_owner_confirmed"] = True
            item["updated_at"] = decision_record["decided_at"]
            if development_record_id:
                item["development_record_id"] = development_record_id
            updated.update(deepcopy(item))
            break
        if normalized_decision == "REJECTED":
            for observation in merged.get("observations", []):
                if (
                    isinstance(observation, dict)
                    and str(observation.get("observation_id") or "")
                    == str(current_item.get("observation_id") or "")
                ):
                    observation["status"] = "CLOSED"
                    observation["closure_reason"] = "rejected_by_product_owner"
                    observation["updated_at"] = decision_record["decided_at"]
                    break
        return merged

    persisted_state, diagnostic = update_json_state(STATE_FILE, _seed, dict, updater)
    _persist_root(persisted_state)
    if not updated:
        return {
            "status": "NOT_FOUND",
            "operation_type": "record_engineering_blocker_decision",
            "engineering_item_id": item_id,
            "failure_reason": "engineering_blocker_not_found_during_update",
            "release": BLOCKER_GOVERNANCE_RELEASE_ID,
            "read_only": True,
        }
    attention = _attention_summary(persisted_state)
    return {
        "status": "PASS",
        "operation_type": "record_engineering_blocker_decision",
        "engineering_item_id": item_id,
        "decision": deepcopy(decision_record),
        "development_record_id": development_record_id,
        "decision_reused": False,
        "engineering_hold": bool(attention.get("stop_recommended")),
        "open_blockers_count": int(attention.get("open_blockers") or 0),
        "hold_blockers_count": int(attention.get("hold_blockers") or 0),
        "approved_blockers_count": int(attention.get("approved_blockers") or 0),
        "product_owner_approval_required": bool(attention.get("product_owner_approval_required")),
        "protected_mutations_allowed": not bool(attention.get("stop_recommended")),
        "engineering_execution_scope": attention.get("engineering_execution_scope"),
        "approved_engineering_item_ids": deepcopy(attention.get("approved_engineering_item_ids") or []),
        "readback_verified": bool(diagnostic.get("readback_verified")),
        "failure_reason": None,
        "release": BLOCKER_GOVERNANCE_RELEASE_ID,
        "read_only": False,
    }


def reconcile_engineering_blocker_development_verification(
    development_record_id: str,
    verdict: str,
    *,
    release_id: str = "",
) -> Dict[str, Any]:
    """Project linked DEV Product Verification back into Governance HOLD.

    A PASS is the evidence transition that verifies the linked ``ENG-*``
    blocker and permits the hold to clear when no other blocker remains. A FAIL
    returns the candidate to its approved blocking state for another bounded
    engineering iteration.
    """
    record_id = str(development_record_id or "").strip()
    normalized_verdict = str(verdict or "").strip().upper()
    if not record_id or normalized_verdict not in {"PASS", "FAIL"}:
        return {
            "status": "NOT_APPLICABLE",
            "operation_type": "reconcile_engineering_blocker_development_verification",
            "verified_engineering_item_ids": [],
            "failure_reason": "linked_development_verification_required",
            "release": BLOCKER_GOVERNANCE_RELEASE_ID,
            "read_only": True,
        }

    changed: List[Dict[str, Any]] = []
    verified_at = _now()

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = _merge_seed(current)
        observations = {
            str(item.get("observation_id") or ""): item
            for item in state.get("observations", [])
            if isinstance(item, dict)
        }
        for item in state.get("engineering_queue", []):
            if not isinstance(item, dict):
                continue
            if str(item.get("development_record_id") or "") != record_id:
                continue
            if str(item.get("status") or "").upper() in {"IMPLEMENTED", "VERIFIED", "CLOSED", "REJECTED"}:
                continue
            verification = {
                "status": normalized_verdict,
                "development_record_id": record_id,
                "release_id": str(release_id or ""),
                "verified_at": verified_at,
            }
            item["status"] = "VERIFIED" if normalized_verdict == "PASS" else "APPROVED"
            item["verification"] = verification
            item["updated_at"] = verified_at
            observation = observations.get(str(item.get("observation_id") or ""))
            if isinstance(observation, dict):
                observation["verification"] = deepcopy(verification)
                observation["updated_at"] = verified_at
                if normalized_verdict == "PASS":
                    observation["status"] = "CLOSED"
                    observation["closure_reason"] = "linked_development_product_verification_pass"
                else:
                    observation["status"] = "OPEN"
                    observation.pop("closure_reason", None)
            changed.append(deepcopy(item))
        return state

    persisted_state, diagnostic = update_json_state(STATE_FILE, _seed, dict, updater)
    _persist_root(persisted_state)
    attention = _attention_summary(persisted_state)
    return {
        "status": "PASS" if changed else "NOT_APPLICABLE",
        "operation_type": "reconcile_engineering_blocker_development_verification",
        "development_record_id": record_id,
        "verdict": normalized_verdict,
        "verified_engineering_item_ids": [
            item.get("engineering_item_id") for item in changed if item.get("engineering_item_id")
        ],
        "open_blockers_count": int(attention.get("open_blockers") or 0),
        "hold_blockers_count": int(attention.get("hold_blockers") or 0),
        "approved_blockers_count": int(attention.get("approved_blockers") or 0),
        "engineering_hold": bool(attention.get("stop_recommended")),
        "product_owner_approval_required": bool(attention.get("product_owner_approval_required")),
        "protected_mutations_allowed": not bool(attention.get("stop_recommended")),
        "engineering_execution_scope": attention.get("engineering_execution_scope"),
        "readback_verified": bool(diagnostic.get("readback_verified")),
        "failure_reason": None,
        "release": BLOCKER_GOVERNANCE_RELEASE_ID,
        "read_only": False,
    }


def register_decision_candidate(
    *,
    title: str,
    owner: str,
    impact: Optional[List[str]] = None,
    criticality: str = "NORMAL",
    source: str = "professional_dialogue",
) -> Dict[str, Any]:
    item = {
        "decision_id": _id("DEC"),
        "title": title,
        "owner": owner,
        "impact": list(impact or []),
        "status": "CANDIDATE",
        "criticality": criticality.upper().strip(),
        "source": source,
        "product_owner_confirmed": False,
        "created_at": _now(),
        "updated_at": _now(),
    }

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = _merge_seed(current)
        state.setdefault("decisions", []).append(item)
        return state

    state, diagnostic = update_json_state(STATE_FILE, _seed, dict, updater)
    _persist_root(state)
    return {"status": "PASS", "decision": deepcopy(item), "readback_verified": bool(diagnostic.get("readback_verified")), "read_only": False}


def transition_decision(
    decision_id: str,
    new_status: str,
    *,
    product_owner_confirmed: bool = False,
    verification: Optional[str] = None,
) -> Dict[str, Any]:
    new_status = new_status.upper().strip()
    if new_status not in VALID_DECISION_STATUSES:
        return {"status": "FAIL", "reason": "invalid_decision_status", "allowed": sorted(VALID_DECISION_STATUSES)}
    requires_confirmation = new_status in {"APPROVED", "DEFERRED", "REJECTED"}
    if requires_confirmation and not product_owner_confirmed:
        return {"status": "HOLD", "reason": "product_owner_confirmation_required", "decision_id": decision_id}
    changed: Dict[str, Any] = {}

    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = _merge_seed(current)
        for item in state.get("decisions", []):
            if isinstance(item, dict) and item.get("decision_id") == decision_id:
                item["status"] = new_status
                item["updated_at"] = _now()
                if product_owner_confirmed:
                    item["product_owner_confirmed"] = True
                if verification:
                    item["verification"] = verification
                changed.update(deepcopy(item))
                break
        return state

    state, diagnostic = update_json_state(STATE_FILE, _seed, dict, updater)
    _persist_root(state)
    return {
        "status": "PASS" if changed else "NOT_FOUND",
        "decision": changed or None,
        "readback_verified": bool(diagnostic.get("readback_verified")),
        "read_only": False,
    }


def _attention_summary(state: Dict[str, Any]) -> Dict[str, Any]:
    backlog = [item for item in state.get("evolution_backlog", []) if isinstance(item, dict) and item.get("status") not in {"CLOSED", "IMPLEMENTED", "REJECTED"}]
    by_subsystem: Dict[str, int] = {}
    for item in backlog:
        key = str(item.get("subsystem") or "unclassified")
        by_subsystem[key] = by_subsystem.get(key, 0) + 1
    recommendations: List[Dict[str, Any]] = []
    for subsystem, count in sorted(by_subsystem.items()):
        level = "CALM"
        if count >= ATTENTION_THRESHOLDS["CRITICAL"]:
            level = "CRITICAL"
        elif count >= ATTENTION_THRESHOLDS["DESIRABLE"]:
            level = "DESIRABLE"
        elif count >= ATTENTION_THRESHOLDS["RECOMMENDED"]:
            level = "RECOMMENDED"
        if level != "CALM":
            recommendations.append({"subsystem": subsystem, "open_improvements": count, "attention_level": level})
    blockers = [
        item for item in state.get("engineering_queue", [])
        if isinstance(item, dict) and item.get("blocking") is True and item.get("status") not in {"IMPLEMENTED", "VERIFIED", "CLOSED", "REJECTED"}
    ]
    approved_blockers = [
        item for item in blockers
        if str(item.get("status") or "").upper() in {"APPROVED", "IN_IMPLEMENTATION"}
        and item.get("product_owner_confirmed") is True
        and str((item.get("owner_decision") or {}).get("status") or "").upper() == "APPROVED"
    ]
    approved_ids = {
        str(item.get("engineering_item_id") or "")
        for item in approved_blockers
    }
    hold_blockers = [
        item for item in blockers
        if str(item.get("engineering_item_id") or "") not in approved_ids
    ]
    return {
        "open_improvements": len(backlog),
        "by_subsystem": by_subsystem,
        "recommendations": recommendations,
        "open_blockers": len(blockers),
        "blockers": deepcopy(blockers),
        "hold_blockers": len(hold_blockers),
        "approved_blockers": len(approved_blockers),
        "approved_engineering_item_ids": sorted(approved_ids),
        "approved_development_record_ids": sorted(
            str(item.get("development_record_id") or "")
            for item in approved_blockers
            if str(item.get("development_record_id") or "")
        ),
        "product_owner_approval_required": bool(hold_blockers),
        "approved_engineering_execution_allowed": bool(approved_blockers),
        "engineering_execution_scope": "APPROVED_BLOCKERS_ONLY" if approved_blockers else "NONE",
        "stop_recommended": bool(hold_blockers),
    }


def get_self_governance_snapshot() -> Dict[str, Any]:
    initialized = initialize_self_governance_state()
    state = initialized.get("governance") or {}
    open_decisions = [
        item for item in state.get("decisions", [])
        if isinstance(item, dict) and item.get("status") not in {"VERIFIED", "CAPITALIZED", "CLOSED", "REJECTED"}
    ]
    attention = _attention_summary(state)
    focus = state.get("active_work_context") or {}
    continuity = state.get("professional_continuity") or {}
    return {
        "status": "HOLD" if attention.get("stop_recommended") else "PASS",
        "governance_id": state.get("governance_id"),
        "version": state.get("version"),
        "active_work_context": deepcopy(focus),
        "professional_continuity": deepcopy(continuity),
        "open_decision_count": len(open_decisions),
        "open_decisions": deepcopy(open_decisions),
        "attention": attention,
        "policy": deepcopy(state.get("policy") or {}),
        "release": RELEASE_ID,
        "read_only": True,
    }


def get_governance_gate() -> Dict[str, Any]:
    snapshot = get_self_governance_snapshot()
    decisions = snapshot.get("open_decisions") or []
    open_critical = [
        item for item in decisions
        if isinstance(item, dict)
        and item.get("criticality") == "CRITICAL"
        and item.get("status") not in {"IMPLEMENTED", "VERIFIED", "CAPITALIZED", "CLOSED", "DEFERRED"}
    ]
    blocker_count = int((snapshot.get("attention") or {}).get("open_blockers") or 0)
    hold_blocker_count = int((snapshot.get("attention") or {}).get("hold_blockers") or 0)
    hold = hold_blocker_count > 0
    return {
        "status": "HOLD" if hold else "PASS",
        "active_work_context": snapshot.get("active_work_context"),
        "professional_continuity": snapshot.get("professional_continuity"),
        "open_critical_count": len(open_critical),
        "open_critical_decisions": deepcopy(open_critical),
        "open_blocker_count": blocker_count,
        "hold_blocker_count": hold_blocker_count,
        "attention": snapshot.get("attention"),
        "continuation_policy": (
            "stop_and_prepare_engineering_task" if hold
            else "continue_current_focus_and_preserve_open_commitments"
        ),
        "read_only": True,
    }


def verify_self_governance_runtime() -> Dict[str, Any]:
    initialized = initialize_self_governance_state()
    snapshot = get_self_governance_snapshot()
    checks = {
        "durable_readback": initialized.get("readback_verified") is True,
        "runtime_root_connected": initialized.get("runtime_root_connected") is True,
        "active_work_context_available": bool(snapshot.get("active_work_context")),
        "professional_continuity_available": bool(snapshot.get("professional_continuity")),
        "decision_lifecycle_available": isinstance(snapshot.get("open_decisions"), list),
        "product_owner_approval_required": bool((snapshot.get("policy") or {}).get("product_owner_approval_required")),
        "automatic_product_decisions_disabled": (snapshot.get("policy") or {}).get("automatic_product_decisions") is False,
        "attention_thresholds_available": bool(((snapshot.get("policy") or {}).get("improvement_attention_thresholds"))),
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "checks": checks,
        "snapshot": snapshot,
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
    }
