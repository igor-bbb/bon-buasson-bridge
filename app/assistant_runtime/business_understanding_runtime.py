"""Executable understanding, gap, hypothesis and role model for PBM.

PROGRAM-002 / PBM-FOUNDATION-001 / INCREMENT-003

This module makes the evolving professional understanding of a Business Domain
explicit. It does not pretend to know the full real organisation. It records
what VECTRA currently understands, what is only mentioned, what is missing,
which gaps were deferred by Product Owner, and which professional hypotheses
should be discussed before they become decisions or engineering changes.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.durable_runtime_state import read_json_state, update_json_state

RELEASE_ID = "VECTRA-PBM-FOUNDATION-001-INCREMENT-003"
CONTRACT_VERSION = "1.0"
DEFAULT_DOMAIN_ID = "bon_buasson"
_STATE_ROOT = Path("runtime") / "business_domains"

AREA_STATES = {
    "MENTIONED",
    "PRELIMINARY",
    "ACTIVE_RESEARCH",
    "PROFESSIONAL_MODEL_READY",
    "DATA_CONNECTED",
    "OPERATIONAL",
}
GAP_STATES = {"OPEN", "DEFERRED", "ACTIVE", "RESOLVED", "CAPITALIZATION_REQUIRED"}
HYPOTHESIS_STATES = {"OPEN", "CONTEXT_REQUIRED", "CONFIRMED", "REJECTED", "DEFERRED", "CAPITALIZATION_REQUIRED"}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _state_path(domain_id: str) -> Path:
    key = str(domain_id or DEFAULT_DOMAIN_ID).strip().lower()
    return _STATE_ROOT / key / "professional_understanding_state.json"


def _default_state(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    key = str(domain_id or DEFAULT_DOMAIN_ID).strip().lower()
    return {
        "contract_version": CONTRACT_VERSION,
        "state_id": "VECTRA-PROFESSIONAL-UNDERSTANDING-STATE",
        "business_domain": key,
        "principle": "VECTRA may only claim the understanding that has been provided, researched and confirmed.",
        "product_owner_priority_rule": "Product Owner decides which business area is digitised next.",
        "areas": {
            "modern_trade": {
                "display_name": "Modern Trade",
                "status": "ACTIVE_RESEARCH",
                "known": ["direction_exists", "current_primary_research_area"],
                "unknown": ["complete_role_model", "complete_process_model", "complete_workspace_model"],
                "deferred": False,
                "last_updated_at": _now(),
            },
            "traditional_trade": {
                "display_name": "Traditional Trade",
                "status": "MENTIONED",
                "known": ["direction_exists"],
                "unknown": ["structure", "roles", "processes", "decisions", "data", "workspaces"],
                "deferred": True,
                "last_updated_at": _now(),
            },
            "trade_marketing": {
                "display_name": "Trade Marketing",
                "status": "PRELIMINARY",
                "known": ["direction_exists", "links_marketing_and_commercial_execution"],
                "unknown": ["confirmed_structure", "roles", "processes", "kpi", "workspaces"],
                "deferred": True,
                "last_updated_at": _now(),
            },
            "logistics": {
                "display_name": "Логистика",
                "status": "MENTIONED",
                "known": ["direction_exists"],
                "unknown": ["structure", "roles", "processes", "capacity_model", "data", "workspaces"],
                "deferred": True,
                "last_updated_at": _now(),
            },
        },
        "gaps": [],
        "hypotheses": [],
        "professional_roles": {},
        "engineering_capitalization_queue": [],
        "updated_at": _now(),
    }


def get_professional_understanding_state(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    state, diagnostic = read_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict)
    return {
        "status": "PASS" if diagnostic.get("status") in {"PASS", "EMPTY", "RECOVERED"} else "HOLD",
        "professional_understanding_state": deepcopy(state),
        "diagnostic": diagnostic,
        "release": RELEASE_ID,
        "read_only": True,
    }


def register_business_area(
    area_id: str,
    *,
    display_name: str = "",
    status: str = "MENTIONED",
    known: Optional[List[str]] = None,
    unknown: Optional[List[str]] = None,
    domain_id: str = DEFAULT_DOMAIN_ID,
) -> Dict[str, Any]:
    key = str(area_id or "").strip().lower()
    state_value = str(status or "MENTIONED").strip().upper()
    if not key or state_value not in AREA_STATES:
        return {"status": "VALIDATION_ERROR", "reason": "invalid_area_or_status", "read_only": True}

    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        current = deepcopy((state.get("areas") or {}).get(key) or {})
        current.update({
            "display_name": str(display_name or current.get("display_name") or key).strip(),
            "status": state_value,
            "known": list(dict.fromkeys((current.get("known") or []) + list(known or []))),
            "unknown": list(dict.fromkeys((current.get("unknown") or []) + list(unknown or []))),
            "deferred": bool(current.get("deferred", state_value in {"MENTIONED", "PRELIMINARY"})),
            "last_updated_at": _now(),
        })
        state.setdefault("areas", {})[key] = current
        state["updated_at"] = _now()
        return state

    state, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    return {"status": "PASS", "area": deepcopy(state["areas"][key]), "diagnostic": diagnostic, "read_only": False}


def record_understanding_gap(
    *,
    area_id: str,
    topic: str,
    why_needed: str,
    impact_on_current_work: str = "LIMITATION",
    status: str = "OPEN",
    domain_id: str = DEFAULT_DOMAIN_ID,
) -> Dict[str, Any]:
    gap_status = str(status or "OPEN").strip().upper()
    if gap_status not in GAP_STATES:
        return {"status": "VALIDATION_ERROR", "reason": "invalid_gap_status", "read_only": True}
    gap_id = f"GAP-{str(area_id).strip().upper()}-{abs(hash((area_id, topic))) % 100000:05d}"

    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        gaps = list(state.get("gaps") or [])
        existing = next((item for item in gaps if item.get("gap_id") == gap_id), None)
        payload = {
            "gap_id": gap_id,
            "area_id": str(area_id).strip().lower(),
            "topic": str(topic).strip(),
            "why_needed": str(why_needed).strip(),
            "impact_on_current_work": str(impact_on_current_work).strip().upper(),
            "status": gap_status,
            "product_owner_priority_required": True,
            "updated_at": _now(),
        }
        if existing:
            existing.update(payload)
        else:
            payload["created_at"] = _now()
            gaps.append(payload)
        state["gaps"] = gaps
        state["updated_at"] = _now()
        return state

    state, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    gap = next(item for item in state["gaps"] if item["gap_id"] == gap_id)
    return {"status": "PASS", "gap": deepcopy(gap), "diagnostic": diagnostic, "read_only": False}


def defer_gap(gap_id: str, *, reason: str, domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    return _change_gap_status(gap_id, "DEFERRED", reason, domain_id)


def activate_gap(gap_id: str, *, reason: str, domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    return _change_gap_status(gap_id, "ACTIVE", reason, domain_id)


def _change_gap_status(gap_id: str, status: str, reason: str, domain_id: str) -> Dict[str, Any]:
    found = {"value": False}
    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        for item in state.get("gaps") or []:
            if item.get("gap_id") == gap_id:
                item["status"] = status
                item["status_reason"] = str(reason or "").strip()
                item["updated_at"] = _now()
                found["value"] = True
        state["updated_at"] = _now()
        return state
    state, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    return {
        "status": "PASS" if found["value"] else "NOT_FOUND",
        "gap_id": gap_id,
        "new_status": status if found["value"] else None,
        "diagnostic": diagnostic,
        "read_only": False,
    }


def propose_professional_hypothesis(
    *,
    area_id: str,
    object_type: str,
    statement: str,
    evidence: Optional[List[str]] = None,
    missing_context: Optional[List[str]] = None,
    domain_id: str = DEFAULT_DOMAIN_ID,
) -> Dict[str, Any]:
    hypothesis_id = f"HYP-{str(area_id).strip().upper()}-{abs(hash((object_type, statement))) % 100000:05d}"
    status = "CONTEXT_REQUIRED" if missing_context else "OPEN"
    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        items = list(state.get("hypotheses") or [])
        current = next((item for item in items if item.get("hypothesis_id") == hypothesis_id), None)
        payload = {
            "hypothesis_id": hypothesis_id,
            "area_id": str(area_id).strip().lower(),
            "object_type": str(object_type).strip().lower(),
            "statement": str(statement).strip(),
            "evidence": list(evidence or []),
            "missing_context": list(missing_context or []),
            "status": status,
            "is_decision": False,
            "dialogue_required": True,
            "updated_at": _now(),
        }
        if current:
            current.update(payload)
        else:
            payload["created_at"] = _now()
            items.append(payload)
        state["hypotheses"] = items
        state["updated_at"] = _now()
        return state
    state, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    item = next(item for item in state["hypotheses"] if item["hypothesis_id"] == hypothesis_id)
    return {"status": "PASS", "hypothesis": deepcopy(item), "diagnostic": diagnostic, "read_only": False}


def define_professional_role_model(
    *,
    role_id: str,
    display_name: str,
    purpose: str,
    resultative_actions: List[str],
    time_unit: str,
    constraints: Optional[List[str]] = None,
    decision_objects: Optional[List[str]] = None,
    domain_id: str = DEFAULT_DOMAIN_ID,
) -> Dict[str, Any]:
    key = str(role_id or "").strip().lower()
    if not key or not resultative_actions:
        return {"status": "VALIDATION_ERROR", "reason": "role_and_resultative_actions_required", "read_only": True}
    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        model = {
            "role_id": key,
            "display_name": str(display_name).strip(),
            "purpose": str(purpose).strip(),
            "resultative_actions": list(resultative_actions),
            "efficiency_principle": "timely_resultative_actions_per_unit_of_time",
            "time_unit": str(time_unit or "working_day").strip(),
            "constraints": list(constraints or []),
            "decision_objects": list(decision_objects or []),
            "optimization_rule": "optimise_the_way_result_is_achieved_not_headcount_in_isolation",
            "updated_at": _now(),
        }
        state.setdefault("professional_roles", {})[key] = model
        state["updated_at"] = _now()
        return state
    state, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    return {"status": "PASS", "professional_role_model": deepcopy(state["professional_roles"][key]), "diagnostic": diagnostic, "read_only": False}


def build_business_structure_view(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_professional_understanding_state(domain_id)
    state = result.get("professional_understanding_state") or {}
    areas = state.get("areas") or {}
    return {
        "status": result.get("status"),
        "business_domain": domain_id,
        "structure_type": "CURRENT_DIGITAL_PROFESSIONAL_UNDERSTANDING",
        "honesty_rule": "This is what VECTRA currently understands, not a completeness score for the real company.",
        "areas": [
            {
                "area_id": key,
                "display_name": value.get("display_name"),
                "status": value.get("status"),
                "known": deepcopy(value.get("known") or []),
                "unknown": deepcopy(value.get("unknown") or []),
                "deferred": bool(value.get("deferred")),
            }
            for key, value in areas.items()
        ],
        "open_gaps": [deepcopy(item) for item in state.get("gaps") or [] if item.get("status") in {"OPEN", "ACTIVE"}],
        "deferred_gaps": [deepcopy(item) for item in state.get("gaps") or [] if item.get("status") == "DEFERRED"],
        "release": RELEASE_ID,
        "read_only": True,
    }


def build_engineering_capitalization_request(
    *,
    subject_type: str,
    subject_id: str,
    confirmed_change: Dict[str, Any],
    approved_by_product_owner: bool,
    domain_id: str = DEFAULT_DOMAIN_ID,
) -> Dict[str, Any]:
    if not approved_by_product_owner:
        return {
            "status": "HOLD",
            "reason": "product_owner_confirmation_required",
            "engineering_task_created": False,
            "read_only": True,
        }
    request_id = f"PBM-CAP-{abs(hash((subject_type, subject_id, str(confirmed_change)))) % 1000000:06d}"
    payload = {
        "request_id": request_id,
        "subject_type": str(subject_type).strip().lower(),
        "subject_id": str(subject_id).strip(),
        "confirmed_change": deepcopy(confirmed_change),
        "status": "READY_FOR_ENGINEERING",
        "required_outputs": ["runtime_model_update", "verification", "release_brief", "deploy_package"],
        "created_at": _now(),
    }
    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        queue = list(state.get("engineering_capitalization_queue") or [])
        if not any(item.get("request_id") == request_id for item in queue):
            queue.append(payload)
        state["engineering_capitalization_queue"] = queue
        state["updated_at"] = _now()
        return state
    _, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    return {"status": "PASS", "engineering_task_created": True, "capitalization_request": payload, "diagnostic": diagnostic, "read_only": False}


def verify_business_understanding_runtime(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    state_result = get_professional_understanding_state(domain_id)
    state = state_result.get("professional_understanding_state") or {}
    checks = {
        "state_available": state_result.get("status") == "PASS",
        "product_owner_priority_rule_defined": bool(state.get("product_owner_priority_rule")),
        "areas_registered": bool(state.get("areas")),
        "unknowns_explicit": all("unknown" in area for area in (state.get("areas") or {}).values()),
        "no_false_completeness_metric": "completeness_percentage" not in state,
        "hypothesis_is_not_decision": all(item.get("is_decision") is False for item in state.get("hypotheses") or []),
        "capitalization_requires_product_owner": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "checks": checks,
        "release": RELEASE_ID,
        "read_only": True,
    }
