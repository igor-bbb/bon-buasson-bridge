"""Self-governance runtime for VECTRA product development.

Keeps accepted decisions, unfinished commitments and release gates durable so
important product intent does not disappear when a chat or model changes.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.durable_runtime_state import read_json_state, update_json_state, update_unified_runtime_root

RELEASE_ID = "VECTRA-SELF-GOVERNANCE-001"
CONTRACT_VERSION = "1.0"
STATE_FILE = Path("runtime") / "governance" / "canonical_decisions.json"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _seed() -> Dict[str, Any]:
    decisions: List[Dict[str, Any]] = [
        {
            "decision_id": "CD-001",
            "title": "VECTRA is a continuous professional digital personality",
            "status": "IMPLEMENTED",
            "criticality": "CRITICAL",
            "verification": "Self Audit and session restoration",
        },
        {
            "decision_id": "CD-002",
            "title": "VECTRA is a digital organization, not one universal assistant",
            "status": "APPROVED_FOR_IMPLEMENTATION",
            "criticality": "CRITICAL",
            "verification": "Role inheritance and task flow across digital colleagues",
        },
        {
            "decision_id": "CD-003",
            "title": "Profit and business development are the common business vector",
            "status": "APPROVED_FOR_IMPLEMENTATION",
            "criticality": "CRITICAL",
            "verification": "Decision and recommendation alignment",
        },
        {
            "decision_id": "CD-004",
            "title": "SKU is the business atom; upper levels are aggregations",
            "status": "APPROVED_FOR_IMPLEMENTATION",
            "criticality": "CRITICAL",
            "verification": "Aggregation and investigation model verification",
        },
        {
            "decision_id": "CD-005",
            "title": "Working desktop is a professional briefing, not a dashboard",
            "status": "APPROVED_FOR_IMPLEMENTATION",
            "criticality": "CRITICAL",
            "verification": "Desktop professional acceptance",
        },
        {
            "decision_id": "CD-006",
            "title": "External business environment is mandatory decision context",
            "status": "APPROVED_FOR_IMPLEMENTATION",
            "criticality": "HIGH",
            "verification": "External context appears in relevant desktop and dialogue scenarios",
        },
        {
            "decision_id": "CD-007",
            "title": "Business Domain restores passport, identity, operating and navigation models before data",
            "status": "APPROVED_FOR_IMPLEMENTATION",
            "criticality": "CRITICAL",
            "verification": "New-session Business Domain restoration",
        },
        {
            "decision_id": "CD-008",
            "title": "VECTRA must govern accepted decisions and unfinished work",
            "status": "IN_IMPLEMENTATION",
            "criticality": "CRITICAL",
            "verification": "Governance state persists and blocks unjustified continuation",
        },
        {
            "decision_id": "CD-009",
            "title": "GPT model changes must not change VECTRA identity or obligations",
            "status": "APPROVED_FOR_IMPLEMENTATION",
            "criticality": "HIGH",
            "verification": "Compatibility audit after model change",
        },
    ]
    return {
        "governance_id": "VECTRA-SELF-GOVERNANCE",
        "version": "1.0",
        "status": "ACTIVE",
        "decisions": decisions,
        "deferred_improvements": [
            {
                "improvement_id": "UX-SELF-AUDIT-001",
                "title": "Group capabilities and make next action more professional",
                "status": "DEFERRED",
                "blocking": False,
            }
        ],
        "updated_at": _now(),
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
    }


def read_self_governance_state() -> Dict[str, Any]:
    state, diagnostic = read_json_state(STATE_FILE, _seed, dict)
    return {"status": diagnostic.get("status"), "governance": deepcopy(state), "diagnostic": diagnostic, "read_only": True}


def initialize_self_governance_state() -> Dict[str, Any]:
    def updater(current: Dict[str, Any]) -> Dict[str, Any]:
        state = dict(current or {})
        seed = _seed()
        state.setdefault("governance_id", seed["governance_id"])
        state.setdefault("version", seed["version"])
        state.setdefault("status", "ACTIVE")
        existing = {str(item.get("decision_id")): item for item in state.get("decisions", []) if isinstance(item, dict)}
        for item in seed["decisions"]:
            existing.setdefault(item["decision_id"], item)
        state["decisions"] = list(existing.values())
        state.setdefault("deferred_improvements", seed["deferred_improvements"])
        state["updated_at"] = _now()
        state["release"] = RELEASE_ID
        state["contract_version"] = CONTRACT_VERSION
        return state

    state, diagnostic = update_json_state(STATE_FILE, _seed, dict, updater)
    unified, root_diag = update_unified_runtime_root(
        "governance",
        state,
        status="CONNECTED",
        source_of_truth="app.assistant_runtime.self_governance_runtime",
    )
    return {
        "status": "PASS",
        "governance": deepcopy(state),
        "readback_verified": bool(diagnostic.get("readback_verified")),
        "runtime_root_connected": (unified.get("governance") or {}).get("status") == "CONNECTED",
        "diagnostic": diagnostic,
        "runtime_diagnostic": root_diag,
        "read_only": False,
    }


def get_governance_gate() -> Dict[str, Any]:
    result = initialize_self_governance_state()
    state = result.get("governance") or {}
    decisions = state.get("decisions") if isinstance(state, dict) else []
    open_critical = [
        item for item in decisions or []
        if isinstance(item, dict)
        and item.get("criticality") == "CRITICAL"
        and item.get("status") not in {"IMPLEMENTED", "VERIFIED", "CLOSED"}
    ]
    return {
        "status": "HOLD" if open_critical else "PASS",
        "open_critical_count": len(open_critical),
        "open_critical_decisions": deepcopy(open_critical),
        "continuation_policy": (
            "finish_or_explicitly_defer_open_critical_decisions_before_starting_unrelated_architecture"
            if open_critical else "continue"
        ),
        "read_only": True,
    }
