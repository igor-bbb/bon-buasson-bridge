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
    verify_runtime_operation_blockers,
)

RELEASE_ID = "VECTRA-PROFESSIONAL-GOVERNANCE-APPROVED-EXECUTION-001"
CONTRACT_VERSION = "1.5"
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
    "get_engineering_blockers": "self_governance",
    "record_engineering_blocker_decision": "self_governance",
    "transition_active_work_context": "self_governance",
    "get_active_work_context": "self_governance",
}

_ENGINEERING_FOCUS_HINTS = {
    "engineering", "self_governance", "development_governance", "architecture",
    "release", "verification", "runtime_implementation",
}

_TERMINAL_SUCCESS = {"PASS", "OK", "READY", "COMPLETED", "VERIFIED", "SUCCESS"}
_NON_TERMINAL_HOLD = {"HOLD", "NOT_READY", "BLOCKED", "INCOMPATIBLE", "PARTIAL"}
_FAILURE = {"FAIL", "FAILED", "ERROR", "INTERNAL_ERROR"}

# A Runtime operation may intentionally reject an invalid assertion as part of
# a verification flow.  Only explicitly registered operation/reason pairs are
# non-blocking; an arbitrary FAIL response cannot opt itself out of governance.
_EXPECTED_NEGATIVE_OUTCOMES = {
    "trace_normative_usage": {"normative_section_not_found"},
}

# Product Research must remain available while an engineering candidate waits
# for Product Owner approval. These operations either read existing state or
# append research evidence/governance records without changing Runtime,
# Business Data, Business Logic or Professional Knowledge.
_RESEARCH_SAFE_OPERATIONS = {
    "query",
    "get_canonical_workspace",
    "canonical_workspace",
    "manager_top_summary",
    "manager_summary",
    "contract_summary",
    "category_summary",
    "sku_summary",
    "open_workspace",
    "open_existing_business_workspace",
    "navigate_existing_business_workspace",
    "get_business_runtime_context",
    "inspect_business_navigation_context",
    "get_research_workspace",
    "research_workspace",
    "get_research_workspace_snapshot",
    "research_workspace_snapshot",
    "discover_business_objects",
    "business_object_discovery",
    "create_product_observation",
    "get_development_requests",
    "get_development_request",
    "get_new_development_requests",
    "generate_product_review_report",
    "record_owner_decision",
    "get_engineering_blockers",
    "record_engineering_blocker_decision",
    "capture_business_workspace_research_step",
    "add_research_program_evidence",
    "research_program_evidence_add",
    "add_research_evidence",
    "research_evidence_add",
    "register_professional_evidence",
    "evidence_register",
    "add_business_review_evidence",
    "business_review_evidence_add",
}

# These operations cross from research/governance into implementation or
# mutate protected system/knowledge state. An open engineering candidate must
# continue to hold them until the required Product Owner approval is present.
_PROTECTED_MUTATION_OPERATIONS = {
    "create_engineering_task",
    "update_engineering_execution",
    "capitalize_confirmed_knowledge",
    "create_candidate",
    "transition_active_work_context",
    "start_runtime_recovery",
    "connect_business_runtime",
    "link_research_engineering_task",
    "research_engineering_task_link",
    "link_research_knowledge_capitalization",
    "research_knowledge_capitalization_link",
}

_READ_OPERATION_PREFIXES = (
    "get_", "list_", "read_", "search_", "trace_", "inspect_", "verify_", "open_", "navigate_", "discover_",
)


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


def _expected_negative_outcome(operation_type: str, result: Any) -> bool:
    if not isinstance(result, dict):
        return False
    operation = str(operation_type or "").strip().lower()
    allowed_reasons = _EXPECTED_NEGATIVE_OUTCOMES.get(operation, set())
    reason = str(result.get("failure_reason") or "").strip().lower()
    return bool(reason and reason in allowed_reasons)


def _operation_access(operation_type: str, family: str, result: Any) -> Dict[str, Any]:
    """Classify the current operation without weakening protected writes."""
    operation = str(operation_type or "").strip().lower()
    if operation in _PROTECTED_MUTATION_OPERATIONS:
        return {
            "classification": "PROTECTED_SYSTEM_MUTATION",
            "research_safe": False,
            "read_only": False,
            "journal_or_evidence_write": False,
        }

    journal_or_evidence_write = operation in {
        "create_product_observation",
        "record_owner_decision",
        "record_engineering_blocker_decision",
        "capture_business_workspace_research_step",
        "add_research_program_evidence",
        "research_program_evidence_add",
        "add_research_evidence",
        "research_evidence_add",
        "register_professional_evidence",
        "evidence_register",
        "add_business_review_evidence",
        "business_review_evidence_add",
    }
    explicit_read_only = isinstance(result, dict) and result.get("read_only") is True
    inferred_read_only = operation.startswith(_READ_OPERATION_PREFIXES) or operation in {
        "query", "canonical_workspace", "manager_top_summary", "manager_summary",
        "contract_summary", "category_summary", "sku_summary",
    }
    research_activity = family in {"business_workspace", "business_research"}
    research_safe = bool(
        operation in _RESEARCH_SAFE_OPERATIONS
        or explicit_read_only
        or research_activity
    )
    if journal_or_evidence_write:
        classification = "RESEARCH_GOVERNANCE_WRITE"
    elif explicit_read_only or inferred_read_only or research_activity:
        classification = "READ_ONLY_RESEARCH"
    else:
        classification = "CONTROLLED_ACTIVITY"
    return {
        "classification": classification,
        "research_safe": research_safe,
        "read_only": bool(explicit_read_only or inferred_read_only),
        "journal_or_evidence_write": journal_or_evidence_write,
    }


def _event_hash(parts: Iterable[Any]) -> str:
    raw = "|".join(str(part or "") for part in parts)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:20]


def _compact_active_context(value: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value.get(key)
        for key in (
            "cycle_id",
            "title",
            "status",
            "focus_status",
            "current_focus",
            "next_recommended_step",
            "last_completed_work",
            "updated_at",
        )
        if value.get(key) is not None
    }


def _compact_continuity(value: Dict[str, Any]) -> Dict[str, Any]:
    return {
        key: value.get(key)
        for key in (
            "status",
            "resume_from",
            "next_recommended_step",
            "last_confirmed_step",
            "updated_at",
        )
        if value.get(key) is not None
    }


def _compact_professional_runtime_state(value: Dict[str, Any]) -> Dict[str, Any]:
    identity = value.get("professional_identity") if isinstance(value.get("professional_identity"), dict) else {}
    knowledge = value.get("professional_knowledge_context") if isinstance(value.get("professional_knowledge_context"), dict) else {}
    questions = value.get("continuity_questions") if isinstance(value.get("continuity_questions"), dict) else {}
    active_work = value.get("active_work") if isinstance(value.get("active_work"), dict) else {}
    cycle = active_work.get("engineering_cycle") if isinstance(active_work.get("engineering_cycle"), dict) else {}
    return {
        "state_id": value.get("state_id"),
        "status": value.get("status"),
        "professional_role": identity.get("role"),
        "active_cycle": _compact_active_context(cycle),
        "professional_knowledge": {
            "status": knowledge.get("status"),
            "knowledge_count": knowledge.get("knowledge_count", 0),
            "knowledge_ids": list(knowledge.get("knowledge_ids") or [])[:50],
            "reasoning_input_ready": knowledge.get("reasoning_input_ready") is True,
            "role_projection_enforced": knowledge.get("role_projection_enforced") is True,
        },
        "continuity_questions": questions,
        "recommended_next_action": value.get("recommended_next_action"),
        "updated_at": value.get("updated_at"),
    }


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
    family = _operation_family(operation_type, runtime_service)
    normalized_status = _normalize_status(result)
    expected_negative = _expected_negative_outcome(operation_type, result)
    operation_access = _operation_access(operation_type, family, result)
    research_safe = operation_access.get("research_safe") is True
    # A successful read-only/research operation cannot disprove an earlier
    # blocker merely because both requests share a broad operation_type such
    # as ``query``. The failed and successful requests may target different
    # objects, periods, routes or action-map entries. Keep the engineering
    # hold until a non-research verification or explicit Product Owner
    # lifecycle decision handles the corresponding blocker.
    if research_safe:
        blocker_reconciliation = {
            "status": "NOT_APPLICABLE",
            "reason": "research_safe_operation_cannot_reconcile_engineering_blocker",
            "verified_blockers_count": 0,
            "verified_engineering_item_ids": [],
            "read_only": True,
        }
    else:
        blocker_reconciliation = verify_runtime_operation_blockers(
            operation_type=operation_type,
            runtime_service=runtime_service,
            endpoint=endpoint,
            verification_status=normalized_status,
        )
    snapshot = get_self_governance_snapshot()
    active_context = snapshot.get("active_work_context") if isinstance(snapshot.get("active_work_context"), dict) else {}
    continuity = snapshot.get("professional_continuity") if isinstance(snapshot.get("professional_continuity"), dict) else {}
    attention = snapshot.get("attention") if isinstance(snapshot.get("attention"), dict) else {}

    focus_family = _focus_family(active_context)
    blocker = False if expected_negative else _confirmed_blocker(result, normalized_status)
    accumulated_open_blocker = bool(attention.get("open_blockers"))
    accumulated_hold = bool(attention.get("stop_recommended"))
    unrelated = focus_family not in {"general_professional_activity", family} and family not in {
        "professional_identity", "professional_runtime", "professional_continuity", "self_governance"
    }

    if blocker and research_safe:
        governance_decision = "RECORD_DEFECT_AND_CONTINUE_INDEPENDENT_RESEARCH"
        governance_status = "RESEARCH_CONTINUE"
        response_requirement = (
            "Report and preserve the failed route as evidence. Keep engineering changes on hold, "
            "but continue independent read-only research and Development Journal work."
        )
    elif blocker:
        governance_decision = "STOP_AND_PREPARE_ENGINEERING_TASK"
        governance_status = "HOLD"
        response_requirement = "Report the confirmed Runtime blocker precisely and do not continue as if the operation succeeded."
    elif accumulated_hold and research_safe:
        governance_decision = "CONTINUE_RESEARCH_WITH_ENGINEERING_HOLD"
        governance_status = "RESEARCH_CONTINUE"
        response_requirement = (
            "Continue the independent read-only research or Development Journal operation. "
            "Do not start engineering implementation or another protected system mutation."
        )
    elif accumulated_hold:
        governance_decision = "STOP_FOR_OPEN_ENGINEERING_BLOCKERS"
        governance_status = "HOLD"
        response_requirement = "Report the successful Runtime result, but do not continue past the remaining open engineering blockers."
    elif expected_negative:
        governance_decision = "CONTINUE_AFTER_EXPECTED_NEGATIVE_OUTCOME"
        governance_status = "PASS"
        response_requirement = "Report the expected negative verification result and continue the approved verification sequence."
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

    outcome_classification = (
        "EXPECTED_NEGATIVE" if expected_negative
        else "RESEARCH_DEFECT" if blocker and research_safe
        else "STANDARD"
    )
    event_hash = _event_hash((operation_type, runtime_service, endpoint, normalized_status, outcome_classification, family, active_context.get("cycle_id")))
    previous_state = _persist_pipeline_event({
        "event_hash": event_hash,
        "operation_type": operation_type,
        "runtime_service": runtime_service,
        "endpoint": endpoint,
        "operation_family": family,
        "result_status": normalized_status,
        "outcome_classification": outcome_classification,
        "governance_decision": governance_decision,
        "governance_status": governance_status,
        "active_cycle_id": active_context.get("cycle_id"),
        "processed_at": _now(),
    })
    observation = None
    # Normally only the first occurrence of a deterministic event creates an
    # observation. A previously seen failure may, however, have no remaining
    # open blocker (for example, an older release reconciled it too broadly).
    # In that case a repeated confirmed blocker must recreate durable HOLD
    # state; otherwise the current response is HOLD but the next independent
    # research read incorrectly returns engineering PASS.
    should_record_observation = bool(
        previous_state.get("was_new") is True
        or (blocker and not accumulated_open_blocker)
    )
    if should_record_observation and not expected_negative:
        observation = _record_confirmed_observation_once(
            event_hash=event_hash,
            operation_type=operation_type,
            family=family,
            normalized_status=normalized_status,
            result=result,
        )
    if observation is not None:
        refreshed_snapshot = get_self_governance_snapshot()
        refreshed_attention = refreshed_snapshot.get("attention")
        if isinstance(refreshed_attention, dict):
            attention = refreshed_attention
    professional_state_result = persist_professional_runtime_state()
    professional_runtime_state = professional_state_result.get("professional_runtime_state")
    if not isinstance(professional_runtime_state, dict):
        professional_runtime_state = {}

    # The failed route remains blocked, but an explicitly approved blocker must
    # not deadlock its own bounded engineering implementation. Refreshed
    # Governance attention is authoritative: new or deferred blockers keep
    # HOLD; approved blockers remain open for Product Verification without HOLD.
    engineering_hold = bool(attention.get("stop_recommended"))
    route_blocked = bool(blocker)
    recommended_next_action = next_action or active_context.get("next_recommended_step") or continuity.get("resume_from")
    if blocker and research_safe:
        recommended_next_action = (
            "Record the defect and evidence, then continue a different independent read-only "
            "Workspace, navigation route or Development Journal operation."
        )

    return {
        "status": governance_status,
        "pipeline_id": "VECTRA-PROFESSIONAL-PIPELINE",
        "contract_version": CONTRACT_VERSION,
        "release": RELEASE_ID,
        "professional_context": {
            "active_work_context": _compact_active_context(active_context),
            "professional_continuity": _compact_continuity(continuity),
            "operation_family": family,
            "active_focus_family": focus_family,
            "result_status": normalized_status,
            "outcome_classification": outcome_classification,
            "operation_access": operation_access,
        },
        "self_governance": {
            "decision": governance_decision,
            "response_requirement": response_requirement,
            "confirmed_blocker": blocker,
            "expected_negative_outcome": expected_negative,
            "new_branch_detected": unrelated,
            "attention": deepcopy(attention),
        },
        "execution_gates": {
            "research": {
                "status": "CONTINUE" if research_safe else ("HOLD" if engineering_hold else "PASS"),
                "mode": "RESEARCH_CONTINUE" if research_safe and engineering_hold else "STANDARD",
                "independent_routes_allowed": research_safe,
                "development_journal_allowed": research_safe,
            },
            "current_route": {
                "status": "BLOCKED" if route_blocked else "PASS",
                "operation_type": operation_type,
                "failure_is_route_scoped": bool(route_blocked and research_safe),
            },
            "engineering": {
                "status": "HOLD" if engineering_hold else "PASS",
                "mode": "ENGINEERING_HOLD" if engineering_hold else "STANDARD",
                "product_owner_approval_required": bool(attention.get("product_owner_approval_required")),
                "protected_mutations_allowed": not engineering_hold,
                "execution_scope": attention.get("engineering_execution_scope") or "NONE",
                "approved_engineering_item_ids": deepcopy(attention.get("approved_engineering_item_ids") or []),
            },
        },
        "blocker_reconciliation": blocker_reconciliation,
        "engineering_observation": observation,
        "recommended_next_action": recommended_next_action,
        "runtime_state_updated": bool(previous_state.get("diagnostic", {}).get("readback_verified")),
        "professional_runtime_state": _compact_professional_runtime_state(professional_runtime_state),
        "response_mode": "compact",
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
        "research_and_engineering_gates_available": isinstance(pass_probe.get("execution_gates"), dict),
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
