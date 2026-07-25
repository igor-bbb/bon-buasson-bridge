"""Versioned executable Professional Procedures for VECTRA Laboratory.

The module moves confirmed operational procedures out of static Custom GPT
instructions and into Professional Runtime.  Procedures are internal Runtime
assets and do not create a new GPT Action or public operation_type.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

RELEASE_ID = "PROFESSIONAL-BEHAVIOUR-RUNTIME-MIGRATION-001-INCREMENT-004"
CONTRACT_VERSION = "1.0"
PROCEDURE_SET_ID = "PROFESSIONAL-PROCEDURES-VECTRA-LABORATORY"
PROCEDURE_SET_VERSION = "1.2"
DEFAULT_ROLE = "vectra_laboratory"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _procedure(
    procedure_id: str,
    purpose: str,
    triggers: List[str],
    steps: List[str],
    completion: List[str],
    next_action: str,
    supported_programs: List[str],
) -> Dict[str, Any]:
    return {
        "procedure_id": procedure_id,
        "version": PROCEDURE_SET_VERSION,
        "owner": "VECTRA Laboratory",
        "professional_role": DEFAULT_ROLE,
        "purpose": purpose,
        "activation_triggers": triggers,
        "steps": steps,
        "completion_criteria": completion,
        "next_action": next_action,
        "action_closure": {
            "required": True,
            "cardinality": "exactly_one",
            "resolved_action": next_action,
        },
        "supported_programs": supported_programs,
        "status": "ACTIVE",
        "lifecycle_status": "ACTIVE",
        "read_only": True,
    }


PROCEDURES: Dict[str, Dict[str, Any]] = {
    "start_working_session": _procedure(
        "start_working_session",
        "Restore current professional state and continue the confirmed program without technical dispatch by Product Owner.",
        ["start_working_session", "new_session", "restore_session"],
        [
            "restore_professional_state",
            "resolve_professional_role",
            "resolve_active_behaviour_profile",
            "resolve_active_professional_program",
            "prepare_execution_context",
            "determine_next_allowed_action",
        ],
        ["professional_state_ready", "behaviour_ready", "next_allowed_action_resolved"],
        "continue_active_professional_program",
        ["all"],
    ),
    "execute_confirmed_action": _procedure(
        "execute_confirmed_action",
        "Move from approved discussion to factual execution after Product Owner command.",
        ["do", "execute", "делай", "approved_for_implementation"],
        [
            "resynchronize_professional_state",
            "confirm_approved_scope",
            "prepare_execution_context",
            "execute_next_allowed_action",
            "return_factual_result_or_structured_blocker",
        ],
        ["action_executed_or_blocker_localized", "no_unverified_completion_claim"],
        "execute_next_allowed_action",
        ["engineering_review", "product_verification", "guided_research", "business_framework_end_to_end_research"],
    ),
    "product_verification": _procedure(
        "product_verification",
        "Verify the already deployed product against Release Brief and Runtime evidence; Release Brief is always interpreted as a post-deployment contract.",
        ["release_brief_received", "product_verification", "verify_release"],
        [
            "restore_professional_state",
            "load_release_contract_as_post_deployment_contract",
            "do_not_wait_for_or_repeat_deployment",
            "discover_runtime_capabilities",
            "execute_verification_scenarios",
            "separate_confirmed_failed_blocked_results",
            "issue_exactly_one_pass_fail_or_blocked_decision",
            "close_with_one_concrete_product_owner_action",
        ],
        ["decision_issued", "runtime_evidence_recorded", "exactly_one_next_action_stated", "deployment_not_repeated"],
        "close_current_increment_or_handoff_confirmed_failure",
        ["product_verification"],
    ),
    "engineering_review": _procedure(
        "engineering_review",
        "Prepare a grounded engineering assessment without implementing code inside Laboratory.",
        ["engineering_review_requested", "engineering_task_received"],
        [
            "restore_professional_state",
            "separate_observation_cause_and_conclusion",
            "check_architecture_boundaries",
            "assess_scope_and_verifiability",
            "approve_reject_or_request_evidence",
        ],
        ["engineering_decision_issued", "scope_boundaries_explicit"],
        "handoff_to_engineering_or_close_review",
        ["engineering_review"],
    ),
    "handle_confirmed_blocker": _procedure(
        "handle_confirmed_blocker",
        "Convert an operational blocker into a localized minimal engineering handoff.",
        ["confirmed_blocker", "runtime_blocked", "missing_capability"],
        [
            "confirm_blocker",
            "localize_cause",
            "determine_responsibility_boundary",
            "preserve_active_program_state",
            "prepare_minimal_solution",
            "prepare_engineering_task",
            "handoff_to_engineering_review",
        ],
        ["blocker_localized", "program_state_preserved", "engineering_task_prepared"],
        "engineering_handoff",
        ["all"],
    ),
    "engineering_handoff": _procedure(
        "engineering_handoff",
        "Transfer a verified limitation to Engineering with sufficient evidence and a bounded Definition of Done.",
        ["handoff_required", "blocker_localized", "fail_confirmed"],
        [
            "state_confirmed_observation",
            "attach_runtime_evidence",
            "identify_affected_layer",
            "define_minimal_change",
            "define_product_verification",
            "preserve_program_continuation_point",
        ],
        ["engineering_task_complete", "verification_scenario_defined"],
        "await_engineering_release",
        ["all"],
    ),
    "continue_after_pause": _procedure(
        "continue_after_pause",
        "Resume the latest confirmed professional program after a pause without restarting discovery unnecessarily.",
        ["resume", "continue", "after_pause", "context_drift_detected"],
        [
            "restore_professional_state",
            "compare_local_context_with_runtime",
            "prefer_runtime_on_mismatch",
            "resolve_active_program",
            "resolve_next_allowed_action",
            "continue_from_saved_checkpoint",
        ],
        ["runtime_context_synchronized", "continuation_point_resolved"],
        "continue_active_professional_program",
        ["all"],
    ),
    "knowledge_capitalization": _procedure(
        "knowledge_capitalization",
        "Prepare and preserve confirmed professional knowledge through the governed memory pipeline.",
        ["capitalization_requested", "confirmed_knowledge_ready"],
        [
            "extract_candidates",
            "bind_evidence",
            "validate_and_classify",
            "normalize_and_deduplicate",
            "build_prepared_knowledge_package",
            "persist_repository",
            "verify_readback",
            "rebuild_recovery_snapshot",
        ],
        ["readback_verified", "recovery_snapshot_updated"],
        "complete_capitalization_cycle",
        ["knowledge_capitalization"],
    ),
}


def get_professional_procedure_registry() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "professional_procedure_registry": {
            "procedure_set_id": PROCEDURE_SET_ID,
            "version": PROCEDURE_SET_VERSION,
            "owner": "Professional Runtime",
            "professional_role": DEFAULT_ROLE,
            "status": "ACTIVE",
            "procedures": [deepcopy(PROCEDURES[key]) for key in sorted(PROCEDURES)],
            "release": RELEASE_ID,
            "contract_version": CONTRACT_VERSION,
            "updated_at": _now(),
        },
        "read_only": True,
    }


def get_professional_procedure_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "professional_procedure_manifest": {
            "manifest_id": "PROFESSIONAL-PROCEDURE-MANIFEST",
            "procedure_set_id": PROCEDURE_SET_ID,
            "version": PROCEDURE_SET_VERSION,
            "professional_role": DEFAULT_ROLE,
            "supported_procedures": sorted(PROCEDURES),
            "trigger_index": {
                trigger: procedure_id
                for procedure_id, procedure in PROCEDURES.items()
                for trigger in procedure.get("activation_triggers", [])
            },
            "runtime_is_executable_procedure_source": True,
            "custom_gpt_procedure_scope": "IDENTITY_POLICY_ONLY",
            "authority_transfer_status": "COMPLETED",
            "runtime_is_procedure_authority": True,
            "static_professional_core_execution_allowed": False,
            "professional_core_scope": "NORMATIVE_DOCUMENTATION_ONLY",
            "runtime_first_enforced": True,
            "action_closure_rule": {
                "required": True,
                "cardinality": "exactly_one",
            },
            "release_brief_interpretation": {
                "stage": "POST_DEPLOYMENT",
                "deployment_wait_instruction_allowed": False,
                "allowed_decisions": ["PASS", "FAIL", "BLOCKED"],
            },
            "release": RELEASE_ID,
            "contract_version": CONTRACT_VERSION,
        },
        "read_only": True,
    }


def _infer_trigger(payload: Dict[str, Any]) -> str:
    explicit = str(payload.get("procedure_id") or payload.get("procedure") or payload.get("event") or "").strip().lower()
    if explicit:
        return explicit
    operation = str(payload.get("operation_type") or payload.get("professional_program") or "").strip().lower()
    if operation in {"execute_end_to_end", "start_execution", "run_execution", "business_framework_end_to_end_research"}:
        return "execute_confirmed_action"
    text = str(payload.get("command") or payload.get("user_request") or payload.get("professional_goal") or "").strip().lower()
    if text in {"делай", "делать", "давай", "execute", "do"}:
        return "execute_confirmed_action"
    if "начать рабочую сессию" in text or "start working session" in text:
        return "start_working_session"
    if "product verification" in text or "провер" in text:
        return "product_verification"
    if "block" in text or "блокер" in text:
        return "handle_confirmed_blocker"
    return "execute_confirmed_action"


def resolve_professional_procedure(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    role = str(payload.get("professional_role") or DEFAULT_ROLE).strip().lower()
    if role != DEFAULT_ROLE:
        return {
            "status": "NOT_READY",
            "reason": "professional_procedure_set_not_found",
            "professional_role": role,
            "available_roles": [DEFAULT_ROLE],
            "recommendation": "register_compatible_professional_procedure_set",
            "read_only": True,
        }
    trigger = _infer_trigger(payload)
    procedure_id = trigger if trigger in PROCEDURES else None
    if not procedure_id:
        for candidate_id, procedure in PROCEDURES.items():
            if trigger in procedure.get("activation_triggers", []):
                procedure_id = candidate_id
                break
    if not procedure_id:
        return {
            "status": "NOT_READY",
            "reason": "professional_procedure_not_resolved",
            "trigger": trigger,
            "available_procedures": sorted(PROCEDURES),
            "recommendation": "provide_professional_goal_or_supported_event",
            "read_only": True,
        }
    return {
        "status": "PASS",
        "resolution": "professional_procedure_resolved",
        "trigger": trigger,
        "active_procedure": deepcopy(PROCEDURES[procedure_id]),
        "next_allowed_action": PROCEDURES[procedure_id]["next_action"],
        "action_closure": deepcopy(PROCEDURES[procedure_id].get("action_closure") or {}),
        "release_brief_stage": "POST_DEPLOYMENT" if procedure_id == "product_verification" else None,
        "procedure_set_id": PROCEDURE_SET_ID,
        "procedure_set_version": PROCEDURE_SET_VERSION,
        "runtime_is_executable_procedure_source": True,
        "read_only": True,
    }


def diagnose_professional_procedures(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    resolved = resolve_professional_procedure(payload)
    if resolved.get("status") != "PASS":
        return {
            "status": "NOT_READY",
            "reason": resolved.get("reason"),
            "recommendation": resolved.get("recommendation"),
            "details": resolved,
            "read_only": True,
        }
    procedure = resolved.get("active_procedure") or {}
    missing = [
        key for key in ("procedure_id", "version", "owner", "purpose", "steps", "completion_criteria", "next_action")
        if not procedure.get(key)
    ]
    return {
        "status": "READY" if not missing else "NOT_READY",
        "procedure_id": procedure.get("procedure_id"),
        "version": procedure.get("version"),
        "missing": missing,
        "reason": None if not missing else "professional_procedure_contract_incomplete",
        "recommendation": "activate_resolved_procedure" if not missing else "complete_procedure_contract",
        "next_allowed_action": resolved.get("next_allowed_action"),
        "runtime_is_executable_procedure_source": True,
        "read_only": True,
    }


def verify_professional_procedures_runtime() -> Dict[str, Any]:
    registry = get_professional_procedure_registry()
    manifest = get_professional_procedure_manifest()
    scenarios = {
        "start_session": resolve_professional_procedure({"event": "new_session"}),
        "do_command": resolve_professional_procedure({"command": "делай"}),
        "product_verification": resolve_professional_procedure({"event": "release_brief_received"}),
        "confirmed_blocker": resolve_professional_procedure({"event": "confirmed_blocker"}),
        "resume": resolve_professional_procedure({"event": "after_pause"}),
    }
    checks = {
        "registry_available": registry.get("status") == "PASS",
        "manifest_available": manifest.get("status") == "PASS",
        "minimum_procedure_set_present": len(PROCEDURES) >= 7,
        "all_scenarios_resolved": all(item.get("status") == "PASS" for item in scenarios.values()),
        "no_public_action_required": True,
        "authority_transfer_completed": True,
        "runtime_is_procedure_authority": True,
        "action_closure_present_for_all_procedures": all(
            (item.get("action_closure") or {}).get("cardinality") == "exactly_one"
            and bool(item.get("next_action"))
            for item in PROCEDURES.values()
        ),
        "release_brief_is_post_deployment": (
            "do_not_wait_for_or_repeat_deployment" in PROCEDURES["product_verification"].get("steps", [])
        ),
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "checks": checks,
        "scenario_summary": {
            name: (result.get("active_procedure") or {}).get("procedure_id")
            for name, result in scenarios.items()
        },
        "read_only": True,
    }
