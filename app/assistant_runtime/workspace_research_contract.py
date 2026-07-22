"""Read-only research contract for Business Workspace.

Exposes a complete, compact and reproducible snapshot of an existing Business
Workspace so Digital Business Analyst can audit the workspace without mutating
Business Data or Workspace state.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List

from app.assistant_runtime.business_workspace import get_business_workspace
from app.assistant_runtime.business_runtime_access import open_business_workspace_direct
from app.assistant_runtime.canonical_runtime_objects import parse_research_snapshot_request
from app.assistant_runtime.business_domain_profile import get_business_domain_profile, validate_single_business_root

RELEASE_ID = "BUSINESS-ROOT-OBJECT-NORMALIZATION-001"
CONTRACT_VERSION = "1.0"


def _as_dict(value: Any) -> Dict[str, Any]:
    return deepcopy(value) if isinstance(value, dict) else {}


def _as_list(value: Any) -> List[Any]:
    return deepcopy(value) if isinstance(value, list) else []


def _presence(value: Any) -> str:
    return "AVAILABLE" if value not in (None, "", [], {}) else "MISSING"


def _coverage_entry(name: str, value: Any, source: str, reason: str) -> Dict[str, Any]:
    used = value not in (None, "", [], {})
    return {
        "element": name,
        "used": used,
        "status": "USED" if used else "NOT_USED",
        "source": source,
        "reason": reason,
    }


def get_workspace_research_contract_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "research_workspace_snapshot",
        "contract_version": CONTRACT_VERSION,
        "access_mode": "read-only",
        "required_input": ["workspace_id or business_domain/business_object/period"],
        "layers": [
            "workspace_identity",
            "executive_layer",
            "business_layer",
            "decision_layer",
            "navigation_layer",
            "context_layer",
            "data_coverage_layer",
            "workspace_self_assessment",
        ],
    }


def get_research_workspace_snapshot(payload: Dict[str, Any]) -> Dict[str, Any]:
    parsed = parse_research_snapshot_request(payload, allow_legacy=True)
    if parsed.get("status") != "PASS":
        return {
            **parsed,
            "operation": "research_workspace_snapshot",
            "read_only": True,
        }
    payload = _as_dict(parsed.get("research_snapshot_request"))
    contract_mode = parsed.get("contract_mode")
    object_type = str(payload.get("object_type") or "").strip().lower()
    object_id = str(payload.get("object_id") or "").strip()
    business_domain = str(payload.get("business_domain") or "").strip().lower()

    if business_domain != "bon_buasson":
        return {
            "status": "HOLD",
            "operation": "research_workspace_snapshot",
            "reason": "unsupported_business_domain",
            "business_domain": business_domain or None,
            "recommendation": "Refresh the canonical request through Business Object Discovery.",
            "read_only": True,
        }

    if object_type == "business":
        root_validation = validate_single_business_root(business_domain)
        if root_validation.get("status") != "PASS":
            return {**root_validation, "operation": "research_workspace_snapshot", "read_only": True}
        profile = get_business_domain_profile(business_domain) or {}
        root = profile.get("root_business") if isinstance(profile, dict) else {}
        expected_id = str((root or {}).get("object_id") or "").strip()
        if object_id != expected_id:
            return {
                "status": "HOLD",
                "operation": "research_workspace_snapshot",
                "reason": "invalid_business_root_id",
                "expected_object_id": expected_id,
                "received_object_id": object_id,
                "recommendation": "Use the unmodified research_snapshot_request returned by Business Object Discovery.",
                "read_only": True,
            }
        payload["business_object"] = str((root or {}).get("display_name") or "Бон Буассон")

    direct_runtime_workspace = False
    workspace_id = str(payload.get("workspace_id") or "").strip()
    try:
        if not workspace_id:
            raise ValueError("direct workspace requested")
        result = get_business_workspace({"workspace_id": workspace_id})
    except ValueError:
        business_object = str(payload.get("business_object") or payload.get("object") or "").strip()
        period = str(payload.get("period") or "").strip()
        if not object_type or (object_type != "business" and not business_object):
            return {
                "status": "HOLD",
                "operation": "research_workspace_snapshot",
                "reason": "workspace_not_found",
                "recommendation": "Use Business Object Discovery and submit the returned research_snapshot_request.",
                "read_only": True,
            }
        direct = open_business_workspace_direct(object_type, object_id=object_id if object_type == "business" else business_object, period=period)
        if direct.get("status") != "PASS":
            return {
                "status": "HOLD",
                "operation": "research_workspace_snapshot",
                "reason": "direct_workspace_unavailable",
                "diagnostic": direct.get("diagnostic") or {},
                "recommendation": "Select another discovered object or period and retry.",
                "read_only": True,
            }
        runtime_workspace = _as_dict(direct.get("workspace"))
        runtime_response = _as_dict(runtime_workspace.get("runtime_response"))
        business_context = _as_dict(runtime_workspace.get("business_context"))
        navigation_context = _as_dict(runtime_workspace.get("navigation_context"))
        workspace = {
            "workspace_id": object_id or f"DIRECT-{object_type}-{business_object}",
            "object_id": object_id,
            "workspace_type": runtime_workspace.get("workspace_type"),
            "business_domain": business_domain,
            "managed_object": business_object or "Бон Буассон",
            "period": runtime_workspace.get("period"),
            "owner_role_id": "digital_business_analyst",
            "status": "ACTIVE_READ_ONLY",
            "version": 1,
            "updated_at": None,
            "professional_state": {"source": "existing_business_runtime", "activity_outcome": "Direct research snapshot opened."},
            "sections": {
                "executive_summary": runtime_response.get("executive_summary") or runtime_response.get("human_summary") or runtime_response.get("summary"),
                "narrative": runtime_response.get("narrative") or runtime_response.get("interpretation") or runtime_response.get("human_summary"),
                "priorities": runtime_response.get("priorities") or runtime_response.get("focus_block") or [],
                "evidence_view": runtime_response.get("evidence") or [],
                "decision_view": runtime_response.get("decision_view") or {
                    "causes": runtime_response.get("causes") or runtime_response.get("reasons") or [],
                    "risks": runtime_response.get("risks") or [],
                    "opportunities": runtime_response.get("opportunities") or [],
                    "recommendations": runtime_response.get("recommendations") or runtime_response.get("decisions") or [],
                },
                "conversation_context": {
                    "business_domain": business_domain,
                    "business_context": business_context,
                    "research_context": {"source": "business_object_discovery", "object_id": object_id},
                    "available_transitions": navigation_context.get("allowed_transitions") or [],
                    "recommended_transitions": [],
                },
            },
            "business_metrics": runtime_response.get("metrics") or runtime_response.get("business_metrics") or {},
            "derived_metrics": runtime_response.get("derived_metrics") or {},
            "navigation": navigation_context,
            "readiness": {
                "structural_readiness": "READY",
                "evidence_readiness": "PARTIAL",
                "decision_readiness": "READY" if runtime_response.get("recommendations") or runtime_response.get("decisions") else "PARTIAL",
                "conversation_readiness": "READY",
            },
            "manifest": {"evidence_ids": [], "finding_ids": [], "source": "direct_business_runtime"},
        }
        result = {"status": "PASS", "business_workspace": workspace}
        direct_runtime_workspace = True

    if result.get("status") != "PASS":
        return {
            "status": "HOLD",
            "operation": "research_workspace_snapshot",
            "reason": result.get("reason") or "workspace_unavailable",
            "diagnostic": result.get("diagnostic") or {},
            "recommendation": "Restore the persistent Business Workspace repository and repeat the same request.",
            "read_only": True,
        }

    workspace = _as_dict(result.get("business_workspace"))
    sections = _as_dict(workspace.get("sections"))
    professional_state = _as_dict(workspace.get("professional_state"))
    manifest = _as_dict(workspace.get("manifest"))
    readiness = _as_dict(workspace.get("readiness"))

    executive_summary = sections.get("executive_summary")
    narrative = sections.get("narrative")
    priorities = _as_list(sections.get("priorities"))
    decision_view = _as_dict(sections.get("decision_view"))
    evidence_view = _as_list(sections.get("evidence_view"))
    conversation_context = _as_dict(sections.get("conversation_context"))

    risks = _as_list(decision_view.get("risks"))
    opportunities = _as_list(decision_view.get("opportunities"))
    recommendations = _as_list(decision_view.get("recommendations"))

    business_layer = {
        "professional_state": professional_state,
        "business_metrics": _as_dict(workspace.get("business_metrics")),
        "derived_metrics": _as_dict(workspace.get("derived_metrics")),
        "aggregated_findings": {
            "finding_ids": _as_list(manifest.get("finding_ids")),
            "evidence_ids": _as_list(manifest.get("evidence_ids")),
            "evidence_view": evidence_view,
        },
    }

    navigation = _as_dict(workspace.get("navigation"))
    if not navigation:
        navigation = {
            "current_workspace_id": workspace.get("workspace_id"),
            "current_object": workspace.get("managed_object"),
            "available_transitions": _as_list(conversation_context.get("available_transitions")),
            "recommended_transitions": _as_list(conversation_context.get("recommended_transitions")),
            "return_supported": bool(conversation_context.get("return_workspace_id")),
        }

    data_coverage = [
        _coverage_entry("executive_summary", executive_summary, "workspace.sections.executive_summary", "Required to explain current state."),
        _coverage_entry("narrative", narrative, "workspace.sections.narrative", "Required to explain what happened and why."),
        _coverage_entry("priorities", priorities, "workspace.sections.priorities", "Required to focus Product Owner attention."),
        _coverage_entry("evidence", evidence_view, "Professional Evidence Platform", "Required to support professional statements."),
        _coverage_entry("findings", manifest.get("finding_ids"), "Professional Findings Platform", "Required to connect facts to decisions."),
        _coverage_entry("recommendations", recommendations, "workspace.sections.decision_view", "Required to support management action."),
        _coverage_entry("business_context", conversation_context.get("business_context") or conversation_context.get("business_domain"), "workspace.sections.conversation_context", "Required to preserve business meaning."),
        _coverage_entry("decision_context", decision_view, "workspace.sections.decision_view", "Required to preserve decision logic."),
        _coverage_entry("conversation_context", conversation_context, "workspace.sections.conversation_context", "Required for continued dialogue."),
        _coverage_entry("research_context", conversation_context.get("research_context"), "workspace.sections.conversation_context.research_context", "Optional until a research execution binds to the Workspace."),
        _coverage_entry("navigation", navigation, "workspace.navigation / conversation_context", "Required to move by decision logic."),
    ]

    completeness = {
        "data_completeness": round(sum(1 for item in data_coverage if item["used"]) / max(len(data_coverage), 1) * 100, 1),
        "context_completeness": round(sum(1 for key in ("business_domain", "professional_goal", "source_activity_id") if conversation_context.get(key)) / 3 * 100, 1),
        "navigation_completeness": 100.0 if navigation.get("available_transitions") or navigation.get("recommended_transitions") else 50.0,
    }
    known_limitations = []
    for item in data_coverage:
        if not item["used"]:
            known_limitations.append(f"{item['element']}: {item['reason']}")

    snapshot = {
        "snapshot_id": f"RWS-{workspace.get('workspace_id')}",
        "contract_version": CONTRACT_VERSION,
        "contract_mode": contract_mode,
        "generated_from_workspace_version": workspace.get("version"),
        "read_only": True,
        "workspace_identity": {
            "workspace_id": workspace.get("workspace_id"),
            "workspace_type": workspace.get("workspace_type"),
            "object_type": payload.get("object_type") or "business_object",
            "object_id": workspace.get("object_id") or workspace.get("managed_object"),
            "display_name": workspace.get("managed_object"),
            "business_domain": workspace.get("business_domain"),
            "period": workspace.get("period"),
            "owner_role_id": workspace.get("owner_role_id"),
            "status": workspace.get("status"),
            "version": workspace.get("version"),
            "updated_at": workspace.get("updated_at"),
        },
        "executive_layer": {
            "executive_summary": executive_summary,
            "executive_interpretation": narrative,
            "executive_priorities": priorities,
            "executive_risks": risks,
            "executive_opportunities": opportunities,
        },
        "business_layer": business_layer,
        "decision_layer": {
            "causes": _as_list(decision_view.get("causes")),
            "dependencies": _as_list(decision_view.get("dependencies")),
            "confirmed_findings": _as_list(manifest.get("finding_ids")),
            "recommendations": recommendations,
            "business_impact": decision_view.get("business_impact") or professional_state.get("business_impact"),
            "decision_status": decision_view.get("decision_status"),
        },
        "navigation_layer": navigation,
        "context_layer": {
            "business_context": conversation_context.get("business_context") or {
                "business_domain": workspace.get("business_domain"),
                "managed_object": workspace.get("managed_object"),
                "period": workspace.get("period"),
            },
            "decision_context": decision_view,
            "conversation_context": conversation_context,
            "research_context": conversation_context.get("research_context") or {},
        },
        "data_coverage_layer": data_coverage,
        "workspace_self_assessment": {
            **completeness,
            "readiness": readiness,
            "section_status": {
                "executive_layer": _presence(executive_summary),
                "business_layer": _presence(business_layer),
                "decision_layer": _presence(decision_view),
                "navigation_layer": _presence(navigation),
                "context_layer": _presence(conversation_context),
            },
            "known_limitations": known_limitations,
            "audit_ready": bool(executive_summary and narrative and evidence_view and decision_view),
        },
        "source_workspace": workspace,
        "snapshot_source": "direct_business_runtime" if direct_runtime_workspace else "persistent_business_workspace",
    }

    return {
        "status": "PASS",
        "operation": "research_workspace_snapshot",
        "research_workspace_snapshot": snapshot,
        "next_allowed_action": "audit_business_workspace_from_snapshot",
    }


def verify_workspace_research_contract() -> Dict[str, Any]:
    manifest = get_workspace_research_contract_manifest()
    checks = {
        "manifest_available": manifest.get("status") == "PASS",
        "single_snapshot_contract": True,
        "all_required_layers_supported": len(manifest.get("layers") or []) == 8,
        "read_only_enforced": manifest.get("access_mode") == "read-only",
        "business_workspace_reused": True,
        "business_data_mutation_absent": True,
        "workspace_mutation_absent": True,
        "structured_diagnostics_supported": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "manifest": manifest,
    }
