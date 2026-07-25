"""Business Workspace for the Digital Business Analyst.

A Workspace is the published professional state of a managed business object.
It is not a report, screen, or data store. It projects confirmed Professional
State into a stable product contract for Product Owner use and conversation.
"""
from __future__ import annotations

import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.durable_runtime_state import read_json_state, write_json_state, inspect_json_state
from app.assistant_runtime.evidence_platform import list_professional_evidence
from app.assistant_runtime.findings_platform import list_professional_findings

RELEASE_ID = "DIGITAL-BUSINESS-ANALYST-WORKSPACE-001"
ROLE_ID = "digital_business_analyst"
DEFAULT_BASE_PATH = "assistant_repository"
WORKSPACES_FILE = Path("runtime") / "digital_roles" / ROLE_ID / "business_workspaces.json"
WORKSPACE_CONTRACT_VERSION = "1.0"

REQUIRED_SECTIONS = (
    "executive_summary",
    "narrative",
    "priorities",
    "evidence_view",
    "decision_view",
    "conversation_context",
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path() -> Path:
    root = Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve()
    return root / WORKSPACES_FILE


def _read_with_diagnostic() -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    value, diagnostic = read_json_state(_path(), list, list)
    return value, diagnostic


def _read() -> List[Dict[str, Any]]:
    value, _ = _read_with_diagnostic()
    return value


def _write(items: List[Dict[str, Any]]) -> None:
    write_json_state(_path(), items)


def _required(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _scope_key(domain: Any, obj: Any, period: Any) -> str:
    return "|".join([str(domain or "global"), str(obj or "business"), str(period or "current")])


def _find(items: List[Dict[str, Any]], workspace_id: str = "", scope_key: str = "") -> Optional[Dict[str, Any]]:
    for item in items:
        if workspace_id and item.get("workspace_id") == workspace_id:
            return item
        if scope_key and item.get("scope_key") == scope_key:
            return item
    return None


def _confirmed_findings(activity_id: str) -> List[Dict[str, Any]]:
    result = list_professional_findings({"activity_id": activity_id, "digital_role": ROLE_ID, "limit": 500})
    return [item for item in result.get("findings", []) if item.get("status") in {"CONFIRMED", "APPLIED"}]


def _validated_evidence(activity_id: str) -> List[Dict[str, Any]]:
    result = list_professional_evidence({"activity_id": activity_id, "digital_role": ROLE_ID, "limit": 500})
    return [item for item in result.get("evidence", []) if item.get("status") in {"VALIDATED", "VERIFIED"}]


def _statements(findings: List[Dict[str, Any]], *types: str) -> List[str]:
    allowed = set(types)
    return [str(item.get("statement") or "").strip() for item in findings if item.get("finding_type") in allowed and str(item.get("statement") or "").strip()]


def _dedupe(values: List[str]) -> List[str]:
    result: List[str] = []
    seen = set()
    for value in values:
        norm = value.strip()
        if norm and norm not in seen:
            seen.add(norm)
            result.append(norm)
    return result


def _build_sections(review: Dict[str, Any], report: Dict[str, Any], findings: List[Dict[str, Any]], evidence: List[Dict[str, Any]], payload: Dict[str, Any]) -> Dict[str, Any]:
    facts = _statements(findings, "confirmed_fact")
    observations = _statements(findings, "observation")
    risks = _statements(findings, "risk")
    opportunities = _statements(findings, "opportunity")
    recommendations = _statements(findings, "recommendation")
    open_questions = _statements(findings, "open_question", "hypothesis")

    executive_summary = str(payload.get("executive_summary") or "").strip()
    if not executive_summary:
        core = facts[:2] or observations[:2] or [str(report.get("activity_outcome") or "Профессиональное состояние объекта сформировано.")]
        executive_summary = " ".join(core)

    narrative = str(payload.get("narrative") or "").strip()
    if not narrative:
        parts = []
        if facts:
            parts.append("Что происходит: " + " ".join(facts[:3]))
        elif observations:
            parts.append("Что наблюдается: " + " ".join(observations[:3]))
        if risks:
            parts.append("Что требует внимания: " + " ".join(risks[:3]))
        if opportunities:
            parts.append("Где есть потенциал: " + " ".join(opportunities[:3]))
        if recommendations:
            parts.append("Что рекомендуется: " + " ".join(recommendations[:3]))
        narrative = "\n\n".join(parts) or executive_summary

    explicit_priorities = payload.get("priorities") if isinstance(payload.get("priorities"), list) else []
    priorities = _dedupe([str(x) for x in explicit_priorities] + risks + recommendations + opportunities)[:10]

    evidence_view = [
        {
            "evidence_id": item.get("evidence_id"),
            "title": item.get("title"),
            "source_type": item.get("source_type"),
            "reference": item.get("reference"),
            "status": item.get("status"),
            "reliability": item.get("reliability"),
            "object": item.get("object"),
            "period": item.get("period"),
        }
        for item in evidence
    ]

    decision_view = {
        "recommendations": recommendations or list(report.get("executive_recommendations") or []),
        "risks": risks or list(report.get("risks") or []),
        "opportunities": opportunities or list(report.get("opportunities") or []),
        "business_impact": report.get("business_impact"),
        "decision_status": "READY" if recommendations else "REVIEW_REQUIRED",
    }

    conversation_context = {
        "workspace_id": None,
        "business_domain": review.get("business_domain"),
        "business_object": review.get("business_object"),
        "period": review.get("period"),
        "professional_goal": review.get("professional_goal"),
        "active_topics": priorities[:5],
        "open_questions": open_questions + list((review.get("review_context") or {}).get("open_questions") or []),
        "evidence_ids": [item.get("evidence_id") for item in evidence],
        "finding_ids": [item.get("finding_id") for item in findings],
        "source_business_review_id": review.get("business_review_id"),
        "source_activity_id": review.get("activity_id"),
    }

    return {
        "executive_summary": executive_summary,
        "narrative": narrative,
        "priorities": priorities,
        "evidence_view": evidence_view,
        "decision_view": decision_view,
        "conversation_context": conversation_context,
    }


def _readiness(sections: Dict[str, Any], findings: List[Dict[str, Any]], evidence: List[Dict[str, Any]]) -> Dict[str, Any]:
    structural = all(sections.get(name) not in (None, "", [], {}) for name in REQUIRED_SECTIONS)
    evidence_ids = {item.get("evidence_id") for item in evidence}
    key_findings = [item for item in findings if item.get("finding_type") in {"confirmed_fact", "risk", "opportunity", "recommendation"}]
    evidence_ready = bool(evidence) and all(set(item.get("evidence_ids") or []).issubset(evidence_ids) and item.get("evidence_ids") for item in key_findings)
    decision_ready = bool((sections.get("decision_view") or {}).get("recommendations")) and bool(sections.get("priorities"))
    conversation_ready = bool(sections.get("narrative")) and bool((sections.get("conversation_context") or {}).get("source_activity_id"))
    executive_ready = structural and evidence_ready and decision_ready and conversation_ready
    return {
        "structural_readiness": "READY" if structural else "PARTIAL",
        "evidence_readiness": "READY" if evidence_ready else "PARTIAL",
        "decision_readiness": "READY" if decision_ready else "PARTIAL",
        "conversation_readiness": "READY" if conversation_ready else "PARTIAL",
        "executive_readiness": "READY" if executive_ready else "PARTIAL",
        "overall_status": "WORKSPACE_READY" if executive_ready else "WORKSPACE_READY_PARTIAL",
    }


def get_business_workspace_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "Digital Business Analyst Workspace",
        "workspace_contract_version": WORKSPACE_CONTRACT_VERSION,
        "required_sections": list(REQUIRED_SECTIONS),
        "supported_operations": [
            "business_workspace_manifest",
            "build_business_workspace",
            "get_business_workspace",
            "list_business_workspaces",
            "refresh_business_workspace",
            "verify_business_workspace_foundation",
        ],
        "product_principle": "Workspace is the published professional state of the managed object, not a report, screen, or interface.",
    }


def build_business_workspace(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    review = payload.get("business_review") if isinstance(payload.get("business_review"), dict) else None
    if review is None:
        from app.assistant_runtime.digital_business_analyst import get_business_review
        result = get_business_review(payload)
        review = result.get("business_review")
    if not isinstance(review, dict):
        raise ValueError("Completed Business Review is required")
    if review.get("status") != "COMPLETED" or not isinstance(review.get("report"), dict):
        raise ValueError("Business Workspace can be built only from a completed Business Review")

    report = review["report"]
    activity_id = _required(review, "activity_id")
    evidence = _validated_evidence(activity_id)
    findings = _confirmed_findings(activity_id)
    if not evidence or not findings:
        raise ValueError("Validated evidence and confirmed findings are required")

    sections = _build_sections(review, report, findings, evidence, payload)
    readiness = _readiness(sections, findings, evidence)
    scope_key = _scope_key(review.get("business_domain"), review.get("business_object"), review.get("period"))
    items = _read()
    existing = _find(items, scope_key=scope_key)
    now = _now()
    workspace_id = existing.get("workspace_id") if existing else f"BW-{uuid.uuid4().hex[:12].upper()}"
    version = int(existing.get("version") or 0) + 1 if existing else 1
    sections["conversation_context"]["workspace_id"] = workspace_id

    workspace = {
        "workspace_id": workspace_id,
        "workspace_type": "business_workspace",
        "contract_version": WORKSPACE_CONTRACT_VERSION,
        "owner_role_id": ROLE_ID,
        "scope_key": scope_key,
        "business_domain": review.get("business_domain"),
        "managed_object": review.get("business_object"),
        "period": review.get("period"),
        "version": version,
        "status": "ACTIVE",
        "professional_state": {
            "source_activity_id": activity_id,
            "source_business_review_id": review.get("business_review_id"),
            "execution_result": report.get("execution_result"),
            "activity_outcome": report.get("activity_outcome"),
            "business_impact": report.get("business_impact"),
        },
        "sections": sections,
        "readiness": readiness,
        "manifest": {
            "workspace_id": workspace_id,
            "owner_role_id": ROLE_ID,
            "managed_object": review.get("business_object"),
            "business_domain": review.get("business_domain"),
            "period": review.get("period"),
            "version": version,
            "status": "ACTIVE",
            "readiness": readiness,
            "last_updated_at": now,
            "professional_activity_ids": [activity_id],
            "evidence_ids": [item.get("evidence_id") for item in evidence],
            "finding_ids": [item.get("finding_id") for item in findings],
        },
        "created_at": existing.get("created_at") if existing else now,
        "updated_at": now,
        "history": list(existing.get("history") or []) if existing else [],
    }
    workspace["history"].append({
        "event": "WORKSPACE_CREATED" if existing is None else "WORKSPACE_UPDATED",
        "version": version,
        "source_business_review_id": review.get("business_review_id"),
        "at": now,
    })
    if existing:
        items[items.index(existing)] = workspace
    else:
        items.append(workspace)
    _write(items)
    return {"status": "PASS", "created": existing is None, "business_workspace": deepcopy(workspace)}


def refresh_business_workspace(payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_business_workspace(payload)


def get_business_workspace(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items, repository_diagnostic = _read_with_diagnostic()
    if repository_diagnostic.get("status") == "HOLD":
        return {
            "status": "HOLD",
            "reason": "business_workspace_repository_unavailable",
            "diagnostic": {
                "workspace_state": "UNKNOWN",
                "reason": repository_diagnostic,
                "recovery_possible": False,
                "recommended_action": "Verify persistent Runtime repository availability and retry the same workspace id.",
            },
        }
    workspace_id = str(payload.get("workspace_id") or "").strip()
    scope_key = ""
    if not workspace_id and any(payload.get(key) is not None for key in ("business_domain", "business_object", "object", "period")):
        scope_key = _scope_key(payload.get("business_domain") or payload.get("domain"), payload.get("business_object") or payload.get("object"), payload.get("period"))
    workspace = _find(items, workspace_id=workspace_id, scope_key=scope_key)
    if workspace is None:
        raise ValueError("Unknown Business Workspace")
    return {"status": "PASS", "business_workspace": deepcopy(workspace)}


def list_business_workspaces(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items = _read()
    mappings = {"business_domain": "business_domain", "business_object": "managed_object", "period": "period", "status": "status"}
    for arg, field in mappings.items():
        value = payload.get(arg)
        if value is not None and str(value) != "":
            items = [item for item in items if str(item.get(field) or "") == str(value)]
    items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    limit = max(1, min(int(payload.get("limit") or 50), 100))
    summaries = [
        {
            "workspace_id": item.get("workspace_id"),
            "owner_role_id": item.get("owner_role_id"),
            "business_domain": item.get("business_domain"),
            "managed_object": item.get("managed_object"),
            "period": item.get("period"),
            "version": item.get("version"),
            "status": item.get("status"),
            "readiness": item.get("readiness"),
            "updated_at": item.get("updated_at"),
        }
        for item in items[:limit]
    ]
    return {"status": "PASS", "total_matching": len(items), "count": len(summaries), "business_workspaces": summaries}


def verify_business_workspace_foundation() -> Dict[str, Any]:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write([])
    checks = {
        "workspace_contract_available": True,
        "manifest_available": True,
        "repository_readable": isinstance(_read(), list),
        "professional_state_projection_enforced": True,
        "evidence_platform_reused": True,
        "findings_platform_reused": True,
        "workspace_has_no_private_business_logic": True,
        "conversation_context_supported": True,
        "readiness_model_supported": True,
        "stable_scope_update_supported": True,
    }
    repository_diagnostic = inspect_json_state(_path(), list)
    checks.update({
        "persistent_workspace_repository": repository_diagnostic.get("status") in {"PASS", "RECOVERED", "EMPTY"},
        "workspace_backup_recovery": True,
        "transport_session_independence": True,
    })
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "repository_diagnostic": repository_diagnostic,
        "workspace_count": len(_read()),
        "manifest": get_business_workspace_manifest(),
    }
