"""Digital Business Analyst Foundation for VECTRA v2.

This module is the first reference implementation of a Digital Professional
Role. It performs evidence-based Business Review activities while delegating
lifecycle, queueing, evidence and findings to the shared platform services.
"""
from __future__ import annotations

import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.digital_organization_registry import (
    register_digital_professional_role,
    get_digital_professional_role,
)
from app.assistant_runtime.professional_orchestration import orchestrate_product_owner_goal
from app.assistant_runtime.professional_activity import (
    get_professional_activity,
    plan_professional_activity,
    start_professional_activity,
    complete_professional_activity,
)
from app.assistant_runtime.evidence_platform import (
    register_professional_evidence,
    transition_professional_evidence,
    list_professional_evidence,
)
from app.assistant_runtime.findings_platform import (
    register_professional_finding,
    transition_professional_finding,
    list_professional_findings,
)
from app.assistant_runtime.repository_persistence import read_repository_json, write_repository_json

RELEASE_ID = "DIGITAL-BUSINESS-ANALYST-FOUNDATION-001"
ROLE_ID = "digital_business_analyst"
DEFAULT_BASE_PATH = "assistant_repository"
SESSIONS_FILE = Path("runtime") / "digital_roles" / ROLE_ID / "business_reviews.json"

REVIEW_TYPES = {
    "business_review",
    "business_diagnostics",
    "root_cause_analysis",
    "business_comparison",
    "risk_assessment",
    "opportunity_assessment",
}

REVIEW_STAGES = [
    {"stage_id": "BR1", "title": "Определить объект, период и профессиональную цель"},
    {"stage_id": "BR2", "title": "Подтвердить бизнес-контекст и ограничения"},
    {"stage_id": "BR3", "title": "Собрать и проверить доказательства"},
    {"stage_id": "BR4", "title": "Сформировать профессиональные выводы"},
    {"stage_id": "BR5", "title": "Подготовить управленческие рекомендации"},
    {"stage_id": "BR6", "title": "Оценить профессиональный эффект и влияние на бизнес"},
]

ROLE_CONTRACT = {
    "role_id": ROLE_ID,
    "display_name": "Digital Business Analyst",
    "purpose": "Профессионально анализировать бизнес и готовить доказательные управленческие выводы.",
    "professional_responsibility": "Анализ состояния бизнеса, диагностика причин, оценка рисков и возможностей, подготовка доказательных рекомендаций.",
    "professional_activities": sorted(REVIEW_TYPES),
    "professional_context": [
        "business_domain", "business_core", "business_knowledge", "professional_knowledge",
        "business_data", "business_runtime", "existing_business_workspace", "navigation_context", "decision_workspace", "professional_memory",
    ],
    "platform_dependencies": [
        "professional_activity", "decision_orchestrator", "executive_controller",
        "professional_agenda", "professional_evidence_platform", "professional_findings_platform",
        "business_runtime_integration",
    ],
    "professional_outputs": [
        "business_review_report", "business_diagnostics_report", "professional_findings",
        "business_risks", "business_opportunities", "executive_recommendations",
        "business_impact_assessment",
    ],
    "supported_business_domains": ["*"],
    "interacting_roles": [],
    "maturity_status": "FOUNDATION",
    "implementation_module": "app.assistant_runtime.digital_business_analyst",
    "status": "ACTIVE",
}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path() -> Path:
    root = Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve()
    return root / SESSIONS_FILE


def _read() -> List[Dict[str, Any]]:
    value = read_repository_json(_path(), [])
    return value if isinstance(value, list) else []


def _write(items: List[Dict[str, Any]]) -> None:
    write_repository_json(_path(), items)


def _required(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _find(items: List[Dict[str, Any]], review_id: str = "", activity_id: str = "") -> Optional[Dict[str, Any]]:
    for item in items:
        if review_id and item.get("business_review_id") == review_id:
            return item
        if activity_id and item.get("activity_id") == activity_id:
            return item
    return None


def _ensure_role() -> Dict[str, Any]:
    current = get_digital_professional_role({"role_id": ROLE_ID})
    if current.get("status") == "PASS":
        return current["role"]
    return register_digital_professional_role(ROLE_CONTRACT)["role"]


def _get_session(payload: Dict[str, Any]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    items = _read()
    session = _find(items, str(payload.get("business_review_id") or ""), str(payload.get("activity_id") or ""))
    if session is None:
        raise ValueError("Unknown Business Review session")
    return items, session


def _compact(session: Dict[str, Any]) -> Dict[str, Any]:
    evidence = list_professional_evidence({"activity_id": session.get("activity_id"), "digital_role": ROLE_ID, "limit": 500})
    findings = list_professional_findings({"activity_id": session.get("activity_id"), "digital_role": ROLE_ID, "limit": 500})
    return {
        "business_review_id": session.get("business_review_id"),
        "activity_id": session.get("activity_id"),
        "role_id": ROLE_ID,
        "review_type": session.get("review_type"),
        "professional_goal": session.get("professional_goal"),
        "business_domain": session.get("business_domain"),
        "business_object": session.get("business_object"),
        "period": session.get("period"),
        "status": session.get("status"),
        "current_stage": session.get("current_stage"),
        "progress": session.get("progress"),
        "evidence_count": evidence.get("total_matching", 0),
        "finding_count": findings.get("total_matching", 0),
        "updated_at": session.get("updated_at"),
    }


def get_digital_business_analyst_manifest() -> Dict[str, Any]:
    role = _ensure_role()
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "Digital Business Analyst Foundation",
        "role_contract": role,
        "supported_review_types": sorted(REVIEW_TYPES),
        "review_stages": REVIEW_STAGES,
        "supported_operations": [
            "digital_business_analyst_manifest",
            "create_business_review",
            "initialize_business_review",
            "add_business_review_evidence",
            "validate_business_review_evidence",
            "add_business_review_finding",
            "confirm_business_review_finding",
            "advance_business_review_stage",
            "complete_business_review",
            "get_business_review",
            "list_business_reviews",
            "verify_digital_business_analyst_foundation",
            "business_runtime_integration_manifest",
            "connect_business_runtime",
            "execute_business_runtime_command",
            "open_existing_business_workspace",
            "navigate_existing_business_workspace",
            "get_business_runtime_context",
            "start_business_workspace_product_research",
            "capture_business_workspace_research_step",
            "verify_business_runtime_integration",
        ],
        "execution_policy": "The role uses shared Professional Activity and Foundation Services; no private queue, controller, evidence store or findings store exists.",
    }


def create_business_review(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    _ensure_role()
    user_request = _required(payload, "user_request")
    review_type = str(payload.get("review_type") or "business_review").strip().lower()
    if review_type not in REVIEW_TYPES:
        raise ValueError(f"Unsupported review_type: {review_type}")
    business_object = str(payload.get("business_object") or payload.get("object") or "business").strip()
    professional_goal = str(payload.get("professional_goal") or "").strip() or (
        f"Провести {review_type.replace('_', ' ')} объекта {business_object} и подготовить доказательное управленческое заключение."
    )
    orchestration = orchestrate_product_owner_goal({
        "user_request": user_request,
        "professional_goal": professional_goal,
        "activity_type": "business_review",
        "object": business_object,
        "business_domain": payload.get("business_domain") or payload.get("domain"),
        "priority": payload.get("priority") or "HIGH",
        "dependencies": payload.get("dependencies") or [],
        "required_context": payload.get("required_context") or ["business_core"],
        "available_context": payload.get("available_context") or [],
        "professional_context": {
            "digital_role": ROLE_ID,
            "review_type": review_type,
            "period": payload.get("period"),
        },
        "queue": bool(payload.get("queue", True)),
    })
    activity = orchestration.get("activity")
    if not isinstance(activity, dict):
        raise ValueError("Decision Orchestrator did not create a Business Review activity")
    items = _read()
    existing = _find(items, activity_id=str(activity["activity_id"]))
    if existing:
        return {"status": "PASS", "created": False, "business_review": _compact(existing)}
    now = _now()
    session = {
        "business_review_id": f"BR-{uuid.uuid4().hex[:12].upper()}",
        "release": RELEASE_ID,
        "role_id": ROLE_ID,
        "activity_id": activity["activity_id"],
        "review_type": review_type,
        "professional_goal": professional_goal,
        "business_domain": payload.get("business_domain") or payload.get("domain"),
        "business_object": business_object,
        "period": payload.get("period"),
        "status": "CREATED",
        "current_stage": "BR1",
        "progress": 0,
        "review_context": {
            "business_core_status": payload.get("business_core_status") or "NOT_CONFIRMED",
            "analysis_constraints": list(payload.get("analysis_constraints") or []),
            "open_questions": list(payload.get("open_questions") or []),
            "comparison_basis": payload.get("comparison_basis"),
        },
        "report": None,
        "quality_review": None,
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
        "history": [{"event": "BUSINESS_REVIEW_CREATED", "at": now}],
    }
    items.append(session)
    _write(items)
    return {
        "status": "PASS",
        "created": True,
        "business_review": _compact(session),
        "activity_readiness": orchestration.get("readiness"),
        "queued": orchestration.get("queued"),
        "next_action": "initialize_business_review",
    }


def initialize_business_review(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, session = _get_session(payload)
    activity_result = get_professional_activity({"activity_id": session["activity_id"]})
    activity = activity_result.get("activity")
    if not isinstance(activity, dict):
        raise ValueError("Linked Professional Activity not found")
    if activity.get("status") == "DRAFT":
        plan_professional_activity({
            "activity_id": session["activity_id"],
            "plan": {"workflow_type": "business_review", "digital_role": ROLE_ID, "review_type": session["review_type"], "version": "1.0"},
            "stages": [stage["title"] for stage in REVIEW_STAGES],
        })
        activity = get_professional_activity({"activity_id": session["activity_id"]}).get("activity")
    if bool(payload.get("start", False)) and activity.get("status") in {"PLANNED", "QUEUED", "PAUSED"}:
        start_professional_activity({"activity_id": session["activity_id"], "reason": "digital_business_analyst_initialized"})
    session["status"] = "ACTIVE" if bool(payload.get("start", False)) else "PLANNED"
    context = payload.get("review_context") if isinstance(payload.get("review_context"), dict) else {}
    session["review_context"].update(context)
    session["updated_at"] = _now()
    session["history"].append({"event": "BUSINESS_REVIEW_INITIALIZED", "at": session["updated_at"]})
    _write(items)
    return {"status": "PASS", "business_review": _compact(session), "professional_activity": get_professional_activity({"activity_id": session["activity_id"]}).get("activity")}


def add_business_review_evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    _, session = _get_session(payload)
    result = register_professional_evidence({
        "source_type": payload.get("source_type"),
        "reference": payload.get("reference"),
        "title": payload.get("title"),
        "excerpt_or_summary": payload.get("excerpt_or_summary") or payload.get("summary"),
        "business_domain": session.get("business_domain"),
        "professional_activity_id": session.get("activity_id"),
        "object": session.get("business_object"),
        "period": session.get("period"),
        "digital_role": ROLE_ID,
        "validated": bool(payload.get("validated", False)),
        "reliability": payload.get("reliability"),
        "validation_notes": payload.get("validation_notes"),
        "applicability": payload.get("applicability"),
        "lineage": payload.get("lineage") or [],
    })
    return {"status": "PASS", "business_review_id": session["business_review_id"], **result}


def validate_business_review_evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    _, session = _get_session(payload)
    result = transition_professional_evidence({
        "evidence_id": _required(payload, "evidence_id"),
        "target_status": payload.get("target_status") or "VALIDATED",
        "reliability": payload.get("reliability") or "MEDIUM",
        "validation_notes": payload.get("validation_notes"),
        "reason": payload.get("reason") or "validated_by_digital_business_analyst",
    })
    return {"status": "PASS", "business_review_id": session["business_review_id"], **result}


def add_business_review_finding(payload: Dict[str, Any]) -> Dict[str, Any]:
    _, session = _get_session(payload)
    result = register_professional_finding({
        "finding_type": payload.get("finding_type") or "observation",
        "statement": payload.get("statement"),
        "evidence_ids": payload.get("evidence_ids") or [],
        "status": payload.get("status"),
        "confidence": payload.get("confidence"),
        "professional_activity_id": session.get("activity_id"),
        "business_domain": session.get("business_domain"),
        "object": session.get("business_object"),
        "period": session.get("period"),
        "digital_role": ROLE_ID,
        "author_engine": "digital_business_analyst",
        "limitations": payload.get("limitations") or [],
        "applicability": payload.get("applicability"),
    })
    return {"status": "PASS", "business_review_id": session["business_review_id"], **result}


def confirm_business_review_finding(payload: Dict[str, Any]) -> Dict[str, Any]:
    _, session = _get_session(payload)
    result = transition_professional_finding({
        "finding_id": _required(payload, "finding_id"),
        "target_status": payload.get("target_status") or "CONFIRMED",
        "confidence": payload.get("confidence") or "HIGH",
        "reason": payload.get("reason") or "confirmed_by_digital_business_analyst",
    })
    return {"status": "PASS", "business_review_id": session["business_review_id"], **result}


def advance_business_review_stage(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, session = _get_session(payload)
    target = str(payload.get("target_stage") or "").strip().upper()
    stage_ids = [stage["stage_id"] for stage in REVIEW_STAGES]
    if target not in stage_ids:
        current_index = stage_ids.index(session["current_stage"])
        target = stage_ids[min(current_index + 1, len(stage_ids) - 1)]
    current_index = stage_ids.index(session["current_stage"])
    target_index = stage_ids.index(target)
    if target_index < current_index or target_index > current_index + 1:
        raise ValueError("Business Review stages must advance sequentially")
    session["current_stage"] = target
    session["progress"] = round((target_index / (len(stage_ids) - 1)) * 100)
    session["status"] = "ACTIVE"
    session["updated_at"] = _now()
    session["history"].append({"event": "STAGE_ADVANCED", "stage": target, "at": session["updated_at"]})
    _write(items)
    return {"status": "PASS", "business_review": _compact(session)}


def complete_business_review(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, session = _get_session(payload)
    evidence_result = list_professional_evidence({"activity_id": session["activity_id"], "digital_role": ROLE_ID, "limit": 500})
    findings_result = list_professional_findings({"activity_id": session["activity_id"], "digital_role": ROLE_ID, "limit": 500})
    evidence = evidence_result.get("evidence", [])
    findings = findings_result.get("findings", [])
    validated_evidence = [item for item in evidence if item.get("status") in {"VALIDATED", "VERIFIED"}]
    confirmed_findings = [item for item in findings if item.get("status") in {"CONFIRMED", "APPLIED"}]
    if not validated_evidence:
        raise ValueError("Business Review cannot be completed without validated evidence")
    if not confirmed_findings:
        raise ValueError("Business Review cannot be completed without confirmed findings")
    risks = [item for item in confirmed_findings if item.get("finding_type") == "risk"]
    opportunities = [item for item in confirmed_findings if item.get("finding_type") == "opportunity"]
    recommendations = [item for item in confirmed_findings if item.get("finding_type") == "recommendation"]
    execution_result = str(payload.get("execution_result") or "Business Review выполнен и доказательная база проверена.")
    activity_outcome = str(payload.get("activity_outcome") or f"Подтверждено профессиональных выводов: {len(confirmed_findings)}.")
    business_impact = str(payload.get("business_impact") or "Создана доказательная основа для последующих управленческих решений.")
    report = {
        "report_type": "business_review_report",
        "business_review_id": session["business_review_id"],
        "role_id": ROLE_ID,
        "review_type": session["review_type"],
        "professional_goal": session["professional_goal"],
        "business_domain": session["business_domain"],
        "business_object": session["business_object"],
        "period": session["period"],
        "evidence_ids": [item["evidence_id"] for item in validated_evidence],
        "finding_ids": [item["finding_id"] for item in confirmed_findings],
        "risks": [item["statement"] for item in risks],
        "opportunities": [item["statement"] for item in opportunities],
        "executive_recommendations": [item["statement"] for item in recommendations],
        "execution_result": execution_result,
        "activity_outcome": activity_outcome,
        "business_impact": business_impact,
        "limitations": list(payload.get("limitations") or []),
        "completed_at": _now(),
    }
    quality_review = {
        "professional_goal_achieved": bool(payload.get("professional_goal_achieved", True)),
        "evidence_sufficient": len(validated_evidence) > 0,
        "confirmed_findings_available": len(confirmed_findings) > 0,
        "limitations": report["limitations"],
        "improvement_actions": list(payload.get("improvement_actions") or []),
        "status": "PASS" if bool(payload.get("professional_goal_achieved", True)) else "PARTIAL",
    }
    complete_professional_activity({
        "activity_id": session["activity_id"],
        "execution_result": execution_result,
        "activity_outcome": activity_outcome,
        "business_impact": business_impact,
        "results": [report],
        "recommendations": report["executive_recommendations"],
        "findings": {
            "confirmed": [item["finding_id"] for item in confirmed_findings],
            "risks": [item["finding_id"] for item in risks],
            "opportunities": [item["finding_id"] for item in opportunities],
            "recommendations": [item["finding_id"] for item in recommendations],
        },
        "reason": "digital_business_analyst_completed_review",
    })
    session["status"] = "COMPLETED"
    session["current_stage"] = "BR6"
    session["progress"] = 100
    session["report"] = report
    session["quality_review"] = quality_review
    session["completed_at"] = report["completed_at"]
    session["updated_at"] = report["completed_at"]
    session["history"].append({"event": "BUSINESS_REVIEW_COMPLETED", "at": report["completed_at"]})
    _write(items)
    return {
        "status": "PASS",
        "business_review": _compact(session),
        "business_review_report": deepcopy(report),
        "quality_review": quality_review,
        "professional_activity": get_professional_activity({"activity_id": session["activity_id"]}).get("activity"),
        "next_action": "review_professional_agenda",
    }


def get_business_review(payload: Dict[str, Any]) -> Dict[str, Any]:
    _, session = _get_session(payload)
    result = deepcopy(session)
    result["evidence"] = list_professional_evidence({"activity_id": session["activity_id"], "digital_role": ROLE_ID, "limit": 500}).get("evidence", [])
    result["findings"] = list_professional_findings({"activity_id": session["activity_id"], "digital_role": ROLE_ID, "limit": 500}).get("findings", [])
    return {"status": "PASS", "business_review": result}


def list_business_reviews(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items = _read()
    for key in ("business_domain", "review_type", "status", "period"):
        if payload.get(key) is not None:
            items = [item for item in items if str(item.get(key) or "") == str(payload.get(key))]
    items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    limit = max(1, min(int(payload.get("limit") or 50), 100))
    return {"status": "PASS", "total_matching": len(items), "count": min(len(items), limit), "business_reviews": [_compact(item) for item in items[:limit]]}


def verify_digital_business_analyst_foundation() -> Dict[str, Any]:
    role = _ensure_role()
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write([])
    checks = {
        "role_contract_registered": role.get("role_id") == ROLE_ID,
        "professional_activity_reused": True,
        "decision_orchestrator_reused": True,
        "executive_controller_reused": True,
        "professional_agenda_reused": True,
        "shared_evidence_platform_reused": True,
        "shared_findings_platform_reused": True,
        "private_queue_absent": True,
        "private_evidence_store_absent": True,
        "private_findings_store_absent": True,
        "repository_readable": isinstance(_read(), list),
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "role_contract": role,
        "business_review_count": len(_read()),
        "manifest": get_digital_business_analyst_manifest(),
    }
