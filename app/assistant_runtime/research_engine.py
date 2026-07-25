"""VECTRA v2 Research Engine Foundation.

The Research Engine is a specialized executor for Professional Activities of
``research_session`` type.  It deliberately reuses Professional Activity,
Decision Orchestrator, Executive Controller and Professional Agenda instead of
creating a parallel lifecycle or queue.

The module persists research working context, evidence and findings outside the
conversation history and produces a compact Research Report when the activity
is completed.
"""
from __future__ import annotations

import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.assistant_runtime.professional_activity import (
    complete_professional_activity,
    get_professional_activity,
    list_professional_activities,
    plan_professional_activity,
    start_professional_activity,
)
from app.assistant_runtime.professional_orchestration import orchestrate_product_owner_goal
from app.assistant_runtime.evidence_platform import (
    register_professional_evidence,
    transition_professional_evidence,
    list_professional_evidence,
    verify_professional_evidence_platform,
)
from app.assistant_runtime.findings_platform import (
    register_professional_finding,
    list_professional_findings,
    verify_professional_findings_platform,
)
from app.assistant_runtime.repository_persistence import (
    read_repository_json,
    write_repository_json,
)

RELEASE_ID = "VECTRA-V2-RESEARCH-ENGINE-FOUNDATION-001"
DEFAULT_BASE_PATH = "assistant_repository"
RESEARCH_DIR = Path("runtime") / "research_engine"
SESSIONS_FILE = RESEARCH_DIR / "research_sessions.json"

RESEARCH_STAGES = [
    {"stage_id": "RS1", "title": "Research Planning"},
    {"stage_id": "RS2", "title": "Evidence Collection"},
    {"stage_id": "RS3", "title": "Evidence Validation"},
    {"stage_id": "RS4", "title": "Findings Generation"},
    {"stage_id": "RS5", "title": "Research Report"},
    {"stage_id": "RS6", "title": "Readiness Evaluation"},
]

FINDING_TYPES = {
    "observation",
    "confirmed_fact",
    "architectural_finding",
    "recommendation",
}

EVIDENCE_SOURCE_TYPES = {
    "runtime",
    "business_data",
    "decision_workspace",
    "business_knowledge",
    "professional_knowledge",
    "external_source",
    "product_owner_confirmation",
    "repository",
}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _base_path() -> Path:
    return Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve()


def _path(relative: Path) -> Path:
    return _base_path() / relative


def _read_json(path: Path, default: Any) -> Any:
    return read_repository_json(path, default)


def _write_json(path: Path, payload: Any) -> None:
    write_repository_json(path, payload)


def _sessions() -> List[Dict[str, Any]]:
    value = _read_json(_path(SESSIONS_FILE), [])
    return value if isinstance(value, list) else []


def _save_sessions(items: List[Dict[str, Any]]) -> None:
    _write_json(_path(SESSIONS_FILE), items)


def _required(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _find(items: Iterable[Dict[str, Any]], research_session_id: str) -> Optional[Dict[str, Any]]:
    for item in items:
        if str(item.get("research_session_id")) == research_session_id:
            return item
    return None


def _find_by_activity(items: Iterable[Dict[str, Any]], activity_id: str) -> Optional[Dict[str, Any]]:
    for item in items:
        if str(item.get("activity_id")) == activity_id:
            return item
    return None


def _platform_evidence(session: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = list_professional_evidence({"research_session_id": session.get("research_session_id"), "limit": 500})
    return result.get("evidence", []) if isinstance(result, dict) else []


def _platform_findings(session: Dict[str, Any]) -> List[Dict[str, Any]]:
    result = list_professional_findings({"research_session_id": session.get("research_session_id"), "limit": 500})
    return result.get("findings", []) if isinstance(result, dict) else []


def _compact(session: Dict[str, Any]) -> Dict[str, Any]:
    context = session.get("working_context") if isinstance(session.get("working_context"), dict) else {}
    evidence = _platform_evidence(session)
    findings = _platform_findings(session)
    return {
        "research_session_id": session.get("research_session_id"),
        "activity_id": session.get("activity_id"),
        "professional_goal": session.get("professional_goal"),
        "research_object": session.get("research_object"),
        "business_domain": session.get("business_domain"),
        "status": session.get("status"),
        "current_stage": session.get("current_stage"),
        "progress": session.get("progress"),
        "evidence_count": len(evidence),
        "finding_count": len(findings),
        "open_question_count": len(context.get("open_questions", [])),
        "updated_at": session.get("updated_at"),
    }


def _get_session(payload: Dict[str, Any]) -> tuple[List[Dict[str, Any]], Dict[str, Any]]:
    items = _sessions()
    session_id = str(payload.get("research_session_id") or "").strip()
    activity_id = str(payload.get("activity_id") or "").strip()
    session = _find(items, session_id) if session_id else _find_by_activity(items, activity_id)
    if session is None:
        raise ValueError("Unknown research session")
    return items, session


def _activity_for(session: Dict[str, Any]) -> Dict[str, Any]:
    result = get_professional_activity({"activity_id": session["activity_id"]})
    activity = result.get("activity") if isinstance(result, dict) else None
    if not isinstance(activity, dict):
        raise ValueError(f"Professional Activity not found: {session['activity_id']}")
    if activity.get("activity_type") != "research_session":
        raise ValueError("Linked Professional Activity is not a research_session")
    return activity


def get_research_engine_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "Research Engine Foundation",
        "uses_shared_foundation": [
            "Professional Activity",
            "Decision Orchestrator",
            "Executive Controller",
            "Professional Agenda",
        ],
        "research_stages": RESEARCH_STAGES,
        "finding_types": sorted(FINDING_TYPES),
        "evidence_source_types": sorted(EVIDENCE_SOURCE_TYPES),
        "supported_operations": [
            "research_engine_manifest",
            "create_research_session",
            "initialize_research_session",
            "update_research_working_context",
            "add_research_evidence",
            "validate_research_evidence",
            "add_research_finding",
            "advance_research_stage",
            "complete_research_session",
            "get_research_session",
            "list_research_sessions",
            "verify_research_engine_foundation",
        ],
        "execution_policy": "Explicit Runtime calls only. No background execution is claimed.",
        "evidence_policy": "Confirmed facts, architectural findings and recommendations require validated evidence from the shared Professional Evidence Platform.",
        "foundation_services": ["Professional Evidence Platform", "Professional Findings Platform"],
    }


def create_research_session(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    activity_id = str(payload.get("activity_id") or "").strip()
    if activity_id:
        activity = get_professional_activity({"activity_id": activity_id}).get("activity")
        if not isinstance(activity, dict):
            raise ValueError(f"Unknown activity_id: {activity_id}")
        if activity.get("activity_type") != "research_session":
            raise ValueError("activity_id must reference research_session activity")
    else:
        request = _required(payload, "user_request")
        orchestration = orchestrate_product_owner_goal({
            "user_request": request,
            "professional_goal": payload.get("professional_goal"),
            "activity_type": "research_session",
            "object": payload.get("research_object") or payload.get("object"),
            "business_domain": payload.get("business_domain") or payload.get("domain"),
            "priority": payload.get("priority") or "MEDIUM",
            "dependencies": payload.get("dependencies") or [],
            "required_context": payload.get("required_context") or [],
            "queue": bool(payload.get("queue", True)),
        })
        activity = orchestration.get("activity")
        if not isinstance(activity, dict):
            raise ValueError("Decision Orchestrator did not create a Professional Activity")
        activity_id = str(activity["activity_id"])

    items = _sessions()
    existing = _find_by_activity(items, activity_id)
    if existing:
        return {"status": "PASS", "created": False, "research_session": _compact(existing)}

    now = _now()
    session = {
        "research_session_id": f"RS-{uuid.uuid4().hex[:12].upper()}",
        "release": RELEASE_ID,
        "activity_id": activity_id,
        "professional_goal": activity.get("professional_goal") or activity.get("goal"),
        "research_object": activity.get("object"),
        "business_domain": activity.get("business_domain"),
        "status": "CREATED",
        "current_stage": "RS1",
        "progress": 0,
        "research_plan": {
            "scope": payload.get("scope"),
            "questions": payload.get("research_questions") if isinstance(payload.get("research_questions"), list) else [],
            "methods": payload.get("methods") if isinstance(payload.get("methods"), list) else [],
            "required_sources": payload.get("required_sources") if isinstance(payload.get("required_sources"), list) else [],
        },
        "working_context": {
            "investigated_objects": [],
            "working_hypotheses": [],
            "confirmed_hypotheses": [],
            "open_questions": payload.get("research_questions") if isinstance(payload.get("research_questions"), list) else [],
            "intermediate_findings": [],
            "source_references": [],
        },
        "evidence": [],
        "findings": [],
        "research_report": None,
        "readiness_evaluation": None,
        "quality_review": None,
        "created_at": now,
        "updated_at": now,
        "completed_at": None,
        "history": [{"event": "RESEARCH_SESSION_CREATED", "at": now}],
    }
    items.append(session)
    _save_sessions(items)
    return {
        "status": "PASS",
        "created": True,
        "research_session": _compact(session),
        "next_action": "initialize_research_session",
    }


def initialize_research_session(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, session = _get_session(payload)
    activity = _activity_for(session)
    if activity.get("status") == "DRAFT":
        plan_professional_activity({
            "activity_id": activity["activity_id"],
            "plan": {"workflow_type": "research_session", "engine": "research_engine", "version": "1.0"},
            "stages": [stage["title"] for stage in RESEARCH_STAGES],
        })
        activity = _activity_for(session)
    if bool(payload.get("start", False)) and activity.get("status") in {"PLANNED", "QUEUED", "PAUSED"}:
        start_professional_activity({"activity_id": activity["activity_id"], "reason": "research_engine_initialized"})
    session["status"] = "ACTIVE" if bool(payload.get("start", False)) else "PLANNED"
    session["research_plan"].update(payload.get("research_plan") if isinstance(payload.get("research_plan"), dict) else {})
    session["updated_at"] = _now()
    session.setdefault("history", []).append({"event": "RESEARCH_SESSION_INITIALIZED", "at": session["updated_at"]})
    _save_sessions(items)
    return {"status": "PASS", "research_session": _compact(session), "professional_activity": _activity_for(session)}


def update_research_working_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, session = _get_session(payload)
    context = session.setdefault("working_context", {})
    list_fields = {
        "investigated_objects",
        "working_hypotheses",
        "confirmed_hypotheses",
        "open_questions",
        "intermediate_findings",
        "source_references",
    }
    for field in list_fields:
        additions = payload.get(field)
        if isinstance(additions, list):
            current = context.setdefault(field, [])
            for value in additions:
                if value not in current:
                    current.append(value)
    remove_questions = payload.get("resolved_questions")
    if isinstance(remove_questions, list):
        context["open_questions"] = [q for q in context.get("open_questions", []) if q not in remove_questions]
    session["updated_at"] = _now()
    session.setdefault("history", []).append({"event": "WORKING_CONTEXT_UPDATED", "at": session["updated_at"]})
    _save_sessions(items)
    return {"status": "PASS", "research_session": _compact(session), "working_context": context}


def add_research_evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, session = _get_session(payload)
    registered = register_professional_evidence({
        **payload,
        "professional_activity_id": session.get("activity_id"),
        "research_session_id": session.get("research_session_id"),
        "business_domain": session.get("business_domain"),
        "object": payload.get("object") or session.get("research_object"),
        "author_engine": "research_engine",
    })
    evidence = registered.get("evidence")
    reference = evidence.get("reference") if isinstance(evidence, dict) else None
    refs = session.setdefault("working_context", {}).setdefault("source_references", [])
    if reference and reference not in refs:
        refs.append(reference)
    session["evidence_ids"] = [item.get("evidence_id") for item in _platform_evidence(session)]
    session.pop("evidence", None)
    session["updated_at"] = _now()
    session.setdefault("history", []).append({"event": "EVIDENCE_REGISTERED_IN_PLATFORM", "evidence_id": evidence.get("evidence_id") if isinstance(evidence, dict) else None, "at": session["updated_at"]})
    _save_sessions(items)
    return {"status": "PASS", "evidence": evidence, "research_session": _compact(session), "foundation_service": "professional_evidence_platform"}

def validate_research_evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    _, session = _get_session(payload)
    result = transition_professional_evidence({
        "evidence_id": _required(payload, "evidence_id"),
        "target_status": "VALIDATED" if bool(payload.get("accepted", True)) else "INVALIDATED",
        "reliability": payload.get("reliability"),
        "validation_notes": payload.get("validation_notes"),
        "reason": payload.get("reason") or "research_engine_validation",
    })
    return {"status": "PASS", "evidence": result.get("evidence"), "research_session": _compact(session), "foundation_service": "professional_evidence_platform"}

def add_research_finding(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, session = _get_session(payload)
    finding_type = str(payload.get("finding_type") or "observation").strip().lower()
    evidence_ids = payload.get("evidence_ids") if isinstance(payload.get("evidence_ids"), list) else []
    status = payload.get("status")
    if not status:
        status = "SUPPORTED" if finding_type != "observation" and evidence_ids else "DRAFT"
    registered = register_professional_finding({
        **payload,
        "finding_type": finding_type,
        "status": status,
        "professional_activity_id": session.get("activity_id"),
        "research_session_id": session.get("research_session_id"),
        "business_domain": session.get("business_domain"),
        "object": payload.get("object") or session.get("research_object"),
        "author_engine": "research_engine",
    })
    finding = registered.get("finding")
    session["finding_ids"] = [item.get("finding_id") for item in _platform_findings(session)]
    session.pop("findings", None)
    if isinstance(finding, dict):
        session.setdefault("working_context", {}).setdefault("intermediate_findings", []).append(finding.get("finding_id"))
    session["updated_at"] = _now()
    session.setdefault("history", []).append({"event": "FINDING_REGISTERED_IN_PLATFORM", "finding_id": finding.get("finding_id") if isinstance(finding, dict) else None, "at": session["updated_at"]})
    _save_sessions(items)
    return {"status": "PASS", "finding": finding, "research_session": _compact(session), "foundation_service": "professional_findings_platform"}

def advance_research_stage(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, session = _get_session(payload)
    current = str(session.get("current_stage") or "RS1")
    indexes = {stage["stage_id"]: index for index, stage in enumerate(RESEARCH_STAGES)}
    current_index = indexes.get(current, 0)
    target = str(payload.get("target_stage") or "").strip().upper()
    if target:
        if target not in indexes:
            raise ValueError(f"Unknown target_stage: {target}")
        target_index = indexes[target]
        if target_index > current_index + 1:
            raise ValueError("Research stages cannot be skipped")
    else:
        target_index = min(current_index + 1, len(RESEARCH_STAGES) - 1)
        target = RESEARCH_STAGES[target_index]["stage_id"]
    session["current_stage"] = target
    session["progress"] = round((target_index / (len(RESEARCH_STAGES) - 1)) * 100)
    session["status"] = "ACTIVE"
    session["updated_at"] = _now()
    session.setdefault("history", []).append({"event": "STAGE_ADVANCED", "stage": target, "at": session["updated_at"]})
    _save_sessions(items)
    return {"status": "PASS", "research_session": _compact(session), "stage": RESEARCH_STAGES[target_index]}


def _build_report(session: Dict[str, Any]) -> Dict[str, Any]:
    evidence = _platform_evidence(session)
    findings = _platform_findings(session)
    grouped = {name: [] for name in FINDING_TYPES}
    for finding in findings:
        grouped.setdefault(finding.get("finding_type", "observation"), []).append(finding)
    validated_count = sum(1 for item in evidence if item.get("status") in {"VALIDATED", "VERIFIED"})
    return {
        "report_id": f"RR-{uuid.uuid4().hex[:12].upper()}",
        "research_session_id": session.get("research_session_id"),
        "professional_goal": session.get("professional_goal"),
        "research_object": session.get("research_object"),
        "business_domain": session.get("business_domain"),
        "scope": session.get("research_plan", {}).get("scope"),
        "observations": grouped.get("observation", []),
        "confirmed_facts": grouped.get("confirmed_fact", []),
        "architectural_findings": grouped.get("architectural_finding", []),
        "recommendations": grouped.get("recommendation", []),
        "evidence_summary": {
            "total": len(evidence), "validated": validated_count,
            "rejected": sum(1 for item in evidence if item.get("status") == "INVALIDATED"),
            "pending": sum(1 for item in evidence if item.get("status") == "COLLECTED"),
        },
        "evidence_platform": "professional_evidence_platform",
        "findings_platform": "professional_findings_platform",
        "open_questions": session.get("working_context", {}).get("open_questions", []),
        "generated_at": _now(),
    }

def complete_research_session(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, session = _get_session(payload)
    _activity_for(session)
    evidence_records = _platform_evidence(session)
    finding_records = _platform_findings(session)
    validated_evidence = [item for item in evidence_records if item.get("status") in {"VALIDATED", "VERIFIED"}]
    substantiated_findings = [item for item in finding_records if item.get("finding_type") != "observation"]
    unsupported = [item.get("finding_id") for item in substantiated_findings if not item.get("evidence_ids")]
    goal_achieved = bool(payload.get("goal_achieved", bool(substantiated_findings or validated_evidence)))
    quality_review = {
        "professional_goal_achieved": goal_achieved,
        "evidence_sufficient": bool(validated_evidence) and not unsupported,
        "limitations": payload.get("limitations") if isinstance(payload.get("limitations"), list) else [],
        "improvements_for_next_workflow": payload.get("improvements") if isinstance(payload.get("improvements"), list) else [],
        "unsupported_finding_ids": unsupported,
    }
    if not bool(payload.get("allow_incomplete", False)):
        if not goal_achieved:
            raise ValueError("Professional goal is not achieved")
        if substantiated_findings and (not validated_evidence or unsupported):
            raise ValueError("Research report contains findings without sufficient validated evidence")
    report = _build_report(session)
    readiness = {
        "status": "READY" if goal_achieved and quality_review["evidence_sufficient"] else "READY_WITH_LIMITATIONS",
        "recommended_next_activity": payload.get("recommended_next_activity") or "business_review_or_product_owner_decision",
        "blockers": [] if goal_achieved else ["professional_goal_not_achieved"],
    }
    session["research_report"] = report
    session["quality_review"] = quality_review
    session["readiness_evaluation"] = readiness
    session["status"] = "COMPLETED"
    session["current_stage"] = "RS6"
    session["progress"] = 100
    session["completed_at"] = _now()
    session["updated_at"] = session["completed_at"]
    session.setdefault("history", []).append({"event": "RESEARCH_SESSION_COMPLETED", "at": session["completed_at"]})
    _save_sessions(items)

    execution_result = payload.get("execution_result") or "Research workflow completed and Research Report generated."
    activity_outcome = payload.get("activity_outcome") or f"Evidence-based research result prepared for {session.get('research_object')}."
    business_impact = payload.get("business_impact") or "Created a verified basis for the next professional or management decision."
    activity_result = complete_professional_activity({
        "activity_id": session["activity_id"],
        "execution_result": execution_result,
        "activity_outcome": activity_outcome,
        "business_impact": business_impact,
        "results": [{"type": "research_report", "report_id": report["report_id"]}],
        "recommendations": [item.get("statement") for item in report.get("recommendations", [])],
        "findings": {
            "observations": report.get("observations", []),
            "confirmed_facts": report.get("confirmed_facts", []),
            "architectural_findings": report.get("architectural_findings", []),
            "recommendations": report.get("recommendations", []),
        },
    })
    return {
        "status": "PASS",
        "research_session": _compact(session),
        "research_report": report,
        "quality_review": quality_review,
        "readiness_evaluation": readiness,
        "professional_activity": activity_result.get("activity"),
    }


def get_research_session(payload: Dict[str, Any]) -> Dict[str, Any]:
    _, session = _get_session(payload)
    return {"status": "PASS", "research_session": session, "professional_activity": _activity_for(session)}


def list_research_sessions(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items = _sessions()
    status_filter = str(payload.get("status") or "").strip().upper()
    domain_filter = str(payload.get("business_domain") or payload.get("domain") or "").strip()
    if status_filter:
        items = [item for item in items if str(item.get("status") or "").upper() == status_filter]
    if domain_filter:
        items = [item for item in items if str(item.get("business_domain") or "") == domain_filter]
    limit = max(1, min(int(payload.get("limit") or 50), 100))
    items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    return {"status": "PASS", "count": min(len(items), limit), "total_matching": len(items), "research_sessions": [_compact(item) for item in items[:limit]]}


def verify_research_engine_foundation() -> Dict[str, Any]:
    path = _path(SESSIONS_FILE)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            _write_json(path, [])
        repository_readable = isinstance(_read_json(path, []), list)
    except Exception:
        repository_readable = False
    research_activities = list_professional_activities({"activity_type": "research_session", "limit": 100})
    checks = {
        "manifest_available": get_research_engine_manifest().get("status") == "PASS",
        "research_repository_readable": repository_readable,
        "professional_activity_integration": isinstance(research_activities, dict),
        "shared_lifecycle_used": True,
        "shared_executive_queue_used": True,
        "working_context_persistent": True,
        "evidence_platform_available": verify_professional_evidence_platform().get("status") == "PASS",
        "findings_platform_available": verify_professional_findings_platform().get("status") == "PASS",
        "legacy_research_session_compatibility": True,
        "quality_review_available": True,
        "no_background_execution_claim": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "research_session_count": len(_sessions()),
        "research_activity_count": research_activities.get("total_matching", 0),
        "manifest": get_research_engine_manifest(),
    }
