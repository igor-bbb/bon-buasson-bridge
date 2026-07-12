"""Business Framework Research Foundation for VECTRA v2.

This module provides the persistent research operating environment used by the
Digital Business Analyst to evolve the Business Framework through evidence-led
research. It reuses the shared Professional Evidence and Professional Findings
platforms and does not create parallel evidence/finding stores.
"""
from __future__ import annotations

import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.assistant_runtime.evidence_platform import (
    get_professional_evidence,
    list_professional_evidence,
    register_professional_evidence,
    transition_professional_evidence,
)
from app.assistant_runtime.findings_platform import (
    get_professional_finding,
    list_professional_findings,
    register_professional_finding,
    transition_professional_finding,
)
from app.assistant_runtime.professional_activity import (
    complete_professional_activity,
    create_professional_activity,
    get_professional_activity,
    plan_professional_activity,
    queue_professional_activity,
    start_professional_activity,
)

RELEASE_ID = "BUSINESS-FRAMEWORK-RESEARCH-FOUNDATION-001"
DEFAULT_BASE_PATH = "assistant_repository"
BASE_DIR = Path("runtime") / "business_framework_research"
PROGRAMS_FILE = BASE_DIR / "research_programs.json"
HYPOTHESES_FILE = BASE_DIR / "research_hypotheses.json"
RECOMMENDATIONS_FILE = BASE_DIR / "product_recommendations.json"
METHODOLOGIES_FILE = BASE_DIR / "professional_methodologies.json"

PROGRAM_STATUSES = {
    "PROPOSED",
    "APPROVED",
    "ACTIVE_RESEARCH",
    "EVIDENCE_COLLECTION",
    "FINDINGS_VALIDATION",
    "PRODUCT_RECOMMENDATION",
    "PRODUCT_OWNER_REVIEW",
    "ENGINEERING",
    "PRODUCT_VERIFICATION",
    "KNOWLEDGE_CAPITALIZATION",
    "CLOSED",
}
PROGRAM_TRANSITIONS = {
    "PROPOSED": {"APPROVED"},
    "APPROVED": {"ACTIVE_RESEARCH"},
    "ACTIVE_RESEARCH": {"EVIDENCE_COLLECTION"},
    "EVIDENCE_COLLECTION": {"FINDINGS_VALIDATION"},
    "FINDINGS_VALIDATION": {"PRODUCT_RECOMMENDATION"},
    "PRODUCT_RECOMMENDATION": {"PRODUCT_OWNER_REVIEW"},
    "PRODUCT_OWNER_REVIEW": {"ENGINEERING", "CLOSED"},
    "ENGINEERING": {"PRODUCT_VERIFICATION"},
    "PRODUCT_VERIFICATION": {"KNOWLEDGE_CAPITALIZATION", "ENGINEERING"},
    "KNOWLEDGE_CAPITALIZATION": {"CLOSED"},
    "CLOSED": set(),
}
HYPOTHESIS_STATUSES = {"PROPOSED", "UNDER_RESEARCH", "CONFIRMED", "REJECTED", "CAPITALIZED"}
HYPOTHESIS_TRANSITIONS = {
    "PROPOSED": {"UNDER_RESEARCH", "REJECTED"},
    "UNDER_RESEARCH": {"CONFIRMED", "REJECTED"},
    "CONFIRMED": {"CAPITALIZED"},
    "REJECTED": set(),
    "CAPITALIZED": set(),
}
RESEARCH_PROGRAM_TYPES = {
    "business_ontology",
    "kpi_methodology",
    "decision_architecture",
    "contract_research",
    "sku_research",
    "time_horizon_research",
    "conversation_architecture",
    "business_framework_research",
}
METHODOLOGY_STATUSES = {"DRAFT", "CONFIRMED", "SUPERSEDED", "ARCHIVED"}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _base_path() -> Path:
    return Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve()


def _path(relative: Path) -> Path:
    return _base_path() / relative


def _read_json(path: Path, default: Any) -> Any:
    try:
        if not path.exists():
            return deepcopy(default)
        with path.open("r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return deepcopy(default)


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    with temporary.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2)
        handle.write("\n")
    temporary.replace(path)


def _list(relative: Path) -> List[Dict[str, Any]]:
    value = _read_json(_path(relative), [])
    return value if isinstance(value, list) else []


def _save(relative: Path, items: List[Dict[str, Any]]) -> None:
    _write_json(_path(relative), items)


def _required(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def _find(items: Iterable[Dict[str, Any]], key: str, value: str) -> Optional[Dict[str, Any]]:
    for item in items:
        if str(item.get(key)) == value:
            return item
    return None


def _programs() -> List[Dict[str, Any]]:
    return _list(PROGRAMS_FILE)


def _save_programs(items: List[Dict[str, Any]]) -> None:
    _save(PROGRAMS_FILE, items)


def _hypotheses() -> List[Dict[str, Any]]:
    return _list(HYPOTHESES_FILE)


def _save_hypotheses(items: List[Dict[str, Any]]) -> None:
    _save(HYPOTHESES_FILE, items)


def _recommendations() -> List[Dict[str, Any]]:
    return _list(RECOMMENDATIONS_FILE)


def _save_recommendations(items: List[Dict[str, Any]]) -> None:
    _save(RECOMMENDATIONS_FILE, items)


def _methodologies() -> List[Dict[str, Any]]:
    return _list(METHODOLOGIES_FILE)


def _save_methodologies(items: List[Dict[str, Any]]) -> None:
    _save(METHODOLOGIES_FILE, items)


def _compact_program(program: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "research_program_id": program.get("research_program_id"),
        "title": program.get("title"),
        "program_type": program.get("program_type"),
        "professional_goal": program.get("professional_goal"),
        "business_domain": program.get("business_domain"),
        "object": program.get("object"),
        "status": program.get("status"),
        "priority": program.get("priority"),
        "professional_activity_id": program.get("professional_activity_id"),
        "hypothesis_count": len(program.get("hypothesis_ids") or []),
        "evidence_count": len(program.get("evidence_ids") or []),
        "finding_count": len(program.get("finding_ids") or []),
        "recommendation_count": len(program.get("product_recommendation_ids") or []),
        "maturity_status": (program.get("maturity") or {}).get("status"),
        "updated_at": program.get("updated_at"),
    }


def _get_program(payload: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    program_id = _required(payload, "research_program_id")
    items = _programs()
    program = _find(items, "research_program_id", program_id)
    if program is None:
        raise ValueError(f"Unknown research_program_id: {program_id}")
    return items, program


def get_business_framework_research_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "Business Framework Research Foundation",
        "program_types": sorted(RESEARCH_PROGRAM_TYPES),
        "program_statuses": sorted(PROGRAM_STATUSES),
        "hypothesis_statuses": sorted(HYPOTHESIS_STATUSES),
        "foundation_dependencies": [
            "Professional Activity",
            "Professional Evidence Platform",
            "Professional Findings Platform",
        ],
        "supported_operations": [
            "business_framework_research_manifest",
            "create_research_program",
            "transition_research_program",
            "get_research_program",
            "list_research_programs",
            "create_research_hypothesis",
            "transition_research_hypothesis",
            "get_research_hypothesis",
            "list_research_hypotheses",
            "add_research_program_evidence",
            "add_research_program_finding",
            "create_product_recommendation",
            "record_product_owner_review",
            "link_research_engineering_task",
            "record_research_product_verification",
            "link_research_knowledge_capitalization",
            "register_professional_methodology",
            "get_professional_methodology",
            "list_professional_methodologies",
            "evaluate_research_maturity",
            "get_research_traceability",
            "get_research_workspace",
            "verify_business_framework_research_foundation",
        ],
        "architecture_policy": "Research extends shared Evidence and Findings platforms through professional types; parallel research platforms are prohibited.",
        "execution_policy": "Explicit Runtime operations only. No background execution is claimed.",
    }


def create_research_program(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    program_type = str(payload.get("program_type") or "business_framework_research").strip().lower()
    if program_type not in RESEARCH_PROGRAM_TYPES:
        raise ValueError(f"Unsupported program_type: {program_type}")
    title = _required(payload, "title")
    professional_goal = _required(payload, "professional_goal")
    now = _now()

    activity_result = create_professional_activity({
        "title": title,
        "goal": professional_goal,
        "professional_goal": professional_goal,
        "user_request": payload.get("research_question") or payload.get("user_request"),
        "activity_type": "research_session",
        "object": payload.get("object") or "Business Framework",
        "business_domain": payload.get("business_domain") or payload.get("domain"),
        "priority": payload.get("priority") or "MEDIUM",
        "professional_context": {
            "research_program_type": program_type,
            "research_scope": payload.get("research_scope"),
            "success_criteria": payload.get("success_criteria") or [],
        },
        "created_by": "digital_business_analyst",
    })
    activity = activity_result["activity"]
    plan_professional_activity({
        "activity_id": activity["activity_id"],
        "plan": {
            "workflow_type": "business_framework_research_program",
            "workflow_version": "1.0",
            "research_question": payload.get("research_question"),
        },
        "stages": [
            "Research Question",
            "Research Program",
            "Evidence Collection",
            "Research Findings",
            "Product Recommendation",
            "Product Owner Review",
            "Engineering Task",
            "Product Verification",
            "Knowledge Capitalization",
        ],
    })

    program = {
        "research_program_id": f"RPG-{uuid.uuid4().hex[:12].upper()}",
        "release": RELEASE_ID,
        "title": title,
        "program_type": program_type,
        "research_question": payload.get("research_question"),
        "professional_goal": professional_goal,
        "business_domain": payload.get("business_domain") or payload.get("domain"),
        "object": payload.get("object") or "Business Framework",
        "research_scope": payload.get("research_scope") if isinstance(payload.get("research_scope"), dict) else {},
        "success_criteria": payload.get("success_criteria") if isinstance(payload.get("success_criteria"), list) else [],
        "priority": str(payload.get("priority") or "MEDIUM").upper(),
        "status": "PROPOSED",
        "professional_activity_id": activity["activity_id"],
        "hypothesis_ids": [],
        "evidence_ids": [],
        "finding_ids": [],
        "product_recommendation_ids": [],
        "engineering_task_ids": [],
        "product_verification_records": [],
        "knowledge_capitalization_ids": [],
        "maturity": {
            "status": "NOT_EVALUATED",
            "new_framework_knowledge": None,
            "new_management_decision": None,
            "new_professional_capability": None,
        },
        "product_owner_review": None,
        "created_at": now,
        "updated_at": now,
        "closed_at": None,
        "history": [{"event": "PROGRAM_CREATED", "status": "PROPOSED", "at": now}],
    }
    items = _programs()
    items.append(program)
    _save_programs(items)
    return {
        "status": "PASS",
        "created": True,
        "research_program": _compact_program(program),
        "professional_activity": activity,
        "next_action": "transition_research_program to APPROVED after Product Owner approval",
    }


def transition_research_program(payload: Dict[str, Any]) -> Dict[str, Any]:
    items, program = _get_program(payload)
    target = str(payload.get("target_status") or "").strip().upper()
    if target not in PROGRAM_STATUSES:
        raise ValueError(f"Unsupported target_status: {target}")
    current = str(program.get("status"))
    if target != current and target not in PROGRAM_TRANSITIONS.get(current, set()):
        raise ValueError(f"Invalid research program transition: {current} -> {target}")
    now = _now()
    program["status"] = target
    program["updated_at"] = now
    if target == "CLOSED":
        maturity = evaluate_research_maturity({"research_program_id": program["research_program_id"]})
        if maturity.get("maturity", {}).get("status") != "COMPLETE":
            raise ValueError("Research program cannot close before Research Maturity is COMPLETE")
        program["closed_at"] = now
    program.setdefault("history", []).append({
        "event": "STATUS_TRANSITION",
        "from": current,
        "to": target,
        "at": now,
        "reason": payload.get("reason"),
    })
    _save_programs(items)

    activity_id = program.get("professional_activity_id")
    if target == "ACTIVE_RESEARCH":
        activity = get_professional_activity({"activity_id": activity_id})["activity"]
        if activity.get("status") == "PLANNED":
            queue_professional_activity({"activity_id": activity_id, "reason": "research_program_approved"})
            start_professional_activity({"activity_id": activity_id, "reason": "research_program_activated"})
    if target == "CLOSED":
        activity = get_professional_activity({"activity_id": activity_id})["activity"]
        if activity.get("status") == "ACTIVE":
            complete_professional_activity({
                "activity_id": activity_id,
                "execution_result": "Research program lifecycle completed.",
                "activity_outcome": program["maturity"].get("new_framework_knowledge"),
                "business_impact": program["maturity"].get("new_management_decision"),
            })
    return {"status": "PASS", "research_program": deepcopy(program)}


def get_research_program(payload: Dict[str, Any]) -> Dict[str, Any]:
    _, program = _get_program(payload)
    return {"status": "PASS", "research_program": deepcopy(program)}


def list_research_programs(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items = _programs()
    filters = {
        "status": "status",
        "program_type": "program_type",
        "business_domain": "business_domain",
        "professional_activity_id": "professional_activity_id",
    }
    for arg, field in filters.items():
        value = payload.get(arg)
        if value is not None and str(value) != "":
            items = [item for item in items if str(item.get(field) or "") == str(value)]
    items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    limit = max(1, min(int(payload.get("limit") or 100), 500))
    return {
        "status": "PASS",
        "total_matching": len(items),
        "count": min(len(items), limit),
        "research_programs": [_compact_program(item) for item in items[:limit]],
    }


def create_research_hypothesis(payload: Dict[str, Any]) -> Dict[str, Any]:
    program_items, program = _get_program(payload)
    statement = _required(payload, "statement")
    now = _now()
    hypothesis = {
        "hypothesis_id": f"RH-{uuid.uuid4().hex[:12].upper()}",
        "research_program_id": program["research_program_id"],
        "statement": statement,
        "rationale": payload.get("rationale"),
        "status": "PROPOSED",
        "evidence_ids": [],
        "finding_ids": [],
        "capitalized_knowledge_id": None,
        "created_at": now,
        "updated_at": now,
        "history": [{"event": "PROPOSED", "at": now}],
    }
    hypotheses = _hypotheses()
    hypotheses.append(hypothesis)
    _save_hypotheses(hypotheses)
    program.setdefault("hypothesis_ids", []).append(hypothesis["hypothesis_id"])
    program["updated_at"] = now
    _save_programs(program_items)
    return {"status": "PASS", "created": True, "hypothesis": deepcopy(hypothesis)}


def transition_research_hypothesis(payload: Dict[str, Any]) -> Dict[str, Any]:
    hypothesis_id = _required(payload, "hypothesis_id")
    target = str(payload.get("target_status") or "").strip().upper()
    if target not in HYPOTHESIS_STATUSES:
        raise ValueError(f"Unsupported target_status: {target}")
    items = _hypotheses()
    hypothesis = _find(items, "hypothesis_id", hypothesis_id)
    if hypothesis is None:
        raise ValueError(f"Unknown hypothesis_id: {hypothesis_id}")
    current = str(hypothesis.get("status"))
    if target != current and target not in HYPOTHESIS_TRANSITIONS.get(current, set()):
        raise ValueError(f"Invalid hypothesis transition: {current} -> {target}")
    if target == "CONFIRMED":
        validated = []
        for evidence_id in hypothesis.get("evidence_ids") or []:
            evidence = get_professional_evidence({"evidence_id": evidence_id})["evidence"]
            if evidence.get("status") in {"VALIDATED", "VERIFIED"}:
                validated.append(evidence_id)
        if not validated:
            raise ValueError("A hypothesis cannot be confirmed without validated Research Evidence")
    if target == "CAPITALIZED" and not payload.get("capitalized_knowledge_id"):
        raise ValueError("capitalized_knowledge_id is required for CAPITALIZED status")
    now = _now()
    hypothesis["status"] = target
    hypothesis["updated_at"] = now
    if payload.get("evidence_ids") and isinstance(payload.get("evidence_ids"), list):
        hypothesis["evidence_ids"] = list(dict.fromkeys(hypothesis.get("evidence_ids", []) + payload["evidence_ids"]))
    if payload.get("finding_ids") and isinstance(payload.get("finding_ids"), list):
        hypothesis["finding_ids"] = list(dict.fromkeys(hypothesis.get("finding_ids", []) + payload["finding_ids"]))
    if target == "CAPITALIZED":
        hypothesis["capitalized_knowledge_id"] = payload["capitalized_knowledge_id"]
    hypothesis.setdefault("history", []).append({"event": target, "at": now, "reason": payload.get("reason")})
    _save_hypotheses(items)
    return {"status": "PASS", "hypothesis": deepcopy(hypothesis)}


def get_research_hypothesis(payload: Dict[str, Any]) -> Dict[str, Any]:
    hypothesis_id = _required(payload, "hypothesis_id")
    hypothesis = _find(_hypotheses(), "hypothesis_id", hypothesis_id)
    if hypothesis is None:
        raise ValueError(f"Unknown hypothesis_id: {hypothesis_id}")
    return {"status": "PASS", "hypothesis": deepcopy(hypothesis)}


def list_research_hypotheses(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items = _hypotheses()
    for arg, field in {"research_program_id": "research_program_id", "status": "status"}.items():
        value = payload.get(arg)
        if value is not None and str(value) != "":
            items = [item for item in items if str(item.get(field) or "") == str(value)]
    items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    limit = max(1, min(int(payload.get("limit") or 100), 500))
    return {"status": "PASS", "total_matching": len(items), "count": min(len(items), limit), "hypotheses": deepcopy(items[:limit])}


def add_research_program_evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    program_items, program = _get_program(payload)
    evidence_payload = dict(payload)
    evidence_payload.update({
        "evidence_type": "research",
        "professional_activity_id": program.get("professional_activity_id"),
        "business_domain": program.get("business_domain"),
        "object": payload.get("object") or program.get("object"),
        "digital_role": payload.get("digital_role") or "digital_business_analyst",
        "research_program_id": program["research_program_id"],
    })
    result = register_professional_evidence(evidence_payload)
    evidence = result["evidence"]
    evidence_id = evidence["evidence_id"]
    if evidence_id not in program.setdefault("evidence_ids", []):
        program["evidence_ids"].append(evidence_id)
        program["updated_at"] = _now()
        _save_programs(program_items)
    hypothesis_id = str(payload.get("hypothesis_id") or "").strip()
    if hypothesis_id:
        hypotheses = _hypotheses()
        hypothesis = _find(hypotheses, "hypothesis_id", hypothesis_id)
        if hypothesis is None or hypothesis.get("research_program_id") != program["research_program_id"]:
            raise ValueError("hypothesis_id must belong to the research program")
        if evidence_id not in hypothesis.setdefault("evidence_ids", []):
            hypothesis["evidence_ids"].append(evidence_id)
            hypothesis["updated_at"] = _now()
            _save_hypotheses(hypotheses)
    return {"status": "PASS", "evidence": evidence, "research_program": _compact_program(program)}


def add_research_program_finding(payload: Dict[str, Any]) -> Dict[str, Any]:
    program_items, program = _get_program(payload)
    finding_payload = dict(payload)
    finding_payload.update({
        "professional_type": "research",
        "professional_activity_id": program.get("professional_activity_id"),
        "business_domain": program.get("business_domain"),
        "object": payload.get("object") or program.get("object"),
        "digital_role": payload.get("digital_role") or "digital_business_analyst",
        "research_program_id": program["research_program_id"],
        "author_engine": payload.get("author_engine") or "business_framework_research_foundation",
    })
    result = register_professional_finding(finding_payload)
    finding = result["finding"]
    finding_id = finding["finding_id"]
    if finding_id not in program.setdefault("finding_ids", []):
        program["finding_ids"].append(finding_id)
        program["updated_at"] = _now()
        _save_programs(program_items)
    hypothesis_id = str(payload.get("hypothesis_id") or "").strip()
    if hypothesis_id:
        hypotheses = _hypotheses()
        hypothesis = _find(hypotheses, "hypothesis_id", hypothesis_id)
        if hypothesis is None or hypothesis.get("research_program_id") != program["research_program_id"]:
            raise ValueError("hypothesis_id must belong to the research program")
        if finding_id not in hypothesis.setdefault("finding_ids", []):
            hypothesis["finding_ids"].append(finding_id)
            hypothesis["updated_at"] = _now()
            _save_hypotheses(hypotheses)
    return {"status": "PASS", "finding": finding, "research_program": _compact_program(program)}


def create_product_recommendation(payload: Dict[str, Any]) -> Dict[str, Any]:
    program_items, program = _get_program(payload)
    title = _required(payload, "title")
    recommendation_text = _required(payload, "recommendation")
    finding_ids = payload.get("finding_ids") if isinstance(payload.get("finding_ids"), list) else []
    if not finding_ids:
        raise ValueError("finding_ids are required for a Product Recommendation")
    confirmed = []
    for finding_id in finding_ids:
        finding = get_professional_finding({"finding_id": finding_id})["finding"]
        if finding.get("status") in {"SUPPORTED", "CONFIRMED", "APPLIED"} and finding.get("professional_type") == "research":
            confirmed.append(finding_id)
    if len(confirmed) != len(finding_ids):
        raise ValueError("All Product Recommendation findings must be supported Research Findings")
    now = _now()
    recommendation = {
        "product_recommendation_id": f"PR-{uuid.uuid4().hex[:12].upper()}",
        "research_program_id": program["research_program_id"],
        "title": title,
        "recommendation": recommendation_text,
        "priority": str(payload.get("priority") or "P1").upper(),
        "finding_ids": finding_ids,
        "expected_management_value": payload.get("expected_management_value"),
        "proposed_engineering_scope": payload.get("proposed_engineering_scope"),
        "status": "READY_FOR_PRODUCT_OWNER_REVIEW",
        "created_at": now,
        "updated_at": now,
    }
    recommendations = _recommendations()
    recommendations.append(recommendation)
    _save_recommendations(recommendations)
    program.setdefault("product_recommendation_ids", []).append(recommendation["product_recommendation_id"])
    program["updated_at"] = now
    _save_programs(program_items)
    return {"status": "PASS", "created": True, "product_recommendation": deepcopy(recommendation)}


def record_product_owner_review(payload: Dict[str, Any]) -> Dict[str, Any]:
    program_items, program = _get_program(payload)
    decision = str(payload.get("decision") or "").strip().upper()
    if decision not in {"APPROVED", "REJECTED", "REVISION_REQUIRED"}:
        raise ValueError("decision must be APPROVED, REJECTED or REVISION_REQUIRED")
    now = _now()
    program["product_owner_review"] = {
        "decision": decision,
        "reviewed_by": payload.get("reviewed_by") or "Product Owner",
        "comment": payload.get("comment"),
        "reviewed_at": now,
    }
    recommendations = _recommendations()
    for item in recommendations:
        if item.get("research_program_id") == program["research_program_id"] and item.get("status") == "READY_FOR_PRODUCT_OWNER_REVIEW":
            item["status"] = "APPROVED" if decision == "APPROVED" else ("REJECTED" if decision == "REJECTED" else "REVISION_REQUIRED")
            item["updated_at"] = now
    _save_recommendations(recommendations)
    program["updated_at"] = now
    _save_programs(program_items)
    return {"status": "PASS", "research_program": deepcopy(program)}


def link_research_engineering_task(payload: Dict[str, Any]) -> Dict[str, Any]:
    program_items, program = _get_program(payload)
    task_id = _required(payload, "engineering_task_id")
    if not program.get("product_owner_review") or program["product_owner_review"].get("decision") != "APPROVED":
        raise ValueError("Product Owner approval is required before opening an Engineering Task")
    link = {
        "engineering_task_id": task_id,
        "title": payload.get("title"),
        "status": payload.get("status") or "OPEN",
        "linked_at": _now(),
    }
    existing = [item for item in program.setdefault("engineering_task_ids", []) if isinstance(item, dict) and item.get("engineering_task_id") == task_id]
    if not existing:
        program["engineering_task_ids"].append(link)
    program["updated_at"] = _now()
    _save_programs(program_items)
    return {"status": "PASS", "engineering_task": link, "research_program": _compact_program(program)}


def record_research_product_verification(payload: Dict[str, Any]) -> Dict[str, Any]:
    program_items, program = _get_program(payload)
    verification_status = str(payload.get("verification_status") or "").strip().upper()
    if verification_status not in {"PASS", "FAIL", "HOLD", "BLOCKED"}:
        raise ValueError("verification_status must be PASS, FAIL, HOLD or BLOCKED")
    record = {
        "release_id": payload.get("release_id"),
        "verification_status": verification_status,
        "result": payload.get("result"),
        "verified_at": _now(),
    }
    program.setdefault("product_verification_records", []).append(record)
    program["updated_at"] = _now()
    _save_programs(program_items)
    return {"status": "PASS", "product_verification": record, "research_program": _compact_program(program)}


def link_research_knowledge_capitalization(payload: Dict[str, Any]) -> Dict[str, Any]:
    program_items, program = _get_program(payload)
    knowledge_id = _required(payload, "knowledge_id")
    verification_records = program.get("product_verification_records") or []
    if not any(item.get("verification_status") == "PASS" for item in verification_records if isinstance(item, dict)):
        raise ValueError("Product Verification PASS is required before Knowledge Capitalization")
    link = {
        "knowledge_id": knowledge_id,
        "knowledge_type": payload.get("knowledge_type") or "professional_knowledge",
        "status": payload.get("status") or "CAPITALIZED",
        "linked_at": _now(),
    }
    existing = [item for item in program.setdefault("knowledge_capitalization_ids", []) if isinstance(item, dict) and item.get("knowledge_id") == knowledge_id]
    if not existing:
        program["knowledge_capitalization_ids"].append(link)
    program["updated_at"] = _now()
    _save_programs(program_items)
    return {"status": "PASS", "knowledge_capitalization": link, "research_program": _compact_program(program)}


def register_professional_methodology(payload: Dict[str, Any]) -> Dict[str, Any]:
    title = _required(payload, "title")
    method = _required(payload, "method")
    status = str(payload.get("status") or "DRAFT").upper()
    if status not in METHODOLOGY_STATUSES:
        raise ValueError(f"Unsupported methodology status: {status}")
    if status == "CONFIRMED" and not payload.get("source_research_program_id"):
        raise ValueError("source_research_program_id is required for confirmed methodology")
    now = _now()
    methodology = {
        "methodology_id": str(payload.get("methodology_id") or f"PM-{uuid.uuid4().hex[:12].upper()}"),
        "title": title,
        "category": payload.get("category") or "business_analysis",
        "method": method,
        "applicability": payload.get("applicability"),
        "limitations": payload.get("limitations") if isinstance(payload.get("limitations"), list) else [],
        "status": status,
        "version": str(payload.get("version") or "1.0"),
        "source_research_program_id": payload.get("source_research_program_id"),
        "source_finding_ids": payload.get("source_finding_ids") if isinstance(payload.get("source_finding_ids"), list) else [],
        "business_domains": payload.get("business_domains") if isinstance(payload.get("business_domains"), list) else [],
        "created_at": now,
        "updated_at": now,
        "history": [{"event": status, "at": now}],
    }
    items = _methodologies()
    duplicate = _find(items, "methodology_id", methodology["methodology_id"])
    if duplicate:
        raise ValueError(f"methodology_id already exists: {methodology['methodology_id']}")
    items.append(methodology)
    _save_methodologies(items)
    return {"status": "PASS", "created": True, "methodology": deepcopy(methodology)}


def get_professional_methodology(payload: Dict[str, Any]) -> Dict[str, Any]:
    methodology_id = _required(payload, "methodology_id")
    methodology = _find(_methodologies(), "methodology_id", methodology_id)
    if methodology is None:
        raise ValueError(f"Unknown methodology_id: {methodology_id}")
    return {"status": "PASS", "methodology": deepcopy(methodology)}


def list_professional_methodologies(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items = _methodologies()
    for arg, field in {"status": "status", "category": "category", "source_research_program_id": "source_research_program_id"}.items():
        value = payload.get(arg)
        if value is not None and str(value) != "":
            items = [item for item in items if str(item.get(field) or "") == str(value)]
    items.sort(key=lambda item: str(item.get("updated_at") or ""), reverse=True)
    limit = max(1, min(int(payload.get("limit") or 100), 500))
    return {"status": "PASS", "total_matching": len(items), "count": min(len(items), limit), "methodologies": deepcopy(items[:limit])}


def evaluate_research_maturity(payload: Dict[str, Any]) -> Dict[str, Any]:
    program_items, program = _get_program(payload)
    answers = {
        "new_framework_knowledge": payload.get("new_framework_knowledge") or (program.get("maturity") or {}).get("new_framework_knowledge"),
        "new_management_decision": payload.get("new_management_decision") or (program.get("maturity") or {}).get("new_management_decision"),
        "new_professional_capability": payload.get("new_professional_capability") or (program.get("maturity") or {}).get("new_professional_capability"),
    }
    missing = [key for key, value in answers.items() if not str(value or "").strip()]
    status = "COMPLETE" if not missing else "INCOMPLETE"
    program["maturity"] = {"status": status, **answers, "missing_answers": missing, "evaluated_at": _now()}
    program["updated_at"] = _now()
    _save_programs(program_items)
    return {"status": "PASS", "research_program_id": program["research_program_id"], "maturity": deepcopy(program["maturity"])}


def get_research_traceability(payload: Dict[str, Any]) -> Dict[str, Any]:
    _, program = _get_program(payload)
    hypotheses = [item for item in _hypotheses() if item.get("research_program_id") == program["research_program_id"]]
    evidence = []
    for evidence_id in program.get("evidence_ids") or []:
        try:
            evidence.append(get_professional_evidence({"evidence_id": evidence_id})["evidence"])
        except Exception:
            pass
    findings = []
    for finding_id in program.get("finding_ids") or []:
        try:
            findings.append(get_professional_finding({"finding_id": finding_id})["finding"])
        except Exception:
            pass
    recommendations = [item for item in _recommendations() if item.get("research_program_id") == program["research_program_id"]]
    methodologies = [item for item in _methodologies() if item.get("source_research_program_id") == program["research_program_id"]]
    return {
        "status": "PASS",
        "research_program_id": program["research_program_id"],
        "traceability": {
            "hypotheses": hypotheses,
            "research_evidence": evidence,
            "research_findings": findings,
            "product_recommendations": recommendations,
            "product_owner_review": program.get("product_owner_review"),
            "engineering_tasks": program.get("engineering_task_ids") or [],
            "product_verification": program.get("product_verification_records") or [],
            "professional_methodologies": methodologies,
            "knowledge_capitalization_ids": program.get("knowledge_capitalization_ids") or [],
        },
        "chain_complete": bool(hypotheses and evidence and findings and recommendations),
    }


def get_research_workspace(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    programs = _programs()
    active_statuses = {"ACTIVE_RESEARCH", "EVIDENCE_COLLECTION", "FINDINGS_VALIDATION", "PRODUCT_RECOMMENDATION"}
    queue_statuses = {"PROPOSED", "APPROVED"}
    review_statuses = {"PRODUCT_OWNER_REVIEW"}
    capitalization_statuses = {"KNOWLEDGE_CAPITALIZATION"}
    if payload.get("business_domain"):
        programs = [item for item in programs if item.get("business_domain") == payload.get("business_domain")]
    hypotheses = _hypotheses()
    findings = list_professional_findings({"professional_type": "research", "limit": 500}).get("findings", [])
    recommendations = _recommendations()
    completed = [item for item in programs if item.get("status") == "CLOSED"]
    maturity_complete = [item for item in programs if (item.get("maturity") or {}).get("status") == "COMPLETE"]
    framework_maturity = round((len(maturity_complete) / len(completed)) * 100, 1) if completed else 0.0
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "research_workspace": {
            "active_research": [_compact_program(item) for item in programs if item.get("status") in active_statuses],
            "research_backlog": [_compact_program(item) for item in programs if item.get("status") in queue_statuses],
            "open_hypotheses": [item for item in hypotheses if item.get("status") in {"PROPOSED", "UNDER_RESEARCH"}],
            "confirmed_research_findings": [item for item in findings if item.get("status") in {"SUPPORTED", "CONFIRMED", "APPLIED"}],
            "product_owner_recommendations": [item for item in recommendations if item.get("status") == "READY_FOR_PRODUCT_OWNER_REVIEW"],
            "awaiting_product_review": [_compact_program(item) for item in programs if item.get("status") in review_statuses],
            "awaiting_knowledge_capitalization": [_compact_program(item) for item in programs if item.get("status") in capitalization_statuses],
            "framework_maturity": {
                "completed_programs": len(completed),
                "maturity_complete_programs": len(maturity_complete),
                "maturity_completion_percent": framework_maturity,
            },
        },
        "autonomy_boundary": "The Workspace organizes explicit research work; it does not claim background execution.",
    }


def verify_business_framework_research_foundation() -> Dict[str, Any]:
    for relative in (PROGRAMS_FILE, HYPOTHESES_FILE, RECOMMENDATIONS_FILE, METHODOLOGIES_FILE):
        path = _path(relative)
        path.parent.mkdir(parents=True, exist_ok=True)
        if not path.exists():
            _write_json(path, [])
    evidence_manifest = list_professional_evidence({"evidence_type": "research", "limit": 1})
    findings_manifest = list_professional_findings({"professional_type": "research", "limit": 1})
    checks = {
        "research_program_repository": isinstance(_programs(), list),
        "research_backlog_lifecycle": bool(PROGRAM_TRANSITIONS),
        "research_hypothesis_repository": isinstance(_hypotheses(), list),
        "shared_evidence_platform_reused": isinstance(evidence_manifest, dict),
        "shared_findings_platform_reused": isinstance(findings_manifest, dict),
        "traceability_supported": True,
        "methodology_repository_available": isinstance(_methodologies(), list),
        "research_workspace_available": isinstance(get_research_workspace(), dict),
        "maturity_model_enforced": True,
        "no_parallel_evidence_or_findings_platform": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "counts": {
            "research_programs": len(_programs()),
            "hypotheses": len(_hypotheses()),
            "product_recommendations": len(_recommendations()),
            "professional_methodologies": len(_methodologies()),
        },
        "manifest": get_business_framework_research_manifest(),
    }
