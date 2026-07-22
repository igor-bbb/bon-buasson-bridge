"""GENESIS-0003 Professional Reflection Foundation.

Professional Reflection analyses a completed working stage and creates
Knowledge Candidates. It does not consolidate knowledge, does not update the
Professional Model and does not make Product Decisions automatically.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import (
    ensure_repository,
    _now,
    _read_json,
    _write_json,
    _with_workspace_markdown,
)

REFLECTION_VERSION = "GENESIS-0003"
CANDIDATE_STATUSES = {"NEW", "REVIEW", "APPROVED", "REJECTED"}
REFLECTION_DIR = Path("runtime") / "reflection"
CANDIDATES_PATH = REFLECTION_DIR / "knowledge_candidates.json"
REPORTS_PATH = REFLECTION_DIR / "reflection_reports.json"


def _repo_path(relative: Path) -> Path:
    return ensure_repository() / relative


def ensure_reflection_repository() -> Dict[str, Any]:
    base = ensure_repository()
    (base / REFLECTION_DIR).mkdir(parents=True, exist_ok=True)
    candidates_path = base / CANDIDATES_PATH
    reports_path = base / REPORTS_PATH
    if not candidates_path.exists():
        _write_json(candidates_path, [])
    if not reports_path.exists():
        _write_json(reports_path, [])
    candidates = _read_json(candidates_path, [])
    reports = _read_json(reports_path, [])
    if not isinstance(candidates, list):
        candidates = []
        _write_json(candidates_path, candidates)
    if not isinstance(reports, list):
        reports = []
        _write_json(reports_path, reports)
    return {
        "status": "ok",
        "release": REFLECTION_VERSION,
        "candidates_path": str(CANDIDATES_PATH),
        "reports_path": str(REPORTS_PATH),
        "candidates_count": len(candidates),
        "reports_count": len(reports),
        "candidate_statuses": sorted(CANDIDATE_STATUSES),
        "professional_model_write_enabled": False,
        "knowledge_consolidation_enabled": False,
    }


def get_reflection_status() -> Dict[str, Any]:
    repo = ensure_reflection_repository()
    payload = {
        "status": "ok",
        "render_mode": "vectra_professional_reflection_status",
        "identity_root": "VECTRA",
        "reflection_release": REFLECTION_VERSION,
        "repository": repo,
        "capabilities": [
            "professional_reflection_engine",
            "reflection_analysis",
            "knowledge_candidate_repository",
            "reflection_report",
            "candidate_status_lifecycle",
        ],
        "boundaries": {
            "professional_model_unchanged": True,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
        "human_summary": "Professional Reflection доступен как внутренний Runtime-механизм VECTRA. Он формирует только кандидатов в знания.",
    }
    return _with_workspace_markdown(payload, "Professional Reflection VECTRA", payload)


def _as_list(value: Any) -> List[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    return [value]


def _text(value: Any, fallback: str = "") -> str:
    text = str(value or "").strip()
    return text or fallback


def _candidate_id() -> str:
    return f"KC-{_now().replace(':', '').replace('-', '').replace('Z', 'Z')}-{uuid.uuid4().hex[:8]}"


def _build_candidate(
    *,
    reflection_id: str,
    source: str,
    description: str,
    rationale: str,
    recommended_action: str,
    candidate_type: str,
    status: str = "NEW",
    metadata: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    now = _now()
    clean_status = str(status or "NEW").upper()
    if clean_status not in CANDIDATE_STATUSES:
        clean_status = "NEW"
    return {
        "candidate_id": _candidate_id(),
        "reflection_id": reflection_id,
        "source": source,
        "description": description,
        "rationale": rationale,
        "recommended_action": recommended_action,
        "status": clean_status,
        "candidate_type": candidate_type,
        "created_at": now,
        "updated_at": now,
        "identity_root": "VECTRA",
        "metadata": metadata or {},
    }


def _candidate_from_observation(reflection_id: str, source: str, item: Any) -> Optional[Dict[str, Any]]:
    if isinstance(item, dict):
        description = _text(item.get("description") or item.get("observation") or item.get("title"))
        rationale = _text(item.get("rationale") or item.get("evidence") or item.get("reason"), "Observation was confirmed during the completed working stage.")
        recommended_action = _text(item.get("recommended_action") or item.get("action"), "Review by Product Owner before consolidation.")
        candidate_type = _text(item.get("candidate_type") or item.get("type"), "confirmed_observation")
        status = _text(item.get("status"), "NEW")
        metadata = item.get("metadata") if isinstance(item.get("metadata"), dict) else {}
    else:
        description = _text(item)
        rationale = "Observation was confirmed during the completed working stage."
        recommended_action = "Review by Product Owner before consolidation."
        candidate_type = "confirmed_observation"
        status = "NEW"
        metadata = {}
    if not description:
        return None
    return _build_candidate(
        reflection_id=reflection_id,
        source=source,
        description=description,
        rationale=rationale,
        recommended_action=recommended_action,
        candidate_type=candidate_type,
        status=status,
        metadata=metadata,
    )


def _extract_reflection_inputs(payload: Dict[str, Any]) -> Dict[str, Any]:
    completed_stage = payload.get("completed_stage") if isinstance(payload.get("completed_stage"), dict) else {}
    source = _text(payload.get("source") or completed_stage.get("source") or completed_stage.get("stage_id"), "completed_working_stage")
    summary = _text(
        payload.get("stage_summary")
        or payload.get("summary")
        or completed_stage.get("summary")
        or completed_stage.get("result"),
        "Completed working stage was submitted for Professional Reflection.",
    )
    observations = []
    for key in ("confirmed_observations", "observations", "knowledge_candidates"):
        observations.extend(_as_list(payload.get(key)))
    if isinstance(completed_stage.get("confirmed_observations"), list):
        observations.extend(completed_stage.get("confirmed_observations"))
    potential_knowledge = _as_list(payload.get("potential_permanent_knowledge"))
    responsibility_changes = _as_list(payload.get("professional_responsibility_changes"))
    product_owner_questions = _as_list(payload.get("product_owner_questions"))
    return {
        "source": source,
        "summary": summary,
        "observations": observations,
        "potential_permanent_knowledge": potential_knowledge,
        "responsibility_changes": responsibility_changes,
        "product_owner_questions": product_owner_questions,
    }


def run_professional_reflection(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Run Reflection for a completed stage and persist Knowledge Candidates."""
    ensure_reflection_repository()
    if not isinstance(payload, dict):
        payload = {}
    inputs = _extract_reflection_inputs(payload)
    now = _now()
    reflection_id = f"REF-{now.replace(':', '').replace('-', '').replace('Z', 'Z')}-{uuid.uuid4().hex[:8]}"
    source = inputs["source"]

    new_candidates: List[Dict[str, Any]] = []
    for item in inputs["observations"]:
        candidate = _candidate_from_observation(reflection_id, source, item)
        if candidate:
            new_candidates.append(candidate)
    for item in inputs["potential_permanent_knowledge"]:
        candidate = _candidate_from_observation(reflection_id, source, item)
        if candidate:
            candidate["candidate_type"] = "potential_permanent_knowledge"
            new_candidates.append(candidate)
    for item in inputs["responsibility_changes"]:
        candidate = _candidate_from_observation(reflection_id, source, item)
        if candidate:
            candidate["candidate_type"] = "professional_responsibility_change"
            candidate["recommended_action"] = candidate.get("recommended_action") or "Product Owner must decide whether this responsibility change becomes part of the professional model."
            new_candidates.append(candidate)

    candidates_path = _repo_path(CANDIDATES_PATH)
    all_candidates = _read_json(candidates_path, [])
    if not isinstance(all_candidates, list):
        all_candidates = []
    all_candidates.extend(new_candidates)
    _write_json(candidates_path, all_candidates)

    report = {
        "reflection_id": reflection_id,
        "status": "completed",
        "reflection_release": REFLECTION_VERSION,
        "created_at": now,
        "source": source,
        "stage_summary": inputs["summary"],
        "confirmed_observations_count": len(inputs["observations"]),
        "knowledge_candidates_count": len(new_candidates),
        "knowledge_candidates": new_candidates,
        "professional_responsibility_changes": inputs["responsibility_changes"],
        "product_owner_questions": inputs["product_owner_questions"],
        "stage_analysis_result": {
            "summary": inputs["summary"],
            "candidate_repository_updated": True,
            "professional_model_unchanged": True,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
        "identity_root": "VECTRA",
    }
    reports_path = _repo_path(REPORTS_PATH)
    reports = _read_json(reports_path, [])
    if not isinstance(reports, list):
        reports = []
    reports.append(report)
    _write_json(reports_path, reports)

    result = {
        "status": "ok",
        "render_mode": "vectra_professional_reflection_report",
        "identity_root": "VECTRA",
        "reflection_report": report,
        "knowledge_candidates_count": len(new_candidates),
        "professional_model_unchanged": True,
        "knowledge_consolidation_triggered": False,
        "human_summary": f"Reflection завершён. Сформировано кандидатов в знания: {len(new_candidates)}. Professional Model не изменялась.",
    }
    return _with_workspace_markdown(result, "Reflection Report VECTRA", report)


def list_knowledge_candidates(status: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
    ensure_reflection_repository()
    candidates = _read_json(_repo_path(CANDIDATES_PATH), [])
    if not isinstance(candidates, list):
        candidates = []
    if status:
        clean_status = str(status).upper()
        candidates = [c for c in candidates if isinstance(c, dict) and c.get("status") == clean_status]
    visible = candidates[-max(1, int(limit or 50)):]
    payload = {
        "status": "ok",
        "render_mode": "vectra_knowledge_candidate_repository",
        "identity_root": "VECTRA",
        "candidates_count": len(candidates),
        "candidate_statuses": sorted(CANDIDATE_STATUSES),
        "knowledge_candidates": visible,
        "professional_model_unchanged": True,
        "knowledge_consolidation_triggered": False,
        "human_summary": f"В Repository найдено кандидатов в знания: {len(candidates)}.",
    }
    return _with_workspace_markdown(payload, "Knowledge Candidate Repository VECTRA", visible)


def update_knowledge_candidate_status(candidate_id: str, status: str, reviewer_note: str = "") -> Dict[str, Any]:
    ensure_reflection_repository()
    clean_status = str(status or "").upper()
    if clean_status not in CANDIDATE_STATUSES:
        return _with_workspace_markdown({
            "status": "error",
            "render_mode": "vectra_knowledge_candidate_status_error",
            "reason": "invalid_status",
            "allowed_statuses": sorted(CANDIDATE_STATUSES),
        }, "Ошибка статуса Knowledge Candidate")
    candidates_path = _repo_path(CANDIDATES_PATH)
    candidates = _read_json(candidates_path, [])
    if not isinstance(candidates, list):
        candidates = []
    found = None
    for candidate in candidates:
        if isinstance(candidate, dict) and candidate.get("candidate_id") == candidate_id:
            candidate["status"] = clean_status
            candidate["updated_at"] = _now()
            if reviewer_note:
                candidate["reviewer_note"] = reviewer_note
            found = candidate
            break
    if not found:
        return _with_workspace_markdown({
            "status": "error",
            "render_mode": "vectra_knowledge_candidate_status_error",
            "reason": "candidate_not_found",
            "candidate_id": candidate_id,
        }, "Knowledge Candidate не найден")
    _write_json(candidates_path, candidates)
    payload = {
        "status": "ok",
        "render_mode": "vectra_knowledge_candidate_status_update",
        "identity_root": "VECTRA",
        "candidate": found,
        "professional_model_unchanged": True,
        "knowledge_consolidation_triggered": False,
        "human_summary": f"Статус кандидата {candidate_id} изменён на {clean_status}. Professional Model не изменялась.",
    }
    return _with_workspace_markdown(payload, "Обновление статуса Knowledge Candidate", found)


def list_reflection_reports(limit: int = 20) -> Dict[str, Any]:
    ensure_reflection_repository()
    reports = _read_json(_repo_path(REPORTS_PATH), [])
    if not isinstance(reports, list):
        reports = []
    visible = reports[-max(1, int(limit or 20)):]
    payload = {
        "status": "ok",
        "render_mode": "vectra_reflection_reports",
        "identity_root": "VECTRA",
        "reports_count": len(reports),
        "reflection_reports": visible,
        "professional_model_unchanged": True,
        "knowledge_consolidation_triggered": False,
        "human_summary": f"Найдено Reflection Report: {len(reports)}.",
    }
    return _with_workspace_markdown(payload, "Reflection Reports VECTRA", visible)


def verify_reflection_readback() -> Dict[str, Any]:
    repo = ensure_reflection_repository()
    candidates = _read_json(_repo_path(CANDIDATES_PATH), [])
    reports = _read_json(_repo_path(REPORTS_PATH), [])
    candidates_ok = isinstance(candidates, list)
    reports_ok = isinstance(reports, list)
    statuses_ok = all((not isinstance(c, dict)) or c.get("status") in CANDIDATE_STATUSES for c in candidates)
    payload = {
        "status": "PASS" if candidates_ok and reports_ok and statuses_ok else "FAIL",
        "object": "professional_reflection",
        "readable": candidates_ok and reports_ok,
        "candidates_count": len(candidates) if isinstance(candidates, list) else None,
        "reports_count": len(reports) if isinstance(reports, list) else None,
        "candidate_statuses_supported": sorted(CANDIDATE_STATUSES),
        "candidate_statuses_valid": statuses_ok,
        "professional_model_unchanged": True,
        "knowledge_consolidation_triggered": False,
        "repository": repo,
        "contract": "professional_reflection_readback_required",
    }
    return _with_workspace_markdown(payload, "Readback Verification Professional Reflection", payload)
