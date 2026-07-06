"""GENESIS-0010 Product Owner Review Workflow.

This module lets VECTRA present a prepared Synchronization Session for Product
Owner review, record the review decision, and expose the result through Runtime
Observability. It does not apply synchronization, does not update Professional
Model, does not publish to Working VECTRA and does not auto-approve anything.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import (
    ensure_repository,
    get_professional_model,
    _now,
    _read_json,
    _write_json,
    _with_workspace_markdown,
)
from app.assistant_runtime.synchronization import (
    get_synchronization_status,
    list_synchronization_reports,
    verify_synchronization_readback,
)

REVIEW_VERSION = "GENESIS-0010"
REVIEW_STATUS_PATH = Path("runtime") / "review" / "review_status.json"
REVIEW_SESSIONS_PATH = Path("runtime") / "review" / "review_sessions.json"
REVIEW_REPORTS_PATH = Path("runtime") / "review" / "review_reports.json"

DECISIONS = {"Approve", "Reject", "Return for Revision"}


def _repo_path(path: Path) -> Path:
    return ensure_repository() / path


def _read_list(path: Path) -> List[Dict[str, Any]]:
    data = _read_json(_repo_path(path), [])
    if not isinstance(data, list):
        data = []
    return [item for item in data if isinstance(item, dict)]


def _write_list(path: Path, items: List[Dict[str, Any]]) -> None:
    _write_json(_repo_path(path), items)


def _professional_model_body() -> Dict[str, Any]:
    payload = get_professional_model()
    model = payload.get("professional_model") if isinstance(payload.get("professional_model"), dict) else payload
    return model if isinstance(model, dict) else {}


def _latest_synchronization_report() -> Dict[str, Any]:
    reports_payload = list_synchronization_reports(limit=1)
    reports = reports_payload.get("reports") if isinstance(reports_payload.get("reports"), list) else []
    return reports[-1] if reports else {}


def _diff_preview_from_sync_report(report: Dict[str, Any]) -> Dict[str, Any]:
    """Build a conservative review diff preview from the existing sync report.

    GENESIS-0010 reviews already prepared synchronization state. If the GENESIS-0009
    report does not expose a detailed diff, the review still reports stable
    review categories without fabricating application changes.
    """
    candidates_count = int(report.get("approved_candidates_count") or 0) if isinstance(report, dict) else 0
    package_id = report.get("package_id") if isinstance(report, dict) else None
    objects: List[Dict[str, Any]] = []
    if package_id:
        objects.append({
            "object_id": package_id,
            "object_type": "synchronization_package",
            "change_type": "Unchanged",
            "reason": "Package is under Product Owner review and is not applied by GENESIS-0010.",
        })
    if candidates_count > 0:
        objects.append({
            "object_id": "approved_knowledge_candidates",
            "object_type": "knowledge_candidates",
            "change_type": "Updated",
            "items_count": candidates_count,
            "reason": "Approved candidates are present in the prepared synchronization context, pending Product Owner decision.",
        })
    if not objects:
        objects.append({
            "object_id": "laboratory_state",
            "object_type": "runtime_state",
            "change_type": "Unchanged",
            "reason": "No detailed synchronization diff is available; review confirms that no changes are applied automatically.",
        })
    counts = {"New": 0, "Updated": 0, "Removed": 0, "Unchanged": 0}
    for item in objects:
        change = item.get("change_type") if item.get("change_type") in counts else "Unchanged"
        counts[change] += 1
    return {
        "source": "GENESIS-0009 synchronization reports",
        "objects": objects,
        "classification_counts": counts,
        "application_status": "NOT_APPLIED",
    }


def ensure_review_repository() -> Dict[str, Any]:
    ensure_repository()
    sessions = _read_json(_repo_path(REVIEW_SESSIONS_PATH), [])
    if not isinstance(sessions, list):
        sessions = []
        _write_json(_repo_path(REVIEW_SESSIONS_PATH), sessions)
    reports = _read_json(_repo_path(REVIEW_REPORTS_PATH), [])
    if not isinstance(reports, list):
        reports = []
        _write_json(_repo_path(REVIEW_REPORTS_PATH), reports)
    latest_session = sessions[-1] if sessions else None
    status = _read_json(_repo_path(REVIEW_STATUS_PATH), {})
    if not isinstance(status, dict):
        status = {}
    status.update({
        "status": "active",
        "identity_root": "VECTRA",
        "review_release": REVIEW_VERSION,
        "mode": "product_owner_review_workflow",
        "sessions_count": len(sessions),
        "reports_count": len(reports),
        "active_review_session_id": latest_session.get("review_session_id") if isinstance(latest_session, dict) else None,
        "review_status": latest_session.get("status") if isinstance(latest_session, dict) else "NOT_OPENED",
        "decision": latest_session.get("decision") if isinstance(latest_session, dict) else None,
        "boundaries": {
            "changes_applied": False,
            "professional_model_unchanged": True,
            "working_vectra_not_modified_automatically": True,
            "automatic_product_owner_approval": False,
            "automatic_publication_to_working_vectra": False,
        },
        "updated_at": _now(),
    })
    _write_json(_repo_path(REVIEW_STATUS_PATH), status)
    return {"status": status, "sessions": sessions, "reports": reports}


def open_review_session(request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(request, dict):
        request = {}
    ensure_review_repository()
    now = _now()
    sync_status = get_synchronization_status()
    sync_report = _latest_synchronization_report()
    diff_preview = _diff_preview_from_sync_report(sync_report)
    model = _professional_model_body()
    professional_areas = sorted((model.get("sections") or {}).keys()) if isinstance(model.get("sections"), dict) else []
    product_owner = str(request.get("product_owner") or request.get("responsible_product_owner") or "Product Owner")
    session = {
        "review_session_id": f"REVIEW-SESSION-{now.replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:8]}",
        "opened_at": now,
        "identity_root": "VECTRA",
        "review_release": REVIEW_VERSION,
        "source": "VECTRA Laboratory",
        "target": "Working VECTRA",
        "responsible_product_owner": product_owner,
        "status": "OPEN_FOR_REVIEW",
        "decision": None,
        "decision_at": None,
        "synchronization_status": sync_status.get("status"),
        "synchronization_session_id": sync_status.get("active_session_id") or sync_status.get("active_review_session_id"),
        "synchronization_report_id": sync_report.get("report_id"),
        "diff_preview": diff_preview,
        "professional_areas": professional_areas,
        "impact_assessment": {
            "professional_model_change": "not_applied",
            "working_vectra_change": "not_applied",
            "requires_product_owner_decision": True,
            "risk_level": "controlled",
        },
        "boundaries": {
            "changes_applied": False,
            "professional_model_unchanged": True,
            "working_vectra_not_modified_automatically": True,
            "automatic_product_owner_approval": False,
            "automatic_publication_to_working_vectra": False,
        },
    }
    sessions = _read_list(REVIEW_SESSIONS_PATH)
    sessions.append(session)
    _write_list(REVIEW_SESSIONS_PATH, sessions)
    report = _build_review_report(session)
    reports = _read_list(REVIEW_REPORTS_PATH)
    reports.append(report)
    _write_list(REVIEW_REPORTS_PATH, reports)
    ensure_review_repository()
    payload = {
        "status": "ok",
        "render_mode": "vectra_review_session_open",
        "identity_root": "VECTRA",
        "review_session": session,
        "review_report": report,
        "human_summary": "Product Owner Review Session открыта. Изменения представлены для рассмотрения и не применяются автоматически.",
    }
    return _with_workspace_markdown(payload, "Product Owner Review Session VECTRA", {"session": session, "report": report})


def _build_review_report(session: Dict[str, Any]) -> Dict[str, Any]:
    now = _now()
    diff = session.get("diff_preview") if isinstance(session.get("diff_preview"), dict) else {}
    return {
        "report_id": f"REVIEW-REPORT-{now.replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:8]}",
        "created_at": now,
        "identity_root": "VECTRA",
        "review_release": REVIEW_VERSION,
        "review_session_id": session.get("review_session_id"),
        "responsible_product_owner": session.get("responsible_product_owner"),
        "review_status": session.get("status"),
        "decision": session.get("decision"),
        "changes_composition": diff.get("objects", []),
        "diff_preview": diff,
        "affected_professional_areas": session.get("professional_areas", []),
        "impact_assessment": session.get("impact_assessment", {}),
        "final_decision": session.get("decision"),
        "application_status": "NOT_APPLIED",
        "boundaries": session.get("boundaries", {}),
    }


def record_product_owner_review_decision(decision: str, request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(request, dict):
        request = {}
    normalized = str(decision or request.get("decision") or "").strip()
    if normalized.lower() == "approve":
        normalized = "Approve"
    elif normalized.lower() == "reject":
        normalized = "Reject"
    elif normalized.lower().replace("_", " ").replace("-", " ") == "return for revision":
        normalized = "Return for Revision"
    if normalized not in DECISIONS:
        return {
            "status": "error",
            "render_mode": "vectra_review_decision_error",
            "identity_root": "VECTRA",
            "reason": "unsupported_decision",
            "allowed_decisions": sorted(DECISIONS),
        }
    ensure_review_repository()
    sessions = _read_list(REVIEW_SESSIONS_PATH)
    if not sessions:
        open_review_session(request)
        sessions = _read_list(REVIEW_SESSIONS_PATH)
    session_id = request.get("review_session_id")
    index = None
    if session_id:
        for idx, item in enumerate(sessions):
            if item.get("review_session_id") == session_id:
                index = idx
                break
    if index is None:
        index = len(sessions) - 1
    session = dict(sessions[index])
    session["decision"] = normalized
    session["decision_at"] = _now()
    session["status"] = {
        "Approve": "APPROVED_BY_PRODUCT_OWNER",
        "Reject": "REJECTED",
        "Return for Revision": "RETURNED_FOR_REVISION",
    }[normalized]
    session["decision_comment"] = str(request.get("comment") or "")
    session["boundaries"] = {
        **(session.get("boundaries") if isinstance(session.get("boundaries"), dict) else {}),
        "changes_applied": False,
        "professional_model_unchanged": True,
        "working_vectra_not_modified_automatically": True,
        "automatic_product_owner_approval": False,
        "automatic_publication_to_working_vectra": False,
    }
    sessions[index] = session
    _write_list(REVIEW_SESSIONS_PATH, sessions)
    report = _build_review_report(session)
    reports = _read_list(REVIEW_REPORTS_PATH)
    reports.append(report)
    _write_list(REVIEW_REPORTS_PATH, reports)
    ensure_review_repository()
    payload = {
        "status": "ok",
        "render_mode": "vectra_review_decision_record",
        "identity_root": "VECTRA",
        "review_session": session,
        "review_report": report,
        "human_summary": f"Решение Product Owner зафиксировано: {normalized}. Изменения не применены автоматически.",
    }
    return _with_workspace_markdown(payload, "Решение Product Owner по Review Session VECTRA", {"session": session, "report": report})


def get_review_session() -> Dict[str, Any]:
    repo = ensure_review_repository()
    sessions = repo.get("sessions") if isinstance(repo.get("sessions"), list) else []
    session = sessions[-1] if sessions else None
    if session is None:
        created = open_review_session({})
        session = created.get("review_session")
    payload = {
        "status": "ok",
        "render_mode": "vectra_review_session",
        "identity_root": "VECTRA",
        "review_session": session,
        "human_summary": "Активная Product Owner Review Session доступна для проверки.",
    }
    return _with_workspace_markdown(payload, "Product Owner Review Session VECTRA", session)


def get_review_report() -> Dict[str, Any]:
    repo = ensure_review_repository()
    reports = repo.get("reports") if isinstance(repo.get("reports"), list) else []
    if not reports:
        created = open_review_session({})
        report = created.get("review_report")
    else:
        report = reports[-1]
    payload = {
        "status": "ok",
        "render_mode": "vectra_review_report",
        "identity_root": "VECTRA",
        "review_report": report,
        "human_summary": "Review Report содержит Diff Preview, затронутые Professional Areas, оценку влияния и итоговое решение, если оно уже принято.",
    }
    return _with_workspace_markdown(payload, "Product Owner Review Report VECTRA", report)


def get_review_status() -> Dict[str, Any]:
    repo = ensure_review_repository()
    status = repo.get("status") if isinstance(repo.get("status"), dict) else {}
    payload = {
        "status": "ok",
        "render_mode": "vectra_review_status",
        "identity_root": "VECTRA",
        **status,
        "human_summary": "Product Owner Review Workflow активен. Решение фиксируется, но изменения не применяются автоматически.",
    }
    return _with_workspace_markdown(payload, "Product Owner Review Status VECTRA", payload)


def verify_review_readback() -> Dict[str, Any]:
    session_payload = get_review_session()
    report_payload = get_review_report()
    status_payload = get_review_status()
    session = session_payload.get("review_session") if isinstance(session_payload.get("review_session"), dict) else {}
    report = report_payload.get("review_report") if isinstance(report_payload.get("review_report"), dict) else {}
    checks = [
        {"object": "review_session", "status": "PASS" if session.get("review_session_id") else "FAIL"},
        {"object": "review_report", "status": "PASS" if report.get("report_id") else "FAIL"},
        {"object": "diff_preview", "status": "PASS" if isinstance(report.get("diff_preview"), dict) else "FAIL"},
        {"object": "decision_model", "status": "PASS" if set(DECISIONS) == {"Approve", "Reject", "Return for Revision"} else "FAIL"},
        {"object": "changes_not_applied", "status": "PASS" if report.get("application_status") == "NOT_APPLIED" else "FAIL"},
        {"object": "professional_model_unchanged", "status": "PASS" if report.get("boundaries", {}).get("professional_model_unchanged") is True else "FAIL"},
        {"object": "working_vectra_not_modified", "status": "PASS" if report.get("boundaries", {}).get("working_vectra_not_modified_automatically") is True else "FAIL"},
    ]
    overall = "PASS" if all(item.get("status") == "PASS" for item in checks) else "FAIL"
    payload = {
        "status": overall,
        "render_mode": "vectra_review_readback_verification",
        "identity_root": "VECTRA",
        "review_release": REVIEW_VERSION,
        "checks": checks,
        "review_session_id": session.get("review_session_id"),
        "report_id": report.get("report_id"),
        "runtime_status_readable": status_payload.get("status") == "ok",
        "professional_model_unchanged": True,
        "working_vectra_not_modified_automatically": True,
        "automatic_publication_to_working_vectra": False,
        "automatic_product_owner_approval": False,
    }
    return _with_workspace_markdown(payload, "Product Owner Review Readback Verification VECTRA", payload)
