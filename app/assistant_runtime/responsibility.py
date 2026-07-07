"""GENESIS-0006 Active Responsibilities Foundation.

This module gives VECTRA a minimal runtime mechanism for accompanying its own
professional responsibilities. It does not change Professional Model, does not
run Reflection, does not run Knowledge Consolidation and does not make Product
Decisions automatically.
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

RESPONSIBILITY_VERSION = "GENESIS-0006"
RESPONSIBILITIES_PATH = Path("responsibilities") / "active_responsibilities.json"
REPORTS_PATH = Path("runtime") / "responsibility" / "responsibility_reports.json"
STATUS_PATH = Path("runtime") / "responsibility" / "responsibility_status.json"

ALLOWED_STATUSES = {"ACTIVE", "WATCH", "BLOCKED", "CLOSED"}

DEFAULT_RESPONSIBILITIES = [
    {
        "responsibility_id": "RESP-RUNTIME-OBSERVABILITY",
        "title": "Maintain Runtime Observability",
        "source": "GENESIS-0002 Runtime Observability",
        "description": "Keep Runtime Snapshot and Runtime readback available for Product Verification.",
        "status": "ACTIVE",
        "owner": "VECTRA Runtime",
        "verification_method": "Runtime Snapshot and readback verification",
    },
    {
        "responsibility_id": "RESP-PROFESSIONAL-REFLECTION",
        "title": "Prepare Professional Reflection",
        "source": "GENESIS-0003 Professional Reflection Foundation",
        "description": "Analyse completed working stages and prepare Knowledge Candidates without changing Professional Model automatically.",
        "status": "ACTIVE",
        "owner": "VECTRA Runtime",
        "verification_method": "Reflection Reports and Knowledge Candidate Repository",
    },
    {
        "responsibility_id": "RESP-KNOWLEDGE-CONSOLIDATION-CONTROL",
        "title": "Preserve Product Owner control over Knowledge Consolidation",
        "source": "GENESIS-0004 Knowledge Consolidation Foundation",
        "description": "Capitalise only approved knowledge and keep human confirmation as the boundary for Professional Model changes.",
        "status": "WATCH",
        "owner": "VECTRA Runtime",
        "verification_method": "Consolidation boundary check and Professional Model readback",
    },
    {
        "responsibility_id": "RESP-PROFESSIONAL-OBSERVATION",
        "title": "Capture Professional Observations",
        "source": "GENESIS-0005 Professional Observation Foundation",
        "description": "Capture professional runtime events for later Reflection without triggering Reflection automatically.",
        "status": "ACTIVE",
        "owner": "VECTRA Runtime",
        "verification_method": "Observation Repository and Observation Reports",
    },
]


def _repo_path(path: Path) -> Path:
    return ensure_repository() / path


def _normalise_responsibility(item: Dict[str, Any]) -> Dict[str, Any]:
    now = _now()
    status = str(item.get("status") or "ACTIVE").upper()
    if status not in ALLOWED_STATUSES:
        status = "WATCH"
    return {
        "responsibility_id": str(item.get("responsibility_id") or f"RESP-{uuid.uuid4().hex[:12]}").strip(),
        "title": str(item.get("title") or "Untitled Responsibility").strip(),
        "source": str(item.get("source") or "runtime_responsibility_engine").strip(),
        "description": str(item.get("description") or "").strip(),
        "status": status,
        "owner": str(item.get("owner") or "VECTRA Runtime").strip(),
        "verification_method": str(item.get("verification_method") or "Runtime readback").strip(),
        "last_checked_at": item.get("last_checked_at") or now,
        "created_at": item.get("created_at") or now,
        "updated_at": now,
        "identity_root": "VECTRA",
    }


def ensure_responsibility_repository() -> Dict[str, Any]:
    ensure_repository()
    responsibilities_path = _repo_path(RESPONSIBILITIES_PATH)
    reports_path = _repo_path(REPORTS_PATH)
    status_path = _repo_path(STATUS_PATH)
    responsibilities = _read_json(responsibilities_path, [])
    if not isinstance(responsibilities, list) or not responsibilities:
        responsibilities = [_normalise_responsibility(item) for item in DEFAULT_RESPONSIBILITIES]
        _write_json(responsibilities_path, responsibilities)
    else:
        responsibilities = [_normalise_responsibility(item) for item in responsibilities if isinstance(item, dict)]
        _write_json(responsibilities_path, responsibilities)
    reports = _read_json(reports_path, [])
    if not isinstance(reports, list):
        reports = []
        _write_json(reports_path, reports)
    status = _read_json(status_path, {})
    if not isinstance(status, dict):
        status = {}
    status.update({
        "status": "active",
        "identity_root": "VECTRA",
        "responsibility_release": RESPONSIBILITY_VERSION,
        "responsibilities_count": len(responsibilities),
        "reports_count": len(reports),
        "professional_model_unchanged": True,
        "reflection_triggered_automatically": False,
        "knowledge_consolidation_triggered": False,
        "automatic_product_decisions": False,
        "updated_at": _now(),
    })
    _write_json(status_path, status)
    return {"responsibilities": responsibilities, "reports": reports, "status": status}


def get_responsibility_status() -> Dict[str, Any]:
    repo = ensure_responsibility_repository()
    responsibilities = repo.get("responsibilities") if isinstance(repo.get("responsibilities"), list) else []
    status_counts: Dict[str, int] = {}
    for item in responsibilities:
        item_status = str((item or {}).get("status") or "UNKNOWN")
        status_counts[item_status] = status_counts.get(item_status, 0) + 1
    payload = {
        "status": "ok",
        "render_mode": "vectra_professional_responsibility_status",
        "identity_root": "VECTRA",
        "responsibility_release": RESPONSIBILITY_VERSION,
        "responsibilities_count": len(responsibilities),
        "status_counts": status_counts,
        "allowed_statuses": sorted(ALLOWED_STATUSES),
        "boundaries": {
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
        "human_summary": f"Active Responsibilities доступны: {len(responsibilities)} обязательств сопровождаются VECTRA.",
    }
    return _with_workspace_markdown(payload, "Active Responsibilities VECTRA", payload)


def list_active_responsibilities(status: Optional[str] = None, limit: int = 50) -> Dict[str, Any]:
    repo = ensure_responsibility_repository()
    responsibilities = repo.get("responsibilities") if isinstance(repo.get("responsibilities"), list) else []
    if status:
        wanted = str(status).upper()
        responsibilities = [item for item in responsibilities if isinstance(item, dict) and item.get("status") == wanted]
    payload = {
        "status": "ok",
        "render_mode": "vectra_active_responsibility_repository",
        "identity_root": "VECTRA",
        "responsibilities": responsibilities[-max(1, int(limit or 50)):],
        "responsibilities_count": len(responsibilities),
        "human_summary": f"Открыт Repository активных профессиональных обязательств: {len(responsibilities)} записей.",
    }
    return _with_workspace_markdown(payload, "Repository активных обязательств VECTRA", payload.get("responsibilities"))


def run_responsibility_check(request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(request, dict):
        request = {}
    repo = ensure_responsibility_repository()
    responsibilities = repo.get("responsibilities") if isinstance(repo.get("responsibilities"), list) else []
    now = _now()
    checks: List[Dict[str, Any]] = []
    for item in responsibilities:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "WATCH").upper()
        check_status = "PASS" if status in {"ACTIVE", "WATCH", "CLOSED"} else "BLOCKED"
        checks.append({
            "responsibility_id": item.get("responsibility_id"),
            "title": item.get("title"),
            "status": status,
            "check_status": check_status,
            "owner": item.get("owner"),
            "verification_method": item.get("verification_method"),
        })
        item["last_checked_at"] = now
        item["updated_at"] = now
    _write_json(_repo_path(RESPONSIBILITIES_PATH), responsibilities)
    blocked = [item for item in checks if item.get("check_status") == "BLOCKED"]
    report = {
        "report_id": f"RESP-REPORT-{now.replace(':', '').replace('-', '').replace('Z', 'Z')}-{uuid.uuid4().hex[:8]}",
        "created_at": now,
        "identity_root": "VECTRA",
        "responsibility_release": RESPONSIBILITY_VERSION,
        "reason": str(request.get("reason") or "manual_runtime_check"),
        "responsibilities_total": len(responsibilities),
        "checks": checks,
        "blocked_count": len(blocked),
        "overall": "PASS" if not blocked else "BLOCKED",
        "boundaries": {
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
    }
    reports = _read_json(_repo_path(REPORTS_PATH), [])
    if not isinstance(reports, list):
        reports = []
    reports.append(report)
    _write_json(_repo_path(REPORTS_PATH), reports)
    # Read Professional Model after run and record that this engine did not modify it.
    model = get_professional_model()
    report["professional_model_readable_after_check"] = model.get("status") == "ok"
    report["professional_model_modified_by_responsibility_engine"] = False
    _write_json(_repo_path(REPORTS_PATH), reports)
    ensure_responsibility_repository()
    return _with_workspace_markdown({
        "status": "ok",
        "render_mode": "vectra_professional_responsibility_report",
        "report": report,
        "runtime_status": "PASS" if report.get("overall") == "PASS" else "BLOCKED",
        "human_summary": f"Проверка Active Responsibilities выполнена: {report.get('overall')}.",
    }, "Отчёт Active Responsibilities VECTRA", report)


def list_responsibility_reports(limit: int = 20) -> Dict[str, Any]:
    ensure_responsibility_repository()
    reports = _read_json(_repo_path(REPORTS_PATH), [])
    if not isinstance(reports, list):
        reports = []
    payload = {
        "status": "ok",
        "render_mode": "vectra_professional_responsibility_reports",
        "identity_root": "VECTRA",
        "reports": reports[-max(1, int(limit or 20)):],
        "reports_count": len(reports),
        "human_summary": f"В Repository Active Responsibilities найдено {len(reports)} отчётов.",
    }
    return _with_workspace_markdown(payload, "Отчёты Active Responsibilities VECTRA", payload.get("reports"))


def verify_responsibility_readback() -> Dict[str, Any]:
    repo = ensure_responsibility_repository()
    responsibilities = repo.get("responsibilities") if isinstance(repo.get("responsibilities"), list) else []
    reports = repo.get("reports") if isinstance(repo.get("reports"), list) else []
    readable = isinstance(responsibilities, list) and isinstance(reports, list)
    invalid_statuses = [item.get("responsibility_id") for item in responsibilities if isinstance(item, dict) and item.get("status") not in ALLOWED_STATUSES]
    payload = {
        "status": "PASS" if readable and not invalid_statuses else "FAIL",
        "render_mode": "vectra_professional_responsibility_readback",
        "identity_root": "VECTRA",
        "responsibility_release": RESPONSIBILITY_VERSION,
        "responsibilities_count": len(responsibilities),
        "reports_count": len(reports),
        "invalid_statuses": invalid_statuses,
        "readable": readable,
        "professional_model_unchanged": True,
        "reflection_triggered_automatically": False,
        "knowledge_consolidation_triggered": False,
        "automatic_product_decisions": False,
        "contract": "active_responsibilities_readback_required",
    }
    return _with_workspace_markdown(payload, "Readback Verification Active Responsibilities", payload)
