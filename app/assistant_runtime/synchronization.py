"""GENESIS-0008 Laboratory Synchronization Foundation.

This module gives VECTRA a minimal, verifiable mechanism for preparing safe
Laboratory -> Working VECTRA synchronization packages. It does not change the
Professional Model, does not run Reflection, does not run Knowledge
Consolidation, does not apply changes automatically to Working VECTRA and does
not make Product Decisions automatically.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import (
    ensure_repository,
    get_professional_model,
    get_current_state,
    list_product_decisions,
    list_knowledge_documents,
    list_recovery_snapshots,
    _now,
    _read_json,
    _write_json,
    _with_workspace_markdown,
)
from app.assistant_runtime.reflection import list_knowledge_candidates
from app.assistant_runtime.responsibility import list_active_responsibilities
from app.assistant_runtime.recovery import list_recovery_checkpoints, verify_recovery_evolution_readback

SYNCHRONIZATION_VERSION = "GENESIS-0008"
STATUS_PATH = Path("runtime") / "synchronization" / "synchronization_status.json"
PACKAGES_PATH = Path("runtime") / "synchronization" / "synchronization_packages.json"
REPORTS_PATH = Path("runtime") / "synchronization" / "synchronization_reports.json"


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


def _approved_candidates(limit: int = 100) -> List[Dict[str, Any]]:
    payload = list_knowledge_candidates(status="APPROVED", limit=limit)
    candidates = payload.get("knowledge_candidates") if isinstance(payload.get("knowledge_candidates"), list) else []
    return [item for item in candidates if isinstance(item, dict) and item.get("status") == "APPROVED"]


def _latest_recovery_marker() -> Dict[str, Any]:
    checkpoints = list_recovery_checkpoints(limit=1).get("checkpoints", [])
    snapshots = list_recovery_snapshots(limit=1).get("snapshots", [])
    checkpoint = checkpoints[-1] if isinstance(checkpoints, list) and checkpoints else None
    snapshot = snapshots[-1] if isinstance(snapshots, list) and snapshots else None
    return {
        "checkpoint_id": checkpoint.get("checkpoint_id") if isinstance(checkpoint, dict) else None,
        "snapshot_id": snapshot.get("snapshot_id") if isinstance(snapshot, dict) else None,
    }


def ensure_synchronization_repository() -> Dict[str, Any]:
    ensure_repository()
    packages = _read_json(_repo_path(PACKAGES_PATH), [])
    if not isinstance(packages, list):
        packages = []
        _write_json(_repo_path(PACKAGES_PATH), packages)
    reports = _read_json(_repo_path(REPORTS_PATH), [])
    if not isinstance(reports, list):
        reports = []
        _write_json(_repo_path(REPORTS_PATH), reports)

    model = _professional_model_body()
    approved = _approved_candidates(limit=100)
    latest = _latest_recovery_marker()
    status = _read_json(_repo_path(STATUS_PATH), {})
    if not isinstance(status, dict):
        status = {}
    status.update({
        "status": "active",
        "identity_root": "VECTRA",
        "synchronization_release": SYNCHRONIZATION_VERSION,
        "mode": "laboratory_to_working_vectra_package_preparation",
        "packages_count": len(packages),
        "reports_count": len(reports),
        "approved_candidates_available": len(approved),
        "professional_model_id": model.get("model_id"),
        "professional_model_updated_at": model.get("updated_at"),
        "latest_recovery_checkpoint_id": latest.get("checkpoint_id"),
        "latest_recovery_snapshot_id": latest.get("snapshot_id"),
        "boundaries": {
            "professional_model_unchanged": True,
            "working_vectra_not_modified_automatically": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
            "product_owner_approval_required_for_apply": True,
        },
        "updated_at": _now(),
    })
    _write_json(_repo_path(STATUS_PATH), status)
    return {"status": status, "packages": packages, "reports": reports}


def get_synchronization_status() -> Dict[str, Any]:
    repo = ensure_synchronization_repository()
    payload = {
        "status": "ok",
        "render_mode": "vectra_synchronization_status",
        "identity_root": "VECTRA",
        **(repo.get("status") if isinstance(repo.get("status"), dict) else {}),
        "human_summary": "Laboratory → Working VECTRA Synchronization активна в безопасном режиме подготовки пакетов. Автоматическое применение в Working VECTRA не выполняется.",
    }
    return _with_workspace_markdown(payload, "Laboratory Synchronization VECTRA", payload)


def build_synchronization_package(request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(request, dict):
        request = {}
    ensure_synchronization_repository()
    now = _now()

    model = _professional_model_body()
    state = get_current_state()
    approved = _approved_candidates(limit=int(request.get("candidate_limit") or 100))
    decisions = list_product_decisions(limit=20)
    knowledge = list_knowledge_documents()
    responsibilities = list_active_responsibilities(limit=100)
    latest_recovery = _latest_recovery_marker()
    recovery_verify = verify_recovery_evolution_readback(checkpoint_id=latest_recovery.get("checkpoint_id"))

    package = {
        "package_id": f"SYNC-PACKAGE-{now.replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:8]}",
        "created_at": now,
        "identity_root": "VECTRA",
        "synchronization_release": SYNCHRONIZATION_VERSION,
        "source_environment": "VECTRA Laboratory",
        "target_environment": "Working VECTRA",
        "status": "PREPARED",
        "reason": str(request.get("reason") or "manual_synchronization_preparation"),
        "professional_model_reference": {
            "model_id": model.get("model_id"),
            "updated_at": model.get("updated_at"),
            "sections": sorted((model.get("sections") or {}).keys()) if isinstance(model.get("sections"), dict) else [],
        },
        "approved_candidates": approved,
        "approved_candidates_count": len(approved),
        "active_responsibilities_count": responsibilities.get("responsibilities_count"),
        "product_decisions_count": decisions.get("decisions_count"),
        "knowledge_documents_count": len(knowledge.get("documents") or []) if isinstance(knowledge.get("documents"), list) else 0,
        "recovery_marker": latest_recovery,
        "recovery_readback_status": recovery_verify.get("status"),
        "professional_state_available": isinstance(state, dict) and bool(state),
        "apply_status": "NOT_APPLIED",
        "apply_boundary": "Product Owner approval required before any Working VECTRA apply step.",
        "boundaries": {
            "professional_model_unchanged": True,
            "working_vectra_not_modified_automatically": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
            "product_owner_approval_required_for_apply": True,
        },
    }

    packages = _read_list(PACKAGES_PATH)
    packages.append(package)
    _write_list(PACKAGES_PATH, packages)

    verify = verify_synchronization_readback(package_id=package["package_id"])
    report = {
        "report_id": f"SYNC-REPORT-{now.replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:8]}",
        "created_at": now,
        "identity_root": "VECTRA",
        "synchronization_release": SYNCHRONIZATION_VERSION,
        "package_id": package["package_id"],
        "readback_status": verify.get("status"),
        "overall": "PASS" if verify.get("status") == "PASS" else "FAIL",
        "approved_candidates_count": len(approved),
        "recovery_readback_status": recovery_verify.get("status"),
        "apply_status": package["apply_status"],
        "boundaries": package["boundaries"],
    }
    reports = _read_list(REPORTS_PATH)
    reports.append(report)
    _write_list(REPORTS_PATH, reports)
    ensure_synchronization_repository()

    payload = {
        "status": "ok" if report["overall"] == "PASS" else "degraded",
        "render_mode": "vectra_synchronization_run",
        "identity_root": "VECTRA",
        "package": package,
        "report": report,
        "readback_verification": verify,
        "human_summary": "Synchronization Foundation подготовил проверяемый пакет переноса Laboratory → Working VECTRA без автоматического применения.",
    }
    return _with_workspace_markdown(payload, "Synchronization Run VECTRA", {"package": package, "report": report})


def list_synchronization_packages(limit: int = 20) -> Dict[str, Any]:
    ensure_synchronization_repository()
    packages = _read_list(PACKAGES_PATH)
    limited = packages[-max(1, int(limit or 20)):]
    payload = {
        "status": "ok",
        "render_mode": "vectra_synchronization_packages",
        "identity_root": "VECTRA",
        "packages": limited,
        "packages_count": len(packages),
        "human_summary": f"Synchronization Packages доступны: {len(packages)} подготовленных пакетов.",
    }
    return _with_workspace_markdown(payload, "Synchronization Packages VECTRA", limited)


def list_synchronization_reports(limit: int = 20) -> Dict[str, Any]:
    ensure_synchronization_repository()
    reports = _read_list(REPORTS_PATH)
    limited = reports[-max(1, int(limit or 20)):]
    payload = {
        "status": "ok",
        "render_mode": "vectra_synchronization_reports",
        "identity_root": "VECTRA",
        "reports": limited,
        "reports_count": len(reports),
        "human_summary": f"Synchronization Reports доступны: {len(reports)} отчётов.",
    }
    return _with_workspace_markdown(payload, "Synchronization Reports VECTRA", limited)


def verify_synchronization_readback(package_id: Optional[str] = None) -> Dict[str, Any]:
    ensure_synchronization_repository()
    packages = _read_list(PACKAGES_PATH)
    reports = _read_list(REPORTS_PATH)
    if package_id:
        packages = [item for item in packages if item.get("package_id") == package_id]
    latest = packages[-1] if packages else None
    model = _professional_model_body()
    checks = [
        {"object": "synchronization_repository", "status": "PASS", "packages_count": len(packages)},
        {"object": "professional_model_reference", "status": "PASS" if model.get("model_id") else "FAIL"},
        {"object": "package_readback", "status": "PASS" if isinstance(latest, dict) else "FAIL"},
        {"object": "apply_boundary", "status": "PASS" if not latest or latest.get("apply_status") == "NOT_APPLIED" else "FAIL"},
        {"object": "automatic_product_decisions", "status": "PASS" if not latest or latest.get("boundaries", {}).get("automatic_product_decisions") is False else "FAIL"},
        {"object": "working_vectra_not_modified", "status": "PASS" if not latest or latest.get("boundaries", {}).get("working_vectra_not_modified_automatically") is True else "FAIL"},
    ]
    overall = "PASS" if all(item.get("status") == "PASS" for item in checks) else "FAIL"
    return {
        "status": overall,
        "render_mode": "vectra_synchronization_readback_verification",
        "identity_root": "VECTRA",
        "synchronization_release": SYNCHRONIZATION_VERSION,
        "package_id": latest.get("package_id") if isinstance(latest, dict) else package_id,
        "reports_count": len(reports),
        "checks": checks,
        "professional_model_unchanged": True,
        "working_vectra_not_modified_automatically": True,
        "reflection_triggered_automatically": False,
        "knowledge_consolidation_triggered": False,
        "automatic_product_decisions": False,
        "product_owner_approval_required_for_apply": True,
    }
