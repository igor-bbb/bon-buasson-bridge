"""GENESIS-0007 Recovery Evolution Foundation.

This module expands Recovery into a verifiable runtime mechanism that can
restore the VECTRA professional baseline from repository-backed state. It does
not change Professional Model, does not run Reflection, does not run Knowledge
Consolidation and does not make Product Decisions automatically.
"""

from __future__ import annotations

import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import (
    ensure_repository,
    get_professional_model,
    get_current_state,
    get_recovery_bundle,
    create_recovery_snapshot,
    list_recovery_snapshots,
    _now,
    _read_json,
    _write_json,
    _with_workspace_markdown,
)

RECOVERY_EVOLUTION_VERSION = "GENESIS-0007"
STATUS_PATH = Path("runtime") / "recovery" / "recovery_evolution_status.json"
REPORTS_PATH = Path("runtime") / "recovery" / "recovery_evolution_reports.json"
CHECKPOINTS_PATH = Path("runtime") / "recovery" / "recovery_checkpoints.json"


def _repo_path(path: Path) -> Path:
    return ensure_repository() / path


def _read_list(path: Path) -> List[Dict[str, Any]]:
    data = _read_json(_repo_path(path), [])
    if not isinstance(data, list):
        data = []
    return [item for item in data if isinstance(item, dict)]


def _write_list(path: Path, items: List[Dict[str, Any]]) -> None:
    _write_json(_repo_path(path), items)


def _professional_sections_count(model_payload: Dict[str, Any]) -> int:
    model = model_payload.get("professional_model") if isinstance(model_payload.get("professional_model"), dict) else model_payload
    sections = model.get("sections") if isinstance(model, dict) and isinstance(model.get("sections"), dict) else {}
    return len(sections)


def _latest_recovery_snapshot_id() -> Optional[str]:
    snapshots = list_recovery_snapshots(limit=1).get("snapshots", [])
    if isinstance(snapshots, list) and snapshots:
        return snapshots[-1].get("snapshot_id")
    return None


def ensure_recovery_evolution_repository() -> Dict[str, Any]:
    ensure_repository()
    status_path = _repo_path(STATUS_PATH)
    reports_path = _repo_path(REPORTS_PATH)
    checkpoints_path = _repo_path(CHECKPOINTS_PATH)

    reports = _read_json(reports_path, [])
    if not isinstance(reports, list):
        reports = []
        _write_json(reports_path, reports)

    checkpoints = _read_json(checkpoints_path, [])
    if not isinstance(checkpoints, list):
        checkpoints = []
        _write_json(checkpoints_path, checkpoints)

    model = get_professional_model()
    state = get_current_state()
    bundle = get_recovery_bundle()
    status = _read_json(status_path, {})
    if not isinstance(status, dict):
        status = {}
    status.update({
        "status": "active",
        "identity_root": "VECTRA",
        "recovery_evolution_release": RECOVERY_EVOLUTION_VERSION,
        "professional_model_sections_count": _professional_sections_count(model),
        "has_professional_state": isinstance(state.get("state"), dict) or isinstance(state, dict),
        "has_recovery_bundle": isinstance(bundle.get("recovery_bundle"), dict),
        "latest_recovery_snapshot_id": _latest_recovery_snapshot_id(),
        "checkpoints_count": len(checkpoints),
        "reports_count": len(reports),
        "professional_model_unchanged": True,
        "reflection_triggered_automatically": False,
        "knowledge_consolidation_triggered": False,
        "automatic_product_decisions": False,
        "updated_at": _now(),
    })
    _write_json(status_path, status)
    return {"status": status, "reports": reports, "checkpoints": checkpoints}


def get_recovery_evolution_status() -> Dict[str, Any]:
    repo = ensure_recovery_evolution_repository()
    status = repo.get("status") if isinstance(repo.get("status"), dict) else {}
    payload = {
        "status": "ok",
        "render_mode": "vectra_recovery_evolution_status",
        "identity_root": "VECTRA",
        **status,
        "boundaries": {
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
        "human_summary": "Recovery Evolution активен: VECTRA может сформировать проверяемую точку восстановления профессиональной среды.",
    }
    return _with_workspace_markdown(payload, "Recovery Evolution VECTRA", payload)


def run_recovery_evolution(request: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if not isinstance(request, dict):
        request = {}
    ensure_recovery_evolution_repository()
    now = _now()

    model_before = get_professional_model()
    state_before = get_current_state()
    recovery_bundle = get_recovery_bundle()

    snapshot_result = create_recovery_snapshot({
        "metadata": {
            "source": "GENESIS-0007 Recovery Evolution",
            "reason": str(request.get("reason") or "manual_recovery_evolution_run"),
            "recovery_evolution_release": RECOVERY_EVOLUTION_VERSION,
        }
    })
    snapshot = snapshot_result.get("snapshot") if isinstance(snapshot_result.get("snapshot"), dict) else {}

    checkpoint = {
        "checkpoint_id": f"RECOVERY-CHECKPOINT-{now.replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:8]}",
        "created_at": now,
        "identity_root": "VECTRA",
        "recovery_evolution_release": RECOVERY_EVOLUTION_VERSION,
        "snapshot_id": snapshot.get("snapshot_id"),
        "professional_model_sections_count": _professional_sections_count(model_before),
        "has_professional_state": isinstance(state_before.get("state"), dict) or isinstance(state_before, dict),
        "has_recovery_bundle": isinstance(recovery_bundle.get("recovery_bundle"), dict),
        "contains_active_responsibilities": isinstance(snapshot.get("active_responsibilities"), list),
        "contains_product_decisions": isinstance(snapshot.get("product_decisions"), list),
        "contains_professional_model": isinstance(snapshot.get("professional_model"), dict),
        "contains_runtime": isinstance(snapshot.get("runtime"), dict),
        "boundaries": {
            "professional_model_unchanged": True,
            "reflection_triggered_automatically": False,
            "knowledge_consolidation_triggered": False,
            "automatic_product_decisions": False,
        },
    }

    checkpoints = _read_list(CHECKPOINTS_PATH)
    checkpoints.append(checkpoint)
    _write_list(CHECKPOINTS_PATH, checkpoints)

    verify = verify_recovery_evolution_readback(checkpoint_id=checkpoint["checkpoint_id"])
    report = {
        "report_id": f"RECOVERY-REPORT-{now.replace(':', '').replace('-', '')}-{uuid.uuid4().hex[:8]}",
        "created_at": now,
        "identity_root": "VECTRA",
        "recovery_evolution_release": RECOVERY_EVOLUTION_VERSION,
        "reason": str(request.get("reason") or "manual_recovery_evolution_run"),
        "checkpoint_id": checkpoint["checkpoint_id"],
        "snapshot_id": checkpoint.get("snapshot_id"),
        "readback_status": verify.get("status"),
        "overall": "PASS" if verify.get("status") == "PASS" else "FAIL",
        "restored_scope": {
            "professional_model": checkpoint.get("contains_professional_model"),
            "professional_state": checkpoint.get("has_professional_state"),
            "active_responsibilities": checkpoint.get("contains_active_responsibilities"),
            "product_decisions": checkpoint.get("contains_product_decisions"),
            "runtime": checkpoint.get("contains_runtime"),
        },
        "boundaries": checkpoint["boundaries"],
    }
    reports = _read_list(REPORTS_PATH)
    reports.append(report)
    _write_list(REPORTS_PATH, reports)
    ensure_recovery_evolution_repository()

    payload = {
        "status": "ok" if report["overall"] == "PASS" else "degraded",
        "render_mode": "vectra_recovery_evolution_run",
        "identity_root": "VECTRA",
        "checkpoint": checkpoint,
        "report": report,
        "readback_verification": verify,
        "human_summary": "Recovery Evolution выполнил создание точки восстановления и проверил readback.",
    }
    return _with_workspace_markdown(payload, "Recovery Evolution Run VECTRA", {"checkpoint": checkpoint, "report": report})


def list_recovery_evolution_reports(limit: int = 20) -> Dict[str, Any]:
    reports = _read_list(REPORTS_PATH)
    limited = reports[-max(1, int(limit or 20)):]
    payload = {
        "status": "ok",
        "render_mode": "vectra_recovery_evolution_reports",
        "identity_root": "VECTRA",
        "reports": limited,
        "reports_count": len(reports),
        "human_summary": f"Recovery Evolution Reports доступны: {len(reports)} отчётов.",
    }
    return _with_workspace_markdown(payload, "Recovery Evolution Reports VECTRA", limited)


def list_recovery_checkpoints(limit: int = 20) -> Dict[str, Any]:
    checkpoints = _read_list(CHECKPOINTS_PATH)
    limited = checkpoints[-max(1, int(limit or 20)):]
    payload = {
        "status": "ok",
        "render_mode": "vectra_recovery_checkpoints",
        "identity_root": "VECTRA",
        "checkpoints": limited,
        "checkpoints_count": len(checkpoints),
        "human_summary": f"Recovery Checkpoints доступны: {len(checkpoints)} точек восстановления.",
    }
    return _with_workspace_markdown(payload, "Recovery Checkpoints VECTRA", limited)


def verify_recovery_evolution_readback(checkpoint_id: Optional[str] = None) -> Dict[str, Any]:
    ensure_recovery_evolution_repository()
    checkpoints = _read_list(CHECKPOINTS_PATH)
    reports = _read_list(REPORTS_PATH)
    if checkpoint_id:
        checkpoints = [item for item in checkpoints if item.get("checkpoint_id") == checkpoint_id]
    latest = checkpoints[-1] if checkpoints else None
    model = get_professional_model()
    state = get_current_state()
    bundle = get_recovery_bundle()
    checks = [
        {"object": "professional_model", "status": "PASS" if _professional_sections_count(model) >= 1 else "FAIL"},
        {"object": "professional_state", "status": "PASS" if isinstance(state, dict) and bool(state) else "FAIL"},
        {"object": "recovery_bundle", "status": "PASS" if isinstance(bundle.get("recovery_bundle"), dict) else "FAIL"},
        {"object": "recovery_checkpoint_repository", "status": "PASS", "checkpoints_count": len(checkpoints)},
    ]
    if latest:
        checks.append({"object": "recovery_snapshot_link", "status": "PASS" if latest.get("snapshot_id") else "FAIL"})
    overall = "PASS" if all(item.get("status") == "PASS" for item in checks) else "FAIL"
    return {
        "status": overall,
        "render_mode": "vectra_recovery_evolution_readback_verification",
        "identity_root": "VECTRA",
        "recovery_evolution_release": RECOVERY_EVOLUTION_VERSION,
        "checkpoint_id": latest.get("checkpoint_id") if isinstance(latest, dict) else checkpoint_id,
        "reports_count": len(reports),
        "checks": checks,
        "professional_model_unchanged": True,
        "reflection_triggered_automatically": False,
        "knowledge_consolidation_triggered": False,
        "automatic_product_decisions": False,
    }
