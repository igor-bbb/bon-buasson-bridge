"""RECOVERY-SNAPSHOT-SYNC-001 — Recovery Snapshot synchronization after capitalization.

This module is intentionally small and diagnostic-friendly. It closes the
capitalization lifecycle by rebuilding a full Recovery Snapshot after a
successful Repository write and Readback Verification, then provides a verifier
that compares Repository, Readback and the latest Recovery Snapshot.
"""

from __future__ import annotations

import json
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import (
    ensure_repository,
    create_recovery_snapshot,
    _read_json,
    _write_json,
    _with_workspace_markdown,
    _business_knowledge_for_recovery_snapshot,
)

RECOVERY_SYNC_RELEASE = "RECOVERY-SNAPSHOT-BUSINESS-KNOWLEDGE-HOTFIX-002"
CANONICAL_DOMAIN_ID = "bon_buasson"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _read_list(path: Path) -> List[Dict[str, Any]]:
    value = _read_json(path, [])
    return [dict(item) for item in value if isinstance(item, dict)] if isinstance(value, list) else []


def _checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _latest_capitalization_report(base: Path) -> Optional[Dict[str, Any]]:
    reports = _read_list(base / "runtime" / "knowledge_capitalization" / "reports.json")
    successful = [r for r in reports if r.get("final_status") in {"CAPITALIZED", "PASS"}]
    if not successful:
        return None
    return successful[-1]


def _repository_counts(base: Path, domain_id: str = CANONICAL_DOMAIN_ID) -> Dict[str, Any]:
    professional = _read_list(base / "knowledge" / "professional_knowledge.json")
    product = _read_list(base / "knowledge" / "product_knowledge.json")
    business = _business_knowledge_for_recovery_snapshot(base, domain_id)
    domain_profile = _read_json(base / "runtime" / "business_domains" / domain_id / "domain_profile.json", {})
    product_decisions = _read_list(base / "decisions" / "product_decisions.json")
    kc_reports = _read_list(base / "runtime" / "knowledge_capitalization" / "reports.json")
    return {
        "professional_knowledge_count": len(professional),
        "product_knowledge_count": len(product),
        "business_knowledge_count": len(business),
        "product_decisions_count": len(product_decisions),
        "business_standards_count": len(domain_profile.get("business_standards", [])) if isinstance(domain_profile, dict) else 0,
        "business_decisions_count": len(domain_profile.get("business_decisions", [])) if isinstance(domain_profile, dict) else 0,
        "active_projects_count": len(domain_profile.get("active_projects", [])) if isinstance(domain_profile, dict) else 0,
        "capitalization_reports_count": len(kc_reports),
    }


def rebuild_and_persist_recovery_snapshot_after_capitalization(metadata: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Rebuild the canonical Recovery Snapshot after successful capitalization.

    This function does not write knowledge. It only snapshots the current
    Repository after write/readback have succeeded.
    """
    base = ensure_repository()
    metadata = dict(metadata or {})
    domain_id = str(metadata.get("domain_id") or CANONICAL_DOMAIN_ID)
    latest_report = _latest_capitalization_report(base)
    counts = _repository_counts(base, domain_id=domain_id)
    snapshot_metadata = {
        "reason": "automatic_recovery_snapshot_sync_after_capitalization",
        "release": RECOVERY_SYNC_RELEASE,
        "domain_id": domain_id,
        "capitalization_report_id": metadata.get("capitalization_report_id") or (latest_report or {}).get("report_id"),
        "package_id": metadata.get("package_id") or (latest_report or {}).get("package_id") or (latest_report or {}).get("knowledge_package_id"),
        "knowledge_objects_count": metadata.get("knowledge_objects_count") or (counts["professional_knowledge_count"] + counts["product_knowledge_count"] + counts["business_knowledge_count"]),
        "repository_counts": counts,
        "professional_model_version": metadata.get("professional_model_version") or "current",
        "synced_at": _now(),
    }
    recovery = create_recovery_snapshot({"metadata": snapshot_metadata})
    snapshot = recovery.get("snapshot") if isinstance(recovery, dict) else None
    sync_report = {
        "status": "PASS" if isinstance(snapshot, dict) and snapshot.get("snapshot_id") else "FAIL",
        "verification_status": "PASS" if isinstance(snapshot, dict) and snapshot.get("snapshot_id") else "FAIL",
        "release": RECOVERY_SYNC_RELEASE,
        "snapshot_id": snapshot.get("snapshot_id") if isinstance(snapshot, dict) else None,
        "snapshot_version": snapshot.get("snapshot_version") if isinstance(snapshot, dict) else None,
        "capitalization_report_id": snapshot_metadata.get("capitalization_report_id"),
        "package_id": snapshot_metadata.get("package_id"),
        "repository_counts": counts,
        "created_at": _now(),
    }
    path = base / "runtime" / "knowledge_capitalization" / "recovery_snapshot_sync.json"
    current = _read_json(path, [])
    if not isinstance(current, list):
        current = []
    current.append(sync_report)
    _write_json(path, current)
    return {"status": sync_report["status"], "render_mode": "vectra_recovery_snapshot_sync", "sync_report": sync_report, "recovery_snapshot": recovery}


def verify_recovery_snapshot_sync(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    domain_id = str(payload.get("domain") or payload.get("domain_id") or CANONICAL_DOMAIN_ID)
    from app.assistant_runtime.repository_readback_consistency import verify_repository_readback_consistency
    consistency = verify_repository_readback_consistency({"domain": domain_id})
    status = "PASS" if consistency.get("verification_status") == "PASS" and "RECOVERY_MISMATCH" not in consistency.get("failure_reasons", []) else "FAIL"
    result = {
        "status": "ok" if status == "PASS" else "failed",
        "verification_status": status,
        "release": RECOVERY_SYNC_RELEASE,
        "render_mode": "vectra_recovery_snapshot_sync_verification",
        "domain_id": domain_id,
        "repository_readback_consistency": consistency,
        "recovery_snapshot_sync_status": status,
        "capitalization_pipeline_closed": status == "PASS",
    }
    return _with_workspace_markdown(result, "Recovery Snapshot Sync Verification VECTRA", result)
