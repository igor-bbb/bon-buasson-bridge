"""MEMORY-IMPL-0009 Memory Health and Diagnostics Runtime.

Provides a compact health status for VECTRA long-term memory before Product
Verification: memory object counts, mapping errors, readback status, required
repository paths and Recovery Snapshot availability.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import ensure_repository
from app.assistant_runtime.memory_repository import get_memory_overview, list_memory_objects, readback_memory_object, verify_memory_repository_integrity
from app.assistant_runtime.memory_inspection import get_memory_statistics, get_memory_readback_report
from app.assistant_runtime.memory_spaces import ACTIVE_MEMORY_SPACES, list_memory_spaces

MEMORY_HEALTH_RELEASE = "MEMORY-IMPL-0009/MEMORY-IMPL-0010-0012"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _required_path_status(domain: str = "bonboason") -> Dict[str, Any]:
    repo = ensure_repository()
    required = [
        "knowledge/professional_knowledge.json",
        "business_domains/{}/business_knowledge.json".format(str(domain or "bonboason").strip().lower() or "bonboason"),
        "knowledge/product_knowledge.json",
        "knowledge/general_knowledge.json",
        "decisions/product_decisions.json",
        "releases/release_history.json",
        "memory/revisions.json",
        "recovery/recovery_bundle.json",
    ]
    paths = []
    missing = []
    for rel in required:
        exists = (repo / rel).exists()
        paths.append({"path": rel, "exists": exists})
        if not exists:
            missing.append(rel)
    return {"required_paths": paths, "missing_required_paths": missing, "required_path_status": "PASS" if not missing else "FAIL"}


def get_memory_health_status(domain: str = "bonboason") -> Dict[str, Any]:
    overview = get_memory_overview(domain=domain)
    integrity = verify_memory_repository_integrity(domain=domain)
    statistics = get_memory_statistics(domain=domain)
    readback = get_memory_readback_report(domain=domain, limit=500)
    paths = _required_path_status(domain)
    blocking = []
    if integrity.get("verification_status") != "PASS":
        blocking.append("repository_integrity_failed")
    if int(overview.get("mapping_errors_count") or 0) > 0:
        blocking.append("knowledge_object_mapping_errors")
    if (readback.get("readback_status") or readback.get("verification_status") or readback.get("status")) != "PASS":
        blocking.append("memory_readback_failed")
    if paths.get("required_path_status") != "PASS":
        blocking.append("required_memory_paths_missing")
    health = "PASS" if not blocking else "FAIL"
    return {
        "status": health,
        "health_status": health,
        "verification_status": health,
        "render_mode": "vectra_memory_health_status",
        "release": MEMORY_HEALTH_RELEASE,
        "domain": domain,
        "objects_count": overview.get("objects_count"),
        "readable_objects_count": overview.get("readable_objects_count"),
        "mapping_errors_count": overview.get("mapping_errors_count"),
        "active_memory_spaces": sorted(ACTIVE_MEMORY_SPACES),
        "memory_spaces_used": overview.get("memory_spaces_used"),
        "repository_integrity_status": integrity.get("verification_status"),
        "readback_status": readback.get("readback_status") or readback.get("verification_status") or readback.get("status"),
        "required_path_status": paths.get("required_path_status"),
        "missing_required_paths": paths.get("missing_required_paths"),
        "blocking_issues": blocking,
        "updated_at": _now(),
    }


def get_memory_diagnostics_report(domain: str = "bonboason") -> Dict[str, Any]:
    health = get_memory_health_status(domain=domain)
    overview = get_memory_overview(domain=domain)
    statistics = get_memory_statistics(domain=domain)
    readback = get_memory_readback_report(domain=domain, limit=500)
    integrity = verify_memory_repository_integrity(domain=domain)
    spaces = list_memory_spaces(include_prepared=True)
    paths = _required_path_status(domain)
    return {
        "status": health.get("health_status"),
        "health_status": health.get("health_status"),
        "verification_status": health.get("health_status"),
        "render_mode": "vectra_memory_diagnostics_report",
        "release": MEMORY_HEALTH_RELEASE,
        "domain": domain,
        "health": health,
        "overview": overview,
        "statistics": statistics,
        "readback_report": readback,
        "integrity_report": integrity,
        "memory_space_registry": spaces,
        "required_paths": paths,
        "summary_for_laboratory": {
            "can_product_verification_continue": health.get("health_status") == "PASS",
            "blocking_issues": health.get("blocking_issues", []),
            "memory_spaces_used": overview.get("memory_spaces_used"),
            "objects_count": overview.get("objects_count"),
        },
        "generated_at": _now(),
    }


def verify_memory_health(domain: str = "bonboason") -> Dict[str, Any]:
    report = get_memory_diagnostics_report(domain=domain)
    status = "PASS" if report.get("health_status") == "PASS" else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "readback_status": report.get("readback_report", {}).get("readback_status"),
        "render_mode": "vectra_memory_health_verification",
        "release": MEMORY_HEALTH_RELEASE,
        "domain": domain,
        "health_status": report.get("health_status"),
        "blocking_issues": report.get("summary_for_laboratory", {}).get("blocking_issues", []),
        "objects_count": report.get("overview", {}).get("objects_count"),
        "mapping_errors_count": report.get("overview", {}).get("mapping_errors_count"),
        "repository_integrity_status": report.get("integrity_report", {}).get("verification_status"),
        "recovery_verification_status": "PASS" if "recovery/recovery_bundle.json" not in report.get("required_paths", {}).get("missing_required_paths", []) else "FAIL",
    }
