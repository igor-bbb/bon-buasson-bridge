"""MEMORY-IMPL-0014 Recovery Optimization Runtime.

Builds a compact recovery context for new VECTRA working sessions. The module
keeps full repositories untouched and returns summarized, bounded memory state
so Runtime responses stay within GPT Action limits.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from app.assistant_runtime.memory_health import verify_memory_health
from app.assistant_runtime.memory_repository import get_memory_overview, list_memory_objects
from app.assistant_runtime.memory_spaces import ACTIVE_MEMORY_SPACES
from app.assistant_runtime.repository import get_recovery_bundle, list_recovery_snapshots

RECOVERY_OPTIMIZATION_RELEASE = "MEMORY-IMPL-0014"
DEFAULT_MAX_OBJECTS_PER_SPACE = 5


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compact_object(obj: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "object_id": obj.get("object_id"),
        "knowledge_id": obj.get("knowledge_id"),
        "memory_space": obj.get("memory_space"),
        "knowledge_type": obj.get("knowledge_type"),
        "domain": obj.get("domain"),
        "title": obj.get("title"),
        "version": obj.get("version"),
        "verification_status": obj.get("verification_status"),
        "updated_at": obj.get("updated_at"),
    }


def build_compact_recovery_context(domain: str = "bonboason", max_objects_per_space: int = DEFAULT_MAX_OBJECTS_PER_SPACE) -> Dict[str, Any]:
    limit = max(1, min(int(max_objects_per_space or DEFAULT_MAX_OBJECTS_PER_SPACE), 20))
    overview = get_memory_overview(domain=domain)
    health = verify_memory_health(domain=domain)
    recovery = get_recovery_bundle()
    snapshots = list_recovery_snapshots(limit=1)
    spaces: Dict[str, List[Dict[str, Any]]] = {}
    total_included = 0
    for space in sorted(ACTIVE_MEMORY_SPACES):
        result = list_memory_objects(memory_space=space, domain=domain, limit=limit)
        items = result.get("objects") if isinstance(result, dict) else []
        compact = [_compact_object(item) for item in items if isinstance(item, dict)]
        spaces[space] = compact
        total_included += len(compact)
    latest_snapshot = None
    snapshot_items = snapshots.get("snapshots") if isinstance(snapshots, dict) else None
    if isinstance(snapshot_items, list) and snapshot_items:
        latest_snapshot = snapshot_items[-1]
    return {
        "status": "PASS" if health.get("verification_status") == "PASS" else "FAIL",
        "verification_status": "PASS" if health.get("verification_status") == "PASS" else "FAIL",
        "render_mode": "vectra_compact_recovery_context",
        "release": RECOVERY_OPTIMIZATION_RELEASE,
        "domain": domain,
        "compact_recovery_context": {
            "identity_root": "VECTRA",
            "domain": domain,
            "memory_health_status": health.get("health_status"),
            "objects_count": overview.get("objects_count"),
            "readable_objects_count": overview.get("readable_objects_count"),
            "mapping_errors_count": overview.get("mapping_errors_count"),
            "memory_spaces_used": overview.get("memory_spaces_used"),
            "objects_by_space": spaces,
            "latest_recovery_snapshot_id": latest_snapshot.get("snapshot_id") if isinstance(latest_snapshot, dict) else None,
            "recovery_bundle_status": recovery.get("status"),
        },
        "response_compaction": {
            "max_objects_per_space": limit,
            "included_objects_count": total_included,
            "full_repositories_preserved": True,
            "large_response_protection": True,
        },
        "generated_at": _now(),
    }


def verify_recovery_optimization(domain: str = "bonboason") -> Dict[str, Any]:
    context = build_compact_recovery_context(domain=domain)
    compact = context.get("compact_recovery_context", {})
    blocking = []
    if context.get("verification_status") != "PASS":
        blocking.append("memory_health_not_pass")
    if not isinstance(compact.get("objects_by_space"), dict):
        blocking.append("objects_by_space_missing")
    if compact.get("mapping_errors_count") not in (0, "0", None):
        blocking.append("mapping_errors_present")
    status = "PASS" if not blocking else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "recovery_optimization_status": status,
        "render_mode": "vectra_recovery_optimization_verification",
        "release": RECOVERY_OPTIMIZATION_RELEASE,
        "domain": domain,
        "blocking_issues": blocking,
        "compact_context_available": isinstance(compact, dict) and bool(compact),
        "large_response_protection": context.get("response_compaction", {}).get("large_response_protection") is True,
        "included_objects_count": context.get("response_compaction", {}).get("included_objects_count"),
        "context": context,
    }
