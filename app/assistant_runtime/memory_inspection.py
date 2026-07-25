"""MEMORY-IMPL-0006 Memory Inspection Runtime.

Read-only Laboratory inspection layer for VECTRA long-term memory. This module
uses the unified Memory Repository and never mutates source repositories.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.assistant_runtime.memory_repository import (
    get_memory_object,
    get_memory_overview,
    list_memory_objects,
    readback_memory_object,
    verify_memory_repository_integrity,
)
from app.assistant_runtime.memory_spaces import (
    ACTIVE_MEMORY_SPACES,
    list_memory_spaces,
    normalize_memory_space,
    validate_memory_space,
)
from app.assistant_runtime.knowledge_object import verify_knowledge_object_mapping
from app.assistant_runtime.repository import repository_status

MEMORY_INSPECTION_RELEASE = "MEMORY-IMPL-0006"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def inspect_memory_object(object_id: str, domain: str = "bon_buasson") -> Dict[str, Any]:
    result = get_memory_object(object_id=object_id, domain=domain)
    obj = result.get("memory_object") if isinstance(result, dict) else None
    mapping = verify_knowledge_object_mapping(obj) if isinstance(obj, dict) else {"mapping_status": "FAIL", "reason": "object_not_found"}
    return {
        "status": "ok" if result.get("status") == "ok" and mapping.get("mapping_status") == "PASS" else result.get("status", "FAIL"),
        "render_mode": "vectra_memory_object_inspection",
        "release": MEMORY_INSPECTION_RELEASE,
        "object_id": object_id,
        "inspection_status": "PASS" if mapping.get("mapping_status") == "PASS" else "FAIL",
        "memory_object": deepcopy(obj),
        "mapping_verification": mapping,
        "readback": result,
        "inspected_at": _now(),
    }


def inspect_memory_space(memory_space: str, domain: str = "bon_buasson", limit: int = 100) -> Dict[str, Any]:
    normalized = normalize_memory_space(memory_space)
    validation = validate_memory_space(normalized, require_active=False)
    listing = list_memory_objects(memory_space=normalized, domain=domain, limit=limit) if validation.get("validation_status") == "PASS" else {"objects": [], "objects_count": 0}
    objects = listing.get("objects") if isinstance(listing.get("objects"), list) else []
    mapping = [verify_knowledge_object_mapping(obj) for obj in objects if isinstance(obj, dict)]
    failures = [m for m in mapping if m.get("mapping_status") != "PASS"]
    return {
        "status": "ok" if validation.get("validation_status") == "PASS" and not failures else "degraded",
        "render_mode": "vectra_memory_space_inspection",
        "release": MEMORY_INSPECTION_RELEASE,
        "memory_space": normalized,
        "domain": domain,
        "validation": validation,
        "objects_count": listing.get("objects_count", len(objects)),
        "inspected_objects_count": len(objects),
        "mapping_pass_count": len(mapping) - len(failures),
        "mapping_fail_count": len(failures),
        "objects": deepcopy(objects),
        "inspected_at": _now(),
    }


def get_memory_statistics(domain: str = "bon_buasson") -> Dict[str, Any]:
    listing = list_memory_objects(domain=domain, limit=10000)
    objects = listing.get("objects") if isinstance(listing.get("objects"), list) else []
    by_space: Dict[str, int] = {}
    by_type: Dict[str, int] = {}
    by_status: Dict[str, int] = {}
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        by_space[str(obj.get("memory_space") or "unknown")] = by_space.get(str(obj.get("memory_space") or "unknown"), 0) + 1
        by_type[str(obj.get("knowledge_type") or "unknown")] = by_type.get(str(obj.get("knowledge_type") or "unknown"), 0) + 1
        by_status[str(obj.get("verification_status") or "unknown")] = by_status.get(str(obj.get("verification_status") or "unknown"), 0) + 1
    return {
        "status": "ok",
        "render_mode": "vectra_memory_statistics",
        "release": MEMORY_INSPECTION_RELEASE,
        "domain": domain,
        "objects_count": len(objects),
        "by_memory_space": by_space,
        "by_knowledge_type": by_type,
        "by_verification_status": by_status,
        "active_memory_spaces": sorted(ACTIVE_MEMORY_SPACES),
        "generated_at": _now(),
    }


def get_memory_integrity_report(domain: str = "bon_buasson") -> Dict[str, Any]:
    overview = get_memory_overview(domain=domain)
    repository = verify_memory_repository_integrity(domain=domain)
    repo_status = repository_status()
    statistics = get_memory_statistics(domain=domain)
    registry = list_memory_spaces(include_prepared=True)
    status = "PASS" if repository.get("verification_status") == "PASS" and overview.get("mapping_errors_count", 1) == 0 else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "render_mode": "vectra_memory_integrity_report",
        "release": MEMORY_INSPECTION_RELEASE,
        "domain": domain,
        "overview": overview,
        "repository_integrity": repository,
        "repository_status": repo_status,
        "statistics": statistics,
        "memory_space_registry": registry,
        "source_repositories_preserved": True,
        "generated_at": _now(),
    }


def get_memory_readback_report(domain: str = "bon_buasson", limit: int = 100) -> Dict[str, Any]:
    listing = list_memory_objects(domain=domain, limit=limit)
    objects = listing.get("objects") if isinstance(listing.get("objects"), list) else []
    readbacks: List[Dict[str, Any]] = []
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        rb = readback_memory_object(object_id=obj.get("object_id"), domain=domain)
        readbacks.append({
            "object_id": obj.get("object_id"),
            "knowledge_id": obj.get("knowledge_id"),
            "memory_space": obj.get("memory_space"),
            "readback_status": rb.get("readback_status"),
            "mapping_status": (rb.get("mapping_verification") or {}).get("mapping_status"),
        })
    failures = [r for r in readbacks if r.get("readback_status") != "PASS" or r.get("mapping_status") != "PASS"]
    status = "PASS" if not failures else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "render_mode": "vectra_memory_readback_report",
        "release": MEMORY_INSPECTION_RELEASE,
        "domain": domain,
        "objects_checked": len(readbacks),
        "readback_pass_count": len(readbacks) - len(failures),
        "readback_fail_count": len(failures),
        "readbacks": readbacks,
        "generated_at": _now(),
    }


def run_memory_inspection(operation_type: str = "overview", payload: Optional[Dict[str, Any]] = None, domain: str = "bon_buasson") -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    op = str(operation_type or "overview").strip().lower()
    if op in {"overview", "summary"}:
        return get_memory_overview(domain=domain)
    if op in {"statistics", "stats"}:
        return get_memory_statistics(domain=domain)
    if op in {"integrity", "integrity_report"}:
        return get_memory_integrity_report(domain=domain)
    if op in {"readback", "readback_report"}:
        return get_memory_readback_report(domain=domain, limit=int(payload.get("limit") or 100))
    if op in {"inspect_object", "object"}:
        return inspect_memory_object(object_id=str(payload.get("object_id") or ""), domain=domain)
    if op in {"inspect_space", "space"}:
        return inspect_memory_space(memory_space=str(payload.get("memory_space") or ""), domain=domain, limit=int(payload.get("limit") or 100))
    return {
        "status": "FAIL",
        "render_mode": "vectra_memory_inspection",
        "release": MEMORY_INSPECTION_RELEASE,
        "reason": "unsupported_inspection_operation",
        "operation_type": op,
        "supported_operations": ["overview", "statistics", "integrity_report", "readback_report", "inspect_object", "inspect_space"],
    }
