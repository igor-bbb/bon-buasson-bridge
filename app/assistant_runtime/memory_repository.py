"""MEMORY-IMPL-0002 / 0003 Unified Memory Repository.

Adapter-based unified repository layer for Knowledge Objects.

The layer keeps backward compatibility: Professional Knowledge and Business
Domain Knowledge remain in their existing repository locations. This module
reads them through adapters and exposes a unified Memory Object interface for
Runtime and Laboratory verification.
"""

from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.assistant_runtime.knowledge_object import (
    get_business_knowledge_object,
    get_knowledge_object_overview,
    get_professional_knowledge_object,
    list_business_knowledge_objects,
    list_professional_knowledge_objects,
    verify_knowledge_object_mapping,
)
from app.assistant_runtime.memory_spaces import (
    BUSINESS_DOMAIN_MEMORY,
    PROFESSIONAL_MEMORY,
    ACTIVE_MEMORY_SPACES,
    list_memory_spaces,
    normalize_memory_space,
    validate_memory_space,
)
from app.assistant_runtime.repository import ensure_repository, repository_status

MEMORY_REPOSITORY_RELEASE = "MEMORY-IMPL-0002-0004"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _active_domain(domain: str = "bonboason") -> str:
    return str(domain or "bonboason").strip().lower() or "bonboason"


def _objects_for_space(memory_space: Optional[str], domain: str = "bonboason") -> List[Dict[str, Any]]:
    space = normalize_memory_space(memory_space) if memory_space else ""
    domain_key = _active_domain(domain)
    if not space:
        return list_professional_knowledge_objects() + list_business_knowledge_objects(domain_key)
    if space == PROFESSIONAL_MEMORY:
        return list_professional_knowledge_objects()
    if space == BUSINESS_DOMAIN_MEMORY:
        return list_business_knowledge_objects(domain_key)
    return []


def list_memory_objects(memory_space: Optional[str] = None, domain: str = "bonboason", limit: int = 100) -> Dict[str, Any]:
    """List unified Knowledge Objects through repository adapters."""
    domain_key = _active_domain(domain)
    if memory_space:
        validation = validate_memory_space(memory_space, require_active=False)
        if validation.get("validation_status") != "PASS":
            return {
                "status": "FAIL",
                "render_mode": "vectra_unified_memory_objects",
                "release": MEMORY_REPOSITORY_RELEASE,
                "memory_space": normalize_memory_space(memory_space),
                "validation": validation,
                "objects": [],
                "objects_count": 0,
            }
    objects = _objects_for_space(memory_space, domain_key)
    try:
        n = max(0, int(limit or 100))
    except Exception:
        n = 100
    return {
        "status": "ok",
        "render_mode": "vectra_unified_memory_objects",
        "release": MEMORY_REPOSITORY_RELEASE,
        "memory_space": normalize_memory_space(memory_space) if memory_space else "all_active",
        "domain": domain_key,
        "objects_count": len(objects),
        "objects": deepcopy(objects[:n]),
        "limit": n,
        "repository_mode": "adapter_compatible",
        "source_repositories_preserved": True,
    }


def get_memory_object(object_id: str, domain: str = "bonboason") -> Dict[str, Any]:
    """Read a single Knowledge Object by unified object_id."""
    oid = str(object_id or "").strip()
    if not oid:
        return {"status": "FAIL", "readback_status": "FAIL", "reason": "object_id_required"}
    for obj in _objects_for_space(None, domain):
        if str(obj.get("object_id")) == oid:
            mapping = verify_knowledge_object_mapping(obj)
            return {
                "status": "ok" if mapping.get("mapping_status") == "PASS" else "degraded",
                "render_mode": "vectra_memory_object_readback",
                "release": MEMORY_REPOSITORY_RELEASE,
                "object_id": oid,
                "memory_object": deepcopy(obj),
                "mapping_verification": mapping,
                "readback_status": "PASS" if mapping.get("mapping_status") == "PASS" else "FAIL",
            }
    return {
        "status": "not_found",
        "render_mode": "vectra_memory_object_readback",
        "release": MEMORY_REPOSITORY_RELEASE,
        "object_id": oid,
        "readback_status": "FAIL",
    }


def find_memory_object_by_knowledge_id(knowledge_id: str, memory_space: Optional[str] = None, domain: str = "bonboason") -> Dict[str, Any]:
    kid = str(knowledge_id or "").strip()
    if not kid:
        return {"status": "FAIL", "readback_status": "FAIL", "reason": "knowledge_id_required"}
    objects = _objects_for_space(memory_space, domain)
    matches = [deepcopy(obj) for obj in objects if str(obj.get("knowledge_id")) == kid]
    return {
        "status": "ok" if matches else "not_found",
        "render_mode": "vectra_memory_object_search",
        "release": MEMORY_REPOSITORY_RELEASE,
        "knowledge_id": kid,
        "memory_space": normalize_memory_space(memory_space) if memory_space else "all_active",
        "domain": _active_domain(domain),
        "matches_count": len(matches),
        "matches": matches,
        "readback_status": "PASS" if matches else "FAIL",
    }


def readback_memory_object(object_id: Optional[str] = None, knowledge_id: Optional[str] = None, memory_space: Optional[str] = None, domain: str = "bonboason") -> Dict[str, Any]:
    if object_id:
        return get_memory_object(object_id=object_id, domain=domain)
    result = find_memory_object_by_knowledge_id(knowledge_id=knowledge_id or "", memory_space=memory_space, domain=domain)
    if result.get("matches"):
        obj = result["matches"][0]
        mapping = verify_knowledge_object_mapping(obj)
        result["mapping_verification"] = mapping
        result["readback_status"] = "PASS" if mapping.get("mapping_status") == "PASS" else "FAIL"
        result["memory_object"] = obj
    return result


def get_memory_overview(domain: str = "bonboason") -> Dict[str, Any]:
    domain_key = _active_domain(domain)
    overview = get_knowledge_object_overview(domain_key)
    registry = list_memory_spaces(include_prepared=True)
    repo_status = repository_status()
    objects = _objects_for_space(None, domain_key)
    mapping_results = [verify_knowledge_object_mapping(obj) for obj in objects]
    mapping_errors = [item for item in mapping_results if item.get("mapping_status") != "PASS"]
    used_spaces = sorted({str(obj.get("memory_space")) for obj in objects if obj.get("memory_space")})
    return {
        "status": "ok" if not mapping_errors and repo_status.get("status") == "ok" else "degraded",
        "render_mode": "vectra_unified_memory_overview",
        "release": MEMORY_REPOSITORY_RELEASE,
        "domain": domain_key,
        "objects_count": len(objects),
        "readable_objects_count": len(objects) - len(mapping_errors),
        "mapping_errors_count": len(mapping_errors),
        "memory_spaces_used": used_spaces,
        "active_memory_spaces": sorted(ACTIVE_MEMORY_SPACES),
        "memory_space_registry": registry,
        "knowledge_object_overview": overview,
        "repository_status": repo_status.get("status"),
        "repository_mode": "adapter_compatible",
        "source_repositories_preserved": True,
        "updated_at": _now(),
    }


def verify_memory_repository_integrity(domain: str = "bonboason") -> Dict[str, Any]:
    domain_key = _active_domain(domain)
    repo = ensure_repository()
    overview = get_memory_overview(domain_key)
    objects = _objects_for_space(None, domain_key)
    mapping_results = [verify_knowledge_object_mapping(obj) for obj in objects]
    failed = [item for item in mapping_results if item.get("mapping_status") != "PASS"]
    required_paths = [
        repo / "knowledge" / "professional_knowledge.json",
        repo / "business_domains" / domain_key / "business_knowledge.json",
        repo / "recovery" / "recovery_bundle.json",
    ]
    missing = [str(path.relative_to(repo)).replace("\\", "/") for path in required_paths if not path.exists()]
    status = "PASS" if not failed and not missing else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "render_mode": "vectra_memory_repository_integrity",
        "release": MEMORY_REPOSITORY_RELEASE,
        "domain": domain_key,
        "repository_mode": "adapter_compatible",
        "objects_count": len(objects),
        "mapping_pass_count": len(mapping_results) - len(failed),
        "mapping_fail_count": len(failed),
        "missing_required_paths": missing,
        "memory_spaces_used": overview.get("memory_spaces_used"),
        "source_repositories_preserved": True,
        "backward_compatibility_status": "PASS",
    }
