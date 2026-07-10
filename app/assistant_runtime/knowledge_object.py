"""MEMORY-IMPL-0001 Knowledge Object Runtime Foundation.

Internal unified memory object layer for VECTRA long-term memory.

This module is intentionally internal and read-only for existing records. It maps
current Professional Knowledge and Business Domain Knowledge repositories into a
single Knowledge Object shape without changing public API contracts or repository
layout.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.assistant_runtime.repository import (
    ensure_repository,
    get_business_domain_profile,
    _read_json,
    _write_json,
)

KNOWLEDGE_OBJECT_RELEASE = "MEMORY-IMPL-0001"

PROFESSIONAL_MEMORY = "professional_memory"
BUSINESS_DOMAIN_MEMORY = "business_domain_memory"
PRODUCT_MEMORY = "product_memory"
GENERAL_MEMORY = "general_memory"
RELEASE_HISTORY_MEMORY = "release_history_memory"
PRODUCT_DECISIONS_MEMORY = "product_decisions_memory"
SUPPORTED_MEMORY_SPACES = {PROFESSIONAL_MEMORY, BUSINESS_DOMAIN_MEMORY, PRODUCT_MEMORY, GENERAL_MEMORY, PRODUCT_DECISIONS_MEMORY, RELEASE_HISTORY_MEMORY}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _slug(value: str, fallback: str = "bon_buasson") -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9а-яіїєґ_-]+", "-", raw, flags=re.IGNORECASE).strip("-")
    return raw[:90] or fallback


def _read_list(path: Path) -> List[Dict[str, Any]]:
    value = _read_json(path, [])
    return value if isinstance(value, list) else []


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _repository_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ensure_repository().resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def professional_knowledge_path() -> Path:
    return ensure_repository() / "knowledge" / "professional_knowledge.json"


def business_knowledge_path(domain: str = "bon_buasson") -> Path:
    domain_key = _slug(domain or "bon_buasson", "bon_buasson")
    path = ensure_repository() / "business_domains" / domain_key / "business_knowledge.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write_json(path, [])
    return path


def _business_profile_path(domain: str = "bon_buasson") -> Path:
    return ensure_repository() / "runtime" / "business_domains" / _slug(domain or "bon_buasson", "bon_buasson") / "domain_profile.json"


def _first_present(record: Dict[str, Any], keys: Iterable[str], default: Any = None) -> Any:
    for key in keys:
        value = record.get(key)
        if value not in (None, ""):
            return value
    return default


def _object_id(memory_space: str, knowledge_id: str, domain: Optional[str] = None) -> str:
    domain_part = _slug(domain or "global", "global") if memory_space == BUSINESS_DOMAIN_MEMORY else "global"
    return f"KO-{memory_space}-{domain_part}-{knowledge_id}".replace(" ", "-")


def _verification_status_from_record(record: Dict[str, Any]) -> str:
    status = str(record.get("verification_status") or record.get("readback_status") or "").upper()
    if status in {"PASS", "READBACK_PASS"}:
        return "PASS"
    lifecycle = str(record.get("status") or "").upper()
    if lifecycle in {"CAPITALIZED", "READBACK_PASS", "WRITTEN"} and record.get("knowledge_id"):
        return "PASS"
    return "UNVERIFIED" if record.get("knowledge_id") else "FAIL"


def _evidence(record: Dict[str, Any]) -> List[Any]:
    evidence = record.get("evidence")
    if evidence is None:
        return []
    if isinstance(evidence, list):
        return deepcopy(evidence)
    return [deepcopy(evidence)]


def _normalize_version(record: Dict[str, Any]) -> int:
    try:
        return int(record.get("version") or record.get("revision") or 1)
    except Exception:
        return 1


def professional_record_to_knowledge_object(record: Dict[str, Any]) -> Dict[str, Any]:
    """Map an existing Professional Knowledge record to internal Knowledge Object."""
    record = dict(record or {})
    knowledge_id = str(record.get("knowledge_id") or record.get("id") or "").strip()
    created = _first_present(record, ["created_at", "capitalized_at", "updated_at"], _now())
    updated = _first_present(record, ["updated_at", "capitalized_at", "created_at"], created)
    description = _first_present(record, ["description", "content", "body", "text"], "")
    source = _first_present(record, ["source", "source_context", "source_package_id"], "Runtime Repository")
    knowledge_object = {
        "object_id": _object_id(PROFESSIONAL_MEMORY, knowledge_id or _stable_checksum(record)[:12].upper()),
        "memory_space": PROFESSIONAL_MEMORY,
        "knowledge_type": str(record.get("knowledge_type") or record.get("type") or "professional").lower(),
        "domain": None,
        "title": _first_present(record, ["title", "name"], knowledge_id),
        "description": description,
        "version": _normalize_version(record),
        "lifecycle_status": str(record.get("lifecycle_status") or record.get("status") or "UNKNOWN"),
        "source": source,
        "evidence": _evidence(record),
        "created_at": created,
        "updated_at": updated,
        "verification_status": _verification_status_from_record(record),
        "knowledge_id": knowledge_id,
        "repository_path": record.get("repository_path") or "knowledge/professional_knowledge.json",
        "source_record_checksum": _stable_checksum(record),
    }
    knowledge_object["object_checksum"] = _stable_checksum({k: v for k, v in knowledge_object.items() if k != "object_checksum"})
    return knowledge_object


def business_record_to_knowledge_object(record: Dict[str, Any], domain: str = "bon_buasson") -> Dict[str, Any]:
    """Map an existing Business Domain Knowledge record to internal Knowledge Object."""
    record = dict(record or {})
    domain_key = _slug(str(record.get("domain") or domain or "bon_buasson"), "bon_buasson")
    knowledge_id = str(record.get("knowledge_id") or record.get("id") or "").strip()
    created = _first_present(record, ["created_at", "capitalized_at", "updated_at"], _now())
    updated = _first_present(record, ["updated_at", "capitalized_at", "created_at"], created)
    description = _first_present(record, ["description", "content", "body", "text"], "")
    source = _first_present(record, ["source", "source_context", "source_package_id"], "Runtime Repository")
    knowledge_object = {
        "object_id": _object_id(BUSINESS_DOMAIN_MEMORY, knowledge_id or _stable_checksum(record)[:12].upper(), domain_key),
        "memory_space": BUSINESS_DOMAIN_MEMORY,
        "knowledge_type": str(record.get("knowledge_type") or record.get("type") or "business").lower(),
        "domain": domain_key,
        "title": _first_present(record, ["title", "name"], knowledge_id),
        "description": description,
        "version": _normalize_version(record),
        "lifecycle_status": str(record.get("lifecycle_status") or record.get("status") or "UNKNOWN"),
        "source": source,
        "evidence": _evidence(record),
        "created_at": created,
        "updated_at": updated,
        "verification_status": _verification_status_from_record(record),
        "knowledge_id": knowledge_id,
        "repository_path": record.get("repository_path") or f"business_domains/{domain_key}/business_knowledge.json",
        "source_record_checksum": _stable_checksum(record),
    }
    knowledge_object["object_checksum"] = _stable_checksum({k: v for k, v in knowledge_object.items() if k != "object_checksum"})
    return knowledge_object


def _business_knowledge_items(domain: str = "bon_buasson") -> List[Dict[str, Any]]:
    domain_key = _slug(domain or "bon_buasson", "bon_buasson")
    repo_items = _read_list(business_knowledge_path(domain_key))
    profile_payload = get_business_domain_profile(domain_key)
    profile = profile_payload.get("domain_profile") if isinstance(profile_payload, dict) else None
    profile_items = profile.get("business_knowledge", []) if isinstance(profile, dict) else []
    merged: Dict[str, Dict[str, Any]] = {}
    for item in profile_items if isinstance(profile_items, list) else []:
        if isinstance(item, dict) and item.get("knowledge_id"):
            merged[str(item.get("knowledge_id"))] = dict(item)
    for item in repo_items if isinstance(repo_items, list) else []:
        if isinstance(item, dict) and item.get("knowledge_id"):
            merged[str(item.get("knowledge_id"))] = dict(item)
    return list(merged.values())


def list_professional_knowledge_objects() -> List[Dict[str, Any]]:
    return [professional_record_to_knowledge_object(item) for item in _read_list(professional_knowledge_path()) if isinstance(item, dict)]


def list_business_knowledge_objects(domain: str = "bon_buasson") -> List[Dict[str, Any]]:
    domain_key = _slug(domain or "bon_buasson", "bon_buasson")
    return [business_record_to_knowledge_object(item, domain_key) for item in _business_knowledge_items(domain_key) if isinstance(item, dict)]


def get_professional_knowledge_object(knowledge_id: str) -> Optional[Dict[str, Any]]:
    for item in _read_list(professional_knowledge_path()):
        if isinstance(item, dict) and str(item.get("knowledge_id")) == str(knowledge_id):
            return professional_record_to_knowledge_object(item)
    return None


def get_business_knowledge_object(domain: str, knowledge_id: str) -> Optional[Dict[str, Any]]:
    domain_key = _slug(domain or "bon_buasson", "bon_buasson")
    for item in _business_knowledge_items(domain_key):
        if isinstance(item, dict) and str(item.get("knowledge_id")) == str(knowledge_id):
            return business_record_to_knowledge_object(item, domain_key)
    return None


def verify_knowledge_object_mapping(knowledge_object: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    required = [
        "object_id", "memory_space", "knowledge_type", "domain", "title", "description",
        "version", "lifecycle_status", "source", "evidence", "created_at", "updated_at",
        "verification_status",
    ]
    if not isinstance(knowledge_object, dict):
        return {"status": "FAIL", "mapping_status": "FAIL", "missing_fields": required, "object_checksum": None}
    missing = [field for field in required if field not in knowledge_object]
    invalid_space = knowledge_object.get("memory_space") not in SUPPORTED_MEMORY_SPACES
    invalid_identity = not knowledge_object.get("object_id") or not knowledge_object.get("knowledge_id")
    status = "PASS" if not missing and not invalid_space and not invalid_identity else "FAIL"
    return {
        "status": status,
        "mapping_status": status,
        "missing_fields": missing,
        "memory_space": knowledge_object.get("memory_space"),
        "knowledge_type": knowledge_object.get("knowledge_type"),
        "object_id": knowledge_object.get("object_id"),
        "object_checksum": knowledge_object.get("object_checksum") or _stable_checksum(knowledge_object),
    }


def get_knowledge_object_overview(domain: str = "bon_buasson") -> Dict[str, Any]:
    domain_key = _slug(domain or "bon_buasson", "bon_buasson")
    objects = list_professional_knowledge_objects() + list_business_knowledge_objects(domain_key)
    mapping_results = [verify_knowledge_object_mapping(obj) for obj in objects]
    errors = [result for result in mapping_results if result.get("status") != "PASS"]
    spaces: Dict[str, int] = {}
    for obj in objects:
        space = str(obj.get("memory_space") or "unknown")
        spaces[space] = spaces.get(space, 0) + 1
    return {
        "status": "ok" if not errors else "degraded",
        "render_mode": "vectra_internal_knowledge_object_overview",
        "release": KNOWLEDGE_OBJECT_RELEASE,
        "objects_count": len(objects),
        "readable_objects_count": len(objects) - len(errors),
        "mapping_errors_count": len(errors),
        "memory_spaces": sorted(spaces.keys()),
        "memory_space_counts": spaces,
        "supported_memory_spaces": sorted(SUPPORTED_MEMORY_SPACES),
        "domain": domain_key,
        "repository_paths": {
            "professional_memory": _repository_relative(professional_knowledge_path()),
            "business_domain_memory": _repository_relative(business_knowledge_path(domain_key)),
        },
        "mapping_errors": errors,
        "verification_status": "PASS" if not errors else "FAIL",
    }
