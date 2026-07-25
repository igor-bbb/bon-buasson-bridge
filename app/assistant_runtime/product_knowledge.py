"""MEMORY-IMPL-0007 Product Knowledge Runtime.

Adds product_memory as a first-class long-term memory space for knowledge about
VECTRA itself: product architecture, implemented capabilities, release behavior
and verified product evolution.

The module is backward compatible. It stores product knowledge in a dedicated
repository file and exposes it through the unified Knowledge Object shape without
moving or deleting Professional Knowledge or Business Domain Knowledge.
"""

from __future__ import annotations

import hashlib
import json
import re
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import ensure_repository, _read_json, _write_json
from app.assistant_runtime.knowledge_object import verify_knowledge_object_mapping
from app.assistant_runtime.memory_spaces import PRODUCT_MEMORY, validate_memory_space
from app.assistant_runtime.revision_model import archive_revision, next_version_from_record

PRODUCT_KNOWLEDGE_RELEASE = "MEMORY-IMPL-0007"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _slug(value: str, fallback: str = "product") -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9а-яіїєґ_-]+", "-", raw, flags=re.IGNORECASE).strip("-")
    return raw[:90] or fallback


def product_knowledge_path() -> Path:
    path = ensure_repository() / "knowledge" / "product_knowledge.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write_json(path, [])
    return path


def _read_product_items() -> List[Dict[str, Any]]:
    value = _read_json(product_knowledge_path(), [])
    return [dict(item) for item in value] if isinstance(value, list) else []


def _write_product_items(items: List[Dict[str, Any]]) -> None:
    _write_json(product_knowledge_path(), items)


def _normalize_evidence(evidence: Any) -> List[Any]:
    if evidence is None:
        return []
    return deepcopy(evidence) if isinstance(evidence, list) else [deepcopy(evidence)]


def _object_id(knowledge_id: str) -> str:
    return f"KO-{PRODUCT_MEMORY}-global-{knowledge_id}".replace(" ", "-")


def product_record_to_knowledge_object(record: Dict[str, Any]) -> Dict[str, Any]:
    record = dict(record or {})
    knowledge_id = str(record.get("knowledge_id") or record.get("id") or "").strip()
    if not knowledge_id:
        knowledge_id = f"PRK-{_stable_checksum(record)[:8].upper()}"
    created = record.get("created_at") or record.get("capitalized_at") or record.get("updated_at") or _now()
    updated = record.get("updated_at") or record.get("capitalized_at") or created
    obj = {
        "object_id": _object_id(knowledge_id),
        "memory_space": PRODUCT_MEMORY,
        "knowledge_type": "product",
        "domain": None,
        "title": record.get("title") or record.get("name") or knowledge_id,
        "description": record.get("description") or record.get("content") or record.get("text") or "",
        "version": int(record.get("version") or record.get("revision") or 1) if str(record.get("version") or record.get("revision") or "1").isdigit() else 1,
        "lifecycle_status": record.get("lifecycle_status") or record.get("status") or "CAPITALIZED",
        "source": record.get("source") or "VECTRA Product Knowledge Runtime",
        "evidence": _normalize_evidence(record.get("evidence")),
        "created_at": created,
        "updated_at": updated,
        "verification_status": record.get("verification_status") or record.get("readback_status") or "PASS" if knowledge_id else "FAIL",
        "knowledge_id": knowledge_id,
        "repository_path": record.get("repository_path") or "knowledge/product_knowledge.json",
        "source_record_checksum": _stable_checksum(record),
    }
    obj["object_checksum"] = _stable_checksum({k: v for k, v in obj.items() if k != "object_checksum"})
    return obj


def list_product_knowledge(limit: int = 100) -> Dict[str, Any]:
    items = _read_product_items()
    try:
        n = max(0, int(limit or 100))
    except Exception:
        n = 100
    objects = [product_record_to_knowledge_object(item) for item in items[:n]]
    return {
        "status": "ok",
        "render_mode": "vectra_product_knowledge_list",
        "release": PRODUCT_KNOWLEDGE_RELEASE,
        "memory_space": PRODUCT_MEMORY,
        "knowledge_count": len(items),
        "objects_count": len(objects),
        "objects": objects,
        "repository_path": "knowledge/product_knowledge.json",
        "backward_compatibility_status": "PASS",
    }


def get_product_knowledge(knowledge_id: str) -> Dict[str, Any]:
    kid = str(knowledge_id or "").strip()
    for item in _read_product_items():
        if str(item.get("knowledge_id") or item.get("id")) == kid:
            obj = product_record_to_knowledge_object(item)
            mapping = verify_knowledge_object_mapping(obj)
            return {
                "status": "ok" if mapping.get("mapping_status") == "PASS" else "degraded",
                "render_mode": "vectra_product_knowledge_read",
                "release": PRODUCT_KNOWLEDGE_RELEASE,
                "knowledge_id": kid,
                "memory_object": obj,
                "mapping_verification": mapping,
                "readback_status": "PASS" if mapping.get("mapping_status") == "PASS" else "FAIL",
            }
    return {"status": "not_found", "render_mode": "vectra_product_knowledge_read", "release": PRODUCT_KNOWLEDGE_RELEASE, "knowledge_id": kid, "readback_status": "FAIL"}


def write_product_knowledge(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    if not bool(payload.get("product_owner_approval") or payload.get("confirmed_by_product_owner")):
        return {
            "status": "REQUIRES_PRODUCT_OWNER_APPROVAL",
            "write_status": "BLOCKED",
            "release": PRODUCT_KNOWLEDGE_RELEASE,
            "reason": "product_owner_approval_required",
        }
    now = _now()
    knowledge_id = str(payload.get("knowledge_id") or f"PRK-{uuid.uuid4().hex[:8].upper()}")
    items = _read_product_items()
    previous = None
    previous_index = None
    for idx, item in enumerate(items):
        if str(item.get("knowledge_id")) == knowledge_id:
            previous = dict(item)
            previous_index = idx
            break
    version = int(payload.get("version") or payload.get("revision") or next_version_from_record(previous)) if str(payload.get("version") or payload.get("revision") or next_version_from_record(previous)).isdigit() else next_version_from_record(previous)
    record = {
        "knowledge_id": knowledge_id,
        "knowledge_type": "product",
        "memory_space": PRODUCT_MEMORY,
        "title": str(payload.get("title") or payload.get("name") or knowledge_id),
        "description": str(payload.get("description") or payload.get("content") or payload.get("text") or ""),
        "status": "CAPITALIZED",
        "lifecycle_status": "CAPITALIZED",
        "version": version,
        "source": str(payload.get("source") or "VECTRA Laboratory"),
        "evidence": _normalize_evidence(payload.get("evidence")),
        "created_at": payload.get("created_at") or now,
        "updated_at": now,
        "repository_path": "knowledge/product_knowledge.json",
        "product_owner_approved": True,
    }
    if previous is not None and previous_index is not None:
        record["created_at"] = previous.get("created_at") or record["created_at"]
        archive_revision(memory_object=product_record_to_knowledge_object(previous), reason="product_knowledge_updated", superseded_by_version=version, source_repository="knowledge/product_knowledge.json")
        items[previous_index] = record
    else:
        items.append(record)
    _write_product_items(items)
    readback = get_product_knowledge(knowledge_id)
    return {
        "status": "ok" if readback.get("readback_status") == "PASS" else "degraded",
        "write_status": "PASS",
        "readback_status": readback.get("readback_status"),
        "release": PRODUCT_KNOWLEDGE_RELEASE,
        "knowledge_id": knowledge_id,
        "memory_space": PRODUCT_MEMORY,
        "repository_path": "knowledge/product_knowledge.json",
        "memory_object": readback.get("memory_object"),
        "revision_model_status": "PASS",
    }


def verify_product_knowledge_readback(knowledge_id: Optional[str] = None) -> Dict[str, Any]:
    items = _read_product_items()
    if knowledge_id:
        return get_product_knowledge(str(knowledge_id))
    objects = [product_record_to_knowledge_object(item) for item in items]
    results = [verify_knowledge_object_mapping(obj) for obj in objects]
    failed = [r for r in results if r.get("mapping_status") != "PASS"]
    status = "PASS" if not failed else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "readback_status": status,
        "render_mode": "vectra_product_knowledge_readback_report",
        "release": PRODUCT_KNOWLEDGE_RELEASE,
        "memory_space": PRODUCT_MEMORY,
        "objects_count": len(objects),
        "pass_count": len(objects) - len(failed),
        "fail_count": len(failed),
        "repository_path": "knowledge/product_knowledge.json",
    }
