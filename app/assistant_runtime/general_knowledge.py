"""MEMORY-IMPL-0010 General Knowledge Runtime.

Adds general_memory as a first-class memory space for confirmed knowledge that is
not tied to a specific Business Domain and is not Product Knowledge or Product
Decision memory. Existing repositories are not moved or deleted.
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
from app.assistant_runtime.memory_spaces import GENERAL_MEMORY, validate_memory_space
from app.assistant_runtime.revision_model import archive_revision, next_version_from_record

GENERAL_KNOWLEDGE_RELEASE = "MEMORY-IMPL-0010"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _slug(value: str, fallback: str = "general") -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9а-яіїєґ_-]+", "-", raw, flags=re.IGNORECASE).strip("-")
    return raw[:90] or fallback


def general_knowledge_path() -> Path:
    path = ensure_repository() / "knowledge" / "general_knowledge.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write_json(path, [])
    return path


def _read_items() -> List[Dict[str, Any]]:
    value = _read_json(general_knowledge_path(), [])
    return [dict(item) for item in value] if isinstance(value, list) else []


def _write_items(items: List[Dict[str, Any]]) -> None:
    _write_json(general_knowledge_path(), items)


def _normalize_evidence(evidence: Any) -> List[Any]:
    if evidence is None:
        return []
    return deepcopy(evidence) if isinstance(evidence, list) else [deepcopy(evidence)]


def _object_id(knowledge_id: str) -> str:
    return f"KO-{GENERAL_MEMORY}-global-{knowledge_id}".replace(" ", "-")


def general_record_to_knowledge_object(record: Dict[str, Any]) -> Dict[str, Any]:
    record = dict(record or {})
    knowledge_id = str(record.get("knowledge_id") or record.get("id") or "").strip()
    if not knowledge_id:
        knowledge_id = f"GK-{_stable_checksum(record)[:8].upper()}"
    created = record.get("created_at") or record.get("capitalized_at") or record.get("updated_at") or _now()
    updated = record.get("updated_at") or record.get("capitalized_at") or created
    obj = {
        "object_id": _object_id(knowledge_id),
        "memory_space": GENERAL_MEMORY,
        "knowledge_type": "general",
        "domain": None,
        "title": record.get("title") or record.get("name") or knowledge_id,
        "description": record.get("description") or record.get("content") or record.get("text") or "",
        "version": int(record.get("version") or record.get("revision") or 1) if str(record.get("version") or record.get("revision") or "1").isdigit() else 1,
        "lifecycle_status": record.get("lifecycle_status") or record.get("status") or "CAPITALIZED",
        "source": record.get("source") or "VECTRA General Knowledge Runtime",
        "evidence": _normalize_evidence(record.get("evidence")),
        "created_at": created,
        "updated_at": updated,
        "verification_status": record.get("verification_status") or record.get("readback_status") or "PASS" if knowledge_id else "FAIL",
        "knowledge_id": knowledge_id,
        "repository_path": record.get("repository_path") or "knowledge/general_knowledge.json",
        "source_record_checksum": _stable_checksum(record),
    }
    obj["object_checksum"] = _stable_checksum({k: v for k, v in obj.items() if k != "object_checksum"})
    return obj


def list_general_knowledge(limit: int = 100) -> Dict[str, Any]:
    validate_memory_space(GENERAL_MEMORY, require_active=True)
    items = _read_items()
    try:
        n = max(0, int(limit or 100))
    except Exception:
        n = 100
    objects = [general_record_to_knowledge_object(item) for item in items[:n]]
    return {
        "status": "ok",
        "render_mode": "vectra_general_knowledge_list",
        "release": GENERAL_KNOWLEDGE_RELEASE,
        "memory_space": GENERAL_MEMORY,
        "knowledge_count": len(items),
        "objects_count": len(objects),
        "objects": objects,
        "repository_path": "knowledge/general_knowledge.json",
    }


def get_general_knowledge(knowledge_id: str) -> Dict[str, Any]:
    kid = str(knowledge_id or "").strip()
    for item in _read_items():
        if str(item.get("knowledge_id") or item.get("id")) == kid:
            obj = general_record_to_knowledge_object(item)
            mapping = verify_knowledge_object_mapping(obj)
            return {
                "status": "ok" if mapping.get("mapping_status") == "PASS" else "degraded",
                "render_mode": "vectra_general_knowledge_read",
                "release": GENERAL_KNOWLEDGE_RELEASE,
                "knowledge_id": kid,
                "memory_object": obj,
                "mapping_verification": mapping,
                "readback_status": "PASS" if mapping.get("mapping_status") == "PASS" else "FAIL",
            }
    return {"status": "not_found", "render_mode": "vectra_general_knowledge_read", "release": GENERAL_KNOWLEDGE_RELEASE, "knowledge_id": kid, "readback_status": "FAIL"}


def write_general_knowledge(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    if not bool(payload.get("product_owner_approval") or payload.get("confirmed_by_product_owner") or payload.get("confirmed")):
        return {"status": "REQUIRES_PRODUCT_OWNER_APPROVAL", "write_status": "BLOCKED", "release": GENERAL_KNOWLEDGE_RELEASE, "reason": "product_owner_approval_required"}
    now = _now()
    knowledge_id = str(payload.get("knowledge_id") or f"GK-{uuid.uuid4().hex[:8].upper()}")
    items = _read_items()
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
        "knowledge_type": "general",
        "memory_space": GENERAL_MEMORY,
        "title": str(payload.get("title") or payload.get("name") or knowledge_id),
        "description": str(payload.get("description") or payload.get("content") or payload.get("text") or ""),
        "status": "CAPITALIZED",
        "lifecycle_status": "CAPITALIZED",
        "version": version,
        "source": str(payload.get("source") or "VECTRA Laboratory"),
        "evidence": _normalize_evidence(payload.get("evidence")),
        "created_at": (previous or {}).get("created_at") or payload.get("created_at") or now,
        "updated_at": now,
        "repository_path": "knowledge/general_knowledge.json",
        "product_owner_approved": True,
    }
    if previous is not None and previous_index is not None:
        archive_revision(memory_object=general_record_to_knowledge_object(previous), reason="general_knowledge_updated", superseded_by_version=version, source_repository="knowledge/general_knowledge.json")
        items[previous_index] = record
    else:
        items.append(record)
    _write_items(items)
    readback = get_general_knowledge(knowledge_id)
    return {
        "status": "ok" if readback.get("readback_status") == "PASS" else "degraded",
        "write_status": "PASS",
        "readback_status": readback.get("readback_status"),
        "release": GENERAL_KNOWLEDGE_RELEASE,
        "knowledge_id": knowledge_id,
        "memory_space": GENERAL_MEMORY,
        "repository_path": "knowledge/general_knowledge.json",
        "memory_object": readback.get("memory_object"),
        "revision_model_status": "PASS",
    }


def verify_general_knowledge_readback(knowledge_id: Optional[str] = None) -> Dict[str, Any]:
    if knowledge_id:
        return get_general_knowledge(str(knowledge_id))
    objects = [general_record_to_knowledge_object(item) for item in _read_items()]
    results = [verify_knowledge_object_mapping(obj) for obj in objects]
    failed = [r for r in results if r.get("mapping_status") != "PASS"]
    status = "PASS" if not failed else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "readback_status": status,
        "render_mode": "vectra_general_knowledge_readback_report",
        "release": GENERAL_KNOWLEDGE_RELEASE,
        "memory_space": GENERAL_MEMORY,
        "objects_count": len(objects),
        "pass_count": len(objects) - len(failed),
        "fail_count": len(failed),
        "repository_path": "knowledge/general_knowledge.json",
    }
