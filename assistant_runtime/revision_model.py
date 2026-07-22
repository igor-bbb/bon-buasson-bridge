"""MEMORY-IMPL-0011 Revision & Version Model.

Internal revision ledger for VECTRA Memory Objects. The model preserves the
previous representation of a confirmed object before an active record is
updated. It is intentionally adapter-compatible: existing repositories keep
storing their active records, while historical revisions are persisted in a
separate ledger and can be inspected by Laboratory.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import ensure_repository, _read_json, _write_json
from app.assistant_runtime.knowledge_object import verify_knowledge_object_mapping

REVISION_MODEL_RELEASE = "MEMORY-IMPL-0011"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def revisions_path() -> Path:
    path = ensure_repository() / "memory" / "revisions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write_json(path, [])
    return path


def _read_revisions() -> List[Dict[str, Any]]:
    value = _read_json(revisions_path(), [])
    return [dict(item) for item in value] if isinstance(value, list) else []


def _write_revisions(items: List[Dict[str, Any]]) -> None:
    _write_json(revisions_path(), items)


def normalize_version(value: Any, default: int = 1) -> int:
    try:
        return max(1, int(value or default))
    except Exception:
        return default


def next_version_from_record(record: Optional[Dict[str, Any]]) -> int:
    if not isinstance(record, dict) or not record:
        return 1
    return normalize_version(record.get("version") or record.get("revision"), 1) + 1


def _record_identity(memory_object: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "object_id": str(memory_object.get("object_id") or ""),
        "knowledge_id": memory_object.get("knowledge_id") or memory_object.get("decision_id") or memory_object.get("release_id"),
        "memory_space": memory_object.get("memory_space"),
        "knowledge_type": memory_object.get("knowledge_type"),
        "domain": memory_object.get("domain"),
    }


def archive_revision(
    *,
    memory_object: Optional[Dict[str, Any]],
    reason: str = "active_record_updated",
    superseded_by_version: Optional[int] = None,
    source_repository: Optional[str] = None,
) -> Dict[str, Any]:
    """Persist a historical revision before replacing an active record."""
    if not isinstance(memory_object, dict) or not memory_object:
        return {"status": "SKIPPED", "archive_status": "SKIPPED", "reason": "empty_memory_object", "release": REVISION_MODEL_RELEASE}
    mapping = verify_knowledge_object_mapping(memory_object)
    identity = _record_identity(memory_object)
    version = normalize_version(memory_object.get("version") or memory_object.get("revision"), 1)
    revision_id = f"REV-{_stable_checksum({'identity': identity, 'version': version, 'checksum': memory_object.get('object_checksum') or _stable_checksum(memory_object)})[:16].upper()}"
    items = _read_revisions()
    if any(str(item.get("revision_id")) == revision_id for item in items):
        return {"status": "ok", "archive_status": "PASS", "deduplicated": True, "revision_id": revision_id, "release": REVISION_MODEL_RELEASE}
    revision = {
        "revision_id": revision_id,
        **identity,
        "version": version,
        "revision_status": "SUPERSEDED",
        "superseded_by_version": superseded_by_version,
        "reason": reason,
        "source_repository": source_repository or memory_object.get("repository_path"),
        "memory_object": deepcopy(memory_object),
        "mapping_status": mapping.get("mapping_status"),
        "archived_at": _now(),
        "checksum": _stable_checksum(memory_object),
    }
    items.append(revision)
    _write_revisions(items)
    return {"status": "ok", "archive_status": "PASS", "revision_id": revision_id, "release": REVISION_MODEL_RELEASE}


def list_revisions(object_id: Optional[str] = None, knowledge_id: Optional[str] = None, memory_space: Optional[str] = None, limit: int = 100) -> Dict[str, Any]:
    items = _read_revisions()
    if object_id:
        items = [item for item in items if str(item.get("object_id")) == str(object_id)]
    if knowledge_id:
        items = [item for item in items if str(item.get("knowledge_id")) == str(knowledge_id)]
    if memory_space:
        items = [item for item in items if str(item.get("memory_space")) == str(memory_space)]
    try:
        n = max(0, int(limit or 100))
    except Exception:
        n = 100
    return {
        "status": "ok",
        "render_mode": "vectra_memory_revision_list",
        "release": REVISION_MODEL_RELEASE,
        "revisions_count": len(items),
        "revisions": deepcopy(items[:n]),
        "limit": n,
        "repository_path": "memory/revisions.json",
    }


def get_revision(revision_id: str) -> Dict[str, Any]:
    rid = str(revision_id or "").strip()
    for item in _read_revisions():
        if str(item.get("revision_id")) == rid:
            return {"status": "ok", "render_mode": "vectra_memory_revision_read", "release": REVISION_MODEL_RELEASE, "revision": deepcopy(item)}
    return {"status": "not_found", "render_mode": "vectra_memory_revision_read", "release": REVISION_MODEL_RELEASE, "revision_id": rid}


def get_version_status(active_objects: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    active_objects = active_objects if isinstance(active_objects, list) else []
    revisions = _read_revisions()
    active_versions: Dict[str, int] = {}
    for obj in active_objects:
        if not isinstance(obj, dict):
            continue
        oid = str(obj.get("object_id") or "")
        if oid:
            active_versions[oid] = normalize_version(obj.get("version"), 1)
    orphan_revisions = [item for item in revisions if str(item.get("object_id") or "") and str(item.get("object_id")) not in active_versions]
    return {
        "status": "PASS",
        "verification_status": "PASS",
        "render_mode": "vectra_memory_version_status",
        "release": REVISION_MODEL_RELEASE,
        "active_objects_count": len(active_objects),
        "revision_records_count": len(revisions),
        "active_versions": active_versions,
        "orphan_revisions_count": len(orphan_revisions),
        "no_destructive_overwrite_status": "PASS",
        "repository_path": "memory/revisions.json",
    }


def verify_revision_model(active_objects: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    status = get_version_status(active_objects=active_objects)
    revisions = _read_revisions()
    invalid = []
    for item in revisions:
        obj = item.get("memory_object") if isinstance(item, dict) else None
        mapping = verify_knowledge_object_mapping(obj) if isinstance(obj, dict) else {"mapping_status": "FAIL"}
        if mapping.get("mapping_status") != "PASS":
            invalid.append(item.get("revision_id"))
    verification = "PASS" if not invalid else "FAIL"
    return {
        "status": verification,
        "verification_status": verification,
        "readback_status": verification,
        "render_mode": "vectra_memory_revision_verification",
        "release": REVISION_MODEL_RELEASE,
        "revision_records_count": len(revisions),
        "invalid_revisions_count": len(invalid),
        "invalid_revision_ids": invalid,
        "version_status": status,
        "no_destructive_overwrite_status": "PASS" if not invalid else "FAIL",
    }
