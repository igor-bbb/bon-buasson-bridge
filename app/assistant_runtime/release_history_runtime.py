"""MEMORY-IMPL-0012 Release History Runtime.

Stores verified engineering release history as memory objects. Release history is
separate from Product Knowledge and Product Decisions and can be used to restore
the engineering development trail of VECTRA.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import ensure_repository, _read_json, _write_json
from app.assistant_runtime.knowledge_object import verify_knowledge_object_mapping
from app.assistant_runtime.memory_spaces import RELEASE_HISTORY_MEMORY
from app.assistant_runtime.revision_model import archive_revision, next_version_from_record

RELEASE_HISTORY_RUNTIME_RELEASE = "MEMORY-IMPL-0012"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def release_history_path() -> Path:
    path = ensure_repository() / "releases" / "release_history.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write_json(path, [])
    return path


def _read_releases() -> List[Dict[str, Any]]:
    value = _read_json(release_history_path(), [])
    return [dict(item) for item in value] if isinstance(value, list) else []


def _write_releases(items: List[Dict[str, Any]]) -> None:
    _write_json(release_history_path(), items)


def _normalize_evidence(evidence: Any) -> List[Any]:
    if evidence is None:
        return []
    return deepcopy(evidence) if isinstance(evidence, list) else [deepcopy(evidence)]


def _release_id(record: Dict[str, Any]) -> str:
    return str(record.get("release_id") or record.get("release") or record.get("increment_id") or f"REL-{_stable_checksum(record)[:8].upper()}")


def release_record_to_knowledge_object(record: Dict[str, Any]) -> Dict[str, Any]:
    record = dict(record or {})
    release_id = _release_id(record)
    created = record.get("created_at") or record.get("verified_at") or record.get("deployed_at") or record.get("updated_at") or _now()
    updated = record.get("updated_at") or record.get("verified_at") or record.get("deployed_at") or created
    description = record.get("description") or record.get("summary") or record.get("release_notes") or ""
    obj = {
        "object_id": f"KO-{RELEASE_HISTORY_MEMORY}-global-{release_id}".replace(" ", "-"),
        "memory_space": RELEASE_HISTORY_MEMORY,
        "knowledge_type": "release_history",
        "domain": None,
        "title": record.get("title") or release_id,
        "description": description,
        "version": int(record.get("version") or record.get("revision") or 1) if str(record.get("version") or record.get("revision") or "1").isdigit() else 1,
        "lifecycle_status": record.get("lifecycle_status") or record.get("status") or "VERIFIED",
        "source": record.get("source") or "VECTRA Engineering Release History Runtime",
        "evidence": _normalize_evidence(record.get("evidence")),
        "created_at": created,
        "updated_at": updated,
        "verification_status": record.get("verification_status") or record.get("product_verification_status") or "PASS",
        "knowledge_id": release_id,
        "release_id": release_id,
        "repository_path": record.get("repository_path") or "releases/release_history.json",
        "source_record_checksum": _stable_checksum(record),
        "product_verification_status": record.get("product_verification_status"),
        "deployment_status": record.get("deployment_status"),
        "capitalization_status": record.get("capitalization_status"),
    }
    obj["object_checksum"] = _stable_checksum({k: v for k, v in obj.items() if k != "object_checksum"})
    return obj


def list_release_history(limit: int = 100) -> Dict[str, Any]:
    items = _read_releases()
    try:
        n = max(0, int(limit or 100))
    except Exception:
        n = 100
    objects = [release_record_to_knowledge_object(item) for item in items[:n]]
    return {
        "status": "ok",
        "render_mode": "vectra_release_history_list",
        "release": RELEASE_HISTORY_RUNTIME_RELEASE,
        "memory_space": RELEASE_HISTORY_MEMORY,
        "releases_count": len(items),
        "objects_count": len(objects),
        "objects": objects,
        "repository_path": "releases/release_history.json",
    }


def get_release_history(release_id: str) -> Dict[str, Any]:
    rid = str(release_id or "").strip()
    for item in _read_releases():
        if _release_id(item) == rid:
            obj = release_record_to_knowledge_object(item)
            mapping = verify_knowledge_object_mapping(obj)
            return {
                "status": "ok" if mapping.get("mapping_status") == "PASS" else "degraded",
                "render_mode": "vectra_release_history_read",
                "release": RELEASE_HISTORY_RUNTIME_RELEASE,
                "release_id": rid,
                "memory_object": obj,
                "mapping_verification": mapping,
                "readback_status": "PASS" if mapping.get("mapping_status") == "PASS" else "FAIL",
            }
    return {"status": "not_found", "render_mode": "vectra_release_history_read", "release": RELEASE_HISTORY_RUNTIME_RELEASE, "release_id": rid, "readback_status": "FAIL"}


def write_release_history(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    if not bool(payload.get("product_verification_pass") or payload.get("product_owner_approval") or payload.get("confirmed_by_product_owner")):
        return {"status": "REQUIRES_VERIFIED_RELEASE", "write_status": "BLOCKED", "release": RELEASE_HISTORY_RUNTIME_RELEASE, "reason": "product_verification_pass_required"}
    now = _now()
    release_id = str(payload.get("release_id") or payload.get("release") or payload.get("increment_id") or f"REL-{uuid.uuid4().hex[:8].upper()}")
    items = _read_releases()
    previous = None
    previous_index = None
    for idx, item in enumerate(items):
        if _release_id(item) == release_id:
            previous = dict(item)
            previous_index = idx
            break
    version = int(payload.get("version") or payload.get("revision") or next_version_from_record(previous)) if str(payload.get("version") or payload.get("revision") or next_version_from_record(previous)).isdigit() else next_version_from_record(previous)
    record = {
        "release_id": release_id,
        "knowledge_type": "release_history",
        "memory_space": RELEASE_HISTORY_MEMORY,
        "title": str(payload.get("title") or release_id),
        "description": str(payload.get("description") or payload.get("summary") or payload.get("release_notes") or ""),
        "status": "VERIFIED",
        "lifecycle_status": "VERIFIED",
        "version": version,
        "source": str(payload.get("source") or "VECTRA Laboratory"),
        "evidence": _normalize_evidence(payload.get("evidence")),
        "created_at": (previous or {}).get("created_at") or payload.get("created_at") or now,
        "updated_at": now,
        "verified_at": payload.get("verified_at") or now,
        "deployed_at": payload.get("deployed_at"),
        "product_verification_status": "PASS",
        "deployment_status": payload.get("deployment_status") or "UNKNOWN",
        "capitalization_status": payload.get("capitalization_status") or "UNKNOWN",
        "repository_path": "releases/release_history.json",
    }
    if previous is not None and previous_index is not None:
        archive_revision(memory_object=release_record_to_knowledge_object(previous), reason="release_history_updated", superseded_by_version=version, source_repository="releases/release_history.json")
        items[previous_index] = record
    else:
        items.append(record)
    _write_releases(items)
    readback = get_release_history(release_id)
    return {
        "status": "ok" if readback.get("readback_status") == "PASS" else "degraded",
        "write_status": "PASS",
        "readback_status": readback.get("readback_status"),
        "release": RELEASE_HISTORY_RUNTIME_RELEASE,
        "release_id": release_id,
        "memory_space": RELEASE_HISTORY_MEMORY,
        "repository_path": "releases/release_history.json",
        "memory_object": readback.get("memory_object"),
        "revision_model_status": "PASS",
    }


def verify_release_history_readback(release_id: Optional[str] = None) -> Dict[str, Any]:
    if release_id:
        return get_release_history(str(release_id))
    objects = [release_record_to_knowledge_object(item) for item in _read_releases()]
    results = [verify_knowledge_object_mapping(obj) for obj in objects]
    failed = [r for r in results if r.get("mapping_status") != "PASS"]
    status = "PASS" if not failed else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "readback_status": status,
        "render_mode": "vectra_release_history_readback_report",
        "release": RELEASE_HISTORY_RUNTIME_RELEASE,
        "memory_space": RELEASE_HISTORY_MEMORY,
        "objects_count": len(objects),
        "pass_count": len(objects) - len(failed),
        "fail_count": len(failed),
        "repository_path": "releases/release_history.json",
    }
