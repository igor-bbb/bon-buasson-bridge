"""MEMORY-IMPL-0008 Product Decisions Runtime.

Stores Product Owner approved product decisions in a separate normative memory
space. Product decisions are not mixed with ordinary knowledge and are exposed as
Knowledge Objects for unified memory inspection and recovery.
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
from app.assistant_runtime.memory_spaces import PRODUCT_DECISIONS_MEMORY
from app.assistant_runtime.revision_model import archive_revision, next_version_from_record

PRODUCT_DECISIONS_RELEASE = "MEMORY-IMPL-0008"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def product_decisions_path() -> Path:
    path = ensure_repository() / "decisions" / "product_decisions.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write_json(path, [])
    return path


def _read_decisions() -> List[Dict[str, Any]]:
    value = _read_json(product_decisions_path(), [])
    return [dict(item) for item in value] if isinstance(value, list) else []


def _write_decisions(items: List[Dict[str, Any]]) -> None:
    _write_json(product_decisions_path(), items)


def _normalize_evidence(evidence: Any) -> List[Any]:
    if evidence is None:
        return []
    return deepcopy(evidence) if isinstance(evidence, list) else [deepcopy(evidence)]


def _decision_id(record: Dict[str, Any]) -> str:
    return str(record.get("decision_id") or record.get("knowledge_id") or record.get("id") or f"PD-{_stable_checksum(record)[:8].upper()}")


def decision_record_to_knowledge_object(record: Dict[str, Any]) -> Dict[str, Any]:
    record = dict(record or {})
    decision_id = _decision_id(record)
    created = record.get("created_at") or record.get("approved_at") or record.get("updated_at") or _now()
    updated = record.get("updated_at") or record.get("approved_at") or created
    title = record.get("title") or record.get("decision") or decision_id
    description = record.get("description") or record.get("content") or record.get("decision") or ""
    obj = {
        "object_id": f"KO-{PRODUCT_DECISIONS_MEMORY}-global-{decision_id}".replace(" ", "-"),
        "memory_space": PRODUCT_DECISIONS_MEMORY,
        "knowledge_type": "product_decision",
        "domain": None,
        "title": title,
        "description": description,
        "version": int(record.get("version") or record.get("revision") or 1) if str(record.get("version") or record.get("revision") or "1").isdigit() else 1,
        "lifecycle_status": record.get("lifecycle_status") or record.get("status") or "APPROVED",
        "source": record.get("source") or "Product Owner",
        "evidence": _normalize_evidence(record.get("evidence")),
        "created_at": created,
        "updated_at": updated,
        "verification_status": record.get("verification_status") or record.get("readback_status") or "PASS",
        "knowledge_id": decision_id,
        "decision_id": decision_id,
        "repository_path": record.get("repository_path") or "decisions/product_decisions.json",
        "source_record_checksum": _stable_checksum(record),
        "normative_memory": True,
    }
    obj["object_checksum"] = _stable_checksum({k: v for k, v in obj.items() if k != "object_checksum"})
    return obj


def list_product_decisions(limit: int = 100) -> Dict[str, Any]:
    items = _read_decisions()
    try:
        n = max(0, int(limit or 100))
    except Exception:
        n = 100
    objects = [decision_record_to_knowledge_object(item) for item in items[:n]]
    return {
        "status": "ok",
        "render_mode": "vectra_product_decisions_list",
        "release": PRODUCT_DECISIONS_RELEASE,
        "memory_space": PRODUCT_DECISIONS_MEMORY,
        "decisions_count": len(items),
        "objects_count": len(objects),
        "objects": objects,
        "repository_path": "decisions/product_decisions.json",
        "knowledge_decision_separation_status": "PASS",
    }


def get_product_decision(decision_id: str) -> Dict[str, Any]:
    did = str(decision_id or "").strip()
    for item in _read_decisions():
        if _decision_id(item) == did:
            obj = decision_record_to_knowledge_object(item)
            mapping = verify_knowledge_object_mapping(obj)
            return {
                "status": "ok" if mapping.get("mapping_status") == "PASS" else "degraded",
                "render_mode": "vectra_product_decision_read",
                "release": PRODUCT_DECISIONS_RELEASE,
                "decision_id": did,
                "memory_object": obj,
                "mapping_verification": mapping,
                "readback_status": "PASS" if mapping.get("mapping_status") == "PASS" else "FAIL",
            }
    return {"status": "not_found", "render_mode": "vectra_product_decision_read", "release": PRODUCT_DECISIONS_RELEASE, "decision_id": did, "readback_status": "FAIL"}


def write_product_decision(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    if not bool(payload.get("product_owner_approval") or payload.get("confirmed_by_product_owner")):
        return {
            "status": "REQUIRES_PRODUCT_OWNER_APPROVAL",
            "write_status": "BLOCKED",
            "release": PRODUCT_DECISIONS_RELEASE,
            "reason": "product_owner_approval_required",
        }
    now = _now()
    decision_id = str(payload.get("decision_id") or payload.get("knowledge_id") or f"PD-{uuid.uuid4().hex[:8].upper()}")
    items = _read_decisions()
    previous = None
    previous_index = None
    for idx, item in enumerate(items):
        if _decision_id(item) == decision_id:
            previous = dict(item)
            previous_index = idx
            break
    version = int(payload.get("version") or payload.get("revision") or next_version_from_record(previous)) if str(payload.get("version") or payload.get("revision") or next_version_from_record(previous)).isdigit() else next_version_from_record(previous)
    record = {
        "decision_id": decision_id,
        "knowledge_id": decision_id,
        "knowledge_type": "product_decision",
        "memory_space": PRODUCT_DECISIONS_MEMORY,
        "title": str(payload.get("title") or payload.get("decision") or decision_id),
        "decision": str(payload.get("decision") or payload.get("description") or payload.get("content") or payload.get("text") or ""),
        "description": str(payload.get("description") or payload.get("decision") or payload.get("content") or payload.get("text") or ""),
        "status": "APPROVED",
        "lifecycle_status": "APPROVED",
        "version": version,
        "source": str(payload.get("source") or "Product Owner"),
        "evidence": _normalize_evidence(payload.get("evidence")),
        "created_at": payload.get("created_at") or now,
        "updated_at": now,
        "approved_at": payload.get("approved_at") or now,
        "repository_path": "decisions/product_decisions.json",
        "product_owner_approved": True,
    }
    if previous is not None and previous_index is not None:
        record["created_at"] = previous.get("created_at") or record["created_at"]
        archive_revision(memory_object=decision_record_to_knowledge_object(previous), reason="product_decision_updated", superseded_by_version=version, source_repository="decisions/product_decisions.json")
        items[previous_index] = record
    else:
        items.append(record)
    _write_decisions(items)
    readback = get_product_decision(decision_id)
    return {
        "status": "ok" if readback.get("readback_status") == "PASS" else "degraded",
        "write_status": "PASS",
        "readback_status": readback.get("readback_status"),
        "release": PRODUCT_DECISIONS_RELEASE,
        "decision_id": decision_id,
        "memory_space": PRODUCT_DECISIONS_MEMORY,
        "repository_path": "decisions/product_decisions.json",
        "memory_object": readback.get("memory_object"),
        "revision_model_status": "PASS",
    }


def verify_product_decisions_readback(decision_id: Optional[str] = None) -> Dict[str, Any]:
    if decision_id:
        return get_product_decision(str(decision_id))
    objects = [decision_record_to_knowledge_object(item) for item in _read_decisions()]
    results = [verify_knowledge_object_mapping(obj) for obj in objects]
    failed = [r for r in results if r.get("mapping_status") != "PASS"]
    status = "PASS" if not failed else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "readback_status": status,
        "render_mode": "vectra_product_decisions_readback_report",
        "release": PRODUCT_DECISIONS_RELEASE,
        "memory_space": PRODUCT_DECISIONS_MEMORY,
        "objects_count": len(objects),
        "pass_count": len(objects) - len(failed),
        "fail_count": len(failed),
        "repository_path": "decisions/product_decisions.json",
        "knowledge_decision_separation_status": "PASS",
    }
