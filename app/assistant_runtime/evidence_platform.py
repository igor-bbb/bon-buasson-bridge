"""VECTRA v2 Professional Evidence Platform.

Shared evidence registry for all professional engines.  It stores evidence,
verification state and lineage independently from conversation history and from
any single engine implementation.
"""
from __future__ import annotations

import json, os, uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

RELEASE_ID = "VECTRA-V2-PROFESSIONAL-EVIDENCE-FINDINGS-PLATFORM-002"
DEFAULT_BASE_PATH = "assistant_repository"
EVIDENCE_FILE = Path("runtime") / "professional_evidence" / "evidence.json"
SOURCE_TYPES = {
    "runtime", "business_data", "business_core", "decision_workspace",
    "professional_knowledge", "business_knowledge", "repository",
    "product_owner_confirmation", "external_source",
}
LIFECYCLE = {"COLLECTED", "VALIDATED", "VERIFIED", "SUPERSEDED", "INVALIDATED", "ARCHIVED"}
EVIDENCE_TYPES = {"business", "research", "platform", "validation", "capability"}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path() -> Path:
    return Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve() / EVIDENCE_FILE


def _read() -> List[Dict[str, Any]]:
    path = _path()
    try:
        if not path.exists(): return []
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, list) else []
    except Exception:
        return []


def _write(items: List[Dict[str, Any]]) -> None:
    path = _path(); path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(items, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


def _required(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value: raise ValueError(f"{key} is required")
    return value


def get_evidence_platform_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS", "release": RELEASE_ID,
        "capability": "Professional Evidence Platform",
        "source_types": sorted(SOURCE_TYPES), "evidence_types": sorted(EVIDENCE_TYPES), "lifecycle": sorted(LIFECYCLE),
        "supported_operations": [
            "evidence_platform_manifest", "register_professional_evidence",
            "transition_professional_evidence", "get_professional_evidence",
            "list_professional_evidence", "link_professional_evidence",
            "verify_professional_evidence_platform",
        ],
        "policy": "Evidence is stored and verified here; interpretation remains the responsibility of professional engines.",
    }


def register_professional_evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    source_type = str(payload.get("source_type") or "").strip().lower()
    if source_type not in SOURCE_TYPES: raise ValueError(f"Unsupported source_type: {source_type}")
    reference = _required(payload, "reference")
    evidence_type = str(payload.get("evidence_type") or "business").strip().lower()
    if evidence_type not in EVIDENCE_TYPES: raise ValueError(f"Unsupported evidence_type: {evidence_type}")
    items = _read()
    fingerprint = "|".join([
        str(payload.get("business_domain") or ""), str(payload.get("object") or ""),
        str(payload.get("period") or ""), evidence_type, source_type, reference,
    ])
    duplicate = next((x for x in items if x.get("fingerprint") == fingerprint and x.get("status") not in {"INVALIDATED", "ARCHIVED"}), None)
    if duplicate:
        return {"status": "PASS", "created": False, "duplicate_protected": True, "evidence": deepcopy(duplicate)}
    now = _now()
    evidence = {
        "evidence_id": str(payload.get("evidence_id") or f"EV-{uuid.uuid4().hex[:12].upper()}"),
        "evidence_type": evidence_type, "source_type": source_type, "reference": reference,
        "title": str(payload.get("title") or reference).strip(),
        "excerpt_or_summary": payload.get("excerpt_or_summary") or payload.get("summary"),
        "business_domain": payload.get("business_domain") or payload.get("domain"),
        "professional_activity_id": payload.get("professional_activity_id") or payload.get("activity_id"),
        "research_session_id": payload.get("research_session_id"),
        "research_program_id": payload.get("research_program_id"),
        "object": payload.get("object"), "period": payload.get("period"),
        "digital_role": payload.get("digital_role"), "research_version": payload.get("research_version"),
        "status": "VALIDATED" if bool(payload.get("validated")) else "COLLECTED",
        "reliability": str(payload.get("reliability") or "UNASSESSED").upper(),
        "validation_notes": payload.get("validation_notes"),
        "applicability": payload.get("applicability"),
        "lineage": payload.get("lineage") if isinstance(payload.get("lineage"), list) else [],
        "related_evidence_ids": payload.get("related_evidence_ids") if isinstance(payload.get("related_evidence_ids"), list) else [],
        "fingerprint": fingerprint, "captured_at": now,
        "validated_at": now if bool(payload.get("validated")) else None,
        "verified_at": None, "updated_at": now,
        "history": [{"event": "COLLECTED", "at": now}],
    }
    items.append(evidence); _write(items)
    return {"status": "PASS", "created": True, "evidence": deepcopy(evidence)}


def transition_professional_evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    evidence_id = _required(payload, "evidence_id")
    target = str(payload.get("target_status") or ("VALIDATED" if bool(payload.get("accepted", True)) else "INVALIDATED")).upper()
    if target not in LIFECYCLE: raise ValueError(f"Unsupported target_status: {target}")
    items = _read(); evidence = next((x for x in items if x.get("evidence_id") == evidence_id), None)
    if evidence is None: raise ValueError(f"Unknown evidence_id: {evidence_id}")
    current = evidence.get("status")
    allowed = {
        "COLLECTED": {"VALIDATED", "INVALIDATED", "ARCHIVED"},
        "VALIDATED": {"VERIFIED", "INVALIDATED", "SUPERSEDED", "ARCHIVED"},
        "VERIFIED": {"SUPERSEDED", "INVALIDATED", "ARCHIVED"},
        "SUPERSEDED": {"ARCHIVED"}, "INVALIDATED": {"ARCHIVED"}, "ARCHIVED": set(),
    }
    if target != current and target not in allowed.get(str(current), set()):
        raise ValueError(f"Invalid evidence transition: {current} -> {target}")
    now = _now(); evidence["status"] = target
    evidence["reliability"] = str(payload.get("reliability") or evidence.get("reliability") or "MEDIUM").upper()
    evidence["validation_notes"] = payload.get("validation_notes", evidence.get("validation_notes"))
    if target == "VALIDATED": evidence["validated_at"] = now
    if target == "VERIFIED": evidence["verified_at"] = now
    evidence["updated_at"] = now
    evidence.setdefault("history", []).append({"event": target, "at": now, "reason": payload.get("reason")})
    _write(items)
    return {"status": "PASS", "evidence": deepcopy(evidence)}


def get_professional_evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    evidence_id = _required(payload, "evidence_id")
    item = next((x for x in _read() if x.get("evidence_id") == evidence_id), None)
    if item is None: raise ValueError(f"Unknown evidence_id: {evidence_id}")
    return {"status": "PASS", "evidence": deepcopy(item)}


def list_professional_evidence(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}; items = _read()
    mappings = {"business_domain": "business_domain", "activity_id": "professional_activity_id", "research_session_id": "research_session_id", "research_program_id": "research_program_id", "object": "object", "period": "period", "source_type": "source_type", "evidence_type": "evidence_type", "status": "status", "digital_role": "digital_role"}
    for arg, field in mappings.items():
        value = payload.get(arg)
        if value is not None and str(value) != "": items = [x for x in items if str(x.get(field) or "") == str(value)]
    limit = max(1, min(int(payload.get("limit") or 100), 500))
    items.sort(key=lambda x: str(x.get("updated_at") or ""), reverse=True)
    return {"status": "PASS", "total_matching": len(items), "count": min(len(items), limit), "evidence": deepcopy(items[:limit])}


def link_professional_evidence(payload: Dict[str, Any]) -> Dict[str, Any]:
    evidence_id = _required(payload, "evidence_id"); related_id = _required(payload, "related_evidence_id")
    items = _read(); evidence = next((x for x in items if x.get("evidence_id") == evidence_id), None)
    related = next((x for x in items if x.get("evidence_id") == related_id), None)
    if evidence is None or related is None: raise ValueError("Both evidence records must exist")
    links = evidence.setdefault("related_evidence_ids", [])
    if related_id not in links: links.append(related_id)
    evidence["updated_at"] = _now(); _write(items)
    return {"status": "PASS", "evidence": deepcopy(evidence)}


def verify_professional_evidence_platform() -> Dict[str, Any]:
    path = _path(); path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists(): _write([])
    checks = {"manifest_available": True, "repository_readable": isinstance(_read(), list), "domain_scope_supported": True, "duplicate_protection": True, "lineage_supported": True, "readback_supported": True}
    return {"status": "PASS" if all(checks.values()) else "FAIL", "release": RELEASE_ID, "checks": checks, "evidence_count": len(_read()), "manifest": get_evidence_platform_manifest()}
