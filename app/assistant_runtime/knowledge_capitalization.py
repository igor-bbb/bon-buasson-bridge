"""FOUNDATION-0009 Knowledge Capitalization Runtime.

Official Product Owner approved Write -> Readback -> Recovery -> Evolution Journal
capitalization flow for Professional and Business Knowledge.
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

from app.assistant_runtime.repository import (
    ensure_repository,
    create_recovery_snapshot,
    get_active_business_domain,
    get_business_domain_profile,
    _read_json,  # internal repository primitives reused inside Runtime package
    _write_json,
    _append_json_list,
    _with_workspace_markdown,
)

KNOWLEDGE_RELEASE = "FOUNDATION-0012"
KNOWLEDGE_STATUSES = [
    "DETECTED",
    "CONFIRMED_BY_PRODUCT_OWNER",
    "CAPITALIZATION_PACKAGE_CREATED",
    "WRITTEN",
    "READBACK_PASS",
    "RECOVERY_UPDATED",
    "CAPITALIZED",
    "FAILED",
]
KNOWLEDGE_TYPES = {"professional", "business"}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _slug(value: str, fallback: str = "knowledge") -> str:
    raw = str(value or "").strip().lower()
    raw = re.sub(r"[^a-z0-9а-яіїєґ_-]+", "-", raw, flags=re.IGNORECASE).strip("-")
    return raw[:90] or fallback




def _repository_relative(path: Path) -> str:
    try:
        return str(path.resolve().relative_to(ensure_repository().resolve())).replace("\\", "/")
    except Exception:
        return str(path).replace("\\", "/")


def _stable_checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def _file_checksum(path: Path) -> Optional[str]:
    try:
        h = hashlib.sha256()
        with path.open("rb") as fh:
            for chunk in iter(lambda: fh.read(65536), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:
        return None


def _normalize_professional_record(record: Dict[str, Any]) -> Dict[str, Any]:
    created = record.get("created_at") or record.get("capitalized_at")
    updated = record.get("updated_at") or record.get("capitalized_at") or created
    return {
        "knowledge_id": record.get("knowledge_id"),
        "title": record.get("title"),
        "type": record.get("type") or record.get("knowledge_type") or "professional",
        "status": record.get("status"),
        "description": record.get("description") or record.get("content"),
        "repository_path": record.get("repository_path") or "knowledge/professional_knowledge.json",
        "capitalization_package": record.get("capitalization_package") or record.get("package_id"),
        "created_at": created,
        "updated_at": updated,
        "source": record.get("source"),
        "professional_model_auto_update": bool(record.get("professional_model_auto_update", False)),
    }

def _paths() -> Dict[str, Path]:
    base = ensure_repository()
    kc = base / "runtime" / "knowledge_capitalization"
    kc.mkdir(parents=True, exist_ok=True)
    for name in ["candidates.json", "packages.json", "reports.json", "status.json", "failed_reports.json"]:
        path = kc / name
        if not path.exists():
            _write_json(path, [] if name != "status.json" else {
                "status": "ready",
                "release": KNOWLEDGE_RELEASE,
                "last_package_id": None,
                "last_report_id": None,
                "last_final_status": None,
                "product_owner_approval_required": True,
                "updated_at": _now(),
            })
    professional_path = base / "knowledge" / "professional_knowledge.json"
    if not professional_path.exists():
        _write_json(professional_path, [])
    return {
        "base": base,
        "kc": kc,
        "candidates": kc / "candidates.json",
        "packages": kc / "packages.json",
        "reports": kc / "reports.json",
        "status": kc / "status.json",
        "failed_reports": kc / "failed_reports.json",
        "professional": professional_path,
        "journal": base / "journal" / "evolution_journal.json",
    }


def _read_list(path: Path) -> List[Dict[str, Any]]:
    value = _read_json(path, [])
    return value if isinstance(value, list) else []


def _find_by_id(items: List[Dict[str, Any]], key: str, value: str) -> Optional[Dict[str, Any]]:
    for item in items:
        if str(item.get(key)) == str(value):
            return item
    return None


def create_knowledge_candidate(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    knowledge_type = str(payload.get("knowledge_type") or payload.get("type") or "professional").strip().lower()
    if knowledge_type not in KNOWLEDGE_TYPES:
        return {"status": "error", "render_mode": "vectra_knowledge_candidate", "reason": "unsupported_knowledge_type", "allowed": sorted(KNOWLEDGE_TYPES)}
    candidate_id = str(payload.get("candidate_id") or f"KC-{uuid.uuid4().hex[:10].upper()}")
    title = str(payload.get("title") or payload.get("name") or candidate_id)
    content = str(payload.get("content") or payload.get("description") or "").strip()
    if not content:
        return {"status": "error", "render_mode": "vectra_knowledge_candidate", "reason": "content_required"}
    active_domain = get_active_business_domain().get("active_domain") or {}
    domain = str(payload.get("domain") or active_domain.get("domain_id") or "bonboason").strip().lower()
    now = _now()
    candidate = {
        "candidate_id": candidate_id,
        "knowledge_id": str(payload.get("knowledge_id") or candidate_id.replace("KC-", "K-")),
        "knowledge_type": knowledge_type,
        "title": title,
        "content": content,
        "domain": domain if knowledge_type == "business" else None,
        "status": "CONFIRMED_BY_PRODUCT_OWNER" if bool(payload.get("product_owner_approval")) else "DETECTED",
        "product_owner_approval": bool(payload.get("product_owner_approval")),
        "source": str(payload.get("source") or "VECTRA Laboratory"),
        "created_at": now,
        "updated_at": now,
        "professional_model_auto_update": False,
        "target_repository": "knowledge/professional_knowledge.json" if knowledge_type == "professional" else f"runtime/business_domains/{domain}/domain_profile.json#business_knowledge",
    }
    paths = _paths()
    candidates = _read_list(paths["candidates"])
    existing = _find_by_id(candidates, "candidate_id", candidate_id)
    if existing:
        existing.update(candidate)
        _write_json(paths["candidates"], candidates)
    else:
        candidates.append(candidate)
        _write_json(paths["candidates"], candidates)
    return _with_workspace_markdown({
        "status": "ok",
        "render_mode": "vectra_knowledge_candidate",
        "release": KNOWLEDGE_RELEASE,
        "candidate": candidate,
        "capitalization_allowed": candidate["product_owner_approval"],
    }, "Knowledge Candidate VECTRA", candidate)


def _professional_knowledge_readback(knowledge_id: str) -> Dict[str, Any]:
    items = _read_list(_paths()["professional"])
    found = _find_by_id(items, "knowledge_id", knowledge_id)
    return {"status": "PASS" if found else "FAIL", "knowledge_id": knowledge_id, "found": bool(found), "record": found}


def _domain_knowledge_path(domain: str) -> Path:
    domain_key = _slug(domain or "bonboason", "bonboason")
    return ensure_repository() / "runtime" / "business_domains" / domain_key / "domain_profile.json"


def _business_knowledge_readback(domain: str, knowledge_id: str) -> Dict[str, Any]:
    profile_payload = get_business_domain_profile(domain)
    profile = profile_payload.get("domain_profile") if isinstance(profile_payload, dict) else None
    items = profile.get("business_knowledge", []) if isinstance(profile, dict) else []
    found = _find_by_id(items if isinstance(items, list) else [], "knowledge_id", knowledge_id)
    return {"status": "PASS" if found else "FAIL", "domain": domain, "knowledge_id": knowledge_id, "found": bool(found), "record": found}



def _update_candidate(candidate: Dict[str, Any]) -> None:
    paths = _paths()
    candidates = _read_list(paths["candidates"])
    updated = False
    for item in candidates:
        if item.get("candidate_id") == candidate.get("candidate_id"):
            item.update(candidate)
            updated = True
    if not updated:
        candidates.append(candidate)
    _write_json(paths["candidates"], candidates)


def _resolve_confirmed_candidate(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    paths = _paths()
    candidates = _read_list(paths["candidates"])
    candidate_id = str(payload.get("candidate_id") or "").strip()
    candidate = _find_by_id(candidates, "candidate_id", candidate_id) if candidate_id else None
    if not candidate:
        created = create_knowledge_candidate(payload)
        candidate = created.get("candidate") if isinstance(created, dict) else None
    if not isinstance(candidate, dict):
        raise ValueError("candidate_not_found_or_invalid")
    if bool(payload.get("product_owner_approval")):
        candidate["product_owner_approval"] = True
        candidate["status"] = "CONFIRMED_BY_PRODUCT_OWNER"
        candidate["updated_at"] = _now()
        _update_candidate(candidate)
    if not candidate.get("product_owner_approval") or candidate.get("status") != "CONFIRMED_BY_PRODUCT_OWNER":
        raise PermissionError("product_owner_approval_required")
    return candidate


def create_capitalization_package(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create an explicit capitalization package without writing knowledge.

    FOUNDATION-0010 exposes this as a separate Laboratory command so VECTRA can run
    the official sequence: candidate -> package -> write -> report.
    """
    payload = payload if isinstance(payload, dict) else {}
    try:
        candidate = _resolve_confirmed_candidate(payload)
    except PermissionError as exc:
        return _failed_report(None, None, str(exc))
    except ValueError as exc:
        return _failed_report(None, None, str(exc))

    paths = _paths()
    knowledge_id = str(candidate.get("knowledge_id") or f"K-{uuid.uuid4().hex[:8]}")
    knowledge_type = str(candidate.get("knowledge_type") or "professional").lower()
    domain = str(candidate.get("domain") or (get_active_business_domain().get("active_domain") or {}).get("domain_id") or "bonboason").lower()
    target_repository = candidate.get("target_repository") or ("knowledge/professional_knowledge.json" if knowledge_type == "professional" else f"runtime/business_domains/{domain}/domain_profile.json#business_knowledge")
    package_id = str(payload.get("package_id") or f"KCAP-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}")
    now = _now()

    package = {
        "package_id": package_id,
        "candidate_id": candidate.get("candidate_id"),
        "knowledge_id": knowledge_id,
        "knowledge_type": knowledge_type,
        "domain": domain if knowledge_type == "business" else None,
        "target_repository": target_repository,
        "status": "CAPITALIZATION_PACKAGE_CREATED",
        "product_owner_approval": True,
        "created_at": now,
        "write_readback_verification_required": True,
        "write_status": "NOT_EXECUTED",
        "readback_status": "NOT_EXECUTED",
        "final_status": "PACKAGE_CREATED",
    }
    packages = _read_list(paths["packages"])
    existing = _find_by_id(packages, "package_id", package_id)
    if existing:
        existing.update(package)
    else:
        packages.append(package)
    _write_json(paths["packages"], packages)

    candidate["status"] = "CAPITALIZATION_PACKAGE_CREATED"
    candidate["updated_at"] = now
    _update_candidate(candidate)

    _write_json(paths["status"], {
        "status": "ok",
        "release": KNOWLEDGE_RELEASE,
        "last_package_id": package_id,
        "last_report_id": None,
        "last_final_status": "PACKAGE_CREATED",
        "product_owner_approval_required": True,
        "updated_at": _now(),
    })
    return _with_workspace_markdown({
        "status": "ok",
        "render_mode": "vectra_knowledge_capitalization_package",
        "release": KNOWLEDGE_RELEASE,
        "package": package,
        "candidate": candidate,
        "write_available": True,
    }, "Knowledge Capitalization Package VECTRA", package)


def write_confirmed_knowledge(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Write an already packaged and Product Owner approved knowledge record."""
    payload = payload if isinstance(payload, dict) else {}
    paths = _paths()
    package_id = str(payload.get("package_id") or "").strip()
    package = _find_by_id(_read_list(paths["packages"]), "package_id", package_id) if package_id else None
    if not isinstance(package, dict):
        # Preserve usability: if Laboratory has approval but no package yet, create it first.
        package_response = create_capitalization_package(payload)
        package = package_response.get("package") if isinstance(package_response, dict) else None
    if not isinstance(package, dict):
        return _failed_report(None, None, "capitalization_package_required")

    candidates = _read_list(paths["candidates"])
    candidate = _find_by_id(candidates, "candidate_id", str(package.get("candidate_id")))
    if not isinstance(candidate, dict):
        return _failed_report(None, package, "candidate_not_found_or_invalid")
    if not candidate.get("product_owner_approval"):
        return _failed_report(candidate, package, "product_owner_approval_required")

    knowledge_id = str(package.get("knowledge_id") or candidate.get("knowledge_id") or f"K-{uuid.uuid4().hex[:8]}")
    knowledge_type = str(package.get("knowledge_type") or candidate.get("knowledge_type") or "professional").lower()
    domain = str(package.get("domain") or candidate.get("domain") or (get_active_business_domain().get("active_domain") or {}).get("domain_id") or "bonboason").lower()
    target_repository = package.get("target_repository") or candidate.get("target_repository")
    now = _now()

    knowledge_record = {
        "knowledge_id": knowledge_id,
        "candidate_id": candidate.get("candidate_id"),
        "package_id": package.get("package_id"),
        "capitalization_package": package.get("package_id"),
        "knowledge_type": knowledge_type,
        "type": knowledge_type,
        "title": candidate.get("title"),
        "content": candidate.get("content"),
        "description": candidate.get("content"),
        "repository_path": target_repository,
        "status": "CAPITALIZED",
        "source": candidate.get("source"),
        "created_at": now,
        "updated_at": now,
        "capitalized_at": now,
        "product_owner_approved": True,
        "professional_model_auto_update": False,
    }

    write_status = "FAILED"
    if knowledge_type == "professional":
        items = _read_list(paths["professional"])
        existing = _find_by_id(items, "knowledge_id", knowledge_id)
        if existing:
            existing.update(knowledge_record)
        else:
            items.append(knowledge_record)
        _write_json(paths["professional"], items)
        write_status = "WRITTEN"
        readback = _professional_knowledge_readback(knowledge_id)
    elif knowledge_type == "business":
        domain_path = _domain_knowledge_path(domain)
        profile_payload = get_business_domain_profile(domain)
        profile = profile_payload.get("domain_profile") if isinstance(profile_payload, dict) else None
        if not isinstance(profile, dict):
            return _failed_report(candidate, package, "target_business_domain_not_found")
        items = profile.get("business_knowledge", [])
        if not isinstance(items, list):
            items = []
        business_record = deepcopy(knowledge_record)
        business_record["domain"] = domain
        existing = _find_by_id(items, "knowledge_id", knowledge_id)
        if existing:
            existing.update(business_record)
        else:
            items.append(business_record)
        profile["business_knowledge"] = items
        profile["updated_at"] = now
        _write_json(domain_path, profile)
        write_status = "WRITTEN"
        readback = _business_knowledge_readback(domain, knowledge_id)
    else:
        return _failed_report(candidate, package, "unsupported_knowledge_type")

    if readback.get("status") != "PASS":
        return _failed_report(candidate, package, "readback_failed", write_status=write_status, readback=readback)

    recovery = create_recovery_snapshot({"metadata": {"created_by": "knowledge_capitalization", "package_id": package.get("package_id"), "knowledge_id": knowledge_id, "knowledge_type": knowledge_type}})
    recovery_snapshot_id = (recovery.get("snapshot") or {}).get("snapshot_id") if isinstance(recovery.get("snapshot"), dict) else None
    recovery_status = "RECOVERY_UPDATED" if recovery_snapshot_id else "FAILED"

    journal_entry_id = f"knowledge-capitalization-{uuid.uuid4().hex[:10]}"
    journal_entry = {
        "entry_id": journal_entry_id,
        "timestamp": now,
        "release": KNOWLEDGE_RELEASE,
        "status": "CAPITALIZED" if recovery_snapshot_id else "FAILED",
        "event_type": "knowledge_capitalization",
        "summary": f"Knowledge {knowledge_id} capitalized into {target_repository}.",
        "package_id": package.get("package_id"),
        "knowledge_id": knowledge_id,
        "knowledge_type": knowledge_type,
    }
    _append_json_list(paths["journal"], journal_entry)

    final_status = "CAPITALIZED" if recovery_snapshot_id else "FAILED"
    report_id = f"KCAP-REPORT-{uuid.uuid4().hex[:10].upper()}"
    report = {
        "report_id": report_id,
        "package_id": package.get("package_id"),
        "knowledge_id": knowledge_id,
        "knowledge_type": knowledge_type,
        "target_repository": target_repository,
        "write_status": write_status,
        "readback_status": "READBACK_PASS",
        "recovery_snapshot_status": recovery_status,
        "evolution_journal_entry_id": journal_entry_id,
        "final_status": final_status,
        "statuses_passed": ["DETECTED", "CONFIRMED_BY_PRODUCT_OWNER", "CAPITALIZATION_PACKAGE_CREATED", "WRITTEN", "READBACK_PASS", recovery_status, final_status],
        "product_owner_approval_required": True,
        "product_owner_approval_confirmed": True,
        "professional_model_auto_update": False,
        "created_at": now,
    }
    _append_json_list(paths["reports"], report)

    # Persist package terminal fields.
    packages = _read_list(paths["packages"])
    for item in packages:
        if item.get("package_id") == package.get("package_id"):
            item.update({"status": final_status, "write_status": write_status, "readback_status": "READBACK_PASS", "final_status": final_status, "report_id": report_id, "updated_at": _now()})
    _write_json(paths["packages"], packages)
    candidate["status"] = final_status
    candidate["updated_at"] = _now()
    _update_candidate(candidate)

    _write_json(paths["status"], {
        "status": "ok" if final_status == "CAPITALIZED" else "degraded",
        "release": KNOWLEDGE_RELEASE,
        "last_package_id": package.get("package_id"),
        "last_report_id": report_id,
        "last_final_status": final_status,
        "product_owner_approval_required": True,
        "updated_at": _now(),
    })
    return _with_workspace_markdown({
        "status": "ok" if final_status == "CAPITALIZED" else "degraded",
        "render_mode": "vectra_knowledge_capitalization_report",
        "release": KNOWLEDGE_RELEASE,
        "package": package,
        "report": report,
        "readback": readback,
        "recovery_snapshot": recovery,
    }, "Knowledge Capitalization VECTRA", report)

def capitalize_knowledge(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Backward-compatible one-call capitalization flow.

    FOUNDATION-0010 keeps the previous endpoint but internally runs the official
    explicit sequence: create package -> write confirmed knowledge -> report.
    """
    payload = payload if isinstance(payload, dict) else {}
    package_response = create_capitalization_package(payload)
    package = package_response.get("package") if isinstance(package_response, dict) else None
    if not isinstance(package, dict):
        return package_response
    write_payload = dict(payload)
    write_payload["package_id"] = package.get("package_id")
    return write_confirmed_knowledge(write_payload)

def _failed_report(candidate: Optional[Dict[str, Any]], package: Optional[Dict[str, Any]], reason: str, write_status: str = "FAILED", readback: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    paths = _paths()
    report_id = f"KCAP-FAILED-{uuid.uuid4().hex[:10].upper()}"
    report = {
        "report_id": report_id,
        "package_id": package.get("package_id") if isinstance(package, dict) else None,
        "knowledge_id": candidate.get("knowledge_id") if isinstance(candidate, dict) else None,
        "knowledge_type": candidate.get("knowledge_type") if isinstance(candidate, dict) else None,
        "target_repository": candidate.get("target_repository") if isinstance(candidate, dict) else None,
        "write_status": write_status,
        "readback_status": (readback or {}).get("status") or "NOT_EXECUTED",
        "recovery_snapshot_status": "NOT_EXECUTED",
        "evolution_journal_entry_id": None,
        "final_status": "FAILED",
        "failure_reason": reason,
        "product_owner_approval_required": True,
        "created_at": _now(),
    }
    _append_json_list(paths["failed_reports"], report)
    _append_json_list(paths["reports"], report)
    _write_json(paths["status"], {
        "status": "degraded",
        "release": KNOWLEDGE_RELEASE,
        "last_package_id": report.get("package_id"),
        "last_report_id": report_id,
        "last_final_status": "FAILED",
        "failure_reason": reason,
        "product_owner_approval_required": True,
        "updated_at": _now(),
    })
    return _with_workspace_markdown({"status": "failed", "render_mode": "vectra_knowledge_capitalization_failed", "release": KNOWLEDGE_RELEASE, "report": report}, "Knowledge Capitalization Failed", report)


def get_knowledge_capitalization_status() -> Dict[str, Any]:
    paths = _paths()
    status = _read_json(paths["status"], {})
    return {
        "status": "ok",
        "render_mode": "vectra_knowledge_capitalization_status",
        "release": KNOWLEDGE_RELEASE,
        "capitalization_status": status,
        "candidates_count": len(_read_list(paths["candidates"])),
        "packages_count": len(_read_list(paths["packages"])),
        "reports_count": len(_read_list(paths["reports"])),
        "professional_knowledge_count": len(_read_list(paths["professional"])),
        "allowed_statuses": KNOWLEDGE_STATUSES,
    }


def list_knowledge_capitalization_reports(limit: int = 20, include_failed: bool = True) -> Dict[str, Any]:
    paths = _paths()
    reports = _read_list(paths["reports"])
    safe_limit = min(max(int(limit or 20), 1), 100)
    if not include_failed:
        reports = [r for r in reports if r.get("final_status") != "FAILED"]
    return {
        "status": "ok",
        "render_mode": "vectra_knowledge_capitalization_reports",
        "release": KNOWLEDGE_RELEASE,
        "reports": reports[-safe_limit:],
        "reports_count": len(reports),
    }


def list_professional_knowledge() -> Dict[str, Any]:
    """Return lightweight Professional Knowledge index for Laboratory readback."""
    paths = _paths()
    items = [_normalize_professional_record(item) for item in _read_list(paths["professional"]) if isinstance(item, dict)]
    list_items = [
        {
            "knowledge_id": item.get("knowledge_id"),
            "title": item.get("title"),
            "type": item.get("type"),
            "status": item.get("status"),
            "created_at": item.get("created_at"),
            "updated_at": item.get("updated_at"),
        }
        for item in items
    ]
    last = list_items[-1] if list_items else None
    return _with_workspace_markdown({
        "status": "ok",
        "render_mode": "vectra_professional_knowledge_list",
        "release": KNOWLEDGE_RELEASE,
        "knowledge_type": "professional",
        "repository_path": "knowledge/professional_knowledge.json",
        "items_count": len(list_items),
        "knowledge": list_items,
        "last_document": last,
    }, "Professional Knowledge List VECTRA", {"knowledge": list_items})


def get_professional_knowledge(knowledge_id: Optional[str] = None) -> Dict[str, Any]:
    """Return full Professional Knowledge repository or a single document by id."""
    paths = _paths()
    raw_items = [item for item in _read_list(paths["professional"]) if isinstance(item, dict)]
    if knowledge_id:
        found = _find_by_id(raw_items, "knowledge_id", knowledge_id)
        if not isinstance(found, dict):
            return {
                "status": "not_found",
                "render_mode": "vectra_professional_knowledge_document",
                "release": KNOWLEDGE_RELEASE,
                "knowledge_id": knowledge_id,
                "exists": False,
                "repository_path": "knowledge/professional_knowledge.json",
            }
        normalized = _normalize_professional_record(found)
        return _with_workspace_markdown({
            "status": "ok",
            "render_mode": "vectra_professional_knowledge_document",
            "release": KNOWLEDGE_RELEASE,
            "exists": True,
            "knowledge": normalized,
            **normalized,
        }, "Professional Knowledge VECTRA", normalized)

    items = [_normalize_professional_record(item) for item in raw_items]
    return _with_workspace_markdown({
        "status": "ok",
        "render_mode": "vectra_professional_knowledge",
        "release": KNOWLEDGE_RELEASE,
        "knowledge_type": "professional",
        "target_repository": "knowledge/professional_knowledge.json",
        "items_count": len(items),
        "knowledge": items,
    }, "Professional Knowledge VECTRA", {"knowledge": items})


def verify_professional_knowledge_readback(knowledge_id: str) -> Dict[str, Any]:
    paths = _paths()
    raw_items = [item for item in _read_list(paths["professional"]) if isinstance(item, dict)]
    found = _find_by_id(raw_items, "knowledge_id", knowledge_id)
    exists = isinstance(found, dict)
    normalized = _normalize_professional_record(found) if exists else None
    checksum = _stable_checksum(normalized) if normalized else None
    file_checksum = _file_checksum(paths["professional"])
    verification_status = "PASS" if exists and checksum else "FAIL"
    return {
        "status": verification_status,
        "render_mode": "vectra_professional_knowledge_readback_verification",
        "release": KNOWLEDGE_RELEASE,
        "knowledge_id": knowledge_id,
        "exists": exists,
        "readback_status": "PASS" if exists else "FAIL",
        "checksum": checksum,
        "repository_file_checksum": file_checksum,
        "repository_path": "knowledge/professional_knowledge.json",
        "verification_status": verification_status,
        "professional_model_auto_update": False,
        "professional_model_changed": False,
        "knowledge": normalized,
    }


def get_professional_knowledge_overview() -> Dict[str, Any]:
    paths = _paths()
    professional_items = [item for item in _read_list(paths["professional"]) if isinstance(item, dict)]
    business_documents = 0
    base = paths["base"] / "runtime" / "business_domains"
    if base.exists():
        for profile_path in base.glob("*/domain_profile.json"):
            profile = _read_json(profile_path, {})
            if isinstance(profile, dict) and isinstance(profile.get("business_knowledge"), list):
                business_documents += len(profile.get("business_knowledge") or [])
    last_item = professional_items[-1] if professional_items else None
    last_updated = None
    if paths["professional"].exists():
        last_updated = datetime.fromtimestamp(paths["professional"].stat().st_mtime, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "status": "ok",
        "render_mode": "vectra_professional_knowledge_overview",
        "release": KNOWLEDGE_RELEASE,
        "total_documents": len(professional_items) + business_documents,
        "professional_documents": len(professional_items),
        "business_documents": business_documents,
        "last_document": _normalize_professional_record(last_item) if isinstance(last_item, dict) else None,
        "last_updated": last_updated,
        "repository_status": "readable" if paths["professional"].exists() else "missing",
        "repository_path": "knowledge/professional_knowledge.json",
    }


def get_domain_knowledge(domain: str = "bonboason") -> Dict[str, Any]:
    domain_key = _slug(domain or "bonboason", "bonboason")
    profile_payload = get_business_domain_profile(domain_key)
    profile = profile_payload.get("domain_profile") if isinstance(profile_payload, dict) else None
    items = profile.get("business_knowledge", []) if isinstance(profile, dict) else []
    return _with_workspace_markdown({
        "status": "ok" if isinstance(profile, dict) else "not_found",
        "render_mode": "vectra_domain_knowledge",
        "release": KNOWLEDGE_RELEASE,
        "knowledge_type": "business",
        "domain": domain_key,
        "items_count": len(items) if isinstance(items, list) else 0,
        "knowledge": items if isinstance(items, list) else [],
    }, f"Business Knowledge Domain {domain_key}", {"knowledge": items if isinstance(items, list) else []})


def verify_knowledge_capitalization() -> Dict[str, Any]:
    status = get_knowledge_capitalization_status()
    professional = get_professional_knowledge()
    reports = list_knowledge_capitalization_reports(limit=5)
    latest_report = (reports.get("reports") or [])[-1] if reports.get("reports") else None
    checks = {
        "runtime_ready": status.get("status") == "ok",
        "product_owner_approval_required": True,
        "professional_repository_readable": professional.get("status") == "ok",
        "reports_repository_readable": reports.get("status") == "ok",
        "latest_capitalized_report_available": bool(latest_report and latest_report.get("final_status") == "CAPITALIZED"),
    }
    return {
        "status": "PASS" if all(v is True for v in checks.values()) else "DEGRADED",
        "render_mode": "vectra_knowledge_capitalization_verify",
        "release": KNOWLEDGE_RELEASE,
        "checks": checks,
        "latest_report": latest_report,
        "professional_knowledge_items_count": professional.get("items_count"),
        "allowed_statuses": KNOWLEDGE_STATUSES,
    }
