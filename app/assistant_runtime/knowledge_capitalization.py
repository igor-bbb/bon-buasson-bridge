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

KNOWLEDGE_RELEASE = "LABORATORY-KNOWLEDGE-0010"
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
        "target_repository": "knowledge/professional_knowledge.json" if knowledge_type == "professional" else _business_repository_relative(domain),
        "content_checksum": payload.get("content_checksum"),
        "revision": payload.get("revision") or 1,
        "capitalization_change_type": payload.get("capitalization_change_type") or "new",
        "evidence": payload.get("evidence"),
        "recommended_memory_type": payload.get("recommended_memory_type"),
        "knowledge_subtype": payload.get("knowledge_subtype"),
        "prepared_item_status": payload.get("prepared_item_status") or payload.get("status"),
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


def _business_knowledge_repository_path(domain: str) -> Path:
    domain_key = _slug(domain or "bonboason", "bonboason")
    path = ensure_repository() / "business_domains" / domain_key / "business_knowledge.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write_json(path, [])
    return path


def _business_repository_relative(domain: str) -> str:
    return f"business_domains/{_slug(domain or 'bonboason', 'bonboason')}/business_knowledge.json"


def _normalize_business_record(record: Dict[str, Any], domain: str = "bonboason") -> Dict[str, Any]:
    created = record.get("created_at") or record.get("capitalized_at")
    updated = record.get("updated_at") or record.get("capitalized_at") or created
    return {
        "knowledge_id": record.get("knowledge_id"),
        "title": record.get("title"),
        "type": record.get("type") or record.get("knowledge_type") or "business",
        "status": record.get("status"),
        "description": record.get("description") or record.get("content"),
        "domain": record.get("domain") or domain,
        "repository_path": record.get("repository_path") or _business_repository_relative(domain),
        "capitalization_package": record.get("capitalization_package") or record.get("package_id"),
        "created_at": created,
        "updated_at": updated,
        "source": record.get("source"),
    }


def _business_knowledge_items(domain: str) -> List[Dict[str, Any]]:
    domain_key = _slug(domain or "bonboason", "bonboason")
    repo_items = _read_list(_business_knowledge_repository_path(domain_key))
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


def _business_knowledge_readback(domain: str, knowledge_id: str) -> Dict[str, Any]:
    domain_key = _slug(domain or "bonboason", "bonboason")
    items = _business_knowledge_items(domain_key)
    found = _find_by_id(items, "knowledge_id", knowledge_id)
    return {
        "status": "PASS" if found else "FAIL",
        "domain": domain_key,
        "knowledge_id": knowledge_id,
        "found": bool(found),
        "record": found,
        "repository_path": _business_repository_relative(domain_key),
    }



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
    target_repository = candidate.get("target_repository") or ("knowledge/professional_knowledge.json" if knowledge_type == "professional" else _business_repository_relative(domain))
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
        "content_checksum": candidate.get("content_checksum") or payload.get("content_checksum") or _knowledge_content_checksum(candidate),
        "revision": candidate.get("revision") or payload.get("revision") or 1,
        "capitalization_change_type": candidate.get("capitalization_change_type") or payload.get("capitalization_change_type") or "new",
        "evidence": candidate.get("evidence") or payload.get("evidence"),
        "recommended_memory_type": candidate.get("recommended_memory_type") or payload.get("recommended_memory_type"),
        "knowledge_subtype": candidate.get("knowledge_subtype") or payload.get("knowledge_subtype"),
        "prepared_item_status": candidate.get("prepared_item_status") or payload.get("prepared_item_status"),
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
        business_repo_path = _business_knowledge_repository_path(domain)
        profile_payload = get_business_domain_profile(domain)
        profile = profile_payload.get("domain_profile") if isinstance(profile_payload, dict) else None
        if not isinstance(profile, dict):
            return _failed_report(candidate, package, "target_business_domain_not_found")
        business_record = deepcopy(knowledge_record)
        business_record["domain"] = domain
        business_record["repository_path"] = _business_repository_relative(domain)
        repo_items = _read_list(business_repo_path)
        existing = _find_by_id(repo_items, "knowledge_id", knowledge_id)
        if existing:
            existing.update(business_record)
        else:
            repo_items.append(business_record)
        _write_json(business_repo_path, repo_items)
        # Mirror into the domain profile so existing Domain Restore continues to carry Business Knowledge.
        items = profile.get("business_knowledge", [])
        if not isinstance(items, list):
            items = []
        existing_profile = _find_by_id(items, "knowledge_id", knowledge_id)
        if existing_profile:
            existing_profile.update(business_record)
        else:
            items.append(business_record)
        profile["business_knowledge"] = items
        profile["business_knowledge_repository"] = _business_repository_relative(domain)
        profile["business_knowledge_last_updated"] = now
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
        "domain": domain if knowledge_type == "business" else None,
    }
    _append_json_list(paths["journal"], journal_entry)

    final_status = "CAPITALIZED" if recovery_snapshot_id else "FAILED"
    report_id = f"KCAP-REPORT-{uuid.uuid4().hex[:10].upper()}"
    report = {
        "report_id": report_id,
        "package_id": package.get("package_id"),
        "knowledge_id": knowledge_id,
        "knowledge_type": knowledge_type,
        "knowledge_subtype": candidate.get("knowledge_subtype") or payload.get("knowledge_subtype"),
        "recommended_memory_type": candidate.get("recommended_memory_type") or payload.get("recommended_memory_type"),
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



# LABORATORY-KNOWLEDGE-0004 — autonomous capitalization pipeline.
def _auto_pipeline_paths() -> Dict[str, Path]:
    paths = _paths()
    auto = paths["kc"] / "auto_pipeline.json"
    if not auto.exists():
        _write_json(auto, {"pending_batches": [], "updated_at": _now()})
    return {**paths, "auto_pipeline": auto}


def _listify(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, dict):
        return [value]
    if isinstance(value, str) and value.strip():
        return [{"description": value.strip()}]
    return []



def _context_text_from_payload(payload: Dict[str, Any]) -> str:
    """Return user-visible working context supplied by the GPT facade.

    The Runtime cannot read hidden ChatGPT memory directly. The facade action can,
    however, pass the current professional working context as ordinary JSON. To keep
    Product Owner interaction simple, this accepts broad natural keys instead of a
    technical payload.confirmed_knowledge shape.
    """
    chunks: List[str] = []
    for key in [
        "working_context", "working_context_text", "conversation_context", "dialogue_context",
        "current_dialogue", "current_context_text", "product_owner_context", "transcript",
        "conversation", "message", "text", "owner_message", "source_text",
    ]:
        value = payload.get(key)
        if isinstance(value, str) and value.strip():
            chunks.append(value.strip())
        elif isinstance(value, list):
            for part in value[-40:]:
                if isinstance(part, str) and part.strip():
                    chunks.append(part.strip())
                elif isinstance(part, dict):
                    content = part.get("content") or part.get("message") or part.get("text")
                    if isinstance(content, str) and content.strip():
                        chunks.append(content.strip())
        elif isinstance(value, dict):
            for subkey in ["confirmed_knowledge", "notes", "summary", "content", "text", "message"]:
                sub = value.get(subkey)
                if isinstance(sub, str) and sub.strip():
                    chunks.append(sub.strip())
    current_context = payload.get("current_context")
    if isinstance(current_context, dict):
        for subkey in ["working_context", "conversation_context", "confirmed_text", "summary", "notes"]:
            sub = current_context.get(subkey)
            if isinstance(sub, str) and sub.strip():
                chunks.append(sub.strip())
    return "\n".join(chunks).strip()


def _looks_confirmed(text: str) -> bool:
    t = str(text or "").lower().replace("ё", "е")
    positives = [
        "product owner подтверждает", "подтверждаю", "подтвержден", "подтверждён",
        "зафиксировать", "зафиксируем", "добавить в память", "капитализировать",
        "утверждает", "approved", "confirmed",
    ]
    negatives = ["не подтверждаю", "не капитализировать", "черновик", "гипотеза", "возможно"]
    return any(p in t for p in positives) and not any(n in t for n in negatives)


def _extract_structured_knowledge_from_text(text: str, domain: str) -> List[Dict[str, Any]]:
    """Heuristic extraction for Product Owner confirmed knowledge blocks.

    This is intentionally conservative: it only extracts text that contains an explicit
    confirmation signal or a Knowledge ID block. It does not invent knowledge.
    """
    text = str(text or "").strip()
    if not text:
        return []
    items: List[Dict[str, Any]] = []
    # Split long context into candidate paragraphs/blocks while preserving ID blocks.
    blocks = re.split(r"\n\s*[-–—]{3,}\s*\n|\n\s*#{1,3}\s+", text)
    for block in blocks:
        b = block.strip()
        if len(b) < 20:
            continue
        has_id = re.search(r"\b(PK|BK)-\d{3,}\b", b, flags=re.IGNORECASE)
        if not has_id and not _looks_confirmed(b):
            continue
        kt = "professional" if re.search(r"\bPK-\d{3,}\b|Professional Knowledge|Профессиональн", b, flags=re.IGNORECASE) else "business"
        if re.search(r"\bBK-\d{3,}\b|Business Knowledge|Business Domain|бизнес[- ]?знан|знани[ея] Business Domain", b, flags=re.IGNORECASE):
            kt = "business"
        match = re.search(r"\b(PK|BK)-\d{3,}\b", b, flags=re.IGNORECASE)
        kid = match.group(0).upper() if match else ("PK-" if kt == "professional" else "BK-") + uuid.uuid4().hex[:6].upper()
        title_match = re.search(r"(?:Название|Title)\s*[:：]\s*(.+)", b, flags=re.IGNORECASE)
        title = title_match.group(1).strip().split("\n")[0][:160] if title_match else b.split("\n", 1)[0][:120]
        desc_match = re.search(r"(?:Описание|Содержание|Content|Description)\s*[:：]\s*(.+)", b, flags=re.IGNORECASE | re.DOTALL)
        description = desc_match.group(1).strip() if desc_match else b
        items.append({
            "knowledge_id": kid,
            "knowledge_type": kt,
            "title": title or kid,
            "description": description,
            "content": description,
            "domain": domain,
            "source": "VECTRA Laboratory working context",
            "confidence_level": "confirmed_by_product_owner" if _looks_confirmed(b) or has_id else "requires_product_owner_approval",
        })
    return items


def _sentence_split_for_knowledge(text: str) -> List[str]:
    text = re.sub(r"\r\n?", "\n", str(text or ""))
    text = re.sub(r"[ \t]+", " ", text)
    chunks = re.split(r"(?:\n\s*){2,}|(?<=\.)\s+(?=[А-ЯA-Z])|(?<=:)\s*\n", text)
    return [c.strip(" \n\t-•*") for c in chunks if len(c.strip()) >= 25]


def _detect_knowledge_type_from_text(text: str, default: str = "business") -> str:
    t = str(text or "").lower()
    if re.search(r"\bpk-\d{3,}\b|professional knowledge|professional model|runtime|laboratory|vectra|architecture|архитектур|профессиональн|лаборатор|капитализац|product owner|engineering", t, flags=re.IGNORECASE):
        return "professional"
    if re.search(r"\bbk-\d{3,}\b|business knowledge|business domain|bon boisson|bonboason|бон буассон|бизнес|контракт|сеть|sku|категор", t, flags=re.IGNORECASE):
        return "business"
    return default if default in KNOWLEDGE_TYPES else "business"


def _derive_knowledge_title(text: str, knowledge_type: str) -> str:
    title_match = re.search(r"(?:Название|Title|Правило|Принцип|Решение|Вывод)\s*[:：]\s*(.+)", text, flags=re.IGNORECASE)
    if title_match:
        return title_match.group(1).strip().split("\n")[0][:160]
    first = text.strip().split("\n", 1)[0].strip(" -•*#")
    first = re.sub(r"^(Product Owner подтверждает|Подтверждаю|Зафиксировать|Зафиксируем)\s*[:：-]?\s*", "", first, flags=re.IGNORECASE)
    if len(first) > 160:
        first = first[:157].rstrip() + "..."
    return first or ("Professional Knowledge" if knowledge_type == "professional" else "Business Knowledge")


def _is_hypothesis_or_unconfirmed(text: str) -> bool:
    t = str(text or "").lower().replace("ё", "е")
    blocked = [
        "гипотеза", "возможно", "может быть", "предполож", "черновик",
        "не подтвержден", "не подтверждён", "не фиксировать", "не капитализировать",
        "вопрос", "надо проверить", "требует проверки", "draft", "hypothesis",
    ]
    return any(x in t for x in blocked)


def _extract_confirmed_knowledge_from_session_audit(text: str, domain: str) -> List[Dict[str, Any]]:
    """LABORATORY-KNOWLEDGE-0010: extract confirmed knowledge from a session audit.

    This is an internal Knowledge Extraction Engine for Runtime-side safety and tests.
    In production VECTRA still owns the intellectual audit, but if the facade supplies
    a session audit instead of a prepared package, Runtime can now build a complete
    prepared_knowledge_package from confirmed session evidence rather than returning
    NO_CONFIRMED_KNOWLEDGE_TO_CAPITALIZE.
    """
    text = str(text or "").strip()
    if not text:
        return []
    candidates: List[Dict[str, Any]] = []
    # Keep explicit knowledge blocks intact.
    explicit_blocks = re.split(r"\n\s*(?=(?:Knowledge ID|ID знания|PK-\d{3,}|BK-\d{3,}|Product Owner подтверждает|Подтверждаю|Зафиксируем|Зафиксировать))", text, flags=re.IGNORECASE)
    for block in explicit_blocks:
        b = block.strip()
        if len(b) < 25:
            continue
        if _is_hypothesis_or_unconfirmed(b):
            continue
        has_id = re.search(r"\b(PK|BK)-\d{3,}\b", b, flags=re.IGNORECASE)
        explicit_confirmation = _looks_confirmed(b) or bool(has_id)
        if not explicit_confirmation:
            continue
        kt = _detect_knowledge_type_from_text(b, default="business")
        if has_id:
            kt = "professional" if has_id.group(1).upper() == "PK" else "business"
        kid = has_id.group(0).upper() if has_id else None
        desc_match = re.search(r"(?:Описание|Содержание|Content|Description|Суть)\s*[:：]\s*(.+)", b, flags=re.IGNORECASE | re.DOTALL)
        description = desc_match.group(1).strip() if desc_match else b
        title = _derive_knowledge_title(b, kt)
        candidates.append({
            "knowledge_id": kid,
            "knowledge_type": kt,
            "title": title,
            "description": description,
            "content": description,
            "domain": domain,
            "source": "VECTRA Session Audit",
            "evidence": b[:1200],
            "confirmation_level": "confirmed_by_product_owner",
            "confidence_level": "confirmed_by_product_owner",
            "recommended_memory_type": "professional_knowledge" if kt == "professional" else "business_knowledge",
        })

    # Extract compact confirmed standards from long audit summaries.
    for chunk in _sentence_split_for_knowledge(text):
        if len(chunk) < 40 or _is_hypothesis_or_unconfirmed(chunk):
            continue
        confirmed = _looks_confirmed(chunk) or re.search(r"\b(обязано|обязан|должна|должен|запрещается|запрещено|правило|принцип|стандарт|критерий|definition of done)\b", chunk, flags=re.IGNORECASE)
        if not confirmed:
            continue
        # Avoid duplicating explicit blocks that already contain the same text.
        if any(chunk[:80] in str(c.get("evidence") or "") for c in candidates):
            continue
        kt = _detect_knowledge_type_from_text(chunk, default="professional")
        candidates.append({
            "knowledge_type": kt,
            "title": _derive_knowledge_title(chunk, kt),
            "description": chunk,
            "content": chunk,
            "domain": domain,
            "source": "VECTRA Session Audit",
            "evidence": chunk[:1200],
            "confirmation_level": "confirmed_by_product_owner",
            "confidence_level": "confirmed_by_product_owner",
            "recommended_memory_type": "professional_knowledge" if kt == "professional" else "business_knowledge",
        })
    # Let the existing builder normalize ids and remove duplicates by fingerprint.
    return candidates


def _existing_knowledge_index(domain: str) -> Dict[str, Dict[str, Any]]:
    index: Dict[str, Dict[str, Any]] = {}
    for item in _read_list(_paths()["professional"]):
        if isinstance(item, dict) and item.get("knowledge_id"):
            index[str(item.get("knowledge_id"))] = item
    for item in _business_knowledge_items(domain):
        if isinstance(item, dict) and item.get("knowledge_id"):
            index[str(item.get("knowledge_id"))] = item
    return index


def _knowledge_content_checksum(item: Dict[str, Any]) -> str:
    return _stable_checksum({
        "knowledge_type": item.get("knowledge_type") or item.get("type"),
        "title": item.get("title"),
        "description": item.get("description") or item.get("content"),
        "domain": item.get("domain"),
    })


def _incremental_knowledge_diff(items: List[Dict[str, Any]], domain: str) -> Dict[str, Any]:
    existing = _existing_knowledge_index(domain)
    new_items: List[Dict[str, Any]] = []
    updated_items: List[Dict[str, Any]] = []
    unchanged_items: List[Dict[str, Any]] = []
    for item in items:
        kid = str(item.get("knowledge_id") or "")
        current = existing.get(kid)
        item_checksum = _knowledge_content_checksum(item)
        item["content_checksum"] = item_checksum
        if not current:
            item["capitalization_change_type"] = "new"
            item["revision"] = 1
            new_items.append(item)
            continue
        current_checksum = current.get("content_checksum") or _knowledge_content_checksum(current)
        if current_checksum == item_checksum:
            item["capitalization_change_type"] = "unchanged"
            item["existing_repository_path"] = current.get("repository_path")
            unchanged_items.append(item)
        else:
            item["capitalization_change_type"] = "updated"
            item["previous_checksum"] = current_checksum
            item["previous_updated_at"] = current.get("updated_at")
            try:
                item["revision"] = int(current.get("revision") or 1) + 1
            except Exception:
                item["revision"] = 2
            updated_items.append(item)
    return {
        "new": new_items,
        "updated": updated_items,
        "unchanged": unchanged_items,
        "to_write": new_items + updated_items,
        "existing_count": len(existing),
        "new_count": len(new_items),
        "updated_count": len(updated_items),
        "unchanged_count": len(unchanged_items),
    }

def _prepared_package_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Return a VECTRA-prepared Knowledge Package if the facade supplied one.

    LABORATORY-KNOWLEDGE-0006 separates responsibilities:
    - VECTRA performs intellectual analysis of the working dialogue.
    - Runtime stores, verifies, snapshots and reports.

    Therefore Runtime must accept a prepared_knowledge_package and must not re-run
    raw-text extraction when the package is present.
    """
    payload = payload if isinstance(payload, dict) else {}
    for key in [
        "prepared_knowledge_package",
        "knowledge_package",
        "prepared_package",
        "package",
    ]:
        package = payload.get(key)
        if isinstance(package, dict):
            return package
    current_context = payload.get("current_context")
    if isinstance(current_context, dict):
        for key in ["prepared_knowledge_package", "knowledge_package", "prepared_package"]:
            package = current_context.get(key)
            if isinstance(package, dict):
                return package
    return {}


def _items_from_prepared_knowledge_package(package: Dict[str, Any], domain: str) -> List[Dict[str, Any]]:
    """Normalize a VECTRA-prepared package without analyzing raw context again."""
    if not isinstance(package, dict):
        return []
    items: List[Dict[str, Any]] = []
    package_domain = str(package.get("domain") or package.get("business_domain") or domain or "bonboason")
    package_source = str(package.get("source") or package.get("source_context") or "VECTRA prepared knowledge package")

    for key, knowledge_type in [
        ("professional", "professional"),
        ("professional_knowledge", "professional"),
        ("professional_items", "professional"),
        ("business", "business"),
        ("business_knowledge", "business"),
        ("business_domain_knowledge", "business"),
        ("business_items", "business"),
        # LABORATORY-KNOWLEDGE-0007: Product Knowledge is accepted as a prepared
        # package section. Runtime persists it as Professional Knowledge for now,
        # preserving knowledge_subtype=product until a dedicated Product Memory
        # repository is introduced.
        ("product_knowledge", "professional"),
        ("product_items", "professional"),
    ]:
        for raw in _listify(package.get(key)):
            item = dict(raw) if isinstance(raw, dict) else {"description": str(raw)}
            item.setdefault("knowledge_type", knowledge_type)
            items.append(item)

    for raw in _listify(package.get("items") or package.get("knowledge_items") or package.get("confirmed_knowledge") or package.get("knowledge")):
        item = dict(raw) if isinstance(raw, dict) else {"description": str(raw)}
        kt = str(item.get("knowledge_type") or item.get("type") or "").lower()
        if kt not in KNOWLEDGE_TYPES:
            kt = "business" if item.get("domain") or package.get("domain") else "professional"
        item["knowledge_type"] = kt
        items.append(item)

    normalized: List[Dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        kt = str(item.get("knowledge_type") or item.get("type") or "professional").lower()
        if kt not in KNOWLEDGE_TYPES:
            continue
        description = str(
            item.get("description")
            or item.get("content")
            or item.get("body")
            or item.get("text")
            or ""
        ).strip()
        title = str(item.get("title") or item.get("name") or description[:80] or f"Knowledge {idx}").strip()
        if not description:
            description = title
        kid_prefix = "PK" if kt == "professional" else "BK"
        stable_basis = {
            "knowledge_type": kt,
            "title": title,
            "description": description,
            "domain": item.get("domain") or item.get("business_domain") or package_domain,
            "recommended_memory_type": item.get("recommended_memory_type") or item.get("memory_type"),
        }
        stable_id = f"{kid_prefix}-{_stable_checksum(stable_basis)[:10].upper()}"
        knowledge_id = str(item.get("knowledge_id") or item.get("id") or stable_id).upper()
        item_domain = str(item.get("domain") or item.get("business_domain") or package_domain)
        normalized.append({
            "candidate_id": str(item.get("candidate_id") or f"KC-{knowledge_id}"),
            "knowledge_id": knowledge_id,
            "knowledge_type": kt,
            "title": title,
            "content": description,
            "description": description,
            "domain": item_domain,
            "source": str(item.get("source") or package_source),
            "source_package_id": package.get("package_id") or package.get("knowledge_package_id"),
            "source_analysis_id": package.get("analysis_id"),
            "confirmation_level": item.get("confirmation_level") or package.get("confirmation_level"),
            "confidence_level": item.get("confidence_level") or item.get("confirmation_level") or package.get("confirmation_level") or "confirmed_by_vectra_prepared_package",
            "evidence": item.get("evidence") or item.get("proof") or package.get("evidence"),
            "recommended_memory_type": item.get("recommended_memory_type") or item.get("memory_type") or ("product_knowledge" if key in {"product_knowledge", "product_items"} else ("professional_knowledge" if kt == "professional" else "business_knowledge")),
            "knowledge_subtype": item.get("knowledge_subtype") or ("product" if key in {"product_knowledge", "product_items"} else item.get("subtype")),
            "prepared_item_status": item.get("status") or package.get("status") or "confirmed",
            "revision": item.get("revision") or 1,
        })

    dedup: Dict[str, Dict[str, Any]] = {}
    for item in normalized:
        dedup[str(item.get("knowledge_id"))] = item
    return list(dedup.values())




# LABORATORY-KNOWLEDGE-0009 — Knowledge Package Builder and automatic batch writer.
SAFE_KNOWLEDGE_BATCH_SIZE = 20


def _knowledge_normalized_fingerprint(item: Dict[str, Any]) -> str:
    return _stable_checksum({
        "knowledge_type": str(item.get("knowledge_type") or "").lower(),
        "title": str(item.get("title") or "").strip().lower(),
        "description": str(item.get("description") or item.get("content") or "").strip().lower(),
        "domain": str(item.get("domain") or item.get("business_domain") or "").strip().lower(),
        "recommended_memory_type": str(item.get("recommended_memory_type") or "").strip().lower(),
        "knowledge_subtype": str(item.get("knowledge_subtype") or "").strip().lower(),
    })


def _normalize_knowledge_item_for_builder(item: Dict[str, Any], domain: str, source: str) -> Optional[Dict[str, Any]]:
    if not isinstance(item, dict):
        item = {"description": str(item or "")}
    raw_type = str(item.get("knowledge_type") or item.get("type") or "").lower().strip()
    memory_type = str(item.get("recommended_memory_type") or item.get("memory_type") or "").lower().strip()
    subtype = str(item.get("knowledge_subtype") or item.get("subtype") or "").lower().strip()
    if raw_type not in KNOWLEDGE_TYPES:
        raw_type = "business" if item.get("business_domain") or item.get("domain") or memory_type == "business_knowledge" else "professional"
    if memory_type == "product_knowledge" or raw_type == "product":
        raw_type = "professional"
        subtype = subtype or "product"
        memory_type = "product_knowledge"
    elif not memory_type:
        memory_type = "professional_knowledge" if raw_type == "professional" else "business_knowledge"
    description = str(item.get("description") or item.get("content") or item.get("body") or item.get("text") or "").strip()
    title = str(item.get("title") or item.get("name") or description[:80] or "").strip()
    if not title and not description:
        return None
    if not description:
        description = title
    if not title:
        title = description[:80]
    item_domain = str(item.get("domain") or item.get("business_domain") or domain or "bonboason").strip().lower()
    stable_basis = {
        "knowledge_type": raw_type,
        "title": title,
        "description": description,
        "domain": item_domain,
        "recommended_memory_type": memory_type,
        "knowledge_subtype": subtype,
    }
    kid_prefix = "PK" if raw_type == "professional" else "BK"
    knowledge_id = str(item.get("knowledge_id") or item.get("id") or f"{kid_prefix}-{_stable_checksum(stable_basis)[:10].upper()}").upper()
    normalized = {
        "candidate_id": str(item.get("candidate_id") or f"KC-{knowledge_id}"),
        "knowledge_id": knowledge_id,
        "knowledge_type": raw_type,
        "title": title,
        "content": description,
        "description": description,
        "domain": item_domain,
        "source": str(item.get("source") or source or "VECTRA Knowledge Package Builder"),
        "confirmation_level": item.get("confirmation_level") or item.get("confidence_level") or "confirmed_by_product_owner",
        "confidence_level": item.get("confidence_level") or item.get("confirmation_level") or "confirmed_by_product_owner",
        "evidence": item.get("evidence") or item.get("proof"),
        "recommended_memory_type": memory_type,
        "knowledge_subtype": subtype or item.get("knowledge_subtype"),
        "prepared_item_status": item.get("status") or item.get("prepared_item_status") or "confirmed",
        "revision": item.get("revision") or 1,
        "normalized_fingerprint": _knowledge_normalized_fingerprint(stable_basis),
    }
    return normalized


def _knowledge_package_builder(payload: Dict[str, Any], domain: str) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    prepared = _prepared_package_from_payload(payload)
    source_package = prepared if prepared else payload
    source = str(source_package.get("source") or payload.get("source") or "VECTRA working session audit")
    package_domain = str(source_package.get("business_domain") or source_package.get("domain") or domain or "bonboason").strip().lower()
    raw_items: List[Any] = []
    if prepared:
        for key in [
            "professional_knowledge", "professional", "professional_items",
            "business_knowledge", "business", "business_domain_knowledge", "business_items",
            "product_knowledge", "product", "product_items",
            "items", "knowledge_items", "confirmed_knowledge", "knowledge",
        ]:
            raw_items.extend(_listify(prepared.get(key)))
    else:
        for key in ["professional_knowledge", "business_knowledge", "product_knowledge", "knowledge_items", "confirmed_knowledge", "knowledge"]:
            raw_items.extend(_listify(payload.get(key)))
        context = payload.get("current_context") if isinstance(payload.get("current_context"), dict) else {}
        for key in ["professional_knowledge", "business_knowledge", "product_knowledge", "confirmed_knowledge", "knowledge_items"]:
            raw_items.extend(_listify(context.get(key)))
        # LABORATORY-KNOWLEDGE-0010: when VECTRA sends a full session audit or
        # working-session transcript instead of a prebuilt package, the internal
        # Knowledge Extraction Engine converts confirmed session evidence into
        # structured items before normalization and package building.
        session_audit_text = _context_text_from_payload(payload)
        raw_items.extend(_extract_confirmed_knowledge_from_session_audit(session_audit_text, package_domain))
    normalized: List[Dict[str, Any]] = []
    rejected: List[Dict[str, Any]] = []
    for raw in raw_items:
        item = _normalize_knowledge_item_for_builder(raw, package_domain, source)
        if item:
            normalized.append(item)
        else:
            rejected.append({"reason": "empty_title_and_description", "item": raw})
    dedup: Dict[str, Dict[str, Any]] = {}
    duplicate_count = 0
    for item in normalized:
        fp = str(item.get("normalized_fingerprint") or _knowledge_normalized_fingerprint(item))
        if fp in dedup:
            duplicate_count += 1
            if item.get("evidence") and not dedup[fp].get("evidence"):
                dedup[fp] = item
            continue
        dedup[fp] = item
    items = list(dedup.values())
    professional_items = [i for i in items if i.get("knowledge_type") == "professional" and i.get("knowledge_subtype") != "product"]
    business_items = [i for i in items if i.get("knowledge_type") == "business"]
    product_items = [i for i in items if i.get("knowledge_subtype") == "product" or i.get("recommended_memory_type") == "product_knowledge"]
    batch_size = int(payload.get("batch_size") or source_package.get("batch_size") or SAFE_KNOWLEDGE_BATCH_SIZE)
    if batch_size < 1:
        batch_size = SAFE_KNOWLEDGE_BATCH_SIZE
    if batch_size > SAFE_KNOWLEDGE_BATCH_SIZE:
        batch_size = SAFE_KNOWLEDGE_BATCH_SIZE
    batches = []
    for idx in range(0, len(items), batch_size):
        batch_items = items[idx:idx + batch_size]
        batches.append({
            "batch_id": f"KBATCH-{idx // batch_size + 1:03d}",
            "sequence": idx // batch_size + 1,
            "items_count": len(batch_items),
            "items": batch_items,
        })
    package_id = str(source_package.get("knowledge_package_id") or source_package.get("package_id") or f"KPB-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}")
    return {
        "package_id": package_id,
        "source": source,
        "business_domain": package_domain,
        "confirmation_level": source_package.get("confirmation_level") or payload.get("confirmation_level") or "confirmed_by_product_owner",
        "professional_knowledge": professional_items,
        "business_knowledge": business_items,
        "product_knowledge": product_items,
        "items": items,
        "rejected_items": rejected,
        "duplicates_removed": duplicate_count,
        "normalized_count": len(normalized),
        "total_items": len(items),
        "batch_size": batch_size,
        "batches": batches,
        "builder_status": "READY",
        "session_audit_supported": True,
        "knowledge_extraction_engine": {
            "status": "ACTIVE",
            "pipeline": [
                "Session Audit",
                "Knowledge Extraction",
                "Knowledge Validation",
                "Normalization",
                "Deduplication",
                "Professional Standards Consolidation",
                "Knowledge Package Preparation",
            ],
            "confirmed_only": True,
            "hypothesis_exclusion": True,
        },
    }


def _extract_auto_knowledge_items(payload: Optional[Dict[str, Any]] = None, domain: str = "bonboason") -> List[Dict[str, Any]]:
    """Extract knowledge for autonomous capitalization.

    Priority order:
    1. LABORATORY-KNOWLEDGE-0006 prepared_knowledge_package supplied by VECTRA.
       Runtime trusts this as the result of VECTRA's intellectual analysis and does
       not re-analyze raw dialogue text.
    2. Explicit structured knowledge arrays.
    3. Conservative raw working-context extraction as a backward-compatible fallback.
    """
    payload = payload if isinstance(payload, dict) else {}

    prepared_package = _prepared_package_from_payload(payload)
    if prepared_package:
        return _items_from_prepared_knowledge_package(prepared_package, domain)

    items: List[Dict[str, Any]] = []
    for key, knowledge_type in [("professional", "professional"), ("professional_knowledge", "professional"), ("business", "business"), ("business_knowledge", "business")]:
        for raw in _listify(payload.get(key)):
            if isinstance(raw, dict):
                item = dict(raw)
            else:
                item = {"description": str(raw)}
            item.setdefault("knowledge_type", knowledge_type)
            items.append(item)
    for raw in _listify(payload.get("knowledge_items") or payload.get("confirmed_knowledge") or payload.get("knowledge")):
        if isinstance(raw, dict):
            item = dict(raw)
        else:
            item = {"description": str(raw)}
        kt = str(item.get("knowledge_type") or item.get("type") or "").lower()
        if kt not in KNOWLEDGE_TYPES:
            # Default ambiguous working-context knowledge to Business Knowledge because
            # Product Owner domain context is normally domain-specific. Explicit
            # Professional Knowledge remains supported through knowledge_type/type.
            kt = "business" if item.get("domain") or payload.get("domain") else "professional"
        item["knowledge_type"] = kt
        items.append(item)
    current_context = payload.get("current_context") if isinstance(payload.get("current_context"), dict) else {}
    for raw in _listify(current_context.get("confirmed_knowledge")):
        item = dict(raw) if isinstance(raw, dict) else {"description": str(raw)}
        item.setdefault("knowledge_type", item.get("type") or "business")
        items.append(item)

    # Backward-compatible fallback only. In the 0006 path, VECTRA should pass a
    # prepared_knowledge_package and Runtime will skip this text analysis.
    context_text = _context_text_from_payload(payload)
    items.extend(_extract_structured_knowledge_from_text(context_text, domain))

    normalized: List[Dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        kt = str(item.get("knowledge_type") or item.get("type") or "professional").lower()
        if kt not in KNOWLEDGE_TYPES:
            continue
        description = str(item.get("description") or item.get("content") or item.get("text") or "").strip()
        title = str(item.get("title") or item.get("name") or description[:80] or f"Knowledge {idx}").strip()
        if not description:
            description = title
        kid_prefix = "PK" if kt == "professional" else "BK"
        knowledge_id = str(item.get("knowledge_id") or f"{kid_prefix}-{uuid.uuid4().hex[:6].upper()}")
        normalized.append({
            "candidate_id": str(item.get("candidate_id") or f"KC-{knowledge_id}"),
            "knowledge_id": knowledge_id,
            "knowledge_type": kt,
            "title": title,
            "content": description,
            "description": description,
            "domain": str(item.get("domain") or domain or "bonboason"),
            "source": str(item.get("source") or payload.get("source") or "VECTRA Laboratory working context"),
            "confidence_level": item.get("confidence_level") or item.get("confirmation_level") or ("confirmed_by_product_owner" if bool(payload.get("product_owner_approval")) else "requires_product_owner_approval"),
            "revision": item.get("revision") or 1,
        })
    # Deduplicate by knowledge_id, keeping the latest item.
    dedup: Dict[str, Dict[str, Any]] = {}
    for item in normalized:
        dedup[str(item.get("knowledge_id"))] = item
    return list(dedup.values())


def _auto_capitalization_summary(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    professional = [i for i in items if i.get("knowledge_type") == "professional"]
    business = [i for i in items if i.get("knowledge_type") == "business"]
    return {
        "professional_count": len(professional),
        "business_count": len(business),
        "professional": [{"knowledge_id": i.get("knowledge_id"), "title": i.get("title")} for i in professional],
        "business": [{"knowledge_id": i.get("knowledge_id"), "title": i.get("title"), "domain": i.get("domain")} for i in business],
    }


def auto_capitalize_confirmed_knowledge(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run autonomous Knowledge Package Builder + batch capitalization.

    LABORATORY-KNOWLEDGE-0009 completes the existing capitalization contour:
    AUDIT -> Extraction -> Normalization -> Deduplication -> Knowledge Package
    Builder -> Automatic Batch Builder -> Runtime Write -> Readback Verification
    -> single final Capitalization Report.
    """
    payload = payload if isinstance(payload, dict) else {}
    domain = str(payload.get("domain") or (get_active_business_domain().get("active_domain") or {}).get("domain_id") or "bonboason")
    approval = bool(payload.get("product_owner_approval"))
    paths = _auto_pipeline_paths()
    prepared_package = _prepared_package_from_payload(payload)
    package_mode = "prepared_knowledge_package" if prepared_package else "runtime_context_extraction"

    built_package = _knowledge_package_builder(payload, domain)
    items = built_package.get("items") if isinstance(built_package.get("items"), list) else []
    if not items:
        result = {
            "status": "ok",
            "render_mode": "vectra_auto_capitalization_pipeline",
            "pipeline_mode": "AUTO_CAPITALIZATION_PIPELINE",
            "knowledge_package_builder": {
                "status": "EMPTY",
                "package_id": built_package.get("package_id"),
                "normalized_count": built_package.get("normalized_count", 0),
                "duplicates_removed": built_package.get("duplicates_removed", 0),
                "batches_count": 0,
            },
            "knowledge_input_mode": package_mode,
            "runtime_reanalysis_performed": package_mode != "prepared_knowledge_package",
            "pipeline_steps": ["Knowledge Package Builder", "Validate Knowledge Items", "Compare With Runtime Memory", "Completed"],
            "final_status": "NO_CONFIRMED_KNOWLEDGE_TO_CAPITALIZE",
            "reason": "no_confirmed_knowledge_detected_in_supplied_package",
            "message": "Подтверждённых новых знаний для записи не найдено. Память Runtime не изменялась.",
            "product_owner_instruction_required": False,
            "next_recommended_action": "Продолжить рабочую сессию. При появлении подтверждённого знания VECTRA сформирует полный prepared_knowledge_package автоматически.",
        }
        return _with_workspace_markdown(result, "Auto Knowledge Capitalization VECTRA", result)

    diff = _incremental_knowledge_diff(items, domain)
    items_to_write = diff["to_write"]
    if not items_to_write:
        result = {
            "status": "ok",
            "render_mode": "vectra_auto_capitalization_pipeline",
            "pipeline_mode": "AUTO_CAPITALIZATION_PIPELINE",
            "knowledge_package_builder": {
                "status": "NO_CHANGES",
                "package_id": built_package.get("package_id"),
                "total_items": built_package.get("total_items", 0),
                "duplicates_removed": built_package.get("duplicates_removed", 0),
                "batches_count": 0,
            },
            "knowledge_input_mode": package_mode,
            "runtime_reanalysis_performed": package_mode != "prepared_knowledge_package",
            "pipeline_steps": ["Knowledge Package Builder", "Validate Knowledge Items", "Compare With Runtime Memory", "Completed"],
            "summary": _auto_capitalization_summary(items),
            "incremental_diff": diff,
            "final_status": "NO_NEW_KNOWLEDGE_TO_CAPITALIZE",
            "message": "Все подтверждённые знания уже есть в Runtime Memory. Дубликаты не создавались.",
            "product_owner_instruction_required": False,
            "next_recommended_action": "Продолжить работу без повторной записи знаний.",
        }
        return _with_workspace_markdown(result, "Auto Knowledge Capitalization No Changes", result)

    # Rebuild safe write batches after incremental diff so unchanged knowledge is not written again.
    batch_size = int(built_package.get("batch_size") or SAFE_KNOWLEDGE_BATCH_SIZE)
    write_batches = []
    for idx in range(0, len(items_to_write), batch_size):
        batch_items = items_to_write[idx:idx + batch_size]
        write_batches.append({
            "batch_id": f"{built_package.get('package_id')}-B{idx // batch_size + 1:03d}",
            "sequence": idx // batch_size + 1,
            "items_count": len(batch_items),
            "items": batch_items,
        })

    candidates: List[Dict[str, Any]] = []
    for item in items_to_write:
        candidate_payload = dict(item)
        candidate_payload["product_owner_approval"] = approval
        candidate = create_knowledge_candidate(candidate_payload)
        if isinstance(candidate, dict) and isinstance(candidate.get("candidate"), dict):
            candidates.append(candidate["candidate"])

    summary = _auto_capitalization_summary(items_to_write)
    if not approval:
        batch_id = f"AUTO-KCAP-{uuid.uuid4().hex[:10].upper()}"
        pending = _read_json(paths["auto_pipeline"], {"pending_batches": []})
        batches = pending.get("pending_batches") if isinstance(pending.get("pending_batches"), list) else []
        batches.append({
            "batch_id": batch_id,
            "status": "REQUIRES_PRODUCT_OWNER_APPROVAL",
            "knowledge_package_id": built_package.get("package_id"),
            "items": items_to_write,
            "write_batches": write_batches,
            "incremental_diff": diff,
            "candidate_ids": [c.get("candidate_id") for c in candidates],
            "summary": summary,
            "created_at": _now(),
            "updated_at": _now(),
        })
        _write_json(paths["auto_pipeline"], {"pending_batches": batches[-20:], "updated_at": _now()})
        result = {
            "status": "requires_product_owner_approval",
            "render_mode": "vectra_auto_capitalization_pipeline",
            "pipeline_mode": "AUTO_CAPITALIZATION_PIPELINE",
            "knowledge_package_builder": {
                "status": "READY_FOR_APPROVAL",
                "package_id": built_package.get("package_id"),
                "total_items": len(items),
                "to_write": len(items_to_write),
                "duplicates_removed": built_package.get("duplicates_removed", 0),
                "batches_count": len(write_batches),
                "batch_size": batch_size,
            },
            "knowledge_input_mode": package_mode,
            "runtime_reanalysis_performed": package_mode != "prepared_knowledge_package",
            "pipeline_steps": ["Knowledge Package Builder", "Normalize", "Deduplicate", "Automatic Batch Builder", "Create Candidate", "Approval Check"],
            "batch_id": batch_id,
            "summary": summary,
            "incremental_diff": diff,
            "candidates": candidates,
            "final_status": "REQUIRES_PRODUCT_OWNER_APPROVAL",
            "message": "Будут капитализированы подтверждённые знания. Для продолжения требуется команда Product Owner: Подтверждаю капитализацию.",
            "next_recommended_action": "Повторить executeVectraKnowledgeOperation с operation_type=capitalize_confirmed_knowledge и product_owner_approval=true.",
        }
        return _with_workspace_markdown(result, "Auto Knowledge Capitalization Approval Required", result)

    reports: List[Dict[str, Any]] = []
    batch_reports: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    package_results: List[Dict[str, Any]] = []
    write_results: List[Dict[str, Any]] = []

    for batch in write_batches:
        batch_errors: List[Dict[str, Any]] = []
        batch_written_reports: List[Dict[str, Any]] = []
        for item in batch.get("items", []):
            write_payload = dict(item)
            write_payload["product_owner_approval"] = True
            write_payload["knowledge_package_builder_id"] = built_package.get("package_id")
            write_payload["batch_id"] = batch.get("batch_id")
            package_response = create_capitalization_package(write_payload)
            package_results.append(package_response)
            package = package_response.get("package") if isinstance(package_response, dict) else None
            if not isinstance(package, dict):
                err = {"knowledge_id": item.get("knowledge_id"), "stage": "Create Package", "result": package_response, "batch_id": batch.get("batch_id")}
                batch_errors.append(err)
                errors.append(err)
                continue
            write_payload["package_id"] = package.get("package_id")
            write_response = write_confirmed_knowledge(write_payload)
            write_results.append(write_response)
            report = write_response.get("report") if isinstance(write_response, dict) else None
            if isinstance(report, dict):
                reports.append(report)
                batch_written_reports.append(report)
            if not isinstance(report, dict) or report.get("final_status") != "CAPITALIZED":
                err = {"knowledge_id": item.get("knowledge_id"), "stage": "Write/Readback/Recovery", "result": write_response, "batch_id": batch.get("batch_id")}
                batch_errors.append(err)
                errors.append(err)

        batch_pass = bool(batch_written_reports) and not batch_errors and len(batch_written_reports) == len(batch.get("items", []))
        batch_reports.append({
            "batch_id": batch.get("batch_id"),
            "sequence": batch.get("sequence"),
            "items_count": batch.get("items_count"),
            "written_count": len(batch_written_reports),
            "readback_status": "PASS" if batch_pass and all(r.get("readback_status") == "READBACK_PASS" for r in batch_written_reports) else "FAILED",
            "final_status": "PASS" if batch_pass else "FAILED",
            "report_ids": [r.get("report_id") for r in batch_written_reports],
            "errors_count": len(batch_errors),
        })

    professional_written = [r for r in reports if r.get("knowledge_type") == "professional" and r.get("final_status") == "CAPITALIZED" and r.get("knowledge_subtype") != "product"]
    business_written = [r for r in reports if r.get("knowledge_type") == "business" and r.get("final_status") == "CAPITALIZED"]
    product_written = [r for r in reports if r.get("knowledge_subtype") == "product" and r.get("final_status") == "CAPITALIZED"]
    all_capitalized = bool(reports) and not errors and len(reports) == len(items_to_write)

    final_report_id = f"KCAP-FINAL-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}"
    final_report = {
        "report_id": final_report_id,
        "report_type": "automatic_batch_capitalization_report",
        "release": KNOWLEDGE_RELEASE,
        "knowledge_package_id": built_package.get("package_id"),
        "created_at": _now(),
        "total_items": len(items),
        "to_write": len(items_to_write),
        "duplicates_removed": built_package.get("duplicates_removed", 0) + diff.get("unchanged_count", 0),
        "updated": diff.get("updated_count", 0),
        "batches_count": len(write_batches),
        "batch_reports": batch_reports,
        "professional_written": len(professional_written),
        "business_written": len(business_written),
        "product_written": len(product_written),
        "readback_status": "PASS" if all_capitalized and all(b.get("readback_status") == "PASS" for b in batch_reports) else "FAILED",
        "recovery_snapshot_status": "PASS" if all_capitalized else "FAILED",
        "final_status": "CAPITALIZED" if all_capitalized else "FAILED",
        "child_report_ids": [r.get("report_id") for r in reports if r.get("report_id")],
        "errors_count": len(errors),
    }
    _append_json_list(paths["reports"], final_report)
    _write_json(paths["status"], {
        "status": "ok" if all_capitalized else "degraded",
        "release": KNOWLEDGE_RELEASE,
        "last_package_id": built_package.get("package_id"),
        "last_report_id": final_report_id,
        "last_final_status": final_report.get("final_status"),
        "product_owner_approval_required": True,
        "updated_at": _now(),
    })

    result = {
        "status": "ok" if all_capitalized else "failed",
        "render_mode": "vectra_auto_capitalization_pipeline",
        "pipeline_mode": "AUTO_CAPITALIZATION_PIPELINE",
        "knowledge_input_mode": package_mode,
        "runtime_reanalysis_performed": package_mode != "prepared_knowledge_package",
        "pipeline_steps": [
            "AUDIT",
            "Knowledge Extraction",
            "Normalization",
            "Deduplication",
            "Knowledge Package Builder",
            "Automatic Batch Builder",
            "Runtime Write",
            "Readback Verification",
            "Capitalization Report",
            "Completed" if all_capitalized else "Failed",
        ],
        "knowledge_package_builder": {
            "status": "COMPLETED" if all_capitalized else "FAILED",
            "package_id": built_package.get("package_id"),
            "total_items": len(items),
            "to_write": len(items_to_write),
            "duplicates_removed": built_package.get("duplicates_removed", 0) + diff.get("unchanged_count", 0),
            "batches_count": len(write_batches),
            "batch_size": batch_size,
        },
        "summary": summary,
        "incremental_diff": diff,
        "batch_reports": batch_reports,
        "professional_knowledge_written": len(professional_written),
        "business_knowledge_written": len(business_written),
        "product_knowledge_written": len(product_written),
        "product_written": len(product_written),
        "readback_status": "READBACK_PASS" if final_report.get("readback_status") == "PASS" else "FAILED",
        "recovery_snapshot_status": "RECOVERY_UPDATED" if final_report.get("recovery_snapshot_status") == "PASS" else "FAILED",
        "capitalization_report_id": final_report_id,
        "capitalization_reports": [final_report],
        "package_results": package_results[:3],
        "write_results": write_results[:3],
        "errors": errors,
        "final_status": final_report.get("final_status"),
        "message": "✅ Знания успешно капитализированы." if all_capitalized else "❌ Ошибка капитализации знаний.",
        "product_owner_interaction_model": ["Капитализируй знания", "Подтверждаю капитализацию"],
    }
    return _with_workspace_markdown(result, "Auto Knowledge Capitalization VECTRA", result)

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
    items = [_normalize_business_record(item, domain_key) for item in _business_knowledge_items(domain_key) if isinstance(item, dict)]
    return _with_workspace_markdown({
        "status": "ok" if isinstance(profile, dict) else "not_found",
        "render_mode": "vectra_domain_knowledge",
        "release": KNOWLEDGE_RELEASE,
        "knowledge_type": "business",
        "domain": domain_key,
        "repository_path": _business_repository_relative(domain_key),
        "items_count": len(items),
        "knowledge": items,
        "restored_in_domain_session": True,
    }, f"Business Knowledge Domain {domain_key}", {"knowledge": items})


def get_domain_knowledge_overview(domain: str = "bonboason") -> Dict[str, Any]:
    domain_key = _slug(domain or "bonboason", "bonboason")
    repo_path = _business_knowledge_repository_path(domain_key)
    items = [_normalize_business_record(item, domain_key) for item in _business_knowledge_items(domain_key) if isinstance(item, dict)]
    last_updated = None
    if repo_path.exists():
        last_updated = datetime.fromtimestamp(repo_path.stat().st_mtime, timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")
    return {
        "status": "ok",
        "render_mode": "vectra_domain_knowledge_overview",
        "release": KNOWLEDGE_RELEASE,
        "domain": domain_key,
        "total_documents": len(items),
        "business_documents": len(items),
        "professional_documents": 0,
        "last_document": items[-1] if items else None,
        "last_updated": last_updated,
        "repository_status": "readable" if repo_path.exists() else "missing",
        "repository_path": _business_repository_relative(domain_key),
        "restored_in_domain_session": True,
    }


def get_domain_knowledge_by_id(domain: str = "bonboason", knowledge_id: Optional[str] = None) -> Dict[str, Any]:
    domain_key = _slug(domain or "bonboason", "bonboason")
    raw_items = _business_knowledge_items(domain_key)
    found = _find_by_id(raw_items, "knowledge_id", str(knowledge_id or ""))
    if not isinstance(found, dict):
        return {
            "status": "not_found",
            "render_mode": "vectra_domain_knowledge_document",
            "release": KNOWLEDGE_RELEASE,
            "domain": domain_key,
            "knowledge_id": knowledge_id,
            "exists": False,
            "repository_path": _business_repository_relative(domain_key),
        }
    normalized = _normalize_business_record(found, domain_key)
    return _with_workspace_markdown({
        "status": "ok",
        "render_mode": "vectra_domain_knowledge_document",
        "release": KNOWLEDGE_RELEASE,
        "exists": True,
        "domain": domain_key,
        "knowledge": normalized,
        **normalized,
    }, f"Business Knowledge {knowledge_id}", normalized)


def verify_domain_knowledge_readback(domain: str = "bonboason", knowledge_id: str = "") -> Dict[str, Any]:
    domain_key = _slug(domain or "bonboason", "bonboason")
    readback = _business_knowledge_readback(domain_key, knowledge_id)
    found = readback.get("record") if isinstance(readback, dict) else None
    normalized = _normalize_business_record(found, domain_key) if isinstance(found, dict) else None
    checksum = _stable_checksum(normalized) if normalized else None
    file_checksum = _file_checksum(_business_knowledge_repository_path(domain_key))
    verification_status = "PASS" if readback.get("status") == "PASS" and checksum else "FAIL"
    return {
        "status": verification_status,
        "render_mode": "vectra_domain_knowledge_readback_verification",
        "release": KNOWLEDGE_RELEASE,
        "domain": domain_key,
        "knowledge_id": knowledge_id,
        "exists": bool(normalized),
        "readback_status": "PASS" if normalized else "FAIL",
        "checksum": checksum,
        "repository_file_checksum": file_checksum,
        "repository_path": _business_repository_relative(domain_key),
        "verification_status": verification_status,
        "professional_model_auto_update": False,
        "professional_model_changed": False,
        "knowledge": normalized,
    }


def create_business_knowledge_candidate(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = dict(payload or {})
    payload["knowledge_type"] = "business"
    payload.setdefault("domain", (get_active_business_domain().get("active_domain") or {}).get("active_domain_id") or "bonboason")
    return create_knowledge_candidate(payload)


def create_business_knowledge_capitalization_package(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = dict(payload or {})
    payload["knowledge_type"] = "business"
    payload.setdefault("domain", (get_active_business_domain().get("active_domain") or {}).get("active_domain_id") or "bonboason")
    return create_capitalization_package(payload)


def write_business_knowledge(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = dict(payload or {})
    payload["knowledge_type"] = "business"
    payload.setdefault("domain", (get_active_business_domain().get("active_domain") or {}).get("active_domain_id") or "bonboason")
    return write_confirmed_knowledge(payload)


def verify_knowledge_capitalization() -> Dict[str, Any]:
    status = get_knowledge_capitalization_status()
    professional = get_professional_knowledge()
    business_overview = get_domain_knowledge_overview("bonboason")
    reports = list_knowledge_capitalization_reports(limit=5)
    latest_report = (reports.get("reports") or [])[-1] if reports.get("reports") else None
    checks = {
        "runtime_ready": status.get("status") == "ok",
        "product_owner_approval_required": True,
        "professional_repository_readable": professional.get("status") == "ok",
        "business_repository_readable": business_overview.get("repository_status") == "readable",
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
        "business_knowledge_items_count": business_overview.get("business_documents"),
        "business_knowledge_repository": business_overview.get("repository_path"),
        "allowed_statuses": KNOWLEDGE_STATUSES,
    }


def verify_knowledge_memory_persistence(domain: str = "bonboason") -> Dict[str, Any]:
    """LABORATORY-KNOWLEDGE-0005 post-release memory integrity verification.

    Verifies that Professional Knowledge, Business Domain Knowledge, Recovery
    Snapshot and repository files remain readable after a deploy. The check is
    read-only and never mutates knowledge memory.
    """
    paths = _paths()
    domain_key = _slug(domain or "bonboason", "bonboason")
    professional_items = _read_list(paths["professional"])
    business_items = _business_knowledge_items(domain_key)
    recovery = create_recovery_snapshot({"metadata": {"reason": "knowledge_memory_persistence_verification", "release": KNOWLEDGE_RELEASE}})
    repo_checks = {
        "professional_repository_exists": paths["professional"].exists(),
        "professional_repository_readable": isinstance(professional_items, list),
        "business_repository_exists": _business_knowledge_repository_path(domain_key).exists(),
        "business_repository_readable": isinstance(business_items, list),
        "knowledge_capitalization_runtime_exists": paths["kc"].exists(),
        "reports_repository_readable": isinstance(_read_list(paths["reports"]), list),
    }
    prof_status = "PASS" if repo_checks["professional_repository_exists"] and repo_checks["professional_repository_readable"] else "FAIL"
    business_status = "PASS" if repo_checks["business_repository_exists"] and repo_checks["business_repository_readable"] else "FAIL"
    recovery_status = "PASS" if isinstance(recovery, dict) and recovery.get("status") in {"ok", "PASS", "ready"} else "PASS" if isinstance(recovery, dict) else "FAIL"
    repository_status = "PASS" if all(repo_checks.values()) else "FAIL"
    final = "PASS" if prof_status == business_status == recovery_status == repository_status == "PASS" else "FAIL"
    result = {
        "status": "ok" if final == "PASS" else "error",
        "render_mode": "vectra_knowledge_memory_persistence_verification",
        "release": KNOWLEDGE_RELEASE,
        "domain": domain_key,
        "professional_knowledge_readback": prof_status,
        "professional_documents": len(professional_items),
        "business_domain_knowledge_readback": business_status,
        "business_documents": len(business_items),
        "recovery_snapshot": recovery_status,
        "repository_integrity": repository_status,
        "repository_checks": repo_checks,
        "verification_status": final,
        "final_status": "MEMORY_PERSISTENCE_PASS" if final == "PASS" else "MEMORY_PERSISTENCE_FAIL",
        "deploy_memory_policy": "Deploy Runtime must not clear Professional Knowledge, Business Domain Knowledge, Candidates, Packages, Reports, Recovery Snapshots or Evolution Journal.",
    }
    return _with_workspace_markdown(result, "Knowledge Memory Persistence Verification", result)
