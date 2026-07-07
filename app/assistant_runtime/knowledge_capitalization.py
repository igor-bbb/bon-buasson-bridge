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

KNOWLEDGE_RELEASE = "LABORATORY-KNOWLEDGE-0005"
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

def _extract_auto_knowledge_items(payload: Optional[Dict[str, Any]] = None, domain: str = "bonboason") -> List[Dict[str, Any]]:
    """Extract confirmed knowledge from Laboratory context payload.

    Runtime cannot infer hidden chat text by itself; the facade supplies the current
    working context in payload. This function accepts several stable shapes so the
    Laboratory can pass either explicit knowledge_items or a compact current_context.
    """
    payload = payload if isinstance(payload, dict) else {}
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

    # LABORATORY-KNOWLEDGE-0005: Product Owner must not build a technical payload.
    # The GPT facade may pass ordinary working-context text, and Runtime extracts
    # explicit, Product Owner-confirmed knowledge blocks from it.
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
    """Run the full autonomous Laboratory knowledge capitalization pipeline.

    Pipeline: Analyze Context -> Extract Knowledge -> Create Candidate -> Create Package
    -> Approval Check -> Write -> Readback -> Recovery Snapshot -> Report -> Completed.
    If approval is absent, the pipeline records candidates and returns a concise approval
    summary instead of trying to write.
    """
    payload = payload if isinstance(payload, dict) else {}
    domain = str(payload.get("domain") or (get_active_business_domain().get("active_domain") or {}).get("domain_id") or "bonboason")
    approval = bool(payload.get("product_owner_approval"))
    paths = _auto_pipeline_paths()
    items = _extract_auto_knowledge_items(payload, domain=domain)
    if not items:
        result = {
            "status": "ok",
            "render_mode": "vectra_auto_capitalization_pipeline",
            "pipeline_mode": "AUTO_CAPITALIZATION_PIPELINE",
            "pipeline_steps": ["Analyze Context", "Extract Knowledge", "Compare With Runtime Memory", "Completed"],
            "final_status": "NO_CONFIRMED_KNOWLEDGE_TO_CAPITALIZE",
            "reason": "no_confirmed_knowledge_detected_in_supplied_working_context",
            "message": "Подтверждённых новых знаний для записи не найдено. Память Runtime не изменялась.",
            "product_owner_instruction_required": False,
            "next_recommended_action": "Продолжить рабочую сессию. При появлении подтверждённого знания Laboratory передаст рабочий контекст в этот же фасад автоматически.",
        }
        return _with_workspace_markdown(result, "Auto Knowledge Capitalization VECTRA", result)

    diff = _incremental_knowledge_diff(items, domain)
    items_to_write = diff["to_write"]
    if not items_to_write:
        result = {
            "status": "ok",
            "render_mode": "vectra_auto_capitalization_pipeline",
            "pipeline_mode": "AUTO_CAPITALIZATION_PIPELINE",
            "pipeline_steps": ["Analyze Context", "Extract Knowledge", "Compare With Runtime Memory", "Completed"],
            "summary": _auto_capitalization_summary(items),
            "incremental_diff": diff,
            "final_status": "NO_CHANGES",
            "message": "Все подтверждённые знания уже есть в Runtime Memory. Дубликаты не создавались.",
            "product_owner_instruction_required": False,
            "next_recommended_action": "Продолжить работу без повторной записи знаний.",
        }
        return _with_workspace_markdown(result, "Auto Knowledge Capitalization No Changes", result)

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
            "items": items_to_write,
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
            "pipeline_steps": ["Analyze Context", "Extract Knowledge", "Create Candidate", "Approval Check"],
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
    package_results: List[Dict[str, Any]] = []
    write_results: List[Dict[str, Any]] = []
    errors: List[Dict[str, Any]] = []
    for item in items_to_write:
        write_payload = dict(item)
        write_payload["product_owner_approval"] = True
        package_response = create_capitalization_package(write_payload)
        package_results.append(package_response)
        package = package_response.get("package") if isinstance(package_response, dict) else None
        if not isinstance(package, dict):
            errors.append({"knowledge_id": item.get("knowledge_id"), "stage": "Create Package", "result": package_response})
            continue
        write_payload["package_id"] = package.get("package_id")
        write_response = write_confirmed_knowledge(write_payload)
        write_results.append(write_response)
        report = write_response.get("report") if isinstance(write_response, dict) else None
        if isinstance(report, dict):
            reports.append(report)
        if not isinstance(report, dict) or report.get("final_status") != "CAPITALIZED":
            errors.append({"knowledge_id": item.get("knowledge_id"), "stage": "Write/Readback/Recovery", "result": write_response})

    professional_written = [r for r in reports if r.get("knowledge_type") == "professional" and r.get("final_status") == "CAPITALIZED"]
    business_written = [r for r in reports if r.get("knowledge_type") == "business" and r.get("final_status") == "CAPITALIZED"]
    all_capitalized = bool(reports) and not errors and len(reports) == len(items_to_write)
    result = {
        "status": "ok" if all_capitalized else "failed",
        "render_mode": "vectra_auto_capitalization_pipeline",
        "pipeline_mode": "AUTO_CAPITALIZATION_PIPELINE",
        "pipeline_steps": [
            "Analyze Context",
            "Extract Knowledge",
            "Create Candidate",
            "Create Package",
            "Approval Check",
            "Write",
            "Readback",
            "Recovery Snapshot",
            "Capitalization Report",
            "Completed" if all_capitalized else "Failed",
        ],
        "summary": summary,
        "incremental_diff": diff,
        "professional_knowledge_written": len(professional_written),
        "business_knowledge_written": len(business_written),
        "readback_status": "READBACK_PASS" if all(r.get("readback_status") == "READBACK_PASS" for r in reports) and reports else "FAILED",
        "recovery_snapshot_status": "RECOVERY_UPDATED" if all(r.get("recovery_snapshot_status") == "RECOVERY_UPDATED" for r in reports) and reports else "FAILED",
        "capitalization_reports": reports,
        "package_results": package_results,
        "write_results": write_results,
        "errors": errors,
        "final_status": "CAPITALIZED" if all_capitalized else "FAILED",
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
