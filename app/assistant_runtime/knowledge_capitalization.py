"""FOUNDATION-0009 Knowledge Capitalization Runtime.

Official Product Owner approved Write -> Readback -> Recovery -> Evolution Journal
capitalization flow for Professional and Business Knowledge.
"""

from __future__ import annotations

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

KNOWLEDGE_RELEASE = "FOUNDATION-0009"
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


def capitalize_knowledge(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    paths = _paths()
    candidates = _read_list(paths["candidates"])
    candidate_id = str(payload.get("candidate_id") or "").strip()
    candidate = _find_by_id(candidates, "candidate_id", candidate_id) if candidate_id else None
    if not candidate:
        # Allow direct capitalization request to create a candidate first, but still require approval.
        created = create_knowledge_candidate(payload)
        candidate = created.get("candidate") if isinstance(created, dict) else None
    if not isinstance(candidate, dict):
        return _failed_report(None, None, "candidate_not_found_or_invalid")

    if bool(payload.get("product_owner_approval")):
        candidate["product_owner_approval"] = True
        candidate["status"] = "CONFIRMED_BY_PRODUCT_OWNER"
        candidate["updated_at"] = _now()
        for item in candidates:
            if item.get("candidate_id") == candidate.get("candidate_id"):
                item.update(candidate)
        _write_json(paths["candidates"], candidates)

    if not candidate.get("product_owner_approval") or candidate.get("status") != "CONFIRMED_BY_PRODUCT_OWNER":
        return _failed_report(candidate, None, "product_owner_approval_required")

    package_id = str(payload.get("package_id") or f"KCAP-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}-{uuid.uuid4().hex[:6]}")
    knowledge_id = str(candidate.get("knowledge_id") or f"K-{uuid.uuid4().hex[:8]}")
    knowledge_type = str(candidate.get("knowledge_type") or "professional").lower()
    domain = str(candidate.get("domain") or (get_active_business_domain().get("active_domain") or {}).get("domain_id") or "bonboason").lower()
    now = _now()
    target_repository = candidate.get("target_repository")
    package = {
        "package_id": package_id,
        "candidate_id": candidate.get("candidate_id"),
        "knowledge_id": knowledge_id,
        "knowledge_type": knowledge_type,
        "target_repository": target_repository,
        "status": "CAPITALIZATION_PACKAGE_CREATED",
        "product_owner_approval": True,
        "created_at": now,
        "write_readback_verification_required": True,
    }
    _append_json_list(paths["packages"], package)

    knowledge_record = {
        "knowledge_id": knowledge_id,
        "candidate_id": candidate.get("candidate_id"),
        "package_id": package_id,
        "knowledge_type": knowledge_type,
        "title": candidate.get("title"),
        "content": candidate.get("content"),
        "status": "CAPITALIZED",
        "source": candidate.get("source"),
        "created_at": now,
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

    recovery = create_recovery_snapshot({"metadata": {"created_by": "knowledge_capitalization", "package_id": package_id, "knowledge_id": knowledge_id, "knowledge_type": knowledge_type}})
    recovery_snapshot_id = (recovery.get("snapshot") or {}).get("snapshot_id") if isinstance(recovery.get("snapshot"), dict) else None
    recovery_status = "RECOVERY_UPDATED" if recovery_snapshot_id else "FAILED"

    journal_entry_id = f"knowledge-capitalization-{uuid.uuid4().hex[:10]}"
    journal_entry = {
        "entry_id": journal_entry_id,
        "timestamp": now,
        "release": KNOWLEDGE_RELEASE,
        "status": "CAPITALIZED",
        "event_type": "knowledge_capitalization",
        "summary": f"Knowledge {knowledge_id} capitalized into {target_repository}.",
        "package_id": package_id,
        "knowledge_id": knowledge_id,
        "knowledge_type": knowledge_type,
    }
    _append_json_list(paths["journal"], journal_entry)

    final_status = "CAPITALIZED" if recovery_snapshot_id else "FAILED"
    report_id = f"KCAP-REPORT-{uuid.uuid4().hex[:10].upper()}"
    report = {
        "report_id": report_id,
        "package_id": package_id,
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
    _write_json(paths["status"], {
        "status": "ok" if final_status == "CAPITALIZED" else "degraded",
        "release": KNOWLEDGE_RELEASE,
        "last_package_id": package_id,
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


def get_professional_knowledge() -> Dict[str, Any]:
    items = _read_list(_paths()["professional"])
    return _with_workspace_markdown({
        "status": "ok",
        "render_mode": "vectra_professional_knowledge",
        "release": KNOWLEDGE_RELEASE,
        "knowledge_type": "professional",
        "target_repository": "knowledge/professional_knowledge.json",
        "items_count": len(items),
        "knowledge": items,
    }, "Professional Knowledge VECTRA", {"knowledge": items})


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
