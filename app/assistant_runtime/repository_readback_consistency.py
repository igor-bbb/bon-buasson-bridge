"""Repository Readback Consistency Verification.

KNOWLEDGE-REPOSITORY-READBACK-003

Diagnostic-only runtime layer that compares actual repository files, facade
readback adapters, Recovery Snapshots and capitalization reports. It does not
write knowledge, does not mutate Professional Memory and does not capitalize.
"""

from __future__ import annotations

import hashlib
import json
import re
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.assistant_runtime.repository import ensure_repository, _read_json, _with_workspace_markdown
from app.assistant_runtime.knowledge_capitalization import (
    list_professional_knowledge,
    get_domain_knowledge,
)
from app.assistant_runtime.memory_repository import list_memory_objects

READBACK_RELEASE = "KNOWLEDGE-REPOSITORY-READBACK-003"
CANONICAL_DOMAIN_ID = "bon_buasson"
CANONICAL_DISPLAY_NAME = "Бон Буассон"
LEGACY_DOMAIN_ALIASES = {
    "bon_buasson",
    "bonboason",
    "bonbosson",
    "bon-boisson",
    "bon buasson",
    "bon buasson",
    "бон буассон",
    "бонуассон",
}


def _checksum(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")).hexdigest()


def normalize_domain_id(value: Any) -> str:
    """Normalize legacy Bon Buasson identifiers to the canonical domain_id."""
    if isinstance(value, dict):
        for key in ("domain_id", "active_domain_id", "id", "display_name", "name", "domain"):
            if value.get(key):
                return normalize_domain_id(value.get(key))
        return CANONICAL_DOMAIN_ID
    raw = str(value or "").strip()
    lowered = raw.lower().replace("_", " ").replace("-", " ")
    collapsed = re.sub(r"\s+", " ", lowered).strip()
    if collapsed in LEGACY_DOMAIN_ALIASES or "бон" in collapsed or "buasson" in collapsed or "boason" in collapsed or "bosson" in collapsed:
        return CANONICAL_DOMAIN_ID
    slug = re.sub(r"[^a-z0-9а-яіїєґ]+", "_", raw.lower(), flags=re.IGNORECASE).strip("_")
    return slug or CANONICAL_DOMAIN_ID


def _read_list(path: Path) -> List[Dict[str, Any]]:
    data = _read_json(path, [])
    if not isinstance(data, list):
        return []
    return [dict(item) for item in data if isinstance(item, dict)]


def _dedupe_by_knowledge_id(items: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
    merged: Dict[str, Dict[str, Any]] = {}
    no_id: List[Dict[str, Any]] = []
    for item in items:
        kid = str(item.get("knowledge_id") or item.get("id") or "").strip()
        if not kid:
            no_id.append(dict(item))
            continue
        current = merged.get(kid, {})
        next_item = dict(current)
        next_item.update(item)
        merged[kid] = next_item
    return list(merged.values()) + no_id


def _professional_repository_records(base: Path) -> List[Dict[str, Any]]:
    return _read_list(base / "knowledge" / "professional_knowledge.json")


def _business_repository_records(base: Path, domain_id: str) -> Tuple[List[Dict[str, Any]], List[str]]:
    """Read business knowledge from canonical and legacy locations.

    The diagnostic must reveal path mismatches, not hide them. Therefore it
    reads all plausible paths, annotates source_path, then deduplicates by
    knowledge_id.
    """
    canonical = normalize_domain_id(domain_id)
    candidate_paths = [
        base / "business_domains" / canonical / "business_knowledge.json",
        base / "business_domains" / "bonboason" / "business_knowledge.json",
        base / "business_domains" / "Bonboason" / "business_knowledge.json",
        base / "business_domains" / "Bonbosson" / "business_knowledge.json",
    ]
    records: List[Dict[str, Any]] = []
    paths_used: List[str] = []
    for path in candidate_paths:
        if not path.exists():
            continue
        paths_used.append(str(path.relative_to(base)).replace("\\", "/"))
        for item in _read_list(path):
            row = dict(item)
            row.setdefault("domain", canonical)
            row["_readback_source_path"] = str(path.relative_to(base)).replace("\\", "/")
            records.append(row)

    # Some early domain profiles stored business knowledge inside the profile.
    profile_paths = [
        base / "runtime" / "business_domains" / canonical / "domain_profile.json",
        base / "runtime" / "business_domains" / "bonboason" / "domain_profile.json",
    ]
    for path in profile_paths:
        profile = _read_json(path, {})
        if not isinstance(profile, dict):
            continue
        profile_items = profile.get("business_knowledge")
        if not isinstance(profile_items, list):
            continue
        paths_used.append(str(path.relative_to(base)).replace("\\", "/") + "#business_knowledge")
        for item in profile_items:
            if isinstance(item, dict):
                row = dict(item)
                row.setdefault("domain", canonical)
                row["_readback_source_path"] = str(path.relative_to(base)).replace("\\", "/") + "#business_knowledge"
                records.append(row)
    return _dedupe_by_knowledge_id(records), sorted(set(paths_used))


def _facade_professional_readback_count() -> Tuple[int, List[str], Dict[str, Any]]:
    result = list_professional_knowledge()
    items = result.get("professional_knowledge") or result.get("knowledge") or result.get("items") or []
    if not isinstance(items, list):
        items = []
    ids = [str(item.get("knowledge_id")) for item in items if isinstance(item, dict) and item.get("knowledge_id")]
    return len(items), ids, result


def _facade_business_readback_count(domain_id: str) -> Tuple[int, List[str], Dict[str, Any]]:
    result = get_domain_knowledge(domain=domain_id)
    items = result.get("business_knowledge") or result.get("domain_knowledge") or result.get("knowledge") or result.get("items") or []
    if not isinstance(items, list):
        items = []
    ids = [str(item.get("knowledge_id")) for item in items if isinstance(item, dict) and item.get("knowledge_id")]
    return len(items), ids, result


def _memory_repository_count(memory_space: str, domain_id: str) -> Tuple[int, List[str], Dict[str, Any]]:
    result = list_memory_objects(memory_space=memory_space, domain=domain_id, limit=100000)
    objects = result.get("objects") if isinstance(result.get("objects"), list) else []
    ids = [str(item.get("knowledge_id")) for item in objects if isinstance(item, dict) and item.get("knowledge_id")]
    return len(objects), ids, result


def _latest_snapshot(base: Path) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    snapshots_dir = base / "snapshots"
    if not snapshots_dir.exists():
        return None, None
    files = sorted(snapshots_dir.glob("*.json"), key=lambda p: p.stat().st_mtime)
    if not files:
        return None, None
    path = files[-1]
    data = _read_json(path, {})
    return (data if isinstance(data, dict) else None), str(path.relative_to(base)).replace("\\", "/")


def _snapshot_counts(snapshot: Optional[Dict[str, Any]], domain_id: str) -> Dict[str, Any]:
    if not isinstance(snapshot, dict):
        return {
            "available": False,
            "professional_knowledge_count": 0,
            "business_knowledge_count": 0,
            "professional_knowledge_ids": [],
            "business_knowledge_ids": [],
        }
    professional = snapshot.get("professional_knowledge") if isinstance(snapshot.get("professional_knowledge"), list) else []
    business_keys = [
        "bon_buasson_business_knowledge",
        "bonboason_business_knowledge",
        "business_knowledge",
    ]
    business: List[Dict[str, Any]] = []
    for key in business_keys:
        if isinstance(snapshot.get(key), list):
            business.extend([dict(item) for item in snapshot.get(key) if isinstance(item, dict)])
    business = _dedupe_by_knowledge_id(business)
    return {
        "available": True,
        "snapshot_id": snapshot.get("snapshot_id"),
        "professional_knowledge_count": len(professional),
        "business_knowledge_count": len(business),
        "professional_knowledge_ids": [str(item.get("knowledge_id")) for item in professional if isinstance(item, dict) and item.get("knowledge_id")],
        "business_knowledge_ids": [str(item.get("knowledge_id")) for item in business if isinstance(item, dict) and item.get("knowledge_id")],
    }


def _capitalization_report_summary(base: Path, domain_id: str) -> Dict[str, Any]:
    report_paths = [
        base / "runtime" / "knowledge_capitalization" / "reports.json",
        base / "runtime" / "context_capitalization" / "reports.json",
        base / "runtime" / "business_domains" / domain_id / "capitalization_reports.json",
        base / "runtime" / "business_domains" / "bonboason" / "capitalization_reports.json",
    ]
    reports: List[Dict[str, Any]] = []
    sources: List[str] = []
    for path in report_paths:
        items = _read_list(path)
        if items:
            sources.append(str(path.relative_to(base)).replace("\\", "/"))
            reports.extend(items)
    knowledge_ids = set()
    package_ids = set()
    for report in reports:
        if report.get("knowledge_id"):
            knowledge_ids.add(str(report.get("knowledge_id")))
        if report.get("package_id"):
            package_ids.add(str(report.get("package_id")))
        for key in ("knowledge", "knowledge_objects", "written", "capitalized_knowledge", "items"):
            value = report.get(key)
            if isinstance(value, list):
                for item in value:
                    if isinstance(item, dict) and item.get("knowledge_id"):
                        knowledge_ids.add(str(item.get("knowledge_id")))
    return {
        "reports_count": len(reports),
        "report_sources": sorted(set(sources)),
        "knowledge_ids_in_reports_count": len(knowledge_ids),
        "package_ids_count": len(package_ids),
        "knowledge_ids_in_reports": sorted(knowledge_ids),
    }


def _set_delta(left: Iterable[str], right: Iterable[str]) -> Dict[str, List[str]]:
    lset = {str(x) for x in left if str(x)}
    rset = {str(x) for x in right if str(x)}
    return {
        "missing_from_right": sorted(lset - rset),
        "extra_in_right": sorted(rset - lset),
    }


def verify_repository_readback_consistency(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Run the full diagnostic requested by KNOWLEDGE-REPOSITORY-READBACK-003."""
    payload = payload if isinstance(payload, dict) else {}
    base = ensure_repository()
    domain_id = normalize_domain_id(payload.get("domain") or payload.get("domain_id") or CANONICAL_DOMAIN_ID)

    professional_records = _dedupe_by_knowledge_id(_professional_repository_records(base))
    professional_ids = [str(item.get("knowledge_id")) for item in professional_records if item.get("knowledge_id")]

    business_records, business_paths = _business_repository_records(base, domain_id)
    business_ids = [str(item.get("knowledge_id")) for item in business_records if item.get("knowledge_id")]

    prof_facade_count, prof_facade_ids, prof_facade_result = _facade_professional_readback_count()
    biz_facade_count, biz_facade_ids, biz_facade_result = _facade_business_readback_count(domain_id)

    prof_memory_count, prof_memory_ids, prof_memory_result = _memory_repository_count("professional_memory", domain_id)
    biz_memory_count, biz_memory_ids, biz_memory_result = _memory_repository_count("business_domain_memory", domain_id)

    snapshot, snapshot_path = _latest_snapshot(base)
    snapshot_summary = _snapshot_counts(snapshot, domain_id)
    capitalization_summary = _capitalization_report_summary(base, domain_id)

    checks = {
        "professional_facade_readback_complete": prof_facade_count == len(professional_records),
        "business_facade_readback_complete": biz_facade_count == len(business_records),
        "professional_memory_repository_readback_complete": prof_memory_count == len(professional_records),
        "business_memory_repository_readback_complete": biz_memory_count == len(business_records),
        "professional_recovery_snapshot_consistent": (not snapshot_summary.get("available")) or snapshot_summary.get("professional_knowledge_count") == len(professional_records),
        "business_recovery_snapshot_consistent": (not snapshot_summary.get("available")) or snapshot_summary.get("business_knowledge_count") == len(business_records),
        "no_professional_pagination_limit_detected": prof_memory_count == len(professional_records) and prof_facade_count == len(professional_records),
        "no_business_pagination_limit_detected": biz_memory_count == len(business_records) and biz_facade_count == len(business_records),
        "domain_id_canonical": domain_id == CANONICAL_DOMAIN_ID,
    }

    failure_reasons: List[str] = []
    if not checks["professional_facade_readback_complete"] or not checks["business_facade_readback_complete"]:
        failure_reasons.append("READBACK_FILTERING_DETECTED")
    if not checks["professional_memory_repository_readback_complete"] or not checks["business_memory_repository_readback_complete"]:
        failure_reasons.append("REPOSITORY_INDEX_MISMATCH")
    if not checks["professional_recovery_snapshot_consistent"] or not checks["business_recovery_snapshot_consistent"]:
        failure_reasons.append("RECOVERY_MISMATCH")
    if not checks["no_professional_pagination_limit_detected"] or not checks["no_business_pagination_limit_detected"]:
        failure_reasons.append("PAGINATION_LIMIT_DETECTED")

    verification_status = "PASS" if all(checks.values()) else "FAIL"

    result = {
        "status": "ok" if verification_status == "PASS" else "degraded",
        "verification_status": verification_status,
        "render_mode": "vectra_repository_readback_consistency_verification",
        "release": READBACK_RELEASE,
        "domain_id": domain_id,
        "display_name": CANONICAL_DISPLAY_NAME if domain_id == CANONICAL_DOMAIN_ID else domain_id,
        "diagnostic_only": True,
        "capitalization_executed": False,
        "professional_memory_changed": False,
        "business_domain_changed": False,
        "counts": {
            "professional_repository_count": len(professional_records),
            "professional_facade_readback_count": prof_facade_count,
            "professional_memory_repository_readback_count": prof_memory_count,
            "professional_recovery_snapshot_count": snapshot_summary.get("professional_knowledge_count"),
            "business_repository_count": len(business_records),
            "business_facade_readback_count": biz_facade_count,
            "business_memory_repository_readback_count": biz_memory_count,
            "business_recovery_snapshot_count": snapshot_summary.get("business_knowledge_count"),
            "capitalization_reports_count": capitalization_summary.get("reports_count"),
        },
        "ids": {
            "professional_repository_ids": professional_ids,
            "professional_facade_readback_ids": prof_facade_ids,
            "professional_memory_repository_ids": prof_memory_ids,
            "business_repository_ids": business_ids,
            "business_facade_readback_ids": biz_facade_ids,
            "business_memory_repository_ids": biz_memory_ids,
        },
        "deltas": {
            "professional_repository_vs_facade": _set_delta(professional_ids, prof_facade_ids),
            "professional_repository_vs_memory_repository": _set_delta(professional_ids, prof_memory_ids),
            "professional_repository_vs_recovery": _set_delta(professional_ids, snapshot_summary.get("professional_knowledge_ids") or []),
            "business_repository_vs_facade": _set_delta(business_ids, biz_facade_ids),
            "business_repository_vs_memory_repository": _set_delta(business_ids, biz_memory_ids),
            "business_repository_vs_recovery": _set_delta(business_ids, snapshot_summary.get("business_knowledge_ids") or []),
        },
        "checks": checks,
        "failure_reasons": sorted(set(failure_reasons)),
        "snapshot": {
            "available": bool(snapshot_summary.get("available")),
            "path": snapshot_path,
            "snapshot_id": snapshot_summary.get("snapshot_id"),
        },
        "business_repository_paths_used": business_paths,
        "capitalization_summary": capitalization_summary,
        "repository_checksums": {
            "professional_repository_checksum": _checksum(professional_records),
            "business_repository_checksum": _checksum(business_records),
            "professional_facade_readback_checksum": _checksum(prof_facade_ids),
            "business_facade_readback_checksum": _checksum(biz_facade_ids),
        },
        "source_of_truth": "Runtime Repository files compared with facade readback, memory repository adapters and latest Recovery Snapshot.",
        "next_action": "If verification_status=FAIL, inspect failure_reasons and deltas before changing Professional Memory or Business Domain.",
    }

    # Keep raw nested service payloads out of normal response by default, but allow
    # Laboratory to ask for them when needed.
    if bool(payload.get("include_raw_readback")):
        result["raw_readback"] = {
            "professional_facade": prof_facade_result,
            "business_facade": biz_facade_result,
            "professional_memory_repository": prof_memory_result,
            "business_memory_repository": biz_memory_result,
        }

    title = "Repository Readback Consistency Verification"
    body = {
        "verification_status": verification_status,
        "counts": result["counts"],
        "failure_reasons": result["failure_reasons"],
        "checks": checks,
    }
    return _with_workspace_markdown(result, title, body)
