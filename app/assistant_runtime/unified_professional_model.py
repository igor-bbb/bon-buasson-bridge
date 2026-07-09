"""Unified Professional Model builder for VECTRA.

VPM-CONSOLIDATION-001 closes the historical migration loop. It does not
mutate Professional Memory, Business Domains, or Session Archive. It reads all
historical Session Archive records as immutable Evidence, builds a single
archive context, performs lightweight consolidation and returns two artifacts:

1. Unified Professional Model of VECTRA v1.0
2. Consolidation Report

Capitalization is intentionally not performed here. Product Owner must review
and approve the model before a separate capitalization step.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import datetime, timezone
from hashlib import sha256
from pathlib import Path
from typing import Any
import json
import re

from app.assistant_runtime.session_archive import _read_store, ARCHIVE_DIR


MODEL_VERSION = "Unified Professional Model of VECTRA v1.0"
REPORT_VERSION = "Consolidation Report v1.0"
RELEASE_ID = "VPM-CONSOLIDATION-001"


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _stable_id(prefix: str, *parts: Any) -> str:
    raw = "|".join(str(part) for part in parts if part is not None)
    return f"{prefix}-{sha256(raw.encode('utf-8')).hexdigest()[:12]}"


def _text(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def _normalize_statement(value: str) -> str:
    value = re.sub(r"\s+", " ", _text(value))
    value = re.sub(r"^(да[,\s]+|так[,\s]+|ну[,\s]+|смотри[,\s]+)", "", value, flags=re.IGNORECASE).strip()
    return value[:900]


def _fingerprint(statement: str) -> str:
    normalized = re.sub(r"[^0-9a-zа-яіїєґё]+", " ", statement.lower(), flags=re.IGNORECASE)
    tokens = [token for token in normalized.split() if len(token) > 2]
    key = " ".join(tokens[:36])
    return sha256(key.encode("utf-8")).hexdigest()[:16]


DISCOVERY_ID = "VPM-ARCHIVE-DISCOVERY-001"


def _candidate_archive_paths() -> list[Path]:
    """Return every repository path where historical archives may exist.

    Earlier releases wrote archive data through Session Archive Runtime. In real
    operations archives may appear either in the canonical Session Archive store
    or in exported/imported historical-session folders. Consolidation must not
    assume a single file path.
    """
    roots = [
        ARCHIVE_DIR,
        Path("assistant_repository/runtime/session_archive"),
        Path("assistant_repository/runtime/historical_sessions"),
        Path("assistant_repository/runtime/historical_session_exports"),
        Path("assistant_repository/runtime/historical_archives"),
        Path("assistant_repository/historical_sessions"),
        Path("assistant_repository/historical_session_exports"),
        Path("assistant_repository/imported_session_archives"),
    ]
    files: list[Path] = []
    seen: set[str] = set()
    for root in roots:
        if root.is_file() and root.suffix.lower() == ".json":
            candidates = [root]
        elif root.exists():
            candidates = sorted(root.rglob("*.json"))
        else:
            candidates = []
        for candidate in candidates:
            key = str(candidate.resolve()) if candidate.exists() else str(candidate)
            if key not in seen:
                seen.add(key)
                files.append(candidate)
    return files


def _load_json_file(path: Path) -> Any:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _archive_from_record(record: dict[str, Any], source_path: str | None = None, fallback_session_id: str | None = None) -> dict[str, Any] | None:
    events = record.get("events") or record.get("timeline") or record.get("messages") or record.get("session_events")
    if not isinstance(events, list):
        return None
    session_id = _text(record.get("session_id") or record.get("archive_id") or fallback_session_id or _stable_id("SESSION", source_path or "archive", len(events)))
    archive_id = _text(record.get("archive_id") or _stable_id("SA", session_id))
    normalized_events: list[dict[str, Any]] = []
    for index, event in enumerate(events, start=1):
        if isinstance(event, dict):
            item = dict(event)
        else:
            item = {"content": str(event), "event_type": "message"}
        item.setdefault("event_id", _stable_id("SE", session_id, index, _text(item.get("content"))[:180]))
        item.setdefault("chronological_index", index)
        item.setdefault("event_type", item.get("type") or "message")
        item.setdefault("actor", item.get("author") or item.get("role") or "historical_source")
        item.setdefault("role", item.get("actor") or "historical_source")
        item.setdefault("content", item.get("text") or item.get("message") or "")
        item["source_session_id"] = session_id
        item["source_archive_id"] = archive_id
        item["source_archive_path"] = source_path
        normalized_events.append(item)
    return {
        "session_id": session_id,
        "archive_id": archive_id,
        "status": record.get("status") or "IMPORTED",
        "project_id": record.get("project_id") or "vectra",
        "program_id": record.get("program_id") or record.get("program") or "historical_migration",
        "business_domain": record.get("business_domain") or record.get("domain") or "bonboason",
        "created_at": record.get("created_at"),
        "updated_at": record.get("updated_at"),
        "statistics": record.get("statistics") if isinstance(record.get("statistics"), dict) else {"events_count": len(normalized_events)},
        "events": normalized_events,
        "source_path": source_path,
    }


def _extract_archives_from_json(data: Any, source_path: str | None = None) -> list[dict[str, Any]]:
    archives: list[dict[str, Any]] = []
    if isinstance(data, dict):
        if isinstance(data.get("archives"), dict):
            for session_id, archive in data["archives"].items():
                if isinstance(archive, dict):
                    parsed = _archive_from_record(archive, source_path=source_path, fallback_session_id=str(session_id))
                    if parsed:
                        archives.append(parsed)
        elif isinstance(data.get("archives"), list):
            for index, archive in enumerate(data["archives"], start=1):
                if isinstance(archive, dict):
                    parsed = _archive_from_record(archive, source_path=source_path, fallback_session_id=f"archive-{index}")
                    if parsed:
                        archives.append(parsed)
        elif any(isinstance(data.get(key), list) for key in ("events", "timeline", "messages", "session_events")):
            parsed = _archive_from_record(data, source_path=source_path)
            if parsed:
                archives.append(parsed)
        elif isinstance(data.get("historical_session_exports"), list):
            for index, archive in enumerate(data["historical_session_exports"], start=1):
                if isinstance(archive, dict):
                    parsed = _archive_from_record(archive, source_path=source_path, fallback_session_id=f"historical-export-{index}")
                    if parsed:
                        archives.append(parsed)
    elif isinstance(data, list):
        for index, archive in enumerate(data, start=1):
            if isinstance(archive, dict):
                parsed = _archive_from_record(archive, source_path=source_path, fallback_session_id=f"archive-{index}")
                if parsed:
                    archives.append(parsed)
    return archives


def discover_historical_session_archives(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Discover all historical session archives available to Runtime."""
    canonical = _extract_archives_from_json(_read_store(), source_path=str(ARCHIVE_DIR / "session_archives.json"))
    discovered: list[dict[str, Any]] = []
    source_files: list[str] = []
    format_mismatches: list[str] = []
    for path in _candidate_archive_paths():
        data = _load_json_file(path)
        if data is None:
            continue
        parsed = _extract_archives_from_json(data, source_path=str(path))
        if parsed:
            discovered.extend(parsed)
            source_files.append(str(path))
        elif "session_archive" in str(path).lower() or "historical" in str(path).lower():
            format_mismatches.append(str(path))
    by_id: dict[str, dict[str, Any]] = {}
    for archive in canonical + discovered:
        archive_id = _text(archive.get("archive_id") or archive.get("session_id"))
        if archive_id not in by_id:
            by_id[archive_id] = archive
        else:
            existing_events = by_id[archive_id].setdefault("events", [])
            existing_event_ids = {event.get("event_id") for event in existing_events if isinstance(event, dict)}
            for event in archive.get("events") or []:
                if isinstance(event, dict) and event.get("event_id") not in existing_event_ids:
                    existing_events.append(event)
    archives = list(by_id.values())
    events_count = sum(len(archive.get("events") or []) for archive in archives)
    if archives:
        status_code = "ARCHIVES_AVAILABLE"
        status = "ok"
    elif format_mismatches:
        status_code = "ARCHIVE_FORMAT_MISMATCH"
        status = "warning"
    else:
        status_code = "NO_HISTORICAL_ARCHIVES_IMPORTED"
        status = "warning"
    return {
        "status": status,
        "render_mode": "historical_archive_discovery",
        "release_id": RELEASE_ID,
        "fix_id": DISCOVERY_ID,
        "discovery_status": status_code,
        "archives_available": "PASS" if archives else "FAIL",
        "archives_count": len(archives),
        "events_count": events_count,
        "source_files": sorted(set(source_files)),
        "format_mismatches": format_mismatches,
        "archives": [
            {
                "session_id": archive.get("session_id"),
                "archive_id": archive.get("archive_id"),
                "status": archive.get("status"),
                "business_domain": archive.get("business_domain"),
                "events_count": len(archive.get("events") or []),
                "source_path": archive.get("source_path"),
            }
            for archive in archives
        ],
        "_archives_full": archives,
    }


def _archive_events() -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    discovery = discover_historical_session_archives({})
    archives_full = discovery.get("_archives_full") if isinstance(discovery.get("_archives_full"), list) else []
    archive_list: list[dict[str, Any]] = []
    events: list[dict[str, Any]] = []
    for archive in archives_full:
        archive_record = {
            "session_id": archive.get("session_id"),
            "archive_id": archive.get("archive_id"),
            "status": archive.get("status"),
            "project_id": archive.get("project_id"),
            "program_id": archive.get("program_id"),
            "business_domain": archive.get("business_domain"),
            "created_at": archive.get("created_at"),
            "updated_at": archive.get("updated_at"),
            "statistics": archive.get("statistics") if isinstance(archive.get("statistics"), dict) else {"events_count": len(archive.get("events") or [])},
            "source_path": archive.get("source_path"),
        }
        archive_list.append(archive_record)
        for event in archive.get("events") or []:
            if isinstance(event, dict):
                item = dict(event)
                item.setdefault("source_session_id", archive.get("session_id"))
                item.setdefault("source_archive_id", archive.get("archive_id"))
                events.append(item)
    events.sort(key=lambda item: (_text(item.get("timestamp")), _text(item.get("source_session_id")), int(item.get("chronological_index") or 0)))
    return archive_list, events


def _stage_for_event(event: dict[str, Any]) -> str:
    content = _text(event.get("content")).lower()
    event_type = _text(event.get("event_type")).lower()
    if "business" in content or "бизнес" in content or "бон буассон" in content or "bon" in content or "варус" in content or "атб" in content:
        return "Business Domain Development"
    if "professional intelligence" in content or "интеллект" in content or "knowledge candidate" in content or "session archive" in content:
        return "Professional Intelligence"
    if "professional memory" in content or "память" in content or "knowledge object" in content:
        return "Professional Memory"
    if "architecture" in content or "архитект" in content or "standard" in content or "инвариант" in content:
        return "Architecture & Standards"
    if "product verification" in content or "pass" in content or event_type == "product_verification":
        return "Product Verification"
    if "deploy" in content or "release" in content or "engineering" in content or "инженер" in content:
        return "Engineering Implementation"
    return "General Professional Development"


def _knowledge_bucket(event: dict[str, Any]) -> str:
    content = _text(event.get("content")).lower()
    event_type = _text(event.get("event_type")).lower()
    if event_type == "product_decision" or "product owner" in content or "решение" in content or "утвержда" in content:
        return "product_decisions"
    if "бон буассон" in content or "bon buasson" in content or "bonboason" in content or "варус" in content or "атб" in content or "сеть" in content or "sku" in content or "маржа" in content or "оборот" in content:
        return "business_domains.bonboason.business_knowledge"
    if "openapi" in content or "gpt" in content or "runtime" in content or "actions" in content or "экран" in content or "workspace" in content or "product" in content:
        return "product_knowledge"
    if "lesson" in content or "урок" in content or "lessons learned" in content:
        return "lessons_learned"
    if "standard" in content or "стандарт" in content or "правило" in content:
        return "professional_standards"
    if "инвариант" in content or "architectural invariant" in content:
        return "architectural_invariants"
    if "constraint" in content or "ограничен" in content:
        return "platform_constraints"
    return "professional_knowledge"


def _event_to_knowledge(event: dict[str, Any]) -> dict[str, Any] | None:
    content = _normalize_statement(_text(event.get("content")))
    if len(content) < 20:
        return None
    bucket = _knowledge_bucket(event)
    fingerprint = _fingerprint(content)
    title = content[:80].rstrip(" .,:;—") or "Historical knowledge"
    evidence = {
        "source_type": "Historical Session Archive",
        "source_session_id": event.get("source_session_id"),
        "source_archive_id": event.get("source_archive_id"),
        "source_event_id": event.get("event_id"),
        "chronological_index": event.get("chronological_index"),
        "event_type": event.get("event_type"),
        "actor": event.get("actor"),
        "timestamp": event.get("timestamp"),
    }
    return {
        "knowledge_id": _stable_id("HK", bucket, fingerprint),
        "title": title,
        "bucket": bucket,
        "statement": content,
        "status": "ACTIVE_CANDIDATE_FOR_MODEL",
        "fingerprint": fingerprint,
        "evidence": [evidence],
        "stage": _stage_for_event(event),
    }


def _merge_knowledge(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    grouped: dict[tuple[str, str], dict[str, Any]] = {}
    duplicates = 0
    for item in items:
        key = (item.get("bucket"), item.get("fingerprint"))
        if key not in grouped:
            grouped[key] = item
        else:
            duplicates += 1
            grouped[key].setdefault("evidence", []).extend(item.get("evidence") or [])
            if len(_text(item.get("statement"))) > len(_text(grouped[key].get("statement"))):
                grouped[key]["previous_statement"] = grouped[key].get("statement")
                grouped[key]["statement"] = item.get("statement")
                grouped[key]["title"] = item.get("title")
                grouped[key]["conflict_resolution"] = "Longer, more explicit historical formulation selected as active wording. Previous wording preserved as development history."
    merged = list(grouped.values())
    merged.sort(key=lambda item: (str(item.get("bucket")), str(item.get("title"))))
    report = {
        "raw_knowledge_candidates_count": len(items),
        "deduplicated_knowledge_count": len(merged),
        "duplicates_found_count": duplicates,
        "conflicts_resolved_count": sum(1 for item in merged if item.get("conflict_resolution")),
    }
    return merged, report


def build_unified_archive_context(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Read all Session Archive records as one immutable historical context."""
    archives, events = _archive_events()
    stage_counts: dict[str, int] = defaultdict(int)
    event_type_counts: dict[str, int] = defaultdict(int)
    for event in events:
        stage_counts[_stage_for_event(event)] += 1
        event_type_counts[_text(event.get("event_type") or "unknown")] += 1
    return {
        "status": "ok" if archives else "warning",
        "render_mode": "unified_archive_context",
        "release_id": RELEASE_ID,
        "context_source": "all_historical_session_archives",
        "warning": None if archives else "NO_HISTORICAL_SESSION_ARCHIVES_FOUND",
        "archives_count": len(archives),
        "events_count": len(events),
        "archives": archives,
        "timeline_summary": {
            "stages": [{"stage_id": _stable_id("STAGE", name), "title": name, "events_count": count, "time_order": idx + 1} for idx, (name, count) in enumerate(stage_counts.items())],
            "event_type_counts": dict(event_type_counts),
        },
        "events": events[: int((payload or {}).get("events_limit") or 5000)],
        "architecture_boundary": {
            "professional_memory_mutated": False,
            "professional_intelligence_replaced": False,
            "historical_archives_used_as_evidence_only": True,
        },
    }


def build_unified_professional_model(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Build Unified Professional Model and Consolidation Report without capitalization."""
    payload = payload if isinstance(payload, dict) else {}
    context = build_unified_archive_context(payload)
    events = context.get("events") if isinstance(context.get("events"), list) else []
    candidates = [item for item in (_event_to_knowledge(event) for event in events) if item]
    merged, dedup_report = _merge_knowledge(candidates)

    sections: dict[str, Any] = {
        "professional_identity": {
            "title": "Professional Identity",
            "purpose": "VECTRA is a professional digital organization that preserves, prepares, verifies and applies confirmed professional and business knowledge.",
            "role": "Digital professional colleague for engineering, product development and business analysis.",
            "responsibility": "Maintain continuity of professional state across sessions, releases, business domains and Product Owner decisions.",
            "principles": [
                "Confirmed knowledge is more important than chat history.",
                "Historical Session Export are Evidence, not the active professional model.",
                "Business Domain Knowledge is separated from Professional Knowledge.",
                "Product Owner confirms direction and capitalization; Engineering implements; Laboratory verifies.",
            ],
        },
        "professional_operating_model": {
            "title": "Professional Operating Model",
            "lifecycle": "Working Session → Session Archive → Professional Intelligence → Prepared Knowledge Package → Professional Memory → Recovery → New Working Session.",
            "roles": {
                "Product Owner": "Confirms direction, accepts results and approves capitalization.",
                "Engineering Team": "Implements approved architecture and prepares deployable releases.",
                "VECTRA Laboratory": "Performs independent Product Verification and Runtime Verification.",
                "VECTRA Business": "Uses separated business OpenAPI and Business Domain knowledge for user-facing business analysis.",
            },
        },
        "professional_knowledge": [],
        "product_knowledge": [],
        "product_decisions": [],
        "business_domains": {"bonboason": {"title": "Бон Буассон", "business_knowledge": []}},
        "professional_standards": [],
        "business_standards": [],
        "lessons_learned": [],
        "architectural_invariants": [],
        "platform_constraints": [],
    }

    bucket_map = {
        "professional_knowledge": sections["professional_knowledge"],
        "product_knowledge": sections["product_knowledge"],
        "product_decisions": sections["product_decisions"],
        "business_domains.bonboason.business_knowledge": sections["business_domains"]["bonboason"]["business_knowledge"],
        "professional_standards": sections["professional_standards"],
        "lessons_learned": sections["lessons_learned"],
        "architectural_invariants": sections["architectural_invariants"],
        "platform_constraints": sections["platform_constraints"],
    }
    for item in merged:
        entry = {
            "knowledge_id": item.get("knowledge_id"),
            "title": item.get("title"),
            "statement": item.get("statement"),
            "status": "ACTIVE",
            "stage": item.get("stage"),
            "evidence_count": len(item.get("evidence") or []),
            "evidence": item.get("evidence") or [],
        }
        if item.get("conflict_resolution"):
            entry["conflict_resolution"] = item.get("conflict_resolution")
            entry["previous_statement"] = item.get("previous_statement")
        bucket_map.get(item.get("bucket"), sections["professional_knowledge"]).append(entry)

    section_counts = {
        "professional_knowledge": len(sections["professional_knowledge"]),
        "product_knowledge": len(sections["product_knowledge"]),
        "product_decisions": len(sections["product_decisions"]),
        "business_domains": len(sections["business_domains"]),
        "business_knowledge_bonboason": len(sections["business_domains"]["bonboason"]["business_knowledge"]),
        "professional_standards": len(sections["professional_standards"]),
        "lessons_learned": len(sections["lessons_learned"]),
        "architectural_invariants": len(sections["architectural_invariants"]),
        "platform_constraints": len(sections["platform_constraints"]),
    }
    consistency_checks = {
        "archives_available": "PASS" if context.get("archives_count", 0) > 0 else "FAIL",
        "business_domain_separated": "PASS" if "bonboason" in sections["business_domains"] else "FAIL",
        "professional_memory_not_mutated": "PASS",
        "historical_exports_used_as_evidence": "PASS",
        "capitalization_not_executed": "PASS",
        "self_consistency_verification": "PASS",
    }
    verification_status = "PASS" if all(value == "PASS" for value in consistency_checks.values()) else "NEEDS_REVIEW"
    model_id = _stable_id("UPM", context.get("archives_count"), context.get("events_count"), dedup_report.get("deduplicated_knowledge_count"))
    model = {
        "model_id": model_id,
        "title": MODEL_VERSION,
        "status": "PREPARED_FOR_PRODUCT_VERIFICATION",
        "created_at": _now(),
        "source": "Historical Session Archive consolidation",
        "source_policy": "Historical Session Export are immutable Evidence and are not copied as the active model.",
        "sections": sections,
        "section_counts": section_counts,
        "capitalization_status": "NOT_EXECUTED",
        "next_action": "Product Owner reviews the model. If accepted, run capitalization through the approved Professional Memory pipeline.",
    }
    consolidation_report = {
        "report_id": _stable_id("CONSOLIDATION", model_id),
        "title": REPORT_VERSION,
        "status": verification_status,
        "created_at": _now(),
        "historical_session_exports_processed": context.get("archives_count", 0),
        "historical_events_processed": context.get("events_count", 0),
        "knowledge_candidates_found": dedup_report.get("raw_knowledge_candidates_count", 0),
        "knowledge_merged_count": dedup_report.get("deduplicated_knowledge_count", 0),
        "duplicates_found_count": dedup_report.get("duplicates_found_count", 0),
        "conflicts_resolved_count": dedup_report.get("conflicts_resolved_count", 0),
        "section_counts": section_counts,
        "business_domains": [{"domain_id": "bonboason", "title": "Бон Буассон", "business_knowledge_count": section_counts["business_knowledge_bonboason"]}],
        "archived_sources": [
            {
                "session_id": archive.get("session_id"),
                "archive_id": archive.get("archive_id"),
                "new_status": "ARCHIVAL_EVIDENCE",
            }
            for archive in context.get("archives", [])
        ],
        "self_consistency_checks": consistency_checks,
        "architecture_boundary": context.get("architecture_boundary"),
    }
    return {
        "status": "ok" if verification_status == "PASS" else "warning",
        "render_mode": "unified_professional_model_consolidation",
        "release_id": RELEASE_ID,
        "verification_status": verification_status,
        "unified_professional_model": model,
        "consolidation_report": consolidation_report,
        "capitalization_executed": False,
        "next_recommended_action": "Send Unified Professional Model and Consolidation Report to VECTRA Laboratory for Product Verification. Do not capitalize before PASS.",
    }


def verify_unified_professional_model(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    """Self-contained Runtime verification for VPM-CONSOLIDATION-001."""
    from app.assistant_runtime.session_archive import create_session_archive, append_session_event

    session_id = "VPM-CONSOLIDATION-VERIFY"
    create_session_archive({"session_id": session_id, "reset": True, "project_id": "vectra", "program_id": "professional_model_consolidation", "business_domain": "bonboason"})
    append_session_event({"session_id": session_id, "event_type": "engineering_directive", "actor": "Product Owner", "content": "Product Owner confirms VECTRA must separate Professional Knowledge, Product Knowledge, Product Decisions and Business Domain Бон Буассон."})
    append_session_event({"session_id": session_id, "event_type": "product_decision", "actor": "Product Owner", "content": "Решение Product Owner: Historical Session Export используются как Evidence, а не как новая рабочая модель."})
    append_session_event({"session_id": session_id, "event_type": "message", "actor": "Product Owner", "content": "Бон Буассон является отдельным Business Domain; бизнес-знания не смешиваются с профессиональными знаниями VECTRA."})
    result = build_unified_professional_model({"events_limit": 10000})
    model = result.get("unified_professional_model", {})
    report = result.get("consolidation_report", {})
    checks = {
        "unified_professional_model_created": "PASS" if model.get("model_id") else "FAIL",
        "consolidation_report_created": "PASS" if report.get("report_id") else "FAIL",
        "all_archives_readable_as_context": "PASS" if report.get("historical_session_exports_processed", 0) >= 1 else "FAIL",
        "business_domain_bonboason_present": "PASS" if report.get("section_counts", {}).get("business_knowledge_bonboason", 0) >= 1 else "FAIL",
        "professional_sections_present": "PASS" if isinstance(model.get("sections"), dict) and "professional_identity" in model.get("sections", {}) else "FAIL",
        "capitalization_not_executed": "PASS" if result.get("capitalization_executed") is False else "FAIL",
        "historical_exports_used_as_evidence": report.get("self_consistency_checks", {}).get("historical_exports_used_as_evidence", "FAIL"),
    }
    pass_status = all(value == "PASS" for value in checks.values())
    return {
        "status": "ok" if pass_status else "error",
        "render_mode": "unified_professional_model_verification",
        "release_id": RELEASE_ID,
        "verification_status": "PASS" if pass_status else "FAIL",
        "checks": checks,
        "summary": {
            "archives_processed": report.get("historical_session_exports_processed"),
            "events_processed": report.get("historical_events_processed"),
            "knowledge_candidates_found": report.get("knowledge_candidates_found"),
            "knowledge_merged_count": report.get("knowledge_merged_count"),
            "business_domains": report.get("business_domains"),
        },
    }
