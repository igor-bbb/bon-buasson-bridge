"""Executable continuity guard for VECTRA organizational memory.

The guard keeps the responsibilities separate:

* Professional Knowledge remains in the Knowledge repository.
* active state remains in Runtime.
* journals and decisions remain Organizational Memory.

It only records a durable fingerprint and refuses to certify a startup/deploy
that silently loses previously known identifiers.
"""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from app.assistant_runtime.repository import _read_json, _write_json, ensure_repository
from app.assistant_runtime.repository_persistence import get_persistence_status


RELEASE_ID = "VECTRA-ORGANIZATIONAL-MEMORY-CONTINUITY-001"
CONTRACT_VERSION = "OrganizationalMemoryContinuity/v1.0"
BASELINE_PATH = Path("runtime") / "continuity" / "organizational_memory_baseline.json"
REPORT_PATH = Path("runtime") / "continuity" / "organizational_memory_reports.json"

PROTECTED_OBJECTS = {
    "professional_knowledge": {
        "path": Path("knowledge") / "professional_knowledge.json",
        "id_fields": ("knowledge_id", "id"),
    },
    "development_journal": {
        "path": Path("runtime") / "development" / "development_journal.json",
        "id_fields": ("record_id", "id"),
    },
    "development_journal_continuity": {
        "path": Path("runtime") / "development" / "development_journal_continuity.json",
        "id_fields": ("lost_record_ids",),
    },
    "evolution_journal": {
        "path": Path("journal") / "evolution_journal.json",
        "id_fields": ("entry_id", "id"),
    },
    "product_decisions": {
        "path": Path("decisions") / "product_decisions.json",
        "id_fields": ("decision_id", "id"),
    },
    "knowledge_influence_traces": {
        "path": Path("runtime") / "knowledge_influence" / "events.json",
        "id_fields": ("trace_id", "id"),
    },
}


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _checksum(value: Any) -> str:
    encoded = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _as_items(value: Any) -> List[Dict[str, Any]]:
    if isinstance(value, list):
        return [deepcopy(item) for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        records = value.get("records")
        if isinstance(records, list):
            return [deepcopy(item) for item in records if isinstance(item, dict)]
        return [deepcopy(value)] if value else []
    return []


def _flatten_identifiers(value: Any, fields: Iterable[str]) -> List[str]:
    identifiers: List[str] = []
    for item in _as_items(value):
        for field in fields:
            candidate = item.get(field)
            if isinstance(candidate, list):
                identifiers.extend(str(entry) for entry in candidate if str(entry).strip())
                continue
            if candidate is not None and str(candidate).strip():
                identifiers.append(str(candidate))
                break
    return sorted(set(identifiers))


def build_organizational_memory_fingerprint() -> Dict[str, Any]:
    base = ensure_repository()
    objects: Dict[str, Dict[str, Any]] = {}
    for name, contract in PROTECTED_OBJECTS.items():
        path = base / contract["path"]
        value = _read_json(path, [])
        items = _as_items(value)
        identifiers = _flatten_identifiers(value, contract["id_fields"])
        objects[name] = {
            "repository_path": contract["path"].as_posix(),
            "records_count": len(items),
            "record_ids": identifiers,
            "content_checksum": _checksum(value),
            "available": path.exists(),
        }
    return {
        "contract_version": CONTRACT_VERSION,
        "release": RELEASE_ID,
        "captured_at": _now(),
        "objects": objects,
    }


def _compare_fingerprints(
    baseline: Dict[str, Any],
    current: Dict[str, Any],
) -> Dict[str, Any]:
    previous_objects = baseline.get("objects") if isinstance(baseline.get("objects"), dict) else {}
    current_objects = current.get("objects") if isinstance(current.get("objects"), dict) else {}
    checks: Dict[str, Dict[str, Any]] = {}
    failed_objects: List[str] = []

    for name in PROTECTED_OBJECTS:
        previous = previous_objects.get(name) if isinstance(previous_objects.get(name), dict) else {}
        observed = current_objects.get(name) if isinstance(current_objects.get(name), dict) else {}
        previous_ids = set(str(item) for item in (previous.get("record_ids") or []))
        observed_ids = set(str(item) for item in (observed.get("record_ids") or []))
        missing_ids = sorted(previous_ids - observed_ids)
        count_not_reduced = int(observed.get("records_count") or 0) >= int(previous.get("records_count") or 0)
        passed = not missing_ids and count_not_reduced
        checks[name] = {
            "status": "PASS" if passed else "FAIL",
            "previous_count": int(previous.get("records_count") or 0),
            "current_count": int(observed.get("records_count") or 0),
            "missing_record_ids": missing_ids,
            "count_not_reduced": count_not_reduced,
        }
        if not passed:
            failed_objects.append(name)

    return {
        "status": "PASS" if not failed_objects else "FAIL",
        "checks": checks,
        "failed_objects": failed_objects,
        "failure_reason": (
            None
            if not failed_objects
            else "previously_persisted_organizational_memory_missing_after_startup"
        ),
    }


def verify_and_update_organizational_memory_continuity(
    *,
    deployment_id: Optional[str] = None,
) -> Dict[str, Any]:
    """Verify the previous durable baseline and advance it only on PASS."""
    base = ensure_repository()
    baseline_file = base / BASELINE_PATH
    reports_file = base / REPORT_PATH
    current = build_organizational_memory_fingerprint()
    baseline = _read_json(baseline_file, {})
    seeded = not isinstance(baseline, dict) or not baseline.get("objects")

    comparison = (
        {
            "status": "PASS",
            "checks": {},
            "failed_objects": [],
            "failure_reason": None,
        }
        if seeded
        else _compare_fingerprints(baseline, current)
    )
    persistence = get_persistence_status(base)
    durable = (
        persistence.get("status") == "PASS"
        and persistence.get("source_of_truth") == "database"
        and persistence.get("durable_across_deploys") is True
    )
    status = "PASS" if comparison["status"] == "PASS" and durable else "FAIL"
    failure_reason = comparison.get("failure_reason")
    if not durable:
        failure_reason = "durable_database_source_of_truth_required"

    report = {
        "report_id": f"OMC-{_checksum([deployment_id, current.get('captured_at')])[:12].upper()}",
        "status": status,
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "deployment_id": deployment_id,
        "baseline_seeded": seeded,
        "source_of_truth": persistence.get("source_of_truth"),
        "durable_across_deploys": durable,
        "protected_objects": list(PROTECTED_OBJECTS),
        "checks": comparison.get("checks") or {},
        "failed_objects": comparison.get("failed_objects") or [],
        "failure_reason": failure_reason,
        "verified_at": _now(),
    }

    reports = _read_json(reports_file, [])
    if not isinstance(reports, list):
        reports = []
    reports.append(report)
    _write_json(reports_file, reports[-100:])

    if status == "PASS":
        _write_json(
            baseline_file,
            {
                **current,
                "baseline_id": report["report_id"],
                "deployment_id": deployment_id,
                "verified_at": report["verified_at"],
            },
        )

    readback = _read_json(reports_file, [])
    readback_ok = isinstance(readback, list) and any(
        isinstance(item, dict) and item.get("report_id") == report["report_id"]
        for item in readback
    )
    return {
        **report,
        "readback_status": "PASS" if readback_ok else "FAIL",
        "baseline_updated": status == "PASS",
    }


def get_organizational_memory_continuity_status() -> Dict[str, Any]:
    base = ensure_repository()
    baseline = _read_json(base / BASELINE_PATH, {})
    reports = _read_json(base / REPORT_PATH, [])
    latest = reports[-1] if isinstance(reports, list) and reports else {}
    return {
        "status": latest.get("status") or "NOT_VERIFIED",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "source_of_truth": latest.get("source_of_truth"),
        "durable_across_deploys": latest.get("durable_across_deploys") is True,
        "baseline_id": baseline.get("baseline_id") if isinstance(baseline, dict) else None,
        "protected_objects": list(PROTECTED_OBJECTS),
        "failed_objects": latest.get("failed_objects") or [],
        "failure_reason": latest.get("failure_reason"),
        "verified_at": latest.get("verified_at"),
        "read_only": True,
    }
