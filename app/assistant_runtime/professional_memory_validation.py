"""MEMORY-IMPL-0015 End-to-End Professional Memory Validation.

Final read-only validation of the Professional Memory program. It combines
classification, repository readback, health, architecture conformance, recovery
optimization, revisions and release history into one Laboratory-ready report.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List

from app.assistant_runtime.architecture_conformance import verify_architecture_conformance
from app.assistant_runtime.memory_classification import verify_automatic_classification
from app.assistant_runtime.memory_health import verify_memory_health
from app.assistant_runtime.memory_inspection import get_memory_readback_report, get_memory_statistics
from app.assistant_runtime.memory_repository import get_memory_overview, list_memory_objects, verify_memory_repository_integrity
from app.assistant_runtime.recovery_optimization import verify_recovery_optimization, build_compact_recovery_context
from app.assistant_runtime.release_history_runtime import verify_release_history_readback
from app.assistant_runtime.revision_model import verify_revision_model

PROFESSIONAL_MEMORY_VALIDATION_RELEASE = "MEMORY-IMPL-0015"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _sample_classification_payload() -> Dict[str, Any]:
    return {
        "items": [
            {"title": "Professional Memory validation sample", "description": "Confirmed professional product experience must persist between sessions.", "status": "confirmed", "recommended_memory_type": "professional_knowledge"},
            {"title": "Business Domain validation sample", "description": "Confirmed Bon Boisson domain knowledge must stay separate from professional product knowledge.", "status": "confirmed", "recommended_memory_type": "business_knowledge", "business_domain": "bon_buasson"},
            {"title": "Draft hypothesis sample", "description": "Unconfirmed draft must not be capitalized as permanent memory.", "status": "draft", "recommended_memory_type": "hypothesis"},
        ]
    }


def run_professional_memory_e2e_validation(domain: str = "bon_buasson") -> Dict[str, Any]:
    overview = get_memory_overview(domain=domain)
    objects = list_memory_objects(domain=domain, limit=10000).get("objects", [])
    statistics = get_memory_statistics(domain=domain)
    readback = get_memory_readback_report(domain=domain, limit=1000)
    integrity = verify_memory_repository_integrity(domain=domain)
    health = verify_memory_health(domain=domain)
    revision = verify_revision_model(active_objects=objects)
    release_history = verify_release_history_readback()
    conformance = verify_architecture_conformance(domain=domain)
    recovery = verify_recovery_optimization(domain=domain)
    compact_context = build_compact_recovery_context(domain=domain)
    classification = verify_automatic_classification(_sample_classification_payload(), domain=domain)

    steps = [
        {"step_id": "classification", "title": "Automatic Knowledge Classification", "status": classification.get("verification_status") or classification.get("status")},
        {"step_id": "repository", "title": "Unified Memory Repository", "status": integrity.get("verification_status")},
        {"step_id": "readback", "title": "Readback all memory spaces", "status": readback.get("readback_status") or readback.get("verification_status") or readback.get("status")},
        {"step_id": "revision", "title": "Revision and Version Model", "status": revision.get("verification_status")},
        {"step_id": "release_history", "title": "Release History Runtime", "status": release_history.get("verification_status")},
        {"step_id": "health", "title": "Memory Health", "status": health.get("verification_status")},
        {"step_id": "architecture_conformance", "title": "Architecture Conformance", "status": conformance.get("verification_status")},
        {"step_id": "recovery_optimization", "title": "Recovery Optimization", "status": recovery.get("verification_status")},
        {"step_id": "compact_recovery_context", "title": "Compact recovery context available", "status": "PASS" if compact_context.get("verification_status") == "PASS" else "FAIL"},
    ]
    failed = [step for step in steps if step.get("status") != "PASS"]
    status = "PASS" if not failed else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "program_validation_status": status,
        "render_mode": "vectra_professional_memory_e2e_validation",
        "release": PROFESSIONAL_MEMORY_VALIDATION_RELEASE,
        "program": "Professional Memory v1.0",
        "domain": domain,
        "steps_count": len(steps),
        "failed_steps_count": len(failed),
        "failed_steps": failed,
        "steps": steps,
        "objects_count": overview.get("objects_count"),
        "readable_objects_count": overview.get("readable_objects_count"),
        "mapping_errors_count": overview.get("mapping_errors_count"),
        "memory_spaces_used": overview.get("memory_spaces_used"),
        "statistics": statistics,
        "compact_recovery_context_summary": {
            "available": compact_context.get("verification_status") == "PASS",
            "included_objects_count": compact_context.get("response_compaction", {}).get("included_objects_count"),
            "large_response_protection": compact_context.get("response_compaction", {}).get("large_response_protection"),
        },
        "definition_of_program_done": {
            "p0_complete": True,
            "p1_complete": True,
            "p2_complete": status == "PASS",
            "memory_preserved_between_releases": integrity.get("verification_status") == "PASS",
            "regression_verification": "PASS" if status == "PASS" else "FAIL",
            "new_session_recovery_ready": recovery.get("verification_status") == "PASS",
        },
        "blocking_issues": [step.get("step_id") for step in failed],
        "generated_at": _now(),
    }


def verify_professional_memory_program(domain: str = "bon_buasson") -> Dict[str, Any]:
    report = run_professional_memory_e2e_validation(domain=domain)
    return {
        "status": report.get("program_validation_status"),
        "verification_status": report.get("program_validation_status"),
        "program_validation_status": report.get("program_validation_status"),
        "render_mode": "vectra_professional_memory_program_verification",
        "release": PROFESSIONAL_MEMORY_VALIDATION_RELEASE,
        "program": "Professional Memory v1.0",
        "domain": domain,
        "failed_steps_count": report.get("failed_steps_count"),
        "blocking_issues": report.get("blocking_issues", []),
        "definition_of_program_done": report.get("definition_of_program_done"),
        "report": report,
    }
