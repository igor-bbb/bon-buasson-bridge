"""Compact read-only Product Verification diagnostics for Release 47.

This module executes canonical component verification functions and returns only
small, evidence-oriented summaries suitable for GPT Actions. It deliberately
avoids full manifests and large Runtime snapshots to prevent ResponseTooLargeError.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Callable, Dict

from app.assistant_runtime.professional_business_model import verify_professional_business_model
from app.assistant_runtime.object_passport_contract import verify_object_passport_contract
from app.assistant_runtime.canonical_business_model import verify_canonical_business_model
from app.assistant_runtime.business_research_execution_model import verify_business_research_execution_model
from app.assistant_runtime.dialogue_business_research_program import verify_dialogue_research_program
from app.assistant_runtime.modern_trade_model import verify_modern_trade_model
from app.assistant_runtime.vectra_core_ontology import verify_core_ontology
from app.assistant_runtime.self_governance_runtime import verify_self_governance_runtime
from app.assistant_runtime.architecture_conformance import verify_architecture_conformance

CONTRACT_VERSION = "release47_product_verification.compact.v1"
RELEASE_ID = "VECTRA-RELEASE-47-PBM-FOUNDATION-001-FINAL"
DEFAULT_DOMAIN_ID = "bon_buasson"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _status(value: Any) -> str:
    if not isinstance(value, dict):
        return "UNKNOWN"
    for key in ("status", "verification_status", "overall_status", "result"):
        raw = value.get(key)
        if isinstance(raw, str):
            normalized = raw.upper()
            if normalized in {"OK", "READY", "HEALTHY", "VERIFIED"}:
                return "PASS"
            return normalized
    checks = value.get("checks")
    if isinstance(checks, dict) and checks:
        return "PASS" if all(bool(v) for v in checks.values()) else "FAIL"
    return "UNKNOWN"


def _compact_evidence(value: Any) -> Dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    evidence: Dict[str, Any] = {}
    checks = value.get("checks")
    if isinstance(checks, dict):
        evidence["checks_total"] = len(checks)
        evidence["checks_passed"] = sum(1 for item in checks.values() if bool(item))
        failed = [str(name) for name, item in checks.items() if not bool(item)]
        if failed:
            evidence["failed_checks"] = failed[:10]
    for key in ("domain_id", "model_id", "contract_id", "program_id", "release_id", "version"):
        if key in value and isinstance(value.get(key), (str, int, float, bool)):
            evidence[key] = value.get(key)
    return evidence


def _run(name: str, fn: Callable[[], Any]) -> Dict[str, Any]:
    try:
        result = fn()
        return {
            "component": name,
            "status": _status(result),
            "executed": True,
            "error": None,
            "evidence": _compact_evidence(result),
        }
    except Exception as exc:
        return {
            "component": name,
            "status": "FAIL",
            "executed": True,
            "error": f"{type(exc).__name__}: {exc}",
            "evidence": {},
        }


def collect_release_47_product_verification(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    """Execute compact, read-only component verification for Release 47."""
    components = {
        "professional_business_model": _run(
            "professional_business_model", lambda: verify_professional_business_model(domain_id)
        ),
        "object_passport": _run(
            "object_passport", lambda: verify_object_passport_contract(domain_id)
        ),
        "canonical_business_model": _run(
            "canonical_business_model", lambda: verify_canonical_business_model(domain_id)
        ),
        "business_research_execution": _run(
            "business_research_execution", lambda: verify_business_research_execution_model(domain_id)
        ),
        "dialogue_business_research": _run(
            "dialogue_business_research", lambda: verify_dialogue_research_program(domain_id)
        ),
        "modern_trade_model": _run(
            "modern_trade_model", lambda: verify_modern_trade_model(domain_id)
        ),
        "core_ontology": _run("core_ontology", verify_core_ontology),
        "runtime_governance": _run("runtime_governance", verify_self_governance_runtime),
        "architecture_conformance": _run(
            "architecture_conformance", lambda: verify_architecture_conformance(domain_id)
        ),
    }

    failed = [name for name, item in components.items() if item["status"] == "FAIL"]
    unconfirmed = [name for name, item in components.items() if item["status"] not in {"PASS", "FAIL"}]
    overall = "FAIL" if failed else ("NOT_VERIFIED" if unconfirmed else "PASS")

    return {
        "status": overall,
        "contract_version": CONTRACT_VERSION,
        "release_id": RELEASE_ID,
        "generated_at": _now(),
        "read_only": True,
        "domain_id": domain_id,
        "components": components,
        "summary": {
            "total": len(components),
            "passed": sum(1 for item in components.values() if item["status"] == "PASS"),
            "failed": failed,
            "not_verified": unconfirmed,
        },
    }
