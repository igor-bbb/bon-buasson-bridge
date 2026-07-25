"""Executable object passport contract for VECTRA Professional Business Model.

PROGRAM-002 / PBM-FOUNDATION-001 / INCREMENT-003

The contract gives every business object a professional meaning.  It prevents
VECTRA from treating the hierarchy as mere navigation and defines the minimum
information required to understand, investigate and explain each object.
"""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, Iterable, List

RELEASE_ID = "VECTRA-PBM-FOUNDATION-001-INCREMENT-003"
CONTRACT_VERSION = "1.1"
DEFAULT_DOMAIN_ID = "bon_buasson"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2] / "assistant_repository" / "business_domains"

_REQUIRED_TYPES = ["business", "top_manager", "manager", "contract", "category", "tmc_group", "sku"]
_REQUIRED_SECTIONS = [
    "identity",
    "management_meaning",
    "responsibility",
    "relationships",
    "business_state",
    "dynamics",
    "causal_explanation",
    "financial_effect",
    "risks_and_opportunities",
    "external_context",
    "decision_context",
    "research_route",
    "evidence_state",
]


def _path(domain_id: str) -> Path:
    return _REPOSITORY_ROOT / domain_id / "object_passport_contract.json"


def _load(domain_id: str) -> Dict[str, Any]:
    path = _path(domain_id)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def get_object_passport_contract(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    domain_key = str(domain_id or DEFAULT_DOMAIN_ID).strip().lower()
    contract = _load(domain_key)
    if not contract:
        return {"status": "NOT_FOUND", "business_domain": domain_key, "read_only": True}
    return {
        "status": "PASS",
        "business_domain": domain_key,
        "contract_id": contract.get("contract_id"),
        "contract_version": contract.get("version"),
        "object_passport_contract": deepcopy(contract),
        "source_of_truth": str(_path(domain_key).relative_to(Path(__file__).resolve().parents[2])),
        "release": RELEASE_ID,
        "read_only": True,
    }


def get_object_type_contract(object_type: str, domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_object_passport_contract(domain_id)
    if result.get("status") != "PASS":
        return result
    key = str(object_type or "").strip().lower()
    if key == "network_contract":
        key = "contract"
    contract = result["object_passport_contract"]
    object_types = contract.get("object_types") or {}
    if key not in object_types:
        return {
            "status": "VALIDATION_ERROR",
            "reason": "unsupported_object_type",
            "object_type": key,
            "supported_object_types": list(object_types.keys()),
            "read_only": True,
        }
    return {
        "status": "PASS",
        "business_domain": result.get("business_domain"),
        "object_type": key,
        "object_type_contract": deepcopy(object_types[key]),
        "global_sections": deepcopy(contract.get("required_sections") or []),
        "release": RELEASE_ID,
        "read_only": True,
    }


def build_object_passport_template(
    object_type: str,
    object_id: str = "",
    display_name: str = "",
    domain_id: str = DEFAULT_DOMAIN_ID,
) -> Dict[str, Any]:
    result = get_object_type_contract(object_type, domain_id)
    if result.get("status") != "PASS":
        return result
    type_contract = result["object_type_contract"]
    required_sections = result.get("global_sections") or _REQUIRED_SECTIONS
    passport = {
        "passport_contract_version": CONTRACT_VERSION,
        "business_domain": domain_id,
        "object_type": str(object_type).strip().lower(),
        "object_id": str(object_id or "").strip() or None,
        "display_name": str(display_name or "").strip() or None,
        "professional_role": type_contract.get("professional_role"),
        "management_question": type_contract.get("management_question"),
        "decision_purpose": type_contract.get("decision_purpose"),
        "sections": {section: None for section in required_sections},
        "data_state": {
            "business_data_connected": False,
            "evidence_available": False,
            "external_context_available": False,
            "unknowns_must_remain_explicit": True,
        },
        "readiness": "EMPTY_TEMPLATE",
        "read_only": True,
    }
    passport["sections"]["identity"] = {
        "object_type": passport["object_type"],
        "object_id": passport["object_id"],
        "display_name": passport["display_name"],
    }
    passport["sections"]["management_meaning"] = type_contract.get("management_meaning")
    passport["sections"]["responsibility"] = type_contract.get("responsibility")
    passport["sections"]["relationships"] = deepcopy(type_contract.get("relationships") or {})
    passport["sections"]["research_route"] = deepcopy(type_contract.get("research_route") or {})
    return {"status": "PASS", "object_passport": passport, "release": RELEASE_ID, "read_only": True}


def validate_object_passport(passport: Dict[str, Any], domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    passport = passport if isinstance(passport, dict) else {}
    object_type = str(passport.get("object_type") or "").strip().lower()
    type_result = get_object_type_contract(object_type, domain_id)
    if type_result.get("status") != "PASS":
        return type_result
    sections = passport.get("sections") if isinstance(passport.get("sections"), dict) else {}
    missing_sections = [section for section in _REQUIRED_SECTIONS if section not in sections]
    empty_critical = [
        section
        for section in ("identity", "management_meaning", "responsibility", "relationships", "research_route")
        if not sections.get(section)
    ]
    unknowns_explicit = bool((passport.get("data_state") or {}).get("unknowns_must_remain_explicit"))
    checks = {
        "supported_object_type": object_type in _REQUIRED_TYPES,
        "object_identity_present": bool(passport.get("object_id") or passport.get("display_name")),
        "all_required_sections_present": not missing_sections,
        "critical_professional_sections_filled": not empty_critical,
        "unknowns_explicit": unknowns_explicit,
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "object_type": object_type,
        "checks": checks,
        "missing_sections": missing_sections,
        "empty_critical_sections": empty_critical,
        "release": RELEASE_ID,
        "read_only": True,
    }


def verify_object_passport_contract(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_object_passport_contract(domain_id)
    contract = result.get("object_passport_contract") if result.get("status") == "PASS" else {}
    object_types = contract.get("object_types") if isinstance(contract, dict) else {}
    required_sections = contract.get("required_sections") if isinstance(contract, dict) else []
    checks: Dict[str, bool] = {
        "contract_available": result.get("status") == "PASS",
        "domain_bound": contract.get("business_domain") == str(domain_id or DEFAULT_DOMAIN_ID).strip().lower() if isinstance(contract, dict) else False,
        "all_object_types_defined": all(key in object_types for key in _REQUIRED_TYPES) if isinstance(object_types, dict) else False,
        "all_sections_defined": all(section in required_sections for section in _REQUIRED_SECTIONS) if isinstance(required_sections, list) else False,
        "sku_is_maximum_passport": contract.get("maximum_passport_object") == "sku" if isinstance(contract, dict) else False,
        "contract_is_primary_decision_object": contract.get("primary_commercial_decision_object") == "contract" if isinstance(contract, dict) else False,
    }
    for key in _REQUIRED_TYPES:
        item = object_types.get(key) if isinstance(object_types, dict) else {}
        checks[f"{key}_management_meaning"] = bool((item or {}).get("management_meaning"))
        checks[f"{key}_research_route"] = bool((item or {}).get("research_route"))
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "business_domain": str(domain_id or DEFAULT_DOMAIN_ID).strip().lower(),
        "checks": checks,
        "missing_or_failed": [name for name, passed in checks.items() if not passed],
        "release": RELEASE_ID,
        "read_only": True,
    }
