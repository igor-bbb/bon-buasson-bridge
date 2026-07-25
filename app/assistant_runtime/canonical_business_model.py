"""Canonical, evidence-classified business model for Bon Buasson.

PBM Foundation / Increment 004.
This module restores confirmed business facts, approved professional conclusions,
strategic intentions and explicit unknowns without connecting Business Data.
"""
from __future__ import annotations
import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict

RELEASE_ID = "VECTRA-PBM-FOUNDATION-001-INCREMENT-004"
DEFAULT_DOMAIN_ID = "bon_buasson"
_ROOT = Path(__file__).resolve().parents[2] / "assistant_repository" / "business_domains"


def _path(domain_id: str) -> Path:
    return _ROOT / str(domain_id or DEFAULT_DOMAIN_ID).strip().lower() / "canonical_business_model.json"


def get_canonical_business_model(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    path = _path(domain_id)
    if not path.exists():
        return {"status": "NOT_FOUND", "business_domain": domain_id, "read_only": True}
    try:
        model = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"status": "HOLD", "reason": "canonical_business_model_unreadable", "read_only": True}
    return {"status": "PASS", "business_domain": domain_id, "canonical_business_model": deepcopy(model), "business_data_connected": False, "release": RELEASE_ID, "read_only": True}


def build_business_identity_view(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_canonical_business_model(domain_id)
    model = result.get("canonical_business_model") or {}
    return {
        "status": result.get("status"),
        "business_domain": domain_id,
        "display_name": model.get("display_name"),
        "confirmed_facts": deepcopy(model.get("confirmed_facts") or []),
        "professional_conclusions": deepcopy(model.get("professional_conclusions") or []),
        "strategic_intentions": deepcopy(model.get("strategic_intentions") or []),
        "unknowns": deepcopy(model.get("unknowns") or []),
        "decision_vector": deepcopy(model.get("decision_vector") or {}),
        "honesty_rule": "Unknowns remain explicit and are never completed by inference.",
        "release": RELEASE_ID,
        "read_only": True,
    }


def verify_canonical_business_model(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_canonical_business_model(domain_id)
    model = result.get("canonical_business_model") or {}
    classes = ((model.get("evidence_policy") or {}).get("classes") or [])
    checks = {
        "model_available": result.get("status") == "PASS",
        "domain_bound": model.get("business_domain") == domain_id,
        "evidence_classes_complete": set(classes) == {"CONFIRMED_FACT", "PROFESSIONAL_CONCLUSION", "STRATEGIC_INTENTION", "UNKNOWN"},
        "confirmed_facts_present": bool(model.get("confirmed_facts")),
        "professional_conclusions_present": bool(model.get("professional_conclusions")),
        "strategic_intentions_present": bool(model.get("strategic_intentions")),
        "unknowns_explicit": bool(model.get("unknowns")),
        "decision_vector_defined": bool(model.get("decision_vector")),
        "business_data_not_embedded": "business_data" not in model,
    }
    return {"status": "PASS" if all(checks.values()) else "HOLD", "checks": checks, "release": RELEASE_ID, "read_only": True}
