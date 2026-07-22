"""Executable professional model of Bon Buasson Modern Trade.

PBM Foundation / Increment 004.
The model separates confirmed current structure, approved target transition,
known partial areas and explicit unknowns. It does not claim a complete current
organisation chart where Product Owner has not yet confirmed one.
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
    return _ROOT / str(domain_id or DEFAULT_DOMAIN_ID).strip().lower() / "modern_trade_model.json"


def get_modern_trade_model(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    path = _path(domain_id)
    if not path.exists():
        return {"status": "NOT_FOUND", "business_domain": domain_id, "read_only": True}
    try:
        model = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"status": "HOLD", "reason": "modern_trade_model_unreadable", "read_only": True}
    return {"status": "PASS", "business_domain": domain_id, "modern_trade_model": deepcopy(model), "business_data_connected": False, "release": RELEASE_ID, "read_only": True}


def build_modern_trade_structure_view(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_modern_trade_model(domain_id)
    model = result.get("modern_trade_model") or {}
    return {
        "status": result.get("status"),
        "business_domain": domain_id,
        "area": "modern_trade",
        "current_structure": deepcopy(model.get("confirmed_current_structure") or {}),
        "target_transition": deepcopy(model.get("approved_target_transition") or {}),
        "core_business_objects": deepcopy(model.get("core_business_objects") or []),
        "knowledge_state": deepcopy(model.get("knowledge_state") or {}),
        "honesty_rule": "Current and target structures are never merged into one factual state.",
        "release": RELEASE_ID,
        "read_only": True,
    }


def verify_modern_trade_model(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_modern_trade_model(domain_id)
    model = result.get("modern_trade_model") or {}
    current = model.get("confirmed_current_structure") or {}
    target = model.get("approved_target_transition") or {}
    contract = model.get("contract_model") or {}
    paths = model.get("research_paths") or {}
    checks = {
        "model_available": result.get("status") == "PASS",
        "area_bound": model.get("area_id") == "modern_trade",
        "current_structure_present": bool(current),
        "target_transition_separated": bool(target) and target.get("status") == "APPROVED_DIRECTION_NOT_FULLY_IMPLEMENTED",
        "contract_is_universal_object": contract.get("type") == "contract",
        "contract_specialization_is_network": contract.get("specialization") == "network_contract",
        "sku_in_aggregation_path": (paths.get("aggregation") or [None])[0] == "sku",
        "role_efficiency_defined": bool(model.get("professional_role_efficiency")),
        "unknowns_explicit": bool((model.get("knowledge_state") or {}).get("unknown")),
        "dialogue_research_contract_defined": bool(model.get("dialogue_research_contract")),
        "business_data_not_embedded": "business_data" not in model,
    }
    return {"status": "PASS" if all(checks.values()) else "HOLD", "checks": checks, "release": RELEASE_ID, "read_only": True}
