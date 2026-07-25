"""Executable Professional Business Model (PBM) foundation for VECTRA.

PROGRAM-002 / PBM-FOUNDATION-001

This module defines the canonical, machine-readable professional model used by
VECTRA to understand a Business Domain before Business Data are requested.  It
is deliberately separated from raw metrics and from transient chat context.
"""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List

RELEASE_ID = "VECTRA-PBM-FOUNDATION-001-INCREMENT-005"
CONTRACT_VERSION = "1.4"
MODEL_ID = "VECTRA-PROFESSIONAL-BUSINESS-MODEL"
DEFAULT_DOMAIN_ID = "bon_buasson"

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2] / "assistant_repository" / "business_domains"
_REQUIRED_COMPONENTS = [
    "business_passport",
    "business_identity",
    "business_operating_model",
    "business_object_model",
    "business_research_model",
    "business_workspace_model",
    "executive_brief_model",
    "business_decision_model",
    "external_context_model",
    "business_vocabulary_model",
    "business_knowledge_model",
    "business_learning_model",
    "professional_understanding_model",
    "professional_role_model",
    "decision_acceleration_model",
    "canonical_business_model",
    "modern_trade_model",
    "dialogue_business_research_program",
]


def _model_path(domain_id: str) -> Path:
    return _REPOSITORY_ROOT / domain_id / "professional_business_model.json"


def _load(domain_id: str) -> Dict[str, Any]:
    path = _model_path(domain_id)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def get_professional_business_model(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    domain_key = str(domain_id or DEFAULT_DOMAIN_ID).strip().lower()
    model = _load(domain_key)
    if not model:
        return {
            "status": "NOT_FOUND",
            "business_domain": domain_key,
            "model_id": MODEL_ID,
            "read_only": True,
        }
    return {
        "status": "PASS",
        "business_domain": domain_key,
        "model_id": model.get("model_id") or MODEL_ID,
        "model_version": model.get("version"),
        "professional_business_model": deepcopy(model),
        "business_data_connected": False,
        "business_data_policy": "ON_DEMAND",
        "source_of_truth": str(_model_path(domain_key).relative_to(Path(__file__).resolve().parents[2])),
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
    }


def get_professional_business_model_summary(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_professional_business_model(domain_id)
    model = result.get("professional_business_model") if result.get("status") == "PASS" else {}
    components = model.get("components") if isinstance(model, dict) else {}
    if not isinstance(components, dict):
        components = {}
    return {
        "status": result.get("status"),
        "business_domain": result.get("business_domain"),
        "model_id": result.get("model_id"),
        "model_version": result.get("model_version"),
        "display_name": model.get("display_name") if isinstance(model, dict) else None,
        "purpose": model.get("purpose") if isinstance(model, dict) else None,
        "common_vector": model.get("common_vector") if isinstance(model, dict) else None,
        "component_count": len(components),
        "components": list(components.keys()),
        "current_research_area": ((components.get("business_operating_model") or {}).get("current_primary_research_area")),
        "business_data_policy": result.get("business_data_policy"),
        "read_only": True,
    }


def build_professional_business_runtime_projection(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_professional_business_model(domain_id)
    if result.get("status") != "PASS":
        return result
    model = result.get("professional_business_model") or {}
    components = model.get("components") or {}
    restoration = model.get("restoration_contract") or {}
    from app.assistant_runtime.object_passport_contract import get_object_passport_contract
    from app.assistant_runtime.business_research_execution_model import get_business_research_execution_model
    object_passport_contract = get_object_passport_contract(result.get("business_domain") or domain_id)
    research_execution_model = get_business_research_execution_model(result.get("business_domain") or domain_id)
    from app.assistant_runtime.business_understanding_runtime import (
        get_professional_understanding_state,
        build_business_structure_view,
    )
    understanding_state = get_professional_understanding_state(result.get("business_domain") or domain_id)
    structure_view = build_business_structure_view(result.get("business_domain") or domain_id)
    from app.assistant_runtime.canonical_business_model import build_business_identity_view
    from app.assistant_runtime.modern_trade_model import build_modern_trade_structure_view
    canonical_identity = build_business_identity_view(result.get("business_domain") or domain_id)
    modern_trade_structure = build_modern_trade_structure_view(result.get("business_domain") or domain_id)
    from app.assistant_runtime.dialogue_business_research_program import (
        get_dialogue_research_state, build_active_research_prompt,
    )
    dialogue_research_state = get_dialogue_research_state(result.get("business_domain") or domain_id)
    active_research_prompt = build_active_research_prompt(result.get("business_domain") or domain_id)
    return {
        "status": "PASS",
        "business_domain": result.get("business_domain"),
        "model_id": model.get("model_id"),
        "version": model.get("version"),
        "display_name": model.get("display_name"),
        "purpose": model.get("purpose"),
        "common_vector": deepcopy(model.get("common_vector") or {}),
        "executable_contracts": {
            "object_passport_contract": object_passport_contract,
            "business_research_execution_model": research_execution_model,
            "professional_understanding_state": understanding_state,
            "business_structure_view": structure_view,
            "canonical_business_identity": canonical_identity,
            "modern_trade_structure": modern_trade_structure,
            "dialogue_research_state": dialogue_research_state,
            "active_research_prompt": active_research_prompt,
        },
        "restored_components": {
            key: deepcopy(components.get(key) or {})
            for key in restoration.get("restore_before_business_data", _REQUIRED_COMPONENTS)
        },
        "business_data": {
            "status": "NOT_CONNECTED",
            "policy": restoration.get("business_data_policy") or "ON_DEMAND",
            "reason": "Professional business understanding is restored before factual Business Data are requested.",
        },
        "professional_readiness": {
            "identity_ready": bool(components.get("business_identity")),
            "operating_model_ready": bool(components.get("business_operating_model")),
            "research_model_ready": bool(components.get("business_research_model")),
            "workspace_model_ready": bool(components.get("business_workspace_model")),
            "decision_model_ready": bool(components.get("business_decision_model")),
            "external_context_policy_ready": bool(components.get("external_context_model")),
        },
        "release": RELEASE_ID,
        "read_only": True,
    }


def verify_professional_business_model(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_professional_business_model(domain_id)
    model = result.get("professional_business_model") if result.get("status") == "PASS" else {}
    components = model.get("components") if isinstance(model, dict) else {}
    if not isinstance(components, dict):
        components = {}

    checks: Dict[str, bool] = {
        "model_available": result.get("status") == "PASS",
        "model_identity_valid": model.get("model_id") == MODEL_ID if isinstance(model, dict) else False,
        "domain_bound": model.get("business_domain") == str(domain_id or DEFAULT_DOMAIN_ID).strip().lower() if isinstance(model, dict) else False,
        "common_vector_defined": bool(model.get("common_vector")) if isinstance(model, dict) else False,
        "restoration_contract_defined": bool(model.get("restoration_contract")) if isinstance(model, dict) else False,
    }
    for component in _REQUIRED_COMPONENTS:
        checks[f"component_{component}"] = isinstance(components.get(component), dict) and bool(components.get(component))

    object_model = components.get("business_object_model") or {}
    research_model = components.get("business_research_model") or {}
    workspace_model = components.get("business_workspace_model") or {}
    decision_model = components.get("business_decision_model") or {}
    external_model = components.get("external_context_model") or {}
    from app.assistant_runtime.object_passport_contract import verify_object_passport_contract
    from app.assistant_runtime.business_research_execution_model import verify_business_research_execution_model
    object_passport_verification = verify_object_passport_contract(domain_id)
    research_execution_verification = verify_business_research_execution_model(domain_id)
    from app.assistant_runtime.business_understanding_runtime import verify_business_understanding_runtime
    from app.assistant_runtime.canonical_business_model import verify_canonical_business_model
    from app.assistant_runtime.modern_trade_model import verify_modern_trade_model
    from app.assistant_runtime.dialogue_business_research_program import verify_dialogue_research_program
    understanding_verification = verify_business_understanding_runtime(domain_id)
    canonical_verification = verify_canonical_business_model(domain_id)
    modern_trade_verification = verify_modern_trade_model(domain_id)
    dialogue_research_verification = verify_dialogue_research_program(domain_id)
    checks.update({
        "sku_is_business_atom": object_model.get("business_atom") == "sku",
        "aggregation_path_defined": bool(object_model.get("aggregation_path")),
        "investigation_path_defined": bool(research_model.get("investigation_path")),
        "workspaces_are_professional_briefings": workspace_model.get("workspace_is_dashboard") is False,
        "contract_is_decision_object": decision_model.get("primary_commercial_decision_object") == "contract",
        "external_context_required": external_model.get("required") is True,
        "facts_and_hypotheses_separated": external_model.get("fact_hypothesis_separation_required") is True,
        "object_passport_contract_ready": object_passport_verification.get("status") == "PASS",
        "research_execution_model_ready": research_execution_verification.get("status") == "PASS",
        "professional_understanding_runtime_ready": understanding_verification.get("status") == "PASS",
        "canonical_business_model_ready": canonical_verification.get("status") == "PASS",
        "modern_trade_model_ready": modern_trade_verification.get("status") == "PASS",
        "dialogue_business_research_program_ready": dialogue_research_verification.get("status") == "PASS",
        "decision_acceleration_model_ready": bool(components.get("decision_acceleration_model")),
        "professional_role_model_ready": bool(components.get("professional_role_model")),
        "silent_research_fallback_forbidden": research_model.get("silent_mode_fallback_forbidden") is True,
        "runtime_traversal_not_business_research": research_model.get("runtime_traversal_is_business_research") is False,
    })
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "business_domain": str(domain_id or DEFAULT_DOMAIN_ID).strip().lower(),
        "checks": checks,
        "missing_or_failed": [name for name, passed in checks.items() if not passed],
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
    }
