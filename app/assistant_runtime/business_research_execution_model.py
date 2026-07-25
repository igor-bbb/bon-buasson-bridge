"""Executable Business Research Execution Model for VECTRA PBM.

Separates technical framework verification from professional business research.
The model prevents a traversal of the object hierarchy from being reported as
an analysis of the actual business.
"""
from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any, Dict, List

RELEASE_ID = "VECTRA-PBM-FOUNDATION-001-INCREMENT-002"
CONTRACT_VERSION = "1.0"
DEFAULT_DOMAIN_ID = "bon_buasson"
_REPOSITORY_ROOT = Path(__file__).resolve().parents[2] / "assistant_repository" / "business_domains"

_MODE_ALIASES = {
    "runtime": "runtime_verification",
    "platform": "runtime_verification",
    "framework": "business_model_research",
    "model": "business_model_research",
    "business_model": "business_model_research",
    "data": "business_data_research",
    "business_data": "business_data_research",
    "business": "business_state_research",
    "state": "business_state_research",
    "business_state": "business_state_research",
    "workspace": "workspace_preparation",
    "brief": "workspace_preparation",
}


def _path(domain_id: str) -> Path:
    return _REPOSITORY_ROOT / domain_id / "business_research_execution_model.json"


def _load(domain_id: str) -> Dict[str, Any]:
    path = _path(domain_id)
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def get_business_research_execution_model(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    domain_key = str(domain_id or DEFAULT_DOMAIN_ID).strip().lower()
    model = _load(domain_key)
    if not model:
        return {"status": "NOT_FOUND", "business_domain": domain_key, "read_only": True}
    return {
        "status": "PASS",
        "business_domain": domain_key,
        "model_id": model.get("model_id"),
        "model_version": model.get("version"),
        "business_research_execution_model": deepcopy(model),
        "source_of_truth": str(_path(domain_key).relative_to(Path(__file__).resolve().parents[2])),
        "release": RELEASE_ID,
        "read_only": True,
    }


def resolve_research_mode(requested_mode: str, domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_business_research_execution_model(domain_id)
    if result.get("status") != "PASS":
        return result
    raw = str(requested_mode or "").strip().lower()
    mode = _MODE_ALIASES.get(raw, raw)
    modes = result["business_research_execution_model"].get("modes") or {}
    if mode not in modes:
        return {
            "status": "CLARIFICATION_REQUIRED",
            "reason": "ambiguous_or_unsupported_research_mode",
            "requested_mode": raw or None,
            "supported_modes": list(modes.keys()),
            "rule": "Do not silently reinterpret 'research business' as Runtime traversal.",
            "read_only": True,
        }
    return {
        "status": "PASS",
        "research_mode": mode,
        "mode_contract": deepcopy(modes[mode]),
        "business_domain": result.get("business_domain"),
        "release": RELEASE_ID,
        "read_only": True,
    }


def build_research_execution_plan(payload: Dict[str, Any], domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    requested_mode = str(payload.get("research_mode") or payload.get("mode") or "").strip()
    mode_result = resolve_research_mode(requested_mode, domain_id)
    if mode_result.get("status") != "PASS":
        return mode_result
    contract = mode_result["mode_contract"]
    period = str(payload.get("period") or "").strip()
    object_type = str(payload.get("object_type") or "business").strip().lower()
    object_id = str(payload.get("object_id") or payload.get("business_object") or "").strip()
    inputs = {
        "period_present": bool(period),
        "business_data_available": bool(payload.get("business_data_available", False)),
        "professional_business_model_available": bool(payload.get("professional_business_model_available", True)),
        "object_passport_available": bool(payload.get("object_passport_available", False)),
        "external_context_available": bool(payload.get("external_context_available", False)),
    }
    requirements = contract.get("requirements") or {}
    missing: List[str] = []
    for key, required in requirements.items():
        if required is True and not inputs.get(key):
            missing.append(key)
    status = "READY" if not missing else "HOLD"
    return {
        "status": status,
        "research_mode": mode_result.get("research_mode"),
        "business_domain": domain_id,
        "object_type": object_type,
        "object_id": object_id or None,
        "period": period or None,
        "professional_purpose": contract.get("purpose"),
        "allowed_sources": deepcopy(contract.get("allowed_sources") or []),
        "prohibited_claims": deepcopy(contract.get("prohibited_claims") or []),
        "execution_steps": deepcopy(contract.get("execution_steps") or []),
        "required_outcome": deepcopy(contract.get("required_outcome") or []),
        "input_state": inputs,
        "missing_requirements": missing,
        "completion_rule": contract.get("completion_rule"),
        "honesty_rule": contract.get("honesty_rule"),
        "next_action": contract.get("next_action_when_ready") if status == "READY" else contract.get("next_action_when_hold"),
        "release": RELEASE_ID,
        "read_only": True,
    }


def validate_research_outcome(payload: Dict[str, Any], domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    mode_result = resolve_research_mode(str(payload.get("research_mode") or ""), domain_id)
    if mode_result.get("status") != "PASS":
        return mode_result
    outcome = payload.get("outcome") if isinstance(payload.get("outcome"), dict) else {}
    required = mode_result["mode_contract"].get("required_outcome") or []
    missing = [key for key in required if not outcome.get(key)]
    false_business_claim = (
        mode_result.get("research_mode") in {"runtime_verification", "business_model_research"}
        and bool(outcome.get("business_conclusion"))
        and not bool(outcome.get("business_data_used"))
    )
    checks = {
        "required_outcome_complete": not missing,
        "no_business_claim_without_business_data": not false_business_claim,
        "limitations_explicit": bool(outcome.get("limitations")) or not missing,
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "research_mode": mode_result.get("research_mode"),
        "checks": checks,
        "missing_outcome_sections": missing,
        "false_business_claim_detected": false_business_claim,
        "release": RELEASE_ID,
        "read_only": True,
    }


def verify_business_research_execution_model(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_business_research_execution_model(domain_id)
    model = result.get("business_research_execution_model") if result.get("status") == "PASS" else {}
    modes = model.get("modes") if isinstance(model, dict) else {}
    required_modes = ["runtime_verification", "business_model_research", "business_data_research", "business_state_research", "workspace_preparation"]
    checks: Dict[str, bool] = {
        "model_available": result.get("status") == "PASS",
        "domain_bound": model.get("business_domain") == str(domain_id or DEFAULT_DOMAIN_ID).strip().lower() if isinstance(model, dict) else False,
        "all_modes_defined": all(mode in modes for mode in required_modes) if isinstance(modes, dict) else False,
        "silent_fallback_forbidden": model.get("silent_mode_fallback_forbidden") is True if isinstance(model, dict) else False,
        "runtime_is_not_business_research": model.get("runtime_traversal_is_business_research") is False if isinstance(model, dict) else False,
    }
    for mode in required_modes:
        contract = modes.get(mode) if isinstance(modes, dict) else {}
        checks[f"{mode}_purpose"] = bool((contract or {}).get("purpose"))
        checks[f"{mode}_outcome"] = bool((contract or {}).get("required_outcome"))
        checks[f"{mode}_completion_rule"] = bool((contract or {}).get("completion_rule"))
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "business_domain": str(domain_id or DEFAULT_DOMAIN_ID).strip().lower(),
        "checks": checks,
        "missing_or_failed": [name for name, passed in checks.items() if not passed],
        "release": RELEASE_ID,
        "read_only": True,
    }
