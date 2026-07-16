"""VECTRA Self Model Runtime.

Connects VECTRA's current understanding of itself to the unified Runtime State.
The Self Model is not a replacement for Personality Core. Personality answers
"who am I"; Self Model answers "where am I now, what can I do, and what is my
confirmed professional state".
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.assistant_runtime.durable_runtime_state import (
    read_unified_runtime_state,
    update_unified_runtime_root,
)
from app.assistant_runtime.business_domain_profile import get_business_domain_professional_model
from app.assistant_runtime.self_governance_runtime import get_governance_gate
from app.assistant_runtime.vectra_canonical_model import get_vectra_canonical_model

from app.assistant_runtime.personality_runtime import (
    get_capability_understanding,
    get_personality_core,
)

RELEASE_ID = "VECTRA-COGNITIVE-RUNTIME-V1-WP-003"
CONTRACT_VERSION = "1.0"
SELF_MODEL_VERSION = "1.0"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _resolve_workspace(professional_role: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    explicit = str(payload.get("workspace") or payload.get("workspace_type") or "").strip().lower()
    role = str(professional_role or "vectra_laboratory").strip().lower()
    if explicit:
        workspace_id = explicit
        resolution = "explicit_workspace"
    elif "laboratory" in role:
        workspace_id = "laboratory"
        resolution = "professional_role_mapping"
    else:
        workspace_id = "workspace"
        resolution = "professional_role_mapping"
    purpose = {
        "laboratory": "Исследование, Product Verification и выявление подтверждённых ограничений.",
        "workspace": "Совместная профессиональная работа с бизнесом и поддержка управленческих решений.",
    }.get(workspace_id, "Профессиональная рабочая среда VECTRA.")
    return {
        "workspace_id": workspace_id,
        "display_name": "VECTRA Laboratory" if workspace_id == "laboratory" else "VECTRA Workspace",
        "purpose": purpose,
        "resolution": resolution,
    }


def build_self_model(
    payload: Optional[Dict[str, Any]] = None,
    *,
    active_business_domain: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    professional_role = str(payload.get("professional_role") or "vectra_laboratory").strip().lower()
    personality = (get_personality_core().get("personality_core") or {})
    capabilities = get_capability_understanding()
    capability_items = capabilities.get("capabilities") if isinstance(capabilities, dict) else []
    confirmed_capabilities: List[Dict[str, Any]] = []
    for item in capability_items if isinstance(capability_items, list) else []:
        if not isinstance(item, dict) or not item.get("understood_by_vectra"):
            continue
        confirmed_capabilities.append({
            "capability_id": item.get("capability_id"),
            "title": item.get("title"),
            "purpose": item.get("purpose"),
            "status": item.get("status"),
        })

    current_state = personality.get("current_state") if isinstance(personality, dict) else {}
    limitation = current_state.get("confirmed_limitation") if isinstance(current_state, dict) else None
    domain = active_business_domain if isinstance(active_business_domain, dict) else {}
    workspace = _resolve_workspace(professional_role, payload)
    canonical_model = get_vectra_canonical_model().get("canonical_model") or {}
    domain_id = domain.get("domain_id") or "bon_buasson"
    business_model = get_business_domain_professional_model(domain_id)
    governance_gate = get_governance_gate()

    return {
        "self_model_id": "VECTRA-SELF-MODEL",
        "version": SELF_MODEL_VERSION,
        "status": "ACTIVE",
        "owner": "VECTRA",
        "identity_reference": (personality.get("personality_id") if isinstance(personality, dict) else None),
        "professional_role": professional_role,
        "current_workspace": workspace,
        "current_stage": (current_state.get("stage") if isinstance(current_state, dict) else None),
        "confirmed_capabilities": confirmed_capabilities,
        "capability_summary": {
            "available": len(capability_items) if isinstance(capability_items, list) else 0,
            "understood": len(confirmed_capabilities),
            "not_integrated": capabilities.get("not_integrated") if isinstance(capabilities, dict) else [],
        },
        "confirmed_limitations": [limitation] if limitation else [],
        "active_business_domain": {
            "domain_id": domain.get("domain_id"),
            "display_name": domain.get("display_name"),
            "status": "ACTIVE" if domain.get("domain_id") else "NOT_RESOLVED",
        },
        "professional_status": "READY",
        "canonical_product_model": {
            "model_id": canonical_model.get("model_id"),
            "version": canonical_model.get("version"),
            "mission": canonical_model.get("mission"),
            "business_vector": canonical_model.get("business_vector"),
            "digital_organization": canonical_model.get("digital_organization"),
            "working_desktop": canonical_model.get("working_desktop"),
            "business_object_philosophy": canonical_model.get("business_object_philosophy"),
        },
        "business_domain_professional_model": business_model.get("professional_model") if business_model.get("status") == "PASS" else {},
        "self_governance": governance_gate,
        "self_understanding": {
            "knows_identity": bool(personality.get("identity") if isinstance(personality, dict) else None),
            "knows_workspace": bool(workspace.get("workspace_id")),
            "knows_capabilities": len(confirmed_capabilities) > 0,
            "knows_limitations": bool(limitation),
            "knows_business_context": bool(domain.get("domain_id")),
            "knows_product_mission": bool(canonical_model.get("mission")),
            "knows_digital_organization": bool(canonical_model.get("digital_organization")),
            "knows_business_professional_model": business_model.get("status") == "PASS",
            "knows_governance_commitments": governance_gate.get("status") in {"PASS", "HOLD"},
        },
        "updated_at": _now(),
        "contract_version": CONTRACT_VERSION,
        "release": RELEASE_ID,
    }


def persist_self_model_runtime_state(
    payload: Optional[Dict[str, Any]] = None,
    *,
    active_business_domain: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    model = build_self_model(payload, active_business_domain=active_business_domain)
    state, diagnostic = update_unified_runtime_root(
        "self_model",
        deepcopy(model),
        status="CONNECTED",
        source_of_truth="app.assistant_runtime.self_model_runtime",
    )
    root = state.get("self_model") if isinstance(state, dict) else {}
    readback = root.get("payload") if isinstance(root, dict) else {}
    verified = (
        isinstance(readback, dict)
        and readback.get("self_model_id") == model.get("self_model_id")
        and readback.get("version") == model.get("version")
        and root.get("status") == "CONNECTED"
    )
    return {
        "status": "PASS" if verified else "HOLD",
        "self_model_ready": verified,
        "self_model": readback,
        "runtime_root": "self_model",
        "runtime_state_contract_version": state.get("contract_version") if isinstance(state, dict) else None,
        "readback_verified": bool(diagnostic.get("readback_verified")),
        "diagnostic": diagnostic,
        "read_only": False,
    }


def get_self_model_runtime_state() -> Dict[str, Any]:
    state, diagnostic = read_unified_runtime_state()
    root = state.get("self_model") if isinstance(state, dict) else {}
    return {
        "status": "PASS" if isinstance(root, dict) and root.get("status") == "CONNECTED" else "NOT_READY",
        "self_model": root,
        "runtime_state_contract_version": state.get("contract_version") if isinstance(state, dict) else None,
        "diagnostic": diagnostic,
        "read_only": True,
    }


def verify_self_model_runtime() -> Dict[str, Any]:
    result = persist_self_model_runtime_state({"professional_role": "vectra_laboratory"})
    model = result.get("self_model") if isinstance(result, dict) else {}
    understanding = model.get("self_understanding") if isinstance(model, dict) else {}
    checks = {
        "self_model_connected": result.get("self_model_ready") is True,
        "identity_reference_available": bool(model.get("identity_reference") if isinstance(model, dict) else None),
        "workspace_understood": bool((model.get("current_workspace") or {}).get("workspace_id")) if isinstance(model, dict) else False,
        "capabilities_understood": bool(understanding.get("knows_capabilities")) if isinstance(understanding, dict) else False,
        "limitations_understood": bool(understanding.get("knows_limitations")) if isinstance(understanding, dict) else False,
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "checks": checks,
        "self_model_version": model.get("version") if isinstance(model, dict) else None,
        "release": RELEASE_ID,
        "read_only": True,
    }
