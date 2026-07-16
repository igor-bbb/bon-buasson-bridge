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
from app.assistant_runtime.personality_runtime import (
    get_capability_understanding,
    get_personality_core,
)

RELEASE_ID = "DIGITAL-COLLEAGUE-SPRINT-001-DC-001"
CONTRACT_VERSION = "1.0"
SELF_MODEL_VERSION = "1.1"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")




def _resolve_role_profile(professional_role: str) -> Dict[str, Any]:
    """Resolve a professional role without creating a second personality."""
    role_id = str(professional_role or "vectra_laboratory").strip().lower().replace(" ", "_")
    built_in = {
        "vectra_laboratory": {
            "display_name": "VECTRA Laboratory",
            "purpose": "Исследование, Product Verification и выявление подтверждённых ограничений.",
            "source": "built_in_workspace_role",
        },
        "digital_business_analyst": {
            "display_name": "Digital Business Analyst",
            "purpose": "Профессиональный анализ бизнеса и подготовка доказательных управленческих выводов.",
            "source": "built_in_digital_role",
        },
        "business_analyst": {
            "display_name": "Business Analyst",
            "purpose": "Исследование бизнеса, диагностика причин и подготовка управленческих рекомендаций.",
            "source": "built_in_digital_role",
        },
        "chief_engineer": {
            "display_name": "Chief Engineer",
            "purpose": "Инженерная реализация подтверждённых задач в отдельной инженерной среде.",
            "source": "built_in_external_role",
        },
        "commercial_assistant": {
            "display_name": "Commercial Assistant",
            "purpose": "Поддержка коммерческой работы и управленческих решений в бизнес-пространстве.",
            "source": "built_in_digital_role",
        },
    }
    profile = built_in.get(role_id)
    if profile is None:
        try:
            from app.assistant_runtime.digital_organization_registry import get_digital_professional_role
            result = get_digital_professional_role({"role_id": role_id})
            role = result.get("role") if isinstance(result, dict) else None
            if isinstance(role, dict):
                profile = {
                    "display_name": role.get("display_name") or role_id,
                    "purpose": role.get("purpose") or role.get("professional_responsibility"),
                    "source": "digital_organization_registry",
                }
        except Exception:
            profile = None
    profile = profile or {
        "display_name": role_id.replace("_", " ").title(),
        "purpose": "Специализированная профессиональная роль VECTRA.",
        "source": "fallback_role_profile",
    }
    return {
        "role_id": role_id,
        "display_name": profile.get("display_name"),
        "purpose": profile.get("purpose"),
        "source": profile.get("source"),
        "personality_owner": "VECTRA",
        "creates_separate_personality": False,
    }

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
    professional_role = str(payload.get("professional_role") or "vectra_laboratory").strip().lower().replace(" ", "_")
    role_profile = _resolve_role_profile(professional_role)
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

    return {
        "self_model_id": "VECTRA-SELF-MODEL",
        "version": SELF_MODEL_VERSION,
        "status": "ACTIVE",
        "owner": "VECTRA",
        "identity_reference": (personality.get("personality_id") if isinstance(personality, dict) else None),
        "professional_role": professional_role,
        "role_context": role_profile,
        "personality_inheritance": {
            "personality_id": personality.get("personality_id") if isinstance(personality, dict) else None,
            "personality_version": personality.get("version") if isinstance(personality, dict) else None,
            "identity_locked": True,
            "identity_changed_by_role": False,
        },
        "current_workspace": workspace,
        "current_stage": {
            "stage_id": current_state.get("stage") if isinstance(current_state, dict) else None,
            "display_name": "Развитие профессиональной личности и цифровых коллег",
        },
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
        "self_understanding": {
            "knows_identity": bool(personality.get("identity") if isinstance(personality, dict) else None),
            "knows_workspace": bool(workspace.get("workspace_id")),
            "knows_capabilities": len(confirmed_capabilities) > 0,
            "knows_limitations": bool(limitation),
            "knows_business_context": bool(domain.get("domain_id")),
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


def verify_role_identity_isolation() -> Dict[str, Any]:
    roles = ["vectra_laboratory", "digital_business_analyst", "chief_engineer", "commercial_assistant"]
    models = [build_self_model({"professional_role": role}) for role in roles]
    identities = {
        (model.get("personality_inheritance") or {}).get("personality_id")
        for model in models
    }
    versions = {
        (model.get("personality_inheritance") or {}).get("personality_version")
        for model in models
    }
    checks = {
        "single_personality_id": len(identities) == 1 and None not in identities,
        "single_personality_version": len(versions) == 1 and None not in versions,
        "roles_resolved": all((model.get("role_context") or {}).get("display_name") for model in models),
        "role_does_not_create_personality": all(
            (model.get("role_context") or {}).get("creates_separate_personality") is False
            for model in models
        ),
        "identity_lock_active": all(
            (model.get("personality_inheritance") or {}).get("identity_locked") is True
            and (model.get("personality_inheritance") or {}).get("identity_changed_by_role") is False
            for model in models
        ),
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "checks": checks,
        "roles_tested": [
            {
                "role_id": model.get("professional_role"),
                "display_name": (model.get("role_context") or {}).get("display_name"),
                "personality_id": (model.get("personality_inheritance") or {}).get("personality_id"),
            }
            for model in models
        ],
        "release": RELEASE_ID,
        "read_only": True,
    }


def verify_self_model_runtime() -> Dict[str, Any]:
    result = persist_self_model_runtime_state({"professional_role": "vectra_laboratory"})
    model = result.get("self_model") if isinstance(result, dict) else {}
    understanding = model.get("self_understanding") if isinstance(model, dict) else {}
    role_isolation = verify_role_identity_isolation()
    checks = {
        "self_model_connected": result.get("self_model_ready") is True,
        "identity_reference_available": bool(model.get("identity_reference") if isinstance(model, dict) else None),
        "workspace_understood": bool((model.get("current_workspace") or {}).get("workspace_id")) if isinstance(model, dict) else False,
        "role_context_understood": bool((model.get("role_context") or {}).get("display_name")) if isinstance(model, dict) else False,
        "identity_locked_across_roles": role_isolation.get("status") == "PASS",
        "capabilities_understood": bool(understanding.get("knows_capabilities")) if isinstance(understanding, dict) else False,
        "limitations_understood": bool(understanding.get("knows_limitations")) if isinstance(understanding, dict) else False,
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "checks": checks,
        "role_identity_isolation": role_isolation,
        "self_model_version": model.get("version") if isinstance(model, dict) else None,
        "release": RELEASE_ID,
        "read_only": True,
    }
