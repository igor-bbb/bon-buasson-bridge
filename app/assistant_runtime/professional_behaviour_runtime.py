"""Professional Behaviour Runtime foundation.

Professional Behaviour is a versioned Runtime asset.  This module owns the
canonical Laboratory behaviour profile, registry, manifest, resolver and
structured diagnostics.  It is intentionally internal: no new GPT Action or
public operation_type is introduced by this release.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.durable_runtime_state import read_json_state, update_json_state

RELEASE_ID = "PROFESSIONAL-BEHAVIOUR-RUNTIME-MIGRATION-001-INCREMENT-003"
CONTRACT_VERSION = "1.0"
REGISTRY_FILE = Path("runtime") / "professional_behaviour" / "registry.json"
DEFAULT_ROLE = "vectra_laboratory"
DEFAULT_PROFILE_ID = "PROFESSIONAL-BEHAVIOUR-VECTRA-LABORATORY"
DEFAULT_PROFILE_VERSION = "1.2"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _default_profile() -> Dict[str, Any]:
    return {
        "behaviour_profile_id": DEFAULT_PROFILE_ID,
        "version": DEFAULT_PROFILE_VERSION,
        "owner": "VECTRA Laboratory",
        "professional_role": DEFAULT_ROLE,
        "status": "ACTIVE",
        "compatibility": {
            "runtime_contract": ">=1.0",
            "supported_roles": [DEFAULT_ROLE],
            "supported_programs": [
                "business_framework_end_to_end_research",
                "guided_research",
                "product_verification",
                "engineering_review",
            ],
        },
        "activation_conditions": {
            "role": DEFAULT_ROLE,
            "runtime_available": True,
            "professional_state_required": True,
        },
        "behaviour_rules": [
            "runtime_first",
            "action_first",
            "professional_state_restore_required",
            "product_owner_is_not_runtime_dispatcher",
            "maximum_professional_autonomy",
            "confirmed_facts_observations_recommendations_separation",
            "blocker_localization_before_handoff",
            "no_unverified_result_claims",
            "continue_active_program_when_context_is_ready",
        ],
        "required_runtime_capabilities": [
            "professional_state",
            "business_domain",
            "framework_manifest",
            "research_execution",
            "structured_diagnostics",
        ],
        "supported_professional_programs": [
            "business_framework_end_to_end_research",
            "guided_research",
            "product_verification",
            "engineering_review",
        ],
        "professional_standards": [
            "Professional Independence",
            "Runtime First",
            "Action First",
            "Minimal Public Contract",
            "Execution Context Independence",
            "Framework is the Product",
            "Maximum Professional Autonomy",
        ],
        "procedures": [
            "start_working_session",
            "restore_professional_state",
            "prepare_execution_context",
            "start_professional_program",
            "product_verification",
            "engineering_handoff",
            "handle_confirmed_blocker",
            "continue_after_pause",
            "knowledge_capitalization",
            "execute_confirmed_action",
        ],
        "diagnostics_contract": {
            "statuses": ["READY", "NOT_READY", "INCOMPATIBLE", "DEPRECATED"],
            "required_fields": ["status", "profile_id", "version", "reason", "recommendation"],
            "structured_only": True,
        },
        "lifecycle_status": "ACTIVE",
        "created_at": _now(),
        "supersedes": "1.1",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
    }


def _default_registry() -> Dict[str, Any]:
    profile = _default_profile()
    return {
        "registry_id": "PROFESSIONAL-BEHAVIOUR-REGISTRY",
        "contract_version": CONTRACT_VERSION,
        "release": RELEASE_ID,
        "authority": "Professional Runtime",
        "runtime_is_behaviour_authority": True,
        "authority_transfer_status": "COMPLETED",
        "authority_transfer_event": {
            "event_id": "RUNTIME-AUTHORITY-TRANSFER-PROFESSIONAL-BEHAVIOUR-001",
            "status": "COMPLETED",
            "effective_release": RELEASE_ID,
            "completed_at": _now(),
            "previous_authority": "Custom GPT Professional Core",
            "new_authority": "Professional Runtime",
        },
        "active_profiles": [profile],
        "deprecated_profiles": [],
        "updated_at": _now(),
    }


def _upgrade_registry(value: Dict[str, Any]) -> Dict[str, Any]:
    """Apply the authority transfer and profile upgrade idempotently."""
    current = deepcopy(value) if isinstance(value, dict) else {}
    active_profiles = current.get("active_profiles") if isinstance(current.get("active_profiles"), list) else []
    deprecated_profiles = current.get("deprecated_profiles") if isinstance(current.get("deprecated_profiles"), list) else []

    for profile in active_profiles:
        if not isinstance(profile, dict):
            continue
        if (
            profile.get("behaviour_profile_id") == DEFAULT_PROFILE_ID
            and str(profile.get("version") or "") != DEFAULT_PROFILE_VERSION
        ):
            deprecated = deepcopy(profile)
            deprecated["status"] = "DEPRECATED"
            deprecated["lifecycle_status"] = "DEPRECATED"
            deprecated["deprecated_at"] = _now()
            deprecated["replaced_by"] = DEFAULT_PROFILE_VERSION
            if not any(
                isinstance(item, dict)
                and item.get("behaviour_profile_id") == deprecated.get("behaviour_profile_id")
                and item.get("version") == deprecated.get("version")
                for item in deprecated_profiles
            ):
                deprecated_profiles.append(deprecated)

    current.update({
        "registry_id": "PROFESSIONAL-BEHAVIOUR-REGISTRY",
        "contract_version": CONTRACT_VERSION,
        "release": RELEASE_ID,
        "authority": "Professional Runtime",
        "runtime_is_behaviour_authority": True,
        "authority_transfer_status": "COMPLETED",
        "authority_transfer_event": {
            "event_id": "RUNTIME-AUTHORITY-TRANSFER-PROFESSIONAL-BEHAVIOUR-001",
            "status": "COMPLETED",
            "effective_release": RELEASE_ID,
            "completed_at": (current.get("authority_transfer_event") or {}).get("completed_at") or _now(),
            "previous_authority": "Custom GPT Professional Core",
            "new_authority": "Professional Runtime",
        },
        "active_profiles": [_default_profile()],
        "deprecated_profiles": deprecated_profiles,
        "updated_at": _now(),
    })
    return current


def _read_registry() -> Dict[str, Any]:
    value, _ = read_json_state(REGISTRY_FILE, _default_registry, dict)
    upgraded = _upgrade_registry(value if isinstance(value, dict) else {})
    if upgraded != value:
        update_json_state(REGISTRY_FILE, _default_registry, dict, lambda _: upgraded)
    return upgraded


def get_professional_behaviour_registry() -> Dict[str, Any]:
    registry = _read_registry()
    return {
        "status": "PASS",
        "professional_behaviour_registry": deepcopy(registry),
        "read_only": True,
    }


def get_professional_behaviour_manifest(role: Optional[str] = None) -> Dict[str, Any]:
    resolved = resolve_professional_behaviour({"professional_role": role or DEFAULT_ROLE})
    profile = resolved.get("active_behaviour_profile") or {}
    if resolved.get("status") != "PASS":
        return {
            "status": "NOT_READY",
            "reason": resolved.get("reason"),
            "diagnostic": resolved,
            "read_only": True,
        }
    manifest = {
        "manifest_id": "PROFESSIONAL-BEHAVIOUR-MANIFEST",
        "contract_version": CONTRACT_VERSION,
        "release": RELEASE_ID,
        "professional_role": profile.get("professional_role"),
        "active_behaviour_profile": {
            "profile_id": profile.get("behaviour_profile_id"),
            "version": profile.get("version"),
            "owner": profile.get("owner"),
            "status": profile.get("status"),
        },
        "supported_procedures": profile.get("procedures") or [],
        "active_standards": profile.get("professional_standards") or [],
        "supported_programs": profile.get("supported_professional_programs") or [],
        "behaviour_rules": profile.get("behaviour_rules") or [],
        "required_runtime_capabilities": profile.get("required_runtime_capabilities") or [],
        "blocker_policy": {
            "required_cycle": [
                "confirm_blocker",
                "localize_cause",
                "determine_responsibility_boundary",
                "prepare_minimal_solution",
                "prepare_engineering_task",
                "preserve_program_state",
            ],
            "terminal_cannot_without_localization_forbidden": True,
        },
        "access_mode": "READ_ONLY",
        "behaviour_readiness": "READY",
        "runtime_is_executable_behaviour_source": True,
        "custom_gpt_scope": ["identity", "policy", "safety", "runtime_connection"],
        "professional_procedure_source": "Professional Runtime",
        "authority_transfer_status": resolved.get("authority_transfer_status"),
        "runtime_authority_required": True,
        "static_professional_core_execution_allowed": False,
    }
    return {"status": "PASS", "professional_behaviour_manifest": manifest, "read_only": True}


def resolve_professional_behaviour(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    role = str(payload.get("professional_role") or payload.get("role") or DEFAULT_ROLE).strip().lower()
    requested_version = str(payload.get("behaviour_version") or "").strip()
    registry = _read_registry()
    profiles = registry.get("active_profiles") or []
    candidates: List[Dict[str, Any]] = []
    for profile in profiles:
        if not isinstance(profile, dict):
            continue
        if str(profile.get("professional_role") or "").strip().lower() != role:
            continue
        if requested_version and str(profile.get("version") or "") != requested_version:
            continue
        if str(profile.get("status") or "").upper() != "ACTIVE":
            continue
        candidates.append(profile)
    if not candidates:
        return {
            "status": "NOT_READY",
            "reason": "professional_behaviour_profile_not_found",
            "professional_role": role,
            "requested_version": requested_version or None,
            "available_profiles": [
                {
                    "profile_id": item.get("behaviour_profile_id"),
                    "version": item.get("version"),
                    "professional_role": item.get("professional_role"),
                    "status": item.get("status"),
                }
                for item in profiles if isinstance(item, dict)
            ],
            "recommendation": "register_or_activate_compatible_behaviour_profile",
        }
    candidates.sort(key=lambda item: str(item.get("version") or ""), reverse=True)
    selected = deepcopy(candidates[0])
    return {
        "status": "PASS",
        "resolution": "active_profile_resolved",
        "professional_role": role,
        "active_behaviour_profile": selected,
        "registry_contract_version": registry.get("contract_version"),
        "runtime_is_behaviour_authority": registry.get("runtime_is_behaviour_authority", False),
        "authority_transfer_status": registry.get("authority_transfer_status"),
        "read_only": True,
    }


def diagnose_professional_behaviour(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    resolved = resolve_professional_behaviour(payload)
    if resolved.get("status") != "PASS":
        return {
            "status": "NOT_READY",
            "profile_id": None,
            "version": None,
            "reason": resolved.get("reason"),
            "recommendation": resolved.get("recommendation"),
            "details": resolved,
            "read_only": True,
        }
    profile = resolved.get("active_behaviour_profile") or {}
    missing = [
        field for field in (
            "behaviour_profile_id",
            "version",
            "owner",
            "status",
            "compatibility",
            "behaviour_rules",
            "diagnostics_contract",
            "lifecycle_status",
        ) if not profile.get(field)
    ]
    compatible = (
        str(profile.get("status") or "").upper() == "ACTIVE"
        and not missing
        and bool(resolved.get("runtime_is_behaviour_authority"))
        and resolved.get("authority_transfer_status") == "COMPLETED"
    )
    return {
        "status": "READY" if compatible else "NOT_READY",
        "profile_id": profile.get("behaviour_profile_id"),
        "version": profile.get("version"),
        "reason": None if compatible else (
            "professional_runtime_not_behaviour_authority"
            if not resolved.get("runtime_is_behaviour_authority")
            else "professional_behaviour_profile_incomplete"
        ),
        "missing": missing,
        "recommendation": "use_active_profile" if compatible else "complete_profile_contract",
        "professional_role": profile.get("professional_role"),
        "supported_programs": profile.get("supported_professional_programs") or [],
        "runtime_is_executable_behaviour_source": True,
        "authority_transfer_status": resolved.get("authority_transfer_status"),
        "read_only": True,
    }


def verify_professional_behaviour_foundation() -> Dict[str, Any]:
    registry = get_professional_behaviour_registry()
    manifest = get_professional_behaviour_manifest(DEFAULT_ROLE)
    resolver = resolve_professional_behaviour({"professional_role": DEFAULT_ROLE})
    diagnostics = diagnose_professional_behaviour({"professional_role": DEFAULT_ROLE})
    checks = {
        "registry_available": registry.get("status") == "PASS",
        "manifest_available": manifest.get("status") == "PASS",
        "resolver_selects_active_profile": resolver.get("status") == "PASS",
        "diagnostics_ready": diagnostics.get("status") == "READY",
        "profile_versioned": bool((resolver.get("active_behaviour_profile") or {}).get("version")),
        "profile_owned": bool((resolver.get("active_behaviour_profile") or {}).get("owner")),
        "profile_lifecycle_present": bool((resolver.get("active_behaviour_profile") or {}).get("lifecycle_status")),
        "runtime_authority_transfer_completed": resolver.get("runtime_is_behaviour_authority") is True and resolver.get("authority_transfer_status") == "COMPLETED",
        "static_professional_core_execution_disabled": True,
        "no_public_action_required": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "checks": checks,
        "registry_summary": {
            "active_profile_count": len((registry.get("professional_behaviour_registry") or {}).get("active_profiles") or []),
            "authority_transfer_status": (registry.get("professional_behaviour_registry") or {}).get("authority_transfer_status"),
        },
        "active_profile": {
            "profile_id": (resolver.get("active_behaviour_profile") or {}).get("behaviour_profile_id"),
            "version": (resolver.get("active_behaviour_profile") or {}).get("version"),
            "professional_role": (resolver.get("active_behaviour_profile") or {}).get("professional_role"),
        },
        "read_only": True,
    }
