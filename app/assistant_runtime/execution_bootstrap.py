"""Internal Execution Bootstrap for professional Runtime programs.

The bootstrap prepares and validates the execution context without exposing a
new GPT Action or operation_type. Product Owner supplies a professional goal;
Runtime resolves technical prerequisites automatically whenever the context is
unambiguous.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.assistant_runtime.repository import (
    activate_business_domain,
    get_active_business_domain,
    get_business_domain_registry,
    restore_professional_body_state,
    restore_business_domain,
)
from app.assistant_runtime.business_framework_services import (
    build_research_route,
    get_framework_manifest,
)
from app.assistant_runtime.professional_behaviour_runtime import (
    diagnose_professional_behaviour,
    get_professional_behaviour_manifest,
    resolve_professional_behaviour,
)
from app.assistant_runtime.professional_procedures_runtime import (
    diagnose_professional_procedures,
    get_professional_procedure_manifest,
    resolve_professional_procedure,
)
from app.assistant_runtime.personality_runtime import restore_personality_context

RELEASE_ID = "VECTRA-PERSONALITY-CORE-RUNTIME-001"
CONTRACT_VERSION = "1.6"


def _available_domains(*, active_only: bool = True) -> List[Dict[str, Any]]:
    """Return published Business Domains eligible for automatic activation.

    Automatic activation is intentionally limited to domains explicitly marked
    ``active``. Unknown, draft, disabled, deleted and archived domains never
    participate in the single-domain decision.
    """
    result = get_business_domain_registry()
    registry = result.get("business_domain_registry") if isinstance(result, dict) else {}
    domains = registry.get("domains") if isinstance(registry, dict) else []
    if not isinstance(domains, list):
        return []
    values: List[Dict[str, Any]] = []
    for item in domains:
        if not isinstance(item, dict):
            continue
        domain_id = str(item.get("domain_id") or "").strip()
        status = str(item.get("status") or "unknown").strip().lower()
        if not domain_id:
            continue
        if active_only and status != "active":
            continue
        if not active_only and status in {"disabled", "deleted", "archived"}:
            continue
        values.append({
            "domain_id": domain_id,
            "display_name": item.get("title") or item.get("display_name") or domain_id,
            "status": status,
        })
    return values


def _active_domain_id() -> Optional[str]:
    result = get_active_business_domain()
    active = result.get("active_domain") if isinstance(result, dict) else {}
    value = active.get("active_domain_id") if isinstance(active, dict) else None
    return str(value).strip() if value else None


def _restore_domain_context(domain_id: str) -> Dict[str, Any]:
    restored = restore_business_domain(domain_id)
    ready = isinstance(restored, dict) and restored.get("status") == "PASS" and restored.get("business_context_restored") is True
    return {
        "status": "PASS" if ready else "FAIL",
        "domain_id": domain_id,
        "business_context_restored": bool(ready),
        "restoration_status": restored.get("status") if isinstance(restored, dict) else None,
    }


def _resolve_business_domain(payload: Dict[str, Any]) -> Dict[str, Any]:
    requested = str(
        payload.get("domain_id")
        or payload.get("domain")
        or payload.get("business_domain")
        or ""
    ).strip()
    active = _active_domain_id()

    if requested:
        if active == requested:
            context = _restore_domain_context(active)
            if context.get("status") != "PASS":
                return {
                    "status": "execution_context_not_ready",
                    "reason": "active_business_domain_context_restore_failed",
                    "missing": ["business_context"],
                    "domain_id": active,
                    "business_context": context,
                }
            return {
                "status": "PASS",
                "domain_id": active,
                "resolution": "existing_active_domain",
                "activation_performed": False,
                "business_context_restored": True,
                "business_context": context,
            }
        activation = activate_business_domain({
            "domain_id": requested,
            "source": "Execution Bootstrap explicit domain",
            "session_id": payload.get("session_id"),
            "request_id": payload.get("request_id"),
        })
        resolved = _active_domain_id()
        if resolved == requested:
            context = _restore_domain_context(resolved)
            if context.get("status") != "PASS":
                return {
                    "status": "execution_context_not_ready",
                    "reason": "explicit_business_domain_context_restore_failed",
                    "missing": ["business_context"],
                    "domain_id": resolved,
                    "business_context": context,
                }
            return {
                "status": "PASS",
                "domain_id": resolved,
                "resolution": "explicit_domain_activated",
                "activation_performed": True,
                "activation_status": activation.get("status") if isinstance(activation, dict) else None,
                "business_context_restored": True,
                "business_context": context,
            }
        return {
            "status": "execution_context_not_ready",
            "reason": "requested_business_domain_could_not_be_activated",
            "missing": ["active_business_domain"],
            "requested_domain": requested,
            "available_domains": _available_domains(),
        }

    if active:
        context = _restore_domain_context(active)
        if context.get("status") != "PASS":
            return {
                "status": "execution_context_not_ready",
                "reason": "active_business_domain_context_restore_failed",
                "missing": ["business_context"],
                "domain_id": active,
                "business_context": context,
            }
        return {
            "status": "PASS",
            "domain_id": active,
            "resolution": "existing_active_domain",
            "activation_performed": False,
            "business_context_restored": True,
            "business_context": context,
        }

    domains = _available_domains(active_only=True)
    if len(domains) == 1:
        selected = domains[0]["domain_id"]
        activation = activate_business_domain({
            "domain_id": selected,
            "source": "Execution Bootstrap automatic single-domain resolution",
            "session_id": payload.get("session_id"),
            "request_id": payload.get("request_id"),
        })
        resolved = _active_domain_id()
        if resolved == selected:
            context = _restore_domain_context(resolved)
            if context.get("status") != "PASS":
                return {
                    "status": "execution_context_not_ready",
                    "reason": "single_business_domain_context_restore_failed",
                    "missing": ["business_context"],
                    "domain_id": resolved,
                    "business_context": context,
                }
            return {
                "status": "PASS",
                "domain_id": resolved,
                "resolution": "single_active_domain_auto_selected",
                "activation_performed": True,
                "activation_status": activation.get("status") if isinstance(activation, dict) else None,
                "business_context_restored": True,
                "business_context": context,
            }
        return {
            "status": "execution_context_not_ready",
            "reason": "single_business_domain_activation_failed",
            "missing": ["active_business_domain"],
            "available_domains": domains,
        }

    if len(domains) > 1:
        return {
            "status": "domain_selection_required",
            "reason": "multiple_active_business_domains_available",
            "available_domains": domains,
            "selection_parameter": "domain_id",
        }

    return {
        "status": "execution_context_not_ready",
        "reason": "no_active_business_domain_available",
        "missing": ["active_business_domain"],
        "available_domains": [],
    }


def prepare_execution_context(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Prepare the canonical context required by a professional program."""
    payload = payload if isinstance(payload, dict) else {}
    start_type = str(payload.get("start_object_type") or "business").strip()
    end_type = str(payload.get("end_object_type") or "sku").strip()

    personality = restore_personality_context({**payload, "anchor_trigger": payload.get("anchor_trigger") or "execution_bootstrap"})
    professional = restore_professional_body_state()
    professional_role = str(payload.get("professional_role") or "vectra_laboratory").strip().lower()
    behaviour = resolve_professional_behaviour({
        "professional_role": professional_role,
        "behaviour_version": payload.get("behaviour_version"),
    })
    behaviour_diagnostics = diagnose_professional_behaviour({
        "professional_role": professional_role,
        "behaviour_version": payload.get("behaviour_version"),
    })
    behaviour_manifest = get_professional_behaviour_manifest(professional_role)
    procedure = resolve_professional_procedure({**payload, "professional_role": professional_role})
    procedure_diagnostics = diagnose_professional_procedures({**payload, "professional_role": professional_role})
    procedure_manifest = get_professional_procedure_manifest()
    domain = _resolve_business_domain(payload)
    if domain.get("status") != "PASS":
        return {
            **domain,
            "release": RELEASE_ID,
            "contract_version": CONTRACT_VERSION,
            "bootstrap_internal": True,
            "public_operation_added": False,
            "read_only": True,
        }

    manifest = get_framework_manifest()
    route = build_research_route({
        "start_object_type": start_type,
        "end_object_type": end_type,
    })

    checks = {
        "personality_core": "READY" if personality.get("status") == "PASS" and personality.get("personality_ready") is True else "NOT_READY",
        "personality_runtime_state": "CONNECTED" if (personality.get("personality_runtime_state") or {}).get("personality_runtime_state_connected") is True else "NOT_CONNECTED",
        "professional_state": "READY" if professional.get("status") == "PASS" else "NOT_READY",
        "professional_behaviour": "READY" if behaviour.get("status") == "PASS" and behaviour_diagnostics.get("status") == "READY" else "NOT_READY",
        "runtime_behaviour_authority": "CONFIRMED" if behaviour.get("runtime_is_behaviour_authority") is True and behaviour.get("authority_transfer_status") == "COMPLETED" else "NOT_CONFIRMED",
        "professional_procedure": "READY" if procedure.get("status") == "PASS" and procedure_diagnostics.get("status") == "READY" else "NOT_READY",
        "active_business_domain": "RESOLVED" if domain.get("domain_id") else "NOT_RESOLVED",
        "business_context": "RESTORED" if domain.get("business_context_restored") is True else "NOT_RESTORED",
        "runtime_first": "ENFORCED" if (behaviour_manifest.get("professional_behaviour_manifest") or {}).get("runtime_first_rule", {}).get("fallback_to_static_core_allowed") is False else "NOT_ENFORCED",
        "action_closure": "READY" if bool(procedure.get("next_allowed_action")) and (procedure.get("action_closure") or {}).get("cardinality") == "exactly_one" else "NOT_READY",
        "framework_manifest": "AVAILABLE" if manifest.get("status") == "PASS" else "UNAVAILABLE",
        "research_execution": "AVAILABLE",
        "access_mode": "READ_ONLY",
        "research_route": "BUILDABLE" if route.get("status") == "PASS" else "NOT_BUILDABLE",
    }
    missing_map = {
        "personality_core": checks["personality_core"] != "READY",
        "personality_runtime_state": checks["personality_runtime_state"] != "CONNECTED",
        "professional_state": checks["professional_state"] != "READY",
        "professional_behaviour": checks["professional_behaviour"] != "READY",
        "runtime_behaviour_authority": checks["runtime_behaviour_authority"] != "CONFIRMED",
        "professional_procedure": checks["professional_procedure"] != "READY",
        "active_business_domain": checks["active_business_domain"] != "RESOLVED",
        "business_context": checks["business_context"] != "RESTORED",
        "runtime_first": checks["runtime_first"] != "ENFORCED",
        "action_closure": checks["action_closure"] != "READY",
        "framework_manifest": checks["framework_manifest"] != "AVAILABLE",
        "research_execution": checks["research_execution"] != "AVAILABLE",
        "access_mode": checks["access_mode"] != "READ_ONLY",
        "research_route": checks["research_route"] != "BUILDABLE",
    }
    missing = [name for name, failed in missing_map.items() if failed]
    if missing:
        return {
            "status": "execution_context_not_ready",
            "release": RELEASE_ID,
            "contract_version": CONTRACT_VERSION,
            "missing": missing,
            "context_checks": checks,
            "personality_context": personality,
            "professional_behaviour": behaviour,
            "professional_behaviour_diagnostics": behaviour_diagnostics,
            "professional_procedure": procedure,
            "professional_procedure_diagnostics": procedure_diagnostics,
            "active_business_domain": domain,
            "route_diagnostic": route if route.get("status") != "PASS" else None,
            "bootstrap_internal": True,
            "public_operation_added": False,
            "read_only": True,
        }

    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "execution_context_ready": True,
        "context_checks": checks,
        "personality_context": personality,
        "personality_loaded_first": True,
        "unified_runtime_state": {
            "personality_root": "CONNECTED",
            "personality_version": (personality.get("personality_runtime_state") or {}).get("personality_version"),
            "readback_verified": (personality.get("personality_runtime_state") or {}).get("readback_verified"),
            "restore_order": 1,
        },
        "professional_behaviour": {
            "profile_id": (behaviour.get("active_behaviour_profile") or {}).get("behaviour_profile_id"),
            "version": (behaviour.get("active_behaviour_profile") or {}).get("version"),
            "professional_role": behaviour.get("professional_role"),
            "resolution": behaviour.get("resolution"),
            "manifest": behaviour_manifest.get("professional_behaviour_manifest"),
            "diagnostics": behaviour_diagnostics,
            "runtime_is_executable_behaviour_source": True,
            "runtime_is_behaviour_authority": behaviour.get("runtime_is_behaviour_authority"),
            "authority_transfer_status": behaviour.get("authority_transfer_status"),
            "static_professional_core_execution_allowed": False,
        },
        "professional_procedure": {
            "procedure_id": (procedure.get("active_procedure") or {}).get("procedure_id"),
            "version": (procedure.get("active_procedure") or {}).get("version"),
            "purpose": (procedure.get("active_procedure") or {}).get("purpose"),
            "steps": (procedure.get("active_procedure") or {}).get("steps") or [],
            "completion_criteria": (procedure.get("active_procedure") or {}).get("completion_criteria") or [],
            "next_allowed_action": procedure.get("next_allowed_action"),
            "manifest": procedure_manifest.get("professional_procedure_manifest"),
            "diagnostics": procedure_diagnostics,
            "runtime_is_executable_procedure_source": True,
            "runtime_is_procedure_authority": True,
            "static_professional_core_execution_allowed": False,
        },
        "next_allowed_professional_action": procedure.get("next_allowed_action"),
        "action_closure": {
            "required": True,
            "cardinality": "exactly_one",
            "resolved_action": procedure.get("next_allowed_action"),
        },
        "release_brief_interpretation": {
            "stage": "POST_DEPLOYMENT" if (procedure.get("active_procedure") or {}).get("procedure_id") == "product_verification" else None,
            "deployment_wait_instruction_allowed": False,
        },
        "professional_runtime_authority": {
            "status": "COMPLETED",
            "authority": "Professional Runtime",
            "scope": "executable_professional_activity",
            "custom_gpt_scope": ["identity", "policy", "safety", "runtime_connection"],
        },
        "active_business_domain": domain,
        "route": route,
        "bootstrap_internal": True,
        "public_operation_added": False,
        "read_only": True,
    }
