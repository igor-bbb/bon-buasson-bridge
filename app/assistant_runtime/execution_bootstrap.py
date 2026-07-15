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

RELEASE_ID = "PROFESSIONAL-BEHAVIOUR-RUNTIME-MIGRATION-001-INCREMENT-001"
CONTRACT_VERSION = "1.1"


def _available_domains() -> List[Dict[str, Any]]:
    result = get_business_domain_registry()
    registry = result.get("business_domain_registry") if isinstance(result, dict) else {}
    domains = registry.get("domains") if isinstance(registry, dict) else []
    if not isinstance(domains, list):
        return []
    return [
        {
            "domain_id": str(item.get("domain_id") or "").strip(),
            "display_name": item.get("title") or item.get("display_name") or item.get("domain_id"),
            "status": item.get("status") or "unknown",
        }
        for item in domains
        if isinstance(item, dict)
        and str(item.get("domain_id") or "").strip()
        and str(item.get("status") or "active").lower() not in {"disabled", "deleted", "archived"}
    ]


def _active_domain_id() -> Optional[str]:
    result = get_active_business_domain()
    active = result.get("active_domain") if isinstance(result, dict) else {}
    value = active.get("active_domain_id") if isinstance(active, dict) else None
    return str(value).strip() if value else None


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
            return {
                "status": "PASS",
                "domain_id": active,
                "resolution": "existing_active_domain",
                "activation_performed": False,
            }
        activation = activate_business_domain({
            "domain_id": requested,
            "source": "Execution Bootstrap explicit domain",
            "session_id": payload.get("session_id"),
            "request_id": payload.get("request_id"),
        })
        resolved = _active_domain_id()
        if resolved == requested:
            return {
                "status": "PASS",
                "domain_id": resolved,
                "resolution": "explicit_domain_activated",
                "activation_performed": True,
                "activation_status": activation.get("status") if isinstance(activation, dict) else None,
            }
        return {
            "status": "execution_context_not_ready",
            "reason": "requested_business_domain_could_not_be_activated",
            "missing": ["active_business_domain"],
            "requested_domain": requested,
            "available_domains": _available_domains(),
        }

    if active:
        return {
            "status": "PASS",
            "domain_id": active,
            "resolution": "existing_active_domain",
            "activation_performed": False,
        }

    domains = _available_domains()
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
            return {
                "status": "PASS",
                "domain_id": resolved,
                "resolution": "single_domain_auto_selected",
                "activation_performed": True,
                "activation_status": activation.get("status") if isinstance(activation, dict) else None,
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
            "reason": "multiple_business_domains_available",
            "available_domains": domains,
            "selection_parameter": "domain_id",
        }

    return {
        "status": "execution_context_not_ready",
        "reason": "no_business_domain_available",
        "missing": ["active_business_domain"],
        "available_domains": [],
    }


def prepare_execution_context(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Prepare the canonical context required by a professional program."""
    payload = payload if isinstance(payload, dict) else {}
    start_type = str(payload.get("start_object_type") or "business").strip()
    end_type = str(payload.get("end_object_type") or "sku").strip()

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
        "professional_state": "READY" if professional.get("status") == "PASS" else "NOT_READY",
        "professional_behaviour": "READY" if behaviour.get("status") == "PASS" and behaviour_diagnostics.get("status") == "READY" else "NOT_READY",
        "active_business_domain": "RESOLVED" if domain.get("domain_id") else "NOT_RESOLVED",
        "framework_manifest": "AVAILABLE" if manifest.get("status") == "PASS" else "UNAVAILABLE",
        "research_execution": "AVAILABLE",
        "access_mode": "READ_ONLY",
        "research_route": "BUILDABLE" if route.get("status") == "PASS" else "NOT_BUILDABLE",
    }
    missing_map = {
        "professional_state": checks["professional_state"] != "READY",
        "professional_behaviour": checks["professional_behaviour"] != "READY",
        "active_business_domain": checks["active_business_domain"] != "RESOLVED",
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
            "professional_behaviour": behaviour,
            "professional_behaviour_diagnostics": behaviour_diagnostics,
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
        "professional_behaviour": {
            "profile_id": (behaviour.get("active_behaviour_profile") or {}).get("behaviour_profile_id"),
            "version": (behaviour.get("active_behaviour_profile") or {}).get("version"),
            "professional_role": behaviour.get("professional_role"),
            "resolution": behaviour.get("resolution"),
            "manifest": behaviour_manifest.get("professional_behaviour_manifest"),
            "diagnostics": behaviour_diagnostics,
            "runtime_is_executable_behaviour_source": True,
        },
        "active_business_domain": domain,
        "route": route,
        "bootstrap_internal": True,
        "public_operation_added": False,
        "read_only": True,
    }
