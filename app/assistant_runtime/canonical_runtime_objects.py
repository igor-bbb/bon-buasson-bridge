"""Canonical Runtime Objects Registry and contract enforcement."""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, Optional

CONTRACT_VERSION = "1.0"
OBJECT_NAME = "research_snapshot_request"
OWNER_CAPABILITY = "business_object_discovery"
SUPPORTED_CONSUMERS = [
    "get_research_workspace_snapshot",
    "validate_workspace",
    "start_workspace_research",
    "evaluate_workspace",
]

ROOT_RUNTIME_CONTRACT_VERSION = "1.0"
ROOT_RUNTIME_OBJECTS = [
    {
        "object_name": "personality",
        "owner_capability": "personality_runtime",
        "consumer_capabilities": ["execution_bootstrap", "self_audit"],
    },
    {
        "object_name": "self_model",
        "owner_capability": "personality_runtime",
        "consumer_capabilities": ["execution_bootstrap", "self_audit"],
    },
    {
        "object_name": "organization",
        "owner_capability": "digital_organization_registry",
        "consumer_capabilities": ["execution_bootstrap", "self_audit"],
    },
    {
        "object_name": "professional_memory",
        "owner_capability": "professional_memory",
        "consumer_capabilities": ["recovery_pipeline", "professional_intelligence"],
    },
    {
        "object_name": "professional_behaviour",
        "owner_capability": "professional_behaviour_runtime",
        "consumer_capabilities": ["execution_bootstrap", "professional_programs"],
    },
    {
        "object_name": "business_context",
        "owner_capability": "business_domain_runtime",
        "consumer_capabilities": ["execution_bootstrap", "business_programs"],
    },
    {
        "object_name": "capabilities",
        "owner_capability": "capability_registry",
        "consumer_capabilities": ["personality_runtime", "self_audit"],
    },
    {
        "object_name": "current_activity",
        "owner_capability": "professional_activity",
        "consumer_capabilities": ["runtime_continuation", "execution_bootstrap"],
    },
]


def get_canonical_runtime_objects_registry() -> Dict[str, Any]:
    research_contract = {
        "object_name": OBJECT_NAME,
        "object_kind": "research_contract",
        "contract_version": CONTRACT_VERSION,
        "owner_capability": OWNER_CAPABILITY,
        "consumer_capabilities": list(SUPPORTED_CONSUMERS),
        "backward_compatibility": "LEGACY_INTERNAL_ONLY",
        "lifecycle_status": "ACTIVE",
    }
    root_objects = [
        {
            **item,
            "object_kind": "unified_runtime_state_root",
            "contract_version": ROOT_RUNTIME_CONTRACT_VERSION,
            "backward_compatibility": "EXISTING_SUBSYSTEM_REMAINS_SOURCE_OF_TRUTH",
            "lifecycle_status": "FOUNDATION_READY",
        }
        for item in ROOT_RUNTIME_OBJECTS
    ]
    return {
        "status": "PASS",
        "registry_type": "canonical_runtime_objects",
        "registry_version": "2.0",
        "objects": [research_contract, *root_objects],
        "unified_runtime_state": {
            "contract_version": ROOT_RUNTIME_CONTRACT_VERSION,
            "root_object_count": len(root_objects),
            "migration_mode": "BACKWARD_COMPATIBLE_FOUNDATION",
        },
    }


def build_research_snapshot_request(*, object_type: str, object_id: str, business_domain: str,
                                    business_object: str, period: Optional[str] = None,
                                    workspace_id: Optional[str] = None) -> Dict[str, Any]:
    result: Dict[str, Any] = {
        "contract_version": CONTRACT_VERSION,
        "object_type": object_type,
        "object_id": object_id,
        "business_domain": business_domain,
        "business_object": business_object,
        "period": period,
    }
    if workspace_id:
        result["workspace_id"] = workspace_id
    return result


def parse_research_snapshot_request(payload: Any, *, allow_legacy: bool = True) -> Dict[str, Any]:
    payload = deepcopy(payload) if isinstance(payload, dict) else {}
    canonical = payload.get(OBJECT_NAME)
    legacy_fields = {
        "contract_version", "object_type", "object_id", "business_domain",
        "business_object", "period", "workspace_id", "workspace_type",
    }
    supplied_legacy = sorted(key for key in legacy_fields if key in payload)

    if isinstance(canonical, dict):
        if supplied_legacy:
            return {
                "status": "VALIDATION_ERROR",
                "reason": "mixed_contract_modes_forbidden",
                "conflicting_fields": supplied_legacy,
                "recommendation": "Pass only research_snapshot_request without duplicated top-level fields.",
            }
        request = canonical
        mode = "CANONICAL"
    elif allow_legacy and supplied_legacy:
        request = {key: payload.get(key) for key in supplied_legacy}
        request.setdefault("contract_version", CONTRACT_VERSION)
        mode = "LEGACY_COMPATIBILITY"
    else:
        return {
            "status": "VALIDATION_ERROR",
            "reason": "research_snapshot_request_required",
            "missing_field": OBJECT_NAME,
            "recommendation": "Pass the research_snapshot_request returned by discover_business_objects unchanged.",
        }

    version = str(request.get("contract_version") or "").strip()
    if version != CONTRACT_VERSION:
        return {
            "status": "VALIDATION_ERROR",
            "reason": "unsupported_contract_version",
            "contract_version": version or None,
            "supported_versions": [CONTRACT_VERSION],
            "recommendation": "Refresh the object through Business Object Discovery.",
        }

    required = ["object_type", "object_id", "business_domain"]
    missing = [field for field in required if not str(request.get(field) or "").strip()]
    if missing:
        return {
            "status": "VALIDATION_ERROR",
            "reason": "canonical_contract_incomplete",
            "missing_fields": missing,
            "recommendation": "Use an unmodified research_snapshot_request returned by Business Object Discovery.",
        }
    return {
        "status": "PASS",
        "contract_mode": mode,
        "research_snapshot_request": request,
    }
