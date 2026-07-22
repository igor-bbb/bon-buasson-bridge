"""Unified Business Framework Services for end-to-end autonomous research.

This module publishes the existing Business Framework as a self-describing graph.
It composes existing Business Object Discovery and Workspace Research Contract
capabilities without changing Business Data, calculations or Workspace logic.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.assistant_runtime.business_object_discovery import (
    BUSINESS_DOMAIN,
    OBJECT_TYPES,
    discover_business_objects,
    get_business_object_discovery_manifest,
)

RELEASE_ID = "VECTRA-COGNITIVE-RUNTIME-V1-WP-004"
CONTRACT_VERSION = "1.0"

RESEARCH_OPERATIONS = [
    "open_workspace",
    "open_primary_risk",
    "open_largest_opportunity",
    "show_reasons",
    "show_related_objects",
    "navigate_down",
    "navigate_up",
    "return_previous",
    "open_evidence",
    "form_conclusion",
]


def _level_contract(index: int, item: Dict[str, str]) -> Dict[str, Any]:
    parent = OBJECT_TYPES[index - 1]["object_type"] if index > 0 else None
    child = OBJECT_TYPES[index + 1]["object_type"] if index + 1 < len(OBJECT_TYPES) else None
    return {
        "level_index": index,
        "object_type": item["object_type"],
        "display_name": item["display_name"],
        "workspace_type": item["workspace_type"],
        "parent_types": [parent] if parent else [],
        "child_types": [child] if child else [],
        "research_contract": {
            "contract_version": CONTRACT_VERSION,
            "methodology": [
                "what_is_happening",
                "why_it_is_happening",
                "risks",
                "opportunities",
                "recommended_actions",
                "evidence",
            ],
            "supported_operations": list(RESEARCH_OPERATIONS),
            "unsupported_operation_policy": "Return NOT_APPLICABLE with reason and recommendation.",
        },
        "navigation_contract": {
            "up": bool(parent),
            "down": bool(child),
            "related": True,
            "return_previous": True,
            "context_preservation_required": True,
        },
        "lifecycle_status": "ACTIVE",
    }


def get_framework_manifest() -> Dict[str, Any]:
    levels = [_level_contract(index, item) for index, item in enumerate(OBJECT_TYPES)]
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "business_framework_services",
        "contract_version": CONTRACT_VERSION,
        "business_domain": BUSINESS_DOMAIN,
        "root_object_type": "business",
        "framework_model": "directed_business_framework_graph",
        "professional_independence": True,
        "levels": [
            {
                "level_index": item["level_index"],
                "object_type": item["object_type"],
                "display_name": item["display_name"],
                "workspace_type": item["workspace_type"],
                "parent_types": item["parent_types"],
                "child_types": item["child_types"],
            }
            for item in levels
        ],
        "framework_services": [
            "framework_manifest",
            "framework_registry",
            "navigation_service",
            "workspace_resolver",
            "research_routing",
        ],
        "supported_operations": ["manifest", "registry", "resolve_workspace", "build_route", "navigate", "start_execution", "run_execution", "execute_end_to_end", "get_execution", "verify_execution", "self_audit", "personality", "verify_personality", "verify"],
        "research_contract_version": CONTRACT_VERSION,
        "route_policy": "Route is built from published level relationships; clients must not hard-code Framework structure.",
        "read_only": True,
    }


def get_framework_registry(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    requested_type = str(payload.get("object_type") or "").strip()
    registry = [_level_contract(index, item) for index, item in enumerate(OBJECT_TYPES)]
    if requested_type:
        registry = [item for item in registry if item["object_type"] == requested_type]
    return {
        "status": "PASS" if registry else "NOT_FOUND",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "business_domain": BUSINESS_DOMAIN,
        "registry_type": "business_framework_object_type_registry",
        "object_types": registry,
        "count": len(registry),
        "read_only": True,
    }


def resolve_workspace(payload: Dict[str, Any]) -> Dict[str, Any]:
    object_type = str(payload.get("object_type") or "").strip()
    entry = next((item for index, item in enumerate(OBJECT_TYPES) if item["object_type"] == object_type), None)
    if entry is None:
        return {
            "status": "VALIDATION_ERROR",
            "reason": "unsupported_object_type",
            "supported_object_types": [item["object_type"] for item in OBJECT_TYPES],
            "read_only": True,
        }
    index = next(i for i, item in enumerate(OBJECT_TYPES) if item["object_type"] == object_type)
    contract = _level_contract(index, entry)
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "object_type": object_type,
        "workspace_resolution": {
            "workspace_type": entry["workspace_type"],
            "research_contract": contract["research_contract"],
            "navigation_contract": contract["navigation_contract"],
            "snapshot_action": "get_research_workspace_snapshot",
            "discovery_action": "discover_business_objects",
        },
        "read_only": True,
    }


def build_research_route(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    start_type = str(payload.get("start_object_type") or "business").strip()
    end_type = str(payload.get("end_object_type") or "sku").strip()
    types = [item["object_type"] for item in OBJECT_TYPES]
    if start_type not in types or end_type not in types:
        return {"status": "VALIDATION_ERROR", "reason": "unsupported_route_endpoint", "supported_object_types": types, "read_only": True}
    start = types.index(start_type)
    end = types.index(end_type)
    step = 1 if end >= start else -1
    route_types = types[start:end + 1] if step == 1 else list(reversed(types[end:start + 1]))
    route = []
    for sequence, object_type in enumerate(route_types, start=1):
        resolved = resolve_workspace({"object_type": object_type})
        route.append({
            "sequence": sequence,
            "object_type": object_type,
            "workspace_type": resolved["workspace_resolution"]["workspace_type"],
            "discovery_request": {"object_type": object_type, "summary_only": False},
            "next_action": "discover_business_objects",
        })
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "route_id": f"{start_type}-to-{end_type}",
        "direction": "down" if step == 1 else "up",
        "route": route,
        "context_contract": {
            "preserve": ["research_execution_id", "research_program_id", "current_object", "route_history", "evidence_ids", "finding_ids", "recommendation_ids"],
            "client_must_not_rebuild_route": True,
        },
        "read_only": True,
    }


def navigate_framework(payload: Dict[str, Any]) -> Dict[str, Any]:
    current_type = str(payload.get("current_object_type") or "").strip()
    direction = str(payload.get("direction") or "down").strip().lower()
    types = [item["object_type"] for item in OBJECT_TYPES]
    if current_type not in types or direction not in {"up", "down", "return", "related"}:
        return {"status": "VALIDATION_ERROR", "reason": "invalid_navigation_request", "read_only": True}
    index = types.index(current_type)
    if direction == "up":
        target = types[index - 1] if index > 0 else None
    elif direction == "down":
        target = types[index + 1] if index + 1 < len(types) else None
    elif direction == "return":
        target = str(payload.get("previous_object_type") or "").strip() or None
    else:
        target = str(payload.get("related_object_type") or "").strip() or None
    if not target or target not in types:
        return {
            "status": "NOT_APPLICABLE",
            "reason": "navigation_target_unavailable",
            "current_object_type": current_type,
            "direction": direction,
            "recommendation": "Use a transition published by Framework Registry.",
            "read_only": True,
        }
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "transition": {"from": current_type, "to": target, "direction": direction},
        "workspace_resolution": resolve_workspace({"object_type": target}).get("workspace_resolution"),
        "discovery_request": {"object_type": target, "summary_only": False},
        "context_preservation_required": True,
        "read_only": True,
    }


def verify_framework_services() -> Dict[str, Any]:
    manifest = get_framework_manifest()
    registry = get_framework_registry()
    down = build_research_route({"start_object_type": "business", "end_object_type": "sku"})
    up = build_research_route({"start_object_type": "sku", "end_object_type": "business"})
    discovery = get_business_object_discovery_manifest()
    checks = {
        "manifest_available": manifest.get("status") == "PASS",
        "all_levels_registered": registry.get("count") == len(OBJECT_TYPES),
        "route_business_to_sku": len(down.get("route") or []) == len(OBJECT_TYPES),
        "route_sku_to_business": len(up.get("route") or []) == len(OBJECT_TYPES),
        "workspace_resolver_all_levels": all(resolve_workspace({"object_type": item["object_type"]}).get("status") == "PASS" for item in OBJECT_TYPES),
        "business_object_discovery_connected": discovery.get("status") == "PASS",
        "professional_independence": manifest.get("professional_independence") is True,
        "read_only": all(x.get("read_only") is True for x in [manifest, registry, down, up]),
    }
    passed = all(checks.values())
    return {
        "status": "PASS" if passed else "HOLD",
        "release": RELEASE_ID,
        "checks": checks,
        "manifest": manifest,
        "route_summary": {"down": [x["object_type"] for x in down.get("route") or []], "up": [x["object_type"] for x in up.get("route") or []]},
        "next_allowed_action": "Start end-to-end Laboratory research from manifest and generated route." if passed else "Resolve failed checks before Product Verification.",
    }


def execute_framework_service(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    operation = str(payload.get("operation_type") or "manifest").strip().lower()
    from app.assistant_runtime.business_framework_execution import (start_execution, run_execution, execute_end_to_end, get_execution, verify_framework_execution)
    from app.assistant_runtime.personality_runtime import get_personality_core, run_self_audit, verify_personality_runtime
    operations = {
        "manifest": lambda: get_framework_manifest(),
        "registry": lambda: get_framework_registry(payload),
        "resolve_workspace": lambda: resolve_workspace(payload),
        "build_route": lambda: build_research_route(payload),
        "navigate": lambda: navigate_framework(payload),
        "start_execution": lambda: start_execution(payload),
        "run_execution": lambda: run_execution(payload),
        "execute_end_to_end": lambda: execute_end_to_end(payload),
        "get_execution": lambda: get_execution(payload),
        "verify_execution": lambda: verify_framework_execution(),
        "self_audit": lambda: run_self_audit(payload),
        "personality": lambda: get_personality_core(),
        "verify_personality": lambda: verify_personality_runtime(),
        "verify": lambda: verify_framework_services(),
    }
    handler = operations.get(operation)
    if handler is None:
        return {"status": "VALIDATION_ERROR", "reason": "unsupported_operation_type", "supported_operations": sorted(operations), "read_only": True}
    return handler()
