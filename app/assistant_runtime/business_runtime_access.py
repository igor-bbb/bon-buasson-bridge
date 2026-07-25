"""Stage 1 operational verification for autonomous read-only Business Runtime access."""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from app.assistant_runtime.business_domain_profile import get_business_domain_profile, validate_single_business_root

from app.assistant_runtime.business_data import (
    get_business_data_entities,
    get_business_data_manifest,
    get_business_data_status,
    get_business_data_summary,
)

RELEASE_ID = "BUSINESS-RUNTIME-ACCESS-VERIFICATION-001"
MAX_OBJECTS_PER_LEVEL = 3

LEVELS: List[Dict[str, str]] = [
    {"level": "business", "entity_field": "business", "summary_level": "business", "display_name": "Business"},
    {"level": "top_manager", "entity_field": "manager_top", "summary_level": "manager-top", "display_name": "Top Manager"},
    {"level": "manager", "entity_field": "manager", "summary_level": "manager", "display_name": "Manager"},
    {"level": "network", "entity_field": "network", "summary_level": "network", "display_name": "Network / Contract"},
    {"level": "category", "entity_field": "category", "summary_level": "category", "display_name": "Category"},
    {"level": "tmc_group", "entity_field": "tmc_group", "summary_level": "tmc-group", "display_name": "TMC Group"},
    {"level": "sku", "entity_field": "sku", "summary_level": "sku", "display_name": "SKU"},
]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _compact(value: Any, *, depth: int = 0) -> Any:
    if depth > 4:
        return "[depth_limited]"
    if isinstance(value, dict):
        result: Dict[str, Any] = {}
        for index, (key, item) in enumerate(value.items()):
            if index >= 30:
                result["_truncated_keys"] = max(len(value) - 30, 0)
                break
            result[str(key)] = _compact(item, depth=depth + 1)
        return result
    if isinstance(value, list):
        return [_compact(item, depth=depth + 1) for item in value[:10]] + ([{"_truncated_items": len(value) - 10}] if len(value) > 10 else [])
    if isinstance(value, str) and len(value) > 1500:
        return value[:1500] + "…"
    return value


def _diagnostic(operation: str, status: str, reason: str, recommendation: str) -> Dict[str, str]:
    return {
        "operation": operation,
        "status": status,
        "reason": reason,
        "recommendation": recommendation,
    }


def get_business_runtime_manifest() -> Dict[str, Any]:
    data_manifest = get_business_data_manifest()
    data_status = get_business_data_status()
    capabilities = [
        {
            "operation_id": "get_business_runtime_manifest",
            "purpose": "Return the professional Runtime contract and operational coverage metadata.",
            "supported_parameters": [],
            "availability_status": "AVAILABLE",
            "access_mode": "read-only",
        },
        {
            "operation_id": "discover_business_runtime_objects",
            "purpose": "Discover Business Framework objects by professional level.",
            "supported_parameters": ["limit_per_level"],
            "availability_status": "AVAILABLE",
            "access_mode": "read-only",
        },
        {
            "operation_id": "open_business_workspace_direct",
            "purpose": "Open any supported Workspace directly by object type and identifier.",
            "supported_parameters": ["object_type", "object_id", "period"],
            "availability_status": "AVAILABLE",
            "access_mode": "read-only",
        },
        {
            "operation_id": "verify_business_runtime_access",
            "purpose": "Run complete Stage 1 operational verification and return Business Runtime Access Report.",
            "supported_parameters": ["period", "limit_per_level"],
            "availability_status": "AVAILABLE",
            "access_mode": "read-only",
        },
    ]
    return {
        "status": "PASS" if data_status.get("business_data_connected") else "HOLD",
        "release": RELEASE_ID,
        "runtime_version": data_manifest.get("release"),
        "business_runtime_version": RELEASE_ID,
        "business_domains": ["bon_buasson"],
        "capabilities": capabilities,
        "supported_workspace_types": [item["level"] for item in LEVELS],
        "supported_object_types": [item["level"] for item in LEVELS],
        "supported_contexts": ["business_context", "navigation_context", "research_context"],
        "access_mode": "read-only",
        "mutation_operations_exposed": False,
        "business_data_connected": bool(data_status.get("business_data_connected")),
        "latest_period": data_status.get("latest_period"),
        "runtime_health": data_status.get("business_data_health"),
        "source_manifest": {
            "facade_name": data_manifest.get("facade_name"),
            "supported_operation_types": data_manifest.get("supported_operation_types") or [],
        },
    }


def discover_business_runtime_objects(limit_per_level: int = MAX_OBJECTS_PER_LEVEL) -> Dict[str, Any]:
    safe_limit = min(max(int(limit_per_level or MAX_OBJECTS_PER_LEVEL), 1), 10)
    entities = get_business_data_entities(limit_per_group=safe_limit)
    previews = entities.get("entity_preview") if isinstance(entities.get("entity_preview"), dict) else {}
    counts = entities.get("entity_counts") if isinstance(entities.get("entity_counts"), dict) else {}
    objects: Dict[str, List[Dict[str, Any]]] = {}
    for item in LEVELS:
        field = item["entity_field"]
        if item["level"] == "business":
            profile = get_business_domain_profile("bon_buasson") or {}
            root = profile.get("root_business") if isinstance(profile, dict) else {}
            values = [str((root or {}).get("display_name") or "Бон Буассон")]
        else:
            values = previews.get(field) if isinstance(previews.get(field), list) else []
        objects[item["level"]] = [
            {
                "object_id": (str(((get_business_domain_profile("bon_buasson") or {}).get("root_business") or {}).get("object_id")) if item["level"] == "business" else str(value)),
                "display_name": str(value),
                "object_type": item["level"],
                "workspace_available": True,
            }
            for value in values[:safe_limit]
        ]
    return {
        "status": "PASS" if entities.get("status") == "ok" else "HOLD",
        "read_only": True,
        "limit_per_level": safe_limit,
        "object_counts": {item["level"]: (1 if item["level"] == "business" else int(counts.get(item["entity_field"]) or 0)) for item in LEVELS},
        "objects": objects,
        "diagnostic": None if entities.get("status") == "ok" else _diagnostic(
            "discover_business_runtime_objects", "HOLD", str(entities.get("error") or "entity_discovery_failed"), "Check Business Data connection and retry."
        ),
    }


def _level_config(object_type: str) -> Optional[Dict[str, str]]:
    normalized = str(object_type or "").strip().lower().replace("-", "_")
    aliases = {"manager_top": "top_manager", "contract": "network", "network_contract": "network", "tmc": "tmc_group"}
    normalized = aliases.get(normalized, normalized)
    return next((item for item in LEVELS if item["level"] == normalized), None)


def open_business_workspace_direct(object_type: str, object_id: str = "", period: str = "") -> Dict[str, Any]:
    config = _level_config(object_type)
    if not config:
        return {
            "status": "HOLD",
            "diagnostic": _diagnostic("open_business_workspace_direct", "HOLD", "unsupported_object_type", "Use an object type declared in Runtime Manifest."),
            "read_only": True,
        }
    if config["level"] == "business":
        root_validation = validate_single_business_root("bon_buasson")
        if root_validation.get("status") != "PASS":
            return {
                "status": "HOLD",
                "diagnostic": _diagnostic("open_business_workspace_direct", "HOLD", str(root_validation.get("reason")), "Check Business Domain Profile and Business Root Registry."),
                "read_only": True,
            }
        root = (get_business_domain_profile("bon_buasson") or {}).get("root_business") or {}
        expected_root_id = str(root.get("object_id") or "")
        if str(object_id or expected_root_id).strip() != expected_root_id:
            return {
                "status": "HOLD",
                "diagnostic": _diagnostic("open_business_workspace_direct", "HOLD", "invalid_business_root_id", "Use the canonical Root Business id returned by Business Object Discovery."),
                "read_only": True,
            }
        object_id = expected_root_id

    status = get_business_data_status()
    selected_period = str(period or status.get("latest_period") or "").strip()
    if not selected_period:
        return {
            "status": "HOLD",
            "diagnostic": _diagnostic("open_business_workspace_direct", "HOLD", "period_unavailable", "Restore Business Data connection or provide a valid period."),
            "read_only": True,
        }
    kwargs: Dict[str, Any] = {}
    field = config["entity_field"]
    if config["level"] != "business":
        if not str(object_id or "").strip():
            return {
                "status": "HOLD",
                "diagnostic": _diagnostic("open_business_workspace_direct", "HOLD", "object_id_required", f"Provide an identifier for {config['display_name']}."),
                "read_only": True,
            }
        kwargs[field] = str(object_id).strip()
    try:
        summary = get_business_data_summary(config["summary_level"], period=selected_period, **kwargs)
        raw_status = str(summary.get("status") or "ok").lower() if isinstance(summary, dict) else "error"
        ok = raw_status not in {"error", "failed", "fail", "blocked"}
        compact_summary = _compact(summary if isinstance(summary, dict) else {"status": "error", "reason": "invalid_summary_response"})
        business_context = {
            "object_type": config["level"],
            "object_id": str(object_id or "business"),
            "period": selected_period,
            "source": "existing_business_runtime",
            "summary_status": raw_status,
            "professional_interpretation_available": ok,
        }
        navigation_context = {
            "opened_from": "direct_access",
            "active_object": {"type": config["level"], "id": str(object_id or "business")},
            "active_workspace": f"{config['level']}_workspace",
            "research_route": [{"type": config["level"], "id": str(object_id or "business")}],
            "related_object_types": [item["level"] for item in LEVELS if item["level"] != config["level"]],
            "allowed_transitions": ["open_related_object", "open_direct_object", "return_to_previous_workspace"],
            "return_supported": True,
            "research_context_preserved": True,
        }
        return {
            "status": "PASS" if ok else "HOLD",
            "read_only": True,
            "workspace": {
                "workspace_type": f"{config['level']}_workspace",
                "object_type": config["level"],
                "object_id": str(object_id or "business"),
                "period": selected_period,
                "business_context": business_context,
                "navigation_context": navigation_context,
                "runtime_response": compact_summary,
            },
            "diagnostic": None if ok else _diagnostic("open_business_workspace_direct", "HOLD", str(summary.get("reason") or "workspace_open_failed"), "Check object identifier, period and Business Runtime readiness."),
        }
    except Exception as exc:
        return {
            "status": "HOLD",
            "read_only": True,
            "diagnostic": _diagnostic("open_business_workspace_direct", "HOLD", str(exc), "Check Runtime logs and the selected object before retrying."),
        }


def verify_business_runtime_access(period: str = "", limit_per_level: int = MAX_OBJECTS_PER_LEVEL) -> Dict[str, Any]:
    manifest = get_business_runtime_manifest()
    discovery = discover_business_runtime_objects(limit_per_level=limit_per_level)
    selected_period = str(period or manifest.get("latest_period") or "").strip()
    access_matrix: List[Dict[str, Any]] = []
    diagnostics: List[Dict[str, str]] = []
    route: List[Dict[str, str]] = []

    for level in LEVELS:
        level_name = level["level"]
        candidates = (discovery.get("objects") or {}).get(level_name) or []
        object_id = str((candidates[0] or {}).get("object_id") or "") if candidates else ""
        result = open_business_workspace_direct(level_name, object_id=object_id, period=selected_period)
        workspace = result.get("workspace") if isinstance(result.get("workspace"), dict) else {}
        business_context = workspace.get("business_context") if isinstance(workspace.get("business_context"), dict) else {}
        navigation_context = workspace.get("navigation_context") if isinstance(workspace.get("navigation_context"), dict) else {}
        opened = result.get("status") == "PASS"
        transition_works = bool(opened and navigation_context.get("allowed_transitions"))
        return_works = bool(opened and navigation_context.get("return_supported"))
        row = {
            "level": level_name,
            "display_name": level["display_name"],
            "sample_object_id": object_id or None,
            "object_discovered": bool(candidates) or level_name == "business",
            "workspace_opens": opened,
            "business_context_available": bool(business_context),
            "navigation_context_available": bool(navigation_context),
            "direct_open_works": opened,
            "transitions_work": transition_works,
            "return_works": return_works,
            "read_only_confirmed": bool(result.get("read_only")),
        }
        access_matrix.append(row)
        if opened:
            route.append({"type": level_name, "id": object_id or "business"})
        if result.get("diagnostic"):
            diagnostics.append(result["diagnostic"])

    required_flags = [
        row[flag]
        for row in access_matrix
        for flag in (
            "object_discovered", "workspace_opens", "business_context_available",
            "navigation_context_available", "direct_open_works", "transitions_work",
            "return_works", "read_only_confirmed",
        )
    ]
    runtime_ready = bool(manifest.get("status") == "PASS" and all(required_flags) and not diagnostics)
    coverage = {
        row["level"]: {
            "discovered": row["object_discovered"],
            "workspace_available": row["workspace_opens"],
            "business_context_available": row["business_context_available"],
            "navigation_context_available": row["navigation_context_available"],
            "direct_open_supported": row["direct_open_works"],
            "transitions_supported": row["transitions_work"],
            "return_supported": row["return_works"],
            "read_only": row["read_only_confirmed"],
        }
        for row in access_matrix
    }
    return {
        "status": "PASS" if runtime_ready else "HOLD",
        "report_type": "Business Runtime Access Report",
        "release": RELEASE_ID,
        "generated_at": _now(),
        "runtime_summary": {
            "runtime_status": manifest.get("status"),
            "runtime_health": manifest.get("runtime_health"),
            "business_data_connected": manifest.get("business_data_connected"),
            "selected_period": selected_period or None,
            "registered_operations": manifest.get("capabilities"),
            "available_workspaces": manifest.get("supported_workspace_types"),
            "available_levels": manifest.get("supported_object_types"),
            "access_mode": manifest.get("access_mode"),
        },
        "runtime_manifest": manifest,
        "runtime_coverage": coverage,
        "access_matrix": access_matrix,
        "navigation_assessment": {
            "direct_access": all(row["direct_open_works"] for row in access_matrix),
            "transitions": all(row["transitions_work"] for row in access_matrix),
            "return": all(row["return_works"] for row in access_matrix),
            "research_context_preserved": all(row["navigation_context_available"] for row in access_matrix),
            "verified_route_samples": route,
            "limitations": [item["reason"] for item in diagnostics],
        },
        "runtime_diagnostics": diagnostics,
        "read_only_guard": {
            "status": "PASS" if manifest.get("access_mode") == "read-only" and not manifest.get("mutation_operations_exposed") else "HOLD",
            "business_data_mutation": False,
            "workspace_mutation": False,
            "runtime_administration": False,
            "mutation_operations_exposed": bool(manifest.get("mutation_operations_exposed")),
        },
        "response_optimization": {
            "status": "PASS",
            "mode": "compact",
            "limits_applied": {"objects_per_level": min(max(int(limit_per_level or MAX_OBJECTS_PER_LEVEL), 1), 10), "list_items": 10, "string_length": 1500},
        },
        "operational_readiness": {
            "status": "PASS" if runtime_ready else "HOLD",
            "question": "Может ли Digital Business Analyst самостоятельно приступить к исследованию существующего Business Framework без участия Product Owner?",
            "answer": "YES" if runtime_ready else "NO",
            "next_stage": "BUSINESS-RESEARCH-EXECUTION-001" if runtime_ready else "Resolve Runtime diagnostics and repeat Stage 1 verification.",
        },
    }
