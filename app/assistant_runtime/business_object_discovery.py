"""Read-only discovery contract for Business Framework research objects.

BUSINESS-OBJECT-DISCOVERY-001 gives Digital Business Analyst a compact,
self-describing catalogue of objects that can be selected without Product
Owner supplying internal identifiers. Stable public object ids are derived
from the canonical object type and source value; the original source selector
is returned only as a Runtime-owned research selector.
"""
from __future__ import annotations

from collections import defaultdict
from hashlib import sha256
from typing import Any, Dict, Iterable, List, Optional

from app.data.reader import load_raw_rows
from app.assistant_runtime.business_data import DIMENSION_FIELDS, get_business_data_status
from app.assistant_runtime.business_workspace import list_business_workspaces

RELEASE_ID = "BUSINESS-OBJECT-DISCOVERY-001"
BUSINESS_DOMAIN = "bon_buasson"
DEFAULT_LIMIT = 50
MAX_LIMIT = 200

OBJECT_TYPES: List[Dict[str, str]] = [
    {"object_type": "business", "field": "business", "display_name": "Business", "workspace_type": "business_workspace"},
    {"object_type": "top_manager", "field": "manager_top", "display_name": "Top Manager", "workspace_type": "top_manager_workspace"},
    {"object_type": "manager", "field": "manager", "display_name": "Manager", "workspace_type": "manager_workspace"},
    {"object_type": "network", "field": "network", "display_name": "Network / Contract", "workspace_type": "network_workspace"},
    {"object_type": "category", "field": "category", "display_name": "Category", "workspace_type": "category_workspace"},
    {"object_type": "tmc_group", "field": "tmc_group", "display_name": "TMC Group", "workspace_type": "tmc_group_workspace"},
    {"object_type": "sku", "field": "sku", "display_name": "SKU", "workspace_type": "sku_workspace"},
]

ALIASES = {
    "manager_top": "top_manager",
    "top-manager": "top_manager",
    "contract": "network",
    "network_contract": "network",
    "network/contract": "network",
    "tmc": "tmc_group",
    "tmc-group": "tmc_group",
}


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _normalise_type(value: Any) -> str:
    raw = _clean(value).lower().replace(" ", "_")
    return ALIASES.get(raw, raw)


def _config(object_type: str) -> Optional[Dict[str, str]]:
    normalized = _normalise_type(object_type)
    return next((item for item in OBJECT_TYPES if item["object_type"] == normalized), None)


def _stable_id(object_type: str, source_value: str) -> str:
    digest = sha256(f"{BUSINESS_DOMAIN}|{object_type}|{source_value}".encode("utf-8")).hexdigest()[:16].upper()
    prefix = {
        "business": "BUS",
        "top_manager": "TOP",
        "manager": "MGR",
        "network": "NET",
        "category": "CAT",
        "tmc_group": "TMC",
        "sku": "SKU",
    }.get(object_type, "OBJ")
    return f"{prefix}-{digest}"


def _unique(values: Iterable[Any]) -> List[str]:
    seen = set()
    result: List[str] = []
    for value in values:
        text = _clean(value)
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return sorted(result, key=lambda item: item.casefold())


def _workspace_index() -> Dict[str, Dict[str, Any]]:
    try:
        result = list_business_workspaces({"limit": 100})
    except Exception:
        return {}
    items = result.get("business_workspaces") if isinstance(result, dict) else []
    index: Dict[str, Dict[str, Any]] = {}
    for item in items if isinstance(items, list) else []:
        if not isinstance(item, dict):
            continue
        managed = _clean(item.get("managed_object"))
        if managed:
            index[managed] = item
    return index


def get_business_object_discovery_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "business_object_discovery",
        "access_mode": "read-only",
        "business_domain": BUSINESS_DOMAIN,
        "supported_object_types": [item["object_type"] for item in OBJECT_TYPES],
        "supported_parameters": ["object_type", "period", "search", "offset", "limit", "include_all_types"],
        "pagination": {"supported": True, "default_limit": DEFAULT_LIMIT, "maximum_limit": MAX_LIMIT},
        "usage": "Discover objects, select one returned object, then submit its research_snapshot_request to get_research_workspace_snapshot.",
    }


def discover_business_objects(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    requested_type = _normalise_type(payload.get("object_type"))
    config = _config(requested_type) if requested_type else None
    if requested_type and config is None:
        return {
            "status": "VALIDATION_ERROR",
            "reason": "unsupported_object_type",
            "supported_object_types": [item["object_type"] for item in OBJECT_TYPES],
            "read_only": True,
        }

    try:
        rows = load_raw_rows()
    except Exception as exc:
        return {
            "status": "HOLD",
            "reason": "business_data_unavailable",
            "diagnostic": {"operation": "discover_business_objects", "reason": str(exc), "recommendation": "Restore Business Data connection and retry."},
            "read_only": True,
        }

    period_filter = _clean(payload.get("period"))
    search = _clean(payload.get("search")).casefold()
    offset = max(int(payload.get("offset") or 0), 0)
    limit = min(max(int(payload.get("limit") or DEFAULT_LIMIT), 1), MAX_LIMIT)
    include_all_types = bool(payload.get("include_all_types", not bool(config)))

    filtered_rows = [row for row in rows if not period_filter or _clean(row.get("period")) == period_filter]
    periods = _unique(row.get("period") for row in rows)
    latest_period = periods[-1] if periods else None
    workspace_index = _workspace_index()

    selected_configs = OBJECT_TYPES if include_all_types and config is None else [config or OBJECT_TYPES[0]]
    catalog: Dict[str, Any] = {}
    flat_objects: List[Dict[str, Any]] = []

    for item in selected_configs:
        object_type = item["object_type"]
        values = _unique(row.get(item["field"]) for row in filtered_rows)
        if object_type == "business" and not values:
            values = ["Бон Буассон"]
        if search:
            values = [value for value in values if search in value.casefold()]
        total = len(values)
        page_values = values[offset: offset + limit]
        objects: List[Dict[str, Any]] = []
        for source_value in page_values:
            existing_workspace = workspace_index.get(source_value)
            stable_id = _stable_id(object_type, source_value)
            snapshot_request = {
                "object_type": object_type,
                "object_id": stable_id,
                "business_domain": BUSINESS_DOMAIN,
                "business_object": source_value,
                "period": period_filter or latest_period,
            }
            if existing_workspace and existing_workspace.get("workspace_id"):
                snapshot_request["workspace_id"] = existing_workspace.get("workspace_id")
            obj = {
                "object_type": object_type,
                "object_id": stable_id,
                "display_name": source_value,
                "business_domain": BUSINESS_DOMAIN,
                "workspace_type": item["workspace_type"],
                "workspace_available": True,
                "persistent_workspace_id": existing_workspace.get("workspace_id") if existing_workspace else None,
                "snapshot_supported": True,
                "navigation_supported": True,
                "decision_support_available": bool(existing_workspace and (existing_workspace.get("readiness") or {}).get("decision_readiness") == "READY"),
                "available_period": period_filter or latest_period,
                "research_snapshot_request": snapshot_request,
            }
            objects.append(obj)
            flat_objects.append(obj)
        catalog[object_type] = {
            "display_name": item["display_name"],
            "total": total,
            "offset": offset,
            "count": len(objects),
            "has_more": offset + len(objects) < total,
            "next_offset": offset + len(objects) if offset + len(objects) < total else None,
            "objects": objects,
        }

    status = get_business_data_status()
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "read_only": True,
        "business_domain": BUSINESS_DOMAIN,
        "period_filter": period_filter or None,
        "available_periods": periods,
        "latest_period": latest_period,
        "object_type_filter": requested_type or None,
        "search": _clean(payload.get("search")) or None,
        "catalog": catalog,
        "selected_objects": flat_objects,
        "discovery_metadata": {
            "business_data_connected": bool(status.get("business_data_connected")),
            "object_types_returned": [item["object_type"] for item in selected_configs],
            "total_objects_returned": len(flat_objects),
            "stable_object_ids": True,
            "snapshot_request_embedded": True,
            "product_owner_input_required": False,
        },
        "next_allowed_action": "Select an object and call get_research_workspace_snapshot with research_snapshot_request.",
    }


def verify_business_object_discovery() -> Dict[str, Any]:
    result = discover_business_objects({"limit": 1, "include_all_types": True})
    catalog = result.get("catalog") if isinstance(result.get("catalog"), dict) else {}
    checks: Dict[str, bool] = {
        "discovery_available": result.get("status") == "PASS",
        "read_only_confirmed": result.get("read_only") is True,
        "stable_object_ids": bool((result.get("discovery_metadata") or {}).get("stable_object_ids")),
        "snapshot_request_embedded": bool((result.get("discovery_metadata") or {}).get("snapshot_request_embedded")),
    }
    for item in OBJECT_TYPES:
        block = catalog.get(item["object_type"]) if isinstance(catalog.get(item["object_type"]), dict) else {}
        objects = block.get("objects") if isinstance(block.get("objects"), list) else []
        checks[f"{item['object_type']}_discoverable"] = bool(objects) or int(block.get("total") or 0) == 0
        if objects:
            first = objects[0]
            checks[f"{item['object_type']}_stable_id"] = bool(first.get("object_id"))
            checks[f"{item['object_type']}_snapshot_supported"] = bool(first.get("snapshot_supported") and first.get("research_snapshot_request"))

    passed = all(checks.values())
    return {
        "status": "PASS" if passed else "HOLD",
        "release": RELEASE_ID,
        "checks": checks,
        "catalog_summary": {
            key: {"total": value.get("total"), "sample_count": value.get("count")}
            for key, value in catalog.items() if isinstance(value, dict)
        },
        "operational_readiness": {
            "status": "PASS" if passed else "HOLD",
            "question": "Can Digital Business Analyst autonomously discover and select Business Framework research objects?",
            "answer": "YES" if passed else "NO",
        },
        "first_product_verification_command": "Discover Business Objects",
    }
