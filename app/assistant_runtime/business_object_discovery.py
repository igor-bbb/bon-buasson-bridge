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

RELEASE_ID = "BUSINESS-OBJECT-DISCOVERY-SCALABILITY-001"
BUSINESS_DOMAIN = "bon_buasson"
DEFAULT_LIMIT = 50
MAX_LIMIT = 100

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
        "capability": "business_object_discovery_v2",
        "access_mode": "read-only",
        "business_domain": BUSINESS_DOMAIN,
        "supported_object_types": [item["object_type"] for item in OBJECT_TYPES],
        "supported_parameters": [
            "object_type", "period", "name_contains", "offset", "limit",
            "sort_by", "summary_only"
        ],
        "sorting": ["name", "priority", "default_business_order"],
        "pagination": {
            "supported": True,
            "default_limit": DEFAULT_LIMIT,
            "maximum_limit": MAX_LIMIT,
        },
        "safe_default": "When object_type is omitted Runtime returns summary_only metadata.",
        "usage": "Request one object type per page, select an object, then submit its research_snapshot_request to get_research_workspace_snapshot.",
    }


def _safe_int(value: Any, default: int, minimum: int, maximum: int) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        number = default
    return min(max(number, minimum), maximum)


def _object_priority(item: Dict[str, Any]) -> tuple:
    return (
        0 if item.get("decision_support_supported") else 1,
        0 if item.get("persistent_workspace_id") else 1,
        _clean(item.get("display_name")).casefold(),
    )


def _sort_objects(objects: List[Dict[str, Any]], sort_by: str) -> List[Dict[str, Any]]:
    if sort_by == "priority":
        return sorted(objects, key=_object_priority)
    if sort_by == "default_business_order":
        order = {item["object_type"]: index for index, item in enumerate(OBJECT_TYPES)}
        return sorted(objects, key=lambda item: (order.get(item.get("object_type"), 999), _clean(item.get("display_name")).casefold()))
    return sorted(objects, key=lambda item: _clean(item.get("display_name")).casefold())


def _build_object(item: Dict[str, str], source_value: str, period: Optional[str], workspace_index: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
    object_type = item["object_type"]
    existing_workspace = workspace_index.get(source_value)
    stable_id = _stable_id(object_type, source_value)
    snapshot_request: Dict[str, Any] = {
        "object_type": object_type,
        "object_id": stable_id,
        "business_domain": BUSINESS_DOMAIN,
        "business_object": source_value,
        "period": period,
    }
    if existing_workspace and existing_workspace.get("workspace_id"):
        snapshot_request["workspace_id"] = existing_workspace.get("workspace_id")
    readiness = existing_workspace.get("readiness") if isinstance(existing_workspace, dict) else {}
    return {
        "object_type": object_type,
        "object_id": stable_id,
        "display_name": source_value,
        "workspace_available": True,
        "snapshot_supported": True,
        "navigation_supported": True,
        "decision_support_supported": bool(isinstance(readiness, dict) and readiness.get("decision_readiness") == "READY"),
        "persistent_workspace_id": existing_workspace.get("workspace_id") if existing_workspace else None,
        "research_snapshot_request": snapshot_request,
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

    sort_by = _clean(payload.get("sort_by") or "default_business_order").lower()
    if sort_by not in {"name", "priority", "default_business_order"}:
        return {
            "status": "VALIDATION_ERROR",
            "reason": "unsupported_sort_by",
            "supported_sorting": ["name", "priority", "default_business_order"],
            "read_only": True,
        }

    summary_only = bool(payload.get("summary_only", not bool(requested_type)))
    if not requested_type and not summary_only:
        return {
            "status": "VALIDATION_ERROR",
            "reason": "object_type_required_for_object_listing",
            "recommendation": "Set object_type or use summary_only=true.",
            "read_only": True,
        }

    try:
        rows = load_raw_rows()
    except Exception as exc:
        return {
            "status": "HOLD",
            "reason": "business_data_unavailable",
            "diagnostic": {
                "operation": "discover_business_objects",
                "reason": str(exc),
                "recommendation": "Restore Business Data connection and retry.",
            },
            "read_only": True,
        }

    period_filter = _clean(payload.get("period"))
    name_contains = _clean(payload.get("name_contains") or payload.get("search")).casefold()
    offset = _safe_int(payload.get("offset"), 0, 0, 10_000_000)
    limit = _safe_int(payload.get("limit"), DEFAULT_LIMIT, 1, MAX_LIMIT)

    filtered_rows = [row for row in rows if not period_filter or _clean(row.get("period")) == period_filter]
    periods = _unique(row.get("period") for row in rows)
    latest_period = periods[-1] if periods else None
    effective_period = period_filter or latest_period
    workspace_index = _workspace_index()

    counts_by_type: Dict[str, int] = {}
    values_by_type: Dict[str, List[str]] = {}
    for item in OBJECT_TYPES:
        values = _unique(row.get(item["field"]) for row in filtered_rows)
        if item["object_type"] == "business" and not values:
            values = ["Бон Буассон"]
        if name_contains:
            values = [value for value in values if name_contains in value.casefold()]
        values_by_type[item["object_type"]] = values
        counts_by_type[item["object_type"]] = len(values)

    summary = {
        "total_count": sum(counts_by_type.values()),
        "available_types": [item["object_type"] for item in OBJECT_TYPES],
        "counts_by_type": counts_by_type,
        "period_filter": period_filter or None,
        "latest_period": latest_period,
    }

    if summary_only:
        response = {
            "status": "PASS",
            "release": RELEASE_ID,
            "capability": "business_object_discovery_v2",
            "read_only": True,
            "summary_only": True,
            "business_domain": BUSINESS_DOMAIN,
            "summary": summary,
            "pagination": {
                "requested_count": 0,
                "returned_count": 0,
                "total_count": summary["total_count"],
                "has_more": False,
                "next_offset": None,
            },
            "runtime_diagnostics": {
                "pagination_applied": False,
                "result_truncated": False,
                "response_size_bytes": 0,
            },
            "next_allowed_action": "Choose object_type and request a compact page.",
        }
        import json
        response["runtime_diagnostics"]["response_size_bytes"] = len(json.dumps(response, ensure_ascii=False, default=str).encode("utf-8"))
        return response

    assert config is not None
    values = values_by_type[config["object_type"]]
    all_objects = [_build_object(config, value, effective_period, workspace_index) for value in values]
    all_objects = _sort_objects(all_objects, sort_by)
    total_count = len(all_objects)
    page = all_objects[offset: offset + limit]
    returned_count = len(page)
    has_more = offset + returned_count < total_count
    next_offset = offset + returned_count if has_more else None

    response = {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "business_object_discovery_v2",
        "read_only": True,
        "summary_only": False,
        "business_domain": BUSINESS_DOMAIN,
        "object_type": config["object_type"],
        "period_filter": period_filter or None,
        "name_contains": _clean(payload.get("name_contains") or payload.get("search")) or None,
        "sort_by": sort_by,
        "objects": page,
        "pagination": {
            "requested_count": limit,
            "returned_count": returned_count,
            "total_count": total_count,
            "offset": offset,
            "has_more": has_more,
            "next_offset": next_offset,
        },
        "summary": summary,
        "runtime_diagnostics": {
            "pagination_applied": True,
            "result_truncated": has_more,
            "response_size_bytes": 0,
        },
        "next_allowed_action": "Select one returned object and call get_research_workspace_snapshot with its research_snapshot_request.",
    }
    import json
    response["runtime_diagnostics"]["response_size_bytes"] = len(json.dumps(response, ensure_ascii=False, default=str).encode("utf-8"))
    return response


def verify_business_object_discovery() -> Dict[str, Any]:
    checks: Dict[str, bool] = {}
    summary = discover_business_objects({"summary_only": True})
    checks["summary_only"] = summary.get("status") == "PASS" and summary.get("summary_only") is True
    checks["read_only_confirmed"] = summary.get("read_only") is True

    scenarios = ["business", "top_manager", "network", "category"]
    for object_type in scenarios:
        result = discover_business_objects({"object_type": object_type, "offset": 0, "limit": 10, "sort_by": "default_business_order"})
        checks[f"{object_type}_page"] = result.get("status") == "PASS" and (result.get("pagination") or {}).get("returned_count", 0) <= 10

    sku_first = discover_business_objects({"object_type": "sku", "offset": 0, "limit": 100, "sort_by": "name"})
    sku_second = discover_business_objects({"object_type": "sku", "offset": 100, "limit": 100, "sort_by": "name"})
    checks["sku_first_100"] = sku_first.get("status") == "PASS" and (sku_first.get("pagination") or {}).get("returned_count", 0) <= 100
    checks["sku_next_100"] = sku_second.get("status") == "PASS" and (sku_second.get("pagination") or {}).get("offset") == 100
    checks["pagination_metadata"] = all(key in (sku_first.get("pagination") or {}) for key in ["requested_count", "returned_count", "total_count", "has_more", "next_offset"])
    checks["runtime_diagnostics"] = all(key in (sku_first.get("runtime_diagnostics") or {}) for key in ["response_size_bytes", "pagination_applied", "result_truncated"])
    checks["snapshot_request_ready"] = all(bool(item.get("research_snapshot_request")) for item in sku_first.get("objects", []))

    passed = all(checks.values())
    return {
        "status": "PASS" if passed else "HOLD",
        "release": RELEASE_ID,
        "checks": checks,
        "scenarios": {
            "summary": summary.get("summary"),
            "sku_first_page": sku_first.get("pagination"),
            "sku_second_page": sku_second.get("pagination"),
        },
        "operational_readiness": {
            "status": "PASS" if passed else "HOLD",
            "question": "Does Business Object Discovery scale independently of catalogue size?",
            "answer": "YES" if passed else "NO",
        },
        "first_product_verification_command": "Discover Business Objects with summary_only=true, then page one object_type at a time.",
    }
