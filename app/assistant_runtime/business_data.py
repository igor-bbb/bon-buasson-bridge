"""FOUNDATION-0008 Business Data read-only access for VECTRA Laboratory.

This module intentionally exposes only read-only inspection helpers. It reuses
Working GPT data loading and domain summary functions, so Laboratory sees the
same Business Data source without introducing a second calculation logic.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.config import SHEET_URL
from app.data.loader import normalize_sheet_url, get_csv_text
from app.data.reader import load_raw_rows
from app.domain.filters import get_normalized_rows
from app.domain.summary import (
    get_business_summary,
    get_manager_top_summary,
    get_manager_summary,
    get_network_summary,
    get_category_summary,
    get_tmc_group_summary,
    get_sku_summary,
)
from app.query.entity_dictionary import get_entity_dictionary
from app.query.orchestration import orchestrate_vectra_query
from app.workspace_runtime import apply_runtime_contract

BUSINESS_DATA_ACCESS_RELEASE = "FOUNDATION-0008"
READ_ONLY_ENDPOINTS = [
    "/vectra/laboratory/business-data/status",
    "/vectra/laboratory/business-data/entities",
    "/vectra/laboratory/business-data/sample",
    "/vectra/laboratory/business-data/summary/business",
    "/vectra/laboratory/business-data/summary/manager-top",
    "/vectra/laboratory/business-data/summary/manager",
    "/vectra/laboratory/business-data/summary/network",
    "/vectra/laboratory/business-data/summary/category",
    "/vectra/laboratory/business-data/summary/tmc-group",
    "/vectra/laboratory/business-data/summary/sku",
    "/vectra/laboratory/business-data/query",
]

DIMENSION_FIELDS = ["period", "business", "manager_top", "manager", "network", "category", "tmc_group", "sku"]
NUMERIC_FIELDS = ["revenue", "cost", "finrez_pre", "finrez", "retro_bonus", "logistics_cost", "personnel_cost", "other_costs", "markup", "margin_pre"]


def _unique_values(rows: List[Dict[str, Any]], field: str, limit: Optional[int] = None) -> List[str]:
    values = sorted({str(row.get(field, "")).strip() for row in rows if str(row.get(field, "")).strip()})
    if limit is None:
        return values
    return values[: max(0, int(limit))]


def _field_presence(rows: List[Dict[str, Any]], field: str) -> Dict[str, Any]:
    filled = sum(1 for row in rows if str(row.get(field, "")).strip() != "")
    return {"field": field, "filled": filled, "empty": max(0, len(rows) - filled), "unique_count": len(_unique_values(rows, field))}


def get_business_data_status() -> Dict[str, Any]:
    try:
        csv_text = get_csv_text()
        rows = load_raw_rows()
        normalized_rows = get_normalized_rows()
        health = "PASS" if rows and normalized_rows else "FAIL"
        error = None
    except Exception as exc:  # pragma: no cover - defensive runtime status
        csv_text = ""
        rows = []
        normalized_rows = []
        health = "FAIL"
        error = str(exc)

    dimension_counts = {field: len(_unique_values(rows, field)) for field in DIMENSION_FIELDS}
    periods = _unique_values(rows, "period")
    payload = {
        "status": "ok" if health == "PASS" else "degraded",
        "render_mode": "vectra_laboratory_business_data_status",
        "release": BUSINESS_DATA_ACCESS_RELEASE,
        "business_data_connected": health == "PASS",
        "business_data_health": health,
        "read_only": True,
        "mutation_endpoints_exposed": False,
        "same_source_as_working_gpt": True,
        "source_type": "Google Sheets CSV via Runtime data loader",
        "source_configured": bool(SHEET_URL),
        "source_url_normalized": normalize_sheet_url(SHEET_URL) if SHEET_URL else None,
        "csv_loaded": bool(csv_text),
        "csv_bytes": len(csv_text.encode("utf-8")) if csv_text else 0,
        "rows_count": len(rows),
        "normalized_rows_count": len(normalized_rows),
        "periods_count": len(periods),
        "periods_sample": periods[:10],
        "latest_period": periods[-1] if periods else None,
        "dimension_counts": dimension_counts,
        "field_presence": [_field_presence(rows, field) for field in DIMENSION_FIELDS + NUMERIC_FIELDS],
        "available_read_only_endpoints": READ_ONLY_ENDPOINTS,
        "laboratory_access": {
            "openapi_published": True,
            "openapi_endpoint": "/vectra/laboratory/openapi.json",
            "business_data_scope": "read_only_existing_runtime_business_data",
            "product_owner_manual_copy_required": False,
        },
        "error": error,
    }
    return payload


def get_business_data_entities(limit_per_group: int = 50) -> Dict[str, Any]:
    rows = load_raw_rows()
    dictionary = get_entity_dictionary()
    preview = {field: _unique_values(rows, field, limit=limit_per_group) for field in DIMENSION_FIELDS}
    counts = {field: len(_unique_values(rows, field)) for field in DIMENSION_FIELDS}
    return {
        "status": "ok",
        "render_mode": "vectra_laboratory_business_data_entities",
        "release": BUSINESS_DATA_ACCESS_RELEASE,
        "read_only": True,
        "entity_counts": counts,
        "entity_preview": preview,
        "entity_dictionary": dictionary,
    }


def get_business_data_sample(limit: int = 10) -> Dict[str, Any]:
    safe_limit = min(max(int(limit or 10), 1), 50)
    rows = load_raw_rows()[:safe_limit]
    return {
        "status": "ok",
        "render_mode": "vectra_laboratory_business_data_sample",
        "release": BUSINESS_DATA_ACCESS_RELEASE,
        "read_only": True,
        "limit": safe_limit,
        "rows": rows,
    }


def get_business_data_summary(level: str, period: str, **kwargs: Any) -> Dict[str, Any]:
    level = (level or "").strip().lower().replace("_", "-")
    if level == "business":
        result = get_business_summary(period=period)
    elif level == "manager-top":
        result = get_manager_top_summary(manager_top=kwargs.get("manager_top", ""), period=period)
    elif level == "manager":
        result = get_manager_summary(manager=kwargs.get("manager", ""), period=period)
    elif level == "network":
        result = get_network_summary(network=kwargs.get("network", ""), period=period)
    elif level == "category":
        result = get_category_summary(category=kwargs.get("category", ""), period=period)
    elif level == "tmc-group":
        result = get_tmc_group_summary(tmc_group=kwargs.get("tmc_group", ""), period=period)
    elif level == "sku":
        result = get_sku_summary(sku=kwargs.get("sku", ""), period=period)
    else:
        return {"status": "error", "reason": "unsupported_business_data_summary_level", "level": level, "read_only": True}

    if isinstance(result, dict):
        result = dict(result)
        result.setdefault("status", "ok")
        result["read_only"] = True
        result["business_data_access_release"] = BUSINESS_DATA_ACCESS_RELEASE
        result["same_source_as_working_gpt"] = True
    return result


def run_business_data_query(message: str, session_id: str = "laboratory-read-only") -> Dict[str, Any]:
    """Run existing VECTRA query pipeline for Laboratory in read-only mode.

    The orchestration layer may maintain in-process conversational state, but this
    endpoint does not write Business Data or Runtime Repository objects and does
    not expose any mutation action in Laboratory OpenAPI.
    """
    payload = orchestrate_vectra_query(message or "", session_id=session_id or "laboratory-read-only")
    payload = apply_runtime_contract(payload if isinstance(payload, dict) else {"status": "error", "reason": "invalid_query_payload"})
    if isinstance(payload, dict):
        payload = dict(payload)
        payload["read_only"] = True
        payload["business_data_access_release"] = BUSINESS_DATA_ACCESS_RELEASE
        payload["same_source_as_working_gpt"] = True
        payload["mutation_endpoints_exposed"] = False
    return payload


def verify_business_data_access() -> Dict[str, Any]:
    status = get_business_data_status()
    entities = get_business_data_entities(limit_per_group=5)
    sample = get_business_data_sample(limit=3)
    checks = [
        {"check": "business_data_connected", "status": "PASS" if status.get("business_data_connected") else "FAIL"},
        {"check": "rows_available", "status": "PASS" if status.get("rows_count", 0) > 0 else "FAIL"},
        {"check": "entities_available", "status": "PASS" if entities.get("entity_counts") else "FAIL"},
        {"check": "sample_available", "status": "PASS" if sample.get("rows") else "FAIL"},
        {"check": "read_only", "status": "PASS" if status.get("read_only") and not status.get("mutation_endpoints_exposed") else "FAIL"},
        {"check": "same_source_as_working_gpt", "status": "PASS" if status.get("same_source_as_working_gpt") else "FAIL"},
    ]
    result = "PASS" if all(item.get("status") == "PASS" for item in checks) else "FAIL"
    return {
        "status": "ok" if result == "PASS" else "degraded",
        "render_mode": "vectra_laboratory_business_data_verify",
        "release": BUSINESS_DATA_ACCESS_RELEASE,
        "verification_result": result,
        "checks": checks,
        "business_data_status": status,
        "entity_counts": entities.get("entity_counts"),
        "sample_size": len(sample.get("rows") or []),
        "read_only": True,
        "mutation_endpoints_exposed": False,
    }
