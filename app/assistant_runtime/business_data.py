"""FOUNDATION-0008 Business Data read-only access for VECTRA Laboratory.

This module intentionally exposes only read-only inspection helpers. It reuses
Working GPT data loading and domain summary functions, so Laboratory sees the
same Business Data source without introducing a second calculation logic.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional
from datetime import datetime

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

BUSINESS_DATA_ACCESS_RELEASE = "BUSINESS-DISCOVERY-ENGINE-001"
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
    "/vectra/laboratory/business-data/verify",
]

DIMENSION_FIELDS = ["period", "business", "manager_top", "manager", "network", "category", "tmc_group", "sku"]
NUMERIC_FIELDS = ["revenue", "cost", "finrez_pre", "finrez", "retro_bonus", "logistics_cost", "personnel_cost", "other_costs", "markup", "margin_pre"]


BUSINESS_DATA_OPERATION_MANIFEST = [
    {
        "operation_type": "manifest",
        "aliases": ["capabilities", "business_data_manifest", "business_data_capabilities"],
        "description": "Machine-readable Business Data facade capability manifest.",
        "required_parameters": [],
        "optional_parameters": [],
        "supports_pagination": False,
        "max_response_size": "small",
        "read_only": True,
    },
    {
        "operation_type": "status",
        "description": "Business Data connection, health, source and volume status.",
        "required_parameters": [],
        "optional_parameters": [],
        "supports_pagination": False,
        "max_response_size": "small",
        "read_only": True,
    },
    {
        "operation_type": "entities",
        "description": "Entity dictionary and dimensional values preview.",
        "required_parameters": [],
        "optional_parameters": ["limit_per_group"],
        "supports_pagination": False,
        "max_response_size": "medium",
        "read_only": True,
    },
    {
        "operation_type": "summary",
        "aliases": ["summary_business", "business_summary", "summary/business"],
        "description": "Executive business summary for the selected period.",
        "required_parameters": ["period"],
        "optional_parameters": ["limit"],
        "supports_pagination": False,
        "max_response_size": "medium",
        "read_only": True,
        "level": "business",
    },
    {
        "operation_type": "manager_summary",
        "description": "Manager summary for the selected manager and period.",
        "required_parameters": ["period", "manager"],
        "optional_parameters": ["object_name", "limit"],
        "supports_pagination": False,
        "max_response_size": "medium",
        "read_only": True,
        "level": "manager",
    },
    {
        "operation_type": "contract_summary",
        "description": "Network / contract summary for the selected network and period.",
        "required_parameters": ["period", "network"],
        "optional_parameters": ["object_name", "limit"],
        "supports_pagination": False,
        "max_response_size": "medium",
        "read_only": True,
        "level": "network",
    },
    {
        "operation_type": "category_summary",
        "description": "Category summary for the selected category and period.",
        "required_parameters": ["period", "category"],
        "optional_parameters": ["object_name", "limit"],
        "supports_pagination": False,
        "max_response_size": "medium",
        "read_only": True,
        "level": "category",
    },
    {
        "operation_type": "sku_summary",
        "description": "SKU summary for the selected SKU and period.",
        "required_parameters": ["period", "sku"],
        "optional_parameters": ["object_name", "limit"],
        "supports_pagination": False,
        "max_response_size": "medium_to_large",
        "read_only": True,
        "level": "sku",
    },
    {
        "operation_type": "query",
        "description": "Natural business query through the working VECTRA query pipeline.",
        "required_parameters": ["message"],
        "optional_parameters": ["session_id", "query"],
        "supports_pagination": False,
        "max_response_size": "depends_on_query",
        "read_only": True,
    },
    {
        "operation_type": "discovery",
        "aliases": ["business_discovery", "discover", "inspect_source"],
        "description": "Compact structural discovery of Business Data before business analysis.",
        "required_parameters": [],
        "optional_parameters": ["period", "limit", "limit_per_group", "include_samples", "sample_size"],
        "supports_pagination": False,
        "max_response_size": "medium",
        "read_only": True,
        "default_behavior": "structure_before_analysis",
    },
    {
        "operation_type": "first_impression",
        "aliases": ["explore", "initial_exploration", "business_first_impression"],
        "description": "Exploration-first introduction to Business Data. Runtime handles manifest/status/entities/sample internally and returns a non-technical first professional impression.",
        "required_parameters": [],
        "optional_parameters": ["period", "message", "session_id"],
        "supports_pagination": False,
        "max_response_size": "medium",
        "read_only": True,
        "default_behavior": "explore_before_conclusions",
    },
    {
        "operation_type": "verify",
        "description": "Business Data facade verification.",
        "required_parameters": [],
        "optional_parameters": [],
        "supports_pagination": False,
        "max_response_size": "medium",
        "read_only": True,
    },
]


def get_business_data_manifest() -> Dict[str, Any]:
    """Return the machine-readable Business Data facade contract.

    The manifest is the source of truth for supported Business Data operation_type
    values. VECTRA clients should call this before selecting a Business Data
    operation instead of relying on remembered or guessed operation names.
    """
    status = get_business_data_status()
    supported_operation_types = [item["operation_type"] for item in BUSINESS_DATA_OPERATION_MANIFEST]
    return {
        "status": "ok" if status.get("business_data_health") == "PASS" else "degraded",
        "render_mode": "vectra_business_data_facade_manifest",
        "release": BUSINESS_DATA_ACCESS_RELEASE,
        "verification_status": "PASS" if status.get("business_data_health") == "PASS" else "FAIL",
        "business_data_connected": bool(status.get("business_data_connected")),
        "business_data_health": status.get("business_data_health"),
        "read_only": True,
        "mutation_endpoints_exposed": False,
        "source_type": status.get("source_type"),
        "rows_count": status.get("rows_count"),
        "latest_period": status.get("latest_period"),
        "periods_sample": status.get("periods_sample"),
        "contract_source_of_truth": "executeVectraBusinessDataOperation(operation_type=manifest)",
        "facade_name": "executeVectraBusinessDataOperation",
        "facade_endpoint": "/vectra/laboratory/facade/business-data",
        "supported_operation_types": supported_operation_types,
        "operations": BUSINESS_DATA_OPERATION_MANIFEST,
        "usage_policy": {
            "must_read_manifest_before_operation_selection": True,
            "do_not_guess_operation_type": True,
            "use_query_for_natural_language_business_questions": True,
            "use_summary_for_business_period_summary": True,
            "use_discovery_for_initial_source_research": True,
            "use_first_impression_for_initial_data_acquaintance": True,
            "period_required_for_summary_operations": True,
            "technical_discovery_is_internal": True,
            "do_not_ask_product_owner_for_operation_type": True,
        },
        "exploration_first_policy": {
            "principle": "When connecting to a Runtime data source for the first time, VECTRA must explore before conclusions.",
            "user_visible_behavior": "Return a first professional impression of the business data, not a technical explanation of Actions, Facade, Manifest, or operation_type.",
            "internal_steps": [
                "read_manifest",
                "check_status",
                "run_compact_discovery",
                "inspect_entities",
                "assess_data_quality",
                "build_preliminary_object_map",
                "form_first_impression_without_business_conclusions"
            ],
            "prohibited_user_questions": [
                "Which operation_type should I use?",
                "Which Action should I call?",
                "How is Runtime structured?"
            ]
        },
        "common_user_requests": [
            {
                "user_request": "Подключись к Business Data и дай первое впечатление.",
                "recommended_operation_type": "first_impression",
                "reason": "Initial acquaintance with raw business data without final conclusions."
            },
            {
                "user_request": "Покажи бизнес за период.",
                "recommended_operation_type": "summary",
                "required_parameters": ["period"]
            },
            {
                "user_request": "Ответь на свободный бизнес-вопрос.",
                "recommended_operation_type": "query",
                "required_parameters": ["message"]
            }
        ],
        "large_response_policy": {
            "prefer_period_filter": True,
            "prefer_specific_level_summary_over_raw_query_for_large_requests": True,
            "response_too_large_mitigation": "Use period and object filters; request focused summaries before broad queries.",
        },
    }


def _unique_values(rows: List[Dict[str, Any]], field: str, limit: Optional[int] = None) -> List[str]:
    values = sorted({str(row.get(field, "")).strip() for row in rows if str(row.get(field, "")).strip()})
    if limit is None:
        return values
    return values[: max(0, int(limit))]


def _is_filled(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(text) and text.lower() not in {"none", "nan", "null"}


def _field_presence(rows: List[Dict[str, Any]], field: str) -> Dict[str, Any]:
    filled = sum(1 for row in rows if _is_filled(row.get(field)))
    return {"field": field, "filled": filled, "empty": max(0, len(rows) - filled), "unique_count": len(_unique_values(rows, field))}



def _safe_preview(values: List[str], limit: int = 8) -> List[str]:
    return values[: max(0, int(limit))]



def _filter_rows_by_period(rows: List[Dict[str, Any]], period: Optional[str]) -> List[Dict[str, Any]]:
    selected = (period or "").strip()
    if not selected:
        return rows
    return [row for row in rows if str(row.get("period", "")).strip() == selected]


def _period_gaps(periods: List[str]) -> List[str]:
    parsed = []
    for value in periods:
        try:
            parsed.append(datetime.strptime(value, "%Y-%m"))
        except (TypeError, ValueError):
            continue
    if len(parsed) < 2:
        return []
    parsed = sorted(set(parsed))
    present = {item.strftime("%Y-%m") for item in parsed}
    cursor = parsed[0]
    last = parsed[-1]
    gaps: List[str] = []
    while cursor <= last:
        key = cursor.strftime("%Y-%m")
        if key not in present:
            gaps.append(key)
        year = cursor.year + (1 if cursor.month == 12 else 0)
        month = 1 if cursor.month == 12 else cursor.month + 1
        cursor = cursor.replace(year=year, month=month)
    return gaps


def _completeness(rows: List[Dict[str, Any]], fields: List[str]) -> List[Dict[str, Any]]:
    total = len(rows)
    result: List[Dict[str, Any]] = []
    for field in fields:
        presence = _field_presence(rows, field)
        filled = int(presence["filled"])
        presence["fill_rate_percent"] = round((filled / total * 100.0), 2) if total else 0.0
        result.append(presence)
    return result


def get_business_data_discovery(
    period: Optional[str] = None,
    limit: int = 25,
    limit_per_group: int = 12,
    include_samples: bool = False,
    sample_size: int = 3,
) -> Dict[str, Any]:
    """Return a compact Business Data Discovery Report.

    Discovery describes source structure, volume, cardinality, completeness and
    time coverage. It intentionally does not run an executive summary or infer
    causes, recommendations or business performance conclusions.
    """
    safe_limit = min(max(int(limit or 25), 1), 100)
    safe_group_limit = min(max(int(limit_per_group or 12), 1), 25)
    safe_sample_size = min(max(int(sample_size or 3), 1), 10)
    try:
        all_rows = load_raw_rows()
        rows = _filter_rows_by_period(all_rows, period)
        available_periods = _unique_values(all_rows, "period")
        selected_periods = _unique_values(rows, "period")
        fields = sorted({str(key) for row in rows[:safe_limit] for key in row.keys()})
        dimensions = {field: len(_unique_values(rows, field)) for field in DIMENSION_FIELDS}
        quality = _completeness(rows, DIMENSION_FIELDS + NUMERIC_FIELDS)
        missing_fields = [item["field"] for item in quality if item["filled"] == 0]
        partially_filled_fields = [item["field"] for item in quality if 0 < item["filled"] < len(rows)]
        period_gaps = _period_gaps(available_periods)
        preview = {field: _unique_values(rows, field, limit=safe_group_limit) for field in DIMENSION_FIELDS}
        sample_rows = rows[:safe_sample_size] if include_samples else []
        status = "ok"
        error = None
    except Exception as exc:  # pragma: no cover - defensive Action response
        all_rows = []
        rows = []
        available_periods = []
        selected_periods = []
        fields = []
        dimensions = {field: 0 for field in DIMENSION_FIELDS}
        quality = []
        missing_fields = DIMENSION_FIELDS + NUMERIC_FIELDS
        partially_filled_fields = []
        period_gaps = []
        preview = {field: [] for field in DIMENSION_FIELDS}
        sample_rows = []
        status = "degraded"
        error = str(exc)

    preliminary_object_map = [
        {"from": "business", "to": "manager_top", "confidence": "medium", "status": "observation"},
        {"from": "manager_top", "to": "manager", "confidence": "high", "status": "observation"},
        {"from": "manager", "to": "network", "confidence": "medium", "status": "observation"},
        {"from": "network", "to": "category", "confidence": "medium", "status": "observation"},
        {"from": "category", "to": "tmc_group", "confidence": "high", "status": "observation"},
        {"from": "tmc_group", "to": "sku", "confidence": "high", "status": "observation"},
    ]
    unknown_business_meanings = [
        {"field": "finrez_pre", "question": "Чем финансовый результат ДО отличается от итогового финансового результата?"},
        {"field": "retro_bonus", "question": "Какие начисления и договорные условия входят в ретро-бонус?"},
        {"field": "network", "question": "Network обозначает сеть, клиента или договорный контур?"},
        {"field": "markup", "question": "Какой метод расчёта наценки является бизнес-стандартом?"},
        {"field": "margin_pre", "question": "Какой управленческий смысл закреплён за маржой ДО?"},
    ]
    return {
        "status": status,
        "render_mode": "vectra_business_data_discovery_report",
        "release": BUSINESS_DATA_ACCESS_RELEASE,
        "verification_status": "PASS" if status == "ok" and bool(rows) else "FAIL",
        "business_data_connected": bool(all_rows),
        "read_only": True,
        "capitalization_performed": False,
        "deep_business_analysis_performed": False,
        "manifest_used": True,
        "operation_type_guessing_used": False,
        "selected_period": (period or "").strip() or None,
        "source": {
            "source_type": "Google Sheets CSV via Runtime data loader",
            "access_mode": "read_only",
            "rows_count": len(rows),
            "total_rows_count": len(all_rows),
        },
        "time_structure": {
            "periods_count": len(selected_periods if period else available_periods),
            "first_period": (selected_periods if period else available_periods)[0] if (selected_periods if period else available_periods) else None,
            "latest_period": (selected_periods if period else available_periods)[-1] if (selected_periods if period else available_periods) else None,
            "periods": (selected_periods if period else available_periods)[:safe_limit],
            "periods_truncated": len(selected_periods if period else available_periods) > safe_limit,
            "missing_periods": period_gaps[:safe_limit],
            "regularity": "monthly_without_gaps" if available_periods and not period_gaps else ("monthly_with_gaps" if available_periods else "unknown"),
            "history_sufficient_for_trend_analysis": len(available_periods) >= 12,
        },
        "schema": {
            "fields": fields,
            "dimensions": DIMENSION_FIELDS,
            "measures": NUMERIC_FIELDS,
            "grain_hypothesis": ["period", "business", "manager_top", "manager", "network", "category", "tmc_group", "sku"],
            "grain_status": "observation_requires_business_confirmation",
        },
        "cardinality": dimensions,
        "entity_preview": preview,
        "data_quality": {
            "status": "PASS" if not missing_fields else "WARNING",
            "field_completeness": quality,
            "missing_fields": missing_fields,
            "partially_filled_fields": partially_filled_fields,
        },
        "preliminary_object_map": preliminary_object_map,
        "unknown_business_meanings": unknown_business_meanings,
        "business_source_profile": {
            "profile_type": "runtime_observation_not_business_knowledge",
            "schema_fields": fields,
            "dimension_cardinality": dimensions,
            "periods_count": len(available_periods),
            "latest_period": available_periods[-1] if available_periods else None,
        },
        "confirmed_facts": [
            "Источник доступен в режиме только чтения." if all_rows else "Источник данных недоступен.",
            f"Доступно строк: {len(rows)}.",
            f"Доступно периодов: {len(selected_periods if period else available_periods)}.",
        ],
        "observations": [
            "SKU выглядит наиболее детальным объектным уровнем.",
            "Данные имеют иерархическую структуру, но связи требуют подтверждения Product Owner.",
            "Показатели отделимы от измерений для последующего управленческого анализа.",
        ],
        "recommended_next_step": "Подтвердить бизнес-смысл неизвестных показателей и затем выбрать период или объект для анализа.",
        "samples": sample_rows,
        "samples_included": bool(include_samples),
        "error": error,
    }


def get_business_data_first_impression(period: Optional[str] = None, message: str = "") -> Dict[str, Any]:
    """Return a business-facing introduction based on structural discovery only."""
    discovery = get_business_data_discovery(period=period, include_samples=False)
    source = discovery.get("source") or {}
    time_structure = discovery.get("time_structure") or {}
    cardinality = discovery.get("cardinality") or {}
    return {
        "status": discovery.get("status"),
        "render_mode": "vectra_business_data_first_impression",
        "release": BUSINESS_DATA_ACCESS_RELEASE,
        "verification_status": discovery.get("verification_status"),
        "business_data_connected": discovery.get("business_data_connected"),
        "read_only": True,
        "technical_discovery_performed": True,
        "technical_details_hidden_from_user": True,
        "deep_business_analysis_performed": False,
        "capitalization_performed": False,
        "selected_period": discovery.get("selected_period"),
        "confirmed_facts": {
            "rows_count": source.get("rows_count"),
            "total_rows_count": source.get("total_rows_count"),
            "periods_count": time_structure.get("periods_count"),
            "first_period": time_structure.get("first_period"),
            "latest_period": time_structure.get("latest_period"),
            "dimensions": cardinality,
            "measures": (discovery.get("schema") or {}).get("measures", []),
            "data_quality": discovery.get("data_quality"),
        },
        "observations": discovery.get("observations", []),
        "questions_for_product_owner": discovery.get("unknown_business_meanings", []),
        "preliminary_object_map": discovery.get("preliminary_object_map", []),
        "recommended_next_step": discovery.get("recommended_next_step"),
        "discovery_report": discovery,
    }

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
            "openapi_endpoint": "/vectra/laboratory/openapi/business-data.json",
            "split_openapi_endpoints": {
                "core": "/vectra/laboratory/openapi/core.json",
                "business_data": "/vectra/laboratory/openapi/business-data.json",
                "knowledge_self_evolution": "/vectra/laboratory/openapi/knowledge.json",
            },
            "actions_schema_version": "FOUNDATION-0008-PV",
            "actions_schema_contains_business_data_endpoints": True,
            "business_data_scope": "read_only_existing_runtime_business_data",
            "product_owner_manual_copy_required": False,
        },
        "error": error,
    }
    return payload


def get_business_data_entities(limit_per_group: int = 50) -> Dict[str, Any]:
    try:
        rows = load_raw_rows()
        dictionary = get_entity_dictionary()
        preview = {field: _unique_values(rows, field, limit=limit_per_group) for field in DIMENSION_FIELDS}
        counts = {field: len(_unique_values(rows, field)) for field in DIMENSION_FIELDS}
        status = "ok"
        error = None
    except Exception as exc:  # pragma: no cover - defensive Action response
        preview = {field: [] for field in DIMENSION_FIELDS}
        counts = {field: 0 for field in DIMENSION_FIELDS}
        dictionary = {}
        status = "degraded"
        error = str(exc)
    return {
        "status": status,
        "render_mode": "vectra_laboratory_business_data_entities",
        "release": BUSINESS_DATA_ACCESS_RELEASE,
        "read_only": True,
        "entity_counts": counts,
        "entity_preview": preview,
        "entity_dictionary": dictionary,
        "error": error,
    }


def get_business_data_sample(limit: int = 10) -> Dict[str, Any]:
    safe_limit = min(max(int(limit or 10), 1), 50)
    try:
        rows = load_raw_rows()[:safe_limit]
        status = "ok"
        error = None
    except Exception as exc:  # pragma: no cover - defensive Action response
        rows = []
        status = "degraded"
        error = str(exc)
    return {
        "status": status,
        "render_mode": "vectra_laboratory_business_data_sample",
        "release": BUSINESS_DATA_ACCESS_RELEASE,
        "read_only": True,
        "limit": safe_limit,
        "rows": rows,
        "error": error,
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
    try:
        payload = orchestrate_vectra_query(message or "", session_id=session_id or "laboratory-read-only")
        payload = apply_runtime_contract(payload if isinstance(payload, dict) else {"status": "error", "reason": "invalid_query_payload"})
    except Exception as exc:  # pragma: no cover - defensive Action response
        payload = {
            "status": "degraded",
            "render_mode": "vectra_laboratory_business_data_query",
            "reason": "business_data_query_unavailable",
            "error": str(exc),
        }
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
    exposed_endpoints = status.get("available_read_only_endpoints") or []
    required_action_endpoints = [
        "/vectra/laboratory/business-data/status",
        "/vectra/laboratory/business-data/entities",
        "/vectra/laboratory/business-data/sample",
        "/vectra/laboratory/business-data/query",
    ]
    checks = [
        {"check": "business_data_connected", "status": "PASS" if status.get("business_data_connected") else "FAIL"},
        {"check": "rows_available", "status": "PASS" if status.get("rows_count", 0) > 0 else "FAIL"},
        {"check": "entities_available", "status": "PASS" if entities.get("entity_counts") else "FAIL"},
        {"check": "sample_available", "status": "PASS" if sample.get("rows") else "FAIL"},
        {"check": "read_only", "status": "PASS" if status.get("read_only") and not status.get("mutation_endpoints_exposed") else "FAIL"},
        {"check": "same_source_as_working_gpt", "status": "PASS" if status.get("same_source_as_working_gpt") else "FAIL"},
        {"check": "actions_schema_required_endpoints_declared", "status": "PASS" if all(endpoint in exposed_endpoints for endpoint in required_action_endpoints) else "FAIL"},
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
        "actions_schema_version": "FOUNDATION-0008-PV",
        "required_action_endpoints": required_action_endpoints,
        "available_read_only_endpoints": exposed_endpoints,
    }
