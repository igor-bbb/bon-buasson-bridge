"""Deterministic rolling Category ABC and category-specific matrix candidates.

Category strength is calculated across the whole business inside one exact
category.  Network presence is a separate downstream read-only step and cannot
change Category ABC classification.
"""

from __future__ import annotations

import hashlib
import json
from collections import defaultdict
from typing import Any, Dict, List, Optional

from app.domain.business_abc import (
    ABC_A_LIMIT_PERCENT,
    ABC_B_LIMIT_PERCENT,
    MAX_LIMIT,
    ROLLING_MONTHS,
    _abc_assignments,
    _dominant_text,
    rolling_periods,
)
from app.domain.filters import get_normalized_rows
from app.domain.normalization import clean_text, round_money
from app.domain.summary import _format_from_product_text


CATEGORY_ABC_RELEASE = "VECTRA-CATEGORY-ABC-ROLLING-6M-001"
SUPPORTED_CATEGORIES = ("Вода", "Напитки", "Энергетики")
DEFAULT_LIMIT = 50
MAX_CATEGORY_LIMIT = 100


def _number(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _normalized(value: Any) -> str:
    return clean_text(value).casefold()


def _canonical_category(value: Any) -> str:
    requested = _normalized(value)
    for category in SUPPORTED_CATEGORIES:
        if requested == category.casefold():
            return category
    return ""


def build_category_abc(
    end_period: str,
    category: str,
    *,
    limit: int = DEFAULT_LIMIT,
    rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Return rolling-six-month ABC calculated only within one category."""
    category_name = _canonical_category(category)
    if not category_name:
        return {
            "status": "error",
            "operation_type": "get_category_abc",
            "failure_reason": "supported_category_required",
            "supported_categories": list(SUPPORTED_CATEGORIES),
            "read_only": True,
            "release": CATEGORY_ABC_RELEASE,
        }

    periods = rolling_periods(end_period)
    if not periods:
        return {
            "status": "error",
            "operation_type": "get_category_abc",
            "failure_reason": "valid_end_period_required",
            "expected_format": "YYYY-MM",
            "read_only": True,
            "release": CATEGORY_ABC_RELEASE,
        }

    source_rows = list(rows) if rows is not None else list(get_normalized_rows())
    period_set = set(periods)
    selected_rows = [
        row for row in source_rows
        if clean_text(row.get("period")) in period_set
        and _normalized(row.get("category")) == category_name.casefold()
        and clean_text(row.get("sku"))
        and _normalized(row.get("sku")) not in {"без sku", "total", "итого"}
    ]
    if not selected_rows:
        return {
            "status": "error",
            "operation_type": "get_category_abc",
            "failure_reason": "category_data_not_found_for_period_range",
            "category": category_name,
            "read_only": True,
            "release": CATEGORY_ABC_RELEASE,
            "horizon": {
                "type": "rolling_months",
                "months": ROLLING_MONTHS,
                "start_period": periods[0],
                "end_period": periods[-1],
                "periods": periods,
            },
        }

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in selected_rows:
        grouped[clean_text(row.get("sku"))].append(row)

    items: List[Dict[str, Any]] = []
    for sku in sorted(grouped, key=str.casefold):
        sku_rows = grouped[sku]
        tmc_group = _dominant_text(sku_rows, "tmc_group")
        monthly_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in sku_rows:
            monthly_rows[clean_text(row.get("period"))].append(row)
        dynamics = []
        for period in periods:
            period_rows = monthly_rows.get(period, [])
            dynamics.append({
                "period": period,
                "revenue": round_money(sum(_number(row.get("revenue")) for row in period_rows)),
                "finrez_pre": round_money(sum(_number(row.get("finrez_pre")) for row in period_rows)),
            })
        items.append({
            "sku": sku,
            "category": category_name,
            "format": _format_from_product_text(tmc_group or sku),
            "revenue_6m": round_money(sum(point["revenue"] for point in dynamics)),
            "finrez_pre_6m": round_money(sum(point["finrez_pre"] for point in dynamics)),
            "dynamics_6m": dynamics,
        })

    revenue_abc = _abc_assignments(items, "revenue_6m")
    finrez_abc = _abc_assignments(items, "finrez_pre_6m")
    for item in items:
        sku = str(item["sku"])
        item["category_abc_revenue"] = revenue_abc[sku]
        item["category_abc_finrez"] = finrez_abc[sku]
        item["category_strong_sku"] = (
            revenue_abc[sku]["class"] == "A" and finrez_abc[sku]["class"] == "A"
        )

    items.sort(key=lambda item: (
        not bool(item.get("category_strong_sku")),
        -_number(item.get("revenue_6m")),
        str(item.get("sku") or "").casefold(),
    ))
    total_sku_count = len(items)
    bounded_limit = min(max(int(limit or DEFAULT_LIMIT), 1), MAX_CATEGORY_LIMIT)
    returned_items = items[:bounded_limit]
    covered_periods = sorted({clean_text(row.get("period")) for row in selected_rows})

    return {
        "status": "PASS",
        "operation_type": "get_category_abc",
        "release": CATEGORY_ABC_RELEASE,
        "read_only": True,
        "deterministic": True,
        "scope": "category",
        "category": category_name,
        "network_participates_in_category_strength": False,
        "current_period_network_metrics_used_for_category_strength": False,
        "supported_categories": list(SUPPORTED_CATEGORIES),
        "source": "normalized_business_data",
        "horizon": {
            "type": "rolling_months",
            "months": ROLLING_MONTHS,
            "start_period": periods[0],
            "end_period": periods[-1],
            "periods": periods,
            "covered_periods": covered_periods,
            "coverage_months": len(covered_periods),
        },
        "context_independence": {
            "active_workspace_used": False,
            "session_context_used": False,
            "manager_filter_used": False,
            "network_filter_used": False,
        },
        "category_isolation": {
            "exact_category_filter": True,
            "other_categories_included": False,
            "private_label_auto_merge": False,
        },
        "methodology": {
            "category_abc_revenue_metric": "sum(revenue) within exact category",
            "category_abc_finrez_metric": "sum(finrez_pre) within exact category",
            "abc_thresholds_percent": {"A": ABC_A_LIMIT_PERCENT, "B": ABC_B_LIMIT_PERCENT, "C": 100.0},
            "category_strong_sku_rule": "Category ABC Revenue = A AND Category ABC Finrez = A",
            "network_participates_in_category_strength": False,
        },
        "summary": {
            "total_revenue_6m": round_money(sum(_number(item.get("revenue_6m")) for item in items)),
            "total_finrez_pre_6m": round_money(sum(_number(item.get("finrez_pre_6m")) for item in items)),
            "total_sku_count": total_sku_count,
            "category_strong_sku_count": sum(1 for item in items if item.get("category_strong_sku") is True),
        },
        "bounded_result": {
            "limit": bounded_limit,
            "returned_count": len(returned_items),
            "total_sku_count": total_sku_count,
            "truncated": total_sku_count > len(returned_items),
        },
        "items": returned_items,
        "next_canonical_step": "Category Strong SKU -> presence/absence in selected Network -> Category Matrix Candidate",
    }


def _classification_hash(items: List[Dict[str, Any]], category: str, horizon: Dict[str, Any]) -> str:
    evidence = {
        "category": category,
        "horizon": horizon,
        "items": [
            {
                "sku": item.get("sku"),
                "category_abc_revenue": (item.get("category_abc_revenue") or {}).get("class"),
                "category_abc_finrez": (item.get("category_abc_finrez") or {}).get("class"),
                "category_strong_sku": item.get("category_strong_sku"),
            }
            for item in items
        ],
    }
    raw = json.dumps(evidence, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def build_category_network_sku_package(
    end_period: str,
    category: str,
    network: str,
    *,
    limit: int = DEFAULT_LIMIT,
    rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Apply Network presence only after canonical Category ABC."""
    network_name = clean_text(network)
    if not network_name:
        return {
            "status": "error",
            "operation_type": "get_category_network_sku_package",
            "failure_reason": "network_required",
            "read_only": True,
            "release": CATEGORY_ABC_RELEASE,
        }

    source_rows = list(rows) if rows is not None else list(get_normalized_rows())
    category_abc = build_category_abc(end_period, category, limit=MAX_LIMIT, rows=source_rows)
    if category_abc.get("status") != "PASS":
        return {
            "status": "error",
            "operation_type": "get_category_network_sku_package",
            "failure_reason": "canonical_category_abc_unavailable",
            "category_abc_failure_reason": category_abc.get("failure_reason"),
            "read_only": True,
            "release": CATEGORY_ABC_RELEASE,
        }

    strong_items = [item for item in category_abc.get("items") or [] if item.get("category_strong_sku") is True]
    expected_count = int((category_abc.get("summary") or {}).get("category_strong_sku_count") or 0)
    if len(strong_items) != expected_count:
        return {
            "status": "error",
            "operation_type": "get_category_network_sku_package",
            "failure_reason": "category_strong_sku_evidence_exceeds_canonical_bound",
            "expected_category_strong_sku_count": expected_count,
            "available_category_strong_sku_count": len(strong_items),
            "read_only": True,
            "release": CATEGORY_ABC_RELEASE,
        }

    category_name = str(category_abc.get("category") or "")
    current_rows = [
        row for row in source_rows
        if clean_text(row.get("period")) == clean_text(end_period)
        and _normalized(row.get("category")) == category_name.casefold()
        and _normalized(row.get("network")) == _normalized(network_name)
        and clean_text(row.get("sku"))
    ]
    rows_by_sku: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in current_rows:
        rows_by_sku[_normalized(row.get("sku"))].append(row)

    evaluated: List[Dict[str, Any]] = []
    for item in strong_items:
        sku_rows = rows_by_sku.get(_normalized(item.get("sku")), [])
        present = bool(sku_rows)
        evaluated.append({
            "sku": item.get("sku"),
            "category": category_name,
            "format": item.get("format"),
            "category_abc_revenue": (item.get("category_abc_revenue") or {}).get("class"),
            "category_abc_finrez": (item.get("category_abc_finrez") or {}).get("class"),
            "category_strong_sku": True,
            "presence_in_network": present,
            "category_matrix_candidate": not present,
            "category_6m_metrics": {
                "revenue": item.get("revenue_6m"),
                "finrez_pre": item.get("finrez_pre_6m"),
                "dynamics": item.get("dynamics_6m") or [],
            },
            "current_network_context": {
                "period": end_period,
                "revenue": round_money(sum(_number(row.get("revenue")) for row in sku_rows)),
                "finrez_pre": round_money(sum(_number(row.get("finrez_pre")) for row in sku_rows)),
            },
        })

    evaluated.sort(key=lambda item: (
        -_number((item.get("category_6m_metrics") or {}).get("revenue")),
        str(item.get("sku") or "").casefold(),
    ))
    candidates = [item for item in evaluated if item.get("category_matrix_candidate") is True]
    bounded_limit = min(max(int(limit or DEFAULT_LIMIT), 1), MAX_CATEGORY_LIMIT)
    returned_candidates = candidates[:bounded_limit]
    projection = [
        {
            "sku": item.get("sku"),
            "category_abc_revenue": item.get("category_abc_revenue"),
            "category_abc_finrez": item.get("category_abc_finrez"),
            "category_strong_sku": item.get("category_strong_sku"),
            "presence_in_network": item.get("presence_in_network"),
            "category_matrix_candidate": item.get("category_matrix_candidate"),
        }
        for item in evaluated
    ]

    return {
        "status": "PASS",
        "operation_type": "get_category_network_sku_package",
        "release": CATEGORY_ABC_RELEASE,
        "read_only": True,
        "deterministic": True,
        "scope": "category_network_application",
        "category": category_name,
        "network": network_name,
        "period": end_period,
        "canonical_chain": "Business ABC -> Category ABC rolling 6M -> presence/absence in selected Network -> Category Matrix Candidate",
        "category_strong_sku_rule": (category_abc.get("methodology") or {}).get("category_strong_sku_rule"),
        "category_candidate_rule": "Category Strong SKU AND absent in selected Network",
        "network_participates_in_category_strength": False,
        "category_classification_hash": _classification_hash(strong_items, category_name, category_abc.get("horizon") or {}),
        "horizon": category_abc.get("horizon"),
        "presence_basis": {
            "network": network_name,
            "category": category_name,
            "period": end_period,
            "rule": "SKU has at least one DATA row in selected Network and exact category for the current period",
        },
        "summary": {
            "category_total_sku_count": (category_abc.get("summary") or {}).get("total_sku_count"),
            "category_strong_sku_count": len(evaluated),
            "present_category_strong_sku_count": sum(1 for item in evaluated if item.get("presence_in_network") is True),
            "absent_category_strong_sku_count": len(candidates),
            "category_candidate_count": len(candidates),
        },
        "evaluated_category_strong_skus": projection,
        "candidates": returned_candidates,
        "bounded_result": {
            "limit": bounded_limit,
            "returned_candidate_count": len(returned_candidates),
            "total_candidate_count": len(candidates),
            "truncated": len(candidates) > len(returned_candidates),
        },
    }
