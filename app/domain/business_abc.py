"""Deterministic rolling Business ABC for canonical Strong SKU evidence.

The calculation intentionally has no Workspace or session dependency. It reads
the complete normalized Business Data set, selects a fixed rolling horizon and
aggregates SKU evidence across every network and manager.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, Iterable, List, Optional, Tuple

from app.domain.filters import get_normalized_rows
from app.domain.normalization import clean_text, round_money, round_percent
from app.domain.summary import _format_from_product_text


BUSINESS_ABC_RELEASE = "VECTRA-BUSINESS-ABC-STRONG-SKU-001"
ROLLING_MONTHS = 6
DEFAULT_LIMIT = 50
MAX_LIMIT = 100
ABC_A_LIMIT_PERCENT = 80.0
ABC_B_LIMIT_PERCENT = 95.0


def _number(value: Any) -> float:
    try:
        return float(value or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _parse_month(value: Any) -> Optional[Tuple[int, int]]:
    text = str(value or "").strip()
    if len(text) != 7 or text[4] != "-":
        return None
    try:
        year, month = int(text[:4]), int(text[5:])
    except ValueError:
        return None
    return (year, month) if 1 <= month <= 12 else None


def _month_text(year: int, month: int) -> str:
    return f"{year:04d}-{month:02d}"


def rolling_periods(end_period: str, months: int = ROLLING_MONTHS) -> List[str]:
    parsed = _parse_month(end_period)
    if parsed is None:
        return []
    year, month = parsed
    absolute = year * 12 + month - 1
    periods: List[str] = []
    for offset in range(months - 1, -1, -1):
        value = absolute - offset
        periods.append(_month_text(value // 12, value % 12 + 1))
    return periods


def _dominant_text(rows: Iterable[Dict[str, Any]], field: str) -> str:
    """Choose one stable label using revenue weight and lexical tie-breaking."""
    weights: Dict[str, float] = defaultdict(float)
    for row in rows:
        label = clean_text(row.get(field))
        if label:
            weights[label] += max(_number(row.get("revenue")), 0.0)
    if not weights:
        return ""
    return sorted(weights, key=lambda label: (-weights[label], label.casefold()))[0]


def _abc_assignments(items: List[Dict[str, Any]], metric: str) -> Dict[str, Dict[str, Any]]:
    """Assign ABC by cumulative share of positive metric contribution.

    Non-positive contribution is always C. Classification uses cumulative share
    before the current item so the item crossing an 80/95 boundary remains in
    the class whose contribution it completes.
    """
    ordered = sorted(
        items,
        key=lambda item: (-_number(item.get(metric)), str(item.get("sku") or "").casefold()),
    )
    positive_total = sum(max(_number(item.get(metric)), 0.0) for item in ordered)
    cumulative = 0.0
    assignments: Dict[str, Dict[str, Any]] = {}
    for rank, item in enumerate(ordered, start=1):
        sku = str(item.get("sku") or "")
        value = _number(item.get(metric))
        share = (max(value, 0.0) / positive_total * 100.0) if positive_total > 0 else 0.0
        before = cumulative
        if value <= 0 or positive_total <= 0:
            abc_class = "C"
        elif before < ABC_A_LIMIT_PERCENT:
            abc_class = "A"
        elif before < ABC_B_LIMIT_PERCENT:
            abc_class = "B"
        else:
            abc_class = "C"
        cumulative += share
        assignments[sku] = {
            "class": abc_class,
            "rank": rank,
            "share_percent": round_percent(share),
            "cumulative_share_percent": round_percent(min(cumulative, 100.0)),
            "positive_basis_total": round_money(positive_total),
        }
    return assignments


def build_business_abc(
    end_period: str,
    *,
    limit: int = DEFAULT_LIMIT,
    rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Return compact rolling-six-month Business ABC and Strong SKU evidence."""
    periods = rolling_periods(end_period)
    if not periods:
        return {
            "status": "error",
            "operation_type": "get_business_abc",
            "failure_reason": "valid_end_period_required",
            "expected_format": "YYYY-MM",
            "read_only": True,
            "release": BUSINESS_ABC_RELEASE,
        }

    bounded_limit = min(max(int(limit or DEFAULT_LIMIT), 1), MAX_LIMIT)
    source_rows = list(rows) if rows is not None else list(get_normalized_rows())
    period_set = set(periods)
    selected_rows = [
        row for row in source_rows
        if str(row.get("period") or "").strip() in period_set
        and clean_text(row.get("sku"))
        and clean_text(row.get("sku")).casefold() not in {"без sku", "total", "итого"}
    ]
    if not selected_rows:
        return {
            "status": "error",
            "operation_type": "get_business_abc",
            "failure_reason": "business_data_not_found_for_period_range",
            "read_only": True,
            "release": BUSINESS_ABC_RELEASE,
            "horizon": {"months": ROLLING_MONTHS, "start_period": periods[0], "end_period": periods[-1], "periods": periods},
        }

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in selected_rows:
        grouped[clean_text(row.get("sku"))].append(row)

    items: List[Dict[str, Any]] = []
    for sku in sorted(grouped, key=str.casefold):
        sku_rows = grouped[sku]
        category = _dominant_text(sku_rows, "category") or "категория не определена"
        tmc_group = _dominant_text(sku_rows, "tmc_group")
        format_name = _format_from_product_text(tmc_group or sku)
        monthly_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
        for row in sku_rows:
            monthly_rows[str(row.get("period") or "").strip()].append(row)
        dynamics = []
        for period in periods:
            period_rows = monthly_rows.get(period, [])
            dynamics.append({
                "period": period,
                "revenue": round_money(sum(_number(row.get("revenue")) for row in period_rows)),
                "finrez_pre": round_money(sum(_number(row.get("finrez_pre")) for row in period_rows)),
            })
        revenue = round_money(sum(point["revenue"] for point in dynamics))
        finrez_pre = round_money(sum(point["finrez_pre"] for point in dynamics))
        items.append({
            "sku": sku,
            "category": category,
            "format": format_name,
            "revenue": revenue,
            "finrez_pre": finrez_pre,
            "dynamics": dynamics,
            "dynamic_change": {
                "revenue_money": round_money(dynamics[-1]["revenue"] - dynamics[0]["revenue"]),
                "finrez_pre_money": round_money(dynamics[-1]["finrez_pre"] - dynamics[0]["finrez_pre"]),
            },
        })

    revenue_abc = _abc_assignments(items, "revenue")
    finrez_abc = _abc_assignments(items, "finrez_pre")
    for item in items:
        sku = str(item["sku"])
        item["abc_revenue"] = revenue_abc[sku]
        item["abc_finrez"] = finrez_abc[sku]
        item["strong_sku"] = (
            revenue_abc[sku]["class"] == "A" and finrez_abc[sku]["class"] == "A"
        )

    items.sort(
        key=lambda item: (
            not bool(item.get("strong_sku")),
            -_number(item.get("revenue")),
            str(item.get("sku") or "").casefold(),
        )
    )
    total_sku_count = len(items)
    returned_items = items[:bounded_limit]
    covered_periods = sorted({str(row.get("period") or "") for row in selected_rows})

    return {
        "status": "PASS",
        "operation_type": "get_business_abc",
        "release": BUSINESS_ABC_RELEASE,
        "read_only": True,
        "deterministic": True,
        "scope": "business",
        "source": "normalized_business_data",
        "source_fields": ["period", "sku", "category", "tmc_group", "revenue", "finrez_pre"],
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
        "methodology": {
            "abc_revenue_metric": "sum(revenue)",
            "abc_finrez_metric": "sum(finrez_pre)",
            "abc_thresholds_percent": {"A": ABC_A_LIMIT_PERCENT, "B": ABC_B_LIMIT_PERCENT, "C": 100.0},
            "non_positive_metric_class": "C",
            "strong_sku_rule": "ABC Revenue = A AND ABC Finrez = A",
            "network_participates_in_strong_sku": False,
        },
        "summary": {
            "total_revenue": round_money(sum(_number(item.get("revenue")) for item in items)),
            "total_finrez_pre": round_money(sum(_number(item.get("finrez_pre")) for item in items)),
            "total_sku_count": total_sku_count,
            "strong_sku_count": sum(1 for item in items if item.get("strong_sku") is True),
        },
        "bounded_result": {
            "limit": bounded_limit,
            "returned_count": len(returned_items),
            "total_sku_count": total_sku_count,
            "truncated": total_sku_count > len(returned_items),
        },
        "items": returned_items,
        "next_canonical_step": "Strong SKU -> presence/absence in Network -> candidate for assortment matrix",
    }

