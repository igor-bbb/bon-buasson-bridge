from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from app.domain.filters import filter_rows, get_normalized_rows
from app.domain.metrics import aggregate_metrics
from app.domain.normalization import round_money, round_percent

LEVEL_CHILD = {
    "business": "manager_top",
    "manager_top": "manager",
    "manager": "network",
    "network": "sku",
    "sku": None,
}

STRUCTURE_FIELDS = [
    "markup",
    "retro_bonus",
    "logistics_cost",
    "personnel_cost",
    "other_costs",
]

STRUCTURE_ALIASES = {
    "markup": "markup",
    "retro_bonus": "retro",
    "logistics_cost": "logistics",
    "personnel_cost": "personnel",
    "other_costs": "other",
}


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _safe_get_rows() -> List[Dict[str, Any]]:
    try:
        return get_normalized_rows()
    except Exception:
        return []


def _run_filter(rows: List[Dict[str, Any]], period: str, **kwargs: Any):
    try:
        result = filter_rows(rows, period=period, **kwargs)
    except TypeError:
        result = filter_rows(period=period, **kwargs)
    if isinstance(result, tuple) and len(result) == 2:
        return result
    return result, {}


def _previous_year_period(period: str) -> Optional[str]:
    if not period or not isinstance(period, str):
        return None
    if ":" in period:
        parts = period.split(":")
        if len(parts) == 2:
            start, end = parts
            prev_start = _previous_year_period(start)
            prev_end = _previous_year_period(end)
            return f"{prev_start}:{prev_end}" if prev_start and prev_end else None
        return None
    if len(period) == 7 and period[4] == "-":
        try:
            return f"{int(period[:4]) - 1:04d}-{period[5:7]}"
        except Exception:
            return None
    return None


def _percent_change(current: float, previous: float) -> float:
    if abs(previous) < 1e-9:
        return 0.0
    return round_percent(((current - previous) / abs(previous)) * 100.0)


def _pp_change(current: float, previous: float) -> float:
    return round_percent(current - previous)


def _compute_markup_money(metrics: Dict[str, float]) -> float:
    gross_profit = _to_float(metrics.get("gross_profit"))
    if gross_profit > 0:
        return round_money(gross_profit)
    revenue = _to_float(metrics.get("revenue"))
    cost = _to_float(metrics.get("cost"))
    if revenue > 0 and cost > 0:
        return round_money(revenue - cost)
    markup_pct = _to_float(metrics.get("markup"))
    if revenue > 0 and markup_pct > -100:
        # markup% = gross_profit / cost * 100 ; revenue = cost + gross_profit
        gross_profit = revenue * (markup_pct / (100.0 + markup_pct)) if abs(100.0 + markup_pct) > 1e-9 else 0.0
        return round_money(gross_profit)
    return 0.0


def _metric_money(value: float, prev: float) -> Dict[str, float]:
    return {
        "value_money": round_money(value),
        "delta_money": round_money(value - prev),
        "delta_percent": _percent_change(value, prev),
    }


def _metric_money_percent(value_money: float, prev_money: float, value_percent: float, prev_percent: float) -> Dict[str, float]:
    return {
        "value_money": round_money(value_money),
        "value_percent": round_percent(value_percent),
        "delta_money": round_money(value_money - prev_money),
        "delta_percent": _pp_change(value_percent, prev_percent),
    }


def _build_metrics(current_metrics: Dict[str, float], prev_metrics: Dict[str, float], level: str) -> Dict[str, Any]:
    markup_money = _compute_markup_money(current_metrics)
    prev_markup_money = _compute_markup_money(prev_metrics)
    margin_pre = _to_float(current_metrics.get("margin_pre"))
    prev_margin_pre = _to_float(prev_metrics.get("margin_pre"))
    markup_pct = _to_float(current_metrics.get("markup"))
    prev_markup_pct = _to_float(prev_metrics.get("markup"))
    finrez_pre = _to_float(current_metrics.get("finrez_pre"))
    prev_finrez_pre = _to_float(prev_metrics.get("finrez_pre"))
    gap_pct = round_percent(markup_pct - margin_pre)
    prev_gap_pct = round_percent(prev_markup_pct - prev_margin_pre)
    revenue = _to_float(current_metrics.get("revenue"))
    prev_revenue = _to_float(prev_metrics.get("revenue"))
    gap_money = round_money((gap_pct / 100.0) * revenue) if revenue > 0 else 0.0
    prev_gap_money = round_money((prev_gap_pct / 100.0) * prev_revenue) if prev_revenue > 0 else 0.0

    metrics: Dict[str, Any] = {
        "revenue": _metric_money(revenue, prev_revenue),
        "markup": _metric_money_percent(markup_money, prev_markup_money, markup_pct, prev_markup_pct),
        "finrez_pre": _metric_money_percent(finrez_pre, prev_finrez_pre, margin_pre, prev_margin_pre),
        "margin_pre": _metric_money_percent(finrez_pre, prev_finrez_pre, margin_pre, prev_margin_pre),
        "gap": _metric_money_percent(gap_money, prev_gap_money, gap_pct, prev_gap_pct),
    }
    if level == "business":
        finrez_final = _to_float(current_metrics.get("finrez_final"))
        prev_finrez_final = _to_float(prev_metrics.get("finrez_final"))
        metrics["finrez_final"] = _metric_money(finrez_final, prev_finrez_final)
    return metrics


def _group(rows: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = row.get(field) or ""
        if not key or key in {"Пусто", "Без менеджера"}:
            continue
        grouped[key].append(row)
    return grouped


def _build_structure(current_metrics: Dict[str, float], base_metrics: Dict[str, float], compare_to_business: bool) -> Dict[str, Any]:
    structure: Dict[str, Any] = {}
    current_revenue = _to_float(current_metrics.get("revenue"))
    base_revenue = _to_float(base_metrics.get("revenue"))
    for field in STRUCTURE_FIELDS:
        alias = STRUCTURE_ALIASES[field]
        if field == "markup":
            current_money = _compute_markup_money(current_metrics)
            base_money = _compute_markup_money(base_metrics)
            current_percent = _to_float(current_metrics.get("markup"))
            base_percent = _to_float(base_metrics.get("markup"))
        else:
            current_money = _to_float(current_metrics.get(field))
            base_money = _to_float(base_metrics.get(field))
            current_percent = round_percent((current_money / current_revenue) * 100.0) if current_revenue > 0 else 0.0
            base_percent = round_percent((base_money / base_revenue) * 100.0) if base_revenue > 0 else 0.0
        # for lower levels structure compares to business; for business compares to previous year
        structure[alias] = {
            "value_money": round_money(current_money),
            "value_percent": round_percent(current_percent),
            "delta_money": round_money(current_money - base_money),
            "delta_percent": _pp_change(current_percent, base_percent),
            "base": "business" if compare_to_business else "previous_year",
        }
    return structure


def _build_drain(level: str, scoped_rows: List[Dict[str, Any]], business_margin: float, period: str) -> List[Dict[str, Any]]:
    child = LEVEL_CHILD.get(level)
    if not child:
        return []
    grouped = _group(scoped_rows, child)
    prev_period = _previous_year_period(period)
    prev_grouped: Dict[str, List[Dict[str, Any]]] = {}
    if prev_period:
        prev_rows, _ = _run_filter(_safe_get_rows(), period=prev_period)
        scope = {}
        if scoped_rows:
            sample = scoped_rows[0]
            for dim in ["business", "manager_top", "manager", "network"]:
                if level == dim:
                    break
                val = sample.get(dim)
                if val:
                    scope[dim] = val
        prev_rows, _ = _run_filter(prev_rows, period=prev_period, **scope)
        prev_grouped = _group(prev_rows, child)

    items: List[Dict[str, Any]] = []
    for name, rows in grouped.items():
        current = aggregate_metrics(rows)
        revenue = _to_float(current.get("revenue"))
        if revenue <= 0:
            continue
        margin = _to_float(current.get("margin_pre"))
        finrez = _to_float(current.get("finrez_pre"))
        gap_pp = round_percent(business_margin - margin)
        effect_money = round_money(max(0.0, (gap_pp / 100.0) * revenue))
        if effect_money <= 0:
            continue
        prev = aggregate_metrics(prev_grouped.get(name, [])) if prev_grouped.get(name) else {}
        prev_finrez = _to_float(prev.get("finrez_pre"))
        items.append({
            "object_name": name,
            "fact": {
                "value_money": round_money(finrez),
                "value_percent": round_percent(margin),
            },
            "delta_prev_year": {
                "value_money": round_money(finrez - prev_finrez),
                "value_percent": _percent_change(finrez, prev_finrez),
            },
            "gap_to_business": {
                "value_percent": gap_pp,
            },
            "effect_money": effect_money,
            "potential_money": effect_money,
        })
    items.sort(key=lambda x: x.get("potential_money", 0.0), reverse=True)
    return items[:7]


def _build_goal(level: str, current_metrics: Dict[str, float], prev_metrics: Dict[str, float], drain_block: List[Dict[str, Any]]) -> Dict[str, Any]:
    if level == "business":
        current_final = _to_float(current_metrics.get("finrez_final"))
        prev_final = _to_float(prev_metrics.get("finrez_final"))
        return {
            "delta_money": round_money(current_final - prev_final),
            "delta_percent": _percent_change(current_final, prev_final),
        }
    total_effect = round_money(sum(_to_float(item.get("effect_money")) for item in drain_block))
    revenue = _to_float(current_metrics.get("revenue"))
    return {
        "delta_money": total_effect,
        "delta_percent": round_percent((total_effect / revenue) * 100.0) if revenue > 0 else 0.0,
    }


def _build_navigation(level: str, drain_block: List[Dict[str, Any]]) -> Dict[str, Any]:
    next_map = {
        "business": "manager_top",
        "manager_top": "manager",
        "manager": "network",
        "network": "sku",
        "sku": None,
    }
    return {
        "current_level": level,
        "next_level": next_map.get(level),
        "items": [item.get("object_name") for item in drain_block[:3]],
    }


def _context(level: str, object_name: str, period: str) -> Dict[str, Any]:
    return {"level": level, "object_name": object_name, "period": period}


def _summary_from_rows(level: str, object_name: str, period: str, rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]], business_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    current_metrics = aggregate_metrics(rows)
    prev_metrics = aggregate_metrics(prev_rows) if prev_rows else {}
    business_metrics = aggregate_metrics(business_rows)
    drain_block = _build_drain(level, rows, _to_float(business_metrics.get("margin_pre")), period)
    structure_base_metrics = business_metrics if level != "business" else prev_metrics
    return {
        "context": _context(level, object_name, period),
        "metrics": _build_metrics(current_metrics, prev_metrics, level),
        "structure": _build_structure(current_metrics, structure_base_metrics, compare_to_business=(level != "business")),
        "drain_block": drain_block,
        "goal": _build_goal(level, current_metrics, prev_metrics, drain_block),
        "navigation": _build_navigation(level, drain_block),
    }


def get_business_summary(period: str) -> Dict[str, Any]:
    all_rows = _safe_get_rows()
    rows, meta = _run_filter(all_rows, period=period)
    if not rows:
        return {"error": "no data", "reason": (meta or {}).get("empty_reason")}
    prev_rows = []
    prev_period = _previous_year_period(period)
    if prev_period:
        prev_rows, _ = _run_filter(all_rows, period=prev_period)
    return _summary_from_rows("business", "business", period, rows, prev_rows, rows)


def _object_summary(level: str, field: str, object_name: str, period: str) -> Dict[str, Any]:
    all_rows = _safe_get_rows()
    rows, meta = _run_filter(all_rows, period=period, **{field: object_name})
    if not rows:
        return {"error": "no data", "reason": (meta or {}).get("empty_reason")}
    business_rows, _ = _run_filter(all_rows, period=period)
    prev_rows = []
    prev_period = _previous_year_period(period)
    if prev_period:
        prev_rows, _ = _run_filter(all_rows, period=prev_period, **{field: object_name})
    return _summary_from_rows(level, object_name, period, rows, prev_rows, business_rows)


def get_manager_top_summary(manager_top: str, period: str) -> Dict[str, Any]:
    return _object_summary("manager_top", "manager_top", manager_top, period)


def get_manager_summary(manager: str, period: str) -> Dict[str, Any]:
    return _object_summary("manager", "manager", manager, period)


def get_network_summary(network: str, period: str) -> Dict[str, Any]:
    return _object_summary("network", "network", network, period)


def get_sku_summary(sku: str, period: str) -> Dict[str, Any]:
    return _object_summary("sku", "sku", sku, period)
