from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from app.domain import filters as filters_domain
from app.domain.metrics import aggregate_metrics
from app.domain.normalization import round_money, round_percent
from app.config import DRAIN_MIN_ITEMS

LEVEL_CHILD = {
    "business": "manager_top",
    "manager_top": "manager",
    "manager": "network",
    "network": "sku",
    "sku": None,
}

STRUCTURE_KEYS = [
    ("markup", "gross_profit", False),
    ("retro", "retro_bonus", True),
    ("logistics", "logistics_cost", True),
    ("personnel", "personnel_cost", True),
    ("other", "other_costs", True),
]

HUMAN_LEVEL_NAMES = {
    "business": "Бизнес",
}


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _safe_get_rows() -> List[Dict[str, Any]]:
    try:
        return filters_domain.get_normalized_rows()
    except Exception:
        return []


def _run_filter(rows: List[Dict[str, Any]], period: str, **kwargs: Any):
    try:
        result = filters_domain.filter_rows(rows, period=period, **kwargs)
    except TypeError:
        result = filters_domain.filter_rows(period=period, **kwargs)
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
    if len(period) == 4 and period.isdigit():
        return f"{int(period) - 1:04d}"
    return None


def _percent_change(current: float, previous: float) -> float:
    if abs(previous) < 1e-9:
        return 0.0
    return round_percent(((current - previous) / abs(previous)) * 100.0)


def _pp_change(current: float, previous: float) -> float:
    return round_percent(current - previous)


def _safe_markup_money(metrics: Dict[str, Any]) -> float:
    gross_profit = _to_float(metrics.get("gross_profit"))
    revenue = _to_float(metrics.get("revenue"))
    cost = _to_float(metrics.get("cost"))
    markup_percent = _to_float(metrics.get("markup"))
    finrez_pre = _to_float(metrics.get("finrez_pre"))
    retro = abs(_to_float(metrics.get("retro_bonus")))
    logistics = abs(_to_float(metrics.get("logistics_cost")))
    personnel = abs(_to_float(metrics.get("personnel_cost")))
    other = abs(_to_float(metrics.get("other_costs")))

    if gross_profit > 0:
        return round_money(gross_profit)

    pnl_derived = finrez_pre + retro + logistics + personnel + other
    if pnl_derived > 0:
        return round_money(pnl_derived)

    derived = revenue - cost
    if derived > 0 and cost > 0:
        return round_money(derived)

    if revenue > 0 and markup_percent > 0:
        return round_money(revenue * (markup_percent / 100.0))

    return 0.0


def _safe_markup_percent(metrics: Dict[str, Any]) -> float:
    markup_percent = _to_float(metrics.get("markup"))
    if markup_percent > 0:
        return round_percent(markup_percent)
    revenue = _to_float(metrics.get("revenue"))
    markup_money = _safe_markup_money(metrics)
    return round_percent((markup_money / revenue) * 100.0) if abs(revenue) > 1e-9 else 0.0


def _metric_money(value: float, prev: float) -> Dict[str, float]:
    delta_money = round_money(value - prev)
    return {
        "fact_money": round_money(value),
        "prev_year_money": round_money(prev),
        "delta_money": delta_money,
        "delta_percent": _percent_change(value, prev),
    }


def _metric_percent(value_percent: float, prev_percent: float) -> Dict[str, float]:
    return {
        "fact_percent": round_percent(value_percent),
        "prev_year_percent": round_percent(prev_percent),
        "delta_percent": _pp_change(value_percent, prev_percent),
    }


def _build_metrics(current_metrics: Dict[str, float], prev_metrics: Dict[str, float], level: str) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "revenue": _metric_money(_to_float(current_metrics.get("revenue")), _to_float(prev_metrics.get("revenue"))),
        "markup_percent": _metric_percent(
            _safe_markup_percent(current_metrics),
            _safe_markup_percent(prev_metrics),
        ),
        "finrez_pre": _metric_money(_to_float(current_metrics.get("finrez_pre")), _to_float(prev_metrics.get("finrez_pre"))),
        "margin_percent": _metric_percent(
            _to_float(current_metrics.get("margin_pre")),
            _to_float(prev_metrics.get("margin_pre")),
        ),
    }
    if level == "business":
        metrics["finrez_final"] = _metric_money(_to_float(current_metrics.get("finrez_final")), _to_float(prev_metrics.get("finrez_final")))
    return metrics


def _signed_money(value: float, is_expense: bool) -> float:
    return -abs(value) if is_expense else value


def _signed_percent(signed_money: float, revenue: float) -> float:
    return round_percent((signed_money / revenue) * 100.0) if abs(revenue) > 1e-9 else 0.0


def _effect_money_from_base(current_percent: float, base_percent: float, current_revenue: float) -> float:
    return round_money(((base_percent - current_percent) / 100.0) * current_revenue) if abs(current_revenue) > 1e-9 else 0.0


def _base_money_from_percent(base_percent: float, current_revenue: float) -> float:
    return round_money((base_percent / 100.0) * current_revenue) if abs(current_revenue) > 1e-9 else 0.0


def _structure_item(
    name: str,
    current_money: float,
    base_money: float,
    current_revenue: float,
    base_revenue: float,
    is_expense: bool,
) -> Dict[str, float]:
    current_signed_money = _signed_money(current_money, is_expense)
    base_signed_money = _signed_money(base_money, is_expense)
    current_percent = _signed_percent(current_signed_money, current_revenue)
    base_percent = _signed_percent(base_signed_money, base_revenue)
    scaled_base_money = _base_money_from_percent(base_percent, current_revenue)
    effect_money = _effect_money_from_base(current_percent, base_percent, current_revenue)
    return {
        "value_money": round_money(current_signed_money),
        "base_money": scaled_base_money,
        "value_percent": current_percent,
        "base_percent": base_percent,
        "delta_percent": _pp_change(current_percent, base_percent),
        "effect_money": effect_money,
    }


def _build_structure(current_metrics: Dict[str, float], base_metrics: Dict[str, float]) -> Dict[str, Any]:
    current_revenue = _to_float(current_metrics.get("revenue"))
    base_revenue = _to_float(base_metrics.get("revenue"))
    structure: Dict[str, Any] = {}
    for public_key, metric_key, is_expense in STRUCTURE_KEYS:
        current_money = _safe_markup_money(current_metrics) if public_key == "markup" else _to_float(current_metrics.get(metric_key))
        base_money = _safe_markup_money(base_metrics) if public_key == "markup" else _to_float(base_metrics.get(metric_key))
        structure[public_key] = _structure_item(public_key, current_money, base_money, current_revenue, base_revenue, is_expense)
    return structure


def _group(rows: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = row.get(field) or ""
        if not key or key in {"Пусто", "Без менеджера"}:
            continue
        grouped[key].append(row)
    return grouped


def _drain_item(name: str, rows: List[Dict[str, Any]], business_metrics: Dict[str, Any], prev_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    current = aggregate_metrics(rows)
    structure = _build_structure(current, business_metrics)
    total_effect = round_money(sum(_to_float(value.get("effect_money")) for value in structure.values()))
    return {
        "object_name": name,
        "effect_money": total_effect,
    }


def _build_drain(level: str, object_rows: List[Dict[str, Any]], business_metrics: Dict[str, Any], period: str) -> List[Dict[str, Any]]:
    child = LEVEL_CHILD.get(level)
    if not child:
        return []
    grouped = _group(object_rows, child)
    prev_period = _previous_year_period(period)
    prev_grouped: Dict[str, List[Dict[str, Any]]] = {}
    if prev_period and object_rows:
        sample = object_rows[0]
        scope = {}
        for dim in ["manager_top", "manager", "network", "category", "tmc_group", "sku"]:
            if dim == child:
                continue
            if sample.get(dim):
                scope[dim] = sample.get(dim)
        prev_rows, _ = _run_filter(_safe_get_rows(), period=prev_period, **scope)
        prev_grouped = _group(prev_rows, child)

    items: List[Dict[str, Any]] = []
    for name, rows in grouped.items():
        item = _drain_item(name, rows, business_metrics, prev_grouped.get(name, []))
        if _to_float(item.get("effect_money")) < 0:
            items.append(item)
    items.sort(key=lambda item: _to_float(item.get("effect_money")))
    return items[:3]


def _build_goal(level: str, current_metrics: Dict[str, float], prev_metrics: Dict[str, float], business_metrics: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    if level == "business":
        delta = round_money(_to_float(current_metrics.get("finrez_pre")) - _to_float(prev_metrics.get("finrez_pre")))
        return {"type": "keep" if delta > 0 else "close", "value_money": delta, "label": "удержать" if delta > 0 else "закрыть"}
    effect = round_money(((_to_float(business_metrics.get("margin_pre")) - _to_float(current_metrics.get("margin_pre"))) / 100.0) * _to_float(current_metrics.get("revenue"))) if business_metrics else 0.0
    return {"type": "close", "value_money": effect, "label": "закрыть"}


def _build_navigation(level: str, drain_block: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "current_level": level,
        "next_level": LEVEL_CHILD.get(level),
        "items": [item.get("object_name") for item in drain_block[:3]],
        "all": bool(drain_block),
        "back": True,
    }


def _human_object_name(level: str, object_name: str) -> str:
    return HUMAN_LEVEL_NAMES.get(level, object_name)




def _summary_from_rows(level: str, object_name: str, period: str, rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]], business_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    current_metrics = aggregate_metrics(rows)
    prev_metrics = aggregate_metrics(prev_rows)
    business_metrics = aggregate_metrics(business_rows)
    base_metrics = prev_metrics if level == "business" else business_metrics
    drain_block = _build_drain(level, rows, business_metrics if business_rows else current_metrics, period)
    structure = _build_structure(current_metrics, base_metrics)

    payload = {
        "context": {
            "level": level,
            "object_name": _human_object_name(level, object_name),
            "period": period,
            "compare_base": "previous_year" if level == "business" else "business",
            "previous_year_period": _previous_year_period(period),
        },
        "metrics": _build_metrics(current_metrics, prev_metrics, level),
        "goal": _build_goal(level, current_metrics, prev_metrics, business_metrics),
        "navigation": _build_navigation(level, drain_block),
        # compatibility for orchestration/session internals
        "level": level,
        "object_name": object_name,
        "period": period,
        "children_level": LEVEL_CHILD.get(level),
        "metrics_raw": current_metrics,
        "previous_object_metrics": prev_metrics,
        "business_metrics_raw": business_metrics,
        "filter": {"period": period},
    }

    if level == "sku":
        payload["drain_block"] = []
        return payload

    payload["structure"] = structure
    payload["drain_block"] = drain_block
    return payload


def get_business_summary(period: str) -> Dict[str, Any]:
    all_rows = _safe_get_rows()
    rows, _ = _run_filter(all_rows, period=period)
    prev_rows = []
    prev_period = _previous_year_period(period)
    if prev_period:
        prev_rows, _ = _run_filter(all_rows, period=prev_period)
    return _summary_from_rows("business", "business", period, rows, prev_rows, rows)


def _object_summary(level: str, field: str, object_name: str, period: str) -> Dict[str, Any]:
    all_rows = _safe_get_rows()
    rows, _ = _run_filter(all_rows, period=period, **{field: object_name})
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
