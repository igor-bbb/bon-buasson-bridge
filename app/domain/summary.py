from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from app.domain import filters as filters_domain
from app.domain.metrics import aggregate_metrics
from app.domain.normalization import round_money, round_percent

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

DRAIN_FIELDS = ["markup", "retro", "logistics", "personnel", "other"]

HUMAN_LEVEL_NAMES = {
    "business": "Бизнес",
}

FOCUS_FIELDS = ["tmc_group", "category", "sku"]


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
        "value_money": round_money(value),
        "delta_money": delta_money,
        "delta_percent": _percent_change(value, prev),
    }


def _metric_percent(value_percent: float, prev_percent: float, value_money: float, prev_money: float) -> Dict[str, float]:
    return {
        "value_percent": round_percent(value_percent),
        "delta_percent": _pp_change(value_percent, prev_percent),
        "value_money": round_money(value_money),
        "delta_money": round_money(value_money - prev_money),
    }


def _build_metrics(current_metrics: Dict[str, float], prev_metrics: Dict[str, float], level: str) -> Dict[str, Any]:
    current_markup_money = _safe_markup_money(current_metrics)
    prev_markup_money = _safe_markup_money(prev_metrics)

    metrics: Dict[str, Any] = {
        "revenue": _metric_money(_to_float(current_metrics.get("revenue")), _to_float(prev_metrics.get("revenue"))),
        "markup": _metric_percent(
            _safe_markup_percent(current_metrics),
            _safe_markup_percent(prev_metrics),
            current_markup_money,
            prev_markup_money,
        ),
        "finrez_pre": _metric_money(_to_float(current_metrics.get("finrez_pre")), _to_float(prev_metrics.get("finrez_pre"))),
        "margin_pre": _metric_percent(
            _to_float(current_metrics.get("margin_pre")),
            _to_float(prev_metrics.get("margin_pre")),
            _to_float(current_metrics.get("finrez_pre")),
            _to_float(prev_metrics.get("finrez_pre")),
        ),
    }
    if level == "business":
        metrics["gap"] = {
            "value_percent": round_percent(_to_float(current_metrics.get("gap", current_metrics.get("kpi_gap")))),
            "delta_percent": _pp_change(
                _to_float(current_metrics.get("gap", current_metrics.get("kpi_gap"))),
                _to_float(prev_metrics.get("gap", prev_metrics.get("kpi_gap"))),
            ),
        }
        metrics["finrez_final"] = _metric_money(_to_float(current_metrics.get("finrez_final")), _to_float(prev_metrics.get("finrez_final")))
    return metrics


def _impact_label(effect_money: float) -> str:
    if effect_money > 0:
        return "улучшили"
    if effect_money < 0:
        return "ухудшили"
    return "без изменений"


def _signed_money(value: float, is_expense: bool) -> float:
    return -abs(value) if is_expense else abs(value)


def _signed_percent(signed_money: float, revenue: float) -> float:
    return round_percent((signed_money / revenue) * 100.0) if abs(revenue) > 1e-9 else 0.0


def _effect_money(current_money: float, base_money: float, current_revenue: float, base_revenue: float, is_expense: bool) -> Tuple[float, float, float, float, float]:
    current_signed_money = _signed_money(current_money, is_expense)
    base_signed_money = _signed_money(base_money, is_expense)
    current_percent = _signed_percent(current_signed_money, current_revenue)
    base_percent = _signed_percent(base_signed_money, base_revenue)
    if abs(current_revenue) < 1e-9:
        effect = 0.0
    elif is_expense:
        current_abs_percent = abs(current_percent)
        base_abs_percent = abs(base_percent)
        effect = round_money(((base_abs_percent - current_abs_percent) / 100.0) * current_revenue)
    else:
        effect = round_money(((current_percent - base_percent) / 100.0) * current_revenue)
    return current_signed_money, base_signed_money, current_percent, base_percent, effect


def _structure_item(
    current_money: float,
    base_money: float,
    current_revenue: float,
    base_revenue: float,
    is_expense: bool,
) -> Dict[str, float]:
    current_signed_money, base_signed_money, current_percent, base_percent, effect = _effect_money(
        current_money, base_money, current_revenue, base_revenue, is_expense
    )
    return {
        "value_money": round_money(current_signed_money),
        "value_percent": current_percent,
        "business_percent": base_percent,
        "effect_money": effect,
        "impact": _impact_label(effect),
    }


def _build_structure(current_metrics: Dict[str, float], business_metrics: Dict[str, float]) -> Dict[str, Any]:
    current_revenue = _to_float(current_metrics.get("revenue"))
    business_revenue = _to_float(business_metrics.get("revenue"))
    structure: Dict[str, Any] = {}
    for public_key, metric_key, is_expense in STRUCTURE_KEYS:
        current_money = _safe_markup_money(current_metrics) if public_key == "markup" else _to_float(current_metrics.get(metric_key))
        business_money = _safe_markup_money(business_metrics) if public_key == "markup" else _to_float(business_metrics.get(metric_key))
        structure[public_key] = _structure_item(current_money, business_money, current_revenue, business_revenue, is_expense)
    return structure


def _group(rows: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = row.get(field) or ""
        if not key or key in {"Пусто", "Без менеджера"}:
            continue
        grouped[key].append(row)
    return grouped


def _positive_effect_total(structure: Dict[str, Any]) -> float:
    return round_money(sum(max(0.0, _to_float((structure.get(key) or {}).get("effect_money"))) for key in DRAIN_FIELDS))


def _effect_breakdown(structure: Dict[str, Any]) -> Dict[str, float]:
    return {key: round_money(_to_float((structure.get(key) or {}).get("effect_money"))) for key in DRAIN_FIELDS}


def _drain_item(name: str, rows: List[Dict[str, Any]], business_metrics: Dict[str, Any], prev_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    current = aggregate_metrics(rows)
    prev = aggregate_metrics(prev_rows or []) if prev_rows else {}
    revenue = _to_float(current.get("revenue"))
    margin = _to_float(current.get("margin_pre"))
    finrez = _to_float(current.get("finrez_pre"))
    business_margin = _to_float(business_metrics.get("margin_pre"))
    structure = _build_structure(current, business_metrics)
    effect_breakdown = _effect_breakdown(structure)
    potential = _positive_effect_total(structure)
    item: Dict[str, Any] = {
        "object_name": name,
        "fact": {
            "value_money": round_money(finrez),
            "value_percent": round_percent(margin),
            "revenue_money": round_money(revenue),
        },
        "delta_prev_year": {
            "value_money": round_money(finrez - _to_float(prev.get("finrez_pre"))),
            "value_percent": _percent_change(finrez, _to_float(prev.get("finrez_pre"))),
        },
        "gap_to_business": {
            "value_percent": round_percent(margin - business_margin),
        },
        "effect_money": potential,
        "potential_money": potential,
        "effect_breakdown": effect_breakdown,
    }
    return item


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
        if _to_float(item.get("potential_money")) > 0:
            items.append(item)
    items.sort(key=lambda item: (_to_float(item.get("effect_money")), _to_float((item.get("fact") or {}).get("revenue_money"))), reverse=True)
    return items[: max(3, len(items))]


def _build_goal(level: str, current_metrics: Dict[str, float], prev_metrics: Dict[str, float], structure: Dict[str, Any]) -> Dict[str, Any]:
    if level == "business":
        delta = round_money(_to_float(current_metrics.get("finrez_pre")) - _to_float(prev_metrics.get("finrez_pre")))
        return {"type": "keep_growth" if delta >= 0 else "close_gap", "value_money": delta}
    effect = _positive_effect_total(structure)
    return {"type": "close_gap" if effect > 0 else "keep_growth", "value_money": effect}


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


def _build_focus_block(network_rows: List[Dict[str, Any]], business_metrics: Dict[str, Any]) -> Dict[str, Any]:
    best: Optional[Dict[str, Any]] = None
    for field in FOCUS_FIELDS:
        grouped = _group(network_rows, field)
        if not grouped:
            continue
        for name, rows in grouped.items():
            structure = _build_structure(aggregate_metrics(rows), business_metrics)
            effect = _positive_effect_total(structure)
            candidate = {
                "field": field,
                "object_name": name,
                "effect_money": effect,
            }
            if best is None or effect > _to_float(best.get("effect_money")):
                best = candidate
        if best and _to_float(best.get("effect_money")) > 0:
            break
    return best or {"field": None, "object_name": None, "effect_money": 0.0}


def _build_decision_block(structure: Dict[str, Any], goal: Dict[str, Any]) -> Dict[str, Any]:
    actions: List[Dict[str, Any]] = []
    for key in ["logistics", "retro", "personnel", "markup", "other"]:
        effect_money = round_money(max(0.0, _to_float((structure.get(key) or {}).get("effect_money"))))
        if effect_money <= 0:
            continue
        actions.append({
            "type": key,
            "effect_money": effect_money,
        })
    actions.sort(key=lambda item: _to_float(item.get("effect_money")), reverse=True)
    actions = actions[:3]
    total_effect = round_money(sum(_to_float(item.get("effect_money")) for item in actions))
    return {
        "actions": actions,
        "effect_money": total_effect,
        "goal_money": round_money(_to_float(goal.get("value_money"))),
    }


def _summary_from_rows(level: str, object_name: str, period: str, rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]], business_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    current_metrics = aggregate_metrics(rows)
    prev_metrics = aggregate_metrics(prev_rows)
    business_metrics = aggregate_metrics(business_rows)
    metrics_prev_base = prev_metrics
    drain_block = _build_drain(level, rows, business_metrics if business_rows else current_metrics, period)

    structure_base = business_metrics if level != "business" else business_metrics
    structure = _build_structure(current_metrics, structure_base)
    goal = _build_goal(level, current_metrics, prev_metrics, structure)

    payload: Dict[str, Any] = {
        "context": {
            "level": level,
            "object_name": _human_object_name(level, object_name),
            "period": period,
            "compare_base": "previous_year" if level == "business" else "business",
            "previous_year_period": _previous_year_period(period),
        },
        "metrics": _build_metrics(current_metrics, metrics_prev_base, level),
        "structure": structure,
        "drain_block": drain_block,
        "goal": goal,
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
    if level == "network":
        payload["focus_block"] = _build_focus_block(rows, business_metrics)
        payload["decision_block"] = _build_decision_block(structure, goal)
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
