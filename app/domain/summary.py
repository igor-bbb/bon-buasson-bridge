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

LEVEL_PARENT = {
    "manager_top": ("business", None),
    "manager": ("manager_top", "manager_top"),
    "network": ("manager", "manager"),
    "sku": ("network", "network"),
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
    "manager_top": "Топ-менеджер",
    "manager": "Менеджер",
    "network": "Сеть",
    "sku": "SKU",
}

STRUCTURE_ORDER = ["markup", "retro", "logistics", "personnel", "other"]


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


def _human_object_name(level: str, object_name: str) -> str:
    return "Бизнес" if level == "business" else object_name


def _safe_percent(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-9:
        return 0.0
    return round_percent((numerator / denominator) * 100.0)


def _percent_change(current: float, previous: float) -> float:
    if abs(previous) < 1e-9:
        return 0.0
    return round_percent(((current - previous) / abs(previous)) * 100.0)


def _safe_markup_money(metrics: Dict[str, Any]) -> float:
    gross_profit = _to_float(metrics.get("gross_profit"))
    revenue = _to_float(metrics.get("revenue"))
    cost = _to_float(metrics.get("cost"))
    markup_percent = _to_float(metrics.get("markup"))

    if gross_profit > 0:
        return round_money(gross_profit)
    if revenue > cost:
        return round_money(revenue - cost)
    if revenue > 0 and markup_percent > 0:
        return round_money(revenue * (markup_percent / 100.0))
    return 0.0


def _safe_markup_percent(metrics: Dict[str, Any]) -> float:
    markup_percent = _to_float(metrics.get("markup"))
    if markup_percent > 0:
        return round_percent(markup_percent)
    revenue = _to_float(metrics.get("revenue"))
    return _safe_percent(_safe_markup_money(metrics), revenue)


def _kpi_money_metric(current_value: float, prev_value: float) -> Dict[str, float]:
    return {
        "fact_money": round_money(current_value),
        "prev_year_money": round_money(prev_value),
        "delta_money": round_money(current_value - prev_value),
        "delta_percent": _percent_change(current_value, prev_value),
    }


def _kpi_percent_metric(current_value: float, prev_value: float) -> Dict[str, float]:
    delta_pp = current_value - prev_value
    return {
        "fact_percent": round_percent(current_value),
        "prev_year_percent": round_percent(prev_value),
        "delta_percent_points": round_percent(delta_pp),
        "delta_percent": _percent_change(current_value, prev_value),
    }


def _build_metrics(current_metrics: Dict[str, float], prev_year_metrics: Dict[str, float], level: str) -> Dict[str, Any]:
    metrics: Dict[str, Any] = {
        "revenue": _kpi_money_metric(_to_float(current_metrics.get("revenue")), _to_float(prev_year_metrics.get("revenue"))),
        "finrez_pre": _kpi_money_metric(_to_float(current_metrics.get("finrez_pre")), _to_float(prev_year_metrics.get("finrez_pre"))),
        "markup_percent": _kpi_percent_metric(_safe_markup_percent(current_metrics), _safe_markup_percent(prev_year_metrics)),
        "margin_percent": _kpi_percent_metric(_to_float(current_metrics.get("margin_pre")), _to_float(prev_year_metrics.get("margin_pre"))),
    }
    if level == "business":
        metrics["finrez_final"] = _kpi_money_metric(_to_float(current_metrics.get("finrez_final")), _to_float(prev_year_metrics.get("finrez_final")))
    return metrics


def _signed_money(value: float, is_expense: bool) -> float:
    return -abs(value) if is_expense else abs(value)


def _signed_percent(money: float, revenue: float) -> float:
    return _safe_percent(money, revenue)


def _effect_money_from_delta(delta_percent: float, current_revenue: float) -> float:
    return round_money((delta_percent / 100.0) * current_revenue) if abs(current_revenue) > 1e-9 else 0.0


def _structure_item(current_money: float, base_money: float, current_revenue: float, base_revenue: float, is_expense: bool) -> Dict[str, float]:
    current_signed_money = _signed_money(current_money, is_expense)
    base_signed_money = _signed_money(base_money, is_expense)
    current_percent = _signed_percent(current_signed_money, current_revenue)
    base_percent = _signed_percent(base_signed_money, base_revenue)
    delta_percent = round_percent(current_percent - base_percent)
    return {
        "value_money": round_money(current_signed_money),
        "value_percent": round_percent(current_percent),
        "base_money": round_money(base_signed_money),
        "base_percent": round_percent(base_percent),
        "delta_percent": delta_percent,
        "effect_money": _effect_money_from_delta(delta_percent, current_revenue),
    }


def _build_structure(current_metrics: Dict[str, float], base_metrics: Dict[str, float]) -> Dict[str, Any]:
    current_revenue = _to_float(current_metrics.get("revenue"))
    base_revenue = _to_float(base_metrics.get("revenue"))
    structure: Dict[str, Any] = {}
    for public_key, metric_key, is_expense in STRUCTURE_KEYS:
        current_money = _safe_markup_money(current_metrics) if public_key == "markup" else _to_float(current_metrics.get(metric_key))
        base_money = _safe_markup_money(base_metrics) if public_key == "markup" else _to_float(base_metrics.get(metric_key))
        structure[public_key] = _structure_item(current_money, base_money, current_revenue, base_revenue, is_expense)
    return {key: structure[key] for key in STRUCTURE_ORDER}


def _group(rows: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = row.get(field) or ""
        if not key or key in {"Пусто", "Без менеджера"}:
            continue
        grouped[key].append(row)
    return grouped


def _build_drain(level: str, object_rows: List[Dict[str, Any]], base_metrics: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
    child = LEVEL_CHILD.get(level)
    if not child:
        return [], []
    grouped = _group(object_rows, child)
    items: List[Dict[str, Any]] = []
    for name, rows in grouped.items():
        child_metrics = aggregate_metrics(rows)
        structure = _build_structure(child_metrics, base_metrics)
        total_effect = round_money(sum(_to_float(item.get("effect_money")) for item in structure.values()))
        items.append({
            "object_name": name,
            "effect_money": total_effect,
        })
    items.sort(key=lambda item: (_to_float(item.get("effect_money")), item.get("object_name") or ""))
    return items[:3], items


def _build_goal(level: str, current_metrics: Dict[str, float], prev_metrics: Dict[str, float], propagated_effect: Optional[float] = None) -> Dict[str, Any]:
    if level == "business":
        delta = round_money(_to_float(current_metrics.get("finrez_pre")) - _to_float(prev_metrics.get("finrez_pre")))
        goal_type = "keep" if delta > 0 else "close"
        return {"value_money": delta, "type": goal_type, "label": "удержать" if goal_type == "keep" else "закрыть"}

    effect = round_money(_to_float(propagated_effect)) if propagated_effect is not None else 0.0
    return {"value_money": effect, "type": "close", "label": "закрыть"}


def _build_navigation(level: str, drain_top: List[Dict[str, Any]], drain_all: List[Dict[str, Any]], previous_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        "current_level": level,
        "items": [item.get("object_name") for item in drain_top],
        "all": [item.get("object_name") for item in drain_all],
        "previous_state": previous_state or {},
    }


def _infer_parent_effect(level: str, object_name: str, period: str) -> Optional[float]:
    parent_info = LEVEL_PARENT.get(level)
    if not parent_info:
        return None

    parent_level, parent_field = parent_info
    if parent_level == "business":
        parent_summary = get_business_summary(period)
    else:
        all_rows = _safe_get_rows()
        rows, _ = _run_filter(all_rows, period=period, **{level: object_name})
        if not rows or not parent_field:
            return None
        parent_object_name = rows[0].get(parent_field)
        if not parent_object_name:
            return None
        if parent_level == "manager_top":
            parent_summary = get_manager_top_summary(parent_object_name, period)
        elif parent_level == "manager":
            parent_summary = get_manager_summary(parent_object_name, period)
        elif parent_level == "network":
            parent_summary = get_network_summary(parent_object_name, period)
        else:
            return None

    for item in parent_summary.get("drain_block", []):
        if item.get("object_name") == object_name:
            return _to_float(item.get("effect_money"))
    return None


def _build_focus_decision(structure: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    focus = {}
    decision = {}
    ordered = sorted(structure.items(), key=lambda kv: _to_float((kv[1] or {}).get("effect_money")))
    if ordered and _to_float((ordered[0][1] or {}).get("effect_money")) < 0:
        label_map = {"markup": "Наценка", "retro": "Ретро", "logistics": "Логистика", "personnel": "Персонал", "other": "Прочие"}
        focus = {"object_name": label_map.get(ordered[0][0], ordered[0][0]), "effect_money": round_money(_to_float(ordered[0][1].get("effect_money")))}
        actions: List[Dict[str, Any]] = []
        total = 0.0
        for key, payload in ordered:
            effect = _to_float((payload or {}).get("effect_money"))
            if effect < 0:
                actions.append({"action": label_map.get(key, key), "effect_money": round_money(effect)})
                total += effect
            if len(actions) >= 3:
                break
        decision = {"actions": actions, "effect_money": round_money(total)}
    return focus, decision


def _summary_from_rows(level: str, object_name: str, period: str, rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]], base_rows: List[Dict[str, Any]], previous_state: Optional[Dict[str, Any]] = None, propagated_effect: Optional[float] = None) -> Dict[str, Any]:
    current_metrics = aggregate_metrics(rows)
    prev_metrics = aggregate_metrics(prev_rows)
    base_metrics = aggregate_metrics(base_rows)

    compare_base = "previous_year" if level == "business" else "business"
    structure_base_metrics = prev_metrics if level == "business" else base_metrics
    structure = _build_structure(current_metrics, structure_base_metrics)
    drain_top, drain_all = _build_drain(level, rows, base_metrics if level != "business" else current_metrics)

    response: Dict[str, Any] = {
        "context": {
            "level": level,
            "object_name": _human_object_name(level, object_name),
            "period": period,
            "kpi_base": "previous_year",
            "structure_base": compare_base,
            "previous_year_period": _previous_year_period(period),
        },
        "metrics": _build_metrics(current_metrics, prev_metrics, level),
        "structure": structure,
        "drain_block": drain_top,
        "goal": _build_goal(level, current_metrics, prev_metrics, propagated_effect),
        "navigation": _build_navigation(level, drain_top, drain_all, previous_state=previous_state),
    }
    if level == "network":
        focus, decision = _build_focus_decision(structure)
        response["focus_block"] = focus
        response["decision_block"] = decision
    return response


def get_business_summary(period: str, previous_state: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    all_rows = _safe_get_rows()
    rows, _ = _run_filter(all_rows, period=period)
    prev_rows: List[Dict[str, Any]] = []
    prev_period = _previous_year_period(period)
    if prev_period:
        prev_rows, _ = _run_filter(all_rows, period=prev_period)
    return _summary_from_rows("business", "business", period, rows, prev_rows, rows, previous_state=previous_state)


def _object_summary(level: str, field: str, object_name: str, period: str, previous_state: Optional[Dict[str, Any]] = None, propagated_effect: Optional[float] = None) -> Dict[str, Any]:
    all_rows = _safe_get_rows()
    rows, _ = _run_filter(all_rows, period=period, **{field: object_name})
    base_rows, _ = _run_filter(all_rows, period=period)
    prev_rows: List[Dict[str, Any]] = []
    prev_period = _previous_year_period(period)
    if prev_period:
        prev_rows, _ = _run_filter(all_rows, period=prev_period, **{field: object_name})
    if propagated_effect is None and level != "business":
        propagated_effect = _infer_parent_effect(level, object_name, period)
    return _summary_from_rows(level, object_name, period, rows, prev_rows, base_rows, previous_state=previous_state, propagated_effect=propagated_effect)


def get_manager_top_summary(manager_top: str, period: str, previous_state: Optional[Dict[str, Any]] = None, propagated_effect: Optional[float] = None) -> Dict[str, Any]:
    return _object_summary("manager_top", "manager_top", manager_top, period, previous_state=previous_state, propagated_effect=propagated_effect)


def get_manager_summary(manager: str, period: str, previous_state: Optional[Dict[str, Any]] = None, propagated_effect: Optional[float] = None) -> Dict[str, Any]:
    return _object_summary("manager", "manager", manager, period, previous_state=previous_state, propagated_effect=propagated_effect)


def get_network_summary(network: str, period: str, previous_state: Optional[Dict[str, Any]] = None, propagated_effect: Optional[float] = None) -> Dict[str, Any]:
    return _object_summary("network", "network", network, period, previous_state=previous_state, propagated_effect=propagated_effect)


def get_sku_summary(sku: str, period: str, previous_state: Optional[Dict[str, Any]] = None, propagated_effect: Optional[float] = None) -> Dict[str, Any]:
    return _object_summary("sku", "sku", sku, period, previous_state=previous_state, propagated_effect=propagated_effect)
