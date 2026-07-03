from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from app.domain import filters as filters_domain
from app.domain.metrics import aggregate_metrics
from app.domain.normalization import clean_text, round_money, round_percent
from app.config import DRAIN_MIN_ITEMS

import math

CHILD_PREV_METRICS_CACHE: Dict[Any, Dict[str, Any]] = {}
LEVEL_CHILD = {
    "business": "manager_top",
    "manager_top": "manager",
    "manager": "network",
    "network": "category",
    "category": "sku",
    "tmc_group": "sku",
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


ARTICLE_LABELS_RU = {
    "markup": "наценка",
    "retro": "ретро",
    "logistics": "логистика",
    "personnel": "персонал",
    "other": "прочее",
}

ACTION_LABELS_RU = {
    "markup": "увеличить наценку",
    "retro": "снизить ретро",
    "logistics": "оптимизировать логистику",
    "personnel": "оптимизировать персонал",
    "other": "снизить прочие расходы",
}


def _to_float(value: Any) -> float:
    try:
        result = float(value or 0.0)
        return result if math.isfinite(result) else 0.0
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
    if '..' in period:
        parts = period.split('..')
        if len(parts) == 2:
            start, end = parts
            prev_start = _previous_year_period(start)
            prev_end = _previous_year_period(end)
            return f"{prev_start}..{prev_end}" if prev_start and prev_end else None
        return None
    if ':' in period:
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


MARKUP_PREVIOUS_REVENUE_MIN = 1000.0
MARKUP_PREVIOUS_MARKUP_PERCENT_MAX = 200.0


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
    """Effect sign must match delta_percent sign.

    delta_percent = value_percent - base_percent
    effect_money = delta_percent * revenue
    """
    return round_money(((current_percent - base_percent) / 100.0) * current_revenue) if abs(current_revenue) > 1e-9 else 0.0


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
    fact_money = round_money(current_signed_money)
    fact_percent = current_percent
    delta_percent = _pp_change(current_percent, base_percent)
    return {
        "value_money": fact_money,
        "fact_money": fact_money,
        "base_money": scaled_base_money,
        "value_percent": fact_percent,
        "fact_percent": fact_percent,
        "base_percent": base_percent,
        "delta_percent": delta_percent,
        "effect_money": effect_money,
    }


def _build_structure(current_metrics: Dict[str, float], base_metrics: Dict[str, float]) -> Dict[str, Any]:
    current_revenue = _to_float(current_metrics.get("revenue"))
    base_revenue = _to_float(base_metrics.get("revenue"))
    structure: Dict[str, Any] = {}
    for public_key, metric_key, is_expense in STRUCTURE_KEYS:
        if public_key == "markup":
            # Markup percent source of truth is aggregate_metrics()["markup"].
            # Do not derive Reasons markup percent from gross_profit / revenue:
            # that created a second legacy formula and broke Business -> Reasons.
            current_money = _safe_markup_money(current_metrics)
            current_percent = _safe_markup_percent(current_metrics)
            base_percent = _safe_markup_percent(base_metrics)
            scaled_base_money = _base_money_from_percent(base_percent, current_revenue)
            effect_money = _effect_money_from_base(current_percent, base_percent, current_revenue)
            structure[public_key] = {
                "value_money": round_money(current_money),
                "fact_money": round_money(current_money),
                "base_money": scaled_base_money,
                "value_percent": current_percent,
                "fact_percent": current_percent,
                "base_percent": base_percent,
                "delta_percent": _pp_change(current_percent, base_percent),
                "effect_money": effect_money,
            }
            continue
        current_money = _to_float(current_metrics.get(metric_key))
        base_money = _to_float(base_metrics.get(metric_key))
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




def _first_nonempty(rows: List[Dict[str, Any]], field: str, default: str = "") -> str:
    for row in rows:
        value = str(row.get(field) or "").strip()
        if value and value not in {"Пусто", "Без менеджера"}:
            return value
    return default


def _percentile(sorted_values: List[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    position = (len(sorted_values) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    weight = position - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def _quartile_bounds(values: List[float]) -> Dict[str, float]:
    valid = sorted(_to_float(v) for v in values if v is not None and math.isfinite(_to_float(v)))
    if not valid:
        return {"q1": 0.0, "median": 0.0, "q3": 0.0}
    return {
        "q1": round_percent(_percentile(valid, 0.25)),
        "median": round_percent(_percentile(valid, 0.50)),
        "q3": round_percent(_percentile(valid, 0.75)),
    }


def _margin_signal(margin_pre: float, bounds: Dict[str, float]) -> Dict[str, Any]:
    value = round_percent(_to_float(margin_pre))
    q1 = _to_float(bounds.get("q1"))
    median = _to_float(bounds.get("median"))
    q3 = _to_float(bounds.get("q3"))
    if value <= q1:
        status = "critical"
        label = "критично"
    elif value <= median:
        status = "risk"
        label = "риск"
    elif value <= q3:
        status = "normal"
        label = "норма"
    else:
        status = "strong"
        label = "сильно"
    return {
        "status": status,
        "label": label,
        "margin_pre": value,
        "quartiles": bounds,
    }


def _is_signal_drain_candidate(item: Dict[str, Any]) -> bool:
    signal = item.get("signal") if isinstance(item.get("signal"), dict) else {}
    return signal.get("status") in {"critical", "risk"} and _to_float(item.get("effect_money")) < 0


def _build_network_adaptive_drain(object_rows: List[Dict[str, Any]], business_metrics: Dict[str, Any], limit: Optional[int] = 3) -> Tuple[List[Dict[str, Any]], str]:
    """Build network drain through signal layer.

    The signal is only a selector: SKU margin_pre -> quartiles/median ->
    critical/risk candidates. Money effect and structure calculations stay unchanged.
    """
    sku_groups = _group(object_rows, "sku")
    if not sku_groups:
        return [], "sku"

    prepared: List[Dict[str, Any]] = []
    margins: List[float] = []
    for sku_name, rows in sku_groups.items():
        metrics = aggregate_metrics(rows)
        margin_percent = round_percent(_to_float(metrics.get("margin_pre")))
        drain = _drain_item(sku_name, rows, business_metrics)
        item = {
            "sku": sku_name,
            "category": _first_nonempty(rows, "category", "Без категории"),
            "tmc_group": _first_nonempty(rows, "tmc_group", "Без группы"),
            "margin_percent": margin_percent,
            "effect_money": _to_float(drain.get("effect_money")),
            "object_result_money": _to_float(drain.get("object_result_money")),
            "opportunity_money": _to_float(drain.get("opportunity_money")),
            "article_name": drain.get("article_name"),
        }
        prepared.append(item)
        margins.append(margin_percent)

    bounds = _quartile_bounds(margins)
    signal_filtered: List[Dict[str, Any]] = []
    for item in prepared:
        signal = _margin_signal(item.get("margin_percent"), bounds)
        item["signal"] = {**signal, "source": "margin_pre_quartile"}
        if signal.get("status") in {"critical", "risk"} and _to_float(item.get("effect_money")) < 0:
            signal_filtered.append(item)

    if not signal_filtered:
        # V11.2: even when no strict negative signal exists, keep navigation alive
        # with up to 3 contextual SKU choices.
        prepared.sort(key=lambda item: _to_float(item.get("effect_money")))
        fallback_items = [{
            "object_name": item.get("sku"),
            "effect_money": round_money(_to_float(item.get("effect_money"))),
            "object_result_money": round_money(_to_float(item.get("object_result_money"))),
            "opportunity_money": round_money(_to_float(item.get("opportunity_money"))),
            "article_name": item.get("article_name") or "прочее",
            "signal": {"status": "context", "label": "контекст", "source": "v11_2_no_signal_fill"},
        } for item in prepared[:limit or len(prepared)]]
        return fallback_items, "sku"

    category_totals: Dict[str, float] = defaultdict(float)
    group_totals: Dict[str, float] = defaultdict(float)
    category_articles: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    group_articles: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    category_signals: Dict[str, List[str]] = defaultdict(list)
    group_signals: Dict[str, List[str]] = defaultdict(list)
    total_negative_effect_abs = 0.0

    for item in signal_filtered:
        effect_money = _to_float(item["effect_money"])
        article_name = str(item.get("article_name") or "прочее")
        total_negative_effect_abs += abs(effect_money)
        category_totals[item["category"]] += effect_money
        group_totals[item["tmc_group"]] += effect_money
        category_articles[item["category"]][article_name] += effect_money
        group_articles[item["tmc_group"]][article_name] += effect_money
        category_signals[item["category"]].append((item.get("signal") or {}).get("status") or "")
        group_signals[item["tmc_group"]].append((item.get("signal") or {}).get("status") or "")

    category_shares = {
        name: (abs(value) / total_negative_effect_abs) if total_negative_effect_abs > 1e-9 else 0.0
        for name, value in category_totals.items()
    }
    use_category = bool(category_shares) and max(category_shares.values()) > 0.5

    selected_level = "category" if use_category else "tmc_group"
    selected_totals = category_totals if use_category else group_totals
    selected_articles = category_articles if use_category else group_articles
    selected_signals = category_signals if use_category else group_signals

    drain_items = []
    for name, value in selected_totals.items():
        if _to_float(value) >= 0:
            continue
        article_map = selected_articles.get(name) or {}
        article_name = None
        article_effect = None
        for article, article_value in article_map.items():
            if article_effect is None or article_value < article_effect:
                article_name = article
                article_effect = article_value
        statuses = selected_signals.get(name) or []
        status = "critical" if "critical" in statuses else ("risk" if "risk" in statuses else "")
        drain_items.append({
            "object_name": name,
            "effect_money": round_money(value),
            "object_result_money": round_money(value),
            "opportunity_money": abs(round_money(value)) if _to_float(value) < 0 else 0.0,
            "article_name": article_name or "прочее",
            "signal": {
                "status": status,
                "label": "критично" if status == "critical" else ("риск" if status == "risk" else ""),
                "source": "margin_pre_quartile",
                "quartiles": bounds,
            },
        })
    drain_items.sort(key=lambda item: _to_float(item.get("effect_money")))

    # V10.1: network/product drain also must expose at least 3 selectable
    # objects when available. Fill from all prepared SKU grouped by the selected
    # aggregation level, not only strict negative signal candidates.
    if limit is not None and len(drain_items) < limit:
        all_totals: Dict[str, float] = defaultdict(float)
        all_articles: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
        for item in prepared:
            key_name = item["category"] if selected_level == "category" else item["tmc_group"]
            effect_money = _to_float(item.get("effect_money"))
            article_name = str(item.get("article_name") or "прочее")
            all_totals[key_name] += effect_money
            all_articles[key_name][article_name] += effect_money
        seen = {str(item.get("object_name")) for item in drain_items}
        fillers: List[Dict[str, Any]] = []
        for name, value in all_totals.items():
            if str(name) in seen:
                continue
            article_map = all_articles.get(name) or {}
            article_name = None
            article_effect = None
            for article, article_value in article_map.items():
                if article_effect is None or article_value < article_effect:
                    article_name = article
                    article_effect = article_value
            fillers.append({
                "object_name": name,
                "effect_money": round_money(value),
                "object_result_money": round_money(value),
                "opportunity_money": abs(round_money(value)) if _to_float(value) < 0 else 0.0,
                "article_name": article_name or "прочее",
                "signal": {"status": "context", "label": "контекст", "source": "v10_1_min_3_fill"},
            })
        fillers.sort(key=lambda item: _to_float(item.get("effect_money")))
        drain_items.extend(fillers[: max(0, limit - len(drain_items))])

    return (drain_items[:limit] if limit is not None else drain_items), selected_level


def _drain_item(name: str, rows: List[Dict[str, Any]], business_metrics: Dict[str, Any], prev_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    current = aggregate_metrics(rows)
    structure = _build_structure(current, business_metrics)
    total_effect = round_money(sum(_to_float(value.get("effect_money")) for value in structure.values()))
    article_key = None
    article_effect = None
    for key in ["markup", "retro", "logistics", "personnel", "other"]:
        value = _to_float((structure.get(key) or {}).get("effect_money"))
        if article_effect is None or value < article_effect:
            article_key = key
            article_effect = value
    opportunity = abs(_sum_negative_effect([value for value in structure.values() if isinstance(value, dict)]))
    return {
        "object_name": name,
        "effect_money": total_effect,
        "object_result_money": total_effect,
        "opportunity_money": opportunity,
        "article_name": ARTICLE_LABELS_RU.get(article_key, article_key or "прочее"),
    }


def _build_drain(level: str, object_rows: List[Dict[str, Any]], business_metrics: Dict[str, Any], period: str, limit: Optional[int] = 3) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if level == "network":
        return _build_network_adaptive_drain(object_rows, business_metrics, limit=limit)

    child = LEVEL_CHILD.get(level)
    if not child:
        return [], None
    grouped = _group(object_rows, child)

    prepared: List[Dict[str, Any]] = []
    margins: List[float] = []
    for name, rows in grouped.items():
        metrics = aggregate_metrics(rows)
        margin_pre = round_percent(_to_float(metrics.get("margin_pre")))
        item = _drain_item(name, rows, business_metrics)
        item["margin_pre"] = margin_pre
        prepared.append(item)
        margins.append(margin_pre)

    bounds = _quartile_bounds(margins)
    items: List[Dict[str, Any]] = []
    for item in prepared:
        signal = _margin_signal(item.get("margin_pre"), bounds)
        item["signal"] = {**signal, "source": "margin_pre_quartile"}
        if _is_signal_drain_candidate(item):
            items.append(item)

    items.sort(key=lambda item: _to_float(item.get("effect_money")))

    # V10.1: management drain must always give the user a choice.
    # If strict signal-drain returns fewer than requested items, fill the list
    # with the next objects from the full child set (zero/positive included as
    # context). Calculations stay API-owned; this only affects selection UX.
    if limit is not None and len(items) < limit:
        seen = {str(item.get("object_name")) for item in items}
        fillers = [dict(item) for item in prepared if str(item.get("object_name")) not in seen]
        fillers.sort(key=lambda item: _to_float(item.get("effect_money")))
        items.extend(fillers[: max(0, limit - len(items))])

    return (items[:limit] if limit is not None else items), child


def _build_navigation(level: str, drain_block: List[Dict[str, Any]], next_level: Optional[str] = None, has_all: bool = False, has_causes: bool = False) -> Dict[str, Any]:
    return {
        "current_level": level,
        "next_level": next_level if next_level is not None else LEVEL_CHILD.get(level),
        "items": [item.get("object_name") for item in drain_block[:3]],
        "has_all": has_all,
        "has_causes": has_causes,
        "has_back": True,
    }


def _human_object_name(level: str, object_name: str) -> str:
    return HUMAN_LEVEL_NAMES.get(level, object_name)







def _sort_navigation_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """CHANGE-005 / Profit First Navigation Contract.

    The primary route is now based on profit movement versus the same object
    in the previous period. Objects with the largest profit decline are shown
    first. Opportunity money remains available as a separate reserve layer, but
    it no longer defines the main navigation order.
    """

    def _key(item: Dict[str, Any]):
        delta = _to_float(item.get("profit_delta_money"))
        is_loss = delta < 0
        return (0 if is_loss else 1, delta if is_loss else -delta, str(item.get("object_name") or ""))

    return sorted(items or [], key=_key)


def _ensure_navigation_contract(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    contracted: List[Dict[str, Any]] = []
    for idx, item in enumerate(items or [], start=1):
        if not isinstance(item, dict):
            continue
        cloned = dict(item)
        cloned.setdefault("object_id", idx)
        cloned.setdefault("object_name", cloned.get("name"))
        navigation_money = cloned.get("navigation_money")
        if navigation_money is None:
            effect = _to_float(cloned.get("effect_money"))
            navigation_money = abs(effect) if effect < 0 else 0.0
        cloned["navigation_money"] = round_money(_to_float(navigation_money))
        cloned.setdefault("object_result_money", round_money(_to_float(cloned.get("effect_money"))))
        cloned.setdefault("opportunity_money", cloned["navigation_money"])
        contracted.append(cloned)
    return _sort_navigation_items(contracted)


def _child_prev_metrics(child: str, child_rows: List[Dict[str, Any]], period: str) -> Dict[str, Any]:
    """Previous-period metrics for the same child object and parent context.

    This is the source for Profit First navigation: object vs itself in the
    previous period. It intentionally does not use Business Benchmark or
    Opportunity.
    """
    prev_period = _previous_year_period(period)
    if not prev_period or not child_rows:
        return {}

    first = child_rows[0] or {}
    order = ["manager_top", "manager", "network", "category", "tmc_group", "sku"]
    filters: Dict[str, Any] = {}
    for key in order:
        value = first.get(key)
        if value not in (None, ""):
            filters[key] = value
        if key == child:
            break

    cache_key = (prev_period, child, tuple(sorted((str(k), str(v)) for k, v in filters.items())))
    if cache_key in CHILD_PREV_METRICS_CACHE:
        return CHILD_PREV_METRICS_CACHE[cache_key]
    try:
        all_rows = _safe_get_rows()
        prev_rows, _ = _run_filter(all_rows, prev_period, **filters)
    except Exception:
        return {}
    result = aggregate_metrics(prev_rows or []) if prev_rows else {}
    CHILD_PREV_METRICS_CACHE[cache_key] = result
    return result


def _profit_delta_money(current_metrics: Dict[str, Any], prev_metrics: Dict[str, Any]) -> float:
    return round_money(_to_float(current_metrics.get("finrez_pre")) - _to_float(prev_metrics.get("finrez_pre")))



def _previous_child_groups(level: str, child: str, rows: List[Dict[str, Any]], period: str) -> Dict[str, List[Dict[str, Any]]]:
    prev_period = _previous_year_period(period)
    if not prev_period or not rows:
        return {}
    first = rows[0] or {}
    order = ["manager_top", "manager", "network", "category", "tmc_group", "sku"]
    filters: Dict[str, Any] = {}
    for key in order:
        if key == child:
            break
        value = first.get(key)
        if value not in (None, ""):
            filters[key] = value
        if key == level:
            # Parent scope reached. Do not include lower-level fields.
            continue
    try:
        prev_rows, _ = _run_filter(_safe_get_rows(), prev_period, **filters)
    except Exception:
        return {}
    return _group(prev_rows or [], child)

def _build_all_block(level: str, rows: List[Dict[str, Any]], business_metrics: Dict[str, Any], period: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    # CHANGE-005 / Profit First Navigation:
    # Full navigation source is built once and then sliced for Top-3.
    # Sorting is based on profit_delta_money: object vs same object previous year.
    # Opportunity money stays in the item as a reserve layer, not as the primary route.
    if level == "network":
        # BUG-019 / Product Layer Full List:
        # A Network screen must expose every Product Layer object that exists
        # under the current parent context. The previous adaptive signal drain
        # could build all_block from only risk/critical SKU candidates, so
        # categories with valid DATA rows (for example "Напитки" inside
        # Варус) disappeared from both navigation and the command "все".
        #
        # Keep the architecture stable: Network -> Category -> SKU.
        # Signals may still be used inside factors/reasons, but they must not
        # be a filter for the full Product Layer list.
        child = "category"
        grouped = _group(rows, child)
        prev_grouped = _previous_child_groups(level, child, rows, period)
        items: List[Dict[str, Any]] = []
        for idx, (name, child_rows) in enumerate(grouped.items(), start=1):
            current_child_metrics = aggregate_metrics(child_rows)
            prev_child_metrics = aggregate_metrics(prev_grouped.get(name, []))
            profit_delta = _profit_delta_money(current_child_metrics, prev_child_metrics)
            item = _drain_item(name, child_rows, business_metrics)
            child_structure = _build_structure(current_child_metrics, business_metrics)
            opportunity_money = _opportunity_money(child_structure)
            items.append({
                "object_id": idx,
                "object_name": item.get("object_name"),
                "effect_money": round_money(_to_float(item.get("effect_money"))),
                "object_result_money": round_money(_to_float(item.get("effect_money"))),
                "opportunity_money": opportunity_money,
                "article_name": item.get("article_name"),
                "profit_delta_money": profit_delta,
                "navigation_money": abs(profit_delta),
            })
        return _ensure_navigation_contract(items), child

    child = LEVEL_CHILD.get(level)
    if not child:
        return [], None

    grouped = _group(rows, child)
    prev_grouped = _previous_child_groups(level, child, rows, period)
    items: List[Dict[str, Any]] = []
    for idx, (name, child_rows) in enumerate(grouped.items(), start=1):
        current_child_metrics = aggregate_metrics(child_rows)
        prev_child_metrics = aggregate_metrics(prev_grouped.get(name, []))
        profit_delta = _profit_delta_money(current_child_metrics, prev_child_metrics)

        item = _drain_item(name, child_rows, business_metrics)
        effect_money = round_money(_to_float(item.get("effect_money")))
        child_structure = _build_structure(current_child_metrics, business_metrics)
        opportunity_money = _opportunity_money(child_structure)
        items.append({
            "object_id": idx,
            "object_name": item.get("object_name"),
            "effect_money": effect_money,
            "object_result_money": effect_money,
            "opportunity_money": opportunity_money,
            "profit_delta_money": profit_delta,
            "navigation_money": abs(profit_delta),
        })

    return _ensure_navigation_contract(items), child


SOLUTION_LEVEL_MAP = {
    "markup": "sku",
    "retro": "network",
    "logistics": "network",
    "personnel": "network",
    "other": "network",
}

ACTION_MAP = {
    "markup": "raise_margin",
    "retro": "reduce_retro",
    "logistics": "reduce_logistics",
    "personnel": "reduce_personnel",
    "other": "reduce_other",
}


def _metric_direction(effect_money: float) -> str:
    return "bad" if effect_money < 0 else "good"


def _metric_name(key: str) -> str:
    return "margin" if key == "markup" else key


def _build_decision_block(structure: Dict[str, Any], *, source_level: str, top_n: int = 3) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for key in ["markup", "retro", "logistics", "personnel", "other"]:
        item = structure.get(key) or {}
        effect_money = round_money(_to_float(item.get("effect_money")))
        if effect_money >= 0:
            continue
        items.append({
            "action": ACTION_MAP.get(key, f"fix_{_metric_name(key)}"),
            "metric": _metric_name(key),
            "direction": _metric_direction(effect_money),
            "solution_level": "sku" if source_level == "sku" else SOLUTION_LEVEL_MAP.get(key, "network"),
            "effect_money": effect_money,
            "source_level": source_level,
        })
    items.sort(key=lambda x: _to_float(x.get("effect_money")))
    return items[:top_n]


def _build_grouping_type(rows: List[Dict[str, Any]], period: str) -> str:
    try:
        sku_to_category = {}
        sku_to_group = {}
        for row in rows:
            sku = clean_text(row.get("sku", ""))
            if not sku:
                continue
            sku_to_category[sku] = clean_text(row.get("category", "")) or "Без категории"
            sku_to_group[sku] = clean_text(row.get("tmc_group", "")) or "Без группы"
        if not sku_to_category:
            return "sku"
        total_sku = len(sku_to_category)
        by_category: Dict[str, int] = defaultdict(int)
        for category in sku_to_category.values():
            by_category[category] += 1
        max_share = max(by_category.values()) / total_sku if total_sku else 0.0
        return "category" if max_share > 0.5 else "tmc_group"
    except Exception:
        return "sku"


def _previous_article_values(key: str, prev_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Return previous-year money/percent for a structure article.

    V11.2: protect markup from false previous-year percentages when the
    comparison base is too small or the stored markup percentage is outside the
    company-readable turnover-percent range. Numbers are still API/backend
    supplied; UI gets an explicit flag instead of a misleading 277% value.
    """
    revenue = _to_float(prev_metrics.get("revenue"))
    if key == "markup":
        money = round_money(_safe_markup_money(prev_metrics))
        percent = _safe_markup_percent(prev_metrics)
        has_valid_base = abs(revenue) >= MARKUP_PREVIOUS_REVENUE_MIN and abs(percent) <= MARKUP_PREVIOUS_MARKUP_PERCENT_MAX
        return {
            "previous_money": money,
            "previous_percent": percent if has_valid_base else None,
            "previous_percent_missing": not has_valid_base,
            "previous_note": "" if has_valid_base else "нет корректной базы",
        }

    metric_key = {
        "retro": "retro_bonus",
        "logistics": "logistics_cost",
        "personnel": "personnel_cost",
        "other": "other_costs",
    }.get(key)
    raw_money = _to_float(prev_metrics.get(metric_key)) if metric_key else 0.0
    signed_money = round_money(_signed_money(raw_money, True))
    percent = _signed_percent(signed_money, revenue)
    return {"previous_money": signed_money, "previous_percent": percent}


def _reason_signal(delta_vs_business: float, effect_money: float) -> str:
    if effect_money < 0 and delta_vs_business <= -10:
        return "критично"
    if effect_money < 0 and delta_vs_business <= -5:
        return "риск"
    if effect_money < 0:
        return "внимание"
    return "норма"


def _build_cause_block(structure: Dict[str, Any], prev_metrics: Optional[Dict[str, Any]] = None, current_metrics: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    causes: List[Dict[str, Any]] = []
    prev_metrics = prev_metrics or {}
    current_metrics = current_metrics or {}
    current_revenue = _to_float(current_metrics.get("revenue"))
    for key in ["markup", "retro", "logistics", "personnel", "other"]:
        item = structure.get(key) or {}
        value_money = round_money(_to_float(item.get("value_money", item.get("fact_money"))))
        value_percent = round_percent(_to_float(item.get("value_percent", item.get("fact_percent"))))
        base_percent = round_percent(_to_float(item.get("base_percent")))
        effect_money = round_money(_to_float(item.get("effect_money")))
        effect_vs_business_money = effect_money
        previous = _previous_article_values(key, prev_metrics)
        previous_percent_raw = previous.get("previous_percent")
        previous_percent_missing = bool(previous.get("previous_percent_missing")) or previous_percent_raw is None
        previous_percent = None if previous_percent_missing else round_percent(_to_float(previous_percent_raw))
        delta_vs_business = round_percent(value_percent - base_percent)
        delta_vs_previous = None if previous_percent_missing else round_percent(value_percent - previous_percent)
        effect_vs_previous_money = None if delta_vs_previous is None else round_money((delta_vs_previous / 100.0) * current_revenue)
        causes.append({
            "name": ARTICLE_LABELS_RU.get(key, key).capitalize(),
            "value_money": value_money,
            "money": value_money,
            "value_percent": value_percent,
            "percent": value_percent,
            "base_percent": base_percent,
            "previous_money": round_money(_to_float(previous.get("previous_money"))),
            "previous_percent": previous_percent,
            "previous_percent_missing": previous_percent_missing,
            "previous_note": previous.get("previous_note", ""),
            "delta_vs_business_percent": delta_vs_business,
            "delta_vs_previous_percent": delta_vs_previous,
            "delta_percent": delta_vs_business,
            # effect_money is kept as legacy alias for benchmark/business comparison.
            "effect_money": effect_money,
            "effect_vs_business_money": effect_vs_business_money,
            "effect_vs_previous_money": effect_vs_previous_money,
            "signal": _reason_signal(delta_vs_business, effect_money),
        })
    causes.sort(key=lambda x: _to_float(x.get("effect_money")))
    return causes


def _build_sku_decision(structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _build_decision_block(structure, source_level="sku", top_n=3)


def _product_baseline_rows(
    all_rows: List[Dict[str, Any]],
    level: str,
    period: str,
    current_rows: List[Dict[str, Any]],
    object_name: str,
) -> tuple[List[Dict[str, Any]], str]:
    """
    V11.1 Product Baseline Layer.

    For levels below network, compare product economics with the same product
    across the whole business, not with total business average.
    """
    level = (level or "").lower()

    if level not in {"category", "tmc_group", "sku"}:
        rows, _ = _run_filter(all_rows, period=period)
        return rows, "business"

    category = _first_nonempty(current_rows, "category")
    tmc_group = _first_nonempty(current_rows, "tmc_group")
    sku = _first_nonempty(current_rows, "sku")

    if level == "category":
        key = category or object_name
        rows, _ = _run_filter(all_rows, period=period, category=key)
        return rows, "category_business" if rows else "product_baseline_missing"

    if level == "tmc_group":
        key = tmc_group or object_name
        rows, _ = _run_filter(all_rows, period=period, tmc_group=key)
        return rows, "tmc_group_business" if rows else "product_baseline_missing"

    if level == "sku":
        # priority 1: same SKU across business
        key = sku or object_name
        rows, _ = _run_filter(all_rows, period=period, sku=key)
        if rows:
            return rows, "sku_business"

        # priority 2: same TMC group
        if tmc_group:
            rows, _ = _run_filter(all_rows, period=period, tmc_group=tmc_group)
            if rows:
                return rows, "sku_fallback_tmc_group"

        # priority 3: same category
        if category:
            rows, _ = _run_filter(all_rows, period=period, category=category)
            if rows:
                return rows, "sku_fallback_category"

        return [], "product_baseline_missing"

    rows, _ = _run_filter(all_rows, period=period)
    return rows, "business"




def _sum_negative_effect(items: List[Dict[str, Any]]) -> float:
    total = 0.0
    for item in items or []:
        if not isinstance(item, dict):
            continue
        value = _to_float(item.get("effect_money"))
        if value < 0:
            total += value
    return round_money(total)


def _object_result_money(structure: Dict[str, Any]) -> float:
    """Stage 1 Money Contract: object result vs active Benchmark.

    The Benchmark is already selected before structure is built:
    - business -> previous year
    - management/product objects -> business or product baseline

    Do not use goal/focus/path/vector/drain fields here.
    """
    return round_money(sum(_to_float(item.get("effect_money")) for item in (structure or {}).values() if isinstance(item, dict)))


def _opportunity_money(structure: Dict[str, Any]) -> float:
    """Stage 1 Money Contract: recoverable profit potential inside the object.

    Only negative article effects are recoverable opportunity. Positive effects
    are context and must not compensate losses.
    """
    return abs(_sum_negative_effect([item for item in (structure or {}).values() if isinstance(item, dict)]))


def _product_tmc_decision(level: str, rows: List[Dict[str, Any]], all_rows: List[Dict[str, Any]], period: str) -> Optional[Dict[str, Any]]:
    """ARCH-018: Dynamic product decision support.

    Performance guard: category screens must not run full DATA filtering once
    per товарная группа. Previous-year and business comparison data are grouped
    in bulk and then reused.
    """
    if level != "category" or not rows:
        return None

    grouped = _group(rows, "tmc_group")
    if not grouped:
        return None

    group_names = {str(name) for name in grouped.keys()}
    first = rows[0] or {}
    prev_period = _previous_year_period(period)

    parent_prev_rows: List[Dict[str, Any]] = []
    if prev_period:
        parent_filters = {
            key: first.get(key)
            for key in ["manager_top", "manager", "network", "category"]
            if first.get(key) not in (None, "")
        }
        try:
            parent_prev_rows, _ = _run_filter(all_rows, period=prev_period, **parent_filters)
        except Exception:
            parent_prev_rows = []

    prev_grouped = _group(parent_prev_rows, "tmc_group") if parent_prev_rows else {}

    business_grouped: Dict[str, List[Dict[str, Any]]] = {name: [] for name in group_names}
    for row in all_rows or []:
        try:
            if str(row.get("period") or "") != str(period):
                continue
            name = str(row.get("tmc_group") or "")
            if name in business_grouped:
                business_grouped[name].append(row)
        except Exception:
            continue

    parent_current = aggregate_metrics(rows)
    parent_prev = aggregate_metrics(parent_prev_rows)
    parent_delta = _profit_delta_money(parent_current, parent_prev)
    if abs(parent_delta) > 1e-9:
        denominator = abs(parent_delta)
    else:
        denominator = 0.0
        for name, chunk in grouped.items():
            denominator += abs(_profit_delta_money(aggregate_metrics(chunk), aggregate_metrics(prev_grouped.get(name, []))))

    items: List[Dict[str, Any]] = []
    for name, chunk in grouped.items():
        current = aggregate_metrics(chunk)
        prev = aggregate_metrics(prev_grouped.get(name, []))
        profit_delta = _profit_delta_money(current, prev)
        business_metrics = aggregate_metrics(business_grouped.get(str(name), [])) if business_grouped.get(str(name)) else {}
        margin_delta = round_percent(_to_float(current.get("margin_pre")) - _to_float(business_metrics.get("margin_pre"))) if business_metrics else 0.0
        markup_delta = round_percent(_to_float(current.get("markup")) - _to_float(business_metrics.get("markup"))) if business_metrics else 0.0
        effect_money = round_money((markup_delta / 100.0) * _to_float(current.get("revenue"))) if _to_float(current.get("revenue")) else 0.0
        share = round_percent((abs(profit_delta) / denominator) * 100.0) if denominator > 1e-9 else 0.0
        items.append({
            "object_name": name,
            "profit_delta_money": profit_delta,
            "share_percent": share,
            "margin_percent": round_percent(_to_float(current.get("margin_pre"))),
            "business_margin_percent": round_percent(_to_float(business_metrics.get("margin_pre"))) if business_metrics else None,
            "margin_delta_percent": margin_delta,
            "markup_percent": round_percent(_to_float(current.get("markup"))),
            "business_markup_percent": round_percent(_to_float(business_metrics.get("markup"))) if business_metrics else None,
            "markup_delta_percent": markup_delta,
            "benchmark_effect_money": effect_money,
        })

    items.sort(key=lambda item: (-abs(_to_float(item.get("profit_delta_money"))), str(item.get("object_name") or "")))
    dominant = items[0] if items else None
    dominant_share = _to_float(dominant.get("share_percent")) if dominant else 0.0
    return {
        "level": "tmc_group",
        "mode": "dominant" if dominant_share >= 50.0 else "distributed",
        "dominant_share_threshold": 50.0,
        "items": items,
        "dominant_item": dominant,
    }

def _navigation_money(items: List[Dict[str, Any]]) -> float:
    """Stage 1 Money Contract: lower-level money used only for navigation."""
    return abs(_sum_negative_effect(items or []))


def _attach_navigation_money(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    result: List[Dict[str, Any]] = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        cloned = dict(item)
        effect = _to_float(cloned.get("effect_money"))
        cloned["navigation_money"] = round_money(abs(effect)) if effect < 0 else 0.0
        result.append(cloned)
    return result


def _sum_net_effect(items: List[Dict[str, Any]]) -> float:
    total = 0.0
    for item in items or []:
        if not isinstance(item, dict):
            continue
        total += _to_float(item.get("effect_money"))
    return round_money(total)


def _attach_money_contract(payload: Dict[str, Any], structure: Dict[str, Any], drain_block: List[Dict[str, Any]], all_block: List[Dict[str, Any]]) -> None:
    """Stage 1: canonical domain money contract.

    Required independent entities:
    - object_result_money: result of this object relative to the active Benchmark
    - opportunity_money: recoverable profit potential inside this object
    - navigation_money: lower-level money used only for navigation

    Forbidden as sources for this contract: goal, focus_money, path_goal_money,
    drain_total, vector_block.
    """
    source_items = all_block if all_block else drain_block
    payload["object_result_money"] = _object_result_money(structure)
    payload["opportunity_money"] = _opportunity_money(structure)
    payload["navigation_money"] = _navigation_money(source_items)

    # Legacy aliases stay available for old screens, but they are derived from
    # the locked contract/calculation sources above and no longer drive money.
    payload["net_drain_money"] = _sum_net_effect(source_items)
    payload["gross_loss_money"] = -payload["navigation_money"]
    payload["internal_drain_money"] = -payload["opportunity_money"]
    payload["drain_total"] = -payload["navigation_money"]


def _business_result_money(current_metrics: Dict[str, Any], prev_metrics: Dict[str, Any]) -> float:
    """Stage 3 Business Contract: business result relative to previous year."""
    return round_money(_to_float(current_metrics.get("finrez_pre")) - _to_float(prev_metrics.get("finrez_pre")))


def _rating_item(item: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "object_id": item.get("object_id"),
        "object_name": item.get("object_name"),
        "object_result_money": round_money(_to_float(item.get("object_result_money"))),
        "opportunity_money": round_money(_to_float(item.get("opportunity_money"))),
        "navigation_money": round_money(_to_float(item.get("navigation_money"))),
    }


def _business_result_rating(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Stage 3 Business Contract: rating by object_result_money only."""
    rating = [_rating_item(item) for item in (items or []) if isinstance(item, dict)]
    rating.sort(key=lambda item: (-_to_float(item.get("object_result_money")), str(item.get("object_name") or "")))
    return rating


def _opportunity_rating(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Stage 3 Business Contract: rating by opportunity_money only."""
    rating = [_rating_item(item) for item in (items or []) if isinstance(item, dict)]
    rating.sort(key=lambda item: (-_to_float(item.get("opportunity_money")), str(item.get("object_name") or "")))
    return rating


def _profit_loss_rating(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """CHANGE-005: rating of objects by profit decline vs previous year."""
    rating: List[Dict[str, Any]] = []
    for item in (items or []):
        if not isinstance(item, dict):
            continue
        cloned = _rating_item(item)
        cloned["profit_delta_money"] = round_money(_to_float(item.get("profit_delta_money")))
        rating.append(cloned)
    rating.sort(key=lambda item: (_to_float(item.get("profit_delta_money")), str(item.get("object_name") or "")))
    return rating


def _priority_action(structure: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Stage 3 Business Contract: largest recoverable negative effect."""
    candidates: List[Dict[str, Any]] = []
    for key in ["markup", "retro", "logistics", "personnel", "other"]:
        item = (structure or {}).get(key) or {}
        effect_money = round_money(_to_float(item.get("effect_money")))
        if effect_money >= 0:
            continue
        candidates.append({
            "metric": _metric_name(key),
            "action": ACTION_MAP.get(key, key),
            "effect_money": effect_money,
            "expected_effect_money": abs(effect_money),
        })
    if not candidates:
        return None
    candidates.sort(key=lambda item: -_to_float(item.get("expected_effect_money")))
    return candidates[0]


DECISION_FACTOR_LABELS = {
    "markup": "Наценка",
    "retro": "Ретро",
    "logistics": "Логистика",
    "personnel": "Персонал",
    "other": "Прочие",
}

DECISION_ACTION_LABELS = {
    "raise_margin": "Повысить наценку",
    "reduce_retro": "Пересобрать ретроусловия",
    "reduce_logistics": "Оптимизировать логистику",
    "reduce_personnel": "Оптимизировать персонал",
    "reduce_other": "Снизить прочие затраты",
}


def _decision_workspace_summary_text(current_metrics: Dict[str, Any], prev_metrics: Dict[str, Any]) -> str:
    profit_delta = _profit_delta_money(current_metrics, prev_metrics)
    revenue_delta = round_money(_to_float(current_metrics.get("revenue")) - _to_float(prev_metrics.get("revenue")))
    margin_delta = round_percent(_to_float(current_metrics.get("margin_pre")) - _to_float(prev_metrics.get("margin_pre")))
    if profit_delta < 0 and revenue_delta < 0:
        return "Контракт требует решений по возврату прибыли. Главный диагностический сигнал — снижение прибыли вместе с потерей оборота."
    if profit_delta < 0 and margin_delta < 0:
        return "Контракт требует решений по доходности. Прибыль снизилась, маржа также ухудшилась."
    if profit_delta < 0:
        return "Контракт требует управленческого решения: прибыль снизилась относительно прошлого года."
    return "Контракт показывает положительную динамику прибыли. Decision Engine должен определить, что можно масштабировать или удержать."


def _decision_workspace_factor_items(structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for key in ["markup", "retro", "logistics", "personnel", "other"]:
        factor = structure.get(key) or {}
        effect = round_money(_to_float(factor.get("effect_money")))
        if abs(effect) < 1e-9:
            continue
        items.append({
            "factor": key,
            "name": DECISION_FACTOR_LABELS.get(key, key),
            "effect_money": effect,
            "delta_percent": round_percent(_to_float(factor.get("delta_percent"))),
            "signal": "risk" if effect < 0 else "ok",
        })
    items.sort(key=lambda item: abs(_to_float(item.get("effect_money"))), reverse=True)
    return items


def _decision_workspace_recommended_actions(decision_block: List[Dict[str, Any]], structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    actions: List[Dict[str, Any]] = []
    for item in decision_block or []:
        if not isinstance(item, dict):
            continue
        action_key = str(item.get("action") or "")
        metric = str(item.get("metric") or "")
        source_key = "markup" if metric == "margin" else metric
        factor = structure.get(source_key) or {}
        effect = abs(round_money(_to_float(item.get("effect_money"))))
        if effect <= 0:
            continue
        actions.append({
            "object": "contract",
            "problem": DECISION_FACTOR_LABELS.get(source_key, metric),
            "reason": "Отклонение фактора ухудшает прибыль контракта.",
            "action": DECISION_ACTION_LABELS.get(action_key, action_key or "Проверить фактор"),
            "expected_effect_money": effect,
            "source_factor": source_key,
            "evidence_level": item.get("solution_level") or "network",
            "delta_percent": round_percent(_to_float(factor.get("delta_percent"))),
        })
    actions.sort(key=lambda action: _to_float(action.get("expected_effect_money")), reverse=True)
    return actions


def _rate_percent(rows: List[Dict[str, Any]], field: str) -> float:
    revenue = _sum_metric(rows, "revenue")
    if abs(revenue) < 1e-9:
        return 0.0
    if field == "markup":
        # markup is stored as a percent in DATA rows; use revenue-weighted average.
        weighted = sum(_to_float(row.get("markup")) * _to_float(row.get("revenue")) for row in rows or [])
        return round_percent(weighted / revenue) if abs(revenue) > 1e-9 else 0.0
    # Cost factors are shown as negative percent of revenue in the Workspace.
    return round_percent(-(_sum_metric(rows, field) / revenue) * 100.0)


def _category_potential_breakdown(category_rows: List[Dict[str, Any]], business_category_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    revenue = _sum_metric(category_rows, "revenue")
    if revenue <= 0 or not business_category_rows:
        return {"total_money": 0.0, "items": []}

    factor_specs = [
        ("markup", "Наценка", "markup"),
        ("retro_bonus", "Ретро", "retro"),
        ("logistics_cost", "Логистика", "logistics"),
        ("personnel_cost", "Персонал", "personnel"),
        ("other_costs", "Прочие", "other"),
    ]
    items: List[Dict[str, Any]] = []
    for field, name, code in factor_specs:
        contract_rate = _rate_percent(category_rows, field)
        business_rate = _rate_percent(business_category_rows, field)
        delta_pp = round_percent(contract_rate - business_rate)
        # For markup, lower than business is a loss. For negative cost rates,
        # more negative than business is a loss. Both cases are captured below.
        if field == "markup":
            effect = round_money(max(0.0, (business_rate - contract_rate) / 100.0 * revenue))
        else:
            effect = round_money(max(0.0, (business_rate - contract_rate) / 100.0 * revenue))
        if effect > 0:
            items.append({
                "factor": code,
                "name": name,
                "contract_percent": contract_rate,
                "business_percent": business_rate,
                "delta_pp": delta_pp,
                "effect_money": effect,
            })
    items.sort(key=lambda item: _to_float(item.get("effect_money")), reverse=True)
    return {"total_money": round_money(sum(_to_float(item.get("effect_money")) for item in items)), "items": items}


def _decision_workspace_category_intelligence(
    all_block: List[Dict[str, Any]],
    rows: Optional[List[Dict[str, Any]]] = None,
    all_rows: Optional[List[Dict[str, Any]]] = None,
    period: str = "",
    network: str = "",
) -> List[Dict[str, Any]]:
    rows = rows or []
    all_rows = all_rows or []
    contract_revenue = _sum_metric(rows, "revenue")
    contract_finrez = _sum_metric(rows, "finrez_pre")
    business_period_rows = [row for row in all_rows if clean_text(row.get("period")) == clean_text(period)]
    business_revenue = _sum_metric(business_period_rows, "revenue")
    business_finrez = _sum_metric(business_period_rows, "finrez_pre")

    all_by_name = {clean_text(item.get("object_name")): item for item in (all_block or []) if isinstance(item, dict)}
    categories: List[Dict[str, Any]] = []
    for category, category_rows in _group_rows_by(rows, "category").items():
        business_category_rows = [row for row in business_period_rows if clean_text(row.get("category")) == category]
        revenue = _sum_metric(category_rows, "revenue")
        finrez = _sum_metric(category_rows, "finrez_pre")
        business_category_revenue = _sum_metric(business_category_rows, "revenue")
        business_category_finrez = _sum_metric(business_category_rows, "finrez_pre")
        linked = all_by_name.get(category, {})
        potential = _category_potential_breakdown(category_rows, business_category_rows)
        categories.append({
            "category": category,
            "revenue": round_money(revenue),
            "finrez_pre": round_money(finrez),
            "profit_delta_money": round_money(_to_float(linked.get("profit_delta_money"))),
            "opportunity_money": round_money(_to_float(linked.get("opportunity_money")) or _to_float(potential.get("total_money"))),
            "navigation_money": round_money(_to_float(linked.get("navigation_money"))),
            "share_contract_revenue_percent": round_percent((revenue / contract_revenue) * 100.0) if contract_revenue else 0.0,
            "share_contract_profit_percent": round_percent((finrez / contract_finrez) * 100.0) if abs(contract_finrez) > 1e-9 else 0.0,
            "share_business_revenue_percent": round_percent((business_category_revenue / business_revenue) * 100.0) if business_revenue else 0.0,
            "share_business_profit_percent": round_percent((business_category_finrez / business_finrez) * 100.0) if abs(business_finrez) > 1e-9 else 0.0,
            "potential_breakdown": potential,
        })

    # Keep compatibility if the drilldown already has categories but current rows were unavailable.
    if not categories:
        for item in all_block or []:
            if not isinstance(item, dict):
                continue
            name = item.get("object_name")
            if not name:
                continue
            categories.append({
                "category": name,
                "profit_delta_money": round_money(_to_float(item.get("profit_delta_money"))),
                "opportunity_money": round_money(_to_float(item.get("opportunity_money"))),
                "navigation_money": round_money(_to_float(item.get("navigation_money"))),
                "potential_breakdown": {"total_money": round_money(_to_float(item.get("opportunity_money"))), "items": []},
            })
    categories.sort(key=lambda item: _to_float(item.get("navigation_money")) or _to_float(item.get("profit_delta_money")), reverse=True)
    return categories[:10]

def _sum_metric(rows: List[Dict[str, Any]], field: str) -> float:
    return sum(_to_float(row.get(field)) for row in rows or [])


def _group_rows_by(rows: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows or []:
        key = clean_text(row.get(field))
        if not key or key.lower() in {"без sku", "без sku "}:
            continue
        grouped.setdefault(key, []).append(row)
    return grouped


def _sku_role(revenue: float, share_network: float, share_business: float, finrez_pre: float = 0.0, profit_delta: float = 0.0, potential: float = 0.0) -> str:
    """Assign a business role to SKU using only DATA-derived metrics."""
    if share_network >= 20:
        return "якорная позиция контракта"
    if finrez_pre > 0 and profit_delta > 0 and share_network >= 8:
        return "лидер роста и прибыли"
    if share_business >= 5:
        return "значимая позиция бизнеса"
    if potential > 0:
        return "позиция с потенциалом"
    if revenue > 0 and finrez_pre < 0:
        return "позиция риска"
    if revenue > 0:
        return "рабочая позиция"
    return "низкая роль"


def _sku_development_logic(role: str, share_network: float, share_business: float, profit_delta: float) -> str:
    if role == "якорная позиция контракта":
        return "сохранять представленность и использовать как доказательство силы матрицы"
    if role == "лидер роста и прибыли":
        return "масштабировать в переговорах и защищать условия"
    if role == "значимая позиция бизнеса":
        return "проверить, полностью ли раскрыта роль позиции в контракте"
    if role == "позиция риска":
        return "проверить цену, условия и целесообразность сохранения"
    if profit_delta > 0:
        return "оставить в пакете развития как подтверждённый источник роста"
    return "использовать как вспомогательную позицию, не как главный аргумент"


def _format_from_product_text(value: Any) -> str:
    text = str(value or "")
    normalized = text.lower().replace(".", ",").replace(" ", "")
    for token, label in (
        ("1,5л", "1,5 л"), ("0,75л", "0,75 л"), ("0,5л", "0,5 л"),
        ("5л", "5 л"), ("2л", "2 л"), ("1л", "1 л"),
    ):
        if token in normalized:
            return label
    return "формат не определён"


def _rank_sku_by_metric(grouped_sku_rows: Dict[str, List[Dict[str, Any]]], sku: str, metric: str) -> Optional[int]:
    items = []
    for name, rows in grouped_sku_rows.items():
        value = _sum_metric(rows, metric)
        items.append((name, value))
    items.sort(key=lambda pair: pair[1], reverse=True)
    for idx, (name, _value) in enumerate(items, start=1):
        if clean_text(name) == clean_text(sku):
            return idx
    return None


def _build_sku_passport(
    sku: str,
    period: str,
    rows: List[Dict[str, Any]],
    prev_rows: List[Dict[str, Any]],
    all_rows: List[Dict[str, Any]],
    filter_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Build SKU Passport from DATA only.

    SKU is the primary business object in VECTRA. This passport exposes the
    product evidence available today without inventing price, stock, promo or
    shelf data.
    """
    filter_payload = dict(filter_payload or {})
    sku_name = clean_text(sku) or _first_nonempty(rows, "sku")
    business_rows = [row for row in (all_rows or []) if clean_text(row.get("period")) == clean_text(period)]
    business_sku_rows = [row for row in business_rows if clean_text(row.get("sku")) == sku_name]
    business_total = _sum_metric(business_rows, "revenue")
    sku_business_revenue = _sum_metric(business_sku_rows, "revenue")
    sku_business_finrez = _sum_metric(business_sku_rows, "finrez_pre")
    category = _first_nonempty(rows, "category") or _first_nonempty(business_sku_rows, "category")
    tmc_group = _first_nonempty(rows, "tmc_group") or _first_nonempty(business_sku_rows, "tmc_group")
    network = filter_payload.get("network") or _first_nonempty(rows, "network")

    category_rows = [row for row in business_rows if clean_text(row.get("category")) == clean_text(category)] if category else []
    tmc_rows = [row for row in business_rows if clean_text(row.get("tmc_group")) == clean_text(tmc_group)] if tmc_group else []
    category_revenue = _sum_metric(category_rows, "revenue")
    tmc_revenue = _sum_metric(tmc_rows, "revenue")

    current_metrics = aggregate_metrics(rows) if rows else {}
    previous_metrics = aggregate_metrics(prev_rows) if prev_rows else {}
    revenue = _to_float(current_metrics.get("revenue"))
    finrez_pre = _to_float(current_metrics.get("finrez_pre"))
    prev_revenue = _to_float(previous_metrics.get("revenue"))
    prev_finrez_pre = _to_float(previous_metrics.get("finrez_pre"))
    profit_delta = finrez_pre - prev_finrez_pre

    grouped_business_skus = _group_rows_by(business_rows, "sku")
    grouped_category_skus = _group_rows_by(category_rows, "sku") if category_rows else {}
    grouped_format_skus = _group_rows_by(tmc_rows, "sku") if tmc_rows else {}

    sku_networks = sorted({clean_text(row.get("network")) for row in business_sku_rows if clean_text(row.get("network"))})
    all_networks = sorted({clean_text(row.get("network")) for row in business_rows if clean_text(row.get("network"))})
    missing_networks = [name for name in all_networks if name not in set(sku_networks)]

    network_groups = _group_rows_by(business_sku_rows, "network")
    top_networks = []
    for net_name, net_rows in network_groups.items():
        net_revenue = _sum_metric(net_rows, "revenue")
        top_networks.append({
            "network": net_name,
            "revenue": round_money(net_revenue),
            "finrez_pre": round_money(_sum_metric(net_rows, "finrez_pre")),
            "share_sku_percent": round_percent((net_revenue / sku_business_revenue) * 100.0) if sku_business_revenue else 0.0,
        })
    top_networks.sort(key=lambda item: _to_float(item.get("revenue")), reverse=True)

    contract_share = round_percent((revenue / sku_business_revenue) * 100.0) if sku_business_revenue else 0.0
    business_share = round_percent((sku_business_revenue / business_total) * 100.0) if business_total else 0.0
    category_share = round_percent((sku_business_revenue / category_revenue) * 100.0) if category_revenue else 0.0
    tmc_share = round_percent((sku_business_revenue / tmc_revenue) * 100.0) if tmc_revenue else 0.0
    role = _sku_role(sku_business_revenue, contract_share, business_share, sku_business_finrez, profit_delta)

    return {
        "title": "Паспорт SKU",
        "sku": sku_name,
        "period": period,
        "contract": network,
        "identification": {
            "category": category,
            "tmc_group": tmc_group,
            "format": _format_from_product_text(tmc_group or sku_name),
        },
        "business_role": {
            "business_share_percent": business_share,
            "category_share_percent": category_share,
            "tmc_group_share_percent": tmc_share,
            "contract_share_of_sku_business_percent": contract_share,
            "business_revenue": round_money(sku_business_revenue),
            "business_finrez_pre": round_money(sku_business_finrez),
            "rank_revenue_business": _rank_sku_by_metric(grouped_business_skus, sku_name, "revenue"),
            "rank_profit_business": _rank_sku_by_metric(grouped_business_skus, sku_name, "finrez_pre"),
            "rank_revenue_category": _rank_sku_by_metric(grouped_category_skus, sku_name, "revenue") if grouped_category_skus else None,
            "rank_revenue_format": _rank_sku_by_metric(grouped_format_skus, sku_name, "revenue") if grouped_format_skus else None,
            "network_count": len(sku_networks),
            "total_network_count": len(all_networks),
            "role": role,
        },
        "economics": {
            "revenue": round_money(revenue),
            "previous_revenue": round_money(prev_revenue),
            "finrez_pre": round_money(finrez_pre),
            "previous_finrez_pre": round_money(prev_finrez_pre),
            "profit_delta_money": round_money(profit_delta),
            "margin_pre_percent": round_percent(_to_float(current_metrics.get("margin_pre"))),
            "markup_percent": round_percent(_to_float(current_metrics.get("markup"))),
        },
        "presence": {
            "networks": sku_networks,
            "missing_networks": missing_networks[:20],
            "top_networks": top_networks[:10],
        },
        "decision": {
            "development_logic": _sku_development_logic(role, contract_share, business_share, profit_delta),
            "recommended_action": "использовать паспорт SKU как доказательную базу для развития, ввода или защиты позиции",
            "data_limitations": [
                "цены, остатки, полка, промо и конкуренты будут доступны после расширения Data Mart"
            ],
        },
    }


def _build_contract_assortment_analysis(rows: List[Dict[str, Any]], all_rows: List[Dict[str, Any]], period: str) -> Dict[str, Any]:
    """Build Contract Workspace assortment evidence from DATA rows.

    This does not invent GPT blocks: it only groups already loaded DATA rows for
    the current period into SKU evidence required by the product model.
    """
    contract_revenue = _sum_metric(rows, "revenue")
    business_period_rows = [row for row in (all_rows or []) if clean_text(row.get("period")) == clean_text(period)]
    prev_period = _previous_year_period(period)
    network_name = clean_text(rows[0].get("network")) if rows else ""
    prev_contract_rows = [
        row for row in (all_rows or [])
        if prev_period and clean_text(row.get("period")) == clean_text(prev_period) and clean_text(row.get("network")) == network_name
    ]
    business_revenue = _sum_metric(business_period_rows, "revenue")
    network_sku_names = {clean_text(row.get("sku")) for row in rows or [] if clean_text(row.get("sku"))}

    sku_leaders: List[Dict[str, Any]] = []
    for sku, sku_rows in _group_rows_by(rows, "sku").items():
        revenue = _sum_metric(sku_rows, "revenue")
        finrez_pre = _sum_metric(sku_rows, "finrez_pre")
        sku_business_revenue = _sum_metric([r for r in business_period_rows if clean_text(r.get("sku")) == sku], "revenue")
        prev_sku_rows = [r for r in prev_contract_rows if clean_text(r.get("sku")) == sku]
        prev_finrez_pre = _sum_metric(prev_sku_rows, "finrez_pre")
        profit_delta = finrez_pre - prev_finrez_pre
        share_network = round_percent((revenue / contract_revenue) * 100.0) if contract_revenue else 0.0
        share_business = round_percent((sku_business_revenue / business_revenue) * 100.0) if business_revenue else 0.0
        role = _sku_role(revenue, share_network, share_business, finrez_pre=finrez_pre, profit_delta=profit_delta)
        sku_leaders.append({
            "sku": sku,
            "revenue": round_money(revenue),
            "finrez_pre": round_money(finrez_pre),
            "profit_delta_money": round_money(profit_delta),
            "share_network_percent": share_network,
            "share_business_percent": share_business,
            "role": role,
            "development_logic": _sku_development_logic(role, share_network, share_business, profit_delta),
        })
    sku_leaders.sort(key=lambda item: _to_float(item.get("revenue")), reverse=True)

    missing_business_leaders: List[Dict[str, Any]] = []
    for sku, sku_rows in _group_rows_by(business_period_rows, "sku").items():
        if sku in network_sku_names:
            continue
        revenue = _sum_metric(sku_rows, "revenue")
        finrez_pre = _sum_metric(sku_rows, "finrez_pre")
        if revenue <= 0:
            continue
        missing_business_leaders.append({
            "sku": sku,
            "business_revenue": round_money(revenue),
            "business_finrez_pre": round_money(finrez_pre),
            "status": "отсутствует в контракте",
            "role": "отсутствующий лидер бизнеса",
            "reason": "Лидер бизнеса отсутствует в текущем контракте и может стать кандидатом для первой волны развития.",
            "development_logic": "проверить ввод в матрицу как подтверждённую возможность роста",
        })
    missing_business_leaders.sort(key=lambda item: _to_float(item.get("business_revenue")), reverse=True)

    top5_share = sum(_to_float(item.get("share_network_percent")) for item in sku_leaders[:5])
    concentration_level = "high" if top5_share >= 65 else ("medium" if top5_share >= 45 else "balanced")
    sku_intelligence = {
        "top5_share_percent": round_percent(top5_share),
        "concentration_level": concentration_level,
        "leader_count": len(sku_leaders),
        "missing_count": len(missing_business_leaders),
        "development_plan": [
            "сохранить и защитить позиции-лидеры текущего контракта",
            "собрать первую волну из отсутствующих лидеров бизнеса",
            "после выбора пакета перейти к переговорной позиции и задачам",
        ],
    }
    return {
        "sku_leaders_contract": sku_leaders[:10],
        "missing_business_sku_leaders": missing_business_leaders[:10],
        "contract_sku_count": len(sku_leaders),
        "missing_business_leader_count": len(missing_business_leaders),
        "sku_intelligence": sku_intelligence,
    }



def _build_decision_workspace(
    object_name: str,
    period: str,
    current_metrics: Dict[str, Any],
    prev_metrics: Dict[str, Any],
    business_metrics: Dict[str, Any],
    structure: Dict[str, Any],
    all_block: List[Dict[str, Any]],
    decision_block: List[Dict[str, Any]],
    assortment_analysis: Optional[Dict[str, Any]] = None,
    rows_for_intelligence: Optional[List[Dict[str, Any]]] = None,
    all_rows_for_intelligence: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    """Stage 8.1: stable Decision Engine contract for Network.

    This function does not replace existing Object Screen calculations. It only
    groups already calculated API data into the future Decision Workspace shape,
    so Custom GPT and code can evolve against one explicit contract.
    """
    factor_items = _decision_workspace_factor_items(structure)
    recommended_actions = _decision_workspace_recommended_actions(decision_block, structure)
    category_intelligence = _decision_workspace_category_intelligence(all_block, rows=rows_for_intelligence, all_rows=all_rows_for_intelligence, period=period, network=object_name)
    assortment_analysis = assortment_analysis or {}
    negotiation_package = {
        "goal": "подготовить развитие контракта на основании экономики, категорий и позиций",
        "priority_categories": [item.get("category") for item in category_intelligence[:3] if item.get("category")],
        "sku_package": [item.get("sku") for item in (assortment_analysis.get("missing_business_sku_leaders") or [])[:5] if item.get("sku")],
        "arguments": [
            "категории и позиции подтверждены данными текущего периода",
            "экономика контракта сравнивается со средним уровнем бизнеса",
        ],
        "risks": [],
    }
    return {
        "type": "network_contract_workspace",
        "stage": "8.4",
        "rendering_priority": "contract_workspace",
        "legacy_object_screen_allowed": True,
        "workspace_policy": "full_contract_desktop",
        "contract": clean_text(object_name),
        "period": period,
        "main_question": "Что происходит с нашим бизнесом с этим контрагентом и какие направления работы доступны KAM?",
        "contract_diagnostics": {
            "summary": _decision_workspace_summary_text(current_metrics, prev_metrics),
            "profit_delta_money": _profit_delta_money(current_metrics, prev_metrics),
            "revenue_current": round_money(_to_float(current_metrics.get("revenue"))),
            "revenue_previous_year": round_money(_to_float(prev_metrics.get("revenue"))),
            "margin_current_percent": round_percent(_to_float(current_metrics.get("margin_pre"))),
            "margin_business_percent": round_percent(_to_float(business_metrics.get("margin_pre"))),
        },
        "product_diagnostics": {
            "summary": "Продуктовая детализация используется как доказательная база решения, а не как обязательный следующий экран.",
            "evidence_levels": ["category", "tmc_group", "sku"],
            "category_count": len(category_intelligence),
        },
        "category_intelligence": category_intelligence,
        "assortment_analysis": assortment_analysis,
        "profit_potential": {"opportunity_money": None},
        "negotiation_package": negotiation_package,
        "tasks": [],
        "recommended_actions": recommended_actions,
        "evidence": {
            "navigation_mode": "evidence",
            "items": category_intelligence[:3],
        },
    }


def _attach_business_contract(payload: Dict[str, Any], current_metrics: Dict[str, Any], prev_metrics: Dict[str, Any], structure: Dict[str, Any], all_block: List[Dict[str, Any]], reasons_block: List[Dict[str, Any]]) -> None:
    """Stage 3: canonical Business Screen data contract, independent of Goal/Focus."""
    payload["business_result_money"] = _business_result_money(current_metrics, prev_metrics)
    payload["business_result_rating"] = _business_result_rating(all_block)
    payload["profit_loss_rating"] = _profit_loss_rating(all_block)
    payload["opportunity_rating"] = _opportunity_rating(all_block)
    payload["business_reasons"] = reasons_block
    payload["priority_action"] = _priority_action(structure)

    # Business Contract must not expose legacy Goal/Focus fields.
    for key in (
        "goal",
        "goal_block",
        "focus_money",
        "coverage",
        "coverage_percent",
        "vector_block",
        "path_goal",
        "path_goal_money",
    ):
        payload.pop(key, None)




def _attach_object_contract(payload: Dict[str, Any], structure: Dict[str, Any], reasons_block: List[Dict[str, Any]]) -> None:
    """Stage 4: canonical Object Screen data contract.

    Applies to every management object below Business:
    manager_top, manager, network, category, tmc_group, sku.

    Object Result, Opportunity Money and Navigation Money are independent
    entities locked by _attach_money_contract(). This function only exposes
    the object contract and must not recalculate or mix them.
    """
    payload["object_reasons"] = reasons_block
    payload["priority_action"] = _priority_action(structure)

    # Object Contract must not expose legacy Goal/Focus fields.
    # Legacy aliases may exist elsewhere for old renderers, but not in the
    # canonical object contract returned by the domain summary.
    for key in (
        "goal",
        "goal_block",
        "focus_money",
        "coverage",
        "coverage_percent",
        "vector_block",
        "path_goal",
        "path_goal_money",
    ):
        payload.pop(key, None)




def _count_distinct(rows: List[Dict[str, Any]], field: str) -> int:
    values = set()
    for row in rows or []:
        value = clean_text(row.get(field))
        if value:
            values.add(value)
    return len(values)


def _build_structural_analysis(level: str, rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Compare object structure before interpreting financial changes.

    This is a DATA-only layer. It does not use external context and does not
    infer business reasons. It only counts the current object composition vs
    previous year so the assistant can avoid false financial interpretation
    when the compared object itself changed.
    """
    fields_by_level = {
        'business': [('manager_top', 'Топ-менеджеры'), ('manager', 'Менеджеры'), ('network', 'Сети'), ('category', 'Категории'), ('tmc_group', 'Группы ТМС'), ('sku', 'Позиции')],
        'manager_top': [('manager', 'Менеджеры'), ('network', 'Сети'), ('category', 'Категории'), ('tmc_group', 'Группы ТМС'), ('sku', 'Позиции')],
        'manager': [('network', 'Сети'), ('category', 'Категории'), ('tmc_group', 'Группы ТМС'), ('sku', 'Позиции')],
        'network': [('category', 'Категории'), ('tmc_group', 'Группы ТМС'), ('sku', 'Позиции')],
        'category': [('tmc_group', 'Группы ТМС'), ('sku', 'Позиции'), ('network', 'Сети')],
        'tmc_group': [('sku', 'Позиции'), ('network', 'Сети')],
        'sku': [('network', 'Сети')],
    }.get(level, [])

    items: List[Dict[str, Any]] = []
    material_delta = False
    for field, label in fields_by_level:
        current_count = _count_distinct(rows, field)
        previous_count = _count_distinct(prev_rows, field)
        delta = current_count - previous_count
        if previous_count or current_count:
            if abs(delta) >= 1:
                material_delta = True
            items.append({
                'field': field,
                'name': label,
                'current': current_count,
                'previous_year': previous_count,
                'delta': delta,
            })

    return {
        'status': 'changed' if material_delta else 'stable',
        'is_material': material_delta,
        'items': items,
    }



def _format_distribution(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """DATA-derived product format distribution.

    Format is not a physical DATA column yet, so it is derived from tmc_group/sku
    using the same deterministic parser used by SKU Passport.
    """
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows or []:
        fmt = _format_from_product_text(row.get("tmc_group") or row.get("sku"))
        if not fmt or fmt == "формат не определён":
            continue
        grouped.setdefault(fmt, []).append(row)
    total_revenue = _sum_metric(rows, "revenue")
    total_finrez = _sum_metric(rows, "finrez_pre")
    items: List[Dict[str, Any]] = []
    for fmt, fmt_rows in grouped.items():
        revenue = _sum_metric(fmt_rows, "revenue")
        finrez = _sum_metric(fmt_rows, "finrez_pre")
        items.append({
            "format": fmt,
            "revenue": round_money(revenue),
            "finrez_pre": round_money(finrez),
            "share_revenue_percent": round_percent((revenue / total_revenue) * 100.0) if total_revenue else 0.0,
            "share_profit_percent": round_percent((finrez / total_finrez) * 100.0) if abs(total_finrez) > 1e-9 else 0.0,
            "sku_count": _count_distinct(fmt_rows, "sku"),
            "network_count": _count_distinct(fmt_rows, "network"),
        })
    items.sort(key=lambda item: _to_float(item.get("revenue")), reverse=True)
    return items


def _build_business_context(
    level: str,
    object_name: str,
    period: str,
    rows: List[Dict[str, Any]],
    business_rows: List[Dict[str, Any]],
    current_metrics: Dict[str, Any],
    business_metrics: Dict[str, Any],
    structure: Dict[str, Any],
    compare_base: str,
) -> Dict[str, Any]:
    """Business Context Engine v1.

    Creates a stable, DATA-only comparison contract for every non-business
    object. This is the layer that prevents Workspace from becoming local-only.
    """
    if level == "business":
        return {
            "type": "business_root",
            "message": "Business является верхним уровнем и не сравнивается с самим собой.",
            "compare_base": "previous_year",
        }

    object_revenue = _to_float(current_metrics.get("revenue"))
    object_profit = _to_float(current_metrics.get("finrez_pre"))
    business_revenue = _to_float(business_metrics.get("revenue"))
    business_profit = _to_float(business_metrics.get("finrez_pre"))
    object_margin = _to_float(current_metrics.get("margin_pre"))
    business_margin = _to_float(business_metrics.get("margin_pre"))
    object_markup = _safe_markup_percent(current_metrics)
    business_markup = _safe_markup_percent(business_metrics)

    factor_items: List[Dict[str, Any]] = []
    for key in ["markup", "retro", "logistics", "personnel", "other"]:
        item = structure.get(key) if isinstance(structure, dict) else {}
        if not isinstance(item, dict):
            continue
        factor_items.append({
            "factor": key,
            "name": DECISION_FACTOR_LABELS.get(key, key),
            "object_percent": round_percent(_to_float(item.get("fact_percent"))),
            "business_percent": round_percent(_to_float(item.get("base_percent"))),
            "delta_pp": round_percent(_to_float(item.get("delta_percent"))),
            "effect_money": round_money(_to_float(item.get("effect_money"))),
            "signal": "risk" if _to_float(item.get("effect_money")) < 0 else "strength",
        })
    factor_items.sort(key=lambda item: abs(_to_float(item.get("effect_money"))), reverse=True)

    object_formats = _format_distribution(rows)
    business_formats = _format_distribution(business_rows)
    object_format_names = {str(item.get("format")) for item in object_formats if item.get("format")}
    missing_formats = [item for item in business_formats if item.get("format") and str(item.get("format")) not in object_format_names]

    return {
        "type": "business_context",
        "level": level,
        "object_name": _human_object_name(level, object_name),
        "period": period,
        "compare_base": compare_base,
        "kpi": {
            "revenue_share_business_percent": round_percent((object_revenue / business_revenue) * 100.0) if business_revenue else 0.0,
            "profit_share_business_percent": round_percent((object_profit / business_profit) * 100.0) if abs(business_profit) > 1e-9 else 0.0,
            "margin_object_percent": round_percent(object_margin),
            "margin_business_percent": round_percent(business_margin),
            "margin_delta_pp": round_percent(object_margin - business_margin),
            "markup_object_percent": round_percent(object_markup),
            "markup_business_percent": round_percent(business_markup),
            "markup_delta_pp": round_percent(object_markup - business_markup),
        },
        "structure": {
            "object_category_count": _count_distinct(rows, "category"),
            "business_category_count": _count_distinct(business_rows, "category"),
            "object_tmc_group_count": _count_distinct(rows, "tmc_group"),
            "business_tmc_group_count": _count_distinct(business_rows, "tmc_group"),
            "object_sku_count": _count_distinct(rows, "sku"),
            "business_sku_count": _count_distinct(business_rows, "sku"),
            "object_network_count": _count_distinct(rows, "network"),
            "business_network_count": _count_distinct(business_rows, "network"),
        },
        "factors": factor_items,
        "formats": {
            "object": object_formats,
            "business_reference": business_formats,
            "missing_business_formats": missing_formats,
        },
        "summary": {
            "is_margin_above_business": object_margin >= business_margin,
            "is_markup_above_business": object_markup >= business_markup,
            "has_missing_formats": bool(missing_formats),
            "largest_factor": factor_items[0] if factor_items else None,
        },
    }


def _build_category_workspace(
    category: str,
    period: str,
    rows: List[Dict[str, Any]],
    prev_rows: List[Dict[str, Any]],
    business_rows: List[Dict[str, Any]],
    business_context: Dict[str, Any],
) -> Dict[str, Any]:
    current_metrics = aggregate_metrics(rows)
    prev_metrics = aggregate_metrics(prev_rows)
    profit_delta = _profit_delta_money(current_metrics, prev_metrics)
    formats = _format_distribution(rows)
    business_formats = _format_distribution(business_rows)
    object_names = {str(item.get("format")) for item in formats if item.get("format")}
    missing_formats = [item for item in business_formats if item.get("format") and str(item.get("format")) not in object_names]

    sku_items: List[Dict[str, Any]] = []
    category_revenue = _sum_metric(rows, "revenue")
    business_revenue = _sum_metric(business_rows, "revenue")
    current_skus = set()
    for sku_name, sku_rows in _group_rows_by(rows, "sku").items():
        current_skus.add(sku_name)
        revenue = _sum_metric(sku_rows, "revenue")
        finrez = _sum_metric(sku_rows, "finrez_pre")
        business_sku_rows = [r for r in business_rows if clean_text(r.get("sku")) == sku_name]
        business_sku_revenue = _sum_metric(business_sku_rows, "revenue")
        sku_items.append({
            "sku": sku_name,
            "revenue": round_money(revenue),
            "finrez_pre": round_money(finrez),
            "share_category_percent": round_percent((revenue / category_revenue) * 100.0) if category_revenue else 0.0,
            "share_business_category_percent": round_percent((business_sku_revenue / business_revenue) * 100.0) if business_revenue else 0.0,
            "network_count": _count_distinct(sku_rows, "network"),
            "format": _format_from_product_text(_first_nonempty(sku_rows, "tmc_group") or sku_name),
        })
    sku_items.sort(key=lambda item: _to_float(item.get("revenue")), reverse=True)

    missing_skus: List[Dict[str, Any]] = []
    for sku_name, sku_rows in _group_rows_by(business_rows, "sku").items():
        if sku_name in current_skus:
            continue
        revenue = _sum_metric(sku_rows, "revenue")
        finrez = _sum_metric(sku_rows, "finrez_pre")
        if revenue <= 0:
            continue
        missing_skus.append({
            "sku": sku_name,
            "business_revenue": round_money(revenue),
            "business_finrez_pre": round_money(finrez),
            "format": _format_from_product_text(_first_nonempty(sku_rows, "tmc_group") or sku_name),
            "reason": "позиция присутствует в бизнес-референсе категории, но отсутствует в текущем объекте",
        })
    missing_skus.sort(key=lambda item: _to_float(item.get("business_revenue")), reverse=True)

    return {
        "type": "category_workspace",
        "category": clean_text(category),
        "period": period,
        "profit_delta_money": round_money(profit_delta),
        "business_context": business_context,
        "formats": formats,
        "business_formats": business_formats,
        "missing_business_formats": missing_formats,
        "sku_leaders": sku_items[:12],
        "missing_business_sku_leaders": missing_skus[:12],
        "strategy": {
            "main_focus": "развивать категорию через форматы и SKU, где текущий объект отстаёт от бизнес-референса",
            "format_gap_exists": bool(missing_formats),
            "sku_gap_count": len(missing_skus),
        },
    }


# Sprint 8.3 — Product Intelligence Engine
# DATA-only layer that turns Business Context + Product evidence into stable
# recommendations, opportunity priorities and narrative blocks. It does not
# calculate new source KPI; it derives structured interpretation from existing
# aggregate metrics, structure, assortment and Business Context.

def _opportunity_level_from_value(value: float) -> str:
    value = abs(_to_float(value))
    if value >= 150000:
        return "high"
    if value >= 50000:
        return "medium"
    if value > 0:
        return "low"
    return "none"




def _money_text_ru(value: Any) -> str:
    value = round_money(_to_float(value))
    sign = "−" if value < 0 else ""
    return f"{sign}{int(round(abs(value))):,}".replace(",", " ")

def _build_business_opportunity_engine(
    level: str,
    rows: List[Dict[str, Any]],
    business_rows: List[Dict[str, Any]],
    business_context: Dict[str, Any],
    structure: Dict[str, Any],
    current_metrics: Dict[str, Any],
    prev_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    opportunities: List[Dict[str, Any]] = []
    bc_summary = business_context.get("summary") if isinstance(business_context, dict) else {}
    bc_formats = business_context.get("formats") if isinstance(business_context, dict) else {}
    missing_formats = bc_formats.get("missing_business_formats") if isinstance(bc_formats, dict) else []

    # Factor opportunities: negative vs-business effect means money is worse
    # than the Business Reference and therefore a confirmed economic reserve.
    for item in (business_context.get("factors") or []) if isinstance(business_context, dict) else []:
        if not isinstance(item, dict):
            continue
        effect = _to_float(item.get("effect_money"))
        if effect < 0:
            opportunities.append({
                "type": "factor_gap",
                "object": item.get("name") or item.get("factor"),
                "effect_money": round_money(abs(effect)),
                "priority": _opportunity_level_from_value(effect),
                "reason": "хуже среднего уровня бизнеса",
                "recommended_action": f"проработать {str(item.get('name') or item.get('factor') or '').lower()} относительно бизнес-референса",
            })

    # Format opportunities: business has product formats that the current object
    # does not yet use. This is Product Intelligence, not a UI-only list.
    if isinstance(missing_formats, list):
        for fmt in missing_formats[:8]:
            if not isinstance(fmt, dict):
                continue
            revenue = _to_float(fmt.get("revenue"))
            if revenue <= 0:
                continue
            opportunities.append({
                "type": "missing_format",
                "object": fmt.get("format"),
                "effect_money": round_money(revenue),
                "priority": _opportunity_level_from_value(revenue),
                "reason": "формат присутствует в бизнесе, но отсутствует в текущем объекте",
                "recommended_action": "проверить возможность развития формата перед выбором SKU",
            })

    # SKU opportunities: for network/category/tmc layer detect strongest business
    # SKU absent from the current object. Uses current DATA only.
    current_skus = {clean_text(r.get("sku")) for r in rows or [] if clean_text(r.get("sku"))}
    absent_sku_candidates: List[Dict[str, Any]] = []
    for sku_name, sku_rows in _group_rows_by(business_rows, "sku").items():
        if sku_name in current_skus:
            continue
        revenue = _sum_metric(sku_rows, "revenue")
        finrez = _sum_metric(sku_rows, "finrez_pre")
        if revenue <= 0:
            continue
        absent_sku_candidates.append({
            "type": "missing_sku",
            "object": sku_name,
            "effect_money": round_money(revenue),
            "business_finrez_pre": round_money(finrez),
            "priority": _opportunity_level_from_value(revenue),
            "reason": "SKU является частью бизнес-референса, но отсутствует в текущем объекте",
            "recommended_action": "добавить в пакет развития после проверки формата и полки",
        })
    absent_sku_candidates.sort(key=lambda x: _to_float(x.get("effect_money")), reverse=True)
    if level in {"network", "category", "tmc_group"}:
        opportunities.extend(absent_sku_candidates[:8])

    # Structural opportunity/risk: current SKU structure materially narrower
    # than Business Reference.
    bc_structure = business_context.get("structure") if isinstance(business_context, dict) else {}
    if isinstance(bc_structure, dict):
        object_sku_count = int(_to_float(bc_structure.get("object_sku_count")))
        business_sku_count = int(_to_float(bc_structure.get("business_sku_count")))
        if business_sku_count and object_sku_count and object_sku_count < business_sku_count:
            opportunities.append({
                "type": "assortment_width_gap",
                "object": "ассортимент",
                "effect_money": round_money(business_sku_count - object_sku_count),
                "priority": "medium" if business_sku_count - object_sku_count >= 5 else "low",
                "reason": f"в объекте {object_sku_count} SKU против {business_sku_count} в бизнес-референсе",
                "recommended_action": "сравнить матрицу с бизнесом и выбрать приоритеты ввода",
            })

    opportunities.sort(key=lambda x: (_to_float(x.get("effect_money")), str(x.get("priority"))), reverse=True)
    return {
        "type": "business_opportunity_engine",
        "level": level,
        "items": opportunities[:12],
        "summary": {
            "total_items": len(opportunities),
            "has_factor_gap": any(x.get("type") == "factor_gap" for x in opportunities),
            "has_missing_sku": any(x.get("type") == "missing_sku" for x in opportunities),
            "has_missing_format": any(x.get("type") == "missing_format" for x in opportunities),
            "top_opportunity": opportunities[0] if opportunities else None,
        },
    }


def _build_recommendation_engine(
    level: str,
    object_name: str,
    business_context: Dict[str, Any],
    business_opportunity: Dict[str, Any],
    current_metrics: Dict[str, Any],
    prev_metrics: Dict[str, Any],
) -> Dict[str, Any]:
    recommendations: List[Dict[str, Any]] = []
    opportunities = business_opportunity.get("items") if isinstance(business_opportunity, dict) else []
    profit_delta = _profit_delta_money(current_metrics, prev_metrics)
    revenue_delta = _to_float(current_metrics.get("revenue")) - _to_float(prev_metrics.get("revenue"))

    for item in opportunities[:5] if isinstance(opportunities, list) else []:
        if not isinstance(item, dict):
            continue
        action = item.get("recommended_action") or "проверить возможность"
        recommendations.append({
            "action": action,
            "object": item.get("object"),
            "priority": item.get("priority") or "medium",
            "basis": item.get("reason"),
            "expected_effect_money": item.get("effect_money"),
            "source": item.get("type"),
        })

    if not recommendations:
        kpi = business_context.get("kpi") if isinstance(business_context, dict) else {}
        margin_delta = _to_float(kpi.get("margin_delta_pp")) if isinstance(kpi, dict) else 0.0
        if margin_delta >= 0 and profit_delta >= 0:
            recommendations.append({
                "action": "сохранить текущую модель и масштабировать сильные элементы",
                "object": _human_object_name(level, object_name),
                "priority": "medium",
                "basis": "прибыль и доходность не хуже бизнес-референса",
                "expected_effect_money": round_money(max(profit_delta, 0.0)),
                "source": "strength_protection",
            })
        elif revenue_delta < 0:
            recommendations.append({
                "action": "проверить потерю объёма и изменение структуры",
                "object": _human_object_name(level, object_name),
                "priority": "high",
                "basis": "оборот снизился относительно прошлого года",
                "expected_effect_money": round_money(abs(revenue_delta)),
                "source": "volume_drop",
            })

    return {
        "type": "recommendation_engine",
        "level": level,
        "object_name": _human_object_name(level, object_name),
        "items": recommendations[:5],
        "main_recommendation": recommendations[0] if recommendations else None,
    }


def _build_narrative_engine(
    level: str,
    object_name: str,
    current_metrics: Dict[str, Any],
    prev_metrics: Dict[str, Any],
    business_context: Dict[str, Any],
    business_opportunity: Dict[str, Any],
    recommendation_engine: Dict[str, Any],
) -> Dict[str, Any]:
    object_label = _human_object_name(level, object_name)
    profit_delta = round_money(_profit_delta_money(current_metrics, prev_metrics))
    revenue_delta = round_money(_to_float(current_metrics.get("revenue")) - _to_float(prev_metrics.get("revenue")))
    kpi = business_context.get("kpi") if isinstance(business_context, dict) else {}
    margin_delta = _to_float(kpi.get("margin_delta_pp")) if isinstance(kpi, dict) else 0.0
    top_opp = (business_opportunity.get("summary") or {}).get("top_opportunity") if isinstance(business_opportunity, dict) else None
    main_rec = recommendation_engine.get("main_recommendation") if isinstance(recommendation_engine, dict) else None

    if profit_delta > 0:
        happened = f"{object_label} улучшил финрез до на {_money_text_ru(profit_delta)} грн к прошлому году."
    elif profit_delta < 0:
        happened = f"{object_label} снизил финрез до на {_money_text_ru(abs(profit_delta))} грн к прошлому году."
    else:
        happened = f"{object_label} находится примерно на уровне прошлого года по финрезу до."

    why_parts = []
    if revenue_delta > 0:
        why_parts.append(f"оборот вырос на {_money_text_ru(revenue_delta)} грн")
    elif revenue_delta < 0:
        why_parts.append(f"оборот снизился на {_money_text_ru(abs(revenue_delta))} грн")
    if isinstance(kpi, dict) and kpi:
        why_parts.append(f"маржа относительно бизнеса: {round_percent(margin_delta):.2f} п.п.")
    why = "; ".join(why_parts) + "." if why_parts else "Причина требует проверки через структуру, факторы и Business Context."

    if isinstance(top_opp, dict):
        money = top_opp.get("effect_money")
        where_money = f"Главная подтверждённая возможность: {top_opp.get('object')} — {_money_text_ru(money)} грн / ед. эффекта, причина: {top_opp.get('reason')}."
    else:
        where_money = "Крупная подтверждённая возможность относительно бизнеса не выделена."

    action = main_rec.get("action") if isinstance(main_rec, dict) else "перейти к детализации и подтвердить следующий шаг"
    effect = main_rec.get("expected_effect_money") if isinstance(main_rec, dict) else None
    return {
        "type": "narrative_engine_v2",
        "level": level,
        "object_name": object_label,
        "what_happened": happened,
        "why": why,
        "what_it_means": where_money,
        "what_to_do": action,
        "expected_effect_money": effect,
        "sequence": ["what_happened", "why", "what_it_means", "what_to_do"],
    }


def _build_product_workspace_v2(
    level: str,
    object_name: str,
    period: str,
    business_context: Dict[str, Any],
    business_opportunity: Dict[str, Any],
    recommendation_engine: Dict[str, Any],
    narrative_engine: Dict[str, Any],
    sku_passport: Optional[Dict[str, Any]] = None,
    category_workspace: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    return {
        "type": "product_workspace_v2",
        "level": level,
        "object_name": _human_object_name(level, object_name),
        "period": period,
        "identity": (sku_passport or {}).get("identification") if isinstance(sku_passport, dict) else {},
        "business_context": business_context,
        "opportunities": business_opportunity,
        "recommendations": recommendation_engine,
        "narrative": narrative_engine,
        "sku_evidence": sku_passport or {},
        "category_evidence": category_workspace or {},
        "next_actions": [
            "подготовить переговоры",
            "создать задачу",
            "открыть витрину объекта",
        ],
    }



# Sprint 9 — Management Intelligence Layer
# DATA-only layer for owners of responsibility: Business, Top Manager, Manager.
# It does not replace Product Intelligence; it aggregates it into management
# radar, portfolio, priorities and control signals.

MANAGEMENT_LEVELS = {"business", "manager_top", "manager"}


def _management_child_field(level: str) -> Optional[str]:
    return {"business": "manager_top", "manager_top": "manager", "manager": "network"}.get(level)


def _management_role(level: str) -> str:
    return {
        "business": "коммерческий директор",
        "manager_top": "руководитель направления",
        "manager": "KAM / менеджер портфеля",
    }.get(level, "владелец объекта")


def _management_object_label(level: str) -> str:
    return {
        "business": "бизнес",
        "manager_top": "зона ответственности",
        "manager": "портфель клиентов",
    }.get(level, "объект")


def _portfolio_summary(level: str, rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    child = _management_child_field(level)
    child_label = {"manager_top": "топ-менеджеры", "manager": "менеджеры", "network": "контракты"}.get(child or "", "объекты")
    return {
        "responsibility_object": _management_object_label(level),
        "child_level": child,
        "child_label": child_label,
        "child_count": _count_distinct(rows, child) if child else 0,
        "child_count_previous_year": _count_distinct(prev_rows, child) if child else 0,
        "network_count": _count_distinct(rows, "network"),
        "network_count_previous_year": _count_distinct(prev_rows, "network"),
        "category_count": _count_distinct(rows, "category"),
        "category_count_previous_year": _count_distinct(prev_rows, "category"),
        "tmc_group_count": _count_distinct(rows, "tmc_group"),
        "sku_count": _count_distinct(rows, "sku"),
    }


def _management_child_items(level: str, rows: List[Dict[str, Any]], period: str, business_metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
    child = _management_child_field(level)
    if not child:
        return []
    prev_grouped = _previous_child_groups(level, child, rows, period)
    items: List[Dict[str, Any]] = []
    total_revenue = _sum_metric(rows, "revenue")
    total_profit = _sum_metric(rows, "finrez_pre")
    for name, child_rows in _group_rows_by(rows, child).items():
        metrics = aggregate_metrics(child_rows)
        prev_metrics = aggregate_metrics(prev_grouped.get(name, []))
        profit = _to_float(metrics.get("finrez_pre"))
        revenue = _to_float(metrics.get("revenue"))
        profit_delta = _profit_delta_money(metrics, prev_metrics)
        structure = _build_structure(metrics, business_metrics)
        opportunity = _opportunity_money(structure)
        items.append({
            "object_name": name,
            "level": child,
            "revenue": round_money(revenue),
            "finrez_pre": round_money(profit),
            "profit_delta_money": round_money(profit_delta),
            "share_revenue_percent": round_percent((revenue / total_revenue) * 100.0) if total_revenue else 0.0,
            "share_profit_percent": round_percent((profit / total_profit) * 100.0) if abs(total_profit) > 1e-9 else 0.0,
            "opportunity_money": round_money(opportunity),
            "network_count": _count_distinct(child_rows, "network"),
            "sku_count": _count_distinct(child_rows, "sku"),
            "risk_signal": "risk" if profit_delta < 0 else ("growth" if profit_delta > 0 else "stable"),
        })
    items.sort(key=lambda item: (_to_float(item.get("profit_delta_money")), -_to_float(item.get("opportunity_money"))))
    return items


def _management_radar(items: List[Dict[str, Any]]) -> Dict[str, Any]:
    risks = [item for item in items if _to_float(item.get("profit_delta_money")) < 0]
    growth = [item for item in items if _to_float(item.get("profit_delta_money")) > 0]
    opportunities = sorted(items, key=lambda item: _to_float(item.get("opportunity_money")), reverse=True)
    concentration = sorted(items, key=lambda item: abs(_to_float(item.get("share_profit_percent"))), reverse=True)
    return {
        "attention_required": risks[:5],
        "growth_practices": growth[:5],
        "largest_opportunities": opportunities[:5],
        "profit_concentration": concentration[:5],
        "summary": {
            "risk_count": len(risks),
            "growth_count": len(growth),
            "opportunity_count": len([x for x in opportunities if _to_float(x.get("opportunity_money")) > 0]),
        },
    }


def _management_priority_action(radar: Dict[str, Any], current_metrics: Dict[str, Any], prev_metrics: Dict[str, Any]) -> Dict[str, Any]:
    attention = radar.get("attention_required") if isinstance(radar.get("attention_required"), list) else []
    opportunities = radar.get("largest_opportunities") if isinstance(radar.get("largest_opportunities"), list) else []
    profit_delta = _profit_delta_money(current_metrics, prev_metrics)
    if attention:
        first = attention[0]
        return {
            "action": f"открыть {first.get('object_name')} и разобрать причину просадки",
            "object": first.get("object_name"),
            "level": first.get("level"),
            "priority": "high" if profit_delta < 0 else "medium",
            "basis": f"объект формирует Δ прибыли {round_money(_to_float(first.get('profit_delta_money')))}",
            "expected_effect_money": round_money(abs(_to_float(first.get("profit_delta_money"))) or _to_float(first.get("opportunity_money"))),
        }
    if opportunities and _to_float(opportunities[0].get("opportunity_money")) > 0:
        first = opportunities[0]
        return {
            "action": f"поставить задачу по резерву объекта {first.get('object_name')}",
            "object": first.get("object_name"),
            "level": first.get("level"),
            "priority": "medium",
            "basis": f"подтверждённый резерв {round_money(_to_float(first.get('opportunity_money')))}",
            "expected_effect_money": round_money(_to_float(first.get("opportunity_money"))),
        }
    return {
        "action": "сохранить текущую динамику и контролировать ключевые объекты",
        "priority": "normal",
        "basis": "критических управленческих просадок в текущем разрезе не выявлено",
        "expected_effect_money": 0,
    }


def _management_decision_chain(level: str, object_name: str, radar: Dict[str, Any], priority: Dict[str, Any]) -> List[Dict[str, Any]]:
    chain = [
        {"step": "diagnosis", "title": "Понять, где сформирован результат", "status": "ready"},
        {"step": "focus", "title": f"Сфокусироваться на объекте: {priority.get('object') or _human_object_name(level, object_name)}", "status": "ready"},
        {"step": "action", "title": priority.get("action"), "status": "draft"},
        {"step": "task", "title": "Создать задачу владельцу объекта", "status": "available"},
        {"step": "control", "title": "Проверить эффект в следующем периоде", "status": "planned"},
    ]
    return [x for x in chain if x.get("title")]


def _management_narrative(level: str, current_metrics: Dict[str, Any], prev_metrics: Dict[str, Any], radar: Dict[str, Any], priority: Dict[str, Any]) -> Dict[str, Any]:
    profit_delta = _profit_delta_money(current_metrics, prev_metrics)
    revenue_delta = _to_float(current_metrics.get("revenue")) - _to_float(prev_metrics.get("revenue"))
    summary = radar.get("summary") if isinstance(radar.get("summary"), dict) else {}
    return {
        "what_happened": f"Финрез до изменился на {round_money(profit_delta)}, оборот изменился на {round_money(revenue_delta)}.",
        "why_it_matters": f"В зоне ответственности найдено {summary.get('risk_count') or 0} объектов внимания и {summary.get('opportunity_count') or 0} объектов с подтверждённым резервом.",
        "management_focus": priority.get("action"),
        "control_logic": "приоритет должен завершаться задачей, ответственным и проверкой эффекта в следующем периоде",
    }


def _build_management_intelligence(
    level: str,
    object_name: str,
    period: str,
    rows: List[Dict[str, Any]],
    prev_rows: List[Dict[str, Any]],
    current_metrics: Dict[str, Any],
    prev_metrics: Dict[str, Any],
    business_metrics: Dict[str, Any],
    structural_analysis: Dict[str, Any],
    all_block: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if level not in MANAGEMENT_LEVELS:
        return {}
    child_items = _management_child_items(level, rows, period, business_metrics)
    radar = _management_radar(child_items)
    priority = _management_priority_action(radar, current_metrics, prev_metrics)
    passport = {
        "owner_role": _management_role(level),
        "object_name": _human_object_name(level, object_name),
        "period": period,
        "portfolio": _portfolio_summary(level, rows, prev_rows),
        "structural_analysis": structural_analysis,
        "management_objects": child_items[:12],
    }
    return {
        "type": "management_intelligence",
        "level": level,
        "object_name": _human_object_name(level, object_name),
        "period": period,
        "owner_role": _management_role(level),
        "passport": passport,
        "radar": radar,
        "priority_action": priority,
        "decision_chain": _management_decision_chain(level, object_name, radar, priority),
        "narrative": _management_narrative(level, current_metrics, prev_metrics, radar, priority),
        "workspace": {
            "type": "management_workspace_v1",
            "main_question": {
                "business": "Что сегодня требует внимания коммерческого директора?",
                "manager_top": "Как развивается зона ответственности руководителя?",
                "manager": "Что происходит с портфелем клиентов менеджера?",
            }.get(level, "Что требует управленческого внимания?"),
            "next_actions": [
                priority.get("action"),
                "открыть объект внимания",
                "создать задачу",
                "проверить результат в следующем периоде",
            ],
        },
    }


def _summary_from_rows(level: str, object_name: str, period: str, rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]], business_rows: List[Dict[str, Any]], compare_base: str = "business", parent_object: Optional[str] = None) -> Dict[str, Any]:
    current_metrics = aggregate_metrics(rows)
    prev_metrics = aggregate_metrics(prev_rows)
    business_metrics = aggregate_metrics(business_rows)
    base_metrics = prev_metrics if level == "business" else business_metrics
    all_block, all_next_level = _build_all_block(level, rows, business_metrics if business_rows else current_metrics, period)
    # Stage 2: Top-3 and "all" must use the same navigation source.
    drain_block = all_block[:3]
    adaptive_next_level = all_next_level
    structure = _build_structure(current_metrics, base_metrics)
    structural_analysis = _build_structural_analysis(level, rows, prev_rows)
    business_context = _build_business_context(level, object_name, period, rows, business_rows, current_metrics, business_metrics, structure, compare_base)
    for _item in structure.values():
        if isinstance(_item, dict):
            _item["base_type"] = "previous_year" if level == "business" else compare_base
    cause_block = _build_cause_block(structure, prev_metrics, current_metrics)

    payload = {
        "context": {
            "level": level,
            "object_name": _human_object_name(level, object_name),
            "period": period,
            "parent_object": parent_object,
            "compare_base": "previous_year" if level == "business" else compare_base,
            "previous_year_period": _previous_year_period(period),
        },
        "metrics": _build_metrics(current_metrics, prev_metrics, level),
        "navigation": {**_build_navigation(level, drain_block, adaptive_next_level, has_all=bool(all_block), has_causes=bool(cause_block)), "mode": "default", "vector": [item.get("object_name") for item in drain_block[:3]]},
        "all_block": all_block,
        "reasons_block": cause_block,
        # compatibility for orchestration/session internals
        "level": level,
        "object_name": object_name,
        "period": period,
        "children_level": adaptive_next_level if adaptive_next_level is not None else LEVEL_CHILD.get(level),
        "metrics_raw": current_metrics,
        "previous_object_metrics": prev_metrics,
        "business_metrics_raw": business_metrics,
        "filter": {"period": period},
        "compare_base": "previous_year" if level == "business" else compare_base,
        "structural_analysis": structural_analysis,
        "business_context": business_context,
    }
    if level in MANAGEMENT_LEVELS:
        management_intelligence = _build_management_intelligence(
            level=level,
            object_name=object_name,
            period=period,
            rows=rows,
            prev_rows=prev_rows,
            current_metrics=current_metrics,
            prev_metrics=prev_metrics,
            business_metrics=business_metrics,
            structural_analysis=structural_analysis,
            all_block=all_block,
        )
        payload["management_intelligence"] = management_intelligence
        payload["management_workspace"] = management_intelligence.get("workspace") or {}
        payload["management_passport"] = management_intelligence.get("passport") or {}

    product_tmc_decision = _product_tmc_decision(level, rows, _safe_get_rows(), period)
    if product_tmc_decision:
        payload["product_tmc_decision"] = product_tmc_decision

    _attach_money_contract(payload, structure, drain_block, all_block)
    if level == "business":
        _attach_business_contract(payload, current_metrics, prev_metrics, structure, all_block, cause_block)

    if level == "sku":
        payload["structure"] = structure
        payload["decision_block"] = _build_sku_decision(structure)
        payload["sku_passport"] = _build_sku_passport(
            sku=object_name,
            period=period,
            rows=rows,
            prev_rows=prev_rows,
            all_rows=_safe_get_rows(),
            filter_payload=payload.get("filter") or {},
        )
        sku_navigation_money = abs(_to_float(payload.get("navigation_money")))
        payload["drain_block"] = [{
            "object_name": _human_object_name(level, object_name),
            "effect_money": -round_money(sku_navigation_money),
            "value_money": -round_money(sku_navigation_money),
            "level": "sku",
            "self_drain": True,
        }] if sku_navigation_money > 0 else []
        _attach_object_contract(payload, structure, cause_block)
        payload["business_opportunity"] = _build_business_opportunity_engine(level, rows, business_rows, business_context, structure, current_metrics, prev_metrics)
        payload["recommendation_engine"] = _build_recommendation_engine(level, object_name, business_context, payload["business_opportunity"], current_metrics, prev_metrics)
        payload["narrative_engine"] = _build_narrative_engine(level, object_name, current_metrics, prev_metrics, business_context, payload["business_opportunity"], payload["recommendation_engine"])
        payload["product_workspace"] = _build_product_workspace_v2(
            level=level,
            object_name=object_name,
            period=period,
            business_context=business_context,
            business_opportunity=payload["business_opportunity"],
            recommendation_engine=payload["recommendation_engine"],
            narrative_engine=payload["narrative_engine"],
            sku_passport=payload.get("sku_passport"),
        )
        return payload

    payload["structure"] = structure
    payload["drain_block"] = drain_block
    if level == "network":
        payload["decision_block"] = _build_decision_block(structure, source_level="network", top_n=3)
        payload["decision_workspace"] = _build_decision_workspace(
            object_name=object_name,
            period=period,
            current_metrics=current_metrics,
            prev_metrics=prev_metrics,
            business_metrics=business_metrics,
            structure=structure,
            all_block=all_block,
            decision_block=payload["decision_block"],
            assortment_analysis=_build_contract_assortment_analysis(rows, _safe_get_rows(), period),
            rows_for_intelligence=rows,
            all_rows_for_intelligence=_safe_get_rows(),
        )
        payload["decision_workspace"]["business_context"] = business_context
        payload["decision_workspace"]["structural_analysis"] = structural_analysis
        payload["grouping_type"] = _build_grouping_type(rows, period)
        payload["aggregation_level"] = payload["grouping_type"]
    elif level in {"category", "tmc_group"}:
        payload["decision_block"] = _build_decision_block(structure, source_level=level, top_n=3)
        if level == "category":
            payload["category_workspace"] = _build_category_workspace(
                category=object_name,
                period=period,
                rows=rows,
                prev_rows=prev_rows,
                business_rows=business_rows,
                business_context=business_context,
            )
    if level != "business":
        _attach_object_contract(payload, structure, cause_block)

    if level in {"network", "category", "tmc_group"}:
        payload["business_opportunity"] = _build_business_opportunity_engine(level, rows, business_rows, business_context, structure, current_metrics, prev_metrics)
        payload["recommendation_engine"] = _build_recommendation_engine(level, object_name, business_context, payload["business_opportunity"], current_metrics, prev_metrics)
        payload["narrative_engine"] = _build_narrative_engine(level, object_name, current_metrics, prev_metrics, business_context, payload["business_opportunity"], payload["recommendation_engine"])
        payload["product_workspace"] = _build_product_workspace_v2(
            level=level,
            object_name=object_name,
            period=period,
            business_context=business_context,
            business_opportunity=payload["business_opportunity"],
            recommendation_engine=payload["recommendation_engine"],
            narrative_engine=payload["narrative_engine"],
            category_workspace=payload.get("category_workspace"),
        )
        if isinstance(payload.get("decision_workspace"), dict):
            payload["decision_workspace"]["recommendation_engine"] = payload.get("recommendation_engine")
            payload["decision_workspace"]["business_opportunity"] = payload.get("business_opportunity")
            payload["decision_workspace"]["narrative_engine"] = payload.get("narrative_engine")
    return payload


def _merge_filter_payload(filter_payload: Optional[Dict[str, Any]], **updates: Any) -> Dict[str, Any]:
    payload: Dict[str, Any] = dict(filter_payload or {})
    for key, value in updates.items():
        if value is None:
            continue
        payload[key] = value
    return payload


def get_business_summary(period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    all_rows = _safe_get_rows()
    payload = _merge_filter_payload(filter_payload, period=period)
    kwargs = {k: v for k, v in payload.items() if k != 'period'}
    rows, _ = _run_filter(all_rows, period=period, **kwargs)
    prev_rows = []
    prev_period = _previous_year_period(period)
    if prev_period:
        prev_kwargs = dict(kwargs)
        prev_rows, _ = _run_filter(all_rows, period=prev_period, **prev_kwargs)
    result = _summary_from_rows("business", "business", period, rows, prev_rows, rows)
    result['filter'] = payload
    return result


def _object_summary(level: str, field: str, object_name: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    all_rows = _safe_get_rows()
    payload = _merge_filter_payload(filter_payload, period=period, **{field: object_name})
    kwargs = {k: v for k, v in payload.items() if k != 'period'}
    rows, _ = _run_filter(all_rows, period=period, **kwargs)
    baseline_rows, compare_base = _product_baseline_rows(all_rows, level, period, rows, object_name)
    if not baseline_rows and level in {"category", "tmc_group", "sku"}:
        baseline_rows, _ = _run_filter(all_rows, period=period)
        compare_base = "product_baseline_missing"
    prev_rows = []
    prev_period = _previous_year_period(period)
    if prev_period:
        prev_kwargs = dict(kwargs)
        prev_rows, _ = _run_filter(all_rows, period=prev_period, **prev_kwargs)
    parent_object = payload.get("network") if level in {"category", "tmc_group", "sku"} else None
    if parent_object is None and level in {"tmc_group", "sku"}:
        parent_object = payload.get("category")
    if parent_object is None and level in {"manager", "network"}:
        parent_object = payload.get("manager_top")
    result = _summary_from_rows(level, object_name, period, rows, prev_rows, baseline_rows, compare_base=compare_base, parent_object=parent_object)
    result['filter'] = payload
    result['compare_base'] = compare_base
    return result


def get_manager_top_summary(manager_top: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _object_summary("manager_top", "manager_top", manager_top, period, filter_payload=filter_payload)


def get_manager_summary(manager: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _object_summary("manager", "manager", manager, period, filter_payload=filter_payload)


def get_network_summary(network: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _object_summary("network", "network", network, period, filter_payload=filter_payload)


def get_category_summary(category: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _object_summary("category", "category", category, period, filter_payload=filter_payload)


def get_tmc_group_summary(tmc_group: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _object_summary("tmc_group", "tmc_group", tmc_group, period, filter_payload=filter_payload)


def get_sku_summary(sku: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _object_summary("sku", "sku", sku, period, filter_payload=filter_payload)
