from __future__ import annotations

from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from app.domain import filters as filters_domain
from app.domain.metrics import aggregate_metrics
from app.domain.normalization import clean_text, round_money, round_percent
from app.config import DRAIN_MIN_ITEMS

import math
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
        return [], "sku"

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
            "article_name": article_name or "прочее",
            "signal": {
                "status": status,
                "label": "критично" if status == "critical" else ("риск" if status == "risk" else ""),
                "source": "margin_pre_quartile",
                "quartiles": bounds,
            },
        })
    drain_items.sort(key=lambda item: _to_float(item.get("effect_money")))
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
    return {
        "object_name": name,
        "effect_money": total_effect,
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
    return (items[:limit] if limit is not None else items), child


def _build_goal(level: str, current_metrics: Dict[str, float], prev_metrics: Dict[str, float], business_metrics: Optional[Dict[str, float]] = None) -> Dict[str, Any]:
    if level == "business":
        delta = round_money(_to_float(current_metrics.get("finrez_pre")) - _to_float(prev_metrics.get("finrez_pre")))
        return {"type": "keep" if delta > 0 else "close", "value_money": delta, "label": "удержать" if delta > 0 else "закрыть"}
    effect = round_money(((_to_float(business_metrics.get("margin_pre")) - _to_float(current_metrics.get("margin_pre"))) / 100.0) * _to_float(current_metrics.get("revenue"))) if business_metrics else 0.0
    return {"type": "close", "value_money": effect, "label": "закрыть"}


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







def _build_all_block(level: str, rows: List[Dict[str, Any]], business_metrics: Dict[str, Any], period: str) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    # Full level list for branch "all" must not reuse top-3 drain logic.
    # It should include every object of the next level, sorted by effect_money ASC.
    if level == "network":
        items, next_level = _build_network_adaptive_drain(rows, business_metrics, limit=None)
        return items, next_level

    child = LEVEL_CHILD.get(level)
    if not child:
        return [], None

    grouped = _group(rows, child)
    items: List[Dict[str, Any]] = []
    for name, child_rows in grouped.items():
        item = _drain_item(name, child_rows, business_metrics)
        items.append({
            "object_name": item.get("object_name"),
            "effect_money": round_money(_to_float(item.get("effect_money"))),
        })

    items.sort(key=lambda item: _to_float(item.get("effect_money")))
    return items, child


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


def _build_focus_block(structure: Dict[str, Any], *, level: str, top_n: int = 3) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    for key in ["markup", "retro", "logistics", "personnel", "other"]:
        item = structure.get(key) or {}
        effect_money = round_money(_to_float(item.get("effect_money")))
        items.append({
            "metric": _metric_name(key),
            "effect_money": effect_money,
            "solution_level": "sku" if level == "sku" else SOLUTION_LEVEL_MAP.get(key, "network"),
            "source_level": level,
        })
    items.sort(key=lambda x: _to_float(x.get("effect_money")))
    return items[:top_n]


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


def _build_cause_block(structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    causes: List[Dict[str, Any]] = []
    for key in ["markup", "retro", "logistics", "personnel", "other"]:
        item = structure.get(key) or {}
        causes.append({
            "name": ARTICLE_LABELS_RU.get(key, key).capitalize(),
            "effect_money": round_money(_to_float(item.get("effect_money"))),
        })
    causes.sort(key=lambda x: _to_float(x.get("effect_money")))
    return causes


def _build_network_focus(structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _build_focus_block(structure, level="network", top_n=3)

def _build_sku_focus(structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _build_focus_block(structure, level="sku", top_n=3)


def _build_sku_decision(structure: Dict[str, Any]) -> List[Dict[str, Any]]:
    return _build_decision_block(structure, source_level="sku", top_n=3)


def _build_sku_prompt(focus_block: List[Dict[str, Any]]) -> List[str]:
    return []


def _summary_from_rows(level: str, object_name: str, period: str, rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]], business_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    current_metrics = aggregate_metrics(rows)
    prev_metrics = aggregate_metrics(prev_rows)
    business_metrics = aggregate_metrics(business_rows)
    base_metrics = prev_metrics if level == "business" else business_metrics
    drain_block, adaptive_next_level = _build_drain(level, rows, business_metrics if business_rows else current_metrics, period)
    all_block, all_next_level = _build_all_block(level, rows, business_metrics if business_rows else current_metrics, period)
    structure = _build_structure(current_metrics, base_metrics)
    cause_block = _build_cause_block(structure)

    payload = {
        "context": {
            "level": level,
            "object_name": _human_object_name(level, object_name),
            "period": period,
            "compare_base": "previous_year",
            "previous_year_period": _previous_year_period(period),
        },
        "metrics": _build_metrics(current_metrics, prev_metrics, level),
        "goal": _build_goal(level, current_metrics, prev_metrics, business_metrics),
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
    }

    if level == "sku":
        focus_block = _build_sku_focus(structure)
        payload["structure"] = structure
        payload["focus_block"] = focus_block
        payload["decision_block"] = _build_sku_decision(structure)
        payload["provocation_block"] = _build_sku_prompt(focus_block)
        payload["drain_block"] = []
        return payload

    payload["structure"] = structure
    payload["drain_block"] = drain_block
    if level == "network":
        payload["focus_block"] = _build_network_focus(structure)
        payload["decision_block"] = _build_decision_block(structure, source_level="network", top_n=3)
        payload["grouping_type"] = _build_grouping_type(rows, period)
        payload["aggregation_level"] = payload["grouping_type"]
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
    business_rows, _ = _run_filter(all_rows, period=period)
    prev_rows = []
    prev_period = _previous_year_period(period)
    if prev_period:
        prev_kwargs = dict(kwargs)
        prev_rows, _ = _run_filter(all_rows, period=prev_period, **prev_kwargs)
    result = _summary_from_rows(level, object_name, period, rows, prev_rows, business_rows)
    result['filter'] = payload
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
