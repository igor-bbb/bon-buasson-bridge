
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

MONEY_METRICS = [
    "revenue",
    "finrez_pre",
    "finrez_final",
    "retro_bonus",
    "logistics_cost",
    "personnel_cost",
    "other_costs",
]

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
    metrics: Dict[str, Any] = {
        "revenue": _metric_money(_to_float(current_metrics.get("revenue")), _to_float(prev_metrics.get("revenue"))),
        "margin_pre": _metric_percent(
            _to_float(current_metrics.get("margin_pre")),
            _to_float(prev_metrics.get("margin_pre")),
            _to_float(current_metrics.get("finrez_pre")),
            _to_float(prev_metrics.get("finrez_pre")),
        ),
        "markup": _metric_percent(
            _to_float(current_metrics.get("markup")),
            _to_float(prev_metrics.get("markup")),
            _to_float(current_metrics.get("gross_profit", current_metrics.get("revenue"))),
            _to_float(prev_metrics.get("gross_profit", prev_metrics.get("revenue"))),
        ),
        "finrez_pre": _metric_money(_to_float(current_metrics.get("finrez_pre")), _to_float(prev_metrics.get("finrez_pre"))),
        "retro_bonus": _metric_money(_to_float(current_metrics.get("retro_bonus")), _to_float(prev_metrics.get("retro_bonus"))),
        "logistics_cost": _metric_money(_to_float(current_metrics.get("logistics_cost")), _to_float(prev_metrics.get("logistics_cost"))),
        "personnel_cost": _metric_money(_to_float(current_metrics.get("personnel_cost")), _to_float(prev_metrics.get("personnel_cost"))),
        "other_costs": _metric_money(_to_float(current_metrics.get("other_costs")), _to_float(prev_metrics.get("other_costs"))),
        "gap": {
            "value_percent": round_percent(_to_float(current_metrics.get("kpi_gap", current_metrics.get("gap")))),
            "delta_percent": _pp_change(
                _to_float(current_metrics.get("kpi_gap", current_metrics.get("gap"))),
                _to_float(prev_metrics.get("kpi_gap", prev_metrics.get("gap"))),
            ),
        },
    }
    if level == "business":
        metrics["finrez_final"] = _metric_money(_to_float(current_metrics.get("finrez_final")), _to_float(prev_metrics.get("finrez_final")))
    return metrics

def _group(rows: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = row.get(field) or ""
        if not key or key in {"Пусто", "Без менеджера"}:
            continue
        grouped[key].append(row)
    return grouped

def _drain_item(name: str, rows: List[Dict[str, Any]], business_margin: float, prev_rows: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    current = aggregate_metrics(rows)
    prev = aggregate_metrics(prev_rows or []) if prev_rows else {}
    revenue = _to_float(current.get("revenue"))
    margin = _to_float(current.get("margin_pre"))
    finrez = _to_float(current.get("finrez_pre"))
    potential = round_money(max(0.0, (business_margin - margin) / 100.0 * revenue))
    item: Dict[str, Any] = {
        "object_name": name,
        "fact": {
            "finrez": round_money(finrez),
            "margin": round_percent(margin),
            "revenue": round_money(revenue),
        },
        "gap_to_business_pp": round_percent(margin - business_margin),
        "potential_money": potential,
        "potential_explanation": {
            "formula": "(margin_business - margin_object) × revenue",
            "components": {
                "margin_business": round_percent(business_margin),
                "margin_object": round_percent(margin),
                "revenue": round_money(revenue),
            }
        }
    }
    if prev_rows:
        prev_metrics = aggregate_metrics(prev_rows)
        prev_finrez = _to_float(prev_metrics.get("finrez_pre"))
        item["delta_prev_year"] = {
            "money": round_money(finrez - prev_finrez),
            "percent": _percent_change(finrez, prev_finrez),
        }
    return item

def _build_drain(level: str, object_rows: List[Dict[str, Any]], business_margin: float, period: str) -> List[Dict[str, Any]]:
    child = LEVEL_CHILD.get(level)
    if not child:
        return []
    grouped = _group(object_rows, child)
    prev_period = _previous_year_period(period)
    prev_grouped: Dict[str, List[Dict[str, Any]]] = {}
    if prev_period:
        prev_rows, _ = _run_filter(_safe_get_rows(), period=prev_period)
        # keep same parent scope based on current object_rows unique dimensions except child, period
        # simpler: prev grouped on same child within same top scope inferred from first row
        scope = {}
        if object_rows:
            sample = object_rows[0]
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
        revenue = sum(_to_float(r.get("revenue")) for r in rows)
        if revenue <= 0:
            continue
        item = _drain_item(name, rows, business_margin, prev_grouped.get(name))
        if item["potential_money"] <= 0:
            continue
        items.append(item)

    items.sort(key=lambda x: x.get("potential_money", 0.0), reverse=True)
    if len(items) >= 3:
        return items[:7]
    return items

def _build_goal(level: str, current_metrics: Dict[str, float], prev_metrics: Dict[str, float]) -> Dict[str, Any]:
    delta_money = round_money(_to_float(current_metrics.get("finrez_pre")) - _to_float(prev_metrics.get("finrez_pre")))
    return {
        "type": "keep_growth" if delta_money >= 0 else "close_gap",
        "value_money": delta_money,
    }

def _build_focus(drain_block: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not drain_block:
        return {}
    total = sum(_to_float(item.get("potential_money")) for item in drain_block)
    if total <= 0:
        return {}
    top = drain_block[0]
    share = _to_float(top.get("potential_money")) / total
    if share <= 0.30:
        return {}
    return {
        "type": "manager_top",
        "object": top.get("object_name"),
        "share": round(share, 3),
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
        "items": list(range(1, min(len(drain_block), 3) + 1)),
        "all": bool(drain_block),
        "reasons": True,
        "back": True,
    }

def _context(level: str, object_name: str, period: str) -> Dict[str, Any]:
    return {"level": level, "object_name": object_name, "period": period}

def _summary_from_rows(level: str, object_name: str, period: str, rows: List[Dict[str, Any]], prev_rows: List[Dict[str, Any]], business_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    current_metrics = aggregate_metrics(rows)
    prev_metrics = aggregate_metrics(prev_rows) if prev_rows else {}
    business_metrics = aggregate_metrics(business_rows)
    metrics = _build_metrics(current_metrics, prev_metrics, level)
    drain_block = _build_drain(level, rows, _to_float(business_metrics.get("margin_pre")), period)
    return {
        "context": _context(level, object_name, period),
        "metrics": metrics,
        "drain_block": drain_block,
        "goal": _build_goal(level, current_metrics, prev_metrics),
        "focus_block": _build_focus(drain_block),
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
