from typing import Any, Dict, List, Optional

from app.domain.metrics import build_solutions_from_effects


def _to_float(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _money(x):
    return f"{_to_float(x):,.0f}".replace(",", " ")


def _percent(x):
    return f"{_to_float(x):.2f}%"


def _pp(x):
    return f"{_to_float(x):.2f} п.п."


# =========================
# OBJECT
# =========================

def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    metrics = payload.get("metrics", {}).get("object_metrics", {})
    impact = payload.get("impact", {})
    previous = payload.get("previous_object_metrics", {})

    def yoy(cur, prev):
        if not prev:
            return None
        return _money(_to_float(cur) - _to_float(prev))

    anchor = [
        {
            "label": "Оборот",
            "value": _money(metrics.get("revenue")),
            "yoy": yoy(metrics.get("revenue"), previous.get("revenue")),
        },
        {
            "label": "Финрез",
            "value": _money(metrics.get("finrez_pre")),
            "yoy": yoy(metrics.get("finrez_pre"), previous.get("finrez_pre")),
        },
        {
            "label": "Маржа",
            "value": _percent(metrics.get("margin_pre")),
            "yoy": _pp(_to_float(metrics.get("margin_pre")) - _to_float(previous.get("margin_pre"))),
        },
    ]

    vector = {
        "money": _money(impact.get("gap_loss_money")),
        "delta": _pp(impact.get("gap_percent")),
    }

    # ДРЕНАЖ
    drain = []
    if isinstance(drain_payload, dict):
        for item in (drain_payload.get("items") or [])[:3]:
            m = item.get("metrics", {}).get("object_metrics", {})
            imp = item.get("impact", {})

            drain.append({
                "name": item.get("object_name"),
                "line": f"{item.get('object_name')} | {_money(imp.get('gap_loss_money'))}"
            })

    # ПРИЧИНЫ
    reasons = []
    for k, v in (impact.get("per_metric_effects") or {}).items():
        if _to_float(v) <= 0:
            continue
        reasons.append({
            "name": k,
            "money": _money(v)
        })

    reasons = sorted(reasons, key=lambda x: -_to_float(x["money"].replace(" ", "")))[:3]

    # РЕШЕНИЯ
    solutions_raw = build_solutions_from_effects(metrics, impact.get("per_metric_effects") or {})

    solutions = []
    for s in solutions_raw[:3]:
        solutions.append({
            "title": s.get("metric"),
            "action": s.get("action"),
            "effect": _money(s.get("effect")),
        })

    return {
        "type": "object",
        "object": payload.get("object_name"),
        "level": payload.get("level"),
        "period": payload.get("period"),
        "anchor": anchor,
        "vector": vector,
        "drain": drain,
        "reasons": reasons,
        "solutions": solutions,
    }


# =========================
# LIST
# =========================

def build_list_view(current_payload: Dict[str, Any], source_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = source_payload.get("items") or []

    out = []

    for idx, item in enumerate(items, 1):
        metrics = item.get("metrics", {}).get("object_metrics", {})
        impact = item.get("impact", {})

        out.append({
            "index": idx,
            "name": item.get("object_name"),
            "line": f"{item.get('object_name')} → {_money(impact.get('gap_loss_money'))}"
        })

    return {
        "type": "management_list",
        "items": out,
    }


# =========================
# REASONS
# =========================

def build_reasons_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    impact = payload.get("impact", {})

    reasons = []

    for k, v in (impact.get("per_metric_effects") or {}).items():
        if _to_float(v) <= 0:
            continue
        reasons.append({
            "name": k,
            "money": _money(v)
        })

    return {
        "type": "reasons",
        "items": reasons,
    }


# =========================
# LOSSES
# =========================

def build_losses_view_from_children(source_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = source_payload.get("items") or []

    out = []

    for idx, item in enumerate(items[:3], 1):
        impact = item.get("impact", {})

        out.append({
            "index": idx,
            "name": item.get("object_name"),
            "money": _money(impact.get("gap_loss_money")),
        })

    return {
        "type": "losses",
        "items": out,
    }


# =========================
# COMPARISON
# =========================

def build_comparison_management_view(query: Dict[str, Any], current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "type": "comparison",
        "current": current,
        "previous": previous,
    }
