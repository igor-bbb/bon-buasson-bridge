from typing import Any, Dict, List


# =========================
# FORMAT HELPERS
# =========================

def _to_float(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _format_money(x: Any) -> str:
    value = _to_float(x)
    return f"{value:,.0f}".replace(",", " ")


def _format_percent(x: Any) -> str:
    value = _to_float(x)
    return f"{value:.2f}%"


def _format_pp(x: Any) -> str:
    value = _to_float(x)
    return f"{value:.2f} п.п."


# =========================
# CORE BUILDERS
# =========================

def build_object_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = payload.get("metrics", {})
    impact = payload.get("impact", {})
    diagnosis = payload.get("diagnosis", {})
    previous = payload.get("previous_object_metrics", {})

    object_metrics = metrics.get("object_metrics", {})
    business_metrics = metrics.get("business_metrics", {})

    # =========================
    # ЯКОРЬ
    # =========================
    revenue = object_metrics.get("revenue")
    finrez = object_metrics.get("finrez_pre")
    margin = object_metrics.get("margin_pre")

    prev_revenue = previous.get("revenue")
    prev_finrez = previous.get("finrez_pre")
    prev_margin = previous.get("margin_pre")

    def yoy(current, prev):
        if not prev:
            return None
        return ((current - prev) / abs(prev)) * 100 if prev != 0 else None

    anchor = [
        {
            "label": "Оборот",
            "value": _format_money(revenue),
            "yoy": _format_percent(yoy(revenue, prev_revenue)) if prev_revenue else None,
        },
        {
            "label": "Финрез",
            "value": _format_money(finrez),
            "yoy": _format_percent(yoy(finrez, prev_finrez)) if prev_finrez else None,
        },
        {
            "label": "Маржа",
            "value": _format_percent(margin),
            "yoy": _format_pp(margin - prev_margin) if prev_margin else None,
        },
    ]

    # =========================
    # ВЕКТОР
    # =========================
    vector = {
        "money": _format_money(impact.get("gap_loss_money")),
        "delta": _format_pp(impact.get("gap_percent")),
    }

    # =========================
    # ПРИЧИНЫ
    # =========================
    effects = impact.get("per_metric_effects", {})
    reasons = []

    for key, value in effects.items():
        money = _to_float(value)
        if money <= 0:
            continue

        reasons.append({
            "name": key,
            "money": _format_money(money),
            "percent": None,
        })

    reasons = sorted(reasons, key=lambda x: -_to_float(x["money"].replace(" ", "")))[:3]

    # =========================
    # РЕШЕНИЯ (НОВОЕ)
    # =========================
    solutions = []

    for r in reasons:
        val = _to_float(r["money"].replace(" ", ""))
        if val <= 0:
            continue

        solutions.append({
            "title": r["name"],
            "action": "изменить на 5%",
            "effect": r["money"],
            "line": f"{r['name']}\nИзменение на 5%\n→ {r['money']}"
        })

    # =========================
    # ФИНАЛ
    # =========================
    return {
        "object": payload.get("object_name"),
        "level": payload.get("level"),
        "period": payload.get("period"),

        "anchor": anchor,
        "vector": vector,
        "reasons": reasons,
        "solutions": solutions,
    }


# =========================
# LIST VIEW (КРИТИЧНО)
# =========================

def build_list_view(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []

    for item in items:
        metrics = item.get("metrics", {}).get("object_metrics", {})
        impact = item.get("impact", {})

        finrez = _format_money(metrics.get("finrez_pre"))
        margin = _format_percent(metrics.get("margin_pre"))
        money = _format_money(impact.get("gap_loss_money"))

        line = (
            f"{item.get('object_name')}\n"
            f"Финрез: {finrez} | Маржа: {margin}\n"
            f"→ {money}"
        )

        out.append({
            "name": item.get("object_name"),
            "line": line,
        })

    return out


# =========================
# DRAIN VIEW
# =========================

def build_drain_view(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    out = []

    for item in items:
        metrics = item.get("metrics", {}).get("object_metrics", {})
        impact = item.get("impact", {})

        line = (
            f"{item.get('object_name')}\n"
            f"Маржа: {_format_percent(metrics.get('margin_pre'))}\n"
            f"Δ к бизнесу: {_format_pp(impact.get('gap_percent'))}\n"
            f"→ {_format_money(impact.get('gap_loss_money'))}"
        )

        out.append({
            "name": item.get("object_name"),
            "line": line,
        })

    return out
