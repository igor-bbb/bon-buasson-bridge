from typing import Any, Dict, List


SIGNAL_LABELS_RU = {
    "critical": "критично",
    "risk": "риск",
    "attention": "внимание",
    "ok": "норма",
}


def _round(value: float) -> float:
    return round(float(value or 0.0), 2)


def _percentile(sorted_values: List[float], q: float) -> float:
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])

    position = (len(sorted_values) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(sorted_values) - 1)
    fraction = position - lower

    lower_value = float(sorted_values[lower])
    upper_value = float(sorted_values[upper])
    return lower_value + (upper_value - lower_value) * fraction


def _build_quartile_bounds(values: List[float]) -> Dict[str, float]:
    sorted_values = sorted(float(v) for v in values)
    return {
        "q1": _round(_percentile(sorted_values, 0.25)),
        "q2": _round(_percentile(sorted_values, 0.50)),
        "q3": _round(_percentile(sorted_values, 0.75)),
    }


def _detect_status(metric_value: float, q1: float, q2: float, q3: float) -> str:
    if metric_value <= q1:
        return "critical"
    if metric_value <= q2:
        return "risk"
    if metric_value <= q3:
        return "attention"
    return "ok"


def _detect_rank(metric_value: float, q1: float, q2: float, q3: float) -> str:
    if metric_value <= q1:
        return "Q1"
    if metric_value <= q2:
        return "Q2"
    if metric_value <= q3:
        return "Q3"
    return "Q4"


def _comment_for_status(status: str) -> str:
    mapping = {
        "critical": "объект в нижнем квартиле margin_pre",
        "risk": "объект ниже медианы margin_pre",
        "attention": "объект в зоне внимания margin_pre",
        "ok": "объект в верхнем квартиле margin_pre",
    }
    return mapping.get(status, "сигнал периода рассчитан")


def _signal_unavailable(comment: str) -> Dict[str, Any]:
    return {
        "status": "no_data",
        "label": "нет данных",
        "comment": comment,
        "reason": "margin_pre",
        "reason_value": None,
        "rank": None,
        "priority": "low",
        "quartiles": None,
    }


def build_period_signal(*, level: str, object_name: str, margin_pre: float | None, peer_items: List[Dict[str, Any]]) -> Dict[str, Any]:
    if margin_pre is None:
        return _signal_unavailable("нет margin_pre для сигнала периода")

    if level == "business":
        return {
            "status": "ok",
            "label": SIGNAL_LABELS_RU["ok"],
            "comment": "базовый уровень периода",
            "reason": "margin_pre",
            "reason_value": _round(margin_pre),
            "rank": "BASE",
            "priority": "high",
            "quartiles": None,
        }

    valid_peer_items = [item for item in peer_items if item.get("margin_pre") is not None]
    values = [float(item["margin_pre"]) for item in valid_peer_items]
    if not values:
        return _signal_unavailable("нет базы margin_pre для сигнала периода")

    quartiles = _build_quartile_bounds(values)
    q1 = quartiles["q1"]
    q2 = quartiles["q2"]
    q3 = quartiles["q3"]

    status = _detect_status(float(margin_pre), q1, q2, q3)
    rank = _detect_rank(float(margin_pre), q1, q2, q3)

    return {
        "status": status,
        "label": SIGNAL_LABELS_RU[status],
        "comment": _comment_for_status(status),
        "reason": "margin_pre",
        "reason_value": _round(margin_pre),
        "rank": rank,
        "priority": "high" if status == "critical" else ("medium" if status == "risk" else "low"),
        "quartiles": quartiles,
    }
