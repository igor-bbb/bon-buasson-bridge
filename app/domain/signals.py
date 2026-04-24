import math
from typing import Any, Dict, List, Optional


SIGNAL_LABELS_RU = {
    "critical": "критично",
    "risk": "риск",
    "attention": "внимание",
    "ok": "норма",
}


def _round(value: float) -> float:
    v = float(value or 0.0)
    return round(v, 2) if math.isfinite(v) else 0.0


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


def _comment_for_status(status: str, metric_name: str = "разрыв") -> str:
    mapping = {
        "critical": f"объект в критичном квартиле {metric_name}",
        "risk": f"объект ниже медианы {metric_name}",
        "attention": f"объект в зоне внимания {metric_name}",
        "ok": f"объект в сильном квартиле {metric_name}",
    }
    return mapping.get(status, "сигнал периода рассчитан")


def _signal_unavailable(comment: str, metric_name: str = 'разрыв') -> Dict[str, Any]:
    return {
        "status": "no_data",
        "label": "нет данных",
        "comment": comment,
        "reason": metric_name,
        "reason_value": None,
        "rank": None,
        "priority": "low",
        "quartiles": None,
    }


def build_period_signal(
    *,
    level: str,
    object_name: str,
    margin_pre: Optional[float] = None,
    metric_value: Optional[float] = None,
    peer_items: List[Dict[str, Any]],
    metric_name: str = 'разрыв',
) -> Dict[str, Any]:
    current_value = metric_value if metric_value is not None else margin_pre
    peer_key = 'metric_value' if metric_value is not None else 'margin_pre'
    default_metric_name = metric_name if metric_value is not None else 'margin_pre'

    if current_value is None:
        return _signal_unavailable(f"нет {default_metric_name} для сигнала периода", default_metric_name)

    if level == "business":
        return {
            "status": "ok",
            "label": SIGNAL_LABELS_RU["ok"],
            "comment": "базовый уровень периода",
            "reason": default_metric_name,
            "reason_value": _round(current_value),
            "rank": "BASE",
            "priority": "high",
            "quartiles": None,
        }

    valid_peer_items = [item for item in peer_items if item.get(peer_key) is not None]
    values = [float(item[peer_key]) for item in valid_peer_items]
    if not values:
        return _signal_unavailable(f"нет базы {default_metric_name} для сигнала периода", default_metric_name)

    quartiles = _build_quartile_bounds(values)
    q1 = quartiles["q1"]
    q2 = quartiles["q2"]
    q3 = quartiles["q3"]

    status = _detect_status(float(current_value), q1, q2, q3)
    rank = _detect_rank(float(current_value), q1, q2, q3)

    return {
        "status": status,
        "label": SIGNAL_LABELS_RU[status],
        "comment": _comment_for_status(status, default_metric_name),
        "reason": default_metric_name,
        "reason_value": _round(current_value),
        "rank": rank,
        "priority": "high" if status == "critical" else ("medium" if status == "risk" else "low"),
        "quartiles": quartiles,
    }