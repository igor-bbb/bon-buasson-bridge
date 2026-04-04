from typing import Any, Dict, List, Tuple

from app.domain.normalization import round_money


def pick_top_drain(
    effects_by_metric: Dict[str, Dict[str, Any]],
    low_volume: bool,
) -> Tuple[str, float, bool]:
    if low_volume:
        return "", 0.0, False

    negative_items = []
    for metric, payload in effects_by_metric.items():
        if payload["is_negative_for_business"]:
            negative_items.append((metric, payload["effect_value"], True))

    if not negative_items:
        return "", 0.0, False

    top_metric, top_effect, top_negative = max(negative_items, key=lambda x: abs(x[1]))
    return top_metric, round_money(top_effect), top_negative


def sort_items_by_top_problem(items: List[Dict[str, Any]]) -> None:
    def sort_key(item: Dict[str, Any]):
        flags = item.get("flags", {})
        low_volume = flags.get("low_volume", False)

        top_effect = item.get("top_drain_effect", 0)
        is_negative = item.get("top_drain_is_negative_for_business", False)

        if low_volume:
            return (1, 0.0)

        return (0, -abs(top_effect) if is_negative else 0)

    items.sort(key=sort_key)
    
