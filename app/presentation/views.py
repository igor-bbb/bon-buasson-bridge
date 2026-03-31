from typing import Any, Dict, List


def sort_effect_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(item: Dict[str, Any]):
        is_negative = item["is_negative_for_business"]
        effect_value = item["effect_value"]

        if is_negative:
            return (0, -abs(effect_value))
        return (1, -abs(effect_value))

    return sorted(entries, key=sort_key)


def build_reasons_view(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    effects_by_metric = comparison_payload["effects_by_metric"]

    reasons = []
    for metric, payload in effects_by_metric.items():
        reasons.append({
            "metric": metric,
            "effect_value": payload["effect_value"],
            "effect_direction": payload["effect_direction"],
            "type": payload["type"],
            "is_negative_for_business": payload["is_negative_for_business"],
        })

    reasons = sort_effect_entries(reasons)

    return {
        "level": comparison_payload["level"],
        "object_name": comparison_payload["object_name"],
        "period": comparison_payload["period"],
        "top_drain_metric": comparison_payload["top_drain_metric"],
        "top_drain_effect": comparison_payload["top_drain_effect"],
        "reasons": reasons,
    }


def build_losses_view_from_summary(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    effects_by_metric = comparison_payload["effects_by_metric"]

    losses = []
    for metric, payload in effects_by_metric.items():
        if payload["is_negative_for_business"]:
            losses.append({
                "metric": metric,
                "effect_value": payload["effect_value"],
                "type": payload["type"],
                "is_negative_for_business": payload["is_negative_for_business"],
            })

    losses.sort(key=lambda x: -abs(x["effect_value"]))

    return {
        "level": comparison_payload["level"],
        "object_name": comparison_payload["object_name"],
        "period": comparison_payload["period"],
        "top_drain_metric": comparison_payload["top_drain_metric"],
        "top_drain_effect": comparison_payload["top_drain_effect"],
        "losses": losses,
    }


def build_losses_view_from_children(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = drilldown_payload["items"]

    losses = []
    for item in items:
        losses.append({
            "object_name": item["object_name"],
            "top_drain_metric": item["top_drain_metric"],
            "top_drain_effect": item["top_drain_effect"],
            "is_negative_for_business": item["top_drain_is_negative_for_business"],
            "flags": item["flags"],
        })

    def sort_key(item: Dict[str, Any]):
        low_volume = item["flags"]["low_volume"]
        is_negative = item["is_negative_for_business"]
        top_effect = item["top_drain_effect"]

        if low_volume:
            return (1, 0.0)

        if is_negative:
            return (0, -abs(top_effect))

        return (0, float("inf"))

    losses.sort(key=sort_key)

    return {
        "level": drilldown_payload["level"],
        "object_name": drilldown_payload["object_name"],
        "period": drilldown_payload["period"],
        "children_level": drilldown_payload["children_level"],
        "losses": losses,
    }
