from typing import Any, Dict, List, Tuple


DEFAULT_LIMIT = 5
FULL_VIEW_MARKERS = {"все", "покажи все", "полный список", "full", "show all"}


def round_money(value: float) -> float:
    return round(float(value or 0.0), 2)


def pick_top_drain(effects_by_metric: Dict[str, Dict[str, Any]], low_volume: bool) -> Tuple[str, float, bool]:
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


def _signal_status(item: Dict[str, Any]) -> str:
    return str((item.get("signal") or {}).get("status") or "ok")


def _finrez_pre(item: Dict[str, Any]) -> float:
    return float(item.get("metrics", {}).get("object_metrics", {}).get("finrez_pre", 0.0) or 0.0)


def sort_items_by_top_problem(items: List[Dict[str, Any]]) -> None:
    def sort_key(item: Dict[str, Any]):
        flags = item.get("flags", {})
        low_volume = flags.get("low_volume", False)
        status = _signal_status(item)
        finrez_pre = _finrez_pre(item)
        is_drain = status == "critical" and finrez_pre < 0

        if low_volume:
            return (2, 0.0, 0.0)
        if is_drain:
            return (0, -abs(finrez_pre), finrez_pre)
        return (1, finrez_pre, finrez_pre)

    items.sort(key=sort_key)


def _build_items_meta(total_count: int, returned_count: int) -> Dict[str, Any]:
    hidden_count = max(total_count - returned_count, 0)
    return {
        "total_count": total_count,
        "returned_count": returned_count,
        "hidden_count": hidden_count,
        "has_more": hidden_count > 0,
    }


def _is_drain_candidate(item: Dict[str, Any]) -> bool:
    return _signal_status(item) == "critical" and _finrez_pre(item) < 0


def select_visible_items(items: List[Dict[str, Any]], *, full_view: bool = False, limit: int = DEFAULT_LIMIT) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not items:
        return [], _build_items_meta(0, 0)

    total_count = len(items)
    sorted_items = list(items)
    sort_items_by_top_problem(sorted_items)

    if full_view:
        full_sorted = sorted(sorted_items, key=_finrez_pre)
        return full_sorted, _build_items_meta(total_count, len(full_sorted))

    drain_items = [item for item in sorted_items if _is_drain_candidate(item)]
    visible = drain_items[:limit]
    return visible, _build_items_meta(total_count, len(visible))
