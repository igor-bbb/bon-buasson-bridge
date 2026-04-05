from typing import Dict, Any, List

from app.domain.comparison import build_comparison_payload
from app.domain.filters import get_normalized_rows


LEVEL_ORDER = ['business', 'manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']


def _next_level(level: str) -> str:
    if level not in LEVEL_ORDER:
        return None
    idx = LEVEL_ORDER.index(level)
    if idx + 1 >= len(LEVEL_ORDER):
        return None
    return LEVEL_ORDER[idx + 1]


def build_drilldown_payload(level: str, object_name: str, period: str) -> Dict[str, Any]:

    children_level = _next_level(level)

    if not children_level:
        return {
            "level": level,
            "object_name": object_name,
            "period": period,
            "children_level": None,
            "items": []
        }

    rows = get_normalized_rows()

    # фильтрация по текущему объекту
    filtered = []
    for r in rows:
        if r.get("period") != period:
            continue
        if level != "business" and r.get(level) != object_name:
            continue
        filtered.append(r)

    if not filtered:
        return {
            "level": level,
            "object_name": object_name,
            "period": period,
            "children_level": children_level,
            "items": []
        }

    # группировка
    grouped = {}
    for r in filtered:
        key = r.get(children_level)
        if not key:
            continue
        grouped.setdefault(key, []).append(r)

    items: List[Dict[str, Any]] = []

    for child_name in grouped.keys():
        payload = build_comparison_payload(
            level=children_level,
            object_name=child_name,
            period=period
        )
        if payload:
            items.append(payload)

    return {
        "level": level,
        "object_name": object_name,
        "period": period,
        "children_level": children_level,
        "items": items
    }
