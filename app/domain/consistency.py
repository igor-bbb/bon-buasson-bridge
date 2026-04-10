from collections import defaultdict
from typing import Any, Dict, List, Optional


CHILD_FIELD_BY_LEVEL = {
    'business': 'manager_top',
    'manager_top': 'manager',
    'manager': 'network',
    'network': 'sku',
}


def _round(value: float) -> float:
    return round(float(value or 0.0), 2)


def _normalize_child_key(row: Dict[str, Any], child_field: str) -> Optional[str]:
    value = row.get(child_field)
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def calculate_consistency(parent_finrez_pre: float, child_finrez_values: List[float]) -> Dict[str, Any]:
    if not child_finrez_values:
        return {
            'status': 'not_available',
            'gap': None,
            'gap_percent': None,
            'children_sum': None,
        }

    parent_value = _round(parent_finrez_pre)
    children_sum = _round(sum(float(v or 0.0) for v in child_finrez_values))
    gap = _round(parent_value - children_sum)

    if parent_value == 0:
        gap_percent = 0.0 if gap == 0 else 100.0
    else:
        gap_percent = _round(abs(gap) / abs(parent_value) * 100.0)

    if gap_percent <= 1.0:
        status = 'ok'
    elif gap_percent <= 5.0:
        status = 'warning'
    else:
        status = 'error'

    return {
        'status': status,
        'gap': gap,
        'gap_percent': gap_percent,
        'children_sum': children_sum,
    }


def build_consistency_from_rows(level: str, parent_finrez_pre: float, rows: Optional[List[Dict[str, Any]]]) -> Dict[str, Any]:
    child_field = CHILD_FIELD_BY_LEVEL.get(level)
    if not child_field or not rows:
        return {
            'status': 'not_available',
            'gap': None,
            'gap_percent': None,
            'children_sum': None,
            'child_level': child_field,
        }

    grouped: Dict[str, float] = defaultdict(float)
    for row in rows:
        key = _normalize_child_key(row, child_field)
        if not key:
            continue
        grouped[key] += float(row.get('finrez_pre', 0.0) or 0.0)

    payload = calculate_consistency(parent_finrez_pre, list(grouped.values()))
    payload['child_level'] = child_field
    payload['children_count'] = len(grouped)
    return payload
