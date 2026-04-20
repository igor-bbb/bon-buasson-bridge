from collections import defaultdict
from typing import Any, Dict, List, Optional

from app.domain.metrics import aggregate_metrics
from app.domain.normalization import round_money, round_percent


CHILD_LEVEL_BY_LEVEL = {
    'business': 'manager_top',
    'manager_top': 'manager',
    'manager': 'network',
    'network': 'sku',
    'category': 'sku',
    'tmc_group': 'sku',
}


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _group_rows(rows: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        key = row.get(field)
        if key in (None, ''):
            continue
        grouped[str(key)].append(row)
    return grouped


def _resolve_child_level(level: Optional[str]) -> Optional[str]:
    if not level:
        return None
    return CHILD_LEVEL_BY_LEVEL.get(level)


def _build_child_metrics_list(rows: List[Dict[str, Any]], child_level: str) -> List[Dict[str, Any]]:
    grouped = _group_rows(rows, child_level)
    child_metrics_list: List[Dict[str, Any]] = []

    for _, chunk in grouped.items():
        metrics = aggregate_metrics(chunk)
        child_metrics_list.append({
            'finrez_pre': metrics.get('finrez_pre', 0.0),
        })

    return child_metrics_list


def build_consistency_from_rows(
    *,
    level: str,
    parent_finrez_pre: float,
    rows: Optional[List[Dict[str, Any]]],
) -> Dict[str, Any]:
    if not rows:
        return {
            'status': 'warning',
            'gap': None,
            'gap_percent': None,
            'children_sum': 0.0,
            'child_level': _resolve_child_level(level),
            'children_count': 0,
        }

    child_level = _resolve_child_level(level)

    if not child_level:
        return {
            'status': 'ok',
            'gap': 0.0,
            'gap_percent': 0.0,
            'children_sum': round_money(parent_finrez_pre),
            'child_level': None,
            'children_count': 0,
        }

    child_metrics_list = _build_child_metrics_list(rows, child_level)
    children_sum = round_money(sum(_to_float(item.get('finrez_pre')) for item in child_metrics_list))
    gap = round_money(_to_float(parent_finrez_pre) - children_sum)

    if abs(_to_float(parent_finrez_pre)) < 1e-9:
        gap_percent = 0.0 if abs(gap) < 1e-9 else None
    else:
        gap_percent = round_percent((abs(gap) / abs(_to_float(parent_finrez_pre))) * 100.0)

    if gap_percent is None:
        status = 'warning'
    elif gap_percent < 1:
        status = 'ok'
    elif gap_percent <= 5:
        status = 'warning'
    else:
        status = 'critical'

    return {
        'status': status,
        'gap': gap,
        'gap_percent': gap_percent,
        'children_sum': children_sum,
        'child_level': child_level,
        'children_count': len(child_metrics_list),
    }
