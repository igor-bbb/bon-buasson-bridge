from typing import Any, Dict, List, Optional


SIGNAL_LABELS_RU = {
    'critical': 'критично',
    'risk': 'риск',
    'attention': 'внимание',
    'ok': 'норма',
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
        'q1': _round(_percentile(sorted_values, 0.25)),
        'q2': _round(_percentile(sorted_values, 0.50)),
        'q3': _round(_percentile(sorted_values, 0.75)),
    }


def _detect_status(finrez_pre: float, q1: float, q2: float, q3: float) -> str:
    if finrez_pre <= q1:
        return 'critical'
    if finrez_pre <= q2:
        return 'risk'
    if finrez_pre <= q3:
        return 'attention'
    return 'ok'


def _detect_rank(finrez_pre: float, q1: float, q2: float, q3: float) -> str:
    if finrez_pre <= q1:
        return 'Q1'
    if finrez_pre <= q2:
        return 'Q2'
    if finrez_pre <= q3:
        return 'Q3'
    return 'Q4'


def _comment_for_status(status: str) -> str:
    mapping = {
        'critical': 'объект в нижнем квартиле периода',
        'risk': 'объект ниже медианной зоны периода',
        'attention': 'объект в зоне внимания периода',
        'ok': 'объект в верхнем квартиле периода',
    }
    return mapping.get(status, 'сигнал периода рассчитан')


def _problem_money(finrez_pre: float, q2: float) -> float:
    if finrez_pre < 0:
        return abs(finrez_pre)
    return max(q2 - finrez_pre, 0.0)


def _assign_priorities(problem_items: List[Dict[str, Any]]) -> Dict[str, str]:
    if not problem_items:
        return {}

    sorted_items = sorted(problem_items, key=lambda x: x['problem_money'], reverse=True)
    total_problem_money = sum(item['problem_money'] for item in sorted_items)

    if total_problem_money <= 0:
        return {item['object_name']: 'low' for item in sorted_items}

    cumulative_share = 0.0
    priorities: Dict[str, str] = {}

    for item in sorted_items:
        cumulative_share += item['problem_money'] / total_problem_money

        if cumulative_share <= 0.80:
            priority = 'high'
        elif cumulative_share <= 0.95:
            priority = 'medium'
        else:
            priority = 'low'

        priorities[item['object_name']] = priority

    return priorities


def build_period_signal(
    *,
    level: str,
    object_name: str,
    finrez_pre: float,
    peer_items: List[Dict[str, Any]],
) -> Dict[str, Any]:
    if level == 'business':
        return {
            'status': 'ok',
            'label': SIGNAL_LABELS_RU['ok'],
            'comment': 'базовый уровень периода',
            'reason': 'finrez_pre',
            'reason_value': _round(finrez_pre),
            'rank': 'BASE',
            'priority': 'high',
            'quartiles': None,
            'problem_money': _round(abs(finrez_pre)) if finrez_pre < 0 else 0.0,
        }

    values = [float(item.get('finrez_pre', 0.0)) for item in peer_items]
    if not values:
        return {
            'status': 'ok',
            'label': SIGNAL_LABELS_RU['ok'],
            'comment': 'нет базы для сигнала периода',
            'reason': 'finrez_pre',
            'reason_value': _round(finrez_pre),
            'rank': 'BASE',
            'priority': 'low',
            'quartiles': None,
            'problem_money': 0.0,
        }

    quartiles = _build_quartile_bounds(values)
    q1 = quartiles['q1']
    q2 = quartiles['q2']
    q3 = quartiles['q3']

    status = _detect_status(finrez_pre, q1, q2, q3)
    rank = _detect_rank(finrez_pre, q1, q2, q3)
    comment = _comment_for_status(status)

    problem_items: List[Dict[str, Any]] = []
    for peer in peer_items:
        peer_finrez = float(peer.get('finrez_pre', 0.0))
        peer_status = _detect_status(peer_finrez, q1, q2, q3)
        if peer_status not in {'critical', 'risk'}:
            continue
        problem_items.append({
            'object_name': peer.get('object_name'),
            'problem_money': _problem_money(peer_finrez, q2),
        })

    priorities = _assign_priorities(problem_items)
    priority = priorities.get(object_name, 'low') if status in {'critical', 'risk'} else 'low'
    problem_money = _problem_money(finrez_pre, q2) if status in {'critical', 'risk'} else 0.0

    return {
        'status': status,
        'label': SIGNAL_LABELS_RU[status],
        'comment': comment,
        'reason': 'finrez_pre',
        'reason_value': _round(finrez_pre),
        'rank': rank,
        'priority': priority,
        'quartiles': quartiles,
        'problem_money': _round(problem_money),
    }
