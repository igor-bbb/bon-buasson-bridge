from typing import Any, Dict, List, Tuple


MIN_VISIBLE_PROBLEM_MONEY = 1000.0
PARETO_SHARE = 0.80
DEFAULT_LIMIT = 5
FULL_VIEW_MARKERS = {'все', 'покажи все', 'полный список', 'full', 'show all'}


def round_money(value: float) -> float:
    return round(float(value or 0.0), 2)


def pick_top_drain(effects_by_metric: Dict[str, Dict[str, Any]], low_volume: bool) -> Tuple[str, float, bool]:
    if low_volume:
        return '', 0.0, False

    negative_items = []
    for metric, payload in effects_by_metric.items():
        if payload['is_negative_for_business']:
            negative_items.append((metric, payload['effect_value'], True))

    if not negative_items:
        return '', 0.0, False

    top_metric, top_effect, top_negative = max(negative_items, key=lambda x: abs(x[1]))
    return top_metric, round_money(top_effect), top_negative


def sort_items_by_top_problem(items: List[Dict[str, Any]]) -> None:
    def sort_key(item: Dict[str, Any]):
        flags = item.get('flags', {})
        low_volume = flags.get('low_volume', False)
        signal = item.get('signal', {})
        problem_money = abs(float(signal.get('problem_money', 0.0) or 0.0))
        top_effect = abs(float(item.get('top_drain_effect', 0.0) or 0.0))
        finrez_pre = float(item.get('metrics', {}).get('object_metrics', {}).get('finrez_pre', 0.0) or 0.0)

        if low_volume:
            return (1, 0.0, 0.0, 0.0)

        return (0, -problem_money, -top_effect, finrez_pre)

    items.sort(key=sort_key)


def _build_items_meta(total_count: int, returned_count: int) -> Dict[str, Any]:
    hidden_count = max(total_count - returned_count, 0)
    return {
        'total_count': total_count,
        'returned_count': returned_count,
        'hidden_count': hidden_count,
        'has_more': hidden_count > 0,
    }


def _is_meaningful_problem(item: Dict[str, Any]) -> bool:
    signal = item.get('signal', {})
    problem_money = abs(float(signal.get('problem_money', 0.0) or 0.0))
    status = signal.get('status')

    if status in {'critical', 'risk'} and problem_money >= MIN_VISIBLE_PROBLEM_MONEY:
        return True

    finrez_pre = float(item.get('metrics', {}).get('object_metrics', {}).get('finrez_pre', 0.0) or 0.0)
    return finrez_pre < 0 and abs(finrez_pre) >= MIN_VISIBLE_PROBLEM_MONEY


def select_visible_items(
    items: List[Dict[str, Any]],
    *,
    full_view: bool = False,
    limit: int = DEFAULT_LIMIT,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    if not items:
        return [], _build_items_meta(0, 0)

    total_count = len(items)
    sorted_items = list(items)
    sort_items_by_top_problem(sorted_items)

    if full_view:
        return sorted_items, _build_items_meta(total_count, len(sorted_items))

    problem_candidates = [item for item in sorted_items if _is_meaningful_problem(item)]

    if not problem_candidates:
        visible = sorted_items[:limit]
        return visible, _build_items_meta(total_count, len(visible))

    total_problem_money = sum(abs(float(item.get('signal', {}).get('problem_money', 0.0) or 0.0)) for item in problem_candidates)

    visible: List[Dict[str, Any]] = []
    if total_problem_money > 0:
        cumulative = 0.0
        for item in problem_candidates:
            problem_money = abs(float(item.get('signal', {}).get('problem_money', 0.0) or 0.0))
            visible.append(item)
            cumulative += problem_money / total_problem_money
            if cumulative >= PARETO_SHARE and len(visible) >= min(3, limit):
                break

    if not visible:
        visible = problem_candidates[:limit]

    if len(visible) < limit:
        existing = {id(item) for item in visible}
        for item in sorted_items:
            if id(item) in existing:
                continue
            visible.append(item)
            if len(visible) >= limit:
                break

    return visible[:limit], _build_items_meta(total_count, min(len(visible), limit))
