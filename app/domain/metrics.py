from statistics import median
from typing import Any, Dict, List, Optional, Tuple

from app.domain.normalization import round_money, round_percent

MONEY_METRICS = [
    'revenue',
    'cost',
    'gross_profit',
    'retro_bonus',
    'logistics_cost',
    'personnel_cost',
    'other_costs',
    'finrez_pre',
]

EFFECT_METRICS = [
    'retro_bonus',
    'logistics_cost',
    'personnel_cost',
    'other_costs',
]

NEGATIVE_EFFECT_METRICS = {
    'retro_bonus',
    'logistics_cost',
    'personnel_cost',
    'other_costs',
}

MIN_MEDIAN_SAMPLE = 3


def _to_float(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _safe_percent(numerator: float, denominator: float) -> float:
    if abs(denominator) < 1e-9:
        return 0.0
    return round_percent((numerator / denominator) * 100.0)


def aggregate_margin_pre_from_rows(rows: List[Dict[str, Any]]) -> Optional[float]:
    if not rows:
        return None
    revenue = round_money(sum(_to_float(r.get('revenue')) for r in rows))
    finrez_pre = round_money(sum(_to_float(r.get('finrez_pre')) for r in rows))
    return _safe_percent(finrez_pre, revenue)


def aggregate_metrics(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    revenue = round_money(sum(_to_float(r.get('revenue')) for r in rows))
    cost = round_money(sum(_to_float(r.get('cost')) for r in rows))
    gross_profit = round_money(sum(_to_float(r.get('gross_profit')) for r in rows))
    retro_bonus = round_money(sum(_to_float(r.get('retro_bonus')) for r in rows))
    logistics_cost = round_money(sum(_to_float(r.get('logistics_cost')) for r in rows))
    personnel_cost = round_money(sum(_to_float(r.get('personnel_cost')) for r in rows))
    other_costs = round_money(sum(_to_float(r.get('other_costs')) for r in rows))
    finrez_pre = round_money(sum(_to_float(r.get('finrez_pre')) for r in rows))
    finrez_final = round_money(
        finrez_pre
        - retro_bonus
        - logistics_cost
        - personnel_cost
        - other_costs
    )

    margin_pre = _safe_percent(finrez_pre, revenue)
    markup = _safe_percent(gross_profit, cost)
    kpi_gap = round_percent(markup - margin_pre)

    return {
        'revenue': revenue,
        'cost': cost,
        'gross_profit': gross_profit,
        'retro_bonus': retro_bonus,
        'logistics_cost': logistics_cost,
        'personnel_cost': personnel_cost,
        'other_costs': other_costs,
        'finrez_pre': finrez_pre,
        'finrez_final': finrez_final,
        'margin_pre': margin_pre,
        'markup': markup,
        'kpi_gap': kpi_gap,
    }


def build_consistency(
    parent_finrez_pre: float,
    child_metrics_list: List[Dict[str, Any]],
    child_level: str,
) -> Dict[str, Any]:
    if not child_metrics_list:
        return {
            'checked': False,
            'reason': 'no_child_data',
        }

    child_sum = round_money(sum(_to_float(item.get('finrez_pre')) for item in child_metrics_list))
    delta = round_money(_to_float(parent_finrez_pre) - child_sum)

    if abs(parent_finrez_pre) < 1e-9:
        delta_pct = 0.0 if abs(delta) < 1e-9 else None
    else:
        delta_pct = round_percent((abs(delta) / abs(parent_finrez_pre)) * 100.0)

    if delta_pct is None:
        status = 'warning'
    elif delta_pct < 1:
        status = 'ok'
    elif delta_pct <= 5:
        status = 'warning'
    else:
        status = 'critical'

    return {
        'checked': True,
        'child_level': child_level,
        'parent_finrez_pre': round_money(parent_finrez_pre),
        'child_sum_finrez_pre': child_sum,
        'delta': delta,
        'delta_pct': delta_pct,
        'status': status,
    }


def build_expected_metrics(
    object_metrics: Dict[str, float],
    business_metrics: Dict[str, float],
) -> Tuple[Dict[str, float], bool, bool]:
    business_revenue = _to_float(business_metrics.get('revenue'))
    object_revenue = _to_float(object_metrics.get('revenue'))

    invalid_benchmark = abs(business_revenue) < 1e-9
    negative_benchmark = False
    expected: Dict[str, float] = {}

    for metric in EFFECT_METRICS:
        if invalid_benchmark:
            expected_value = 0.0
        else:
            ratio = _to_float(business_metrics.get(metric)) / business_revenue
            expected_value = ratio * object_revenue

        expected_value = round_money(expected_value)
        if expected_value < 0:
            negative_benchmark = True
        expected[metric] = expected_value

    return expected, invalid_benchmark, negative_benchmark


def build_gaps(
    object_metrics: Dict[str, float],
    expected_metrics: Dict[str, float],
) -> Dict[str, float]:
    gaps: Dict[str, float] = {}
    for metric in EFFECT_METRICS:
        gaps[metric] = round_money(_to_float(object_metrics.get(metric)) - _to_float(expected_metrics.get(metric)))
    return gaps


def interpret_effect(metric: str, effect_value: float) -> Dict[str, Any]:
    is_negative_for_business = metric in NEGATIVE_EFFECT_METRICS and effect_value > 0
    effect_direction = 'loss' if is_negative_for_business else 'gain'
    return {
        'effect_value': round_money(effect_value),
        'effect_direction': effect_direction,
        'type': 'cost',
        'is_negative_for_business': is_negative_for_business,
    }


def build_effects(gaps_by_metric: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    return {metric: interpret_effect(metric, value) for metric, value in gaps_by_metric.items()}


def compute_margin_gap(object_metrics: Dict[str, float], business_metrics: Dict[str, float]) -> float:
    return round_percent(_to_float(object_metrics.get('margin_pre')) - _to_float(business_metrics.get('margin_pre')))


def compute_gap_money(object_metrics: Dict[str, float], business_metrics: Dict[str, float]) -> float:
    revenue = _to_float(object_metrics.get('revenue'))
    object_margin = _to_float(object_metrics.get('margin_pre'))
    business_margin = _to_float(business_metrics.get('margin_pre'))
    gap_pp = business_margin - object_margin
    if gap_pp <= 0 or revenue <= 0:
        return 0.0
    return round_money((gap_pp / 100.0) * revenue)


def compute_total_loss(effects_by_metric: Dict[str, Dict[str, Any]]) -> float:
    total = 0.0
    for payload in effects_by_metric.values():
        if payload.get('is_negative_for_business'):
            total += abs(_to_float(payload.get('effect_value')))
    return round_money(total)


def compute_per_metric_effects(effects_by_metric: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    return {
        metric: round_money(_to_float(payload.get('effect_value')))
        for metric, payload in effects_by_metric.items()
    }


def compute_loss_share(total_loss: float, business_metrics: Dict[str, float]) -> float:
    business_finrez_abs = abs(_to_float(business_metrics.get('finrez_pre')))
    if business_finrez_abs <= 0:
        return 0.0
    return round_percent((total_loss / business_finrez_abs) * 100.0)


def compute_median_gap(items: List[Dict[str, Any]]) -> Optional[float]:
    values: List[float] = []
    for item in items:
        metrics = item.get('object_metrics') or item.get('metrics') or {}
        value = metrics.get('kpi_gap')
        if value is None:
            continue
        values.append(_to_float(value))

    if len(values) < MIN_MEDIAN_SAMPLE:
        return None

    return round_percent(float(median(values)))


def detect_kpi_zone(kpi_gap: float, median_gap: Optional[float]) -> str:
    if median_gap is None:
        if kpi_gap >= 20:
            return 'критично'
        if kpi_gap >= 10:
            return 'риск'
        return 'норма'

    if kpi_gap >= median_gap + 10:
        return 'критично'
    if kpi_gap >= median_gap:
        return 'риск'
    return 'норма'


def detect_status(finrez_pre: float, kpi_zone: Optional[str]) -> str:
    if finrez_pre < 0:
        return 'critical'
    if kpi_zone == 'критично':
        return 'critical'
    if kpi_zone == 'риск':
        return 'risk'
    return 'ok'


def detect_priority(
    status: str,
    total_loss: float,
    loss_share: float,
    kpi_gap: float,
    margin_gap: float,
) -> str:
    if status == 'critical' and (total_loss >= 1000 or loss_share >= 20 or kpi_gap >= 20 or margin_gap <= -5):
        return 'high'
    if status == 'critical':
        return 'medium'
    if total_loss >= 1000 or loss_share >= 20 or kpi_gap >= 20 or margin_gap <= -5:
        return 'high'
    if total_loss >= 200 or loss_share >= 5 or kpi_gap >= 10 or margin_gap < 0:
        return 'medium'
    return 'low'


def detect_next_step(level: str) -> str:
    chain = {
        'business': 'спуститься до топ-менеджеров',
        'manager_top': 'спуститься до менеджеров',
        'manager': 'спуститься до сетей',
        'network': 'спуститься до SKU',
        'category': 'спуститься до SKU',
        'tmc_group': 'спуститься до SKU',
        'sku': 'принять решение по SKU',
    }
    return chain.get(level, 'уточнить объект')


def detect_suggested_action(status: str, priority: str, top_drain_metric: Optional[str], level: str) -> str:
    if status == 'critical' and level == 'sku':
        return 'проверить цену, контракт и экономику SKU'
    if status == 'critical':
        return 'сразу провалиться глубже и проверить главный дренаж'
    if priority == 'high' and top_drain_metric:
        return f'разобрать отклонение по {top_drain_metric}'
    if top_drain_metric:
        return f'проверить {top_drain_metric}'
    return 'контроль без эскалации'
