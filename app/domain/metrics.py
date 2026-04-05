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
EFFECT_METRICS = ['finrez_pre', 'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs']

METRIC_TYPES = {
    'finrez_pre': 'income',
    'retro_bonus': 'cost',
    'logistics_cost': 'cost',
    'personnel_cost': 'cost',
    'other_costs': 'cost',
}

MIN_MEDIAN_SAMPLE = 3


def _safe_percent(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return round_percent((numerator / denominator) * 100.0)


def aggregate_metrics(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    SKU-driven aggregation.

    Все верхние уровни считаются только через сумму базовых значений.
    Никаких средневзвешенных margin/markup поверх готовых процентов.
    """
    revenue = round_money(sum(r.get('revenue', 0.0) for r in rows))
    cost = round_money(sum(r.get('cost', 0.0) for r in rows))
    gross_profit = round_money(sum(r.get('gross_profit', 0.0) for r in rows))
    retro_bonus = round_money(sum(r.get('retro_bonus', 0.0) for r in rows))
    logistics_cost = round_money(sum(r.get('logistics_cost', 0.0) for r in rows))
    personnel_cost = round_money(sum(r.get('personnel_cost', 0.0) for r in rows))
    other_costs = round_money(sum(r.get('other_costs', 0.0) for r in rows))
    finrez_pre = round_money(sum(r.get('finrez_pre', 0.0) for r in rows))

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
        'margin_pre': margin_pre,
        'markup': markup,
        'kpi_gap': kpi_gap,
    }


def build_expected_metrics(
    object_metrics: Dict[str, float],
    business_metrics: Dict[str, float],
) -> Tuple[Dict[str, float], bool, bool]:
    business_revenue = business_metrics['revenue']
    object_revenue = object_metrics['revenue']

    invalid_benchmark = business_revenue == 0
    negative_benchmark = False

    expected = {}

    for metric in EFFECT_METRICS:
        if invalid_benchmark:
            expected_value = 0.0
        else:
            expected_value = (business_metrics.get(metric, 0.0) / business_revenue) * object_revenue

        expected_value = round_money(expected_value)

        if expected_value < 0:
            negative_benchmark = True

        expected[metric] = expected_value

    return expected, invalid_benchmark, negative_benchmark


def build_gaps(
    object_metrics: Dict[str, float],
    expected_metrics: Dict[str, float],
) -> Dict[str, float]:
    gaps = {}
    for metric in EFFECT_METRICS:
        gaps[metric] = round_money(object_metrics[metric] - expected_metrics[metric])
    return gaps


def interpret_effect(metric: str, effect_value: float) -> Dict[str, Any]:
    metric_type = METRIC_TYPES[metric]

    if metric_type == 'cost':
        is_negative_for_business = effect_value > 0
    else:
        is_negative_for_business = effect_value < 0

    effect_direction = 'loss' if is_negative_for_business else 'gain'

    return {
        'effect_value': round_money(effect_value),
        'effect_direction': effect_direction,
        'type': metric_type,
        'is_negative_for_business': is_negative_for_business,
    }


def build_effects(gaps_by_metric: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    effects = {}
    for metric, gap_value in gaps_by_metric.items():
        effects[metric] = interpret_effect(metric, gap_value)
    return effects


def compute_margin_gap(object_metrics: Dict[str, float], business_metrics: Dict[str, float]) -> float:
    return round_percent(object_metrics['margin_pre'] - business_metrics['margin_pre'])


def compute_total_loss(effects_by_metric: Dict[str, Dict[str, Any]]) -> float:
    total = 0.0
    for payload in effects_by_metric.values():
        if payload['is_negative_for_business']:
            total += abs(payload['effect_value'])
    return round_money(total)


def compute_per_metric_effects(effects_by_metric: Dict[str, Dict[str, Any]]) -> Dict[str, float]:
    return {metric: round_money(payload['effect_value']) for metric, payload in effects_by_metric.items()}


def compute_loss_share(total_loss: float, business_metrics: Dict[str, float]) -> float:
    business_finrez_abs = abs(business_metrics['finrez_pre'])
    if business_finrez_abs <= 0:
        return 0.0
    return round_percent((total_loss / business_finrez_abs) * 100.0)


def detect_status(finrez_pre: float, kpi_zone: Optional[str]) -> str:
    if finrez_pre < 0:
        return 'critical'
    if kpi_zone == 'критично':
        return 'critical'
    if kpi_zone == 'риск':
        return 'risk'
    return 'ok'


def detect_priority(status: str, total_loss: float, loss_share: float, kpi_gap: float, margin_gap: float) -> str:
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
        'network': 'спуститься до категорий',
        'category': 'спуститься до групп ТМЦ',
        'tmc_group': 'спуститься до SKU',
        'sku': 'перейти к решению по SKU',
    }
    return chain.get(level, 'уточнить объект')


def detect_suggested_action(status: str, priority: str, top_drain_metric: str, level: str) -> str:
    if status == 'critical' and level == 'sku':
        return 'проверить экономику SKU и принять решение по контракту или цене'
    if status == 'critical':
        return 'сразу провалиться глубже и проверить главный дренаж'
    if priority == 'high' and top_drain_metric:
        return f'разобрать отклонение по {top_drain_metric} и найти денежный источник потери'
    if priority == 'medium':
        return 'провести drill-down и подтвердить структуру отклонения'
    return 'наблюдать и контролировать без срочной эскалации'


def compute_median_gap(items: List[Dict[str, Any]]) -> Optional[float]:
    valid = [item['object_metrics']['kpi_gap'] for item in items if item['object_metrics']['revenue'] > 0]
    if len(valid) < MIN_MEDIAN_SAMPLE:
        return None
    return round(float(median(valid)), 2)


def detect_kpi_zone(kpi_gap: float, median_gap: Optional[float]) -> Optional[str]:
    if median_gap is None:
        return None

    delta = round(kpi_gap - median_gap, 2)
    if delta <= -5:
        return 'хорошо'
    if delta <= 5:
        return 'норма'
    if delta <= 15:
        return 'риск'
    return 'критично'
