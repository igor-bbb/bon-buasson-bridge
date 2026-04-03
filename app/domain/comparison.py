from collections import defaultdict
from typing import Any, Dict, List

from app.domain.filters import filter_rows, get_normalized_rows
from app.config import LOW_VOLUME_THRESHOLD

from app.domain.metrics import (
    aggregate_metrics,
    build_effects,
    build_expected_metrics,
    build_gaps,
    compute_loss_share,
    compute_margin_gap,
    compute_median_gap,
    compute_per_metric_effects,
    compute_total_loss,
    detect_kpi_zone,
    detect_next_step,
    detect_priority,
    detect_status,
    detect_suggested_action,
)
from app.domain.sorting import pick_top_drain


CHILD_LEVEL_BY_LEVEL = {
    'business': 'manager',
    'manager_top': 'manager',
    'manager': 'network',
    'network': 'category',
    'category': 'tmc_group',
    'tmc_group': 'sku',
}


def build_flags(
    object_metrics: Dict[str, float],
    invalid_benchmark: bool,
    negative_benchmark: bool,
) -> Dict[str, bool]:
    low_volume = object_metrics['revenue'] < LOW_VOLUME_THRESHOLD
    return {
        'low_volume': low_volume,
        'invalid_benchmark': invalid_benchmark,
        'negative_benchmark': negative_benchmark,
    }


def _group_rows_by_level(rows: List[Dict[str, Any]], level: str) -> List[List[Dict[str, Any]]]:
    if level == 'business':
        return [rows]

    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        name = row.get(level, '')
        if name == '':
            continue
        groups[name].append(row)
    return list(groups.values())


def compute_level_median_gap(level: str, period: str) -> Any:
    if level == 'business':
        return None

    rows = get_normalized_rows()
    rows = filter_rows(rows, period=period)

    grouped_rows = _group_rows_by_level(rows, level)
    items = []
    for chunk in grouped_rows:
        items.append({'object_metrics': aggregate_metrics(chunk)})
    return compute_median_gap(items)


def build_comparison_payload(
    level: str,
    object_name: str,
    object_metrics: Dict[str, float],
    business_metrics: Dict[str, float],
    period: str,
) -> Dict[str, Any]:
    expected_metrics, invalid_benchmark, negative_benchmark = build_expected_metrics(
        object_metrics=object_metrics,
        business_metrics=business_metrics,
    )

    gaps_by_metric = build_gaps(
        object_metrics=object_metrics,
        expected_metrics=expected_metrics,
    )

    effects_by_metric = build_effects(gaps_by_metric)

    flags = build_flags(
        object_metrics=object_metrics,
        invalid_benchmark=invalid_benchmark,
        negative_benchmark=negative_benchmark,
    )

    top_drain_metric, top_drain_effect, top_drain_is_negative_for_business = pick_top_drain(
        effects_by_metric=effects_by_metric,
        low_volume=flags['low_volume'],
    )

    median_gap = compute_level_median_gap(level=level, period=period)
    kpi_zone = detect_kpi_zone(object_metrics['kpi_gap'], median_gap)
    margin_gap = compute_margin_gap(object_metrics, business_metrics)
    total_loss = compute_total_loss(effects_by_metric)
    per_metric_effects = compute_per_metric_effects(effects_by_metric)
    loss_share = compute_loss_share(total_loss, business_metrics)
    status = detect_status(object_metrics['finrez_pre'], kpi_zone)
    priority = detect_priority(status, total_loss, loss_share, object_metrics['kpi_gap'], margin_gap)
    next_step = detect_next_step(level)
    suggested_action = detect_suggested_action(status, priority, top_drain_metric, level)

    return {
        'level': level,
        'object_name': object_name,
        'period': period,
        'signal': {
            'finrez_pre': object_metrics['finrez_pre'],
            'status': status,
        },
        'navigation': {
            'kpi_gap': object_metrics['kpi_gap'],
            'median_gap': median_gap,
            'kpi_zone': kpi_zone,
        },
        'context': {
            'margin_pre_object': object_metrics['margin_pre'],
            'margin_pre_business': business_metrics['margin_pre'],
            'margin_gap': margin_gap,
        },
        'metrics': {
            'object_metrics': object_metrics,
            'business_metrics': business_metrics,
        },
        'diagnosis': {
            'expected_metrics': expected_metrics,
            'gaps_by_metric': gaps_by_metric,
            'effects_by_metric': effects_by_metric,
        },
        'impact': {
            'total_loss': total_loss,
            'per_metric_effects': per_metric_effects,
        },
        'priority': {
            'loss_share': loss_share,
            'priority': priority,
        },
        'action': {
            'suggested_action': suggested_action,
            'next_step': next_step,
        },
        'object_metrics': object_metrics,
        'business_metrics': business_metrics,
        'expected_metrics': expected_metrics,
        'gaps_by_metric': gaps_by_metric,
        'effects_by_metric': effects_by_metric,
        'margin_gap': margin_gap,
        'median_gap': median_gap,
        'kpi_zone': kpi_zone,
        'top_drain_metric': top_drain_metric,
        'top_drain_effect': top_drain_effect,
        'top_drain_is_negative_for_business': top_drain_is_negative_for_business,
        'total_loss': total_loss,
        'loss_share': loss_share,
        'status': status,
        'flags': flags,
    }


def get_business_comparison(period: str) -> Dict[str, Any]:
    rows = get_normalized_rows()
    business_rows = filter_rows(rows, period=period)

    if not business_rows:
        return {'error': 'business not found or no data'}

    business_metrics = aggregate_metrics(business_rows)
    return build_comparison_payload(
        level='business',
        object_name='business',
        object_metrics=business_metrics,
        business_metrics=business_metrics,
        period=period,
    )


def _single_object_comparison(level: str, period: str, **filters: Any) -> Dict[str, Any]:
    rows = get_normalized_rows()

    object_rows = filter_rows(rows, period=period, **filters)
    business_rows = filter_rows(rows, period=period)

    if not object_rows:
        return {'error': f'{level} not found or no data'}

    object_metrics = aggregate_metrics(object_rows)
    business_metrics = aggregate_metrics(business_rows)
    object_name = next(iter(filters.values()))

    return build_comparison_payload(
        level=level,
        object_name=object_name,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
        period=period,
    )


def get_manager_top_comparison(manager_top: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('manager_top', period, manager_top=manager_top)


def get_manager_comparison(manager: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('manager', period, manager=manager)


def get_network_comparison(network: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('network', period, network=network)


def get_category_comparison(category: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('category', period, category=category)


def get_tmc_group_comparison(tmc_group: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('tmc_group', period, tmc_group=tmc_group)


def get_sku_comparison(sku: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('sku', period, sku=sku)
