from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from app.config import LOW_VOLUME_THRESHOLD
from app.domain.filters import filter_rows, get_normalized_rows
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
    detect_priority,
    detect_status,
    detect_suggested_action,
)
from app.domain.signals import build_period_signal
from app.domain.sorting import pick_top_drain


CHILD_LEVEL_BY_LEVEL = {
    'business': 'manager_top',
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
    low_volume = object_metrics.get('revenue', 0.0) < LOW_VOLUME_THRESHOLD
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
        name = row.get(level)
        if not name:
            continue
        groups[name].append(row)

    return list(groups.values())


MEDIAN_CACHE: Dict[Tuple[str, str], Optional[float]] = {}
PEER_SIGNAL_CACHE: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}


def compute_level_median_gap(level: str, period: str) -> Optional[float]:
    cache_key = (level, period)

    if cache_key in MEDIAN_CACHE:
        return MEDIAN_CACHE[cache_key]

    if level == 'business':
        MEDIAN_CACHE[cache_key] = None
        return None

    rows = get_normalized_rows()
    rows, _ = filter_rows(rows, period=period)

    if not rows:
        MEDIAN_CACHE[cache_key] = None
        return None

    grouped_rows = _group_rows_by_level(rows, level)

    items = []
    for chunk in grouped_rows:
        items.append({'object_metrics': aggregate_metrics(chunk)})

    median = compute_median_gap(items)
    MEDIAN_CACHE[cache_key] = median

    return median


def compute_level_signal_peers(level: str, period: str) -> List[Dict[str, Any]]:
    cache_key = (level, period)
    if cache_key in PEER_SIGNAL_CACHE:
        return PEER_SIGNAL_CACHE[cache_key]

    rows = get_normalized_rows()
    rows, _ = filter_rows(rows, period=period)
    if not rows:
        PEER_SIGNAL_CACHE[cache_key] = []
        return []

    if level == 'business':
        metrics = aggregate_metrics(rows)
        peers = [{'object_name': 'business', 'finrez_pre': metrics.get('finrez_pre', 0.0)}]
        PEER_SIGNAL_CACHE[cache_key] = peers
        return peers

    grouped_rows = _group_rows_by_level(rows, level)
    peers: List[Dict[str, Any]] = []
    for chunk in grouped_rows:
        first = chunk[0] if chunk else {}
        object_name = first.get(level)
        metrics = aggregate_metrics(chunk)
        peers.append({
            'object_name': object_name,
            'finrez_pre': metrics.get('finrez_pre', 0.0),
        })

    PEER_SIGNAL_CACHE[cache_key] = peers
    return peers


def _compute_gap_loss_money(margin_gap: float, revenue: float) -> float:
    if margin_gap >= 0:
        return 0.0
    return round(abs(margin_gap) / 100.0 * revenue, 2)


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
    kpi_zone = detect_kpi_zone(object_metrics.get('kpi_gap', 0.0), median_gap)

    margin_gap = compute_margin_gap(object_metrics, business_metrics)
    total_loss = compute_total_loss(effects_by_metric)
    per_metric_effects = compute_per_metric_effects(effects_by_metric)
    loss_share = compute_loss_share(total_loss, business_metrics)
    gap_loss_money = _compute_gap_loss_money(margin_gap, object_metrics.get('revenue', 0.0))

    diagnostic_status = detect_status(object_metrics.get('finrez_pre', 0.0), kpi_zone)
    priority = detect_priority(
        diagnostic_status,
        total_loss,
        loss_share,
        object_metrics.get('kpi_gap', 0.0),
        margin_gap,
    )

    next_step = CHILD_LEVEL_BY_LEVEL.get(level)
    suggested_action = detect_suggested_action(diagnostic_status, priority, top_drain_metric, level)
    period_signal = build_period_signal(
        level=level,
        object_name=object_name,
        finrez_pre=object_metrics.get('finrez_pre', 0.0),
        peer_items=compute_level_signal_peers(level, period),
    )

    return {
        'level': level,
        'object_name': object_name,
        'period': period,

        'signal': {
            'status': period_signal.get('status'),
            'label': period_signal.get('label'),
            'comment': period_signal.get('comment'),
            'reason': period_signal.get('reason'),
            'reason_value': period_signal.get('reason_value'),
            'rank': period_signal.get('rank'),
            'priority': period_signal.get('priority'),
            'problem_money': period_signal.get('problem_money'),
            'quartiles': period_signal.get('quartiles'),
            'finrez_pre': object_metrics.get('finrez_pre'),
            'margin_gap': margin_gap,
            'kpi_gap': object_metrics.get('kpi_gap'),
            'median_gap': median_gap,
            'kpi_zone': kpi_zone,
            'diagnostic_status': diagnostic_status,
        },

        'navigation': {
            'kpi_gap': object_metrics.get('kpi_gap'),
            'median_gap': median_gap,
            'kpi_zone': kpi_zone,
        },

        'context': {
            'margin_pre_object': object_metrics.get('margin_pre'),
            'margin_pre_business': business_metrics.get('margin_pre'),
            'margin_gap': margin_gap,
            'costs': {
                'retro_bonus': object_metrics.get('retro_bonus'),
                'logistics_cost': object_metrics.get('logistics_cost'),
                'personnel_cost': object_metrics.get('personnel_cost'),
                'other_costs': object_metrics.get('other_costs'),
            },
        },

        'metrics': {
            'object_metrics': object_metrics,
            'business_metrics': business_metrics,
        },

        'diagnosis': {
            'expected_metrics': expected_metrics,
            'gaps_by_metric': gaps_by_metric,
            'effects_by_metric': effects_by_metric,
            'top_drain_metric': top_drain_metric,
            'top_drain_effect': top_drain_effect,
            'top_drain_is_negative_for_business': top_drain_is_negative_for_business,
        },

        'impact': {
            'total_loss': total_loss,
            'gap_loss_money': gap_loss_money,
            'per_metric_effects': per_metric_effects,
            'cost_structure': {
                'retro_bonus': object_metrics.get('retro_bonus'),
                'logistics_cost': object_metrics.get('logistics_cost'),
                'personnel_cost': object_metrics.get('personnel_cost'),
                'other_costs': object_metrics.get('other_costs'),
            },
        },

        'priority': {
            'loss_share': loss_share,
            'priority': priority,
        },

        'action': {
            'suggested_action': suggested_action,
            'next_step': next_step,
        },

        'flags': flags,

        'top_drain_metric': top_drain_metric,
        'top_drain_effect': top_drain_effect,
        'top_drain_is_negative_for_business': top_drain_is_negative_for_business,
    }


def _handle_empty_filter(meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'error': 'no data after filtering',
        'reason': meta.get('empty_reason'),
        'trace': meta.get('trace'),
    }


def get_business_comparison(period: str) -> Dict[str, Any]:
    rows = get_normalized_rows()
    business_rows, meta = filter_rows(rows, period=period)

    if not business_rows:
        return _handle_empty_filter(meta)

    business_metrics = aggregate_metrics(business_rows)

    return build_comparison_payload(
        level='business',
        object_name='business',
        object_metrics=business_metrics,
        business_metrics=business_metrics,
        period=period,
    )


def _single_object_comparison(
    level: str,
    object_name: str,
    period: str,
    **filters: Any
) -> Dict[str, Any]:
    rows = get_normalized_rows()

    object_rows, object_meta = filter_rows(rows, period=period, **filters)
    business_rows, business_meta = filter_rows(rows, period=period)

    if not object_rows:
        return _handle_empty_filter(object_meta)

    if not business_rows:
        return _handle_empty_filter(business_meta)

    object_metrics = aggregate_metrics(object_rows)
    business_metrics = aggregate_metrics(business_rows)

    return build_comparison_payload(
        level=level,
        object_name=object_name,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
        period=period,
    )


def get_manager_top_comparison(manager_top: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('manager_top', manager_top, period, manager_top=manager_top)


def get_manager_comparison(manager: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('manager', manager, period, manager=manager)


def get_network_comparison(network: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('network', network, period, network=network)


def get_category_comparison(category: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('category', category, period, category=category)


def get_tmc_group_comparison(tmc_group: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('tmc_group', tmc_group, period, tmc_group=tmc_group)


def get_sku_comparison(sku: str, period: str) -> Dict[str, Any]:
    return _single_object_comparison('sku', sku, period, sku=sku)
