from collections import defaultdict

from app.config import AI_FOCUS_SHARE_THRESHOLD
from typing import Any, Dict, List, Optional, Tuple

from app.domain.consistency import build_consistency_from_rows
from app.domain.filters import filter_rows, get_normalized_rows
from app.domain.metrics import (
    aggregate_margin_pre_from_rows,
    aggregate_metrics,
    build_effects,
    build_expected_metrics,
    build_gaps,
    compute_gap_money,
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
from app.domain.signals import build_period_signal

FILTER_DIMENSIONS = [
    'business',
    'manager_top',
    'manager',
    'network',
    'category',
    'tmc_group',
    'sku',
]

CHILD_LEVEL_BY_LEVEL = {
    'business': 'manager_top',
    'manager_top': 'manager',
    'manager': 'network',
    'network': 'sku',
    'category': 'sku',
    'tmc_group': 'sku',
}


def _safe_get_rows() -> List[Dict[str, Any]]:
    try:
        return get_normalized_rows()
    except Exception:
        return []


def _run_filter(rows: List[Dict[str, Any]], period: str, **kwargs: Any):
    try:
        result = filter_rows(rows, period=period, **kwargs)
    except TypeError:
        result = filter_rows(period=period, **kwargs)

    if isinstance(result, tuple) and len(result) == 2:
        return result

    return result, {}


def _normalize_filter_payload(filter_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    payload = dict(filter_payload or {})
    return {
        k: v
        for k, v in payload.items()
        if k in FILTER_DIMENSIONS and v not in (None, '')
    }


def _merge_filters(filter_payload: Optional[Dict[str, Any]], **filters: Any) -> Dict[str, Any]:
    merged = _normalize_filter_payload(filter_payload)
    for key, value in filters.items():
        if key in FILTER_DIMENSIONS and value not in (None, ''):
            merged[key] = value
    merged.pop('business', None)
    return merged


def _validate_required_parent(level: str, merged_filters: Dict[str, Any]) -> Optional[str]:
    required_parent = {
        'sku': ['network'],
    }.get(level, [])

    for key in required_parent:
        if not merged_filters.get(key):
            return f'{level} requires parent filter: {key}'
    return None


def _group_rows_by_level(rows: List[Dict[str, Any]], level: str) -> List[List[Dict[str, Any]]]:
    if level == 'business':
        return [rows]

    grouped: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        name = row.get(level)
        if not name:
            continue
        grouped[name].append(row)
    return list(grouped.values())


MEDIAN_CACHE: Dict[Tuple[str, str], Optional[float]] = {}


def compute_level_median_gap(level: str, period: str) -> Optional[float]:
    cache_key = (level, period)
    if cache_key in MEDIAN_CACHE:
        return MEDIAN_CACHE[cache_key]

    if level == 'business':
        MEDIAN_CACHE[cache_key] = None
        return None

    rows = _safe_get_rows()
    rows, _ = _run_filter(rows, period=period)
    if not rows:
        MEDIAN_CACHE[cache_key] = None
        return None

    grouped_rows = _group_rows_by_level(rows, level)
    items = [{'object_metrics': aggregate_metrics(chunk)} for chunk in grouped_rows]
    value = compute_median_gap(items)
    MEDIAN_CACHE[cache_key] = value
    return value


def _build_level_signal(
    level: str,
    period: str,
    object_name: str,
    object_metrics: Dict[str, float],
    object_rows: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    current_kpi_gap = float(object_metrics.get('kpi_gap', 0.0) or 0.0)

    if level == 'business':
        return build_period_signal(
            level=level,
            object_name=object_name,
            metric_value=current_kpi_gap,
            peer_items=[],
            metric_name='разрыв',
        )

    rows = _safe_get_rows()
    rows, _ = _run_filter(rows, period=period)
    if not rows:
        return build_period_signal(
            level=level,
            object_name=object_name,
            metric_value=current_kpi_gap,
            peer_items=[],
            metric_name='разрыв',
        )

    grouped_rows = _group_rows_by_level(rows, level)
    peer_items = []
    for chunk in grouped_rows:
        signal_metrics = aggregate_metrics(chunk)
        signal_kpi_gap = signal_metrics.get('kpi_gap')
        if signal_kpi_gap is None:
            continue
        peer_items.append({
            'object_name': chunk[0].get(level),
            'metric_value': signal_kpi_gap,
        })

    return build_period_signal(
        level=level,
        object_name=object_name,
        metric_value=current_kpi_gap,
        peer_items=peer_items,
        metric_name='разрыв',
    )




def _build_ai_focus(object_rows: Optional[List[Dict[str, Any]]], level: str) -> Optional[Dict[str, Any]]:
    if level not in {"network", "manager", "manager_top"} or not object_rows:
        return None

    business_margin = aggregate_margin_pre_from_rows(object_rows) or 0.0

    def _group_loss(field: str):
        grouped: Dict[str, float] = defaultdict(float)
        total = 0.0
        for row in object_rows:
            name = row.get(field)
            if not name:
                continue
            revenue = float(row.get('revenue') or 0.0)
            margin_pre = float(row.get('margin_pre') or 0.0)
            gap_pp = business_margin - margin_pre
            loss = round((gap_pp / 100.0) * revenue, 2) if gap_pp > 0 and revenue > 0 else 0.0
            if loss <= 0:
                continue
            grouped[str(name)] += loss
            total += loss
        if total <= 0 or not grouped:
            return None
        name = max(grouped, key=lambda k: grouped[k])
        value = grouped[name]
        share = value / total if total > 0 else 0.0
        return {'name': name, 'loss': round(value, 2), 'share': round(share * 100.0, 2)}

    category_focus = _group_loss('category')
    if category_focus and category_focus['share'] / 100.0 >= AI_FOCUS_SHARE_THRESHOLD:
        return {'focus_type': 'category', **category_focus}
    group_focus = _group_loss('tmc_group')
    if group_focus and group_focus['share'] / 100.0 >= AI_FOCUS_SHARE_THRESHOLD:
        return {'focus_type': 'tmc_group', **group_focus}
    sku_focus = _group_loss('sku')
    if sku_focus:
        return {'focus_type': 'sku', **sku_focus}
    return None

def build_flags(
    object_metrics: Dict[str, float],
    invalid_benchmark: bool,
    negative_benchmark: bool,
) -> Dict[str, bool]:
    revenue = float(object_metrics.get('revenue', 0.0) or 0.0)
    return {
        'low_volume': revenue < 1000,
        'invalid_benchmark': invalid_benchmark,
        'negative_benchmark': negative_benchmark,
    }


def _select_top_driver(per_metric_effects: Dict[str, float]) -> Tuple[Optional[str], float]:
    if not per_metric_effects:
        return None, 0.0

    filtered = {
        k: float(v or 0.0)
        for k, v in per_metric_effects.items()
        if k in {'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs'}
    }
    if not filtered:
        return None, 0.0

    metric = max(filtered, key=lambda key: abs(filtered[key]))
    return metric, filtered[metric]


def build_comparison_payload(
    level: str,
    object_name: str,
    object_metrics: Dict[str, float],
    business_metrics: Dict[str, float],
    period: str,
    object_rows: Optional[List[Dict[str, Any]]] = None,
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

    median_gap = compute_level_median_gap(level=level, period=period)
    kpi_zone = detect_kpi_zone(object_metrics.get('kpi_gap', 0.0), median_gap)

    signal_payload = _build_level_signal(level, period, object_name, object_metrics, object_rows=object_rows)

    margin_gap = compute_margin_gap(object_metrics, business_metrics)
    gap_loss_money = compute_gap_money(object_metrics, business_metrics)
    total_loss = compute_total_loss(effects_by_metric)
    per_metric_effects = compute_per_metric_effects(effects_by_metric)
    loss_share = compute_loss_share(total_loss, business_metrics)

    top_drain_metric, top_drain_effect = _select_top_driver(per_metric_effects)

    status = signal_payload.get('status') or detect_status(
        object_metrics.get('finrez_pre', 0.0),
        kpi_zone,
    )
    priority = detect_priority(
        status,
        total_loss,
        loss_share,
        object_metrics.get('kpi_gap', 0.0),
        margin_gap,
    )
    next_level = CHILD_LEVEL_BY_LEVEL.get(level)
    next_step = detect_next_step(level)
    suggested_action = detect_suggested_action(status, priority, top_drain_metric, level)

    ai_focus = _build_ai_focus(object_rows, level)

    consistency = build_consistency_from_rows(
        level=level,
        parent_finrez_pre=object_metrics.get('finrez_pre', 0.0),
        rows=object_rows,
    )

    return {
        'level': level,
        'object_name': object_name,
        'period': period,
        'signal': {
            'status': status,
            'label': signal_payload.get('label'),
            'comment': signal_payload.get('comment'),
            'reason': signal_payload.get('reason'),
            'reason_value': signal_payload.get('reason_value'),
            'rank': signal_payload.get('rank'),
            'quartiles': signal_payload.get('quartiles'),
            'margin_gap': margin_gap,
            'kpi_gap': object_metrics.get('kpi_gap'),
            'median_gap': median_gap,
            'kpi_zone': kpi_zone,
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
            'top_drain_is_negative_for_business': bool(top_drain_effect > 0),
        },
        'impact': {
            'gap_loss_money': gap_loss_money,
            'gap_percent': margin_gap,
            'total_loss': total_loss,
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
            'next_level': next_level,
        },
        'flags': flags,
        'ai_focus': ai_focus,
        'consistency': consistency,
        'top_drain_metric': top_drain_metric,
        'top_drain_effect': top_drain_effect,
        'top_drain_is_negative_for_business': bool(top_drain_effect > 0),
    }


def _handle_empty_filter(meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'error': 'no data after filtering',
        'reason': meta.get('empty_reason'),
        'trace': meta.get('trace'),
    }


def get_business_comparison(period: str) -> Dict[str, Any]:
    rows = _safe_get_rows()
    business_rows, meta = _run_filter(rows, period=period)
    if not business_rows:
        return _handle_empty_filter(meta)

    business_metrics = aggregate_metrics(business_rows)
    payload = build_comparison_payload(
        level='business',
        object_name='business',
        object_metrics=business_metrics,
        business_metrics=business_metrics,
        period=period,
        object_rows=business_rows,
    )
    payload['filter'] = {}
    return payload


def _single_object_comparison(
    level: str,
    object_name: str,
    period: str,
    filter_payload: Optional[Dict[str, Any]] = None,
    **filters: Any,
) -> Dict[str, Any]:
    rows = _safe_get_rows()
    merged_filters = _merge_filters(filter_payload, **filters)

    parent_error = _validate_required_parent(level, merged_filters)
    if parent_error:
        return {'error': parent_error, 'reason': 'sku requires parent filter'}

    object_rows, object_meta = _run_filter(rows, period=period, **merged_filters)
    business_rows, business_meta = _run_filter(rows, period=period)

    if not object_rows:
        return _handle_empty_filter(object_meta)
    if not business_rows:
        return _handle_empty_filter(business_meta)

    object_metrics = aggregate_metrics(object_rows)
    business_metrics = aggregate_metrics(business_rows)

    payload = build_comparison_payload(
        level=level,
        object_name=object_name,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
        period=period,
        object_rows=object_rows,
    )
    payload['filter'] = merged_filters
    payload['debug'] = {
        'level': level,
        'object_name': object_name,
        'filters': dict(merged_filters, period=period),
        'rows_count_before': len(rows),
        'rows_count_after': len(object_rows),
    }
    return payload


def get_manager_top_comparison(manager_top: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _single_object_comparison('manager_top', manager_top, period, filter_payload=filter_payload, manager_top=manager_top)


def get_manager_comparison(manager: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _single_object_comparison('manager', manager, period, filter_payload=filter_payload, manager=manager)


def get_network_comparison(network: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _single_object_comparison('network', network, period, filter_payload=filter_payload, network=network)


def get_category_comparison(category: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _single_object_comparison('category', category, period, filter_payload=filter_payload, category=category)


def get_tmc_group_comparison(tmc_group: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _single_object_comparison('tmc_group', tmc_group, period, filter_payload=filter_payload, tmc_group=tmc_group)


def get_sku_comparison(sku: str, period: str, filter_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return _single_object_comparison('sku', sku, period, filter_payload=filter_payload, sku=sku)
