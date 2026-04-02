from typing import Any, Dict, List


def sort_effect_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(item: Dict[str, Any]):
        is_negative = item["is_negative_for_business"]
        effect_value = item["effect_value"]

        if is_negative:
            return (0, -abs(effect_value))
        return (1, -abs(effect_value))

    return sorted(entries, key=sort_key)


def build_reasons_view(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    effects_by_metric = comparison_payload["effects_by_metric"]

    reasons = []
    for metric, payload in effects_by_metric.items():
        reasons.append({
            "metric": metric,
            "effect_value": payload["effect_value"],
            "effect_direction": payload["effect_direction"],
            "type": payload["type"],
            "is_negative_for_business": payload["is_negative_for_business"],
        })

    reasons = sort_effect_entries(reasons)

    return {
        "level": comparison_payload["level"],
        "object_name": comparison_payload["object_name"],
        "period": comparison_payload["period"],
        "top_drain_metric": comparison_payload["top_drain_metric"],
        "top_drain_effect": comparison_payload["top_drain_effect"],
        "reasons": reasons,
    }


def build_losses_view_from_summary(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    effects_by_metric = comparison_payload["effects_by_metric"]

    losses = []
    for metric, payload in effects_by_metric.items():
        if payload["is_negative_for_business"]:
            losses.append({
                "metric": metric,
                "effect_value": payload["effect_value"],
                "type": payload["type"],
                "is_negative_for_business": payload["is_negative_for_business"],
            })

    losses.sort(key=lambda x: -abs(x["effect_value"]))

    return {
        "level": comparison_payload["level"],
        "object_name": comparison_payload["object_name"],
        "period": comparison_payload["period"],
        "top_drain_metric": comparison_payload["top_drain_metric"],
        "top_drain_effect": comparison_payload["top_drain_effect"],
        "losses": losses,
    }


def build_losses_view_from_children(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = drilldown_payload["items"]

    losses = []
    for item in items:
        losses.append({
            "object_name": item["object_name"],
            "top_drain_metric": item["top_drain_metric"],
            "top_drain_effect": item["top_drain_effect"],
            "is_negative_for_business": item["top_drain_is_negative_for_business"],
            "flags": item["flags"],
        })

    def sort_key(item: Dict[str, Any]):
        low_volume = item["flags"]["low_volume"]
        is_negative = item["is_negative_for_business"]
        top_effect = item["top_drain_effect"]

        if low_volume:
            return (1, 0.0)

        if is_negative:
            return (0, -abs(top_effect))

        return (0, float("inf"))

    losses.sort(key=sort_key)

    return {
        "level": drilldown_payload["level"],
        "object_name": drilldown_payload["object_name"],
        "period": drilldown_payload["period"],
        "children_level": drilldown_payload["children_level"],
        "losses": losses,
    }


STATUS_RANK = {'ok': 0, 'risk': 1, 'critical': 2}
PRIORITY_RANK = {'low': 0, 'medium': 1, 'high': 2}
METRIC_LABELS = {
    'retro_bonus': 'ретро',
    'logistics_cost': 'логистика',
    'personnel_cost': 'персонал',
    'other_costs': 'прочие затраты',
    'finrez_pre': 'финрез до распределения',
    'markup': 'наценка',
    'margin_pre': 'маржа до распределения',
    'margin_gap': 'отклонение маржи к бизнесу',
    'kpi_gap': 'KPI_GAP',
}



def _status_change_label(current_status: str, previous_status: str) -> str:
    current_rank = STATUS_RANK.get(current_status, 0)
    previous_rank = STATUS_RANK.get(previous_status, 0)
    if current_rank > previous_rank:
        return 'риск усилился'
    if current_rank < previous_rank:
        return 'риск снизился'
    return 'риск без изменений'



def _priority_change_label(current_priority: str, previous_priority: str) -> str:
    current_rank = PRIORITY_RANK.get(current_priority, 0)
    previous_rank = PRIORITY_RANK.get(previous_priority, 0)
    if current_rank > previous_rank:
        return 'приоритет вырос'
    if current_rank < previous_rank:
        return 'приоритет снизился'
    return 'приоритет без изменений'



def _finrez_delta_status(delta_finrez: float) -> str:
    if delta_finrez > 0:
        return 'рост прибыли'
    if delta_finrez < 0:
        return 'падение прибыли'
    return 'без изменений'



def _kpi_direction(delta_kpi_gap: float) -> str:
    if delta_kpi_gap > 0:
        return 'ухудшение'
    if delta_kpi_gap < 0:
        return 'улучшение'
    return 'без изменений'



def _context_direction(delta_value: float) -> str:
    if delta_value > 0:
        return 'улучшение'
    if delta_value < 0:
        return 'ухудшение'
    return 'без изменений'



def _cost_direction(delta_value: float) -> str:
    if delta_value > 0:
        return 'ухудшение'
    if delta_value < 0:
        return 'улучшение'
    return 'без изменений'



def _find_main_change(changes: List[Dict[str, Any]]) -> Dict[str, Any]:
    negative = [c for c in changes if c['is_negative_for_business']]
    source = negative if negative else changes
    if not source:
        return {
            'metric': None,
            'label': None,
            'delta_value': 0.0,
            'direction': 'без изменений',
            'is_negative_for_business': False,
        }
    return max(source, key=lambda x: abs(x['delta_value']))



def _build_comparison_action(main_change: Dict[str, Any], level: str, deterioration: bool, fallback_next_step: str) -> Dict[str, str]:
    metric = main_change.get('metric')
    next_step = fallback_next_step

    if metric == 'retro_bonus':
        suggestion = 'рост потерь связан с увеличением ретро — проверить условия контракта'
    elif metric == 'logistics_cost':
        suggestion = 'рост потерь связан с логистикой — проверить схему поставки и стоимость обслуживания'
    elif metric == 'personnel_cost':
        suggestion = 'рост потерь связан с персоналом — проверить нагрузку и модель покрытия'
    elif metric == 'other_costs':
        suggestion = 'рост потерь связан с прочими затратами — проверить состав статьи и источник отклонения'
    elif metric == 'finrez_pre' and deterioration:
        suggestion = 'просел финрез — проверить, какая статья дренирует деньги сильнее всего'
    elif deterioration:
        suggestion = 'ситуация ухудшилась — подтвердить источник отклонения на следующем уровне'
    else:
        suggestion = 'контролировать динамику и подтвердить, что улучшение устойчиво'

    if level == 'sku':
        next_step = 'проверить цену, контракт и промо по SKU'

    return {
        'suggested_action': suggestion,
        'next_step': next_step,
    }



def build_comparison_management_view(query: Dict[str, Any], current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    current_obj = current['metrics']['object_metrics']
    previous_obj = previous['metrics']['object_metrics']

    delta_finrez = round(current['signal']['finrez_pre'] - previous['signal']['finrez_pre'], 2)
    delta_kpi_gap = round(current['navigation']['kpi_gap'] - previous['navigation']['kpi_gap'], 2)
    delta_margin_pre = round(current['context']['margin_pre_object'] - previous['context']['margin_pre_object'], 2)
    delta_margin_gap = round(current['context']['margin_gap'] - previous['context']['margin_gap'], 2)

    diagnosis_changes = []
    for metric in ['retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs']:
        delta_value = round(current_obj.get(metric, 0.0) - previous_obj.get(metric, 0.0), 2)
        is_negative = delta_value > 0
        diagnosis_changes.append({
            'metric': metric,
            'label': METRIC_LABELS.get(metric, metric),
            'delta_value': delta_value,
            'direction': _cost_direction(delta_value),
            'is_negative_for_business': is_negative,
        })

    main_change = _find_main_change(diagnosis_changes)
    deterioration = delta_finrez < 0 or delta_kpi_gap > 0 or delta_margin_gap < 0
    status_change = _status_change_label(current['signal']['status'], previous['signal']['status'])
    priority_change = _priority_change_label(current['priority']['priority'], previous['priority']['priority'])
    action = _build_comparison_action(main_change, query['level'], deterioration, current['action']['next_step'])

    return {
        'mode': 'comparison',
        'level': query['level'],
        'object_name': query['object_name'],
        'period_current': query['period_current'],
        'period_previous': query['period_previous'],
        'signal': {
            'delta_finrez_pre': delta_finrez,
            'delta_status': _finrez_delta_status(delta_finrez),
        },
        'navigation': {
            'delta_kpi_gap': delta_kpi_gap,
            'direction': _kpi_direction(delta_kpi_gap),
        },
        'context': {
            'delta_margin_pre': delta_margin_pre,
            'delta_margin_gap': delta_margin_gap,
            'margin_pre_direction': _context_direction(delta_margin_pre),
            'margin_gap_direction': _context_direction(delta_margin_gap),
        },
        'diagnosis_change': {
            'delta_retro_bonus': round(current_obj.get('retro_bonus', 0.0) - previous_obj.get('retro_bonus', 0.0), 2),
            'delta_logistics_cost': round(current_obj.get('logistics_cost', 0.0) - previous_obj.get('logistics_cost', 0.0), 2),
            'delta_personnel_cost': round(current_obj.get('personnel_cost', 0.0) - previous_obj.get('personnel_cost', 0.0), 2),
            'delta_other_costs': round(current_obj.get('other_costs', 0.0) - previous_obj.get('other_costs', 0.0), 2),
            'items': diagnosis_changes,
        },
        'impact': {
            'money_effect': delta_finrez,
            'main_driver_metric': main_change.get('metric'),
            'main_driver_label': main_change.get('label'),
            'main_driver_delta': main_change.get('delta_value'),
            'main_driver_direction': main_change.get('direction'),
        },
        'priority_change': {
            'status_current': current['signal']['status'],
            'status_previous': previous['signal']['status'],
            'priority_current': current['priority']['priority'],
            'priority_previous': previous['priority']['priority'],
            'status_change': status_change,
            'priority_change': priority_change,
        },
        'action': action,
        'current': current,
        'previous': previous,
        'delta': {
            'finrez_pre': delta_finrez,
            'kpi_gap': delta_kpi_gap,
            'margin_pre': delta_margin_pre,
            'margin_gap': delta_margin_gap,
        },
    }
