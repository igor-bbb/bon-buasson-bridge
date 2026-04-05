from typing import Any, Dict, List


LEVEL_LABELS_RU = {
    'business': 'Бизнес',
    'manager_top': 'Топ-менеджер',
    'manager': 'Менеджер',
    'network': 'Сеть',
    'category': 'Категория',
    'tmc_group': 'Группа ТМЦ',
    'sku': 'Товар',
}

STATUS_RANK = {'ok': 0, 'risk': 1, 'critical': 2}
PRIORITY_RANK = {'low': 0, 'medium': 1, 'high': 2}

STATUS_LABELS_RU = {
    'ok': 'норма',
    'risk': 'риск',
    'critical': 'критично',
}

METRIC_LABELS = {
    'retro_bonus': 'ретро',
    'logistics_cost': 'логистика',
    'personnel_cost': 'персонал',
    'other_costs': 'прочие затраты',
    'finrez_pre': 'финрез до распределения',
    'markup': 'наценка',
    'margin_pre': 'маржа до распределения',
    'margin_gap': 'отклонение маржи к бизнесу',
    'kpi_gap': 'разрыв KPI',
}

MAX_DRILLDOWN_ITEMS = 5
MAX_LOSSES_ITEMS = 5
MAX_REASON_ITEMS = 10
MAX_REASON_ITEMS_DRILLDOWN = 1


def _level_label(level: str) -> str:
    return LEVEL_LABELS_RU.get(level, level)


def _status_label(status: str) -> str:
    return STATUS_LABELS_RU.get(status, status)


def _round(value: float) -> float:
    return round(float(value or 0.0), 2)


def _format_signal_message(payload: Dict[str, Any]) -> str:
    margin_gap = _round(payload['context']['margin_gap'])
    status = _status_label(payload['signal']['status'])

    if margin_gap < 0:
        return f'маржа ниже бизнеса на {abs(margin_gap)} п.п. — {status}'
    if margin_gap > 0:
        return f'маржа выше бизнеса на +{margin_gap} п.п. — {status}'
    return f'маржа на уровне бизнеса — {status}'


def _format_problem_message(payload: Dict[str, Any]) -> str:
    margin_gap = _round(payload['context']['margin_gap'])
    status = payload['signal']['status']

    if margin_gap < 0:
        return f'теряет относительно бизнеса — {_status_label(status)}'
    if margin_gap > 0:
        return f'работает выше бизнеса — {_status_label(status)}'
    return f'работает на уровне бизнеса — {_status_label(status)}'


def sort_effect_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    def sort_key(item: Dict[str, Any]):
        is_negative = item['is_negative_for_business']
        effect_value = item['effect_value']

        if is_negative:
            return (0, -abs(effect_value))
        return (1, -abs(effect_value))

    return sorted(entries, key=sort_key)


def _extract_reasons_from_summary(comparison_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    effects_by_metric = comparison_payload['diagnosis']['effects_by_metric']

    reasons = []
    for metric, payload in effects_by_metric.items():
        reasons.append({
            'metric': metric,
            'metric_label': METRIC_LABELS.get(metric, metric),
            'effect_value': _round(payload['effect_value']),
            'effect_direction': payload['effect_direction'],
            'type': payload['type'],
            'is_negative_for_business': payload['is_negative_for_business'],
        })

    return sort_effect_entries(reasons)


def _extract_negative_reasons(reasons: List[Dict[str, Any]], limit: int = 2) -> List[Dict[str, Any]]:
    negative = [item for item in reasons if item['is_negative_for_business']]
    if negative:
        return negative[:limit]
    return reasons[:limit]


def _extract_data_flags(flags: Dict[str, Any]) -> List[str]:
    result = []

    if flags.get('low_volume'):
        result.append('низкий объём')
    if flags.get('invalid_benchmark'):
        result.append('нет корректной базы сравнения')
    if flags.get('negative_benchmark'):
        result.append('в базе сравнения есть отрицательные статьи')

    return result


def _build_reason_summary(negative_reasons: List[Dict[str, Any]]) -> List[str]:
    lines: List[str] = []
    for item in negative_reasons:
        sign = '-' if item['is_negative_for_business'] else '+'
        lines.append(f"{item['metric_label']}: {sign}{abs(_round(item['effect_value']))} грн")
    return lines


def _build_action_summary(comparison_payload: Dict[str, Any]) -> str:
    metric = comparison_payload['top_drain_metric']

    if metric == 'retro_bonus':
        return 'пересмотреть условия ретро'
    if metric == 'logistics_cost':
        return 'оптимизировать логистику'
    if metric == 'personnel_cost':
        return 'проверить загрузку команды'
    if metric == 'other_costs':
        return 'проверить прочие расходы'

    return comparison_payload['action']['suggested_action']


def _build_effect_summary(comparison_payload: Dict[str, Any]) -> str:
    total_loss = _round(comparison_payload['impact']['total_loss'])
    if total_loss > 0:
        return f'потенциал: +{total_loss} грн'
    return 'потерь не выявлено'


def build_management_view(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    object_metrics = comparison_payload['metrics']['object_metrics']
    reasons = _extract_reasons_from_summary(comparison_payload)
    negative_reasons = _extract_negative_reasons(reasons, limit=2)
    flags = _extract_data_flags(comparison_payload.get('flags', {}))

    return {
        'mode': 'management',
        'level': comparison_payload['level'],
        'level_label': _level_label(comparison_payload['level']),
        'object_name': comparison_payload['object_name'],
        'period': comparison_payload['period'],

        'signal': {
            'status': comparison_payload['signal']['status'],
            'label': _status_label(comparison_payload['signal']['status']),
            'message': _format_signal_message(comparison_payload),
            'margin_gap': _round(comparison_payload['context']['margin_gap']),
            'kpi_gap': _round(comparison_payload['navigation']['kpi_gap']),
            'median_gap': comparison_payload['navigation']['median_gap'],
            'kpi_zone': comparison_payload['navigation']['kpi_zone'],
        },

        'basis': {
            'revenue': _round(object_metrics['revenue']),
            'finrez_pre': _round(object_metrics['finrez_pre']),
            'margin_pre_object': _round(comparison_payload['context']['margin_pre_object']),
            'margin_pre_business': _round(comparison_payload['context']['margin_pre_business']),
            'margin_gap': _round(comparison_payload['context']['margin_gap']),
            'markup': _round(object_metrics['markup']),
        },

        'cause': {
            'top_drain_metric': comparison_payload['top_drain_metric'],
            'top_drain_metric_label': METRIC_LABELS.get(comparison_payload['top_drain_metric'], comparison_payload['top_drain_metric']),
            'top_drain_effect': _round(comparison_payload['top_drain_effect']),
            'items': negative_reasons,
        },

        'money': {
            'gap_loss_money': _round(comparison_payload['impact']['gap_loss_money']),
            'article_loss_money': _round(comparison_payload['impact']['total_loss']),
            'revenue_base': _round(object_metrics['revenue']),
        },

        'action': {
            'suggested_action': _build_action_summary(comparison_payload),
            'next_step': comparison_payload['action']['next_step'],
        },

        'management': {
            'problem': _format_problem_message(comparison_payload),
            'reason': _build_reason_summary(negative_reasons),
            'action': _build_action_summary(comparison_payload),
            'effect': _build_effect_summary(comparison_payload),
        },

        'flags': {
            'business_flags': comparison_payload.get('flags', {}),
            'data_flags': flags,
        },
    }


def _build_compact_drill_item(item: Dict[str, Any]) -> Dict[str, Any]:
    management_item = build_management_view(item)

    compact_cause_items = management_item['cause'].get('items', [])[:MAX_REASON_ITEMS_DRILLDOWN]

    return {
        'level': management_item['level'],
        'level_label': management_item['level_label'],
        'object_name': management_item['object_name'],
        'signal': management_item['signal'],
        'basis': management_item['basis'],
        'cause': {
            'top_drain_metric': management_item['cause']['top_drain_metric'],
            'top_drain_metric_label': management_item['cause']['top_drain_metric_label'],
            'top_drain_effect': management_item['cause']['top_drain_effect'],
            'items': compact_cause_items,
        },
        'money': management_item['money'],
        'action': management_item['action'],
        'management': management_item['management'],
    }


def build_drilldown_management_view(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = drilldown_payload['items']

    prepared_items = []
    for item in items[:MAX_DRILLDOWN_ITEMS]:
        prepared_items.append(_build_compact_drill_item(item))

    return {
        'mode': 'drill_down',
        'level': drilldown_payload['level'],
        'level_label': _level_label(drilldown_payload['level']),
        'object_name': drilldown_payload['object_name'],
        'period': drilldown_payload['period'],
        'children_level': drilldown_payload['children_level'],
        'children_level_label': _level_label(drilldown_payload['children_level']),
        'items_count': len(prepared_items),
        'items': prepared_items,
        'action': {
            'suggested_action': 'провалиться в следующий уровень и найти главный источник потери',
            'next_step': drilldown_payload['children_level'],
        },
    }


def build_reasons_view(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    reasons = _extract_reasons_from_summary(comparison_payload)

    return {
        'level': comparison_payload['level'],
        'level_label': _level_label(comparison_payload['level']),
        'object_name': comparison_payload['object_name'],
        'period': comparison_payload['period'],
        'top_drain_metric': comparison_payload['top_drain_metric'],
        'top_drain_metric_label': METRIC_LABELS.get(comparison_payload['top_drain_metric'], comparison_payload['top_drain_metric']),
        'top_drain_effect': _round(comparison_payload['top_drain_effect']),
        'reasons': reasons[:MAX_REASON_ITEMS],
        'management': {
            'problem': _format_problem_message(comparison_payload),
            'reason': _build_reason_summary(_extract_negative_reasons(reasons, limit=2)),
            'action': _build_action_summary(comparison_payload),
            'effect': _build_effect_summary(comparison_payload),
        },
        'flags': {
            'business_flags': comparison_payload.get('flags', {}),
            'data_flags': _extract_data_flags(comparison_payload.get('flags', {})),
        },
    }


def build_losses_view_from_children(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = drilldown_payload['items']

    losses = []
    for item in items:
        management = build_management_view(item)
        losses.append({
            'object_name': item['object_name'],
            'level': item['level'],
            'level_label': _level_label(item['level']),
            'signal_message': _format_signal_message(item),
            'margin_gap': _round(item['context']['margin_gap']),
            'revenue': _round(item['metrics']['object_metrics']['revenue']),
            'gap_loss_money': _round(item['impact']['gap_loss_money']),
            'article_loss_money': _round(item['impact']['total_loss']),
            'top_drain_metric': item['top_drain_metric'],
            'top_drain_metric_label': METRIC_LABELS.get(item['top_drain_metric'], item['top_drain_metric']),
            'top_drain_effect': _round(item['top_drain_effect']),
            'is_negative_for_business': item['top_drain_is_negative_for_business'],
            'management': management['management'],
            'flags': {
                'business_flags': item.get('flags', {}),
                'data_flags': _extract_data_flags(item.get('flags', {})),
            },
        })

    def sort_key(item: Dict[str, Any]):
        low_volume = item['flags']['business_flags'].get('low_volume', False)
        gap_loss = item['gap_loss_money']
        article_loss = item['article_loss_money']

        if low_volume:
            return (1, 0.0, 0.0)

        return (0, -abs(gap_loss), -abs(article_loss))

    losses.sort(key=sort_key)

    return {
        'level': drilldown_payload['level'],
        'level_label': _level_label(drilldown_payload['level']),
        'object_name': drilldown_payload['object_name'],
        'period': drilldown_payload['period'],
        'children_level': drilldown_payload['children_level'],
        'children_level_label': _level_label(drilldown_payload['children_level']),
        'losses': losses[:MAX_LOSSES_ITEMS],
        'action': {
            'suggested_action': 'выбрать объект с максимальными потерями и провалиться глубже',
            'next_step': drilldown_payload['children_level'],
        },
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


def _build_comparison_action(main_change: Dict[str, Any], level: str, deterioration: bool, fallback_next_step: str) -> Dict[str, str]:
    metric = main_change.get('metric')
    next_step = fallback_next_step

    if metric == 'retro_bonus':
        suggestion = 'пересмотреть условия ретро'
    elif metric == 'logistics_cost':
        suggestion = 'оптимизировать логистику'
    elif metric == 'personnel_cost':
        suggestion = 'проверить загрузку команды'
    elif metric == 'other_costs':
        suggestion = 'проверить прочие расходы'
    elif metric == 'finrez_pre' and deterioration:
        suggestion = 'проверить, какая статья дренирует деньги сильнее всего'
    elif deterioration:
        suggestion = 'подтвердить источник отклонения на следующем уровне'
    else:
        suggestion = 'контролировать динамику и подтвердить, что улучшение устойчиво'

    if level == 'sku':
        next_step = 'проверить цену, контракт и промо по товару'

    return {
        'suggested_action': suggestion,
        'next_step': next_step,
    }


def build_comparison_management_view(query: Dict[str, Any], current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    current_obj = current['metrics']['object_metrics']
    previous_obj = previous['metrics']['object_metrics']

    delta_finrez = _round(current['signal']['finrez_pre'] - previous['signal']['finrez_pre'])
    delta_kpi_gap = _round(current['navigation']['kpi_gap'] - previous['navigation']['kpi_gap'])
    delta_margin_pre = _round(current['context']['margin_pre_object'] - previous['context']['margin_pre_object'])
    delta_margin_gap = _round(current['context']['margin_gap'] - previous['context']['margin_gap'])
    delta_gap_loss_money = _round(current['impact']['gap_loss_money'] - previous['impact']['gap_loss_money'])

    diagnosis_changes = []
    for metric in ['retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs']:
        delta_value = _round(current_obj.get(metric, 0.0) - previous_obj.get(metric, 0.0))
        is_negative = delta_value > 0
        diagnosis_changes.append({
            'metric': metric,
            'label': METRIC_LABELS.get(metric, metric),
            'delta_value': delta_value,
            'direction': 'ухудшение' if delta_value > 0 else ('улучшение' if delta_value < 0 else 'без изменений'),
            'is_negative_for_business': is_negative,
        })

    negative = [c for c in diagnosis_changes if c['is_negative_for_business']]
    source = negative if negative else diagnosis_changes
    main_change = max(source, key=lambda x: abs(x['delta_value'])) if source else {
        'metric': None,
        'label': None,
        'delta_value': 0.0,
        'direction': 'без изменений',
    }

    deterioration = delta_finrez < 0 or delta_kpi_gap > 0 or delta_margin_gap < 0
    status_change = _status_change_label(current['signal']['status'], previous['signal']['status'])
    priority_change = _priority_change_label(current['priority']['priority'], previous['priority']['priority'])
    action = _build_comparison_action(main_change, query['level'], deterioration, current['action']['next_step'])

    return {
        'mode': 'comparison',
        'level': query['level'],
        'level_label': _level_label(query['level']),
        'object_name': query['object_name'],
        'period_current': query['period_current'],
        'period_previous': query['period_previous'],

        'signal': {
            'delta_finrez_pre': delta_finrez,
            'delta_status': _finrez_delta_status(delta_finrez),
            'delta_kpi_gap': delta_kpi_gap,
            'delta_margin_gap': delta_margin_gap,
        },

        'basis': {
            'current_margin_pre': _round(current['context']['margin_pre_object']),
            'previous_margin_pre': _round(previous['context']['margin_pre_object']),
            'current_margin_gap': _round(current['context']['margin_gap']),
            'previous_margin_gap': _round(previous['context']['margin_gap']),
        },

        'cause': {
            'items': diagnosis_changes[:MAX_REASON_ITEMS],
            'main_driver_metric': main_change.get('metric'),
            'main_driver_label': main_change.get('label'),
            'main_driver_delta': main_change.get('delta_value'),
            'main_driver_direction': main_change.get('direction'),
        },

        'money': {
            'delta_finrez_pre': delta_finrez,
            'delta_gap_loss_money': delta_gap_loss_money,
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

        'current': build_management_view(current),
        'previous': build_management_view(previous),

        'delta': {
            'finrez_pre': delta_finrez,
            'kpi_gap': delta_kpi_gap,
            'margin_pre': delta_margin_pre,
            'margin_gap': delta_margin_gap,
            'gap_loss_money': delta_gap_loss_money,
        },
    }
