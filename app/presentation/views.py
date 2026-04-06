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
    'attention': 'внимание',
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
    signal = payload.get('signal', {})
    if signal.get('comment'):
        return str(signal.get('comment'))

    context = payload.get('context', {})
    margin_gap = _round(context.get('margin_gap', 0.0))
    status = _status_label(signal.get('status'))

    if margin_gap < 0:
        return f'маржа ниже бизнеса на {abs(margin_gap)} п.п. — {status}'
    if margin_gap > 0:
        return f'маржа выше бизнеса на +{margin_gap} п.п. — {status}'
    return f'маржа на уровне бизнеса — {status}'


def _format_problem_message(payload: Dict[str, Any]) -> str:
    signal = payload.get('signal', {})
    if signal.get('comment'):
        return f"{signal.get('label', _status_label(signal.get('status')))} — {signal.get('comment')}"

    context = payload.get('context', {})
    status = signal.get('status')
    margin_gap = _round(context.get('margin_gap', 0.0))

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
    diagnosis = comparison_payload.get('diagnosis') or {}
    effects_by_metric = diagnosis.get('effects_by_metric') or {}

    if not effects_by_metric:
        return []

    reasons = []
    for metric, payload in effects_by_metric.items():
        reasons.append({
            'metric': metric,
            'metric_label': METRIC_LABELS.get(metric, metric),
            'effect_value': _round(payload.get('effect_value', 0.0)),
            'effect_direction': payload.get('effect_direction'),
            'type': payload.get('type'),
            'is_negative_for_business': payload.get('is_negative_for_business', False),
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
    metric = comparison_payload.get('top_drain_metric')

    if metric == 'retro_bonus':
        return 'пересмотреть условия ретро'
    if metric == 'logistics_cost':
        return 'оптимизировать логистику'
    if metric == 'personnel_cost':
        return 'проверить загрузку команды'
    if metric == 'other_costs':
        return 'проверить прочие расходы'

    return comparison_payload.get('action', {}).get('suggested_action', 'уточнить действие')


def _build_effect_summary(comparison_payload: Dict[str, Any]) -> str:
    impact = comparison_payload.get('impact', {})
    total_loss = _round(impact.get('total_loss', 0.0))
    if total_loss > 0:
        return f'потенциал: +{total_loss} грн'
    return 'потерь не выявлено'


def build_management_view(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = comparison_payload.get('metrics', {})
    object_metrics = metrics.get('object_metrics', {})
    navigation = comparison_payload.get('navigation', {})
    context = comparison_payload.get('context', {})
    signal = comparison_payload.get('signal', {})
    impact = comparison_payload.get('impact', {})
    flags = comparison_payload.get('flags', {})

    reasons = _extract_reasons_from_summary(comparison_payload)
    negative_reasons = _extract_negative_reasons(reasons, limit=2)
    data_flags = _extract_data_flags(flags)

    return {
        'mode': 'management',
        'level': comparison_payload.get('level'),
        'level_label': _level_label(comparison_payload.get('level')),
        'object_name': comparison_payload.get('object_name'),
        'period': comparison_payload.get('period'),

        'signal': {
            'status': signal.get('status'),
            'label': signal.get('label', _status_label(signal.get('status'))),
            'comment': signal.get('comment'),
            'reason': signal.get('reason'),
            'reason_value': _round(signal.get('reason_value', 0.0)),
            'rank': signal.get('rank'),
            'priority': signal.get('priority'),
            'problem_money': _round(signal.get('problem_money', 0.0)),
            'quartiles': signal.get('quartiles'),
            'message': _format_signal_message(comparison_payload),
            'margin_gap': _round(context.get('margin_gap', 0.0)),
            'kpi_gap': _round(navigation.get('kpi_gap', 0.0)),
            'median_gap': navigation.get('median_gap'),
            'kpi_zone': navigation.get('kpi_zone'),
        },

        'basis': {
            'revenue': _round(object_metrics.get('revenue', 0.0)),
            'finrez_pre': _round(object_metrics.get('finrez_pre', 0.0)),
            'margin_pre_object': _round(context.get('margin_pre_object', 0.0)),
            'margin_pre_business': _round(context.get('margin_pre_business', 0.0)),
            'margin_gap': _round(context.get('margin_gap', 0.0)),
            'markup': _round(object_metrics.get('markup', 0.0)),
            'retro_bonus': _round(object_metrics.get('retro_bonus', 0.0)),
            'logistics_cost': _round(object_metrics.get('logistics_cost', 0.0)),
            'personnel_cost': _round(object_metrics.get('personnel_cost', 0.0)),
            'other_costs': _round(object_metrics.get('other_costs', 0.0)),
        },

        'cause': {
            'top_drain_metric': comparison_payload.get('top_drain_metric'),
            'top_drain_metric_label': METRIC_LABELS.get(
                comparison_payload.get('top_drain_metric'),
                comparison_payload.get('top_drain_metric')
            ),
            'top_drain_effect': _round(comparison_payload.get('top_drain_effect', 0.0)),
            'items': negative_reasons,
        },

        'money': {
            'gap_loss_money': _round(impact.get('gap_loss_money', 0.0)),
            'article_loss_money': _round(impact.get('total_loss', 0.0)),
            'revenue_base': _round(object_metrics.get('revenue', 0.0)),
        },

        'action': {
            'suggested_action': _build_action_summary(comparison_payload),
            'next_step': comparison_payload.get('action', {}).get('next_step'),
        },

        'management': {
            'problem': _format_problem_message(comparison_payload),
            'reason': _build_reason_summary(negative_reasons),
            'action': _build_action_summary(comparison_payload),
            'effect': _build_effect_summary(comparison_payload),
        },

        'flags': {
            'business_flags': flags,
            'data_flags': data_flags,
        },
    }


def _build_compact_drill_item(item: Dict[str, Any]) -> Dict[str, Any]:
    management_item = build_management_view(item)

    compact_cause_items = management_item.get('cause', {}).get('items', [])[:MAX_REASON_ITEMS_DRILLDOWN]

    return {
        'level': management_item.get('level'),
        'level_label': management_item.get('level_label'),
        'object_name': management_item.get('object_name'),
        'signal': management_item.get('signal'),
        'basis': management_item.get('basis'),
        'cause': {
            'top_drain_metric': management_item.get('cause', {}).get('top_drain_metric'),
            'top_drain_metric_label': management_item.get('cause', {}).get('top_drain_metric_label'),
            'top_drain_effect': management_item.get('cause', {}).get('top_drain_effect'),
            'items': compact_cause_items,
        },
        'money': management_item.get('money'),
        'action': management_item.get('action'),
        'management': management_item.get('management'),
    }


def build_drilldown_management_view(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = drilldown_payload.get('items', [])

    prepared_items = []
    for item in items[:MAX_DRILLDOWN_ITEMS]:
        prepared_items.append(_build_compact_drill_item(item))

    return {
        'mode': 'drill_down',
        'level': drilldown_payload.get('level'),
        'level_label': _level_label(drilldown_payload.get('level')),
        'object_name': drilldown_payload.get('object_name'),
        'period': drilldown_payload.get('period'),
        'children_level': drilldown_payload.get('children_level'),
        'children_level_label': _level_label(drilldown_payload.get('children_level')),
        'items_count': len(prepared_items),
        'items': prepared_items,
        'action': {
            'suggested_action': 'провалиться в следующий уровень и найти главный источник потери',
            'next_step': drilldown_payload.get('children_level'),
        },
    }


def build_reasons_view(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    reasons = _extract_reasons_from_summary(comparison_payload)

    return {
        'level': comparison_payload.get('level'),
        'level_label': _level_label(comparison_payload.get('level')),
        'object_name': comparison_payload.get('object_name'),
        'period': comparison_payload.get('period'),
        'top_drain_metric': comparison_payload.get('top_drain_metric'),
        'top_drain_metric_label': METRIC_LABELS.get(
            comparison_payload.get('top_drain_metric'),
            comparison_payload.get('top_drain_metric')
        ),
        'top_drain_effect': _round(comparison_payload.get('top_drain_effect', 0.0)),
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
    items = drilldown_payload.get('items', [])

    losses = []
    for item in items:
        management = build_management_view(item)
        losses.append({
            'object_name': item.get('object_name'),
            'level': item.get('level'),
            'level_label': _level_label(item.get('level')),
            'signal_message': _format_signal_message(item),
            'margin_gap': _round(item.get('context', {}).get('margin_gap', 0.0)),
            'revenue': _round(item.get('metrics', {}).get('object_metrics', {}).get('revenue', 0.0)),
            'gap_loss_money': _round(item.get('impact', {}).get('gap_loss_money', 0.0)),
            'article_loss_money': _round(item.get('impact', {}).get('total_loss', 0.0)),
            'top_drain_metric': item.get('top_drain_metric'),
            'top_drain_metric_label': METRIC_LABELS.get(item.get('top_drain_metric'), item.get('top_drain_metric')),
            'top_drain_effect': _round(item.get('top_drain_effect', 0.0)),
            'is_negative_for_business': item.get('top_drain_is_negative_for_business', False),
            'management': management.get('management'),
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
        'level': drilldown_payload.get('level'),
        'level_label': _level_label(drilldown_payload.get('level')),
        'object_name': drilldown_payload.get('object_name'),
        'period': drilldown_payload.get('period'),
        'children_level': drilldown_payload.get('children_level'),
        'children_level_label': _level_label(drilldown_payload.get('children_level')),
        'losses': losses[:MAX_LOSSES_ITEMS],
        'action': {
            'suggested_action': 'выбрать объект с максимальными потерями и провалиться глубже',
            'next_step': drilldown_payload.get('children_level'),
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
    current_obj = current.get('metrics', {}).get('object_metrics', {})
    previous_obj = previous.get('metrics', {}).get('object_metrics', {})

    delta_finrez = _round(current.get('signal', {}).get('finrez_pre', 0.0) - previous.get('signal', {}).get('finrez_pre', 0.0))
    delta_kpi_gap = _round(current.get('navigation', {}).get('kpi_gap', 0.0) - previous.get('navigation', {}).get('kpi_gap', 0.0))
    delta_margin_pre = _round(current.get('context', {}).get('margin_pre_object', 0.0) - previous.get('context', {}).get('margin_pre_object', 0.0))
    delta_margin_gap = _round(current.get('context', {}).get('margin_gap', 0.0) - previous.get('context', {}).get('margin_gap', 0.0))
    delta_gap_loss_money = _round(current.get('impact', {}).get('gap_loss_money', 0.0) - previous.get('impact', {}).get('gap_loss_money', 0.0))

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
    status_change = _status_change_label(current.get('signal', {}).get('status'), previous.get('signal', {}).get('status'))
    priority_change = _priority_change_label(
        current.get('priority', {}).get('priority'),
        previous.get('priority', {}).get('priority'),
    )
    action = _build_comparison_action(main_change, query.get('level'), deterioration, current.get('action', {}).get('next_step'))

    return {
        'mode': 'comparison',
        'level': query.get('level'),
        'level_label': _level_label(query.get('level')),
        'object_name': query.get('object_name'),
        'period_current': query.get('period_current'),
        'period_previous': query.get('period_previous'),

        'signal': {
            'status': 'ok' if delta_finrez >= 0 else 'risk',
            'label': 'OK' if delta_finrez >= 0 else 'RISK',
            'comment': 'результат улучшился период к периоду' if delta_finrez > 0 else ('результат ухудшился период к периоду' if delta_finrez < 0 else 'результат без изменений период к периоду'),
            'reason': main_change.get('metric'),
            'reason_value': _round(main_change.get('delta_value', 0.0)),
            'delta_finrez_pre': delta_finrez,
            'delta_status': _finrez_delta_status(delta_finrez),
            'delta_kpi_gap': delta_kpi_gap,
            'delta_margin_gap': delta_margin_gap,
        },

        'basis': {
            'current_margin_pre': _round(current.get('context', {}).get('margin_pre_object', 0.0)),
            'previous_margin_pre': _round(previous.get('context', {}).get('margin_pre_object', 0.0)),
            'current_margin_gap': _round(current.get('context', {}).get('margin_gap', 0.0)),
            'previous_margin_gap': _round(previous.get('context', {}).get('margin_gap', 0.0)),
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
            'status_current': current.get('signal', {}).get('status'),
            'status_previous': previous.get('signal', {}).get('status'),
            'priority_current': current.get('priority', {}).get('priority'),
            'priority_previous': previous.get('priority', {}).get('priority'),
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
