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
STATUS_LABELS_RU = {'ok': 'норма', 'attention': 'внимание', 'risk': 'риск', 'critical': 'критично'}
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
MAX_REASON_ITEMS = 10
MAX_REASON_ITEMS_DRILLDOWN = 1
MAX_LOSSES_ITEMS = 5


def _finrez_pre(payload: Dict[str, Any]) -> float:
    metrics = payload.get('metrics', {}) or {}
    object_metrics = metrics.get('object_metrics', {}) or {}
    if 'basis' in payload and isinstance(payload.get('basis'), dict):
        return _round(payload.get('basis', {}).get('finrez_pre', 0.0))
    return _round(object_metrics.get('finrez_pre', 0.0))


def _build_top_summary(items: List[Dict[str, Any]], limit: int = 5) -> Dict[str, Any]:
    ranked = []
    for item in items:
        finrez_pre = _finrez_pre(item)
        ranked.append({
            'object_name': item.get('object_name'),
            'level': item.get('level'),
            'finrez_pre': finrez_pre,
            'signal': (item.get('signal') or {}).get('status'),
        })

    ranked.sort(key=lambda x: (x.get('finrez_pre', 0.0), x.get('object_name') or ''))
    negative = [item for item in ranked if item.get('finrez_pre', 0.0) < 0]
    source = negative if negative else ranked
    top_items = source[:limit]

    total_negative_loss = sum(abs(item.get('finrez_pre', 0.0)) for item in negative)
    top_negative_loss = sum(abs(item.get('finrez_pre', 0.0)) for item in top_items if item.get('finrez_pre', 0.0) < 0)
    concentration_share = round(top_negative_loss / total_negative_loss, 4) if total_negative_loss > 0 else 0.0

    return {
        'top_items': top_items,
        'focus_object': top_items[0].get('object_name') if top_items else None,
        'focus_finrez_pre': top_items[0].get('finrez_pre') if top_items else 0.0,
        'negative_items_count': len(negative),
        'total_negative_loss': _round(total_negative_loss),
        'concentration_share': concentration_share,
    }


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
    return f"сигнал периода: {_status_label(signal.get('status'))}"


def _format_problem_message(payload: Dict[str, Any]) -> str:
    signal = payload.get('signal', {})
    if signal.get('comment'):
        return f"{signal.get('label', _status_label(signal.get('status')))} — {signal.get('comment')}"
    return f"статус объекта — {_status_label(signal.get('status'))}"


def sort_effect_entries(entries: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(entries, key=lambda item: (0 if item['is_negative_for_business'] else 1, -abs(item['effect_value'])))


def _extract_reasons_from_summary(comparison_payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    diagnosis = comparison_payload.get('diagnosis') or {}
    effects_by_metric = diagnosis.get('effects_by_metric') or {}
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
    return negative[:limit] if negative else reasons[:limit]


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
    return [f"{item['metric_label']}: {'-' if item['is_negative_for_business'] else '+'}{abs(_round(item['effect_value']))} грн" for item in negative_reasons]


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


def _build_consistency_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    consistency = payload.get('consistency') or {}
    return {
        'status': consistency.get('status', 'not_available'),
        'gap': _round(consistency.get('gap', 0.0)) if consistency.get('gap') is not None else None,
        'gap_percent': _round(consistency.get('gap_percent', 0.0)) if consistency.get('gap_percent') is not None else None,
        'children_sum': _round(consistency.get('children_sum', 0.0)) if consistency.get('children_sum') is not None else None,
        'child_level': consistency.get('child_level'),
        'children_count': consistency.get('children_count'),
    }


def _build_items_meta_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    items_meta = payload.get('items_meta') or {}
    return {
        'total_count': items_meta.get('total_count', 0),
        'returned_count': items_meta.get('returned_count', 0),
        'hidden_count': items_meta.get('hidden_count', 0),
        'has_more': items_meta.get('has_more', False),
        'hint': f"ещё {items_meta.get('hidden_count', 0)} скрыто (введи 'покажи все')" if items_meta.get('has_more') else None,
    }


def _build_focus_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    signal = payload.get('signal', {})
    top_metric = payload.get('top_drain_metric')
    return {
        'problem': _format_problem_message(payload),
        'signal_label': signal.get('label', _status_label(signal.get('status'))),
        'priority': signal.get('priority'),
        'problem_money': _round(signal.get('problem_money', 0.0)),
        'focus_metric': METRIC_LABELS.get(top_metric, top_metric),
        'focus_action': _build_action_summary(payload),
    }


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

    return {
        'mode': 'management',
        'level': comparison_payload.get('level'),
        'level_label': _level_label(comparison_payload.get('level')),
        'object_name': comparison_payload.get('object_name'),
        'period': comparison_payload.get('period'),
        'focus': _build_focus_block(comparison_payload),
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
            'top_drain_metric_label': METRIC_LABELS.get(comparison_payload.get('top_drain_metric'), comparison_payload.get('top_drain_metric')),
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
        'flags': {'business_flags': flags, 'data_flags': _extract_data_flags(flags)},
        'consistency': _build_consistency_view(comparison_payload),
    }


def _build_compact_drill_item(item: Dict[str, Any]) -> Dict[str, Any]:
    management_item = build_management_view(item)
    compact_cause_items = management_item.get('cause', {}).get('items', [])[:1]
    finrez_pre = management_item.get('basis', {}).get('finrez_pre', 0.0)
    return {
        'level': management_item.get('level'),
        'level_label': management_item.get('level_label'),
        'object_name': management_item.get('object_name'),
        'finrez_pre': finrez_pre,
        'priority': management_item.get('signal', {}).get('priority'),
        'focus': management_item.get('focus'),
        'signal': management_item.get('signal'),
        'basis': management_item.get('basis'),
        'cause': {
            'top_drain_metric': management_item.get('cause', {}).get('top_drain_metric'),
            'top_drain_metric_label': management_item.get('cause', {}).get('top_drain_metric_label'),
            'top_drain_effect': management_item.get('cause', {}).get('top_drain_effect'),
            'items': compact_cause_items,
        },
        'money': management_item.get('money'),
        'consistency': management_item.get('consistency'),
        'action': management_item.get('action'),
        'management': management_item.get('management'),
    }


def build_drilldown_management_view(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = drilldown_payload.get('items', [])
    prepared_items = [_build_compact_drill_item(item) for item in items]
    top_summary = _build_top_summary(prepared_items)
    focus = prepared_items[0].get('focus') if prepared_items else None
    return {
        'mode': 'drill_down',
        'level': drilldown_payload.get('level'),
        'level_label': _level_label(drilldown_payload.get('level')),
        'object_name': drilldown_payload.get('object_name'),
        'period': drilldown_payload.get('period'),
        'children_level': drilldown_payload.get('children_level'),
        'children_level_label': _level_label(drilldown_payload.get('children_level')),
        'focus': focus,
        'top_summary': top_summary,
        'items_count': len(prepared_items),
        'items': prepared_items,
        'items_meta': _build_items_meta_view(drilldown_payload),
        'full_view': drilldown_payload.get('full_view', False),
        'consistency': _build_consistency_view(drilldown_payload),
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
        'focus': _build_focus_block(comparison_payload),
        'top_drain_metric': comparison_payload.get('top_drain_metric'),
        'top_drain_metric_label': METRIC_LABELS.get(comparison_payload.get('top_drain_metric'), comparison_payload.get('top_drain_metric')),
        'top_drain_effect': _round(comparison_payload.get('top_drain_effect', 0.0)),
        'reasons': reasons[:MAX_REASON_ITEMS],
        'management': {
            'problem': _format_problem_message(comparison_payload),
            'reason': _build_reason_summary(_extract_negative_reasons(reasons, limit=2)),
            'action': _build_action_summary(comparison_payload),
            'effect': _build_effect_summary(comparison_payload),
        },
        'flags': {'business_flags': comparison_payload.get('flags', {}), 'data_flags': _extract_data_flags(comparison_payload.get('flags', {}))},
        'consistency': _build_consistency_view(comparison_payload),
    }


def build_losses_view_from_summary(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    reasons = _extract_reasons_from_summary(comparison_payload)
    negative = [item for item in reasons if item['is_negative_for_business']]
    losses = []
    for item in negative[:MAX_LOSSES_ITEMS]:
        losses.append({
            'object_name': comparison_payload.get('object_name'),
            'level': comparison_payload.get('level'),
            'level_label': _level_label(comparison_payload.get('level')),
            'metric': item.get('metric'),
            'metric_label': item.get('metric_label'),
            'effect_value': item.get('effect_value'),
            'is_negative_for_business': item.get('is_negative_for_business', False),
        })
    return {
        'level': comparison_payload.get('level'),
        'level_label': _level_label(comparison_payload.get('level')),
        'object_name': comparison_payload.get('object_name'),
        'period': comparison_payload.get('period'),
        'losses': losses,
        'consistency': _build_consistency_view(comparison_payload),
        'action': {'suggested_action': _build_action_summary(comparison_payload), 'next_step': comparison_payload.get('action', {}).get('next_step')},
    }


def build_losses_view_from_children(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = drilldown_payload.get('items', [])
    losses = []
    for item in items[:MAX_LOSSES_ITEMS]:
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
            'focus': management.get('focus'),
            'management': management.get('management'),
            'flags': {'business_flags': item.get('flags', {}), 'data_flags': _extract_data_flags(item.get('flags', {}))},
        })
    return {
        'level': drilldown_payload.get('level'),
        'level_label': _level_label(drilldown_payload.get('level')),
        'object_name': drilldown_payload.get('object_name'),
        'period': drilldown_payload.get('period'),
        'children_level': drilldown_payload.get('children_level'),
        'children_level_label': _level_label(drilldown_payload.get('children_level')),
        'items_meta': _build_items_meta_view(drilldown_payload),
        'losses': losses,
        'action': {'suggested_action': 'выбрать объект с максимальными потерями и провалиться глубже', 'next_step': drilldown_payload.get('children_level')},
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
    return {'suggested_action': suggestion, 'next_step': next_step}


def build_comparison_management_view(query: Dict[str, Any], current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    current_obj = current.get('metrics', {}).get('object_metrics', {})
    previous_obj = previous.get('metrics', {}).get('object_metrics', {})
    current_finrez = _round(current.get('signal', {}).get('finrez_pre', 0.0))
    previous_finrez = _round(previous.get('signal', {}).get('finrez_pre', 0.0))
    current_margin_pre = _round(current.get('context', {}).get('margin_pre_object', 0.0))
    previous_margin_pre = _round(previous.get('context', {}).get('margin_pre_object', 0.0))
    current_margin_gap = _round(current.get('context', {}).get('margin_gap', 0.0))
    previous_margin_gap = _round(previous.get('context', {}).get('margin_gap', 0.0))
    delta_finrez = _round(current_finrez - previous_finrez)
    delta_kpi_gap = _round(current.get('navigation', {}).get('kpi_gap', 0.0) - previous.get('navigation', {}).get('kpi_gap', 0.0))
    delta_margin_pre = _round(current_margin_pre - previous_margin_pre)
    delta_margin_gap = _round(current_margin_gap - previous_margin_gap)
    delta_gap_loss_money = _round(current.get('impact', {}).get('gap_loss_money', 0.0) - previous.get('impact', {}).get('gap_loss_money', 0.0))
    diagnosis_changes = []
    for metric in ['retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs']:
        delta_value = _round(current_obj.get(metric, 0.0) - previous_obj.get(metric, 0.0))
        diagnosis_changes.append({'metric': metric, 'label': METRIC_LABELS.get(metric, metric), 'delta_value': delta_value, 'direction': 'ухудшение' if delta_value > 0 else ('улучшение' if delta_value < 0 else 'без изменений'), 'is_negative_for_business': delta_value > 0})
    negative = [c for c in diagnosis_changes if c['is_negative_for_business']]
    source = negative if negative else diagnosis_changes
    main_change = max(source, key=lambda x: abs(x['delta_value'])) if source else {'metric': None, 'label': None, 'delta_value': 0.0, 'direction': 'без изменений'}
    deterioration = delta_finrez < 0 or delta_kpi_gap > 0 or delta_margin_gap < 0
    status_change = _status_change_label(current.get('signal', {}).get('status'), previous.get('signal', {}).get('status'))
    priority_change = _priority_change_label(current.get('priority', {}).get('priority'), previous.get('priority', {}).get('priority'))
    action = _build_comparison_action(main_change, query.get('level'), deterioration, current.get('action', {}).get('next_step'))
    management_reason = []
    if main_change.get('label'):
        direction = 'давление выросло' if main_change.get('delta_value', 0.0) > 0 else ('давление снизилось' if main_change.get('delta_value', 0.0) < 0 else 'без изменений')
        management_reason.append(f"{main_change.get('label')}: {direction} на {abs(_round(main_change.get('delta_value', 0.0)))} грн")
    if delta_margin_pre != 0:
        management_reason.append(f"маржа до распределения: {'+' if delta_margin_pre > 0 else '-'}{abs(delta_margin_pre)} п.п.")
    return {
        'mode': 'comparison',
        'level': query.get('level'),
        'level_label': _level_label(query.get('level')),
        'object_name': query.get('object_name'),
        'period_current': query.get('period_current'),
        'period_previous': query.get('period_previous'),
        'focus': {'problem': 'прибыль выросла' if delta_finrez > 0 else ('прибыль снизилась' if delta_finrez < 0 else 'прибыль без изменений'), 'signal_label': current.get('signal', {}).get('label', 'OK'), 'priority': current.get('signal', {}).get('priority'), 'problem_money': _round(abs(delta_finrez)) if delta_finrez < 0 else 0.0, 'focus_metric': main_change.get('label'), 'focus_action': action.get('suggested_action')},
        'signal': {'status': 'ok' if delta_finrez >= 0 else 'risk', 'label': 'OK' if delta_finrez >= 0 else 'RISK', 'comment': 'результат улучшился период к периоду' if delta_finrez > 0 else ('результат ухудшился период к периоду' if delta_finrez < 0 else 'результат без изменений'), 'reason': main_change.get('metric'), 'reason_value': _round(abs(main_change.get('delta_value', 0.0)))},
        'comparison': {'finrez_pre': {'previous': previous_finrez, 'current': current_finrez, 'delta': delta_finrez}, 'margin_pre': {'previous': previous_margin_pre, 'current': current_margin_pre, 'delta': delta_margin_pre}},
        'cause': {'items': diagnosis_changes[:MAX_REASON_ITEMS], 'main_driver_metric': main_change.get('metric'), 'main_driver_label': main_change.get('label'), 'main_driver_delta': main_change.get('delta_value'), 'main_driver_direction': main_change.get('direction')},
        'money': {'delta_finrez_pre': delta_finrez, 'delta_gap_loss_money': delta_gap_loss_money},
        'priority_change': {'status_current': current.get('signal', {}).get('status'), 'status_previous': previous.get('signal', {}).get('status'), 'priority_current': current.get('priority', {}).get('priority'), 'priority_previous': previous.get('priority', {}).get('priority'), 'status_change': status_change, 'priority_change': priority_change},
        'action': action,
        'management': {'problem': 'прибыль выросла' if delta_finrez > 0 else ('прибыль снизилась' if delta_finrez < 0 else 'прибыль без изменений'), 'reason': management_reason, 'action': action.get('suggested_action'), 'effect': f'финрез: {previous_finrez} → {current_finrez} грн'},
        'current': build_management_view(current),
        'previous': build_management_view(previous),
        'delta': {'finrez_pre': delta_finrez, 'kpi_gap': delta_kpi_gap, 'margin_pre': delta_margin_pre, 'margin_gap': delta_margin_gap, 'gap_loss_money': delta_gap_loss_money},
    }


def build_signal_flow_view(summary_payload: Dict[str, Any], drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    summary_view = build_management_view(summary_payload)
    drilldown_view = build_drilldown_management_view(drilldown_payload)

    next_level = drilldown_payload.get('children_level')

    return {
        'mode': 'signal',
        'level': summary_view.get('level'),
        'level_label': summary_view.get('level_label'),
        'object_name': summary_view.get('object_name'),
        'period': summary_view.get('period'),
        'summary': summary_view,
        'focus': summary_view.get('focus'),
        'signal': summary_view.get('signal'),
        'consistency': summary_view.get('consistency'),
        'top_summary': drilldown_view.get('top_summary'),
        'next_level': next_level,
        'next_level_label': _level_label(next_level) if next_level else None,
        'items_count': drilldown_view.get('items_count', 0),
        'items': drilldown_view.get('items', []),
        'items_meta': drilldown_view.get('items_meta'),
        'full_view': drilldown_view.get('full_view', False),
        'allowed_next': ['drilldown', 'reasons', 'losses', 'compare'],
        'action': {
            'suggested_action': f"выбрать объект уровня {_level_label(next_level)} и провалиться глубже" if next_level else summary_view.get('action', {}).get('suggested_action'),
            'next_step': next_level,
        },
    }
