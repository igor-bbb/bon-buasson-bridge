from typing import Any, Dict, List, Optional


ACTION_MAP = {
    'retro_bonus': 'Снизить ретробонус',
    'logistics_cost': 'Сократить логистическое плечо',
    'personnel_cost': 'Оптимизировать нагрузку персонала',
    'other_costs': 'Оптимизировать прочие затраты',
}

BUSINESS_COMPARE_FIELDS = [
    'revenue',
    'retro_bonus',
    'logistics_cost',
    'personnel_cost',
    'other_costs',
    'margin_pre',
    'markup',
    'finrez_pre',
    'finrez_final',
]

OBJECT_COMPARE_FIELDS = [
    'revenue',
    'margin_pre',
    'markup',
    'finrez_pre',
]

PP_FIELDS = {'margin_pre', 'markup'}


LEVEL_COMMANDS = {
    'sku': ['причины'],
}

DEFAULT_COMMANDS = ['причины', 'все', '1', '2', '3']


METRIC_LABELS = {
    'retro_bonus': 'Ретробонус',
    'logistics_cost': 'Логистика',
    'personnel_cost': 'Персонал',
    'other_costs': 'Прочее',
}


METRIC_DISPLAY_ORDER = {
    'business': [
        'revenue',
        'retro_bonus',
        'logistics_cost',
        'personnel_cost',
        'other_costs',
        'margin_pre',
        'markup',
        'finrez_pre',
        'finrez_final',
    ],
    'default': [
        'revenue',
        'margin_pre',
        'markup',
        'finrez_pre',
    ],
}

METRIC_TITLES = {
    'revenue': 'Оборот',
    'retro_bonus': 'Ретробонус',
    'logistics_cost': 'Логистика',
    'personnel_cost': 'Персонал',
    'other_costs': 'Прочее',
    'margin_pre': 'Маржа до',
    'markup': 'Наценка',
    'finrez_pre': 'Финрез до',
    'finrez_final': 'Финрез итог',
}

MIN_DRAIN_ITEMS = 3


def _round(value: Any, digits: int = 2) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None


def _safe_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    return (payload.get('metrics') or {}).get('object_metrics') or {}


def _safe_business_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    return (payload.get('metrics') or {}).get('business_metrics') or {}


def _safe_previous_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    return payload.get('previous_object_metrics') or {}


def _percent_change(current: Any, previous: Any) -> Optional[float]:
    current_num = _round(current, 6)
    previous_num = _round(previous, 6)
    if current_num is None or previous_num is None or abs(previous_num) < 1e-9:
        return None
    return _round(((current_num - previous_num) / abs(previous_num)) * 100.0)


def _pp_change(current: Any, previous: Any) -> Optional[float]:
    current_num = _round(current, 6)
    previous_num = _round(previous, 6)
    if current_num is None or previous_num is None:
        return None
    return _round(current_num - previous_num)


def _metric_compare_map(current_metrics: Dict[str, Any], previous_metrics: Dict[str, Any], fields: List[str]) -> Dict[str, Optional[float]]:
    compare: Dict[str, Optional[float]] = {}
    for field in fields:
        current = current_metrics.get(field)
        previous = previous_metrics.get(field)
        compare[field] = _pp_change(current, previous) if field in PP_FIELDS else _percent_change(current, previous)
    return compare


def _raw_finrez(item: Dict[str, Any]) -> Optional[float]:
    metrics = (item.get('metrics') or {}).get('object_metrics') or {}
    raw = metrics.get('finrez_pre') if 'metrics' in item else item.get('finrez_pre')
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _gap_margin_pp(payload: Dict[str, Any]) -> Optional[float]:
    object_margin = _safe_metrics(payload).get('margin_pre')
    business_margin = _safe_business_metrics(payload).get('margin_pre')
    return _pp_change(object_margin, business_margin)


def _gap_value(payload: Dict[str, Any]) -> Optional[float]:
    impact = payload.get('impact') or {}
    gap_loss_money = impact.get('gap_loss_money')
    if gap_loss_money is not None:
        return _round(gap_loss_money)

    object_metrics = _safe_metrics(payload)
    business_metrics = _safe_business_metrics(payload)
    revenue = object_metrics.get('revenue')
    business_margin = business_metrics.get('margin_pre')
    finrez_pre = object_metrics.get('finrez_pre')
    if revenue is None or business_margin is None or finrez_pre is None:
        return None
    expected_finrez = (float(revenue) * float(business_margin)) / 100.0
    return _round(expected_finrez - float(finrez_pre))


def _build_metrics(level: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = _safe_metrics(payload)
    if level == 'business':
        return {
            'revenue': _round(metrics.get('revenue')),
            'retro_bonus': _round(metrics.get('retro_bonus')),
            'logistics_cost': _round(metrics.get('logistics_cost')),
            'personnel_cost': _round(metrics.get('personnel_cost')),
            'other_costs': _round(metrics.get('other_costs')),
            'margin_pre': _round(metrics.get('margin_pre')),
            'markup': _round(metrics.get('markup')),
            'finrez_pre': _round(metrics.get('finrez_pre')),
            'finrez_final': _round(metrics.get('finrez') if metrics.get('finrez') is not None else metrics.get('finrez_final')),
        }
    return {
        'revenue': _round(metrics.get('revenue')),
        'margin_pre': _round(metrics.get('margin_pre')),
        'markup': _round(metrics.get('markup')),
        'finrez_pre': _round(metrics.get('finrez_pre')),
    }


def _build_comparisons(level: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    current = _safe_metrics(payload)
    previous = _safe_previous_metrics(payload)
    fields = BUSINESS_COMPARE_FIELDS if level == 'business' else OBJECT_COMPARE_FIELDS
    return _metric_compare_map(current, previous, fields)


def _build_vs_business(level: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if level == 'business':
        return None
    business_metrics = _safe_business_metrics(payload)
    return {
        'business_margin_pre': _round(business_metrics.get('margin_pre')),
        'gap_to_business_pp': _gap_margin_pp(payload),
        'lost_profit_value': _gap_value(payload),
    }


def _format_money(value: Any) -> str:
    rounded = _round(value)
    if rounded is None:
        return '—'
    return f"{int(round(rounded)):,}".replace(',', ' ')


def _format_percent(value: Any) -> str:
    rounded = _round(value)
    if rounded is None:
        return '—'
    return f"{rounded:.2f}%"


def _format_pp(value: Any) -> str:
    rounded = _round(value)
    if rounded is None:
        return '—'
    return f"{rounded:+.2f} п.п."


def _format_delta(field: str, value: Any) -> str:
    if value is None:
        return '—'
    return _format_pp(value) if field in PP_FIELDS else _format_percent(value)


def _format_metric_value(field: str, value: Any) -> str:
    if field in PP_FIELDS:
        return _format_percent(value)
    return _format_money(value)


def _build_metric_rows(level: str, metrics: Dict[str, Any], comparisons: Dict[str, Any]) -> List[Dict[str, Any]]:
    order = METRIC_DISPLAY_ORDER['business'] if level == 'business' else METRIC_DISPLAY_ORDER['default']
    rows: List[Dict[str, Any]] = []
    for field in order:
        rows.append({
            'field': field,
            'label': METRIC_TITLES.get(field, field),
            'value': metrics.get(field),
            'value_display': _format_metric_value(field, metrics.get(field)),
            'yoy': comparisons.get(field),
            'yoy_display': _format_delta(field, comparisons.get(field)),
            'line': f"{METRIC_TITLES.get(field, field)} { _format_metric_value(field, metrics.get(field)) } { _format_delta(field, comparisons.get(field)) }",
        })
    return rows


def _build_reason_summary_lines(summary: Dict[str, Any]) -> List[str]:
    return [
        f"Бизнес маржа {_format_percent(summary.get('business_margin_pre'))}",
        f"Факт {_format_percent(summary.get('object_margin_pre'))}",
        f"Отклонение {_format_pp(summary.get('gap_pp'))}",
        f"Недозаработано к бизнесу {_format_money(summary.get('lost_profit_value'))}",
    ]


def _build_drain_rows(payload: Dict[str, Any], include_positive: bool = False, max_items: int = 5) -> List[Dict[str, Any]]:
    raw_items = payload.get('all_items') or payload.get('items') or []
    items = [item for item in raw_items if isinstance(item, dict)]
    prepared: List[Dict[str, Any]] = []
    for item in items:
        finrez = _raw_finrez(item)
        if finrez is None:
            continue
        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        prepared.append({
            'object_name': item.get('object_name'),
            'finrez_pre': _round(finrez),
            'margin_pre': _round(metrics.get('margin_pre')),
            'revenue': _round(metrics.get('revenue')),
        })

    prepared.sort(key=lambda x: float(x.get('finrez_pre') or 0.0))
    negatives = [row for row in prepared if (row.get('finrez_pre') or 0) < 0]
    negative_total = sum(abs(float(row.get('finrez_pre') or 0.0)) for row in negatives)

    if include_positive:
        selected = prepared[:max_items]
    else:
        selected = negatives[:max_items]
        if len(selected) < MIN_DRAIN_ITEMS and len(negatives) > len(selected):
            selected = negatives[:max(MIN_DRAIN_ITEMS, min(max_items, len(negatives)))]

    out: List[Dict[str, Any]] = []
    for row in selected:
        finrez = float(row.get('finrez_pre') or 0.0)
        share = (abs(finrez) / negative_total * 100.0) if negative_total and finrez < 0 else 0.0
        out.append({
            **row,
            'impact_share': _round(share),
            'impact_share_display': _format_percent(share),
            'finrez_pre_display': _format_money(finrez),
        })
    return out


def _warning_flag(payload: Dict[str, Any]) -> bool:
    consistency = payload.get('consistency') or {}
    status = consistency.get('status')
    return status in {'warning', 'critical'}


def _children_level_from_payload(payload: Optional[Dict[str, Any]]) -> Optional[str]:
    if not payload:
        return None
    return payload.get('children_level') or payload.get('level')


def _commands(level: str) -> List[str]:
    return LEVEL_COMMANDS.get(level, DEFAULT_COMMANDS)


def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    level = payload.get('level')
    source = drain_payload if drain_payload is not None else payload
    metrics = _build_metrics(level, payload)
    comparisons = _build_comparisons(level, payload)
    drain_items = [] if level == 'sku' else _build_drain_rows(source, include_positive=False)
    return {
        'type': 'management',
        'mode': 'management',
        'view_mode': 'drain',
        'level': level,
        'object_name': payload.get('object_name'),
        'period': payload.get('period'),
        'children_level': _children_level_from_payload(drain_payload),
        'metrics': metrics,
        'comparisons': comparisons,
        'metric_rows': _build_metric_rows(level, metrics, comparisons),
        'vs_business': _build_vs_business(level, payload),
        'drain_items': drain_items,
        'losses': drain_items,
        'commands': _commands(level),
        'warning_flag': _warning_flag(payload) or _warning_flag(source),
        'consistency': payload.get('consistency') or source.get('consistency'),
        'signal': payload.get('signal'),
        'navigation': payload.get('navigation'),
        'context': payload.get('context'),
        'diagnosis': payload.get('diagnosis'),
        'impact': payload.get('impact'),
        'priority': payload.get('priority'),
        'action': payload.get('action'),
    }


def build_management_view(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_object_view(comparison_payload)


def build_list_view(scope_payload: Dict[str, Any], list_payload: Dict[str, Any]) -> Dict[str, Any]:
    level = scope_payload.get('level')
    metrics = _build_metrics(level, scope_payload)
    comparisons = _build_comparisons(level, scope_payload)
    return {
        'type': 'management_list',
        'mode': 'management',
        'view_mode': 'all',
        'level': level,
        'object_name': scope_payload.get('object_name'),
        'period': scope_payload.get('period'),
        'children_level': list_payload.get('children_level') or list_payload.get('level'),
        'metrics': metrics,
        'comparisons': comparisons,
        'metric_rows': _build_metric_rows(level, metrics, comparisons),
        'vs_business': _build_vs_business(level, scope_payload),
        'items': _build_drain_rows(list_payload, include_positive=True, max_items=100),
        'commands': _commands(level),
        'warning_flag': _warning_flag(scope_payload) or _warning_flag(list_payload),
        'consistency': scope_payload.get('consistency') or list_payload.get('consistency'),
    }


def build_drilldown_management_view(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_object_view(drilldown_payload)


def build_comparison_management_view(query: Dict[str, Any], current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    current_view = build_object_view(current)
    previous_view = build_object_view(previous)
    current_signal = current.get('signal') or {}
    previous_signal = previous.get('signal') or {}
    current_priority = (current.get('priority') or {}).get('priority')
    previous_priority = (previous.get('priority') or {}).get('priority')
    current_impact = current.get('impact') or {}
    per_metric = current_impact.get('per_metric_effects') or {}
    main_driver_metric = None
    allowed_driver_metrics = {'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs'}
    filtered_metric_effects = {k: v for k, v in per_metric.items() if k in allowed_driver_metrics}
    if filtered_metric_effects:
        main_driver_metric = max(filtered_metric_effects, key=lambda k: abs(float(filtered_metric_effects.get(k) or 0.0)))
    return {
        'type': 'comparison',
        'mode': 'comparison',
        'level': current.get('level'),
        'object_name': current.get('object_name'),
        'period_current': query.get('period_current'),
        'period_previous': query.get('period_previous'),
        'current': current_view,
        'previous': previous_view,
        'signal': {
            'current': current_signal,
            'previous': previous_signal,
            'delta_status': 'без изменений' if current_signal.get('status') == previous_signal.get('status') else 'изменился',
        },
        'navigation': {
            'current': current.get('navigation'),
            'previous': previous.get('navigation'),
        },
        'context': {
            'current': current.get('context'),
            'previous': previous.get('context'),
        },
        'diagnosis_change': {
            'current': current.get('diagnosis'),
            'previous': previous.get('diagnosis'),
        },
        'impact': {
            'current': current_impact,
            'previous': previous.get('impact'),
            'main_driver_metric': main_driver_metric,
        },
        'priority_change': {
            'current': current_priority,
            'previous': previous_priority,
        },
        'action': {
            'current': current.get('action'),
            'previous': previous.get('action'),
        },
    }


def build_losses_view_from_children(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    losses = _build_drain_rows(drilldown_payload, include_positive=False)
    return {
        'type': 'losses',
        'mode': 'management',
        'level': drilldown_payload.get('level'),
        'object_name': drilldown_payload.get('object_name'),
        'period': drilldown_payload.get('period'),
        'children_level': drilldown_payload.get('children_level'),
        'items': losses,
        'losses': losses,
        'warning_flag': _warning_flag(drilldown_payload),
    }


def build_reasons_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    object_metrics = _safe_metrics(payload)
    business_metrics = _safe_business_metrics(payload)
    revenue = float(object_metrics.get('revenue') or 0.0)
    diagnosis = payload.get('diagnosis') or {}
    effects = diagnosis.get('effects_by_metric') or {}

    reason_rows: List[Dict[str, Any]] = []
    negative_total = 0.0
    for factor in ['retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs']:
        effect_payload = effects.get(factor) or {}
        impact_value = float(effect_payload.get('effect_value') or 0.0)
        is_negative = bool(effect_payload.get('is_negative_for_business', False))
        if is_negative:
            negative_total += abs(impact_value)
        fact_value = float(object_metrics.get(factor) or 0.0)
        fact_percent = (fact_value / revenue * 100.0) if revenue else 0.0
        business_value = float(business_metrics.get(factor) or 0.0)
        business_revenue = float(business_metrics.get('revenue') or 0.0)
        business_percent = (business_value / business_revenue * 100.0) if business_revenue else 0.0
        reason_rows.append({
            'factor': factor,
            'factor_label': METRIC_LABELS.get(factor, factor),
            'fact_value': _round(fact_value),
            'fact_value_display': _format_money(fact_value),
            'fact_percent': _round(fact_percent),
            'fact_percent_display': _format_percent(fact_percent),
            'business_percent': _round(business_percent),
            'business_percent_display': _format_percent(business_percent),
            'gap_pp': _round(fact_percent - business_percent),
            'gap_pp_display': _format_pp(fact_percent - business_percent),
            'impact_value': _round(abs(impact_value)),
            'impact_value_display': _format_money(abs(impact_value)),
            'impact_share': 0.0,
            'impact_share_display': _format_percent(0.0),
            'is_primary': False,
            'is_negative_for_business': is_negative,
            'action': ACTION_MAP.get(factor),
        })

    reason_rows.sort(key=lambda x: float(x.get('impact_value') or 0.0), reverse=True)
    for idx, row in enumerate(reason_rows):
        impact_value = float(row.get('impact_value') or 0.0)
        share = (impact_value / negative_total * 100.0) if negative_total else 0.0
        row['impact_share'] = _round(share)
        row['impact_share_display'] = _format_percent(share)
        row['is_primary'] = idx == 0
        row['priority_icon'] = '🔥' if share > 30 else ('⚠️' if share >= 10 else '')
        row['lines'] = [
            f"Факт {row['fact_percent_display']} ({row['fact_value_display']})",
            f"Норма бизнеса {row['business_percent_display']}",
            f"Отклонение {row['gap_pp_display']}",
            f"Влияние {row['impact_value_display']} ({row['impact_share_display']})",
        ]

    summary = {
        'business_margin_pre': _round(business_metrics.get('margin_pre')),
        'object_margin_pre': _round(object_metrics.get('margin_pre')),
        'gap_pp': _gap_margin_pp(payload),
        'lost_profit_value': _gap_value(payload),
    }

    return {
        'type': 'reasons',
        'mode': 'reasons',
        'level': payload.get('level'),
        'object_name': payload.get('object_name'),
        'period': payload.get('period'),
        'summary': summary,
        'summary_lines': _build_reason_summary_lines(summary),
        'reasons': reason_rows[:5],
        'commands': ['причины'],
        'warning_flag': _warning_flag(payload),
        'consistency': payload.get('consistency'),
    }


def build_losses_view_from_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    diagnosis = payload.get('diagnosis') or {}
    effects = diagnosis.get('effects_by_metric') or {}
    losses = []
    for factor, effect_payload in effects.items():
        if not effect_payload.get('is_negative_for_business', False):
            continue
        losses.append({
            'factor': factor,
            'factor_label': METRIC_LABELS.get(factor, factor),
            'impact_value': _round(abs(float(effect_payload.get('effect_value') or 0.0))),
            'is_negative_for_business': True,
        })
    losses.sort(key=lambda x: float(x.get('impact_value') or 0.0), reverse=True)
    return {
        'type': 'losses',
        'mode': 'management',
        'level': payload.get('level'),
        'object_name': payload.get('object_name'),
        'period': payload.get('period'),
        'losses': losses,
        'warning_flag': _warning_flag(payload),
        'consistency': payload.get('consistency'),
    }
