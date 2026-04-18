from typing import Any, Dict, List, Optional

from app.config import DRAIN_MAX_ITEMS, DRAIN_MIN_ITEMS, DRAIN_SHARE_THRESHOLD
from app.domain.decision import build_decision_block


ACTION_MAP = {
    'retro_bonus': 'Проверить ретробонус',
    'logistics_cost': 'Снизить логистику',
    'personnel_cost': 'Сократить персонал',
    'other_costs': 'Снизить прочие затраты',
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

PP_FIELDS = {'margin_pre', 'markup', 'kpi_gap'}

LEVEL_COMMANDS = {
    'sku': ['причины', 'назад'],
}

DEFAULT_COMMANDS = ['причины', 'все', '1', '2', '3', 'назад']

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
        'kpi_gap',
        'finrez_pre',
    ],
}

METRIC_TITLES = {
    'revenue': 'Оборот',
    'retro_bonus': 'Ретробонус',
    'logistics_cost': 'Логистика',
    'personnel_cost': 'Персонал',
    'other_costs': 'Прочее',
    'margin_pre': 'Маржа',
    'markup': 'Наценка',
    'kpi_gap': 'Разрыв',
    'finrez_pre': 'Финрез',
    'finrez_final': 'Финрез итог',
}

SIGNAL_LABELS = {
    'critical': 'Критично',
    'risk': 'Риск',
    'attention': 'Внимание',
    'ok': 'Норма',
    'no_data': 'Нет данных',
}

MIN_DRAIN_ITEMS = DRAIN_MIN_ITEMS
MAX_DRAIN_ITEMS = DRAIN_MAX_ITEMS


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


def _gap_margin_pp(payload: Dict[str, Any]) -> Optional[float]:
    return _round((payload.get('signal') or {}).get('margin_gap'))


def _gap_value(payload: Dict[str, Any]) -> Optional[float]:
    return _round((payload.get('impact') or {}).get('gap_loss_money'))


def _top_driver(payload: Dict[str, Any]) -> Optional[str]:
    diagnosis = payload.get('diagnosis') or {}
    return diagnosis.get('top_drain_metric')


def _format_money(value: Any) -> str:
    rounded = _round(value)
    if rounded is None:
        return '—'
    sign = '−' if rounded < 0 else ''
    return f"{sign}{int(round(abs(rounded))):,}".replace(',', ' ')


def _format_percent(value: Any) -> str:
    rounded = _round(value)
    if rounded is None:
        return '—'
    sign = '+' if rounded > 0 else ('−' if rounded < 0 else '')
    return f"{sign}{abs(rounded):.2f}%"


def _format_plain_percent(value: Any) -> str:
    rounded = _round(value)
    if rounded is None:
        return '—'
    return f"{rounded:.2f}%"


def _format_pp(value: Any) -> str:
    rounded = _round(value)
    if rounded is None:
        return '—'
    sign = '+' if rounded > 0 else ('-' if rounded < 0 else '')
    return f"{sign}{abs(rounded):.2f} п.п."


def _format_delta(field: str, value: Any) -> str:
    if value is None:
        return '—'
    return _format_pp(value) if field in PP_FIELDS else _format_percent(value)


def _format_metric_value(field: str, value: Any) -> str:
    if field in PP_FIELDS:
        return _format_plain_percent(value)
    return _format_money(value)


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
            'finrez_final': _round(metrics.get('finrez_final')),
        }
    return {
        'revenue': _round(metrics.get('revenue')),
        'margin_pre': _round(metrics.get('margin_pre')),
        'markup': _round(metrics.get('markup')),
        'kpi_gap': _round(metrics.get('kpi_gap')),
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
    object_metrics = _safe_metrics(payload)
    business_metrics = _safe_business_metrics(payload)
    object_margin = _round(object_metrics.get('margin_pre'))
    object_markup = _round(object_metrics.get('markup'))
    object_finrez = _round(object_metrics.get('finrez_pre'))
    business_margin = _round(business_metrics.get('margin_pre'))
    business_markup = _round(business_metrics.get('markup'))
    business_finrez = _round(business_metrics.get('finrez_pre'))
    gap_margin_pp = None if object_margin is None or business_margin is None else _round(object_margin - business_margin)
    gap_markup_pp = None if object_markup is None or business_markup is None else _round(object_markup - business_markup)
    gap_finrez_value = None if object_finrez is None or business_finrez is None else _round(object_finrez - business_finrez)
    delta_prev_year = payload.get('delta_prev_year') or {}
    return {
        'business_margin': business_margin,
        'business_markup': business_markup,
        'business_finrez_pre': business_finrez,
        'gap_to_business_pp': _gap_margin_pp(payload),
        'lost_profit_value': _gap_value(payload),
        'gap_margin_pp': gap_margin_pp,
        'gap_markup_pp': gap_markup_pp,
        'gap_finrez_value': gap_finrez_value,
        'delta_prev_year': delta_prev_year,
    }


def _build_metric_rows(level: str, metrics: Dict[str, Any], comparisons: Dict[str, Any]) -> List[Dict[str, Any]]:
    order = METRIC_DISPLAY_ORDER['business'] if level == 'business' else METRIC_DISPLAY_ORDER['default']
    rows: List[Dict[str, Any]] = []

    for field in order:
        label = METRIC_TITLES.get(field, field)
        value = metrics.get(field)
        yoy = comparisons.get(field)

        rows.append({
            'field': field,
            'label': label,
            'value': value,
            'value_display': _format_metric_value(field, value),
            'yoy': yoy,
            'yoy_display': _format_delta(field, yoy),
            'line': f"{label}: {_format_metric_value(field, value)} ({_format_delta(field, yoy)} к ПГ)",
        })
    return rows


def _warning_flag(payload: Dict[str, Any]) -> bool:
    consistency = payload.get('consistency') or {}
    status = consistency.get('status')
    return status in {'warning', 'critical'}


def _children_level_from_payload(payload: Optional[Dict[str, Any]]) -> Optional[str]:
    if not payload:
        return None
    return payload.get('children_level') or payload.get('level')


def _compact_item_yoy(item: Dict[str, Any]) -> Optional[str]:
    current_metrics = (item.get('metrics') or {}).get('object_metrics') or {}
    previous_metrics = item.get('previous_object_metrics') or {}
    yoy = _percent_change(current_metrics.get('finrez_pre'), previous_metrics.get('finrez_pre'))
    return _format_percent(yoy) if yoy is not None else '—'


def _build_slice(payload: Dict[str, Any]) -> Dict[str, Any]:
    filter_payload = payload.get('filter') or {}
    return {
        'Бизнес': filter_payload.get('business') or 'Весь бизнес',
        'Дивизиональный менеджер': filter_payload.get('manager_top'),
        'Менеджер': filter_payload.get('manager'),
        'Сеть': filter_payload.get('network'),
        'SKU': filter_payload.get('sku'),
        'Период': payload.get('period'),
    }


def _context_path(payload: Dict[str, Any]) -> str:
    filter_payload = payload.get('filter') or {}
    parts: List[str] = ['Бизнес']
    for key in ['manager_top', 'manager', 'network', 'sku']:
        value = filter_payload.get(key)
        if value:
            parts.append(str(value))
    object_name = payload.get('object_name')
    if object_name and str(object_name).lower() != 'business' and str(object_name) not in parts:
        parts.append(str(object_name))
    return ' / '.join(parts)


def _format_period_label(period: Any) -> str:
    if not isinstance(period, str) or not period:
        return '—'
    if ':' in period:
        start, end = period.split(':', 1)
        return f'{start} → {end}'
    return period


def _build_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    level = str(payload.get('level') or '')
    metrics = _safe_metrics(payload)
    previous = _safe_previous_metrics(payload)
    business_metrics = _safe_business_metrics(payload)

    if level == 'business':
        current_finrez = _round(metrics.get('finrez_final'))
        base_finrez = _round(previous.get('finrez_final'))
    else:
        current_finrez = _round(metrics.get('finrez_pre'))
        base_finrez = _round(business_metrics.get('finrez_pre'))

    goal_value = None if current_finrez is None or base_finrez is None else _round(current_finrez - base_finrez)
    if goal_value is None:
        goal = {'type': 'unknown', 'description': None, 'value_money': None}
    elif goal_value < 0:
        goal = {'type': 'close_gap', 'description': f"закрыть {_format_money(goal_value)}", 'value_money': goal_value}
    elif goal_value > 0:
        goal = {'type': 'keep_growth', 'description': f"удержать +{_format_money(goal_value).replace('−', '')}", 'value_money': goal_value}
    else:
        goal = {'type': 'keep_result', 'description': 'удержать текущий результат', 'value_money': goal_value}
    return {
        'path': _context_path(payload),
        'period_label': _format_period_label(payload.get('period')),
        'goal': goal,
    }


def _build_drain_rows(payload: Dict[str, Any], max_items: int = MAX_DRAIN_ITEMS) -> List[Dict[str, Any]]:
    raw_items = payload.get('all_items') or payload.get('items') or []
    items = [item for item in raw_items if isinstance(item, dict)]

    prepared: List[Dict[str, Any]] = []
    for item in items:
        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        business_metrics = (item.get('metrics') or {}).get('business_metrics') or {}
        previous_metrics = item.get('previous_object_metrics') or {}
        impact = item.get('impact') or {}

        finrez = _round(metrics.get('finrez_pre'))
        margin = _round(metrics.get('margin_pre'))
        revenue = _round(metrics.get('revenue'))
        potential_money = _round(impact.get('gap_loss_money'))
        if potential_money is None:
            potential_money = abs(finrez) if finrez is not None and finrez < 0 else 0.0
        item_business_margin = _round(business_metrics.get('margin_pre'))
        gap_to_business_pp = None if margin is None or item_business_margin is None else _round(margin - item_business_margin)
        prev_finrez = _round(previous_metrics.get('finrez_pre'))
        delta_py_money = None if finrez is None or prev_finrez is None else _round(finrez - prev_finrez)
        delta_py_percent = _percent_change(finrez, prev_finrez)
        prepared.append({
            'object_name': item.get('object_name'),
            'fact': {
                'margin': margin,
                'margin_display': _format_plain_percent(margin),
                'finrez': finrez,
                'finrez_display': _format_money(finrez),
                'revenue': revenue,
                'revenue_display': _format_money(revenue),
            },
            'finrez_pre': finrez,
            'finrez_pre_display': _format_money(finrez),
            'margin': margin,
            'margin_display': _format_plain_percent(margin),
            'yoy_display': _format_percent(delta_py_percent),
            'delta_prev_year': {
                'money': delta_py_money,
                'money_display': _format_money(delta_py_money),
                'percent': _round(delta_py_percent),
                'percent_display': _format_percent(delta_py_percent),
            },
            'gap_to_business_pp': gap_to_business_pp,
            'gap_to_business_pp_display': _format_pp(gap_to_business_pp),
            'potential_money': potential_money,
            'potential_money_display': _format_money(potential_money),
            'is_negative_for_business': float(potential_money or 0.0) > 0.0,
            'line': f"{item.get('object_name')} | {_format_money(potential_money)} | {_format_plain_percent(margin)} | {_format_pp(gap_to_business_pp)}",
        })

    negatives = [row for row in prepared if row.get('is_negative_for_business')]
    negatives.sort(key=lambda row: float(row.get('potential_money') or 0.0), reverse=True)
    total_negative = sum(float(row.get('potential_money') or 0.0) for row in negatives)
    selected_negatives: List[Dict[str, Any]] = []
    covered = 0.0
    for row in negatives:
        selected_negatives.append(row)
        covered += float(row.get('potential_money') or 0.0)
        if len(selected_negatives) >= MIN_DRAIN_ITEMS and (covered >= total_negative * DRAIN_SHARE_THRESHOLD or len(selected_negatives) >= max_items):
            break
    selected_negatives = selected_negatives[:max_items]

    positives = [row for row in prepared if not row.get('is_negative_for_business') and float((row.get('fact') or {}).get('finrez') or 0.0) > 0.0]
    positives.sort(key=lambda row: float((row.get('fact') or {}).get('finrez') or 0.0), reverse=True)
    positives = positives[:3]
    payload['_positive_items_preview'] = positives

    def _decorate(rows: List[Dict[str, Any]], total: float) -> List[Dict[str, Any]]:
        decorated = []
        for idx, row in enumerate(rows, start=1):
            value = float(row.get('potential_money') or 0.0)
            share = round((value / total) * 100.0, 2) if total > 0 else None
            copy = dict(row)
            copy['rank'] = idx
            copy['share_of_total'] = share
            copy['share_of_total_display'] = _format_percent(share) if share is not None else '—'
            decorated.append(copy)
        return decorated

    selected_negatives = _decorate(selected_negatives, total_negative)
    payload['_negative_items_preview'] = selected_negatives
    return selected_negatives


def _build_positive_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    raw = payload.get('_positive_items_preview') or []
    total = sum(abs(float(r.get('finrez_pre') or 0.0)) for r in raw)
    out = []
    for row in raw:
        value = abs(float(row.get('finrez_pre') or 0.0))
        share = (value / total * 100.0) if total > 0 else 0.0
        clean = dict(row)
        clean['impact_share'] = _round(share)
        clean['impact_share_display'] = _format_percent(share)
        clean['line'] = f"{row['object_name']} | +{_format_money(value).replace('−','')} | {row['margin_display']} | {row['yoy_display']} к ПГ"
        out.append(clean)
    return out


def _build_list_items(list_payload: Dict[str, Any], max_items: int = 500) -> List[Dict[str, Any]]:
    raw_items = list_payload.get('all_items') or list_payload.get('items') or []
    items = [item for item in raw_items if isinstance(item, dict)]

    out: List[Dict[str, Any]] = []
    for item in items[:max_items]:
        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        impact = item.get('impact') or {}

        finrez = _round(metrics.get('finrez_pre'))
        gap_money = _round(impact.get('gap_loss_money'))
        if gap_money is None:
            gap_money = abs(finrez) if finrez is not None and finrez < 0 else finrez

        row = {
            'object_name': item.get('object_name'),
            'gap_loss_money': _round(gap_money),
            'gap_loss_money_display': _format_money(gap_money),
            'gap_percent': _round(impact.get('gap_percent')),
            'gap_percent_display': _format_pp(impact.get('gap_percent')),
            'finrez_pre': finrez,
            'finrez_pre_display': _format_money(finrez),
            'margin': _round(metrics.get('margin_pre')),
            'margin_display': _format_plain_percent(metrics.get('margin_pre')),
            'yoy_display': _compact_item_yoy(item),
        }
        row['line'] = (
            f"{row['object_name']} "
            f"Финрез: {row['finrez_pre_display']} | "
            f"Маржа: {row['margin_display']} | "
            f"Δ: {row['yoy_display']} | "
            f"→ {row['gap_loss_money_display']}"
        )
        out.append(row)

    out.sort(key=lambda row: float(row.get('gap_loss_money') or 0.0), reverse=True)
    return out


def _commands(level: str) -> List[str]:
    if level == 'business':
        return ['причины', 'все', '1', '2', '3']
    return LEVEL_COMMANDS.get(level, DEFAULT_COMMANDS)




def _build_summary_lines(level: str, payload: Dict[str, Any], drain_items: List[Dict[str, Any]]) -> List[str]:
    metrics = _safe_metrics(payload)
    impact = payload.get('impact') or {}
    lines: List[str] = []
    gap_value = _round(impact.get('gap_loss_money'))
    if level == 'business':
        if gap_value is not None:
            lines.append(f"Бизнес: {_format_money(gap_value)} к ПГ")
        if drain_items:
            lines.append(f"Главный дренаж: {drain_items[0].get('object_name')}")
    elif level in {'manager_top', 'manager', 'network'}:
        if gap_value is not None:
            lines.append(f"Отклонение: {_format_money(gap_value)} к бизнесу")
        top_driver = METRIC_LABELS.get(_top_driver(payload), _top_driver(payload) or '—')
        lines.append(f"Причина: {top_driver}")
        if drain_items:
            lines.append(f"Где: {drain_items[0].get('object_name')}")
    return lines


def _build_focus_block(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    focus = payload.get('ai_focus')
    if not isinstance(focus, dict):
        return None
    focus_type = focus.get('focus_type')
    focus_name = focus.get('name')
    if not focus_type or not focus_name:
        return None
    return {
        'type': focus_type,
        'object': focus_name,
        'share': _round((focus.get('share') or 0.0) / 100.0 if float(focus.get('share') or 0.0) > 1 else focus.get('share')),
        'loss_money': _round(focus.get('loss')),
    }


def _build_decision_view(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    block = build_decision_block(payload)
    actions = block.get('actions') or []
    if not actions:
        return None
    return {
        'actions': actions,
        'total_effect': _round(block.get('total_effect')),
        'goal_gap': _round(block.get('goal_gap')),
        'goal_closed': bool(block.get('goal_closed')),
    }

def _prune_nones(obj):
    if isinstance(obj, dict):
        return {k: _prune_nones(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_prune_nones(v) for v in obj if v is not None]
    return obj

def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    level = payload.get('level')
    source = drain_payload if drain_payload is not None else payload
    metrics_raw = _safe_metrics(payload)
    prev_raw = _safe_previous_metrics(payload)
    business_raw = _safe_business_metrics(payload)

    context_block = _build_context(payload)
    drain_block = [] if level == 'sku' else _build_drain_rows(source, max_items=MAX_DRAIN_ITEMS)
    focus_block = _build_focus_block(payload)

    metrics: Dict[str, Any] = {}
    metric_fields = BUSINESS_COMPARE_FIELDS if level == 'business' else OBJECT_COMPARE_FIELDS

    for field in metric_fields:
        value = _round(metrics_raw.get(field))
        base_value = _round(prev_raw.get(field)) if level == 'business' else _round(business_raw.get(field))

        if field in PP_FIELDS:
            entry = {
                'value_percent': value,
                'delta_percent': _round(_pp_change(value, base_value)),
                'value_money': _round(metrics_raw.get('finrez_final' if level == 'business' else 'finrez_pre')),
                'delta_money': None if (
                    _round(metrics_raw.get('finrez_final' if level == 'business' else 'finrez_pre')) is None
                    or _round((prev_raw if level == 'business' else business_raw).get('finrez_final' if level == 'business' else 'finrez_pre')) is None
                ) else _round(
                    _round(metrics_raw.get('finrez_final' if level == 'business' else 'finrez_pre')) -
                    _round((prev_raw if level == 'business' else business_raw).get('finrez_final' if level == 'business' else 'finrez_pre'))
                ),
            }
        else:
            entry = {
                'value_money': value,
                'delta_money': None if value is None or base_value is None else _round(value - base_value),
                'delta_percent': _round(_percent_change(value, base_value)),
            }

        if level != 'business':
            py_base = _round(prev_raw.get(field))
            py_delta_money = None if field in PP_FIELDS else (None if value is None or py_base is None else _round(value - py_base))
            py_delta_percent = _pp_change(value, py_base) if field in PP_FIELDS else _percent_change(value, py_base)
            delta_prev_year = _prune_nones({'money': py_delta_money, 'percent': _round(py_delta_percent)})
            if delta_prev_year:
                entry['delta_prev_year'] = delta_prev_year

        metrics[field] = _prune_nones(entry) or {}

    # locked GAP inside metrics only
    markup = _round(metrics_raw.get('markup'))
    margin_pre = _round(metrics_raw.get('margin_pre'))
    prev_markup = _round(prev_raw.get('markup')) if level == 'business' else _round(business_raw.get('markup'))
    prev_margin = _round(prev_raw.get('margin_pre')) if level == 'business' else _round(business_raw.get('margin_pre'))

    metrics['gap'] = _prune_nones({
        'value_percent': None if markup is None or margin_pre is None else _round(markup - margin_pre),
        'delta_percent': None if prev_markup is None or prev_margin is None or markup is None or margin_pre is None else _round((markup - margin_pre) - (prev_markup - prev_margin))
    }) or {}

    # remove finrez_final below business
    if level != 'business' and 'finrez_final' in metrics:
        del metrics['finrez_final']

    navigation = _prune_nones({
        'current_level': level,
        'next_level': _children_level_from_payload(drain_payload),
        'items': [item.get('object_name') for item in drain_block if item.get('object_name')],
        'all': True,
        'reasons': True,
        'back': True,
    }) or {}

    result = {
        'context': _prune_nones({
            'path': context_block.get('path'),
            'period': context_block.get('period_label'),
            'level': level,
            'object_name': payload.get('object_name'),
        }) or {},
        'metrics': metrics,
        'drain_block': drain_block,
        'goal': context_block.get('goal') or {},
        'focus_block': focus_block or {},
        'navigation': navigation,
    }
    return _prune_nones(result) or {}

def build_management_view(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_object_view(comparison_payload)


def build_list_view(scope_payload: Dict[str, Any], list_payload: Dict[str, Any]) -> Dict[str, Any]:
    level = scope_payload.get('level')
    metrics = _build_metrics(level, scope_payload)
    comparisons = _build_comparisons(level, scope_payload)
    items = _build_list_items(list_payload, max_items=500)

    return {
        'type': 'management_list',
        'mode': 'management',
        'view_mode': 'all',
        'level': level,
        'object_name': scope_payload.get('object_name'),
        'period': scope_payload.get('period'),
        'children_level': list_payload.get('children_level') or list_payload.get('level'),
        'slice': _build_slice(scope_payload),
        'header_note': 'к ПГ',
        'metrics': metrics,
        'comparisons': comparisons,
        'metric_rows': _build_metric_rows(level, metrics, comparisons),
        'vs_business': _build_vs_business(level, scope_payload),
        'items': items,
        'positive_items': _build_positive_items(list_payload),
        'commands': _commands(level),
        'warning_flag': _warning_flag(scope_payload) or _warning_flag(list_payload),
        'consistency': scope_payload.get('consistency') or list_payload.get('consistency'),
        'items_meta': list_payload.get('items_meta'),
        'context_block': _build_context(scope_payload),
    }


def build_comparison_management_view(query: Dict[str, Any], current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    current_view = build_object_view(current)
    previous_view = build_object_view(previous)

    current_signal = current.get('signal') or {}
    previous_signal = previous.get('signal') or {}
    current_priority = (current.get('priority') or {}).get('priority')
    previous_priority = (previous.get('priority') or {}).get('priority')
    current_impact = current.get('impact') or {}
    previous_impact = previous.get('impact') or {}

    per_metric = current_impact.get('per_metric_effects') or {}
    main_driver_metric = None
    if per_metric:
        filtered = {k: v for k, v in per_metric.items() if k in {'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs'}}
        if filtered:
            main_driver_metric = max(filtered, key=lambda k: abs(float(filtered.get(k) or 0.0)))

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
        'impact': {
            'current': current_impact,
            'previous': previous_impact,
            'main_driver_metric': main_driver_metric,
            'gap_loss_money_delta': _round(
                float(current_impact.get('gap_loss_money') or 0.0) - float(previous_impact.get('gap_loss_money') or 0.0)
            ),
        },
        'priority_change': {
            'current': current_priority,
            'previous': previous_priority,
        },
        'diagnosis_change': {
            'current': current.get('diagnosis'),
            'previous': previous.get('diagnosis'),
        },
        'action': {
            'current': current.get('action'),
            'previous': previous.get('action'),
        },
        'navigation': current.get('navigation'),
        'context': current.get('context'),
    }


def build_losses_view_from_children(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    losses = _build_drain_rows(drilldown_payload, max_items=500)
    return {
        'type': 'losses',
        'mode': 'management',
        'level': drilldown_payload.get('level'),
        'object_name': drilldown_payload.get('object_name'),
        'period': drilldown_payload.get('period'),
        'children_level': drilldown_payload.get('children_level'),
        'slice': _build_slice(drilldown_payload),
        'items': losses,
        'losses': losses,
        'warning_flag': _warning_flag(drilldown_payload),
    }


def build_losses_view_from_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    reason_view = build_reasons_view(payload)
    losses = [dict(item) for item in reason_view.get('reasons') or [] if item.get('is_negative_for_business')]
    for item in losses:
        item['loss_value'] = item.get('impact_value')
        item['loss_value_display'] = item.get('impact_value_display')
    return {
        'type': 'losses',
        'mode': 'management',
        'level': payload.get('level'),
        'object_name': payload.get('object_name'),
        'period': payload.get('period'),
        'slice': _build_slice(payload),
        'losses': losses,
        'items': losses,
    }


def build_reasons_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    object_metrics = _safe_metrics(payload)
    business_metrics = _safe_business_metrics(payload)
    diagnosis = payload.get('diagnosis') or {}
    impact = payload.get('impact') or {}

    effects = diagnosis.get('effects_by_metric') or impact.get('per_metric_effects') or {}
    revenue = float(object_metrics.get('revenue') or 0.0)
    business_revenue = float(business_metrics.get('revenue') or 0.0)

    reason_rows: List[Dict[str, Any]] = []
    total_negative = 0.0
    ordered_factors = ['markup', 'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs']

    def _impact_for_factor(factor: str) -> tuple[float, bool]:
        payload_value = effects.get(factor)
        if isinstance(payload_value, dict):
            effect_value = float(payload_value.get('effect_value') or 0.0)
            is_negative = bool(payload_value.get('is_negative_for_business'))
            return abs(effect_value), is_negative
        if factor == 'markup':
            gap = float((business_metrics.get('markup') or 0.0) - (object_metrics.get('markup') or 0.0))
            revenue_local = float(object_metrics.get('revenue') or 0.0)
            effect_value = abs(gap) * revenue_local / 100.0 if gap > 0 and revenue_local else 0.0
            return effect_value, effect_value > 0
        effect_value = float(payload_value or 0.0)
        return abs(effect_value), effect_value > 0

    for factor in ordered_factors:
        fact_value = float(object_metrics.get(factor) or 0.0)
        business_value = float(business_metrics.get(factor) or 0.0)

        if factor in PP_FIELDS:
            fact_percent = fact_value
            business_percent = business_value
        else:
            fact_percent = (fact_value / revenue * 100.0) if abs(revenue) > 1e-9 else 0.0
            business_percent = (business_value / business_revenue * 100.0) if abs(business_revenue) > 1e-9 else 0.0

        impact_value, is_negative = _impact_for_factor(factor)
        if is_negative:
            total_negative += abs(impact_value)

        delta_value = fact_value - business_value
        row = {
            'factor': factor,
            'factor_label': 'Наценка' if factor == 'markup' else METRIC_LABELS.get(factor, factor),
            'fact_value': _round(fact_value),
            'fact_value_display': _format_plain_percent(fact_value) if factor in PP_FIELDS else _format_money(fact_value),
            'business_value': _round(business_value),
            'business_value_display': _format_plain_percent(business_value) if factor in PP_FIELDS else _format_money(business_value),
            'fact_percent': _round(fact_percent),
            'fact_percent_display': _format_plain_percent(fact_percent),
            'business_percent': _round(business_percent),
            'business_percent_display': _format_plain_percent(business_percent),
            'delta_value': _round(delta_value),
            'delta_value_display': _format_money(delta_value) if factor not in PP_FIELDS else _format_plain_percent(delta_value),
            'gap_pp': _round(fact_percent - business_percent),
            'gap_pp_display': _format_pp(fact_percent - business_percent),
            'impact_value': _round(impact_value),
            'impact_value_display': _format_money(impact_value),
            'is_negative_for_business': bool(is_negative),
            'action': ACTION_MAP.get(factor, 'Проверить фактор'),
        }
        reason_rows.append(row)

    reason_rows.sort(key=lambda row: abs(float(row.get('impact_value') or 0.0)), reverse=True)

    for row in reason_rows:
        impact_value = float(row.get('impact_value') or 0.0)
        share = (abs(impact_value) / total_negative * 100.0) if total_negative > 0 and row.get('is_negative_for_business') else 0.0
        row['impact_share'] = _round(share)
        row['impact_share_display'] = _format_percent(share) if share > 0 else '—'
        factor_name = str(row.get('factor') or '')
        if factor_name in PP_FIELDS:
            row['lines'] = [
                f"Факт: {row['fact_value_display']}",
                f"Бизнес: {row['business_value_display']}",
                f"Δ: {row['gap_pp_display']}",
                f"Потеря: {row['impact_value_display']}",
            ]
        else:
            row['lines'] = [
                f"Факт: {row['fact_value_display']} ({row['fact_percent_display']})",
                f"Бизнес: {row['business_value_display']} ({row['business_percent_display']})",
                f"Δ: {row['delta_value_display']} ({row['gap_pp_display']})",
                f"Потеря: {row['impact_value_display']}",
            ]
        row['line'] = f"{row['factor_label']} факт {row['fact_value_display']} бизнес {row['business_value_display']} Δ {row['delta_value_display']} {row['gap_pp_display']}"

    summary = {
        'business_margin': _round(business_metrics.get('margin_pre')),
        'object_margin': _round(object_metrics.get('margin_pre')),
        'gap_pp': _gap_margin_pp(payload),
        'lost_profit_value': _gap_value(payload),
    }

    return {
        'type': 'reasons',
        'mode': 'management',
        'level': payload.get('level'),
        'object_name': payload.get('object_name'),
        'period': payload.get('period'),
        'slice': _build_slice(payload),
        'summary': summary,
        'reasons': reason_rows,
        'commands': ['решения', 'назад'],
        'context_block': _build_context(payload),
    }


# ===== STRICT LOCK v2 OVERRIDES =====

STRICT_METRIC_FIELDS_BUSINESS = [
    'revenue',
    'margin_pre',
    'markup',
    'finrez_pre',
    'finrez_final',
    'retro_bonus',
    'logistics_cost',
    'personnel_cost',
    'other_costs',
]

STRICT_METRIC_FIELDS_OBJECT = [
    'revenue',
    'margin_pre',
    'markup',
    'finrez_pre',
    'retro_bonus',
    'logistics_cost',
    'personnel_cost',
    'other_costs',
]

def _metric_entry(field: str, value: Optional[float], delta_money: Optional[float], delta_percent: Optional[float],
                  money_value: Optional[float] = None, allow_value_money: bool = True) -> Dict[str, Any]:
    entry: Dict[str, Any] = {}
    if field in PP_FIELDS:
        entry['value_percent'] = value
        entry['delta_percent'] = _round(delta_percent)
        if allow_value_money:
            entry['value_money'] = _round(money_value)
            entry['delta_money'] = _round(delta_money)
    else:
        entry['value_money'] = _round(value)
        entry['delta_money'] = _round(delta_money)
        entry['delta_percent'] = _round(delta_percent)
    return entry

def _strict_metrics(payload: Dict[str, Any], level: str) -> Dict[str, Any]:
    metrics_raw = _safe_metrics(payload)
    prev_raw = _safe_previous_metrics(payload)
    business_raw = _safe_business_metrics(payload)

    out: Dict[str, Any] = {}
    fields = STRICT_METRIC_FIELDS_BUSINESS if level == 'business' else STRICT_METRIC_FIELDS_OBJECT

    # shared bases
    rev_val = _round(metrics_raw.get('revenue'))
    rev_base = _round(prev_raw.get('revenue')) if level == 'business' else _round(business_raw.get('revenue'))
    finrez_pre_val = _round(metrics_raw.get('finrez_pre'))
    finrez_pre_base = _round(prev_raw.get('finrez_pre')) if level == 'business' else _round(business_raw.get('finrez_pre'))

    for field in fields:
        value = _round(metrics_raw.get(field))
        base = _round(prev_raw.get(field)) if level == 'business' else _round(business_raw.get(field))
        if field in PP_FIELDS:
            delta_percent = _pp_change(value, base)
            # money support for % metrics except gap: margin_pre -> finrez_pre, markup -> revenue
            if field == 'margin_pre':
                money_value = finrez_pre_val
                delta_money = None if finrez_pre_val is None or finrez_pre_base is None else _round(finrez_pre_val - finrez_pre_base)
            elif field == 'markup':
                money_value = rev_val
                delta_money = None if rev_val is None or rev_base is None else _round(rev_val - rev_base)
            else:
                money_value = None
                delta_money = None
            out[field] = _metric_entry(field, value, delta_money, delta_percent, money_value=money_value, allow_value_money=True)
        else:
            delta_money = None if value is None or base is None else _round(value - base)
            delta_percent = _percent_change(value, base)
            out[field] = _metric_entry(field, value, delta_money, delta_percent)

    gap_value = _round((metrics_raw.get('markup') or 0.0) - (metrics_raw.get('margin_pre') or 0.0))
    if level == 'business':
        gap_base = _round((prev_raw.get('markup') or 0.0) - (prev_raw.get('margin_pre') or 0.0))
    else:
        gap_base = _round((business_raw.get('markup') or 0.0) - (business_raw.get('margin_pre') or 0.0))
    out['gap'] = {
        'value_percent': gap_value,
        'delta_percent': None if gap_value is None or gap_base is None else _round(gap_value - gap_base),
    }
    return out

def _strict_drain_block(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    source = drain_payload if drain_payload is not None else payload
    rows = _build_drain_rows(source, max_items=MAX_DRAIN_ITEMS)
    clean: List[Dict[str, Any]] = []
    for row in rows:
        rev = _round(((row.get('fact') or {}).get('revenue')))
        pot = _round(row.get('potential_money'))
        name = str(row.get('object_name') or '').strip()
        if not name or name.lower() in {'пусто', 'без менеджера'}:
            continue
        if rev is None or rev <= 0:
            continue
        if pot is None or pot <= 0:
            continue
        clean.append({
            'object_name': row.get('object_name'),
            'fact': {
                'finrez': _round(((row.get('fact') or {}).get('finrez'))),
                'margin': _round(((row.get('fact') or {}).get('margin'))),
                'revenue': rev,
            },
            **({'delta_prev_year': row.get('delta_prev_year')} if row.get('delta_prev_year') and row.get('delta_prev_year') != {'money': None, 'percent': None} else {}),
            'gap_to_business_pp': _round(row.get('gap_to_business_pp')),
            'potential_money': pot,
            'potential_explanation': row.get('potential_explanation'),
        })
    clean.sort(key=lambda r: float(r.get('potential_money') or 0.0), reverse=True)
    return clean[:MAX_DRAIN_ITEMS]

def _strict_goal(payload: Dict[str, Any], level: str) -> Dict[str, Any]:
    metrics_raw = _safe_metrics(payload)
    prev_raw = _safe_previous_metrics(payload)
    current = _round(metrics_raw.get('finrez_pre'))
    prev = _round(prev_raw.get('finrez_pre'))
    delta = None if current is None or prev is None else _round(current - prev)
    if delta is None:
        return {'type': 'unknown', 'value_money': None}
    return {
        'type': 'keep_growth' if delta >= 0 else 'close_gap',
        'value_money': delta,
    }

def _strict_focus(drain_block: List[Dict[str, Any]], next_level: Optional[str]) -> Dict[str, Any]:
    if not drain_block:
        return {}
    total = sum(float(item.get('potential_money') or 0.0) for item in drain_block)
    if total <= 0:
        return {}
    top = drain_block[0]
    share = float(top.get('potential_money') or 0.0) / total
    if share <= 0.30:
        return {}
    return {
        'type': next_level or 'object',
        'object': top.get('object_name'),
        'share': _round(min(share, 1.0), 3),
    }

def _strict_navigation(level: str, drain_block: List[Dict[str, Any]], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {
        'current_level': level,
        'next_level': _children_level_from_payload(drain_payload),
        'items': [item.get('object_name') for item in drain_block[:3]],
        'all': True,
        'reasons': True,
        'back': True,
    }

def _prune_nones(obj):
    if isinstance(obj, dict):
        return {k: _prune_nones(v) for k, v in obj.items() if v is not None}
    if isinstance(obj, list):
        return [_prune_nones(v) for v in obj if v is not None]
    return obj

def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:

    level = payload.get('level')
    drain_block = [] if level == 'sku' else _strict_drain_block(payload, drain_payload)
    return {
        'context': {
            'level': level,
            'object_name': payload.get('object_name'),
            'period': payload.get('period'),
        },
        'metrics': _strict_metrics(payload, level),
        'drain_block': drain_block,
        'goal': _strict_goal(payload, level),
        'focus_block': _strict_focus(drain_block, _children_level_from_payload(drain_payload)),
        'navigation': _strict_navigation(level, drain_block, drain_payload),
    }

def build_comparison_management_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return build_object_view(payload, drain_payload)

def build_list_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    rows = _strict_drain_block(payload, payload)
    return {
        'type': 'list',
        'level': payload.get('level'),
        'object_name': payload.get('object_name'),
        'period': payload.get('period'),
        'items': rows,
        'navigation': {
            'current_level': payload.get('level'),
            'next_level': payload.get('children_level'),
            'items': [item.get('object_name') for item in rows],
            'all': True,
            'reasons': True,
            'back': True,
        },
    }

def build_losses_view_from_children(payload: Dict[str, Any]) -> Dict[str, Any]:
    rows = _strict_drain_block(payload, payload)
    return {
        'type': 'losses',
        'level': payload.get('level'),
        'object_name': payload.get('object_name'),
        'period': payload.get('period'),
        'losses': rows,
        'navigation': {
            'current_level': payload.get('level'),
            'next_level': payload.get('children_level'),
            'items': [item.get('object_name') for item in rows],
            'all': True,
            'reasons': True,
            'back': True,
        },
    }


# ===== FINAL LOCK v2 VIEW OVERRIDES =====

KPI_ORDER = ['revenue', 'markup', 'finrez_pre', 'margin_pre']
REASON_ORDER = ['markup', 'logistics_cost', 'retro_bonus', 'personnel_cost', 'other_costs']
REASON_LABELS_V2 = {
    'markup': 'Наценка',
    'margin_pre': 'Маржа',
    'logistics_cost': 'Логистика',
    'retro_bonus': 'Ретро',
    'personnel_cost': 'Персонал',
    'other_costs': 'Прочие',
}

def _to_num(value: Any) -> Optional[float]:
    try:
        if value is None or value == '':
            return None
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None

def _metric_delta_value(field: str, current_metrics: Dict[str, Any], base_metrics: Dict[str, Any]) -> tuple[Optional[float], Optional[float]]:
    cur = _to_num(current_metrics.get(field))
    base = _to_num(base_metrics.get(field))
    if field in {'markup', 'margin_pre'}:
        return None, _pp_change(cur, base)
    delta_money = None if cur is None or base is None else _round(cur - base)
    delta_percent = _percent_change(cur, base)
    return delta_money, delta_percent

def _build_kpi_block_v2(level: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    current = _safe_metrics(payload)
    previous = _safe_previous_metrics(payload)
    rows = []
    for field in KPI_ORDER:
        base = previous
        delta_money, delta_percent = _metric_delta_value(field, current, base)
        entry = {'field': field, 'label': METRIC_TITLES.get(field, field)}
        if field in {'markup', 'margin_pre'}:
            entry['value_percent'] = _to_num(current.get(field))
            entry['delta_pp'] = delta_percent
        else:
            entry['value_money'] = _to_num(current.get(field))
            entry['delta_money'] = delta_money
            entry['delta_percent'] = delta_percent
        rows.append(entry)
    return rows

def _build_gap_to_business_v2(level: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    if level == 'business':
        return {}
    obj = _safe_metrics(payload)
    biz = _safe_business_metrics(payload)
    effect = _to_num((payload.get('impact') or {}).get('gap_loss_money'))
    if effect is not None:
        effect = -abs(effect) if effect > 0 else abs(effect)
    return {
        'metric': 'margin_pre',
        'label': 'Маржа',
        'fact': {'value_money': _to_num(obj.get('finrez_pre')), 'value_percent': _to_num(obj.get('margin_pre'))},
        'business_percent': _to_num(biz.get('margin_pre')),
        'delta_pp': None if _to_num(obj.get('margin_pre')) is None or _to_num(biz.get('margin_pre')) is None else _round(_to_num(obj.get('margin_pre')) - _to_num(biz.get('margin_pre'))),
        'effect_money': effect,
    }

def _reason_effect(metric: str, payload: Dict[str, Any]) -> Optional[float]:
    per_metric = (payload.get('impact') or {}).get('per_metric_effects') or {}
    value = _to_num(per_metric.get(metric))
    if value is None:
        return None
    if metric in {'markup', 'margin_pre'}:
        # derive from pp gap vs business, sign from business gap
        obj = _safe_metrics(payload)
        biz = _safe_business_metrics(payload)
        revenue = _to_num(obj.get('revenue')) or 0.0
        obj_pct = _to_num(obj.get(metric))
        biz_pct = _to_num(biz.get(metric))
        if obj_pct is None or biz_pct is None or revenue <= 0:
            return None
        raw = ((obj_pct - biz_pct) / 100.0) * revenue
        return _round(raw)
    # costs: lower is better, so invert sign for presentation
    return _round(-value)

def _build_reasons_v2(level: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    if level == 'business':
        return []
    obj = _safe_metrics(payload)
    biz = _safe_business_metrics(payload)
    revenue = _to_num(obj.get('revenue')) or 0.0
    rows = []
    for metric in REASON_ORDER:
        obj_money = _to_num(obj.get(metric))
        obj_percent = None
        if revenue > 0 and obj_money is not None and metric not in {'markup', 'margin_pre'}:
            obj_percent = _round((obj_money / revenue) * 100.0)
        if metric in {'markup', 'margin_pre'}:
            obj_percent = _to_num(obj.get(metric))
            obj_money = _to_num(obj.get('revenue') if metric == 'markup' else obj.get('finrez_pre'))
        biz_percent = None
        if metric in {'markup', 'margin_pre'}:
            biz_percent = _to_num(biz.get(metric))
        else:
            biz_money = _to_num(biz.get(metric))
            biz_revenue = _to_num(biz.get('revenue')) or 0.0
            if biz_money is not None and biz_revenue > 0:
                biz_percent = _round((biz_money / biz_revenue) * 100.0)
        delta_pp = None if obj_percent is None or biz_percent is None else _round(obj_percent - biz_percent)
        effect_money = _reason_effect(metric, payload)
        rows.append({
            'metric': metric,
            'label': REASON_LABELS_V2.get(metric, metric),
            'fact': {'value_money': obj_money, 'value_percent': obj_percent},
            'business_percent': biz_percent,
            'delta_pp': delta_pp,
            'effect_money': effect_money,
        })
    return rows

def _strict_metrics(payload: Dict[str, Any], level: str) -> Dict[str, Any]:
    return {
        'kpi': _build_kpi_block_v2(level, payload),
        'gap_to_business': _build_gap_to_business_v2(level, payload),
        'reasons': _build_reasons_v2(level, payload),
    }

def _strict_drain_block(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    source = drain_payload if drain_payload is not None else payload
    rows = _build_drain_rows(source, max_items=MAX_DRAIN_ITEMS)
    clean = []
    for row in rows:
        name = str(row.get('object_name') or '').strip()
        if not name:
            continue
        effect = _to_num(row.get('potential_money'))
        if effect is None:
            continue
        clean.append({
            'object_name': name,
            'finrez': _to_num(((row.get('fact') or {}).get('finrez'))),
            'margin': _to_num(((row.get('fact') or {}).get('margin'))),
            'delta_to_business_pp': _to_num(row.get('gap_to_business_pp')),
            'effect_money': -abs(effect) if effect > 0 else abs(effect),
        })
    clean.sort(key=lambda r: abs(float(r.get('effect_money') or 0.0)), reverse=True)
    return clean[:MAX_DRAIN_ITEMS]

def _strict_goal(payload: Dict[str, Any], level: str) -> Dict[str, Any]:
    if level == 'business':
        return {}
    effect = _to_num((payload.get('impact') or {}).get('gap_loss_money'))
    if effect is None:
        return {}
    signed = -abs(effect) if effect > 0 else abs(effect)
    return {
        'value_money': signed,
        'action': 'удержать' if signed > 0 else 'закрыть',
    }

def _strict_focus(drain_block: List[Dict[str, Any]], next_level: Optional[str]) -> Dict[str, Any]:
    if not drain_block:
        return {}
    top = drain_block[0]
    return {'type': next_level or 'object', 'object': top.get('object_name')}

def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    level = str(payload.get('level') or '')
    drain_block = [] if level == 'sku' else _strict_drain_block(payload, drain_payload)
    return _prune_nones({
        'context': {
            'level': level,
            'object_name': payload.get('object_name'),
            'period': payload.get('period'),
            **({'title': f"Бизнес / {payload.get('object_name')}"} if level != 'business' and payload.get('object_name') else {'title': 'Бизнес'}),
        },
        'metrics': _strict_metrics(payload, level),
        'drain_block': drain_block,
        'goal': _strict_goal(payload, level),
        'focus_block': _strict_focus(drain_block, _children_level_from_payload(drain_payload)),
        'navigation': _strict_navigation(level, drain_block, drain_payload),
    })


# ===== FINAL LOCK v3 OVERRIDES =====
KPI_ORDER_V3 = ['revenue', 'markup', 'finrez_pre', 'margin_pre']
REASON_ORDER_V3 = ['markup', 'logistics_cost', 'retro_bonus', 'personnel_cost', 'other_costs']
REASON_LABELS_V3 = {
    'markup': 'Наценка',
    'logistics_cost': 'Логистика',
    'retro_bonus': 'Ретро',
    'personnel_cost': 'Персонал',
    'other_costs': 'Прочие',
}


def _v3_effect_from_margin(business_margin: Any, object_margin: Any, revenue: Any) -> Optional[float]:
    bm = _to_num(business_margin)
    om = _to_num(object_margin)
    rev = _to_num(revenue)
    if bm is None or om is None or rev is None:
        return None
    return _round(((om - bm) / 100.0) * rev)


def _v3_metric_percent(metric: str, metrics: Dict[str, Any]) -> Optional[float]:
    revenue = _to_num(metrics.get('revenue')) or 0.0
    if metric == 'markup':
        return _to_num(metrics.get('markup'))
    if metric == 'margin_pre':
        return _to_num(metrics.get('margin_pre'))
    value = _to_num(metrics.get(metric))
    if value is None or revenue <= 0:
        return None
    return _round((value / revenue) * 100.0)


def _v3_metric_money(metric: str, metrics: Dict[str, Any]) -> Optional[float]:
    if metric == 'markup':
        return _to_num(metrics.get('revenue'))
    if metric == 'margin_pre':
        return _to_num(metrics.get('finrez_pre'))
    return _to_num(metrics.get(metric))


def _v3_kpi_entry(field: str, current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    entry = {'field': field, 'label': METRIC_TITLES.get(field, field)}
    cur = _to_num(current.get(field))
    prev = _to_num(previous.get(field))
    if field in {'markup', 'margin_pre'}:
        entry['value_money'] = _to_num(current.get('revenue') if field == 'markup' else current.get('finrez_pre'))
        entry['value_percent'] = cur
        entry['delta_money'] = None
        entry['delta_percent'] = _pp_change(cur, prev)
    else:
        entry['value_money'] = cur
        entry['delta_money'] = None if cur is None or prev is None else _round(cur - prev)
        entry['delta_percent'] = _percent_change(cur, prev)
    return _prune_nones(entry)


def _strict_metrics(payload: Dict[str, Any], level: str) -> Dict[str, Any]:
    current = _safe_metrics(payload)
    previous = _safe_previous_metrics(payload)
    business = _safe_business_metrics(payload)

    kpi = [_v3_kpi_entry(field, current, previous) for field in KPI_ORDER_V3]

    gap_to_business: Dict[str, Any] = {}
    if level != 'business':
        obj_margin = _to_num(current.get('margin_pre'))
        biz_margin = _to_num(business.get('margin_pre'))
        effect = _v3_effect_from_margin(biz_margin, obj_margin, current.get('revenue'))
        gap_to_business = _prune_nones({
            'metric': 'margin_pre',
            'label': 'Маржа',
            'fact': {'value_money': _to_num(current.get('finrez_pre')), 'value_percent': obj_margin},
            'business_percent': biz_margin,
            'delta_percent': None if obj_margin is None or biz_margin is None else _round(obj_margin - biz_margin),
            'effect_money': effect,
        })

    reasons: List[Dict[str, Any]] = []
    if level != 'business':
        per_metric = (payload.get('impact') or {}).get('per_metric_effects') or {}
        for metric in REASON_ORDER_V3:
            obj_pct = _v3_metric_percent(metric, current)
            biz_pct = _v3_metric_percent(metric, business)
            obj_money = _v3_metric_money(metric, current)
            effect_money = None
            if metric == 'markup':
                effect_money = _v3_effect_from_margin(business.get('markup'), current.get('markup'), current.get('revenue'))
            else:
                raw = _to_num(per_metric.get(metric))
                if raw is not None:
                    effect_money = _round(-raw)
                elif obj_pct is not None and biz_pct is not None and _to_num(current.get('revenue')) is not None:
                    sign = 1.0 if metric == 'markup' else -1.0
                    effect_money = _round(((obj_pct - biz_pct) / 100.0) * (_to_num(current.get('revenue')) or 0.0) * sign)
            reasons.append(_prune_nones({
                'metric': metric,
                'label': REASON_LABELS_V3.get(metric, metric),
                'fact': {'value_money': obj_money, 'value_percent': obj_pct},
                'business_percent': biz_pct,
                'delta_percent': None if obj_pct is None or biz_pct is None else _round(obj_pct - biz_pct),
                'effect_money': effect_money,
            }))

    return _prune_nones({'kpi': kpi, 'gap_to_business': gap_to_business, 'reasons': reasons})


def _strict_drain_block(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    source = drain_payload if drain_payload is not None else payload
    raw_items = source.get('all_items') or source.get('items') or []
    clean: List[Dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        name = str(item.get('object_name') or '').strip()
        if not name:
            continue
        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        business = (item.get('metrics') or {}).get('business_metrics') or {}
        margin = _to_num(metrics.get('margin_pre'))
        biz_margin = _to_num(business.get('margin_pre'))
        revenue = _to_num(metrics.get('revenue'))
        effect = _v3_effect_from_margin(biz_margin, margin, revenue)
        if effect is None or effect >= 0:
            continue
        clean.append(_prune_nones({
            'object_name': name,
            'finrez': _to_num(metrics.get('finrez_pre')),
            'margin': margin,
            'delta_to_business_percent': None if margin is None or biz_margin is None else _round(margin - biz_margin),
            'effect_money': effect,
        }))
    clean.sort(key=lambda r: float(r.get('effect_money') or 0.0))
    return clean[:MAX_DRAIN_ITEMS]


def _strict_goal(payload: Dict[str, Any], level: str) -> Dict[str, Any]:
    if level == 'business':
        return {}
    drain = _strict_drain_block(payload, None)
    total = _round(sum(float(item.get('effect_money') or 0.0) for item in drain))
    if total is None:
        return {}
    return {'value_money': total, 'action': 'удержать' if total > 0 else 'закрыть'}


def _strict_focus(drain_block: List[Dict[str, Any]], next_level: Optional[str]) -> Dict[str, Any]:
    if not drain_block:
        return {}
    return {'type': next_level or 'object', 'object': drain_block[0].get('object_name')}


def _strict_navigation(level: str, drain_block: List[Dict[str, Any]], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return {'current_level': level, 'next_level': _children_level_from_payload(drain_payload), 'items': [item.get('object_name') for item in drain_block[:3]], 'all': True, 'reasons': True, 'back': True}


def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    level = str(payload.get('level') or '')
    drain_block = [] if level == 'sku' else _strict_drain_block(payload, drain_payload)
    goal = {} if level == 'business' else {'value_money': _round(sum(float(item.get('effect_money') or 0.0) for item in drain_block)), 'action': 'удержать' if sum(float(item.get('effect_money') or 0.0) for item in drain_block) > 0 else 'закрыть'}
    return _prune_nones({
        'context': {
            'level': level,
            'object_name': payload.get('object_name'),
            'period': payload.get('period'),
            'title': 'Бизнес' if level == 'business' else f"Бизнес / {payload.get('object_name')}"
        },
        'metrics': _strict_metrics(payload, level),
        'drain_block': drain_block,
        'goal': goal,
        'focus_block': _strict_focus(drain_block, _children_level_from_payload(drain_payload)),
        'navigation': _strict_navigation(level, drain_block, drain_payload),
    })


# ===== FINAL ACCEPTANCE OVERRIDES =====
AC_KPI_ORDER = ['revenue', 'markup', 'finrez_pre', 'margin_pre']
AC_KPI_TITLES = {
    'revenue': 'Оборот',
    'markup': 'Наценка',
    'finrez_pre': 'Финрез',
    'margin_pre': 'Маржа',
}
AC_REASON_ORDER = ['markup', 'logistics_cost', 'retro_bonus', 'personnel_cost', 'other_costs']
AC_REASON_TITLES = {
    'markup': 'Наценка',
    'logistics_cost': 'Логистика',
    'retro_bonus': 'Ретро',
    'personnel_cost': 'Персонал',
    'other_costs': 'Прочие',
}


def _ac_num(value: Any) -> Optional[float]:
    return _to_num(value)


def _ac_metric_percent(metric: str, metrics: Dict[str, Any]) -> Optional[float]:
    revenue = _ac_num(metrics.get('revenue')) or 0.0
    if metric in {'markup', 'margin_pre'}:
        return _ac_num(metrics.get(metric))
    value = _ac_num(metrics.get(metric))
    if value is None or revenue <= 0:
        return None
    return _round((value / revenue) * 100.0)


def _ac_effect_money(business_percent: Any, object_percent: Any, revenue: Any, inverse: bool = False) -> Optional[float]:
    bp = _ac_num(business_percent)
    op = _ac_num(object_percent)
    rev = _ac_num(revenue)
    if bp is None or op is None or rev is None:
        return None
    delta = (bp - op) if not inverse else (op - bp)
    return _round((delta / 100.0) * rev)


def _ac_kpi_entry(field: str, current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    cur = _ac_num(current.get(field))
    prev = _ac_num(previous.get(field))
    value_money = _ac_num(current.get('revenue') if field == 'markup' else current.get('finrez_pre') if field == 'margin_pre' else current.get(field))
    value_percent = _ac_metric_percent(field, current)
    if field in {'markup', 'margin_pre'}:
        delta_money = None
        delta_percent = _pp_change(cur, prev)
    else:
        delta_money = None if cur is None or prev is None else _round(cur - prev)
        delta_percent = _percent_change(cur, prev)
    return _prune_nones({
        'title': AC_KPI_TITLES[field],
        'value_money': value_money,
        'value_percent': value_percent,
        'delta_money': delta_money,
        'delta_percent': delta_percent,
    })


def _ac_metrics(payload: Dict[str, Any], level: str) -> Dict[str, Any]:
    current = _safe_metrics(payload)
    previous = _safe_previous_metrics(payload)
    business = _safe_business_metrics(payload)
    kpi = [_ac_kpi_entry(field, current, previous) for field in AC_KPI_ORDER]

    gap_to_business: Dict[str, Any] = {}
    reasons: List[Dict[str, Any]] = []
    if level != 'business':
        obj_margin = _ac_num(current.get('margin_pre'))
        biz_margin = _ac_num(business.get('margin_pre'))
        effect = _ac_effect_money(biz_margin, obj_margin, current.get('revenue'))
        gap_to_business = _prune_nones({
            'title': 'Маржа',
            'fact': {
                'value_money': _ac_num(current.get('finrez_pre')),
                'value_percent': obj_margin,
            },
            'business_percent': biz_margin,
            'delta_money': None if effect is None else _round(-effect),
            'delta_percent': None if obj_margin is None or biz_margin is None else _round(obj_margin - biz_margin),
            'effect_money': effect,
        })
        per_metric = (payload.get('impact') or {}).get('per_metric_effects') or {}
        revenue = _ac_num(current.get('revenue'))
        for metric in AC_REASON_ORDER:
            obj_pct = _ac_metric_percent(metric, current)
            biz_pct = _ac_metric_percent(metric, business)
            obj_money = _ac_num(current.get('revenue') if metric == 'markup' else current.get('finrez_pre') if metric == 'margin_pre' else current.get(metric))
            if metric == 'markup':
                effect_money = _ac_effect_money(biz_pct, obj_pct, revenue)
            else:
                raw = _ac_num(per_metric.get(metric))
                effect_money = _round(-raw) if raw is not None else _ac_effect_money(biz_pct, obj_pct, revenue, inverse=True)
            reasons.append(_prune_nones({
                'title': AC_REASON_TITLES[metric],
                'fact': {'value_money': obj_money, 'value_percent': obj_pct},
                'business_percent': biz_pct,
                'delta_percent': None if obj_pct is None or biz_pct is None else _round(obj_pct - biz_pct),
                'effect_money': effect_money,
            }))
    return _prune_nones({'kpi': kpi, 'gap_to_business': gap_to_business, 'reasons': reasons})


def _ac_drain_block(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    source = drain_payload if drain_payload is not None else payload
    raw_items = source.get('all_items') or source.get('items') or []
    out: List[Dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        name = str(item.get('object_name') or '').strip()
        if not name:
            continue
        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        business = (item.get('metrics') or {}).get('business_metrics') or {}
        prev = item.get('previous_object_metrics') or {}
        revenue = _ac_num(metrics.get('revenue'))
        if revenue is None or revenue <= 0:
            continue
        finrez = _ac_num(metrics.get('finrez_pre'))
        margin = _ac_num(metrics.get('margin_pre'))
        biz_margin = _ac_num(business.get('margin_pre'))
        potential_money = _ac_effect_money(biz_margin, margin, revenue)
        if potential_money is None or potential_money <= 0:
            continue
        prev_finrez = _ac_num(prev.get('finrez_pre'))
        delta_py_money = None if finrez is None or prev_finrez is None else _round(finrez - prev_finrez)
        delta_py_percent = _percent_change(finrez, prev_finrez)
        gap_money = None if biz_margin is None or margin is None or revenue is None else _round(((margin - biz_margin) / 100.0) * revenue)
        out.append(_prune_nones({
            'object_name': name,
            'fact': {'value_money': finrez, 'value_percent': margin},
            'delta_prev_year': {'value_money': delta_py_money, 'value_percent': delta_py_percent},
            'gap_to_business': {'value_money': gap_money, 'value_percent': None if margin is None or biz_margin is None else _round(margin - biz_margin)},
            'effect_money': potential_money,
            'potential_money': potential_money,
        }))
    out.sort(key=lambda row: float(row.get('potential_money') or 0.0), reverse=True)
    return out[:MAX_DRAIN_ITEMS]


def _ac_goal(payload: Dict[str, Any], level: str, drain_block: List[Dict[str, Any]]) -> Dict[str, Any]:
    if level == 'business':
        current = _safe_metrics(payload)
        previous = _safe_previous_metrics(payload)
        current_finrez = _ac_num(current.get('finrez_final'))
        previous_finrez = _ac_num(previous.get('finrez_final'))
        delta = None if current_finrez is None or previous_finrez is None else _round(current_finrez - previous_finrez)
        return _prune_nones({'value_money': delta, 'action': 'удержать' if (delta or 0) > 0 else 'закрыть'})
    total = _round(sum(float(item.get('effect_money') or 0.0) for item in drain_block))
    return _prune_nones({'value_money': total, 'action': 'удержать' if (total or 0) > 0 else 'закрыть'})


def _ac_focus_block(payload: Dict[str, Any], level: str, drain_block: List[Dict[str, Any]]) -> Dict[str, Any]:
    if level == 'business':
        return {}
    return {'reasons': (_ac_metrics(payload, level).get('reasons') or [])}


def _ac_navigation(level: str, drain_block: List[Dict[str, Any]], drain_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        'current_level': level,
        'next_level': _children_level_from_payload(drain_payload),
        'items': [item.get('object_name') for item in drain_block],
    }


def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    level = str(payload.get('level') or '')
    drain_block = [] if level == 'sku' else _ac_drain_block(payload, drain_payload)
    return _prune_nones({
        'context': {
            'level': level,
            'object_name': payload.get('object_name'),
            'period': payload.get('period'),
        },
        'metrics': _ac_metrics(payload, level),
        'drain_block': drain_block,
        'goal': _ac_goal(payload, level, drain_block),
        'focus_block': _ac_focus_block(payload, level, drain_block),
        'navigation': _ac_navigation(level, drain_block, drain_payload),
    })
