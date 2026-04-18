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
    'finrez_pre': 'Финрез до',
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

def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    level = payload.get('level')
    source = drain_payload if drain_payload is not None else payload
    metrics_raw = _safe_metrics(payload)
    prev_raw = _safe_previous_metrics(payload)
    business_raw = _safe_business_metrics(payload)
    context_block = _build_context(payload)
    drain_block = [] if level == 'sku' else _build_drain_rows(source, max_items=MAX_DRAIN_ITEMS)
    focus_block = _build_focus_block(payload)
    decision_block = _build_decision_view(payload)

    metrics: Dict[str, Any] = {}
    metric_fields = BUSINESS_COMPARE_FIELDS if level == 'business' else OBJECT_COMPARE_FIELDS
    for field in metric_fields:
        value = _round(metrics_raw.get(field))
        if level == 'business':
            base_value = _round(prev_raw.get(field))
        else:
            base_value = _round(business_raw.get(field))
        delta_money = None if field in PP_FIELDS else (None if value is None or base_value is None else _round(value - base_value))
        delta_percent = _pp_change(value, base_value) if field in PP_FIELDS else _percent_change(value, base_value)
        entry = {
            'value_money' if field not in PP_FIELDS else 'value_percent': value,
            'delta_money': delta_money,
            'delta_percent': _round(delta_percent),
        }
        if level != 'business':
            py_base = _round(prev_raw.get(field))
            py_delta_money = None if field in PP_FIELDS else (None if value is None or py_base is None else _round(value - py_base))
            py_delta_percent = _pp_change(value, py_base) if field in PP_FIELDS else _percent_change(value, py_base)
            entry['delta_prev_year'] = {'money': py_delta_money, 'percent': _round(py_delta_percent)}
        metrics[field] = entry

    # aliases and special blocks
    goal = context_block.get('goal') or {}
    analysis_block = {'metrics': []}
    if level == 'business':
        for field in ['retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs']:
            cur = _round(metrics_raw.get(field))
            prev = _round(prev_raw.get(field))
            analysis_block['metrics'].append({
                'name': METRIC_TITLES.get(field, field),
                'value_money': cur,
                'delta_money': None if cur is None or prev is None else _round(cur - prev),
                'delta_percent': _percent_change(cur, prev),
            })
    else:
        analysis_block = {}

    navigation = {
        'current_level': level,
        'next_level': _children_level_from_payload(drain_payload),
        'items': [item.get('object_name') for item in drain_block],
        'all': True,
        'reasons': True,
        'back': True,
    }

    legacy_metrics = _build_metrics(level, payload)
    legacy_comparisons = _build_comparisons(level, payload)
    metric_rows = _build_metric_rows(level, legacy_metrics, legacy_comparisons)
    vs_business = _build_vs_business(level, payload)
    impact = payload.get('impact') or {}
    signal = payload.get('signal') or {}
    diagnosis = payload.get('diagnosis') or {}
    action = payload.get('action') or {}
    priority = payload.get('priority')
    pnl_breakdown = _build_metric_rows('business', legacy_metrics, legacy_comparisons) if level == 'business' else None
    summary_lines = _build_summary_lines(level, payload, [item for item in drain_block if item.get('is_negative_for_business')])
    return {
        'context': {
            'path': context_block.get('path'),
            'period': context_block.get('period_label'),
            'level': level,
            'object_name': payload.get('object_name'),
        },
        'goal': goal,
        'metrics': metrics,
        'analysis_block': analysis_block,
        'drain_block': drain_block,
        'focus_block': focus_block,
        'decision_block': decision_block,
        'navigation': navigation,
        # legacy contract fields retained
        'type': 'management',
        'mode': 'management',
        'view_mode': 'drain',
        'level': level,
        'object_name': payload.get('object_name'),
        'period': payload.get('period'),
        'children_level': _children_level_from_payload(drain_payload),
        'slice': _build_slice(payload),
        'header_note': 'к ПГ',
        'metric_rows': metric_rows,
        'comparisons': legacy_comparisons,
        'vs_business': vs_business,
        'drain_items': drain_block,
        'losses': drain_block,
        'positive_items': _build_positive_items(source),
        'commands': _commands(level),
        'warning_flag': _warning_flag(payload) or _warning_flag(source),
        'consistency': payload.get('consistency') or source.get('consistency'),
        'signal': signal,
        'context_block': context_block,
        'diagnosis': diagnosis,
        'summary_lines': summary_lines,
        'impact': impact,
        'priority': priority,
        'action': action,
        'headline': {
            'gap_loss_money': _round(impact.get('gap_loss_money')),
            'gap_loss_money_display': _format_money(impact.get('gap_loss_money')),
            'gap_percent': _round(impact.get('gap_percent')),
            'gap_percent_display': _format_pp(impact.get('gap_percent')),
            'signal': SIGNAL_LABELS.get(str(signal.get('status') or 'ok'), str(signal.get('status') or 'ok')),
            'top_driver': METRIC_LABELS.get(_top_driver(payload), _top_driver(payload)),
            'suggested_action': action.get('suggested_action'),
        },
        'pnl_breakdown': pnl_breakdown,
    }

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

def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if isinstance(payload, dict) and all(key in payload for key in ('context', 'metrics', 'structure', 'drain_block', 'goal', 'navigation')):
        return {
            'context': payload.get('context') or {},
            'metrics': payload.get('metrics') or {},
            'structure': payload.get('structure') or {},
            'drain_block': payload.get('drain_block') or [],
            'goal': payload.get('goal') or {},
            'navigation': payload.get('navigation') or {},
        }

    level = payload.get('level')
    drain_block = [] if level == 'sku' else _strict_drain_block(payload, drain_payload)
    return {
        'context': {
            'level': level,
            'object_name': payload.get('object_name'),
            'period': payload.get('period'),
        },
        'metrics': _strict_metrics(payload, level),
        'structure': {},
        'drain_block': drain_block,
        'goal': _strict_goal(payload, level),
        'navigation': _strict_navigation(level, drain_block, drain_payload),
    }

def build_comparison_management_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    return build_object_view(payload, drain_payload)

def build_list_view(payload: Dict[str, Any], drill_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    rows = []
    if isinstance(drill_payload, dict):
        rows = _strict_drain_block(drill_payload, drill_payload)
    elif isinstance(payload, dict):
        rows = _strict_drain_block(payload, payload)
    return {
        'context': {
            'level': payload.get('level'),
            'object_name': payload.get('object_name'),
            'period': payload.get('period'),
        },
        'metrics': payload.get('metrics') if isinstance(payload.get('metrics'), dict) else {},
        'structure': payload.get('structure') if isinstance(payload.get('structure'), dict) else {},
        'drain_block': rows,
        'goal': payload.get('goal') if isinstance(payload.get('goal'), dict) else {},
        'navigation': {
            'current_level': payload.get('level'),
            'next_level': payload.get('children_level'),
            'items': [item.get('object_name') for item in rows],
            'all': True,
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
