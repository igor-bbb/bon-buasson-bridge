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
            potential_money = 0.0
        item_business_margin = _round(business_metrics.get('margin_pre'))
        gap_to_business_pp = None if margin is None or item_business_margin is None else _round(margin - item_business_margin)
        prev_finrez = _round(previous_metrics.get('finrez_pre'))
        delta_py_money = None if finrez is None or prev_finrez is None else _round(finrez - prev_finrez)
        delta_py_percent = _percent_change(finrez, prev_finrez)
        delta_prev_year = {}
        if delta_py_money is not None:
            delta_prev_year['money'] = delta_py_money
        if delta_py_percent is not None:
            delta_prev_year['percent'] = _round(delta_py_percent)

        row = {
            'object_name': item.get('object_name'),
            'fact': {
                'finrez': finrez,
                'margin': margin,
                'revenue': revenue,
            },
            'gap_to_business_pp': gap_to_business_pp,
            'potential_money': potential_money,
            'potential_explanation': {
                'formula': '(margin_business - margin_object) × revenue',
                'components': {
                    'margin_business': item_business_margin,
                    'margin_object': margin,
                    'revenue': revenue,
                }
            },
        }
        if delta_prev_year:
            row['delta_prev_year'] = delta_prev_year
        prepared.append(row)

    prepared = [row for row in prepared if (row.get('fact') or {}).get('revenue') not in (None, 0) and (row.get('potential_money') or 0) > 0]
    prepared.sort(key=lambda x: float(x.get('potential_money') or 0.0), reverse=True)
    if not prepared:
        return []
    negatives = prepared[:max_items]
    return negatives



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
    focus_block = _build_focus_block(payload) or {}

    metrics: Dict[str, Any] = {}
    metric_fields = BUSINESS_COMPARE_FIELDS if level == 'business' else [
        'revenue', 'margin_pre', 'markup', 'finrez_pre', 'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs', 'kpi_gap'
    ]
    for field in metric_fields:
        value = _round(metrics_raw.get(field))
        if level == 'business':
            base_value = _round(prev_raw.get(field))
        else:
            base_value = _round(business_raw.get(field))

        # percent metrics still carry money context where possible
        if field in PP_FIELDS:
            value_money = _round(metrics_raw.get('finrez_pre')) if field == 'margin_pre' else (_round(metrics_raw.get('revenue')) if field == 'markup' else None)
            base_money = _round(prev_raw.get('finrez_pre')) if level == 'business' and field == 'margin_pre' else (
                _round(prev_raw.get('revenue')) if level == 'business' and field == 'markup' else (
                    _round(business_raw.get('finrez_pre')) if field == 'margin_pre' else (_round(business_raw.get('revenue')) if field == 'markup' else None)
                )
            )
            delta_money = None if value_money is None or base_money is None else _round(value_money - base_money)
            entry = {
                'value_percent': value,
                'delta_percent': _round(_pp_change(value, base_value)),
            }
            if field != 'kpi_gap':
                entry['value_money'] = value_money
                entry['delta_money'] = delta_money
        else:
            delta_money = None if value is None or base_value is None else _round(value - base_value)
            entry = {
                'value_money': value,
                'delta_money': delta_money,
                'delta_percent': _round(_percent_change(value, base_value)),
            }

        if level != 'business':
            py_base = _round(prev_raw.get(field))
            py_delta_money = None if field in PP_FIELDS else (None if value is None or py_base is None else _round(value - py_base))
            py_delta_percent = _pp_change(value, py_base) if field in PP_FIELDS else _percent_change(value, py_base)
            delta_prev = {}
            if py_delta_money is not None:
                delta_prev['money'] = py_delta_money
            if py_delta_percent is not None:
                delta_prev['percent'] = _round(py_delta_percent)
            if delta_prev:
                entry['delta_prev_year'] = delta_prev

        metrics['gap' if field == 'kpi_gap' else field] = entry

    navigation = {
        'current_level': level,
        'next_level': _children_level_from_payload(drain_payload),
        'items': [item.get('object_name') for item in drain_block],
        'all': True,
        'reasons': True,
        'back': True,
    }

    return {
        'context': {
            'path': context_block.get('path'),
            'period': context_block.get('period_label'),
            'level': level,
            'object_name': payload.get('object_name'),
        },
        'metrics': metrics,
        'drain_block': drain_block,
        'goal': context_block.get('goal') or {},
        'focus_block': focus_block,
        'navigation': navigation,
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
