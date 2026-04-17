
from typing import Any, Dict, List, Optional

from app.config import DRAIN_MAX_ITEMS, DRAIN_MIN_ITEMS, DRAIN_SHARE_THRESHOLD


PP_FIELDS = {'margin_pre', 'markup', 'gap', 'kpi_gap'}


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


def _format_period(period: Any) -> Any:
    if not isinstance(period, str):
        return period
    return period.replace(':', ' → ')


def _context(payload: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'level': payload.get('level'),
        'object_name': payload.get('object_name'),
        'period': _format_period(payload.get('period')),
    }


def _build_metric_entry(field: str, value: Any, base: Any) -> Dict[str, Any]:
    value_r = _round(value)
    base_r = _round(base)

    if field in PP_FIELDS:
        return {
            'value_percent': value_r,
            'delta_percent': _pp_change(value_r, base_r),
        }

    return {
        'value_money': value_r,
        'delta_money': None if value_r is None or base_r is None else _round(value_r - base_r),
        'delta_percent': _percent_change(value_r, base_r),
    }


def _build_percent_with_money(field: str, value_percent: Any, base_percent: Any, money_value: Any, money_base: Any) -> Dict[str, Any]:
    return {
        'value_percent': _round(value_percent),
        'delta_percent': _pp_change(value_percent, base_percent),
        'value_money': _round(money_value),
        'delta_money': None if _round(money_value) is None or _round(money_base) is None else _round(_round(money_value) - _round(money_base)),
    }


def _build_metrics(level: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    obj = _safe_metrics(payload)
    prev = _safe_previous_metrics(payload)
    biz = _safe_business_metrics(payload)
    base = prev if level == 'business' else biz

    revenue = obj.get('revenue')
    revenue_base = base.get('revenue')
    finrez_pre = obj.get('finrez_pre')
    finrez_pre_base = base.get('finrez_pre')

    metrics: Dict[str, Any] = {
        'revenue': _build_metric_entry('revenue', revenue, revenue_base),
        'margin_pre': _build_percent_with_money('margin_pre', obj.get('margin_pre'), base.get('margin_pre'), finrez_pre, finrez_pre_base),
        'markup': _build_percent_with_money('markup', obj.get('markup'), base.get('markup'), revenue, revenue_base),
        'finrez_pre': _build_metric_entry('finrez_pre', finrez_pre, finrez_pre_base),
        'retro_bonus': _build_metric_entry('retro_bonus', obj.get('retro_bonus'), base.get('retro_bonus')),
        'logistics_cost': _build_metric_entry('logistics_cost', obj.get('logistics_cost'), base.get('logistics_cost')),
        'personnel_cost': _build_metric_entry('personnel_cost', obj.get('personnel_cost'), base.get('personnel_cost')),
        'other_costs': _build_metric_entry('other_costs', obj.get('other_costs'), base.get('other_costs')),
        'gap': {
            'value_percent': _round(obj.get('gap', obj.get('kpi_gap'))),
            'delta_percent': _pp_change(obj.get('gap', obj.get('kpi_gap')), base.get('gap', base.get('kpi_gap'))),
        },
    }

    if level == 'business':
        metrics['finrez_final'] = _build_metric_entry('finrez_final', obj.get('finrez_final'), prev.get('finrez_final'))

    return metrics


def _safe_name(name: Any) -> bool:
    if name is None:
        return False
    txt = str(name).strip().lower()
    return txt not in {'', 'пусто', 'без менеджера', 'none', 'null'}


def _build_drain_block(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    source = drain_payload if isinstance(drain_payload, dict) else payload
    raw_items = source.get('all_items') or source.get('items') or []
    if not isinstance(raw_items, list):
        return []

    out: List[Dict[str, Any]] = []
    for item in raw_items:
        if not isinstance(item, dict):
            continue
        name = item.get('object_name')
        if not _safe_name(name):
            continue

        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        business_metrics = (item.get('metrics') or {}).get('business_metrics') or {}
        previous_metrics = item.get('previous_object_metrics') or {}

        revenue = _round(metrics.get('revenue'))
        if revenue is None or revenue <= 0:
            continue

        margin_object = _round(metrics.get('margin_pre'))
        margin_business = _round(business_metrics.get('margin_pre'))
        if margin_object is None or margin_business is None:
            continue

        potential = _round(((margin_business - margin_object) / 100.0) * revenue)
        if potential is None or potential <= 0:
            continue

        finrez = _round(metrics.get('finrez_pre'))
        prev_finrez = _round(previous_metrics.get('finrez_pre'))
        delta_py_money = None if finrez is None or prev_finrez is None else _round(finrez - prev_finrez)
        delta_py_percent = _percent_change(finrez, prev_finrez)

        row = {
            'object_name': item.get('object_name'),
            'fact': {
                'finrez': finrez,
                'margin': margin_object,
                'revenue': revenue,
            },
            'gap_to_business_pp': _pp_change(margin_object, margin_business),
            'potential_money': potential,
            'potential_explanation': {
                'formula': '(margin_business - margin_object) × revenue',
                'components': {
                    'margin_business': margin_business,
                    'margin_object': margin_object,
                    'revenue': revenue,
                }
            }
        }

        if delta_py_money is not None or delta_py_percent is not None:
            row['delta_prev_year'] = {}
            if delta_py_money is not None:
                row['delta_prev_year']['money'] = delta_py_money
            if delta_py_percent is not None:
                row['delta_prev_year']['percent'] = _round(delta_py_percent)

        out.append(row)

    out.sort(key=lambda x: float(x.get('potential_money') or 0.0), reverse=True)
    if not out:
        return []

    # cover up to threshold, but at least min, at most max
    total = sum(float(x.get('potential_money') or 0.0) for x in out)
    selected: List[Dict[str, Any]] = []
    covered = 0.0
    for row in out:
        selected.append(row)
        covered += float(row.get('potential_money') or 0.0)
        if len(selected) >= DRAIN_MIN_ITEMS and (covered >= total * DRAIN_SHARE_THRESHOLD or len(selected) >= DRAIN_MAX_ITEMS):
            break
    return selected[:DRAIN_MAX_ITEMS]


def _build_goal(level: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    obj = _safe_metrics(payload)
    prev = _safe_previous_metrics(payload)
    current = _round(obj.get('finrez_pre'))
    previous = _round(prev.get('finrez_pre'))
    if current is None or previous is None:
        return {}
    delta = _round(current - previous)
    return {
        'type': 'keep_growth' if delta >= 0 else 'close_gap',
        'value_money': delta,
    }


def _build_focus_block(drain_block: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not drain_block:
        return {}
    total = sum(float(x.get('potential_money') or 0.0) for x in drain_block)
    if total <= 0:
        return {}
    top = drain_block[0]
    share = float(top.get('potential_money') or 0.0) / total
    if share <= 0.30:
        return {}
    return {
        'type': 'manager_top',
        'object': top.get('object_name'),
        'share': _round(min(max(share, 0.0), 1.0), 3),
    }


def _children_level_from_payload(payload: Optional[Dict[str, Any]]) -> Optional[str]:
    if not payload:
        return None
    return payload.get('children_level') or payload.get('level')


def _build_navigation(level: str, drain_payload: Optional[Dict[str, Any]], drain_block: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        'current_level': level,
        'next_level': _children_level_from_payload(drain_payload),
        'items': [item.get('object_name') for item in drain_block[:3]],
        'all': True,
        'reasons': True,
        'back': True,
    }


def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    level = str(payload.get('level') or '')
    metrics = _build_metrics(level, payload)
    drain_block = [] if level == 'sku' else _build_drain_block(payload, drain_payload)
    goal = _build_goal(level, payload)
    focus_block = _build_focus_block(drain_block)
    navigation = _build_navigation(level, drain_payload, drain_block)
    return {
        'context': _context(payload),
        'metrics': metrics,
        'drain_block': drain_block,
        'goal': goal,
        'focus_block': focus_block,
        'navigation': navigation,
    }


def build_reasons_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    obj = _safe_metrics(payload)
    biz = _safe_business_metrics(payload)
    prev = _safe_previous_metrics(payload)
    level = str(payload.get('level') or '')
    base = prev if level == 'business' else biz

    rows: List[Dict[str, Any]] = []
    for field, label in [
        ('retro_bonus', 'Ретробонус'),
        ('logistics_cost', 'Логистика'),
        ('personnel_cost', 'Персонал'),
        ('other_costs', 'Прочее'),
    ]:
        cur = _round(obj.get(field))
        base_val = _round(base.get(field))
        delta_money = None if cur is None or base_val is None else _round(cur - base_val)
        rows.append({
            'factor': field,
            'factor_label': label,
            'value_money': cur,
            'base_money': base_val,
            'delta_money': delta_money,
            'delta_percent': _percent_change(cur, base_val),
        })

    rows.sort(key=lambda r: abs(float(r.get('delta_money') or 0.0)), reverse=True)

    return {
        'context': _context(payload),
        'reasons': rows,
        'navigation': {'back': True},
    }
