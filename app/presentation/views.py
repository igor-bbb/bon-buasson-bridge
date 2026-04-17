
from typing import Any, Dict, List, Optional

from app.domain.decision import build_decision_block

MAX_DRAIN_ITEMS = 7
MIN_DRAIN_ITEMS = 3
FOCUS_THRESHOLD = 0.30

PERCENT_FIELDS = {'margin_pre', 'markup', 'gap'}
BUSINESS_METRICS = [
    'revenue', 'margin_pre', 'markup', 'finrez_pre', 'finrez_final',
    'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs', 'gap'
]
OBJECT_METRICS = ['revenue', 'margin_pre', 'markup', 'finrez_pre', 'gap']


def _num(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return round(float(v), 2)
    except Exception:
        return None


def _pct_change(current: Optional[float], base: Optional[float]) -> Optional[float]:
    if current is None or base is None:
        return None
    if abs(base) < 1e-9:
        return 0.0 if abs(current) < 1e-9 else None
    return round(((current - base) / abs(base)) * 100.0, 2)


def _pp_change(current: Optional[float], base: Optional[float]) -> Optional[float]:
    if current is None or base is None:
        return None
    return round(current - base, 2)


def _safe_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    return (payload.get('metrics') or {}).get('object_metrics') or {}


def _safe_previous_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    return payload.get('previous_object_metrics') or {}


def _safe_business_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    return (payload.get('metrics') or {}).get('business_metrics') or {}


def _context(payload: Dict[str, Any]) -> Dict[str, Any]:
    filter_payload = payload.get('filter') or {}
    parts = ['business']
    for key in ['manager_top', 'manager', 'network', 'sku']:
        val = filter_payload.get(key)
        if val:
            parts.append(str(val))
    obj = payload.get('object_name')
    return {
        'level': payload.get('level'),
        'object_name': obj,
        'path': ' / '.join(parts if str(obj).lower() == 'business' else parts + ([str(obj)] if obj else [])),
        'period': payload.get('period'),
    }


def _goal(payload: Dict[str, Any]) -> Dict[str, Any]:
    level = payload.get('level')
    cur = _safe_metrics(payload)
    prev = _safe_previous_metrics(payload)
    if level == 'business':
        cur_val = _num(cur.get('finrez_final'))
        base = _num(prev.get('finrez_final'))
    else:
        cur_val = _num(cur.get('finrez_pre'))
        base = _num(_safe_business_metrics(payload).get('finrez_pre'))
    if cur_val is None or base is None:
        return {'type': 'unknown', 'value_money': None}
    delta = round(cur_val - base, 2)
    return {'type': 'keep_growth' if delta >= 0 else 'close_gap', 'value_money': delta}


def _metric_entry(field: str, value: Optional[float], base: Optional[float], money_value: Optional[float]=None, money_base: Optional[float]=None, prev_year: Optional[Dict[str, Any]]=None) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if field in PERCENT_FIELDS:
        out['value_percent'] = _num(value)
        out['delta_percent'] = _pp_change(_num(value), _num(base))
        out['value_money'] = _num(money_value)
        out['delta_money'] = None if money_value is None or money_base is None else round(float(money_value) - float(money_base), 2)
    else:
        out['value_money'] = _num(value)
        out['delta_money'] = None if value is None or base is None else round(float(value) - float(base), 2)
        out['delta_percent'] = _pct_change(_num(value), _num(base))
    if prev_year is not None:
        out['delta_prev_year'] = prev_year
    return out


def _build_metrics_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    level = payload.get('level')
    cur = _safe_metrics(payload)
    prev = _safe_previous_metrics(payload)
    biz = _safe_business_metrics(payload)
    use_fields = BUSINESS_METRICS if level == 'business' else OBJECT_METRICS
    result: Dict[str, Any] = {}
    for field in use_fields:
        if level == 'business':
            base_metrics = prev
        else:
            base_metrics = biz
        value = cur.get(field)
        base = base_metrics.get(field)
        money_value = None
        money_base = None
        if field == 'margin_pre':
            money_value = cur.get('finrez_pre')
            money_base = base_metrics.get('finrez_pre')
        elif field == 'markup':
            money_value = cur.get('gross_profit', cur.get('revenue'))
            money_base = base_metrics.get('gross_profit', base_metrics.get('revenue'))
        elif field == 'gap':
            money_value = (payload.get('impact') or {}).get('gap_loss_money')
            money_base = 0.0
        py_ref = None
        if level != 'business':
            py_base = prev.get(field)
            py_money = None if field in PERCENT_FIELDS else (None if value is None or py_base is None else round(float(value) - float(py_base), 2))
            py_pct = _pp_change(_num(value), _num(py_base)) if field in PERCENT_FIELDS else _pct_change(_num(value), _num(py_base))
            py_ref = {'money': py_money, 'percent': py_pct}
        result[field] = _metric_entry(field, value, base, money_value, money_base, py_ref)
    return result


def _drain_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    source_items = payload.get('all_items') or payload.get('items') or []
    rows: List[Dict[str, Any]] = []
    for item in source_items:
        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        prev = item.get('previous_object_metrics') or {}
        biz = (item.get('metrics') or {}).get('business_metrics') or {}
        impact = item.get('impact') or {}
        pot = _num(impact.get('gap_loss_money'))
        if pot is None:
            continue
        margin = _num(metrics.get('margin_pre'))
        bm = _num(biz.get('margin_pre'))
        rev = _num(metrics.get('revenue'))
        fin = _num(metrics.get('finrez_pre'))
        pfin = _num(prev.get('finrez_pre'))
        rows.append({
            'object_name': item.get('object_name'),
            'fact': {
                'finrez': fin,
                'margin': margin,
                'revenue': rev,
            },
            'delta_prev_year': {
                'money': None if fin is None or pfin is None else round(fin - pfin, 2),
                'percent': _pct_change(fin, pfin),
            },
            'gap_to_business_pp': None if margin is None or bm is None else round(margin - bm, 2),
            'potential_money': pot,
            'potential_explanation': {
                'formula': '(margin_business - margin_object) × revenue',
                'components': {
                    'margin_business': bm,
                    'margin_object': margin,
                    'revenue': rev,
                },
            },
        })
    rows.sort(key=lambda x: float(x.get('potential_money') or 0.0), reverse=True)
    return rows[:MAX_DRAIN_ITEMS]


def _focus_block(drain_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not drain_rows:
        return {}
    total = sum(float(r.get('potential_money') or 0.0) for r in drain_rows)
    if total <= 0:
        return {}
    top = drain_rows[0]
    share = float(top.get('potential_money') or 0.0) / total
    if share <= FOCUS_THRESHOLD:
        return {}
    return {'type': 'object', 'object': top.get('object_name'), 'share': round(share, 3)}


def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    level = payload.get('level')
    source = drain_payload if drain_payload is not None else payload
    drain_block = [] if level == 'sku' else _drain_rows(source)
    goal = _goal(payload)
    decision_block = None if level == 'business' else (build_decision_block(payload) if level in {'network', 'sku'} else None)
    navigation = {
        'current_level': level,
        'next_level': (drain_payload or {}).get('children_level') or payload.get('children_level'),
        'items': [r.get('object_name') for r in drain_block],
        'all': True,
        'reasons': True,
        'back': True,
    }
    return {
        'context': _context(payload),
        'metrics': _build_metrics_block(payload),
        'drain_block': drain_block,
        'goal': goal,
        'focus_block': _focus_block(drain_block),
        'navigation': navigation,
        **({'decision_block': decision_block} if decision_block is not None else {}),
    }


def build_reasons_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    diagnosis = payload.get('diagnosis') or {}
    effects = diagnosis.get('effects_by_metric') or {}
    out = []
    for key in ['logistics_cost','personnel_cost','retro_bonus','other_costs']:
        val = effects.get(key) or {}
        out.append({'name': key, 'effect_value': val.get('effect_value') if isinstance(val, dict) else val})
    return {'context': _context(payload), 'reasons': out}


def build_list_view(scope_payload: Dict[str, Any], list_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = _drain_rows(list_payload)
    return {
        'context': _context(scope_payload),
        'metrics': _build_metrics_block(scope_payload),
        'drain_block': items,
        'goal': _goal(scope_payload),
        'focus_block': _focus_block(items),
        'navigation': {
            'current_level': scope_payload.get('level'),
            'next_level': list_payload.get('children_level') or list_payload.get('level'),
            'items': [r.get('object_name') for r in items],
            'all': True,
            'reasons': True,
            'back': True,
        },
    }


def build_comparison_management_view(query: Dict[str, Any], current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    return {'current': build_object_view(current), 'previous': build_object_view(previous), 'context': _context(current)}


def build_losses_view_from_children(scope_payload: Dict[str, Any], losses_payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_list_view(scope_payload, losses_payload)


# legacy helpers/compatibility

def _legacy_comparisons(level: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    cur = _safe_metrics(payload)
    prev = _safe_previous_metrics(payload)
    biz = _safe_business_metrics(payload)
    out = {}
    fields = BUSINESS_METRICS if level == 'business' else OBJECT_METRICS
    for field in fields:
        base = prev if level == 'business' else biz
        val = cur.get(field)
        b = base.get(field)
        out[field] = _pp_change(_num(val), _num(b)) if field in PERCENT_FIELDS else _pct_change(_num(val), _num(b))
    return out


def _legacy_metric_rows(level: str, payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    metrics = _build_metrics_block(payload)
    rows = []
    order = BUSINESS_METRICS if level == 'business' else OBJECT_METRICS
    for field in order:
        entry = metrics.get(field) or {}
        rows.append({'field': field, 'label': field, 'value': entry.get('value_percent', entry.get('value_money')), 'yoy': entry.get('delta_percent')})
    return rows

_old_build_object_view = build_object_view

def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    data = _old_build_object_view(payload, drain_payload)
    level = payload.get('level')
    data.update({
        'type': 'management',
        'mode': 'management',
        'view_mode': 'drain',
        'level': level,
        'object_name': payload.get('object_name'),
        'period': payload.get('period'),
        'children_level': (drain_payload or {}).get('children_level') or payload.get('children_level'),
        'comparisons': _legacy_comparisons(level, payload),
        'metric_rows': _legacy_metric_rows(level, payload),
        'drain_items': data.get('drain_block', []),
        'commands': ['причины','все','1','2','3'] if level != 'sku' else ['причины','назад'],
        'signal': payload.get('signal', {}),
    })
    return data


def build_reasons_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    diagnosis = payload.get('diagnosis') or {}
    effects = diagnosis.get('effects_by_metric') or {}
    reasons = []
    for key in ['retro_bonus','logistics_cost','personnel_cost','other_costs','finrez_pre']:
        raw = effects.get(key)
        val = raw.get('effect_value') if isinstance(raw, dict) else raw
        reasons.append({
            'factor': key,
            'fact_value': None,
            'fact_percent': None,
            'business_percent': None,
            'gap_pp': None,
            'impact_value': val,
            'impact_share': None,
            'action': None,
            'lines': []
        })
    return {
        'type': 'reasons',
        'level': payload.get('level'),
        'object_name': payload.get('object_name'),
        'summary': {},
        'reasons': reasons,
    }


def build_losses_view_from_summary(summary_payload: Dict[str, Any]) -> Dict[str, Any]:
    drain = _drain_rows(summary_payload)
    losses = []
    for row in drain:
        copy = dict(row)
        copy['is_negative_for_business'] = float(copy.get('potential_money') or 0.0) > 0.0
        losses.append(copy)
    return {'type': 'losses', 'level': summary_payload.get('level'), 'losses': [r for r in losses if r.get('is_negative_for_business')]}

# backward compatible overloaded version
_old_build_losses_view_from_children = build_losses_view_from_children

def build_losses_view_from_children(scope_payload: Dict[str, Any], losses_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    if losses_payload is None:
        items = _drain_rows(scope_payload)
        return {'type': 'losses', 'level': scope_payload.get('level'), 'losses': items}
    return _old_build_losses_view_from_children(scope_payload, losses_payload)
