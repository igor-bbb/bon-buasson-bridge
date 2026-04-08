from typing import Any, Dict, List, Optional


ACTION_MAP = {
    'price': 'пересмотреть цену',
    'retro': 'пересогласовать ретро',
    'logistics': 'сократить плечо',
    'personnel': 'оптимизировать нагрузку',
    'other': 'сократить прочие затраты',
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
    if current_num is None or previous_num is None:
        return None
    if abs(previous_num) < 1e-9:
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
        if field in PP_FIELDS:
            compare[field] = _pp_change(current, previous)
        else:
            compare[field] = _percent_change(current, previous)
    return compare



def _raw_finrez(item: Dict[str, Any]) -> Optional[float]:
    metrics = (item.get('metrics') or {}).get('object_metrics') or {}
    raw = metrics.get('finrez_pre') if 'metrics' in item else item.get('finrez_pre')
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None



def _drill_name(level: Optional[str]) -> Optional[str]:
    mapping = {
        'manager_top': 'топ-менеджерам',
        'manager': 'менеджерам',
        'network': 'сетям',
        'category': 'категориям',
        'tmc_group': 'группам ТМЦ',
        'sku': 'SKU',
    }
    return mapping.get(level)



def _prepare_list_rows(items: List[Dict[str, Any]], include_positive: bool = True) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for item in items:
        finrez_pre = _raw_finrez(item)
        if finrez_pre is None:
            continue
        if not include_positive and finrez_pre >= 0:
            continue
        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        rows.append(
            {
                'name': item.get('object_name'),
                'finrez_pre': _round(finrez_pre),
                'margin_pre': _round(metrics.get('margin_pre')),
                'revenue': _round(metrics.get('revenue')),
            }
        )
    return sorted(rows, key=lambda x: x.get('finrez_pre', 0))



def _drain_rows(payload: Dict[str, Any], include_positive: bool = False) -> List[Dict[str, Any]]:
    items = [item for item in (payload.get('items') or []) if isinstance(item, dict)]
    prepared = _prepare_list_rows(items, include_positive=include_positive)
    negative_total = sum(abs(float(row['finrez_pre'])) for row in prepared if (row.get('finrez_pre') or 0) < 0)
    rows: List[Dict[str, Any]] = []
    for row in prepared:
        finrez = float(row.get('finrez_pre') or 0)
        if not include_positive and finrez >= 0:
            continue
        share = abs(finrez) / negative_total if negative_total and finrez < 0 else 0.0
        rows.append(
            {
                'name': row.get('name'),
                'finrez_pre': row.get('finrez_pre'),
                'share': _round(share),
                'margin_pre': row.get('margin_pre'),
                'revenue': row.get('revenue'),
            }
        )
    return rows



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



def _business_object(payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = _safe_metrics(payload)
    previous = _safe_previous_metrics(payload)
    compare = _metric_compare_map(metrics, previous, BUSINESS_COMPARE_FIELDS)
    return {
        'name': 'Бизнес',
        'level': 'business',
        'period': payload.get('period'),
        'revenue': _round(metrics.get('revenue')),
        'retro_bonus': _round(metrics.get('retro_bonus')),
        'logistics_cost': _round(metrics.get('logistics_cost')),
        'personnel_cost': _round(metrics.get('personnel_cost')),
        'other_costs': _round(metrics.get('other_costs')),
        'margin_pre': _round(metrics.get('margin_pre')),
        'markup': _round(metrics.get('markup')),
        'finrez_pre': _round(metrics.get('finrez_pre')),
        'finrez_final': _round(metrics.get('finrez_final')),
        'compare_previous_year': compare,
    }



def _object_object(payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = _safe_metrics(payload)
    previous = _safe_previous_metrics(payload)
    business_metrics = _safe_business_metrics(payload)
    compare = _metric_compare_map(metrics, previous, OBJECT_COMPARE_FIELDS)
    return {
        'name': payload.get('object_name'),
        'level': payload.get('level'),
        'period': payload.get('period'),
        'revenue': _round(metrics.get('revenue')),
        'margin_pre': _round(metrics.get('margin_pre')),
        'markup': _round(metrics.get('markup')),
        'finrez_pre': _round(metrics.get('finrez_pre')),
        'compare_previous_year': compare,
        'business_margin_pre': _round(business_metrics.get('margin_pre')),
        'gap_to_business_pp': _gap_margin_pp(payload),
        'gap_to_business_value': _gap_value(payload),
    }



def _list_view(scope_payload: Dict[str, Any], list_payload: Dict[str, Any]) -> Dict[str, Any]:
    scope_object = _business_object(scope_payload) if scope_payload.get('level') == 'business' else _object_object(scope_payload)
    target_level = list_payload.get('level')
    return {
        'type': 'object',
        'object': scope_object,
        'drain': [],
        'list': {
            'level': target_level,
            'title': _drill_name(target_level),
            'items': _prepare_list_rows(list_payload.get('items') or [], include_positive=True),
        },
    }



def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    level = payload.get('level')
    object_block = _business_object(payload) if level == 'business' else _object_object(payload)

    if level == 'sku':
        return {
            'type': 'object',
            'object': object_block,
            'drain': [],
        }

    source = drain_payload if drain_payload is not None else payload
    drain = _drain_rows(source, include_positive=False)
    return {
        'type': 'object',
        'object': object_block,
        'drain': drain,
    }



def build_reasons_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    diagnosis = payload.get('diagnosis') or {}
    effects = diagnosis.get('effects_by_metric') or {}
    object_metrics = _safe_metrics(payload)
    revenue = float(object_metrics.get('revenue') or 0.0)

    def pick_effect(*keys: str) -> Optional[float]:
        for key in keys:
            item = effects.get(key) or {}
            value = item.get('effect_value')
            if value is not None and item.get('is_negative_for_business', True):
                return float(value)
        return None

    raw = [
        ('retro', pick_effect('retro_bonus', 'retro_effect')),
        ('logistics', pick_effect('logistics_cost', 'logistics_effect')),
        ('personnel', pick_effect('personnel_cost', 'personnel_effect')),
        ('other', pick_effect('other_costs', 'other_effect')),
        ('price', pick_effect('markup', 'price_effect')),
    ]
    available = [(factor, value) for factor, value in raw if value is not None and abs(value) > 1e-9]
    if not available:
        return {
            'type': 'reasons',
            'object': {'name': payload.get('object_name')},
            'error': 'no reasons data',
            'reasons': [],
        }

    total_loss = sum(abs(value) for _, value in available)
    primary = max(available, key=lambda item: abs(item[1]))[0]
    reasons: List[Dict[str, Any]] = []
    for factor, value in sorted(available, key=lambda item: abs(item[1]), reverse=True):
        share_of_loss = abs(value) / total_loss if total_loss else 0.0
        impact_pp = (float(value) / revenue) * 100.0 if revenue else None
        reasons.append(
            {
                'factor': factor,
                'impact_value': _round(value),
                'impact_share': _round(share_of_loss),
                'impact_pp_to_business': _round(impact_pp),
                'is_primary': factor == primary,
                'action': ACTION_MAP.get(factor),
            }
        )

    return {
        'type': 'reasons',
        'object': {
            'name': payload.get('object_name'),
            'business_margin_pre': _round(_safe_business_metrics(payload).get('margin_pre')),
            'margin_pre': _round(object_metrics.get('margin_pre')),
            'gap_to_business_pp': _gap_margin_pp(payload),
            'gap_to_business_value': _gap_value(payload),
        },
        'reasons': reasons,
    }


# compatibility wrappers

def build_management_view(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_object_view(comparison_payload)


def build_drilldown_management_view(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_object_view(drilldown_payload)


def build_losses_view_from_summary(comparison_payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_object_view(comparison_payload)


def build_losses_view_from_children(drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_object_view(drilldown_payload)


def build_comparison_management_view(query: Dict[str, Any], current: Dict[str, Any], previous: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'type': 'comparison',
        'period_current': query.get('period_current'),
        'period_previous': query.get('period_previous'),
        'current': build_object_view(current),
        'previous': build_object_view(previous),
    }


def build_signal_flow_view(summary_payload: Dict[str, Any], drilldown_payload: Dict[str, Any]) -> Dict[str, Any]:
    return build_object_view(summary_payload, drilldown_payload)


def build_list_view(scope_payload: Dict[str, Any], list_payload: Dict[str, Any]) -> Dict[str, Any]:
    return _list_view(scope_payload, list_payload)
