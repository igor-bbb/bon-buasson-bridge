from typing import Any, Dict, List, Optional


ACTION_MAP = {
    'price': 'пересмотреть цену',
    'retro': 'пересогласовать ретро',
    'logistics': 'сократить плечо',
    'personnel': 'оптимизировать нагрузку',
    'other': 'сократить прочие затраты',
}


def _round(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), 2)
    except (TypeError, ValueError):
        return None


def _safe_metrics(payload: Dict[str, Any]) -> Dict[str, Any]:
    return (payload.get('metrics') or {}).get('object_metrics') or {}


def _safe_summary(payload: Dict[str, Any]) -> Dict[str, Any]:
    metrics = _safe_metrics(payload)
    return {
        'object_name': payload.get('object_name'),
        'level': payload.get('level'),
        'period': payload.get('period'),
        'revenue': _round(metrics.get('revenue')),
        'finrez_pre': _round(metrics.get('finrez_pre')),
        'margin_pre': _round(metrics.get('margin_pre')),
        'delta': _round(payload.get('delta')),
        'delta_percent': _round(payload.get('delta_percent')),
    }


def _raw_finrez(item: Dict[str, Any]) -> Optional[float]:
    metrics = (item.get('metrics') or {}).get('object_metrics') or {}
    raw = metrics.get('finrez_pre') if 'metrics' in item else item.get('finrez_pre')
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _drain_item(item: Dict[str, Any], total_drain_abs: float) -> Optional[Dict[str, Any]]:
    metrics = (item.get('metrics') or {}).get('object_metrics') or {}
    finrez_pre_raw = _raw_finrez(item)
    if finrez_pre_raw is None or abs(finrez_pre_raw) <= 1:
        return None

    delta_raw = item.get('delta')
    margin_raw = metrics.get('margin_pre') if 'metrics' in item else item.get('margin_pre')

    share = abs(finrez_pre_raw) / total_drain_abs if total_drain_abs > 0 else 0.0
    if share > 0.30:
        priority = 'HIGH'
    elif share >= 0.10:
        priority = 'MEDIUM'
    else:
        priority = 'LOW'

    return {
        'name': item.get('object_name'),
        'finrez_pre': _round(finrez_pre_raw),
        'delta': _round(delta_raw),
        'margin_pre': _round(margin_raw),
        'share': _round(share),
        'priority': priority,
    }


def _extract_drain_items(payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not payload:
        return []

    items = [item for item in (payload.get('items') or []) if isinstance(item, dict)]
    negative_values: List[float] = []
    for item in items:
        finrez_pre_raw = _raw_finrez(item)
        if finrez_pre_raw is None or abs(finrez_pre_raw) <= 1:
            continue
        if finrez_pre_raw < 0:
            negative_values.append(abs(finrez_pre_raw))

    total_drain_abs = sum(negative_values)
    prepared: List[Dict[str, Any]] = []
    for item in items:
        clean_item = dict(item)
        clean_item.pop('signal', None)
        drain_item = _drain_item(clean_item, total_drain_abs)
        if drain_item is not None:
            prepared.append(drain_item)

    return sorted(prepared, key=lambda x: x.get('finrez_pre', 0))


def _drain_summary(drain: List[Dict[str, Any]]) -> Dict[str, Optional[float]]:
    negatives = [abs(float(item.get('finrez_pre', 0))) for item in drain if (item.get('finrez_pre') or 0) < 0]
    total_drain = round(sum(negatives), 2) if negatives else 0.0
    top_3_concentration = round((sum(sorted(negatives, reverse=True)[:3]) / total_drain), 4) if total_drain else 0.0
    return {
        'total_drain': total_drain,
        'top_3_concentration': top_3_concentration,
    }


def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    summary = _safe_summary(payload)
    if summary.get('level') == 'business':
        summary['object_name'] = 'Бизнес'

    drain_source = drain_payload if drain_payload is not None else payload
    drain = _extract_drain_items(drain_source)
    drain_meta = _drain_summary(drain)

    return {
        'type': 'object',
        'object': {
            'name': summary.get('object_name'),
            'level': summary.get('level'),
            'period': summary.get('period'),
            'revenue': summary.get('revenue'),
            'finrez_pre': summary.get('finrez_pre'),
            'margin_pre': summary.get('margin_pre'),
            'delta': summary.get('delta'),
            'delta_percent': summary.get('delta_percent'),
            'total_drain': drain_meta.get('total_drain'),
            'top_3_concentration': drain_meta.get('top_3_concentration'),
        },
        'drain': drain,
    }


def build_reasons_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    diagnosis = payload.get('diagnosis') or {}
    effects = diagnosis.get('effects_by_metric') or {}

    def pick(*keys: str) -> Optional[float]:
        for key in keys:
            value = effects.get(key, {}).get('effect_value')
            if value is not None:
                return _round(value)
        return None

    raw_decomposition = [
        {'factor': 'price', 'impact_value': pick('price_effect', 'markup')},
        {'factor': 'retro', 'impact_value': pick('retro_effect', 'retro_bonus')},
        {'factor': 'logistics', 'impact_value': pick('logistics_effect', 'logistics_cost')},
        {'factor': 'personnel', 'impact_value': pick('personnel_effect', 'personnel_cost')},
        {'factor': 'other', 'impact_value': pick('other_effect', 'other_costs')},
    ]

    available = [row for row in raw_decomposition if row['impact_value'] is not None]
    if not available:
        return {
            'type': 'reasons',
            'object': payload.get('object_name'),
            'error': 'no reasons data',
            'decomposition': [],
        }

    total_effect = sum(abs(float(row['impact_value'])) for row in available)
    primary_factor = None
    if available:
        primary_factor = max(available, key=lambda row: abs(float(row['impact_value'])))['factor']

    decomposition = []
    for row in available:
        impact_value = float(row['impact_value'])
        impact_percent = abs(impact_value) / total_effect if total_effect else 0.0
        factor = row['factor']
        decomposition.append({
            'factor': factor,
            'impact_value': _round(impact_value),
            'impact_percent': _round(impact_percent),
            'is_primary': factor == primary_factor,
            'action': ACTION_MAP.get(factor),
        })

    decomposition.sort(key=lambda x: abs(float(x.get('impact_value') or 0)), reverse=True)
    return {
        'type': 'reasons',
        'object': payload.get('object_name'),
        'decomposition': decomposition,
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
