from typing import Any, Dict, List, Optional

from app.domain.metrics import build_solutions_from_effects


METRIC_LABELS_RU = {
    'revenue': 'Оборот',
    'finrez_pre': 'Финрез',
    'finrez_final': 'Финрез итог',
    'margin_pre': 'Маржа',
    'markup': 'Наценка',
    'retro_bonus': 'Ретробонус',
    'logistics_cost': 'Логистика',
    'personnel_cost': 'Персонал',
    'other_costs': 'Прочие',
}

REASON_LABELS_RU = {
    'retro_bonus': 'Ретробонус',
    'logistics_cost': 'Логистика',
    'personnel_cost': 'Персонал',
    'other_costs': 'Прочие',
    'markup': 'Наценка',
    'margin': 'Маржа',
}


def _to_float(x: Any) -> float:
    try:
        return float(x or 0.0)
    except Exception:
        return 0.0


def _format_money(x: Any) -> str:
    value = _to_float(x)
    sign = '-' if value < 0 else ''
    return f"{sign}{abs(value):,.0f}".replace(',', ' ')


def _format_percent(x: Any) -> str:
    value = _to_float(x)
    return f"{value:.2f}%"


def _format_pp(x: Any) -> str:
    value = _to_float(x)
    return f"{value:.2f} п.п."


def _format_yoy_money(current: Any, previous: Any) -> Optional[str]:
    current_value = _to_float(current)
    previous_value = _to_float(previous)
    if abs(previous_value) < 1e-9:
        return None
    delta = current_value - previous_value
    sign = '+' if delta >= 0 else ''
    return f"{sign}{_format_money(delta)} к прошлому году"


def _format_yoy_percent(current: Any, previous: Any) -> Optional[str]:
    current_value = _to_float(current)
    previous_value = _to_float(previous)
    if abs(previous_value) < 1e-9:
        return None
    delta = current_value - previous_value
    sign = '+' if delta >= 0 else ''
    return f"{sign}{_format_pp(delta)} к прошлому году"


def _reason_label(metric: str) -> str:
    return REASON_LABELS_RU.get(metric, metric)


def _base_anchor_rows(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    metrics = (payload.get('metrics') or {}).get('object_metrics') or {}
    previous = payload.get('previous_object_metrics') or {}
    level = payload.get('level')

    rows: List[Dict[str, Any]] = [
        {
            'label': 'Оборот',
            'value': _format_money(metrics.get('revenue')),
            'yoy': _format_yoy_money(metrics.get('revenue'), previous.get('revenue')),
        },
        {
            'label': 'Финрез',
            'value': _format_money(metrics.get('finrez_pre')),
            'yoy': _format_yoy_money(metrics.get('finrez_pre'), previous.get('finrez_pre')),
        },
        {
            'label': 'Маржа',
            'value': _format_percent(metrics.get('margin_pre')),
            'yoy': _format_yoy_percent(metrics.get('margin_pre'), previous.get('margin_pre')),
        },
    ]

    if level == 'business':
        rows.extend([
            {
                'label': 'Наценка',
                'value': _format_percent(metrics.get('markup')),
                'yoy': _format_yoy_percent(metrics.get('markup'), previous.get('markup')),
            },
            {
                'label': 'Финрез итог',
                'value': _format_money(metrics.get('finrez_final')),
                'yoy': _format_yoy_money(metrics.get('finrez_final'), previous.get('finrez_final')),
            },
            {
                'label': 'Ретробонус',
                'value': _format_money(metrics.get('retro_bonus')),
                'yoy': _format_yoy_money(metrics.get('retro_bonus'), previous.get('retro_bonus')),
            },
            {
                'label': 'Логистика',
                'value': _format_money(metrics.get('logistics_cost')),
                'yoy': _format_yoy_money(metrics.get('logistics_cost'), previous.get('logistics_cost')),
            },
            {
                'label': 'Персонал',
                'value': _format_money(metrics.get('personnel_cost')),
                'yoy': _format_yoy_money(metrics.get('personnel_cost'), previous.get('personnel_cost')),
            },
            {
                'label': 'Прочие',
                'value': _format_money(metrics.get('other_costs')),
                'yoy': _format_yoy_money(metrics.get('other_costs'), previous.get('other_costs')),
            },
        ])

    return rows


def _vector_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    impact = payload.get('impact') or {}
    gap_money = _to_float(impact.get('gap_loss_money'))
    gap_percent = _to_float(impact.get('gap_percent'))

    return {
        'money': _format_money(gap_money),
        'delta_to_business': _format_pp(gap_percent),
        'line': f"Недозаработано: {_format_money(gap_money)} ({_format_pp(gap_percent)} к бизнесу)",
    }


def _drain_items(drain_payload: Optional[Dict[str, Any]]) -> List[Dict[str, Any]]:
    if not isinstance(drain_payload, dict):
        return []

    items = drain_payload.get('items') or []
    out: List[Dict[str, Any]] = []

    for idx, item in enumerate(items[:3], start=1):
        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        impact = item.get('impact') or {}

        line = (
            f"{item.get('object_name')}\n"
            f"Финрез: {_format_money(metrics.get('finrez_pre'))} | "
            f"Маржа: {_format_percent(metrics.get('margin_pre'))}\n"
            f"→ {_format_money(impact.get('gap_loss_money'))}"
        )

        out.append({
            'index': idx,
            'object_name': item.get('object_name'),
            'line': line,
            'gap_loss_money': _format_money(impact.get('gap_loss_money')),
            'margin': _format_percent(metrics.get('margin_pre')),
        })

    return out


def _reason_items(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    impact = payload.get('impact') or {}
    context = payload.get('context') or {}
    object_metrics = (payload.get('metrics') or {}).get('object_metrics') or {}
    business_metrics = (payload.get('metrics') or {}).get('business_metrics') or {}

    reasons: List[Dict[str, Any]] = []

    # Наценка как сквозная причина
    gap_money = _to_float(impact.get('gap_loss_money'))
    margin_gap = _to_float(impact.get('gap_percent'))
    if gap_money > 0 and margin_gap < 0:
        reasons.append({
            'metric': 'markup',
            'label': 'Наценка',
            'money': gap_money,
            'delta': abs(margin_gap),
            'line': f"Наценка: {_format_money(gap_money)} ({_format_pp(abs(margin_gap))})",
        })

    for metric, value in (impact.get('per_metric_effects') or {}).items():
        effect_money = _to_float(value)
        if effect_money <= 0:
            continue

        obj_value = _to_float((context.get('costs') or {}).get(metric, object_metrics.get(metric)))
        biz_value = _to_float((business_metrics or {}).get(metric))
        delta = obj_value - biz_value

        reasons.append({
            'metric': metric,
            'label': _reason_label(metric),
            'money': effect_money,
            'delta': abs(delta),
            'line': f"{_reason_label(metric)}: {_format_money(effect_money)} ({_format_money(abs(delta))})",
        })

    reasons.sort(key=lambda item: -item['money'])
    return reasons[:3]


def _solution_items(payload: Dict[str, Any], reasons: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    metrics = (payload.get('metrics') or {}).get('object_metrics') or {}
    impact = payload.get('impact') or {}

    solutions = build_solutions_from_effects(metrics, impact.get('per_metric_effects') or {})

    # добавляем решение по наценке, если есть разрыв по бизнесу
    gap_money = _to_float(impact.get('gap_loss_money'))
    margin_gap = _to_float(impact.get('gap_percent'))
    if gap_money > 0 and margin_gap < 0:
        price_effect = max(0.0, min(gap_money, _to_float(metrics.get('revenue')) * 0.05))
        if price_effect > 0:
            solutions.insert(0, {
                'metric': 'Наценка',
                'action': 'Поднять цену на 5%',
                'effect': price_effect,
            })

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for s in solutions:
        key = s.get('metric')
        if key in seen:
            continue
        seen.add(key)
        deduped.append(s)

    total_effect = 0.0
    out: List[Dict[str, Any]] = []
    for s in deduped[:3]:
        effect = _to_float(s.get('effect'))
        total_effect += effect
        coverage = 0.0
        if gap_money > 0:
            coverage = round((effect / gap_money) * 100.0, 2)
        out.append({
            'title': s.get('metric'),
            'action': s.get('action'),
            'effect': _format_money(effect),
            'coverage_percent': coverage,
            'line': f"{s.get('action')}\n→ +{_format_money(effect)}",
        })

    remaining = max(gap_money - total_effect, 0.0)

    if out:
        out.append({
            'title': 'summary',
            'action': 'Итог',
            'effect': _format_money(total_effect),
            'coverage_percent': round((total_effect / gap_money) * 100.0, 2) if gap_money > 0 else 0.0,
            'line': f"Покрытие: {_format_money(total_effect)} ({round((total_effect / gap_money) * 100.0, 2) if gap_money > 0 else 0.0}%)\nОсталось: {_format_money(remaining)}",
        })

    return out


def build_object_view(payload: Dict[str, Any], drain_payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    object_name = payload.get('object_name') or 'Объект'
    level = payload.get('level')
    period = payload.get('period')

    anchor = _base_anchor_rows(payload)
    vector = _vector_block(payload)
    drain = _drain_items(drain_payload)
    reasons = _reason_items(payload)
    solutions = _solution_items(payload, reasons)

    return {
        'type': 'object',
        'title': f'{object_name} — {period}' if period else str(object_name),
        'object_name': object_name,
        'level': level,
        'period': period,
        'anchor': anchor,
        'vector': vector,
        'drain': drain,
        'reasons': reasons,
        'solutions': solutions,
        'commands': ['1', '2', '3', 'все', 'причины', 'назад'],
    }


def build_list_view(current_payload: Dict[str, Any], source_payload: Dict[str, Any]) -> Dict[str, Any]:
    object_name = current_payload.get('object_name') or 'Объект'
    period = current_payload.get('period')
    level = current_payload.get('level')
    items = source_payload.get('items') or []
    child_level = source_payload.get('children_level')

    list_items: List[Dict[str, Any]] = []
    for idx, item in enumerate(items, start=1):
        metrics = (item.get('metrics') or {}).get('object_metrics') or {}
        impact = item.get('impact') or {}
        previous = item.get('previous_object_metrics') or {}

        yoy = None
        prev_finrez = _to_float(previous.get('finrez_pre'))
        curr_finrez = _to_float(metrics.get('finrez_pre'))
        if abs(prev_finrez) > 1e-9:
            yoy_delta = curr_finrez - prev_finrez
            sign = '+' if yoy_delta >= 0 else ''
            yoy = f"{sign}{_format_money(yoy_delta)} к прошлому году"

        line = (
            f"{item.get('object_name')}\n"
            f"Финрез: {_format_money(metrics.get('finrez_pre'))} | "
            f"Маржа: {_format_percent(metrics.get('margin_pre'))}\n"
            f"{yoy or '—'}\n"
            f"→ {_format_money(impact.get('gap_loss_money'))}"
        )

        list_items.append({
            'index': idx,
            'object_name': item.get('object_name'),
            'line': line,
        })

    return {
        'type': 'management_list',
        'title': f'{object_name} — {period}' if period else str(object_name),
        'level': level,
        'period': period,
        'child_level': child_level,
        'metrics': _base_anchor_rows(current_payload),
        'items': list_items,
        'commands': ['1', '2', '3', 'все', 'назад'],
        'items_meta': source_payload.get('items_meta') or {},
    }


def build_reasons_view(payload: Dict[str, Any]) -> Dict[str, Any]:
    object_name = payload.get('object_name') or 'Объект'
    period = payload.get('period')
    reasons = _reason_items(payload)
    solutions = _solution_items(payload, reasons)

    return {
        'type': 'reasons',
        'title': f'Причины — {object_name} — {period}' if period else f'Причины — {object_name}',
        'object_name': object_name,
        'period': period,
        'reasons': reasons,
        'solutions': solutions,
    }


def build_losses_view_from_children(source_payload: Dict[str, Any]) -> Dict[str, Any]:
    items = source_payload.get('items') or []
    losses: List[Dict[str, Any]] = []

    for idx, item in enumerate(items[:3], start=1):
        impact = item.get('impact') or {}
        diagnosis = item.get('diagnosis') or {}
        metric = diagnosis.get('top_drain_metric')
        losses.append({
            'index': idx,
            'object_name': item.get('object_name'),
            'metric': _reason_label(metric) if metric else '—',
            'money': _format_money(impact.get('gap_loss_money')),
            'line': f"{item.get('object_name')} | {_reason_label(metric) if metric else '—'} | {_format_money(impact.get('gap_loss_money'))}",
        })

    return {
        'type': 'losses',
        'title': 'Потери',
        'items': losses,
    }


def build_comparison_management_view(query: Dict[str, Any], current_payload: Dict[str, Any], previous_payload: Dict[str, Any]) -> Dict[str, Any]:
    current_metrics = (current_payload.get('metrics') or {}).get('object_metrics') or {}
    previous_metrics = (previous_payload.get('metrics') or {}).get('object_metrics') or {}

    rows = [
        {
            'label': 'Оборот',
            'current': _format_money(current_metrics.get('revenue')),
            'previous': _format_money(previous_metrics.get('revenue')),
        },
        {
            'label': 'Финрез',
            'current': _format_money(current_metrics.get('finrez_pre')),
            'previous': _format_money(previous_metrics.get('finrez_pre')),
        },
        {
            'label': 'Маржа',
            'current': _format_percent(current_metrics.get('margin_pre')),
            'previous': _format_percent(previous_metrics.get('margin_pre')),
        },
    ]

    return {
        'type': 'comparison',
        'title': f"{current_payload.get('object_name')} — сравнение периодов",
        'rows': rows,
        'current_period': current_payload.get('period'),
        'previous_period': query.get('period_previous'),
    }
