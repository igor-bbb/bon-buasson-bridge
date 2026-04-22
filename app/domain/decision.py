from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.config import DECISION_MIN_EFFECT_ABS, DECISION_MIN_EFFECT_SHARE

ACTION_LABELS = {
    'logistics_cost': 'Снижение логистики',
    'personnel_cost': 'Снижение персонала',
    'retro_bonus': 'Пересбор ретро',
    'other_costs': 'Снижение прочих затрат',
}

def _r(value: Any, digits: int = 2) -> Optional[float]:
    try:
        if value is None:
            return None
        return round(float(value), digits)
    except (TypeError, ValueError):
        return None

def build_decision_block(payload: Dict[str, Any]) -> Dict[str, Any]:
    level = str(payload.get('level') or '')
    if level not in {'network', 'sku'}:
        return {'actions': [], 'total_effect': 0.0, 'goal_gap': None, 'goal_closed': False}

    metrics = (payload.get('metrics') or {}).get('object_metrics') or {}
    reasons = (payload.get('diagnosis') or {}).get('effects_by_metric') or (payload.get('impact') or {}).get('per_metric_effects') or {}
    revenue = float(metrics.get('revenue') or 0.0)
    if revenue <= 0:
        return {'actions': [], 'total_effect': 0.0, 'goal_gap': None, 'goal_closed': False}

    threshold = max(float(DECISION_MIN_EFFECT_ABS), revenue * float(DECISION_MIN_EFFECT_SHARE))
    actions: List[Dict[str, Any]] = []
    for factor in ['logistics_cost', 'personnel_cost', 'retro_bonus', 'other_costs']:
        raw = reasons.get(factor)
        if isinstance(raw, dict):
            raw = raw.get('effect_value')
        effect = abs(float(raw or 0.0))
        if effect <= 0 or effect < threshold:
            continue
        delta_pp = (effect / revenue) * 100.0 if revenue else 0.0
        actions.append({
            'type': factor,
            'name': ACTION_LABELS.get(factor, factor),
            'delta_pp': _r(delta_pp),
            'effect_money': _r(effect),
        })

    actions.sort(key=lambda item: float(item.get('effect_money') or 0.0), reverse=True)
    actions = actions[:3]
    total_effect = _r(sum(float(item.get('effect_money') or 0.0) for item in actions)) or 0.0
    goal_gap = _r((payload.get('goal') or {}).get('value_money'))
    if goal_gap is None:
        goal_gap = _r((payload.get('impact') or {}).get('gap_loss_money'))
    goal_closed = bool(goal_gap is not None and total_effect >= abs(float(goal_gap or 0.0)))
    return {
        'actions': actions,
        'total_effect': total_effect,
        'goal_gap': goal_gap,
        'goal_closed': goal_closed,
    }
