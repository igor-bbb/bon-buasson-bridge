from __future__ import annotations

from typing import Any, Dict, List, Optional

from app.config import (
    DECISION_MIN_EFFECT_ABS,
    DECISION_MIN_EFFECT_SHARE,
)

ACTION_LABELS = {
    'logistics_cost': 'Снижение логистики',
    'personnel_cost': 'Снижение персонала',
    'retro_bonus': 'Пересмотр ретробонуса',
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
    metrics = (payload.get('metrics') or {}).get('object_metrics') or {}
    reasons = (payload.get('diagnosis') or {}).get('effects_by_metric') or (payload.get('impact') or {}).get('per_metric_effects') or {}
    revenue = float(metrics.get('revenue') or 0.0)
    if revenue <= 0:
        return {'actions': [], 'effect_total': 0.0, 'goal_gap': _r((payload.get('impact') or {}).get('gap_loss_money')), 'goal_closed': False}

    threshold = max(DECISION_MIN_EFFECT_ABS, revenue * DECISION_MIN_EFFECT_SHARE)
    actions: List[Dict[str, Any]] = []
    for factor in ['logistics_cost', 'personnel_cost', 'retro_bonus', 'other_costs']:
        raw = reasons.get(factor)
        if isinstance(raw, dict):
            raw = raw.get('effect_value')
        potential = abs(float(raw or 0.0))
        if potential <= 0:
            continue
        effect = potential
        if effect < threshold:
            continue
        delta_pp = (potential / revenue) * 100.0 if revenue else 0.0
        actions.append({
            'factor': factor,
            'action': ACTION_LABELS.get(factor, factor),
            'base_potential': _r(potential),
            'effect': _r(effect),
            'delta_pp': _r(delta_pp),
        })
    actions.sort(key=lambda item: float(item.get('effect') or 0.0), reverse=True)
    actions = actions[:3]
    effect_total = _r(sum(float(item.get('effect') or 0.0) for item in actions)) or 0.0
    gap = _r((payload.get('impact') or {}).get('gap_loss_money'))
    goal_closed = bool(gap is not None and effect_total >= abs(float(gap or 0.0)))
    return {
        'actions': actions,
        'effect_total': effect_total,
        'goal_gap': gap,
        'goal_closed': goal_closed,
    }
