from typing import Any, Dict, List, Optional

from app.domain.normalization import round_money, round_percent

EFFECT_METRICS = ['finrez_pre', 'retro_bonus', 'logistics_cost', 'personnel_cost', 'other_costs']


def _safe_percent(numerator: float, denominator: float) -> float:
    if denominator == 0:
        return 0.0
    return round_percent((numerator / denominator) * 100.0)


def aggregate_metrics(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    revenue = round_money(sum(r.get('revenue', 0.0) for r in rows))
    cost = round_money(sum(r.get('cost', 0.0) for r in rows))
    gross_profit = round_money(sum(r.get('gross_profit', 0.0) for r in rows))
    retro_bonus = round_money(sum(r.get('retro_bonus', 0.0) for r in rows))
    logistics_cost = round_money(sum(r.get('logistics_cost', 0.0) for r in rows))
    personnel_cost = round_money(sum(r.get('personnel_cost', 0.0) for r in rows))
    other_costs = round_money(sum(r.get('other_costs', 0.0) for r in rows))
    finrez_pre = round_money(sum(r.get('finrez_pre', 0.0) for r in rows))

    margin_pre = _safe_percent(finrez_pre, revenue)
    markup = _safe_percent(gross_profit, cost)

    return {
        'revenue': revenue,
        'cost': cost,
        'gross_profit': gross_profit,
        'retro_bonus': retro_bonus,
        'logistics_cost': logistics_cost,
        'personnel_cost': personnel_cost,
        'other_costs': other_costs,
        'finrez_pre': finrez_pre,
        'margin_pre': margin_pre,
        'markup': markup,
    }


# 🔴 ЕДИНЫЙ GAP

def compute_gap_money(object_metrics: Dict[str, float], business_metrics: Dict[str, float]) -> float:
    margin_gap = business_metrics['margin_pre'] - object_metrics['margin_pre']
    if margin_gap <= 0:
        return 0.0
    return round_money((margin_gap / 100.0) * object_metrics['revenue'])


def compute_margin_gap(object_metrics: Dict[str, float], business_metrics: Dict[str, float]) -> float:
    return round_percent(object_metrics['margin_pre'] - business_metrics['margin_pre'])


# 🔴 EFFECTS

def build_expected_metrics(object_metrics, business_metrics):
    revenue = object_metrics['revenue']
    business_revenue = business_metrics['revenue']

    expected = {}

    for m in EFFECT_METRICS:
        if business_revenue == 0:
            expected[m] = 0.0
        else:
            expected[m] = round_money((business_metrics[m] / business_revenue) * revenue)

    return expected


def build_gaps(object_metrics, expected):
    return {k: round_money(object_metrics[k] - expected[k]) for k in EFFECT_METRICS}


def build_effects(gaps):
    return {k: v for k, v in gaps.items()}


def compute_total_loss(effects: Dict[str, float]) -> float:
    return round_money(sum(abs(v) for v in effects.values() if v > 0))


def compute_top_driver(effects: Dict[str, float]) -> tuple[str, float]:
    if not effects:
        return None, 0.0
    k = max(effects, key=lambda x: abs(effects[x]))
    return k, effects[k]
