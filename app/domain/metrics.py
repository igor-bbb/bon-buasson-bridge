from typing import Any, Dict, List, Tuple

from app.domain.normalization import round_money

MONEY_METRICS = ["revenue", "retro_bonus", "logistics_cost", "other_costs", "finrez_pre"]
EFFECT_METRICS = ["retro_bonus", "logistics_cost", "other_costs", "finrez_pre"]

METRIC_TYPES = {
    "retro_bonus": "cost",
    "logistics_cost": "cost",
    "other_costs": "cost",
    "finrez_pre": "income",
}


def aggregate_metrics(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    revenue = round_money(sum(r["revenue"] for r in rows))
    retro_bonus = round_money(sum(r["retro_bonus"] for r in rows))
    logistics_cost = round_money(sum(r["logistics_cost"] for r in rows))
    other_costs = round_money(sum(r["other_costs"] for r in rows))
    finrez_pre = round_money(sum(r["finrez_pre"] for r in rows))

    margin_pre = round((finrez_pre / revenue) * 100, 2) if revenue != 0 else 0.0
    markup = round(sum(r["markup"] for r in rows) / len(rows), 2) if rows else 0.0

    return {
        "revenue": revenue,
        "retro_bonus": retro_bonus,
        "logistics_cost": logistics_cost,
        "other_costs": other_costs,
        "finrez_pre": finrez_pre,
        "margin_pre": margin_pre,
        "markup": markup,
    }


def build_expected_metrics(
    object_metrics: Dict[str, float],
    business_metrics: Dict[str, float],
) -> Tuple[Dict[str, float], bool, bool]:
    business_revenue = business_metrics["revenue"]
    object_revenue = object_metrics["revenue"]

    invalid_benchmark = business_revenue == 0
    negative_benchmark = False

    expected = {}

    for metric in EFFECT_METRICS:
        if invalid_benchmark:
            expected_value = 0.0
        else:
            expected_value = (business_metrics[metric] / business_revenue) * object_revenue

        expected_value = round_money(expected_value)

        if expected_value < 0:
            negative_benchmark = True

        expected[metric] = expected_value

    return expected, invalid_benchmark, negative_benchmark


def build_gaps(
    object_metrics: Dict[str, float],
    expected_metrics: Dict[str, float],
) -> Dict[str, float]:
    gaps = {}
    for metric in EFFECT_METRICS:
        gaps[metric] = round_money(object_metrics[metric] - expected_metrics[metric])
    return gaps


def interpret_effect(metric: str, effect_value: float) -> Dict[str, Any]:
    metric_type = METRIC_TYPES[metric]

    if metric_type == "cost":
        is_negative_for_business = effect_value > 0
    else:
        is_negative_for_business = effect_value < 0

    effect_direction = "loss" if is_negative_for_business else "gain"

    return {
        "effect_value": round_money(effect_value),
        "effect_direction": effect_direction,
        "type": metric_type,
        "is_negative_for_business": is_negative_for_business,
    }


def build_effects(gaps_by_metric: Dict[str, float]) -> Dict[str, Dict[str, Any]]:
    effects = {}
    for metric, gap_value in gaps_by_metric.items():
        effects[metric] = interpret_effect(metric, gap_value)
    return effects
