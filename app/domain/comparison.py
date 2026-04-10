from app.domain.metrics import (
    compute_gap_money,
    compute_margin_gap,
    build_expected_metrics,
    build_gaps,
    build_effects,
    compute_total_loss,
    compute_top_driver,
)


def build_comparison_payload(
    level,
    object_name,
    object_metrics,
    business_metrics,
    period,
    object_rows=None,
):
    expected = build_expected_metrics(object_metrics, business_metrics)
    gaps = build_gaps(object_metrics, expected)
    effects = build_effects(gaps)

    gap_money = compute_gap_money(object_metrics, business_metrics)
    margin_gap = compute_margin_gap(object_metrics, business_metrics)

    total_loss = compute_total_loss(effects)
    top_metric, top_value = compute_top_driver(effects)

    return {
        "level": level,
        "object_name": object_name,
        "period": period,

        "metrics": {
            "object_metrics": object_metrics,
            "business_metrics": business_metrics,
        },

        "impact": {
            "gap_loss_money": gap_money,
            "total_loss": total_loss,
            "per_metric_effects": effects,
        },

        "signal": {
            "margin_gap": margin_gap,
            "status": "critical" if gap_money > 0 else "ok",
        },

        "diagnosis": {
            "top_drain_metric": top_metric,
            "top_drain_effect": top_value,
        },

        "priority": {
            "priority": "high" if gap_money > 0 else "low"
        },

        "action": {
            "suggested_action": f"разобрать {top_metric}" if top_metric else "ok"
        },
    }
