from typing import Dict


def build_impact(payload: Dict) -> Dict:
    metrics = payload.get("metrics", {})

    object_metrics = metrics.get("object_metrics", {})
    business_metrics = metrics.get("business_metrics", {})

    impact = {}

    gap_money = (
        float(business_metrics.get("finrez_pre", 0) or 0)
        - float(object_metrics.get("finrez_pre", 0) or 0)
    )

    gap_percent = (
        float(business_metrics.get("margin_pre", 0) or 0)
        - float(object_metrics.get("margin_pre", 0) or 0)
    )

    per_metric_effects = {}

    for key in object_metrics:
        obj = float(object_metrics.get(key, 0) or 0)
        biz = float(business_metrics.get(key, 0) or 0)

        diff = biz - obj
        if diff > 0:
            per_metric_effects[key] = diff

    impact["gap_loss_money"] = gap_money
    impact["gap_percent"] = gap_percent
    impact["per_metric_effects"] = per_metric_effects

    return impact
