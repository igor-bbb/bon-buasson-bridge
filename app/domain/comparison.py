from typing import Any, Dict

from app.config import LOW_VOLUME_THRESHOLD
from app.domain.filters import filter_rows
from app.domain.metrics import (
    aggregate_metrics,
    build_effects,
    build_expected_metrics,
    build_gaps,
)
from app.domain.sorting import pick_top_drain


def build_flags(
    object_metrics: Dict[str, float],
    invalid_benchmark: bool,
    negative_benchmark: bool,
) -> Dict[str, bool]:
    low_volume = object_metrics["revenue"] < LOW_VOLUME_THRESHOLD
    return {
        "low_volume": low_volume,
        "invalid_benchmark": invalid_benchmark,
        "negative_benchmark": negative_benchmark,
    }


def build_comparison_payload(
    level: str,
    object_name: str,
    object_metrics: Dict[str, float],
    business_metrics: Dict[str, float],
) -> Dict[str, Any]:
    expected_metrics, invalid_benchmark, negative_benchmark = build_expected_metrics(
        object_metrics=object_metrics,
        business_metrics=business_metrics,
    )

    gaps_by_metric = build_gaps(
        object_metrics=object_metrics,
        expected_metrics=expected_metrics,
    )

    effects_by_metric = build_effects(gaps_by_metric)

    flags = build_flags(
        object_metrics=object_metrics,
        invalid_benchmark=invalid_benchmark,
        negative_benchmark=negative_benchmark,
    )

    top_drain_metric, top_drain_effect, top_drain_is_negative_for_business = pick_top_drain(
        effects_by_metric=effects_by_metric,
        low_volume=flags["low_volume"],
    )

    return {
        "level": level,
        "object_name": object_name,
        "object_metrics": object_metrics,
        "business_metrics": business_metrics,
        "expected_metrics": expected_metrics,
        "gaps_by_metric": gaps_by_metric,
        "effects_by_metric": effects_by_metric,
        "top_drain_metric": top_drain_metric,
        "top_drain_effect": top_drain_effect,
        "top_drain_is_negative_for_business": top_drain_is_negative_for_business,
        "flags": flags,
    }


def get_manager_top_comparison(manager_top: str, period: str) -> Dict[str, Any]:
    object_rows = filter_rows(period=period, manager_top=manager_top)
    business_rows = filter_rows(period=period)

    if not object_rows:
        return {"error": "manager_top not found or no data"}

    object_metrics = aggregate_metrics(object_rows)
    business_metrics = aggregate_metrics(business_rows)

    result = build_comparison_payload(
        level="manager_top",
        object_name=manager_top,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
    )
    result["period"] = period
    return result


def get_manager_comparison(manager: str, period: str) -> Dict[str, Any]:
    object_rows = filter_rows(period=period, manager=manager)
    business_rows = filter_rows(period=period)

    if not object_rows:
        return {"error": "manager not found or no data"}

    object_metrics = aggregate_metrics(object_rows)
    business_metrics = aggregate_metrics(business_rows)

    result = build_comparison_payload(
        level="manager",
        object_name=manager,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
    )
    result["period"] = period
    return result


def get_network_comparison(network: str, period: str) -> Dict[str, Any]:
    object_rows = filter_rows(period=period, network=network)
    business_rows = filter_rows(period=period)

    if not object_rows:
        return {"error": "network not found or no data"}

    object_metrics = aggregate_metrics(object_rows)
    business_metrics = aggregate_metrics(business_rows)

    result = build_comparison_payload(
        level="network",
        object_name=network,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
    )
    result["period"] = period
    return result


def get_category_comparison(category: str, period: str) -> Dict[str, Any]:
    object_rows = filter_rows(period=period, category=category)
    business_rows = filter_rows(period=period)

    if not object_rows:
        return {"error": "category not found or no data"}

    object_metrics = aggregate_metrics(object_rows)
    business_metrics = aggregate_metrics(business_rows)

    result = build_comparison_payload(
        level="category",
        object_name=category,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
    )
    result["period"] = period
    return result


def get_tmc_group_comparison(tmc_group: str, period: str) -> Dict[str, Any]:
    object_rows = filter_rows(period=period, tmc_group=tmc_group)
    business_rows = filter_rows(period=period)

    if not object_rows:
        return {"error": "tmc_group not found or no data"}

    object_metrics = aggregate_metrics(object_rows)
    business_metrics = aggregate_metrics(business_rows)

    result = build_comparison_payload(
        level="tmc_group",
        object_name=tmc_group,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
    )
    result["period"] = period
    return result


def get_sku_comparison(sku: str, period: str) -> Dict[str, Any]:
    object_rows = filter_rows(period=period, sku=sku)
    business_rows = filter_rows(period=period)

    if not object_rows:
        return {"error": "sku not found or no data"}

    object_metrics = aggregate_metrics(object_rows)
    business_metrics = aggregate_metrics(business_rows)

    result = build_comparison_payload(
        level="sku",
        object_name=sku,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
    )
    result["period"] = period
    return result
