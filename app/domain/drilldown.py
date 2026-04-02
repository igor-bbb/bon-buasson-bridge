from typing import Any, Dict, List

from app.config import EMPTY_SKU_LABEL
from app.domain.comparison import build_comparison_payload
from app.domain.filters import filter_rows
from app.domain.metrics import aggregate_metrics
from app.domain.normalization import normalize_sku_name
from app.domain.sorting import sort_items_by_top_problem


def get_manager_comparison_from_rows(
    manager_name: str,
    manager_rows: List[Dict[str, Any]],
    business_metrics: Dict[str, float],
    period: str,
) -> Dict[str, Any]:
    object_metrics = aggregate_metrics(manager_rows)

    return build_comparison_payload(
        level='manager',
        object_name=manager_name,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
        period=period,
    )


def get_network_comparison_from_rows(
    network_name: str,
    network_rows: List[Dict[str, Any]],
    business_metrics: Dict[str, float],
    period: str,
) -> Dict[str, Any]:
    object_metrics = aggregate_metrics(network_rows)

    return build_comparison_payload(
        level='network',
        object_name=network_name,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
        period=period,
    )


def get_category_comparison_from_rows(
    category_name: str,
    category_rows: List[Dict[str, Any]],
    business_metrics: Dict[str, float],
    period: str,
) -> Dict[str, Any]:
    object_metrics = aggregate_metrics(category_rows)

    return build_comparison_payload(
        level='category',
        object_name=category_name,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
        period=period,
    )


def get_tmc_group_comparison_from_rows(
    tmc_group_name: str,
    tmc_group_rows: List[Dict[str, Any]],
    business_metrics: Dict[str, float],
    period: str,
) -> Dict[str, Any]:
    object_metrics = aggregate_metrics(tmc_group_rows)

    return build_comparison_payload(
        level='tmc_group',
        object_name=tmc_group_name,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
        period=period,
    )


def get_sku_comparison_from_rows(
    sku_name: str,
    sku_rows: List[Dict[str, Any]],
    business_metrics: Dict[str, float],
    period: str,
) -> Dict[str, Any]:
    object_metrics = aggregate_metrics(sku_rows)

    return build_comparison_payload(
        level='sku',
        object_name=sku_name,
        object_metrics=object_metrics,
        business_metrics=business_metrics,
        period=period,
    )


def get_manager_top_managers_comparison(manager_top: str, period: str) -> Dict[str, Any]:
    manager_top_rows = filter_rows(period=period, manager_top=manager_top)
    business_rows = filter_rows(period=period)

    if not manager_top_rows:
        return {'error': 'manager_top not found or no data'}

    business_metrics = aggregate_metrics(business_rows)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in manager_top_rows:
        manager_name = row['manager']
        grouped.setdefault(manager_name, []).append(row)

    items = []
    for manager_name, manager_rows in grouped.items():
        item = get_manager_comparison_from_rows(
            manager_name=manager_name,
            manager_rows=manager_rows,
            business_metrics=business_metrics,
            period=period,
        )
        items.append(item)

    sort_items_by_top_problem(items)

    return {
        'level': 'manager_top',
        'object_name': manager_top,
        'period': period,
        'children_level': 'manager',
        'items': items,
    }


def get_manager_networks_comparison(manager: str, period: str) -> Dict[str, Any]:
    manager_rows = filter_rows(period=period, manager=manager)
    business_rows = filter_rows(period=period)

    if not manager_rows:
        return {'error': 'manager not found or no data'}

    business_metrics = aggregate_metrics(business_rows)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in manager_rows:
        network_name = row['network']
        grouped.setdefault(network_name, []).append(row)

    items = []
    for network_name, network_rows in grouped.items():
        item = get_network_comparison_from_rows(
            network_name=network_name,
            network_rows=network_rows,
            business_metrics=business_metrics,
            period=period,
        )
        items.append(item)

    sort_items_by_top_problem(items)

    return {
        'level': 'manager',
        'object_name': manager,
        'period': period,
        'children_level': 'network',
        'items': items,
    }


def get_network_categories_comparison(network: str, period: str) -> Dict[str, Any]:
    network_rows = filter_rows(period=period, network=network)
    business_rows = filter_rows(period=period)

    if not network_rows:
        return {'error': 'network not found or no data'}

    business_metrics = aggregate_metrics(business_rows)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in network_rows:
        category_name = row['category']
        grouped.setdefault(category_name, []).append(row)

    items = []
    for category_name, category_rows in grouped.items():
        item = get_category_comparison_from_rows(
            category_name=category_name,
            category_rows=category_rows,
            business_metrics=business_metrics,
            period=period,
        )
        items.append(item)

    sort_items_by_top_problem(items)

    return {
        'level': 'network',
        'object_name': network,
        'period': period,
        'children_level': 'category',
        'items': items,
    }


def get_category_tmc_groups_comparison(category: str, period: str) -> Dict[str, Any]:
    category_rows = filter_rows(period=period, category=category)
    business_rows = filter_rows(period=period)

    if not category_rows:
        return {'error': 'category not found or no data'}

    business_metrics = aggregate_metrics(business_rows)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in category_rows:
        tmc_group_name = row['tmc_group']
        grouped.setdefault(tmc_group_name, []).append(row)

    items = []
    for tmc_group_name, tmc_group_rows in grouped.items():
        item = get_tmc_group_comparison_from_rows(
            tmc_group_name=tmc_group_name,
            tmc_group_rows=tmc_group_rows,
            business_metrics=business_metrics,
            period=period,
        )
        items.append(item)

    sort_items_by_top_problem(items)

    return {
        'level': 'category',
        'object_name': category,
        'period': period,
        'children_level': 'tmc_group',
        'items': items,
    }


def get_tmc_group_skus_comparison(tmc_group: str, period: str) -> Dict[str, Any]:
    tmc_group_rows = filter_rows(period=period, tmc_group=tmc_group)
    business_rows = filter_rows(period=period)

    if not tmc_group_rows:
        return {'error': 'tmc_group not found or no data'}

    business_metrics = aggregate_metrics(business_rows)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in tmc_group_rows:
        sku_name = normalize_sku_name(row['sku'])
        grouped.setdefault(sku_name, []).append(row)

    items = []
    for sku_name, sku_rows in grouped.items():
        item = get_sku_comparison_from_rows(
            sku_name=sku_name,
            sku_rows=sku_rows,
            business_metrics=business_metrics,
            period=period,
        )
        items.append(item)

    sort_items_by_top_problem(items)

    return {
        'level': 'tmc_group',
        'object_name': tmc_group,
        'period': period,
        'children_level': 'sku',
        'empty_sku_policy': EMPTY_SKU_LABEL,
        'items': items,
    }


def get_business_manager_tops_comparison(period: str) -> Dict[str, Any]:
    business_rows = filter_rows(period=period)
    if not business_rows:
        return {'error': 'business not found or no data'}

    business_metrics = aggregate_metrics(business_rows)

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in business_rows:
        manager_top_name = row['manager_top'] or row['manager']
        grouped.setdefault(manager_top_name, []).append(row)

    items = []
    for manager_top_name, manager_top_rows in grouped.items():
        level = 'manager_top' if any(r.get('manager_top') for r in manager_top_rows) else 'manager'
        item = build_comparison_payload(
            level=level,
            object_name=manager_top_name,
            object_metrics=aggregate_metrics(manager_top_rows),
            business_metrics=business_metrics,
            period=period,
        )
        items.append(item)

    sort_items_by_top_problem(items)

    return {
        'level': 'business',
        'object_name': 'business',
        'period': period,
        'children_level': 'manager_top',
        'items': items,
    }
