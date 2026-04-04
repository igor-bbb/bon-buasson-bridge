from typing import Any, Dict, List, Tuple

from app.config import EMPTY_SKU_LABEL
from app.domain.comparison import build_comparison_payload
from app.domain.filters import filter_rows, get_normalized_rows
from app.domain.metrics import aggregate_metrics
from app.domain.normalization import normalize_sku_name
from app.domain.sorting import sort_items_by_top_problem


def _handle_empty(meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'error': 'no data after filtering',
        'reason': meta.get('empty_reason'),
        'trace': meta.get('trace'),
    }


def _group(rows: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for row in rows:
        key = row.get(field)
        if not key:
            continue
        grouped.setdefault(key, []).append(row)

    return grouped


# --- BUSINESS → MANAGER_TOP

def get_business_manager_tops_comparison(period: str) -> Dict[str, Any]:
    rows = get_normalized_rows()
    business_rows, meta = filter_rows(rows, period=period)

    if not business_rows:
        return _handle_empty(meta)

    business_metrics = aggregate_metrics(business_rows)

    grouped = _group(business_rows, 'manager_top')

    items = []
    for name, chunk in grouped.items():
        item = build_comparison_payload(
            level='manager_top',
            object_name=name,
            object_metrics=aggregate_metrics(chunk),
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


# --- MANAGER_TOP → MANAGER

def get_manager_top_managers_comparison(manager_top: str, period: str) -> Dict[str, Any]:
    rows = get_normalized_rows()
    manager_top_rows, meta = filter_rows(rows, period=period, manager_top=manager_top)

    if not manager_top_rows:
        return _handle_empty(meta)

    business_rows, _ = filter_rows(rows, period=period)
    business_metrics = aggregate_metrics(business_rows)

    grouped = _group(manager_top_rows, 'manager')

    items = []
    for name, chunk in grouped.items():
        item = build_comparison_payload(
            level='manager',
            object_name=name,
            object_metrics=aggregate_metrics(chunk),
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


# --- MANAGER → NETWORK

def get_manager_networks_comparison(manager: str, period: str) -> Dict[str, Any]:
    rows = get_normalized_rows()
    manager_rows, meta = filter_rows(rows, period=period, manager=manager)

    if not manager_rows:
        return _handle_empty(meta)

    business_rows, _ = filter_rows(rows, period=period)
    business_metrics = aggregate_metrics(business_rows)

    grouped = _group(manager_rows, 'network')

    items = []
    for name, chunk in grouped.items():
        item = build_comparison_payload(
            level='network',
            object_name=name,
            object_metrics=aggregate_metrics(chunk),
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


# --- NETWORK → CATEGORY

def get_network_categories_comparison(network: str, period: str) -> Dict[str, Any]:
    rows = get_normalized_rows()
    network_rows, meta = filter_rows(rows, period=period, network=network)

    print("DEBUG get_network_categories_comparison")
    print("DEBUG network =", repr(network))
    print("DEBUG period =", repr(period))
    print("DEBUG rows after filter =", len(network_rows))
    print("DEBUG trace =", meta.get("trace"))

    if not network_rows:
        return _handle_empty(meta)

    business_rows, _ = filter_rows(rows, period=period)
    business_metrics = aggregate_metrics(business_rows)

    grouped = _group(network_rows, 'category')

    items = []
    for name, chunk in grouped.items():
        item = build_comparison_payload(
            level='category',
            object_name=name,
            object_metrics=aggregate_metrics(chunk),
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


# --- CATEGORY → TMC_GROUP

def get_category_tmc_groups_comparison(category: str, period: str) -> Dict[str, Any]:
    rows = get_normalized_rows()
    category_rows, meta = filter_rows(rows, period=period, category=category)

    if not category_rows:
        return _handle_empty(meta)

    business_rows, _ = filter_rows(rows, period=period)
    business_metrics = aggregate_metrics(business_rows)

    grouped = _group(category_rows, 'tmc_group')

    items = []
    for name, chunk in grouped.items():
        item = build_comparison_payload(
            level='tmc_group',
            object_name=name,
            object_metrics=aggregate_metrics(chunk),
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


# --- TMC_GROUP → SKU

def get_tmc_group_skus_comparison(tmc_group: str, period: str) -> Dict[str, Any]:
    rows = get_normalized_rows()
    tmc_group_rows, meta = filter_rows(rows, period=period, tmc_group=tmc_group)

    if not tmc_group_rows:
        return _handle_empty(meta)

    business_rows, _ = filter_rows(rows, period=period)
    business_metrics = aggregate_metrics(business_rows)

    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for row in tmc_group_rows:
        sku_name = normalize_sku_name(row['sku'])
        grouped.setdefault(sku_name, []).append(row)

    items = []
    for name, chunk in grouped.items():
        item = build_comparison_payload(
            level='sku',
            object_name=name,
            object_metrics=aggregate_metrics(chunk),
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
