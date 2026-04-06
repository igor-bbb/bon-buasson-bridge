from typing import Any, Dict, List

from app.domain.comparison import build_comparison_payload
from app.domain.consistency import build_consistency_from_rows
from app.domain.filters import filter_rows, get_normalized_rows
from app.domain.metrics import aggregate_metrics
from app.domain.normalization import normalize_sku_name
from app.domain.sorting import select_visible_items, sort_items_by_top_problem


def _handle_empty(meta: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'error': 'no data after filtering',
        'reason': meta.get('empty_reason'),
        'trace': meta.get('trace'),
    }


def _group(rows: List[Dict[str, Any]], field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        key = row.get(field) or 'UNKNOWN'
        grouped.setdefault(key, []).append(row)
    return grouped


def _build_items(grouped, level, business_metrics, period):
    items = []
    for name, chunk in grouped.items():
        item = build_comparison_payload(
            level=level,
            object_name=name,
            object_metrics=aggregate_metrics(chunk),
            business_metrics=business_metrics,
            period=period,
            object_rows=chunk,
        )
        items.append(item)

    sort_items_by_top_problem(items)
    return items


def _run_drilldown(
    *,
    filter_kwargs: Dict[str, Any],
    group_field: str,
    child_level: str,
    parent_level: str,
    parent_object: str,
    period: str,
    transform_sku: bool = False,
    full_view: bool = False,
) -> Dict[str, Any]:
    rows = get_normalized_rows()
    filtered_rows, meta = filter_rows(rows, period=period, **filter_kwargs)

    if not filtered_rows:
        return _handle_empty(meta)

    business_rows, _ = filter_rows(rows, period=period)
    business_metrics = aggregate_metrics(business_rows)

    if transform_sku:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for row in filtered_rows:
            sku_name = normalize_sku_name(row['sku'])
            grouped.setdefault(sku_name, []).append(row)
    else:
        grouped = _group(filtered_rows, group_field)

    all_items = _build_items(grouped, child_level, business_metrics, period)
    visible_items, items_meta = select_visible_items(all_items, full_view=full_view)

    parent_metrics = aggregate_metrics(filtered_rows)
    parent_consistency = build_consistency_from_rows(
        level=parent_level,
        parent_finrez_pre=parent_metrics.get('finrez_pre', 0.0),
        rows=filtered_rows,
    )

    return {
        'level': parent_level,
        'object_name': parent_object,
        'period': period,
        'children_level': child_level,
        'items': visible_items,
        'items_meta': items_meta,
        'full_view': full_view,
        'consistency': parent_consistency,
    }


def get_business_manager_tops_comparison(period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={}, group_field='manager_top', child_level='manager_top', parent_level='business', parent_object='business', period=period, full_view=full_view)


def get_business_managers_comparison(period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={}, group_field='manager', child_level='manager', parent_level='business', parent_object='business', period=period, full_view=full_view)


def get_business_networks_comparison(period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={}, group_field='network', child_level='network', parent_level='business', parent_object='business', period=period, full_view=full_view)


def get_business_categories_comparison(period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={}, group_field='category', child_level='category', parent_level='business', parent_object='business', period=period, full_view=full_view)


def get_business_tmc_groups_comparison(period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={}, group_field='tmc_group', child_level='tmc_group', parent_level='business', parent_object='business', period=period, full_view=full_view)


def get_business_skus_comparison(period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={}, group_field='sku', child_level='sku', parent_level='business', parent_object='business', period=period, transform_sku=True, full_view=full_view)


def get_manager_top_managers_comparison(manager_top: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'manager_top': manager_top}, group_field='manager', child_level='manager', parent_level='manager_top', parent_object=manager_top, period=period, full_view=full_view)


def get_manager_networks_comparison(manager: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'manager': manager}, group_field='network', child_level='network', parent_level='manager', parent_object=manager, period=period, full_view=full_view)


def get_manager_categories_comparison(manager: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'manager': manager}, group_field='category', child_level='category', parent_level='manager', parent_object=manager, period=period, full_view=full_view)


def get_manager_skus_comparison(manager: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'manager': manager}, group_field='sku', child_level='sku', parent_level='manager', parent_object=manager, period=period, transform_sku=True, full_view=full_view)


def get_network_categories_comparison(network: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'network': network}, group_field='category', child_level='category', parent_level='network', parent_object=network, period=period, full_view=full_view)


def get_network_tmc_groups_comparison(network: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'network': network}, group_field='tmc_group', child_level='tmc_group', parent_level='network', parent_object=network, period=period, full_view=full_view)


def get_network_skus_comparison(network: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'network': network}, group_field='sku', child_level='sku', parent_level='network', parent_object=network, period=period, transform_sku=True, full_view=full_view)


def get_category_tmc_groups_comparison(category: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'category': category}, group_field='tmc_group', child_level='tmc_group', parent_level='category', parent_object=category, period=period, full_view=full_view)


def get_category_skus_comparison(category: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'category': category}, group_field='sku', child_level='sku', parent_level='category', parent_object=category, period=period, transform_sku=True, full_view=full_view)


def get_tmc_group_skus_comparison(tmc_group: str, period: str, full_view: bool = False) -> Dict[str, Any]:
    return _run_drilldown(filter_kwargs={'tmc_group': tmc_group}, group_field='sku', child_level='sku', parent_level='tmc_group', parent_object=tmc_group, period=period, transform_sku=True, full_view=full_view)
