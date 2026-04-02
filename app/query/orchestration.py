from typing import Any, Dict

from app.domain.comparison import (
    get_business_comparison,
    get_category_comparison,
    get_manager_comparison,
    get_manager_top_comparison,
    get_network_comparison,
    get_sku_comparison,
    get_tmc_group_comparison,
)
from app.domain.drilldown import (
    get_business_manager_tops_comparison,
    get_category_tmc_groups_comparison,
    get_manager_networks_comparison,
    get_manager_top_managers_comparison,
    get_network_categories_comparison,
    get_tmc_group_skus_comparison,
)
from app.presentation.contracts import error_response, not_implemented_response, ok_response
from app.presentation.views import (
    build_comparison_management_view,
    build_losses_view_from_children,
    build_reasons_view,
)
from app.query.parsing import parse_query_intent


LEVEL_COMPARATORS = {
    'business': lambda object_name, period: get_business_comparison(period=period),
    'manager_top': lambda object_name, period: get_manager_top_comparison(manager_top=object_name, period=period),
    'manager': lambda object_name, period: get_manager_comparison(manager=object_name, period=period),
    'network': lambda object_name, period: get_network_comparison(network=object_name, period=period),
    'category': lambda object_name, period: get_category_comparison(category=object_name, period=period),
    'tmc_group': lambda object_name, period: get_tmc_group_comparison(tmc_group=object_name, period=period),
    'sku': lambda object_name, period: get_sku_comparison(sku=object_name, period=period),
}


def _run_summary(level: str, object_name: str, period: str) -> Dict[str, Any]:
    return LEVEL_COMPARATORS[level](object_name, period)


def _build_comparison_mode_payload(query: Dict[str, Any]) -> Dict[str, Any]:
    level = query['level']
    object_name = query['object_name']
    period_current = query['period_current']
    period_previous = query['period_previous']

    current = _run_summary(level, object_name, period_current)
    if 'error' in current:
        return error_response(current['error'], query)

    previous = _run_summary(level, object_name, period_previous)
    if 'error' in previous:
        return error_response(previous['error'], query)

    data = build_comparison_management_view(query, current, previous)
    return ok_response(query, data)


def route_query(query: Dict[str, Any]) -> Dict[str, Any]:
    period = query['period_current']
    level = query['level']
    object_name = query['object_name']
    query_type = query['query_type']
    mode = query.get('mode', 'diagnosis')

    if mode == 'comparison':
        if query_type != 'summary':
            return not_implemented_response(query, 'comparison supports summary only')
        return _build_comparison_mode_payload(query)

    if level == 'business' and query_type == 'summary':
        data = get_business_comparison(period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'manager_top' and query_type == 'summary':
        data = get_manager_top_comparison(manager_top=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'manager' and query_type == 'summary':
        data = get_manager_comparison(manager=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'network' and query_type == 'summary':
        data = get_network_comparison(network=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'category' and query_type == 'summary':
        data = get_category_comparison(category=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'tmc_group' and query_type == 'summary':
        data = get_tmc_group_comparison(tmc_group=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'sku' and query_type == 'summary':
        data = get_sku_comparison(sku=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'business' and query_type == 'drill_down':
        data = get_business_manager_tops_comparison(period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'manager_top' and query_type == 'drill_down':
        data = get_manager_top_managers_comparison(manager_top=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'manager' and query_type == 'drill_down':
        data = get_manager_networks_comparison(manager=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'network' and query_type == 'drill_down':
        data = get_network_categories_comparison(network=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'category' and query_type == 'drill_down':
        data = get_category_tmc_groups_comparison(category=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'tmc_group' and query_type == 'drill_down':
        data = get_tmc_group_skus_comparison(tmc_group=object_name, period=period)
        if 'error' in data:
            return error_response(data['error'], query)
        return ok_response(query, data)

    if level == 'manager_top' and query_type == 'reasons':
        source = get_manager_top_comparison(manager_top=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == 'manager' and query_type == 'reasons':
        source = get_manager_comparison(manager=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == 'network' and query_type == 'reasons':
        source = get_network_comparison(network=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == 'category' and query_type == 'reasons':
        source = get_category_comparison(category=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == 'tmc_group' and query_type == 'reasons':
        source = get_tmc_group_comparison(tmc_group=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == 'sku' and query_type == 'reasons':
        source = get_sku_comparison(sku=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_reasons_view(source)
        return ok_response(query, data)

    if level == 'manager_top' and query_type == 'losses':
        source = get_manager_top_managers_comparison(manager_top=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    if level == 'manager' and query_type == 'losses':
        source = get_manager_networks_comparison(manager=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    if level == 'network' and query_type == 'losses':
        source = get_network_categories_comparison(network=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    if level == 'category' and query_type == 'losses':
        source = get_category_tmc_groups_comparison(category=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    if level == 'tmc_group' and query_type == 'losses':
        source = get_tmc_group_skus_comparison(tmc_group=object_name, period=period)
        if 'error' in source:
            return error_response(source['error'], query)
        data = build_losses_view_from_children(source)
        return ok_response(query, data)

    return not_implemented_response(query, 'scenario not implemented')


def orchestrate_vectra_query(message: str) -> Dict[str, Any]:
    parsed = parse_query_intent(message)

    if parsed['status'] != 'ok':
        return parsed

    return route_query(parsed['query'])
