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


SESSION_STORE: Dict[str, Dict[str, Any]] = {}


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


def _get_session_context(session_id: str) -> Dict[str, Any]:
    return SESSION_STORE.get(session_id, {})


def _save_session_context(session_id: str, query: Dict[str, Any]) -> None:
    SESSION_STORE[session_id] = {
        'level': query.get('level'),
        'object_name': query.get('object_name'),
        'period_current': query.get('period_current'),
        'period_previous': query.get('period_previous'),
        'mode': query.get('mode'),
        'query_type': query.get('query_type'),
    }


def _merge_with_session_context(query: Dict[str, Any], session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(query)

    if not merged.get('object_name') and session_ctx.get('object_name'):
        merged['object_name'] = session_ctx['object_name']

    if not merged.get('period_current') and session_ctx.get('period_current'):
        merged['period_current'] = session_ctx['period_current']

    if not merged.get('period_previous') and session_ctx.get('period_previous'):
        merged['period_previous'] = session_ctx['period_previous']

    # КРИТИЧНО:
    # для drill_down сохраняем родительский уровень из контекста,
    # а не переводим его в дочерний.
    if merged.get('query_type') == 'drill_down':
        if session_ctx.get('level'):
            merged['level'] = session_ctx['level']
    else:
        if not merged.get('level') and session_ctx.get('level'):
            merged['level'] = session_ctx['level']

    return merged


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


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    parsed = parse_query_intent(message)

    if parsed['status'] != 'ok':
        return parsed

    query = parsed['query']
    session_ctx = _get_session_context(session_id)
    query = _merge_with_session_context(query, session_ctx)

    response = route_query(query)

    if response.get('status') == 'ok':
        _save_session_context(session_id, query)

    return response
