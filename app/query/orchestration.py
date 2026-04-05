from typing import Any, Dict, Optional

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
    get_business_categories_comparison,
    get_business_manager_tops_comparison,
    get_business_managers_comparison,
    get_business_networks_comparison,
    get_business_skus_comparison,
    get_business_tmc_groups_comparison,
    get_category_skus_comparison,
    get_category_tmc_groups_comparison,
    get_manager_categories_comparison,
    get_manager_networks_comparison,
    get_manager_top_managers_comparison,
    get_network_categories_comparison,
    get_network_skus_comparison,
    get_network_tmc_groups_comparison,
    get_tmc_group_skus_comparison,
)
from app.presentation.contracts import error_response, not_implemented_response, ok_response
from app.presentation.views import (
    build_comparison_management_view,
    build_drilldown_management_view,
    build_losses_view_from_children,
    build_management_view,
    build_reasons_view,
)
from app.query.parsing import parse_query_intent


# встроенная сессия, отдельный session.py не нужен
SESSION_STORE: Dict[str, Dict[str, Any]] = {}


def get_session(session_id: str) -> Dict[str, Any]:
    return SESSION_STORE.get(session_id, {})


def update_session(session_id: str, data: Dict[str, Any]) -> None:
    current = SESSION_STORE.get(session_id, {})
    current.update(data)
    SESSION_STORE[session_id] = current


SHORT_COMMAND_TARGETS = {
    'топы': 'manager_top',
    'топ менеджеры': 'manager_top',
    'топ-менеджеры': 'manager_top',
    'менеджеры': 'manager',
    'менеджер': 'manager',
    'сети': 'network',
    'сеть': 'network',
    'категории': 'category',
    'категория': 'category',
    'группы': 'tmc_group',
    'группа': 'tmc_group',
    'группы тмц': 'tmc_group',
    'группа тмц': 'tmc_group',
    'товары': 'sku',
    'товар': 'sku',
    'sku': 'sku',
    'скю': 'sku',
    'причины': 'reasons',
    'потери': 'losses',
    'сигнал': 'summary',
}

DEFAULT_NEXT_LEVEL = {
    'business': 'manager_top',
    'manager_top': 'manager',
    'manager': 'network',
    'network': 'category',
    'category': 'tmc_group',
    'tmc_group': 'sku',
}

SUMMARY_EXECUTORS = {
    'business': lambda obj, p: get_business_comparison(period=p),
    'manager_top': lambda obj, p: get_manager_top_comparison(manager_top=obj, period=p),
    'manager': lambda obj, p: get_manager_comparison(manager=obj, period=p),
    'network': lambda obj, p: get_network_comparison(network=obj, period=p),
    'category': lambda obj, p: get_category_comparison(category=obj, period=p),
    'tmc_group': lambda obj, p: get_tmc_group_comparison(tmc_group=obj, period=p),
    'sku': lambda obj, p: get_sku_comparison(sku=obj, period=p),
}


def _normalize_message(message: str) -> str:
    return (message or '').strip().lower()


def _is_short_command(message: str) -> bool:
    return _normalize_message(message) in SHORT_COMMAND_TARGETS


def _build_drill_from_scope(scope_level: str, scope_object_name: str, target_level: str, period: str) -> Dict[str, Any]:
    # business-wide navigation
    if scope_level == 'business':
        if target_level == 'manager_top':
            return get_business_manager_tops_comparison(period=period)
        if target_level == 'manager':
            return get_business_managers_comparison(period=period)
        if target_level == 'network':
            return get_business_networks_comparison(period=period)
        if target_level == 'category':
            return get_business_categories_comparison(period=period)
        if target_level == 'tmc_group':
            return get_business_tmc_groups_comparison(period=period)
        if target_level == 'sku':
            return get_business_skus_comparison(period=period)

    # manager_top-specific
    if scope_level == 'manager_top' and scope_object_name and target_level == 'manager':
        return get_manager_top_managers_comparison(manager_top=scope_object_name, period=period)

    # manager-specific
    if scope_level == 'manager' and scope_object_name:
        if target_level == 'network':
            return get_manager_networks_comparison(manager=scope_object_name, period=period)
        if target_level == 'category':
            return get_manager_categories_comparison(manager=scope_object_name, period=period)

    # network-specific
    if scope_level == 'network' and scope_object_name:
        if target_level == 'category':
            return get_network_categories_comparison(network=scope_object_name, period=period)
        if target_level == 'tmc_group':
            return get_network_tmc_groups_comparison(network=scope_object_name, period=period)
        if target_level == 'sku':
            return get_network_skus_comparison(network=scope_object_name, period=period)

    # category-specific
    if scope_level == 'category' and scope_object_name:
        if target_level == 'tmc_group':
            return get_category_tmc_groups_comparison(category=scope_object_name, period=period)
        if target_level == 'sku':
            return get_category_skus_comparison(category=scope_object_name, period=period)

    # tmc_group-specific
    if scope_level == 'tmc_group' and scope_object_name and target_level == 'sku':
        return get_tmc_group_skus_comparison(tmc_group=scope_object_name, period=period)

    return {'error': f'drilldown not supported: {scope_level} -> {target_level}'}


def _build_query_from_short_command(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_message(message)

    if normalized not in SHORT_COMMAND_TARGETS:
        return {}

    if not session_ctx or not session_ctx.get('scope_level') or not session_ctx.get('period_current'):
        return {'status': 'error', 'reason': 'уточни объект'}

    target = SHORT_COMMAND_TARGETS[normalized]

    if target in {'summary', 'reasons', 'losses'}:
        return {
            'status': 'ok',
            'query': {
                'mode': session_ctx.get('mode', 'diagnosis'),
                'level': session_ctx.get('scope_level'),
                'object_name': session_ctx.get('scope_object_name'),
                'period_current': session_ctx.get('period_current'),
                'period_previous': session_ctx.get('period_previous'),
                'query_type': target,
                'period': session_ctx.get('period_current'),
                'object': session_ctx.get('scope_object_name'),
            }
        }

    return {
        'status': 'ok',
        'query': {
            'mode': session_ctx.get('mode', 'diagnosis'),
            'level': session_ctx.get('scope_level'),
            'object_name': session_ctx.get('scope_object_name'),
            'period_current': session_ctx.get('period_current'),
            'period_previous': session_ctx.get('period_previous'),
            'query_type': 'drill_down',
            'target_level': target,
            'period': session_ctx.get('period_current'),
            'object': session_ctx.get('scope_object_name'),
        }
    }


def _store_scope_from_base(session_id: str, query: Dict[str, Any]) -> None:
    update_session(session_id, {
        'scope_level': query.get('level'),
        'scope_object_name': query.get('object_name'),
        'period_current': query.get('period_current'),
        'period_previous': query.get('period_previous'),
        'mode': query.get('mode', 'diagnosis'),
    })


def _route_base_query(query: Dict[str, Any]) -> Dict[str, Any]:
    level = query.get('level')
    period = query.get('period_current')
    object_name = query.get('object_name')
    mode = query.get('mode', 'diagnosis')

    if not level:
        return error_response('level not recognized', query)

    if not period:
        return error_response('period not recognized', query)

    if level != 'business' and not object_name:
        return error_response('object not recognized', query)

    executor = SUMMARY_EXECUTORS.get(level)
    if executor is None:
        return not_implemented_response(query, 'base query not supported')

    current = executor(object_name, period)
    if 'error' in current:
        return error_response(current['error'], query)

    if mode == 'comparison':
        previous_period = query.get('period_previous')
        if not previous_period:
            return error_response('comparison period not recognized', query)

        previous = executor(object_name, previous_period)
        if 'error' in previous:
            return error_response(previous['error'], query)

        return ok_response(query, build_comparison_management_view(query, current, previous))

    if query.get('query_type') == 'reasons':
        return ok_response(query, build_reasons_view(current))

    if query.get('query_type') == 'losses':
        target_level = DEFAULT_NEXT_LEVEL.get(level)
        if not target_level:
            return not_implemented_response(query, 'losses not supported for this level')

        source = _build_drill_from_scope(level, object_name, target_level, period)
        if 'error' in source:
            return error_response(source['error'], query)

        return ok_response(query, build_losses_view_from_children(source))

    return ok_response(query, build_management_view(current))


def _route_drill_query(query: Dict[str, Any], session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    scope_level = session_ctx.get('scope_level')
    scope_object_name = session_ctx.get('scope_object_name')
    period = session_ctx.get('period_current')
    target_level = query.get('target_level')

    if not scope_level or not period:
        return {'status': 'error', 'reason': 'уточни объект'}

    if not target_level:
        target_level = DEFAULT_NEXT_LEVEL.get(scope_level)

    if not target_level:
        return error_response('next drilldown level not available', query)

    payload = _build_drill_from_scope(scope_level, scope_object_name, target_level, period)
    if 'error' in payload:
        return error_response(payload['error'], query)

    return ok_response(query, build_drilldown_management_view(payload))


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    session_ctx = get_session(session_id)
    normalized = _normalize_message(message)

    # short commands always go through session first
    if _is_short_command(normalized):
        parsed = _build_query_from_short_command(normalized, session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    else:
        parsed = parse_query_intent(message)
        if parsed.get('status') != 'ok':
            return parsed

    query = parsed['query']
    query_type = query.get('query_type', 'summary')

    if query_type == 'drill_down':
        return _route_drill_query(query, session_ctx)

    response = _route_base_query(query)

    if response.get('status') == 'ok':
        _store_scope_from_base(session_id, query)

    return response
