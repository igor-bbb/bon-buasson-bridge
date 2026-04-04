from typing import Any, Dict

from app.domain.comparison import *
from app.domain.drilldown import *
from app.presentation.contracts import error_response, not_implemented_response, ok_response
from app.presentation.views import (
    build_comparison_management_view,
    build_drilldown_management_view,
    build_management_view,
    build_losses_view_from_children,
    build_reasons_view,
)
from app.query.parsing import parse_query_intent

SESSION_STORE: Dict[str, Dict[str, Any]] = {}

LEVEL_HIERARCHY = [
    'business',
    'manager_top',
    'manager',
    'category',
    'sku',
]

SHORT_COMMANDS = {
    'сигнал': 'summary',
    'причины': 'reasons',
    'потери': 'losses',
    'категории': 'drill_down',
    'товары': 'drill_down',
    'менеджер': 'drill_down',
}


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
        'parent': query.get('parent'),
    }


def _merge_with_session_context(query: Dict[str, Any], session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(query)

    for field in [
        'object_name',
        'period_current',
        'period_previous',
        'level',
        'query_type',
        'mode',
        'parent',
    ]:
        if not merged.get(field) and session_ctx.get(field):
            merged[field] = session_ctx[field]

    if not merged.get('mode'):
        merged['mode'] = 'diagnosis'

    return merged


def _normalize_message(message: str) -> str:
    return (message or '').strip().lower()


def _is_short_command(message: str) -> bool:
    return _normalize_message(message) in SHORT_COMMANDS


def _build_query_from_short_command(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_message(message)

    if normalized not in SHORT_COMMANDS:
        return {}

    if not session_ctx:
        return {
            'status': 'error',
            'reason': 'уточни объект и период',
        }

    if not session_ctx.get('level') or not session_ctx.get('period_current'):
        return {
            'status': 'error',
            'reason': 'уточни объект и период',
        }

    query_type = SHORT_COMMANDS[normalized]

    return {
        'status': 'ok',
        'query': {
            'mode': session_ctx.get('mode', 'diagnosis'),
            'level': session_ctx.get('level'),
            'object_name': session_ctx.get('object_name'),
            'period_current': session_ctx.get('period_current'),
            'period_previous': session_ctx.get('period_previous'),
            'query_type': query_type,
            'parent': session_ctx.get('parent'),
        }
    }


def _apply_short_command_overrides(message: str, query: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_message(message)
    if normalized not in SHORT_COMMANDS:
        return query

    patched = dict(query)
    patched['query_type'] = SHORT_COMMANDS[normalized]

    if not patched.get('mode'):
        patched['mode'] = 'diagnosis'

    return patched


SUMMARY_EXECUTORS = {
    'business': lambda obj, p: get_business_comparison(period=p),
    'manager_top': lambda obj, p: get_manager_top_comparison(manager_top=obj, period=p),
    'manager': lambda obj, p: get_manager_comparison(manager=obj, period=p),
    'network': lambda obj, p: get_network_comparison(network=obj, period=p),
    'category': lambda obj, p: get_category_comparison(category=obj, period=p),
    'tmc_group': lambda obj, p: get_tmc_group_comparison(tmc_group=obj, period=p),
    'sku': lambda obj, p: get_sku_comparison(sku=obj, period=p),
}

DRILL_EXECUTORS = {
    'business': lambda obj, p: get_business_manager_tops_comparison(period=p),
    'manager_top': lambda obj, p: get_manager_top_managers_comparison(manager_top=obj, period=p),
    'manager': lambda obj, p: get_manager_categories_comparison(manager=obj, period=p),
    'network': lambda obj, p: get_network_categories_comparison(network=obj, period=p),
    'category': lambda obj, p: get_category_skus_comparison(category=obj, period=p),
    'tmc_group': lambda obj, p: get_tmc_group_skus_comparison(tmc_group=obj, period=p),
}

REASONS_EXECUTORS = SUMMARY_EXECUTORS
LOSSES_EXECUTORS = DRILL_EXECUTORS


def _validate_query(query: Dict[str, Any]) -> Dict[str, Any] | None:
    if not query.get('level'):
        return {'status': 'error', 'reason': 'level not recognized'}

    if not query.get('period_current'):
        return {'status': 'error', 'reason': 'period not recognized'}

    if not query.get('query_type'):
        return {'status': 'error', 'reason': 'query type not recognized'}

    query_type = query.get('query_type')
    level = query.get('level')

    if query_type in ('summary', 'reasons', 'drill_down', 'losses'):
        if level != 'business' and not query.get('object_name'):
            return {'status': 'error', 'reason': 'object not recognized'}

    return None


def route_query(query: Dict[str, Any]) -> Dict[str, Any]:
    validation_error = _validate_query(query)
    if validation_error:
        return validation_error

    level = query['level']
    query_type = query['query_type']
    mode = query.get('mode', 'diagnosis')

    if query_type == 'summary':
        data = SUMMARY_EXECUTORS[level](query.get('object_name'), query['period_current'])
        if 'error' in data:
            return error_response(data['error'], query)

        if mode == 'comparison':
            prev_period = query.get('period_previous')
            if not prev_period:
                return {
                    'status': 'error',
                    'reason': 'period_previous not recognized',
                }

            prev = SUMMARY_EXECUTORS[level](query.get('object_name'), prev_period)
            if 'error' in prev:
                return error_response(prev['error'], query)

            return ok_response(query, build_comparison_management_view(query, data, prev))

        return ok_response(query, build_management_view(data))

    if query_type == 'drill_down':
        if level not in DRILL_EXECUTORS:
            return not_implemented_response(query, 'drill not supported')

        data = DRILL_EXECUTORS[level](query.get('object_name'), query['period_current'])
        if 'error' in data:
            return error_response(data['error'], query)

        return ok_response(query, build_drilldown_management_view(data))

    if query_type == 'reasons':
        source = REASONS_EXECUTORS[level](query.get('object_name'), query['period_current'])
        if 'error' in source:
            return error_response(source['error'], query)

        return ok_response(query, build_reasons_view(source))

    if query_type == 'losses':
        if level not in LOSSES_EXECUTORS:
            return not_implemented_response(query, 'losses not supported')

        source = LOSSES_EXECUTORS[level](query.get('object_name'), query['period_current'])
        if 'error' in source:
            return error_response(source['error'], query)

        return ok_response(query, build_losses_view_from_children(source))

    return not_implemented_response(query, 'scenario not implemented')


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    session_ctx = _get_session_context(session_id)
    normalized = _normalize_message(message)

    if _is_short_command(normalized):
        parsed = _build_query_from_short_command(normalized, session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    else:
        parsed = parse_query_intent(message)
        if parsed.get('status') != 'ok':
            return parsed

    query = parsed['query']
    query = _apply_short_command_overrides(message, query)
    query = _merge_with_session_context(query, session_ctx)

    response = route_query(query)

    if response.get('status') == 'ok':
        _save_session_context(session_id, query)

    return response
