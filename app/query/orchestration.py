from typing import Any, Dict, Tuple

from app.domain.comparison import *
from app.domain.drilldown import *
from app.presentation.contracts import error_response, not_implemented_response, ok_response
from app.presentation.views import (
    build_comparison_management_view,
    build_losses_view_from_children,
    build_reasons_view,
)
from app.query.parsing import parse_query_intent

SESSION_STORE: Dict[str, Dict[str, Any]] = {}

LEVEL_HIERARCHY = [
    'business',
    'manager_top',
    'manager',
    'network',
    'category',
    'tmc_group',
    'sku',
]


# ========================
# SESSION
# ========================

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

    for field in ['object_name', 'period_current', 'period_previous', 'level']:
        if not merged.get(field) and session_ctx.get(field):
            merged[field] = session_ctx[field]

    return merged


# ========================
# LEVEL NAVIGATION
# ========================

def _get_next_level(level: str) -> str:
    if level not in LEVEL_HIERARCHY:
        return level
    idx = LEVEL_HIERARCHY.index(level)
    if idx + 1 < len(LEVEL_HIERARCHY):
        return LEVEL_HIERARCHY[idx + 1]
    return level


# ========================
# EXECUTION MAP
# ========================

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
    'manager': lambda obj, p: get_manager_networks_comparison(manager=obj, period=p),
    'network': lambda obj, p: get_network_categories_comparison(network=obj, period=p),
    'category': lambda obj, p: get_category_tmc_groups_comparison(category=obj, period=p),
    'tmc_group': lambda obj, p: get_tmc_group_skus_comparison(tmc_group=obj, period=p),
}

REASONS_EXECUTORS = SUMMARY_EXECUTORS
LOSSES_EXECUTORS = DRILL_EXECUTORS


# ========================
# ROUTER
# ========================

def _run_executor(executor, query, is_child=False):
    level = query['level']
    obj = query['object_name']
    period = query['period_current']

    result = executor(level, obj, period)
    if 'error' in result:
        return error_response(result['error'], query)

    return result


def route_query(query: Dict[str, Any]) -> Dict[str, Any]:
    level = query['level']
    query_type = query['query_type']
    mode = query.get('mode', 'diagnosis')

    # ========================
    # SUMMARY
    # ========================
    if query_type == 'summary':
        data = SUMMARY_EXECUTORS[level](query['object_name'], query['period_current'])
        if 'error' in data:
            return error_response(data['error'], query)

        # comparison overlay
        if mode == 'comparison':
            prev = SUMMARY_EXECUTORS[level](query['object_name'], query['period_previous'])
            return ok_response(query, build_comparison_management_view(query, data, prev))

        return ok_response(query, data)

    # ========================
    # DRILL DOWN
    # ========================
    if query_type == 'drill_down':
        if level not in DRILL_EXECUTORS:
            return not_implemented_response(query, 'drill not supported')

        data = DRILL_EXECUTORS[level](query['object_name'], query['period_current'])
        if 'error' in data:
            return error_response(data['error'], query)

        return ok_response(query, data)

    # ========================
    # REASONS
    # ========================
    if query_type == 'reasons':
        source = SUMMARY_EXECUTORS[level](query['object_name'], query['period_current'])
        if 'error' in source:
            return error_response(source['error'], query)

        return ok_response(query, build_reasons_view(source))

    # ========================
    # LOSSES
    # ========================
    if query_type == 'losses':
        source = DRILL_EXECUTORS[level](query['object_name'], query['period_current'])
        if 'error' in source:
            return error_response(source['error'], query)

        return ok_response(query, build_losses_view_from_children(source))

    return not_implemented_response(query, 'scenario not implemented')


# ========================
# ENTRYPOINT
# ========================

def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    parsed = parse_query_intent(message)

    if parsed['status'] != 'ok':
        return parsed

    query = parsed['query']
    session_ctx = _get_session_context(session_id)

    query = _merge_with_session_context(query, session_ctx)

    print("DEBUG FINAL QUERY =", query)

    response = route_query(query)

    if response.get('status') == 'ok':
        _save_session_context(session_id, query)

    return response
