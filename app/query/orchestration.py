from typing import Any, Dict, Optional, Tuple

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
from app.presentation.contracts import error_response
from app.query.parsing import parse_query_intent
from app.query.session import get_session, update_session


BASE_HANDLER_MAP: Dict[str, Tuple[Any, Optional[str]]] = {
    'business': (get_business_comparison, None),
    'manager_top': (get_manager_top_comparison, 'manager_top'),
    'manager': (get_manager_comparison, 'manager'),
    'network': (get_network_comparison, 'network'),
    'category': (get_category_comparison, 'category'),
    'tmc_group': (get_tmc_group_comparison, 'tmc_group'),
    'sku': (get_sku_comparison, 'sku'),
}


DRILL_HANDLER_MAP: Dict[Tuple[str, str], Tuple[Any, Optional[str]]] = {
    ('business', 'manager_top'): (get_business_manager_tops_comparison, None),
    ('manager_top', 'manager'): (get_manager_top_managers_comparison, 'manager_top'),
    ('manager', 'network'): (get_manager_networks_comparison, 'manager'),
    ('manager', 'category'): (get_manager_categories_comparison, 'manager'),
    ('network', 'category'): (get_network_categories_comparison, 'network'),
    ('network', 'tmc_group'): (get_network_tmc_groups_comparison, 'network'),
    ('network', 'sku'): (get_network_skus_comparison, 'network'),
    ('category', 'tmc_group'): (get_category_tmc_groups_comparison, 'category'),
    ('category', 'sku'): (get_category_skus_comparison, 'category'),
    ('tmc_group', 'sku'): (get_tmc_group_skus_comparison, 'tmc_group'),
}


LEVEL_ORDER = [
    'business',
    'manager_top',
    'manager',
    'network',
    'category',
    'tmc_group',
    'sku',
]


LEVEL_TO_FILTER_FIELD = {
    'manager_top': 'manager_top',
    'manager': 'manager',
    'network': 'network',
    'category': 'category',
    'tmc_group': 'tmc_group',
    'sku': 'sku',
}


def _get_next_level(level: str) -> Optional[str]:
    if level not in LEVEL_ORDER:
        return None

    index = LEVEL_ORDER.index(level)
    if index + 1 >= len(LEVEL_ORDER):
        return None

    return LEVEL_ORDER[index + 1]


def _prune_deeper_filters(query: Dict[str, Any], level: str) -> Dict[str, Any]:
    cleaned = dict(query)

    if level not in LEVEL_ORDER:
        return cleaned

    current_index = LEVEL_ORDER.index(level)

    for deeper_level in LEVEL_ORDER[current_index + 1:]:
        field_name = LEVEL_TO_FILTER_FIELD.get(deeper_level)
        if field_name:
            cleaned.pop(field_name, None)

    return cleaned


def _build_base_query(parsed_query: Dict[str, Any], session: Dict[str, Any]) -> Dict[str, Any]:
    level = parsed_query.get('level') or session.get('level')
    object_name = parsed_query.get('object_name')
    period = parsed_query.get('period') or session.get('period')

    if not level:
        return {}

    base_query: Dict[str, Any] = {
        'level': level,
        'object_name': object_name if object_name is not None else session.get('object_name'),
        'period': period,
    }

    for field in ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']:
        if field in parsed_query and parsed_query.get(field) is not None:
            base_query[field] = parsed_query.get(field)
        elif level == field:
            base_query[field] = base_query.get('object_name')

    base_query = _prune_deeper_filters(base_query, level)
    return base_query


def _call_base_handler(query: Dict[str, Any]) -> Dict[str, Any]:
    level = query.get('level')
    period = query.get('period')

    if not level:
        return error_response('level not recognized')

    if not period:
        return error_response('period not recognized')

    handler_meta = BASE_HANDLER_MAP.get(level)
    if not handler_meta:
        return error_response(f'base handler not found for level={level}')

    handler, object_arg = handler_meta

    if object_arg is None:
        return handler(period=period)

    object_value = query.get(object_arg)
    if not object_value:
        return error_response(f'{object_arg} not recognized')

    return handler(**{
        object_arg: object_value,
        'period': period,
    })


def _call_drill_handler(session: Dict[str, Any], target_level: Optional[str]) -> Dict[str, Any]:
    parent_level = session.get('level')
    period = session.get('period')

    if not parent_level or not period:
        return {'status': 'error', 'reason': 'Нет контекста для drilldown'}

    if not target_level:
        target_level = _get_next_level(parent_level)

    if not target_level:
        return error_response('next drilldown level not available')

    handler_meta = DRILL_HANDLER_MAP.get((parent_level, target_level))
    if not handler_meta:
        return error_response(f'drilldown not supported: {parent_level} -> {target_level}')

    handler, parent_arg = handler_meta

    if parent_arg is None:
        return handler(period=period)

    parent_value = session.get(parent_arg)
    if not parent_value:
        return error_response(f'{parent_arg} not found in session context')

    return handler(**{
        parent_arg: parent_value,
        'period': period,
    })


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    session = get_session(session_id)

    parsed = parse_query_intent(message)
    if parsed.get('status') != 'ok':
        return parsed

    query = parsed['query']
    query_type = query.get('query_type')

    # =========================
    # BASE
    # =========================
    if query_type == 'base':
        base_query = _build_base_query(query, session)
        if not base_query:
            return error_response('base query not recognized')

        response = _call_base_handler(base_query)

        if response.get('error') or response.get('status') == 'error':
            return response

        update_session(session_id, base_query)
        return response

    # =========================
    # DRILLDOWN (🔥 FIX)
    # =========================
    if query_type == 'drill_down':
    target_level = query.get('target_level')

    response = _call_drill_handler(session, target_level)

    if response.get('error') or response.get('status') == 'error':
        return response

    session_update = {
        'level': target_level or session.get('level'),
        'period': session.get('period'),
    }

    update_session(session_id, session_update)

    return response
