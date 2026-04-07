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
    get_manager_skus_comparison,
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
    build_signal_flow_view,
)
from app.query.entity_dictionary import get_entity_dictionary, normalize_entity_text
from app.query.parsing import normalize_user_message, parse_query_intent


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
    'топ менеджер': 'manager_top',
    'топ-менеджеры': 'manager_top',
    'топ-менеджер': 'manager_top',
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
FULL_VIEW_COMMANDS = {'покажи все', 'все', 'full'}

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
    return normalize_user_message(message)


def _is_short_command(message: str) -> bool:
    return _normalize_message(message) in SHORT_COMMAND_TARGETS


def _is_full_view_command(message: str) -> bool:
    return _normalize_message(message) in FULL_VIEW_COMMANDS


def _build_drill_from_scope(
    scope_level: str,
    scope_object_name: Optional[str],
    target_level: str,
    period: str,
    full_view: bool = False,
) -> Dict[str, Any]:
    if scope_level == 'business':
        if target_level == 'manager_top':
            return get_business_manager_tops_comparison(period=period, full_view=full_view)
        if target_level == 'manager':
            return get_business_managers_comparison(period=period, full_view=full_view)
        if target_level == 'network':
            return get_business_networks_comparison(period=period, full_view=full_view)
        if target_level == 'category':
            return get_business_categories_comparison(period=period, full_view=full_view)
        if target_level == 'tmc_group':
            return get_business_tmc_groups_comparison(period=period, full_view=full_view)
        if target_level == 'sku':
            return get_business_skus_comparison(period=period, full_view=full_view)

    if scope_level == 'manager_top' and scope_object_name:
        if target_level == 'manager':
            return get_manager_top_managers_comparison(manager_top=scope_object_name, period=period, full_view=full_view)

    if scope_level == 'manager' and scope_object_name:
        if target_level == 'network':
            return get_manager_networks_comparison(manager=scope_object_name, period=period, full_view=full_view)
        if target_level == 'category':
            return get_manager_categories_comparison(manager=scope_object_name, period=period, full_view=full_view)
        if target_level == 'sku':
            return get_manager_skus_comparison(manager=scope_object_name, period=period, full_view=full_view)

    if scope_level == 'network' and scope_object_name:
        if target_level == 'category':
            return get_network_categories_comparison(network=scope_object_name, period=period, full_view=full_view)
        if target_level == 'tmc_group':
            return get_network_tmc_groups_comparison(network=scope_object_name, period=period, full_view=full_view)
        if target_level == 'sku':
            return get_network_skus_comparison(network=scope_object_name, period=period, full_view=full_view)

    if scope_level == 'category' and scope_object_name:
        if target_level == 'tmc_group':
            return get_category_tmc_groups_comparison(category=scope_object_name, period=period, full_view=full_view)
        if target_level == 'sku':
            return get_category_skus_comparison(category=scope_object_name, period=period, full_view=full_view)

    if scope_level == 'tmc_group' and scope_object_name:
        if target_level == 'sku':
            return get_tmc_group_skus_comparison(tmc_group=scope_object_name, period=period, full_view=full_view)

    return {'error': f'drilldown not supported: {scope_level} -> {target_level}'}


def _store_scope(session_id: str, level: str, object_name: str, period_current: str, period_previous: Any, mode: str) -> None:
    update_session(session_id, {
        'scope_level': level,
        'scope_object_name': object_name,
        'period_current': period_current,
        'period_previous': period_previous,
        'mode': mode,
    })


def _store_list_context(
    session_id: str,
    parent_level: str,
    parent_object_name: str,
    period_current: str,
    period_previous: Any,
    mode: str,
    list_level: str,
    response_type: str = 'drill_down',
) -> None:
    update_session(session_id, {
        'scope_level': parent_level,
        'scope_object_name': parent_object_name,
        'period_current': period_current,
        'period_previous': period_previous,
        'mode': mode,
        'last_list_level': list_level,
        'last_response_type': response_type,
    })


def _build_query_from_short_command(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_message(message)

    if normalized not in SHORT_COMMAND_TARGETS:
        return {}

    if not session_ctx or not session_ctx.get('scope_level') or not session_ctx.get('period_current'):
        return {'status': 'error', 'reason': 'Нет активного объекта для выполнения команды.'}

    target = SHORT_COMMAND_TARGETS[normalized]

    if target in {'summary', 'reasons', 'losses'}:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': session_ctx.get('scope_level'),
                'object_name': session_ctx.get('scope_object_name'),
                'period_current': session_ctx.get('period_current'),
                'period_previous': session_ctx.get('period_previous'),
                'query_type': target,
                'period': session_ctx.get('period_current'),
                'object': session_ctx.get('scope_object_name'),
            },
        }

    return {
        'status': 'ok',
        'query': {
            'mode': 'diagnosis',
            'level': session_ctx.get('scope_level'),
            'object_name': session_ctx.get('scope_object_name'),
            'period_current': session_ctx.get('period_current'),
            'period_previous': session_ctx.get('period_previous'),
            'query_type': 'drill_down',
            'target_level': target,
            'period': session_ctx.get('period_current'),
            'object': session_ctx.get('scope_object_name'),
        },
    }


def _build_query_from_full_view(session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    if not session_ctx or not session_ctx.get('period_current') or not session_ctx.get('last_list_level'):
        return {'status': 'error', 'reason': 'Нет данных для отображения.'}

    response_type = session_ctx.get('last_response_type', 'drill_down')
    query_type = 'summary' if response_type == 'signal_flow' else 'drill_down'

    return {
        'status': 'ok',
        'query': {
            'mode': 'diagnosis',
            'level': session_ctx.get('scope_level'),
            'object_name': session_ctx.get('scope_object_name'),
            'period_current': session_ctx.get('period_current'),
            'period_previous': session_ctx.get('period_previous'),
            'query_type': query_type,
            'target_level': session_ctx.get('last_list_level'),
            'period': session_ctx.get('period_current'),
            'object': session_ctx.get('scope_object_name'),
            'full_view': True,
            'preserve_signal_flow': response_type == 'signal_flow',
        },
    }


def _extract_period_token(normalized: str) -> Optional[str]:
    m = re.search(r'\b(20\d{2})-(0[1-9]|1[0-2])\b', normalized)
    return f'{m.group(1)}-{m.group(2)}' if m else None


def _fallback_parse_direct_object(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_message(message)
    period = _extract_period_token(normalized) or session_ctx.get('period_current')
    if not period:
        return {'status': 'error', 'reason': 'period not recognized'}

    object_text = normalized.replace(period, ' ').strip()
    if not object_text:
        return {'status': 'error', 'reason': 'object not recognized'}

    object_text_norm = normalize_entity_text(object_text)
    if not object_text_norm:
        return {'status': 'error', 'reason': 'object not recognized'}

    dictionary = get_entity_dictionary(period)
    priority = ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']

    best = None
    object_tokens = set(object_text_norm.split())

    for level in priority:
        level_index = dictionary.get(level, {}).get('index', {})
        for alias, canonical in level_index.items():
            alias_norm = normalize_entity_text(alias)
            if not alias_norm:
                continue

            alias_tokens = set(alias_norm.split())
            exact = 1 if object_text_norm == alias_norm else 0
            whole = 1 if f' {alias_norm} ' in f' {object_text_norm} ' else 0
            partial = 1 if alias_norm in object_text_norm else 0
            overlap = len(object_tokens & alias_tokens)

            if not (exact or whole or partial or overlap):
                continue

            candidate = (exact, whole, overlap, len(alias_norm), level, canonical)
            if best is None or candidate > best:
                best = candidate

    if best is None:
        return {'status': 'error', 'reason': 'object not recognized'}

    return {
        'status': 'ok',
        'query': {
            'mode': 'diagnosis',
            'level': best[4],
            'object_name': best[5],
            'period_current': period,
            'period_previous': None,
            'query_type': 'summary',
            'period': period,
            'object': best[5],
        },
    }


def _route_signal_flow(query: Dict[str, Any], current: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    level = query.get('level')
    object_name = query.get('object_name')
    period = query.get('period_current')
    period_previous = query.get('period_previous')
    full_view = query.get('full_view', False)

    target_level = query.get('target_level') or DEFAULT_NEXT_LEVEL.get(level)
    if not target_level:
        response = ok_response(query, build_management_view(current))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, period_previous, 'diagnosis')
            update_session(session_id, {'last_response_type': 'management', 'last_list_level': None, 'full_view': False})
        return response

    source = _build_drill_from_scope(level, object_name, target_level, period, full_view=full_view)
    if 'error' in source:
        response = ok_response(query, build_management_view(current))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, period_previous, 'diagnosis')
            update_session(session_id, {'last_response_type': 'management', 'last_list_level': None, 'full_view': False})
        return response

    response = ok_response(query, build_signal_flow_view(current, source))
    if response.get('status') == 'ok':
        _store_scope(session_id, level, object_name, period, period_previous, 'diagnosis')
        _store_list_context(session_id, level, object_name, period, period_previous, 'diagnosis', target_level, response_type='signal_flow')
        update_session(session_id, {'full_view': full_view})
    return response


def _route_base_query(query: Dict[str, Any], session_id: str) -> Dict[str, Any]:
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

        response = ok_response(query, build_comparison_management_view(query, current, previous))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, previous_period, mode)
            update_session(session_id, {'last_response_type': 'comparison', 'last_list_level': None, 'full_view': False})
        return response

    if query.get('query_type') == 'reasons':
        response = ok_response(query, build_reasons_view(current))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, query.get('period_previous'), 'diagnosis')
            update_session(session_id, {'last_response_type': 'reasons', 'full_view': False})
        return response

    if query.get('query_type') == 'losses':
        target_level = DEFAULT_NEXT_LEVEL.get(level)
        if not target_level:
            return not_implemented_response(query, 'losses not supported for this level')

        source = _build_drill_from_scope(level, object_name, target_level, period, full_view=False)
        if 'error' in source:
            return error_response(source['error'], query)

        response = ok_response(query, build_losses_view_from_children(source))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, query.get('period_previous'), 'diagnosis')
            _store_list_context(session_id, level, object_name, period, query.get('period_previous'), 'diagnosis', target_level, response_type='losses')
            update_session(session_id, {'full_view': False})
        return response

    return _route_signal_flow(query, current, session_id)


def _route_drill_query(query: Dict[str, Any], session_ctx: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    scope_level = query.get('level') or session_ctx.get('scope_level')
    scope_object_name = query.get('object_name') or session_ctx.get('scope_object_name')
    period = query.get('period_current') or session_ctx.get('period_current')
    target_level = query.get('target_level') or DEFAULT_NEXT_LEVEL.get(scope_level)
    period_previous = query.get('period_previous') or session_ctx.get('period_previous')
    full_view = query.get('full_view', False)

    if not scope_level or not period:
        return {'status': 'error', 'reason': 'Нет активного объекта для анализа.'}
    if not target_level:
        return error_response('next drilldown level not available', query)

    payload = _build_drill_from_scope(scope_level, scope_object_name, target_level, period, full_view=full_view)
    if 'error' in payload:
        return error_response(payload['error'], query)

    response = ok_response(query, build_drilldown_management_view(payload))
    if response.get('status') == 'ok':
        _store_scope(session_id, scope_level, scope_object_name, period, period_previous, 'diagnosis')
        _store_list_context(session_id, scope_level, scope_object_name, period, period_previous, 'diagnosis', target_level, response_type='drill_down')
        update_session(session_id, {'full_view': full_view})
    return response


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    session_ctx = get_session(session_id)
    normalized = _normalize_message(message)

    if _is_short_command(normalized):
        parsed = _build_query_from_short_command(normalized, session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    elif _is_full_view_command(normalized):
        parsed = _build_query_from_full_view(session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    else:
        parsed = parse_query_intent(message)
        if parsed.get('status') != 'ok':
            parsed = _fallback_parse_direct_object(message, session_ctx)
            if parsed.get('status') != 'ok':
                return parsed

    query = parsed['query']
    query_type = query.get('query_type', 'summary')

    if query_type == 'drill_down':
        return _route_drill_query(query, session_ctx, session_id)

    return _route_base_query(query, session_id)
