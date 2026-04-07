from typing import Any, Dict, List, Optional

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
from app.query.parsing import normalize_user_message, parse_query_intent, resolve_period_from_message


SESSION_STORE: Dict[str, Dict[str, Any]] = {}
MAX_LAST_LIST_ITEMS = 100


def get_session(session_id: str) -> Dict[str, Any]:
    return SESSION_STORE.get(session_id, {})


def update_session(session_id: str, data: Dict[str, Any]) -> None:
    current = SESSION_STORE.get(session_id, {})
    current.update(data)
    SESSION_STORE[session_id] = current


def get_session_state(session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'level': session_ctx.get('scope_level'),
        'object_name': session_ctx.get('scope_object_name'),
        'period': session_ctx.get('period_current'),
        'period_previous': session_ctx.get('period_previous'),
        'last_list_level': session_ctx.get('last_list_level'),
        'last_response_type': session_ctx.get('last_response_type'),
        'full_view': bool(session_ctx.get('full_view', False)),
        'last_list_items': session_ctx.get('last_list_items') or [],
        'mode': session_ctx.get('mode'),
    }


def save_session_state(
    session_id: str,
    *,
    level: Optional[str] = None,
    object_name: Optional[str] = None,
    period: Optional[str] = None,
    period_previous: Any = None,
    last_list_level: Optional[str] = None,
    last_response_type: Optional[str] = None,
    full_view: Optional[bool] = None,
    last_list_items: Optional[List[Dict[str, Any]]] = None,
    mode: Optional[str] = None,
) -> None:
    payload: Dict[str, Any] = {}
    if level is not None:
        payload['scope_level'] = level
    if object_name is not None:
        payload['scope_object_name'] = object_name
    if period is not None:
        payload['period_current'] = period
    if period_previous is not None:
        payload['period_previous'] = period_previous
    if last_list_level is not None:
        payload['last_list_level'] = last_list_level
    if last_response_type is not None:
        payload['last_response_type'] = last_response_type
    if full_view is not None:
        payload['full_view'] = full_view
    if last_list_items is not None:
        payload['last_list_items'] = last_list_items
    if mode is not None:
        payload['mode'] = mode

    if payload:
        update_session(session_id, payload)


def clear_full_view_flag(session_id: str) -> None:
    update_session(session_id, {'full_view': False})


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


def _has_consistent_object_state(state: Dict[str, Any]) -> bool:
    return bool(state.get('level') and state.get('object_name') and state.get('period'))


def _is_summary_entry_query(query: Dict[str, Any]) -> bool:
    return (
        query.get('query_type') == 'summary'
        and bool(query.get('level'))
        and bool(query.get('period_current'))
        and (query.get('level') == 'business' or bool(query.get('object_name')))
    )


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


def _normalize_list_aliases(object_name: str) -> List[str]:
    normalized = normalize_entity_text(object_name)
    return [normalized] if normalized else []


def _build_last_list_items(items: List[Dict[str, Any]], level: Optional[str]) -> List[Dict[str, Any]]:
    if not level:
        return []

    prepared: List[Dict[str, Any]] = []
    for item in items:
        object_name = item.get('object_name')
        if not object_name:
            continue
        prepared.append({
            'object_name': object_name,
            'level': level,
            'normalized_name': normalize_entity_text(object_name),
            'aliases': _normalize_list_aliases(object_name),
        })
        if len(prepared) >= MAX_LAST_LIST_ITEMS:
            break
    return prepared


def _store_scope(session_id: str, level: str, object_name: str, period_current: str, period_previous: Any, mode: str) -> None:
    save_session_state(
        session_id,
        level=level,
        object_name=object_name,
        period=period_current,
        period_previous=period_previous,
        mode=mode,
    )


def _store_list_context(
    session_id: str,
    parent_level: str,
    parent_object_name: str,
    period_current: str,
    period_previous: Any,
    mode: str,
    list_level: str,
    response_type: str = 'drill_down',
    list_items: Optional[List[Dict[str, Any]]] = None,
    full_view: bool = False,
) -> None:
    save_session_state(
        session_id,
        level=parent_level,
        object_name=parent_object_name,
        period=period_current,
        period_previous=period_previous,
        mode=mode,
        last_list_level=list_level,
        last_response_type=response_type,
        last_list_items=list_items or [],
        full_view=full_view,
    )


def _build_query_from_short_command(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_message(message)

    if normalized not in SHORT_COMMAND_TARGETS:
        return {}

    state = get_session_state(session_ctx)
    if not _has_consistent_object_state(state):
        return {'status': 'error', 'reason': 'Нет активного объекта для выполнения команды.'}

    target = SHORT_COMMAND_TARGETS[normalized]

    if target in {'summary', 'reasons', 'losses'}:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': state.get('level'),
                'object_name': state.get('object_name'),
                'period_current': state.get('period'),
                'period_previous': state.get('period_previous'),
                'query_type': target,
                'period': state.get('period'),
                'object': state.get('object_name'),
            },
        }

    return {
        'status': 'ok',
        'query': {
            'mode': 'diagnosis',
            'level': state.get('level'),
            'object_name': state.get('object_name'),
            'period_current': state.get('period'),
            'period_previous': state.get('period_previous'),
            'query_type': 'drill_down',
            'target_level': target,
            'period': state.get('period'),
            'object': state.get('object_name'),
        },
    }


def _build_query_from_full_view(session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    state = get_session_state(session_ctx)
    if not state.get('object_name') or not state.get('period') or not state.get('last_list_level'):
        return {'status': 'error', 'reason': 'Нет данных для отображения.'}

    response_type = state.get('last_response_type', 'drill_down')
    query_type = 'summary' if response_type == 'signal_flow' else 'drill_down'

    return {
        'status': 'ok',
        'query': {
            'mode': 'diagnosis',
            'level': state.get('level'),
            'object_name': state.get('object_name'),
            'period_current': state.get('period'),
            'period_previous': state.get('period_previous'),
            'query_type': query_type,
            'target_level': state.get('last_list_level'),
            'period': state.get('period'),
            'object': state.get('object_name'),
            'full_view': True,
            'preserve_signal_flow': response_type == 'signal_flow',
        },
    }


def _resolve_period_for_entry(message: str, session_ctx: Dict[str, Any]) -> Optional[str]:
    period_current, _ = resolve_period_from_message(message)
    if period_current:
        return period_current
    state = get_session_state(session_ctx)
    if _has_consistent_object_state(state):
        return state.get('period')
    return None


def _match_against_last_active_list(message: str, state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    last_list_level = state.get('last_list_level')
    if not last_list_level:
        return None

    normalized_message = normalize_entity_text(message)
    if not normalized_message:
        return None

    matches = []
    for item in state.get('last_list_items') or []:
        if item.get('level') != last_list_level:
            continue
        object_name = item.get('object_name')
        normalized_name = item.get('normalized_name') or normalize_entity_text(object_name)
        aliases = item.get('aliases') or []
        candidates = {normalized_name, *[normalize_entity_text(alias) for alias in aliases]}
        if normalized_message in {candidate for candidate in candidates if candidate}:
            matches.append({'object_name': object_name, 'level': item.get('level')})

    return matches[0] if len(matches) == 1 else None


def _match_current_scope(message: str, state: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not _has_consistent_object_state(state):
        return None
    normalized_message = normalize_entity_text(message)
    object_name = state.get('object_name')
    if normalized_message and normalized_message == normalize_entity_text(object_name):
        return {'object_name': object_name, 'level': state.get('level')}
    return None


def _resolve_exact_object_in_dictionary(message: str, period: str) -> Optional[Dict[str, Any]]:
    normalized_message = normalize_entity_text(message)
    if not normalized_message:
        return None

    if normalized_message in {'бизнес', 'business', 'компания', 'весь бизнес'}:
        return {'level': 'business', 'object_name': 'business'}

    dictionary = get_entity_dictionary(period)
    priority = {'manager': 5, 'manager_top': 4, 'network': 3, 'category': 2, 'tmc_group': 1, 'sku': 0}

    message_tokens = set(normalized_message.split())
    padded = f' {normalized_message} '
    best: Optional[tuple] = None
    best_entity: Optional[Dict[str, Any]] = None
    seen_entities = set()

    for level, level_rank in priority.items():
        level_index = dictionary.get(level, {}).get('index', {})
        for alias, canonical in level_index.items():
            alias_norm = normalize_entity_text(alias)
            if not alias_norm:
                continue

            alias_tokens = set(alias_norm.split())
            exact = 1 if normalized_message == alias_norm else 0
            whole = 1 if f' {alias_norm} ' in padded else 0
            subset = 1 if message_tokens and message_tokens.issubset(alias_tokens) else 0
            overlap = len(message_tokens & alias_tokens)

            if not (exact or whole or subset or overlap):
                continue

            score = (exact, whole, subset, overlap, len(alias_tokens), len(alias_norm), level_rank)
            entity_key = (level, canonical)
            if best is None or score > best:
                best = score
                best_entity = {'level': level, 'object_name': canonical}
                seen_entities = {entity_key}
            elif score == best:
                seen_entities.add(entity_key)

    if best_entity is None:
        return None
    if len(seen_entities) > 1:
        return None
    return best_entity


def _resolve_direct_object_entry(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    state = get_session_state(session_ctx)
    period = _resolve_period_for_entry(message, session_ctx)
    if not period:
        return {'status': 'error', 'reason': 'period not recognized'}

    normalized = _normalize_message(message)

    # 1) exact object in message
    exact = _resolve_exact_object_in_dictionary(normalized, period)
    if exact:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': exact['level'],
                'object_name': exact['object_name'],
                'period_current': period,
                'period_previous': None,
                'query_type': 'summary',
                'period': period,
                'object': exact['object_name'],
            },
        }

    # 2) match in last active list
    last_list_match = _match_against_last_active_list(normalized, state)
    if last_list_match:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': last_list_match['level'],
                'object_name': last_list_match['object_name'],
                'period_current': period,
                'period_previous': None,
                'query_type': 'summary',
                'period': period,
                'object': last_list_match['object_name'],
            },
        }

    # 3) match current scope
    scope_match = _match_current_scope(normalized, state)
    if scope_match:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': scope_match['level'],
                'object_name': scope_match['object_name'],
                'period_current': period,
                'period_previous': None,
                'query_type': 'summary',
                'period': period,
                'object': scope_match['object_name'],
            },
        }

    # 4) global fallback
    return _fallback_parse_direct_object(message, session_ctx)


def _fallback_parse_direct_object(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_message(message)
    period, _ = resolve_period_from_message(message)
    if not period:
        state = get_session_state(session_ctx)
        if _has_consistent_object_state(state):
            period = state.get('period')
    if not period:
        return {'status': 'error', 'reason': 'period not recognized'}

    object_text = normalized.replace(period, ' ').strip()
    if not object_text:
        object_text = normalized
    if not object_text:
        return {'status': 'error', 'reason': 'object not recognized'}

    object_text_norm = normalize_entity_text(object_text)
    if not object_text_norm:
        return {'status': 'error', 'reason': 'object not recognized'}

    dictionary = get_entity_dictionary(period)
    priority = ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']

    best = None
    object_tokens = set(object_text_norm.split())
    priority_rank = {'manager': 5, 'manager_top': 4, 'network': 3, 'category': 2, 'tmc_group': 1, 'sku': 0}
    best_entities = set()

    for level in priority:
        level_index = dictionary.get(level, {}).get('index', {})
        for alias, canonical in level_index.items():
            alias_norm = normalize_entity_text(alias)
            if not alias_norm:
                continue

            alias_tokens = set(alias_norm.split())
            exact = 1 if object_text_norm == alias_norm else 0
            whole = 1 if f' {alias_norm} ' in f' {object_text_norm} ' else 0
            subset = 1 if object_tokens and object_tokens.issubset(alias_tokens) else 0
            partial = 1 if alias_norm in object_text_norm else 0
            overlap = len(object_tokens & alias_tokens)

            if not (exact or whole or subset or partial or overlap):
                continue

            candidate = (exact, whole, subset, overlap, len(alias_tokens), len(alias_norm), priority_rank.get(level, 0))
            entity_key = (level, canonical)
            if best is None or candidate > best:
                best = candidate
                best_entities = {entity_key}
            elif candidate == best:
                best_entities.add(entity_key)

    if best is None or len(best_entities) != 1:
        return {'status': 'error', 'reason': 'object not recognized'}

    level, canonical = next(iter(best_entities))
    return {
        'status': 'ok',
        'query': {
            'mode': 'diagnosis',
            'level': level,
            'object_name': canonical,
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
            save_session_state(
                session_id,
                last_response_type='management',
                last_list_level=None,
                last_list_items=[],
                full_view=False,
            )
        return response

    source = _build_drill_from_scope(level, object_name, target_level, period, full_view=full_view)
    if 'error' in source:
        response = ok_response(query, build_management_view(current))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, period_previous, 'diagnosis')
            save_session_state(
                session_id,
                last_response_type='management',
                last_list_level=None,
                last_list_items=[],
                full_view=False,
            )
        return response

    response = ok_response(query, build_signal_flow_view(current, source))
    if response.get('status') == 'ok':
        _store_scope(session_id, level, object_name, period, period_previous, 'diagnosis')
        list_items = _build_last_list_items(response['data'].get('items', []), target_level)
        _store_list_context(
            session_id,
            level,
            object_name,
            period,
            period_previous,
            'diagnosis',
            target_level,
            response_type='signal_flow',
            list_items=list_items,
            full_view=full_view,
        )
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
            save_session_state(session_id, last_response_type='comparison', last_list_level=None, last_list_items=[], full_view=False)
        return response

    if query.get('query_type') == 'reasons':
        response = ok_response(query, build_reasons_view(current))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, query.get('period_previous'), 'diagnosis')
            save_session_state(session_id, last_response_type='reasons', full_view=False)
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
            list_items = _build_last_list_items(response['data'].get('losses', []), target_level)
            _store_list_context(
                session_id,
                level,
                object_name,
                period,
                query.get('period_previous'),
                'diagnosis',
                target_level,
                response_type='losses',
                list_items=list_items,
                full_view=False,
            )
        return response

    return _route_signal_flow(query, current, session_id)


def _route_drill_query(query: Dict[str, Any], session_ctx: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    state = get_session_state(session_ctx)
    scope_level = query.get('level') or state.get('level')
    scope_object_name = query.get('object_name') or state.get('object_name')
    period = query.get('period_current') or state.get('period')
    target_level = query.get('target_level') or DEFAULT_NEXT_LEVEL.get(scope_level)
    period_previous = query.get('period_previous') or state.get('period_previous')
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
        list_items = _build_last_list_items(response['data'].get('items', []), target_level)
        _store_list_context(
            session_id,
            scope_level,
            scope_object_name,
            period,
            period_previous,
            'diagnosis',
            target_level,
            response_type='drill_down',
            list_items=list_items,
            full_view=full_view,
        )
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
        clear_full_view_flag(session_id)
        parsed = _resolve_direct_object_entry(message, session_ctx)
        if parsed.get('status') != 'ok':
            parsed = parse_query_intent(message)
            if parsed.get('status') != 'ok':
                parsed = _fallback_parse_direct_object(message, session_ctx)
                if parsed.get('status') != 'ok':
                    return parsed

    query = parsed['query']
    query_type = query.get('query_type', 'summary')

    if query_type == 'drill_down':
        return _route_drill_query(query, session_ctx, session_id)

    if _is_summary_entry_query(query):
        return _route_base_query(query, session_id)

    return _route_base_query(query, session_id)
