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
    build_object_view,
    build_reasons_view,
    build_list_view,
)
from app.query.entity_dictionary import get_entity_dictionary, normalize_entity_text
from app.domain.filters import get_normalized_rows
from app.query.parsing import normalize_user_message, parse_query_intent, resolve_period_from_message


import json
import os
from threading import Lock

SESSION_STORE: Dict[str, Dict[str, Any]] = {}
SESSION_FILE = '/tmp/vectra_session_store.json'
SESSION_LOCK = Lock()
MAX_LAST_LIST_ITEMS = 100

MANAGEMENT_FLOW_NEXT_LEVEL = {
    'business': 'manager_top',
    'manager_top': 'manager',
    'manager': 'network',
    'network': 'sku',
}

FULL_VIEW_NEXT_LEVEL = {
    'business': 'manager_top',
    'manager_top': 'manager',
    'manager': 'network',
    'network': 'sku',
}


def _read_persistent_sessions() -> Dict[str, Dict[str, Any]]:
    if not os.path.exists(SESSION_FILE):
        return {}
    try:
        with open(SESSION_FILE, 'r', encoding='utf-8') as fh:
            payload = json.load(fh)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _write_persistent_sessions(payload: Dict[str, Dict[str, Any]]) -> None:
    tmp_path = SESSION_FILE + '.tmp'
    with open(tmp_path, 'w', encoding='utf-8') as fh:
        json.dump(payload, fh, ensure_ascii=False)
    os.replace(tmp_path, SESSION_FILE)


def get_session(session_id: str) -> Dict[str, Any]:
    with SESSION_LOCK:
        if session_id in SESSION_STORE:
            return dict(SESSION_STORE.get(session_id, {}))
        persisted = _read_persistent_sessions()
        session = persisted.get(session_id, {})
        SESSION_STORE[session_id] = dict(session)
        return dict(session)


def update_session(session_id: str, data: Dict[str, Any]) -> None:
    with SESSION_LOCK:
        persisted = _read_persistent_sessions()
        current = dict(persisted.get(session_id, {}))
        current.update(data)
        persisted[session_id] = current
        SESSION_STORE[session_id] = dict(current)
        _write_persistent_sessions(persisted)


def get_session_state(session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'entry_role': session_ctx.get('entry_role') or 'ceo',
        'entry_object_name': session_ctx.get('entry_object_name') or 'business',
        'mode': session_ctx.get('mode') or 'management',
        'view_mode': session_ctx.get('view_mode') or ('all' if bool(session_ctx.get('full_view', False)) else 'drain'),
        'level': session_ctx.get('scope_level'),
        'object_name': session_ctx.get('scope_object_name'),
        'period': session_ctx.get('period_current'),
        'period_previous': session_ctx.get('period_previous'),
        'filter': session_ctx.get('filter') or {},
        'last_list_level': session_ctx.get('last_list_level'),
        'last_response_type': session_ctx.get('last_response_type'),
        'full_view': bool(session_ctx.get('full_view', False)),
        'last_list_items': session_ctx.get('last_list_items') or [],
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
    entry_role: Optional[str] = None,
    entry_object_name: Optional[str] = None,
    view_mode: Optional[str] = None,
    filter_payload: Optional[Dict[str, Any]] = None,
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
    if entry_role is not None:
        payload['entry_role'] = entry_role
    if entry_object_name is not None:
        payload['entry_object_name'] = entry_object_name
    if view_mode is not None:
        payload['view_mode'] = view_mode
        payload['full_view'] = view_mode == 'all'
    if filter_payload is not None:
        payload['filter'] = filter_payload

    if payload:
        update_session(session_id, payload)


def clear_full_view_flag(session_id: str) -> None:
    update_session(session_id, {'full_view': False})


def save_last_payload(session_id: str, payload: Dict[str, Any]) -> None:
    if payload:
        update_session(session_id, {'last_payload': payload})


def _extract_state_from_last_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    data = payload.get('data') or payload
    if not isinstance(data, dict):
        return {}

    level = data.get('level')
    object_name = data.get('object_name')
    period = data.get('period') or data.get('period_current')
    last_list_level = data.get('next_level') or data.get('children_level')
    items = data.get('items') or data.get('losses') or []
    response_type = data.get('mode') or 'management'
    return {
        'level': level,
        'object_name': object_name,
        'period': period,
        'last_list_level': last_list_level,
        'last_response_type': response_type,
        'last_list_items': _build_last_list_items(items, last_list_level),
    }


def hydrate_session_from_last_payload(session_id: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    state = get_session_state(session_ctx)
    if _has_consistent_object_state(state) and (state.get('last_list_items') or state.get('last_list_level')):
        return get_session(session_id)

    last_payload = session_ctx.get('last_payload')
    restored = _extract_state_from_last_payload(last_payload)
    if not restored.get('level') or not restored.get('object_name') or not restored.get('period'):
        return session_ctx

    save_session_state(
        session_id,
        level=restored.get('level'),
        object_name=restored.get('object_name'),
        period=restored.get('period'),
        last_list_level=restored.get('last_list_level'),
        last_response_type=restored.get('last_response_type'),
        last_list_items=restored.get('last_list_items') or [],
        full_view=False,
    )
    return get_session(session_id)


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
    'все причины': 'reasons',
    'все причина': 'reasons',
    'причины все': 'reasons',
    'разбор': 'reasons',
    'почему': 'reasons',
    'потери': 'losses',
    'сигнал': 'summary',
}
FULL_VIEW_COMMANDS = {'покажи все', 'все', 'full'}
ENTRY_START_COMMANDS = {'начать анализ', 'старт', 'start'}
ENTRY_LEVEL_CHOICES = {'1': 'business', '2': 'manager_top', '3': 'manager', '4': 'analytics'}


def _get_latest_available_period() -> Optional[str]:
    try:
        periods = sorted({str(row.get('period')) for row in get_normalized_rows() if row.get('period')})
        return periods[-1] if periods else None
    except Exception:
        return None


def _extract_direct_entry_hints(text: str) -> tuple[str, str, Optional[str]]:
    working = text
    query_type = 'summary'
    target_level = None

    if f' потери ' in f' {working} ':
        query_type = 'losses'
        working = working.replace('потери', ' ').strip()
    elif f' причины ' in f' {working} ':
        query_type = 'reasons'
        working = working.replace('причины', ' ').strip()

    drill_aliases = [
        'топ менеджеры', 'топ менеджер', 'топ-менеджеры', 'топ-менеджер',
        'менеджеры', 'менеджер', 'сети', 'сеть', 'категории', 'категория',
        'группы тмц', 'группа тмц', 'группы', 'группа', 'товары', 'товар', 'sku', 'скю'
    ]
    for alias in sorted(drill_aliases, key=len, reverse=True):
        token = f' {alias} '
        if token in f' {working} ':
            target_level = SHORT_COMMAND_TARGETS.get(alias)
            working = f' {working} '.replace(token, ' ').strip()
            break

    for prefix in ['топ менеджер', 'топ менеджеры', 'топ-менеджер', 'топ-менеджеры', 'менеджер', 'менеджеры', 'категория', 'категории', 'группа тмц', 'группы тмц', 'группа', 'группы', 'сеть', 'сети', 'товар', 'товары', 'sku', 'скю']:
        if working.startswith(prefix + ' '):
            working = working[len(prefix):].strip()
            break

    if target_level and query_type == 'summary':
        query_type = 'drill_down'

    return working, query_type, target_level


def _build_query_from_numeric_selection(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    if not message.isdigit():
        return {}
    state = get_session_state(session_ctx)
    items = state.get('last_list_items') or []
    if not items:
        restored = _extract_state_from_last_payload(session_ctx.get('last_payload') or {})
        items = restored.get('last_list_items') or []
        if restored.get('period') and not state.get('period'):
            state = {**state, **restored}
    index = int(message) - 1
    if 0 <= index < len(items):
        selected = items[index]
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': selected.get('level'),
                'object_name': selected.get('object_name'),
                'period_current': state.get('period'),
                'period_previous': state.get('period_previous'),
                'query_type': 'summary',
                'period': state.get('period'),
                'object': selected.get('object_name'),
            },
        }
    return {'status': 'error', 'reason': 'Нет активного списка для выбора.'}


def _handle_entry_flow(message: str, session_id: str, session_ctx: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    normalized = _normalize_message(message)
    if normalized in ENTRY_START_COMMANDS:
        return _build_entry_start_response(session_id)

    entry_step = session_ctx.get('entry_step')
    if entry_step == 'await_level' and normalized in ENTRY_LEVEL_CHOICES:
        return _build_entry_period_response(session_id, ENTRY_LEVEL_CHOICES[normalized])

    if entry_step == 'await_period':
        period_current, _ = resolve_period_from_message(message)
        if not period_current:
            return {'status': 'error', 'reason': 'period not recognized'}
        update_session(session_id, {'entry_step': None})
        level = session_ctx.get('entry_level') or 'business'
        object_name = 'business' if level == 'business' else session_ctx.get('scope_object_name')
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': level,
                'object_name': object_name,
                'period_current': period_current,
                'period_previous': None,
                'query_type': 'summary',
                'period': period_current,
                'object': object_name,
            },
        }

    return None

DEFAULT_NEXT_LEVEL = dict(MANAGEMENT_FLOW_NEXT_LEVEL)

SUMMARY_EXECUTORS = {
    'business': lambda obj, p, fp=None: get_business_comparison(period=p),
    'manager_top': lambda obj, p, fp=None: get_manager_top_comparison(manager_top=obj, period=p, filter_payload=fp),
    'manager': lambda obj, p, fp=None: get_manager_comparison(manager=obj, period=p, filter_payload=fp),
    'network': lambda obj, p, fp=None: get_network_comparison(network=obj, period=p, filter_payload=fp),
    'category': lambda obj, p, fp=None: get_category_comparison(category=obj, period=p, filter_payload=fp),
    'tmc_group': lambda obj, p, fp=None: get_tmc_group_comparison(tmc_group=obj, period=p, filter_payload=fp),
    'sku': lambda obj, p, fp=None: get_sku_comparison(sku=obj, period=p, filter_payload=fp),
}






def _summary_filter_for_query(level: str, object_name: Optional[str], period: str, session_ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    state = get_session_state(session_ctx or {}) if session_ctx is not None else {}
    base_filter = dict(state.get('filter') or {})
    base_filter['period'] = period
    if level and object_name and level != 'business':
        base_filter[level] = object_name
    return base_filter


def _sku_has_parent_filter(filter_payload: Dict[str, Any]) -> bool:
    return bool(filter_payload.get('network'))


def _execute_summary(level: str, object_name: Optional[str], period: str, session_ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    executor = SUMMARY_EXECUTORS.get(level)
    if executor is None:
        return {'error': 'base query not supported'}
    filter_payload = _summary_filter_for_query(level, object_name, period, session_ctx)
    if level == 'sku' and not _sku_has_parent_filter(filter_payload):
        return {'error': 'no data after filtering'}
    return executor(object_name, period, filter_payload)

def _previous_year_period(period: Optional[str]) -> Optional[str]:
    if not period or not isinstance(period, str):
        return None
    if len(period) == 7 and period[4] == '-':
        try:
            return f"{int(period[:4]) - 1:04d}-{period[5:7]}"
        except Exception:
            return None
    return None


def _with_previous_metrics(payload: Dict[str, Any], previous_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    enriched = dict(payload)
    if previous_payload and isinstance(previous_payload, dict):
        previous_metrics = ((previous_payload.get('metrics') or {}).get('object_metrics') or {})
        enriched['previous_object_metrics'] = previous_metrics
    return enriched

def _normalize_message(message: str) -> str:
    return normalize_user_message(message)


def _is_short_command(message: str) -> bool:
    return _normalize_message(message) in SHORT_COMMAND_TARGETS


def _is_full_view_command(message: str) -> bool:
    return _normalize_message(message) in FULL_VIEW_COMMANDS


def _has_consistent_object_state(state: Dict[str, Any]) -> bool:
    return bool(state.get('level') and state.get('object_name') and state.get('period'))




def _strict_next_level(level: Optional[str]) -> Optional[str]:
    return FULL_VIEW_NEXT_LEVEL.get(level or '')

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
    filter_payload: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    if scope_level == 'business':
        if target_level == 'manager_top':
            return get_business_manager_tops_comparison(period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'manager':
            return get_business_managers_comparison(period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'network':
            return get_business_networks_comparison(period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'category':
            return get_business_categories_comparison(period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'tmc_group':
            return get_business_tmc_groups_comparison(period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'sku':
            return get_business_skus_comparison(period=period, full_view=full_view, filter_payload=filter_payload)

    if scope_level == 'manager_top' and scope_object_name:
        if target_level == 'manager':
            return get_manager_top_managers_comparison(manager_top=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)

    if scope_level == 'manager' and scope_object_name:
        if target_level == 'network':
            return get_manager_networks_comparison(manager=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'category':
            return get_manager_categories_comparison(manager=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'sku':
            return get_manager_skus_comparison(manager=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)

    if scope_level == 'network' and scope_object_name:
        if target_level == 'category':
            return get_network_categories_comparison(network=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'tmc_group':
            return get_network_tmc_groups_comparison(network=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'sku':
            return get_network_skus_comparison(network=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)

    if scope_level == 'category' and scope_object_name:
        if target_level == 'tmc_group':
            return get_category_tmc_groups_comparison(category=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)
        if target_level == 'sku':
            return get_category_skus_comparison(category=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)

    if scope_level == 'tmc_group' and scope_object_name:
        if target_level == 'sku':
            return get_tmc_group_skus_comparison(tmc_group=scope_object_name, period=period, full_view=full_view, filter_payload=filter_payload)

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


def _build_filter_from_scope(level: str, object_name: Optional[str], period_current: str, existing_filter: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = dict(existing_filter or {})
    payload['period'] = period_current
    if level and object_name and level != 'business':
        payload[level] = object_name
    return payload


def _entry_role_for_level(level: str) -> str:
    return {'business': 'ceo', 'manager_top': 'manager_top', 'manager': 'manager', 'network': 'manager', 'sku': 'manager'}.get(level, 'ceo')


def _store_scope(session_id: str, level: str, object_name: str, period_current: str, period_previous: Any, mode: str) -> None:
    existing_filter = (get_session(session_id).get('filter') or {})
    filter_payload = _build_filter_from_scope(level, object_name, period_current, existing_filter)
    save_session_state(
        session_id,
        level=level,
        object_name=object_name,
        period=period_current,
        period_previous=period_previous,
        mode='management' if mode == 'diagnosis' else mode,
        entry_role=_entry_role_for_level(level),
        entry_object_name=object_name,
        view_mode='drain',
        filter_payload=filter_payload,
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
    existing_filter = (get_session(session_id).get('filter') or {})
    filter_payload = _build_filter_from_scope(parent_level, parent_object_name, period_current, existing_filter)
    save_session_state(
        session_id,
        level=parent_level,
        object_name=parent_object_name,
        period=period_current,
        period_previous=period_previous,
        mode='management' if mode == 'diagnosis' else mode,
        entry_role=_entry_role_for_level(parent_level),
        entry_object_name=parent_object_name,
        view_mode='all' if full_view else 'drain',
        filter_payload=filter_payload,
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
    target = SHORT_COMMAND_TARGETS[normalized]

    if target in {'summary', 'reasons', 'losses'}:
        if not _has_consistent_object_state(state):
            return {'status': 'error', 'reason': 'Нет активного объекта для выполнения команды.'}
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

    if not _has_consistent_object_state(state):
        period = _get_latest_available_period()
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': 'business',
                'object_name': 'business',
                'period_current': period,
                'period_previous': None,
                'query_type': 'drill_down',
                'target_level': target,
                'period': period,
                'object': 'business',
                'list_mode': True,
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
            'list_mode': True,
        },
    }


def _build_query_from_full_view(session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    state = get_session_state(session_ctx)
    if not state.get('object_name') or not state.get('period') or not state.get('level'):
        return {'status': 'error', 'reason': 'Нет данных для отображения.'}

    response_type = state.get('last_response_type', 'drill_down')
    query_type = 'drill_down'
    target_level = _strict_next_level(state.get('level'))
    if not target_level:
        return {'status': 'error', 'reason': 'Нет данных для отображения.'}

    return {
        'status': 'ok',
        'query': {
            'mode': 'diagnosis',
            'level': state.get('level'),
            'object_name': state.get('object_name'),
            'period_current': state.get('period'),
            'period_previous': state.get('period_previous'),
            'query_type': query_type,
            'target_level': target_level,
            'period': state.get('period'),
            'object': state.get('object_name'),
            'full_view': True,
            'preserve_signal_flow': response_type == 'signal_flow',
            'list_mode': True,
        },
    }


def _resolve_period_for_entry(message: str, session_ctx: Dict[str, Any]) -> Optional[str]:
    period_current, _ = resolve_period_from_message(message)
    if period_current:
        return period_current

    state = get_session_state(session_ctx)
    if state.get('period'):
        return state.get('period')

    return _get_latest_available_period()


def _strip_period_text(message: str) -> str:
    period_current, period_previous = resolve_period_from_message(message)
    cleaned = _normalize_message(message)
    for token in [period_current, period_previous]:
        if token:
            cleaned = cleaned.replace(token, ' ')
            if ':' in token:
                for part in token.split(':'):
                    cleaned = cleaned.replace(part, ' ')
    return _normalize_message(cleaned)


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
    normalized_message = normalize_entity_text(_strip_period_text(message))
    if not normalized_message:
        return None

    if normalized_message in {'бизнес', 'business', 'компания', 'весь бизнес'}:
        return {'level': 'business', 'object_name': 'business'}

    dictionary = get_entity_dictionary(period)
    priority = ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']

    matches: List[Dict[str, Any]] = []
    padded = f' {normalized_message} '
    for level in priority:
        level_index = dictionary.get(level, {}).get('index', {})
        for alias, canonical in level_index.items():
            alias_norm = normalize_entity_text(alias)
            if not alias_norm:
                continue
            if normalized_message == alias_norm or f' {alias_norm} ' in padded:
                matches.append({'level': level, 'object_name': canonical, 'alias': alias_norm})

    unique = {(item['level'], item['object_name']) for item in matches}
    if len(unique) != 1:
        return None
    only = matches[0]
    return {'level': only['level'], 'object_name': only['object_name']}


def _resolve_direct_object_entry(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    state = get_session_state(session_ctx)
    explicit_period, _ = resolve_period_from_message(message)
    period = _resolve_period_for_entry(message, session_ctx)
    if not period:
        return {'status': 'error', 'reason': 'period not recognized'}

    normalized = _normalize_message(message)
    normalized_without_period = _strip_period_text(normalized)
    normalized_without_period, query_type, target_level = _extract_direct_entry_hints(normalized_without_period)
    normalized_object = normalize_entity_text(normalized_without_period.replace(':', ' ').strip())
    if normalized_object in {'бизнес', 'business', 'компания', 'весь бизнес'} or normalized_object.startswith('бизнес '):
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': 'business',
                'object_name': 'business',
                'period_current': period,
                'period_previous': None,
                'query_type': query_type,
                'period': period,
                'object': 'business',
                'target_level': DEFAULT_NEXT_LEVEL.get('business') if query_type == 'drill_down' else None,
            },
        }

    only_period = normalized_object == ''
    if not explicit_period and not only_period:
        return {'status': 'error', 'reason': 'period not recognized'}

    if only_period:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': 'business',
                'object_name': 'business',
                'period_current': period,
                'period_previous': None,
                'query_type': query_type,
                'period': period,
                'object': 'business',
                'target_level': DEFAULT_NEXT_LEVEL.get('business') if query_type == 'drill_down' else None,
            },
        }

    # 1) exact object in message
    exact = _resolve_exact_object_in_dictionary(normalized_without_period or normalized, period)
    if exact:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': exact['level'],
                'object_name': exact['object_name'],
                'period_current': period,
                'period_previous': None,
                'query_type': query_type,
                'period': period,
                'object': exact['object_name'],
                'target_level': DEFAULT_NEXT_LEVEL.get(exact['level']) if query_type == 'drill_down' else None,
            },
        }

    # 2) match in last active list
    last_list_match = _match_against_last_active_list(normalized_without_period or normalized, state)
    if last_list_match:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': last_list_match['level'],
                'object_name': last_list_match['object_name'],
                'period_current': period,
                'period_previous': None,
                'query_type': query_type,
                'period': period,
                'object': last_list_match['object_name'],
                'target_level': DEFAULT_NEXT_LEVEL.get(last_list_match['level']) if query_type == 'drill_down' else None,
            },
        }

    # 3) match current scope
    scope_match = _match_current_scope(normalized_without_period or normalized, state)
    if scope_match:
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': scope_match['level'],
                'object_name': scope_match['object_name'],
                'period_current': period,
                'period_previous': None,
                'query_type': query_type,
                'period': period,
                'object': scope_match['object_name'],
                'target_level': DEFAULT_NEXT_LEVEL.get(scope_match['level']) if query_type == 'drill_down' else None,
            },
        }

    # 4) global fallback
    return _fallback_parse_direct_object(message, session_ctx)


def _fallback_parse_direct_object(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_message(message)
    period, _ = resolve_period_from_message(message)
    if not period:
        return {'status': 'error', 'reason': 'period not recognized'}

    query_type = 'summary'
    object_text = _strip_period_text(normalized).strip()
    if f' потери ' in f' {object_text} ':
        query_type = 'losses'
        object_text = object_text.replace('потери', ' ').strip()
    elif f' причины ' in f' {object_text} ':
        query_type = 'reasons'
        object_text = object_text.replace('причины', ' ').strip()

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
            'query_type': query_type,
            'period': period,
            'object': best[5],
        },
    }


def sanitize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return {
        'summary': payload.get('summary'),
        'items': payload.get('items'),
        'reasons': payload.get('reasons'),
        'object_name': payload.get('object_name'),
        'level': payload.get('level'),
        'period': payload.get('period'),
        'metrics': payload.get('metrics'),
        'children_level': payload.get('children_level'),
        'consistency': payload.get('consistency'),
        'delta': payload.get('delta'),
        'delta_percent': payload.get('delta_percent'),
        'signal': payload.get('signal'),
        'navigation': payload.get('navigation'),
        'context': payload.get('context'),
        'diagnosis': payload.get('diagnosis'),
        'impact': payload.get('impact'),
        'priority': payload.get('priority'),
        'action': payload.get('action'),
        'previous_object_metrics': payload.get('previous_object_metrics'),
    }


def enforce_contract(response: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(response, dict):
        return {'status': 'error', 'reason': 'invalid response'}
    if response.get('status') != 'ok':
        return response
    data = response.get('data')
    if not isinstance(data, dict):
        return {'status': 'error', 'reason': 'invalid response data'}
    response_type = data.get('type')
    if response_type not in {'object', 'management', 'management_list', 'reasons', 'comparison', 'losses'}:
        return {'status': 'error', 'reason': 'invalid response type'}
    if response_type in {'management', 'management_list'} and ('metrics' not in data or 'commands' not in data):
        return {'status': 'error', 'reason': 'invalid management structure'}
    if response_type == 'reasons' and 'reasons' not in data:
        return {'status': 'error', 'reason': 'invalid reasons structure'}
    return response


def _is_valid_signal_response(response: Dict[str, Any]) -> bool:
    data = response.get('data') or {}
    if response.get('status') != 'ok':
        return False
    if data.get('mode') != 'signal':
        return True
    summary = data.get('summary') or {}
    top_summary = data.get('top_summary') or {}
    return bool(summary and data.get('items') is not None and top_summary.get('top_items') is not None)


def _route_signal_flow(query: Dict[str, Any], current: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    level = query.get('level')
    object_name = query.get('object_name')
    period = query.get('period_current')
    period_previous = query.get('period_previous')
    full_view = query.get('full_view', False)

    target_level = query.get('target_level') or DEFAULT_NEXT_LEVEL.get(level)
    drain_payload = None
    if target_level:
        source = _build_drill_from_scope(level, object_name, target_level, period, full_view=full_view, filter_payload=(get_session(session_id).get('filter') or {}))
        if 'error' not in source:
            drain_payload = source

    current = _with_previous_metrics(current, _execute_summary(level, object_name, _previous_year_period(period), get_session(session_id)) if _previous_year_period(period) and SUMMARY_EXECUTORS.get(level) else None)
    response = ok_response(query, build_object_view(sanitize_payload(current), sanitize_payload(drain_payload) if drain_payload is not None else None))
    if response.get('status') == 'ok':
        _store_scope(session_id, level, object_name, period, period_previous, 'diagnosis')
        if drain_payload is not None:
            list_items = _build_last_list_items(drain_payload.get('all_items') or drain_payload.get('items', []), target_level)
            _store_list_context(
                session_id,
                level,
                object_name,
                period,
                period_previous,
                'diagnosis',
                target_level,
                response_type='object',
                list_items=list_items,
                full_view=full_view,
            )
        else:
            save_session_state(
                session_id,
                last_response_type='object',
                last_list_level=None,
                last_list_items=[],
                full_view=False,
            )
        save_last_payload(session_id, response)
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

    current = _execute_summary(level, object_name, period, get_session(session_id))
    if 'error' in current:
        return error_response(current['error'], query)
    if level == 'business':
        current = dict(current)
        current['object_name'] = 'Бизнес'
        current['level'] = 'business'

    previous_same_period = None
    previous_same_period_key = _previous_year_period(period)
    if previous_same_period_key:
        try:
            previous_same_period = _execute_summary(level, object_name, previous_same_period_key, get_session(session_id))
            if 'error' in previous_same_period:
                previous_same_period = None
        except Exception:
            previous_same_period = None
    current = _with_previous_metrics(current, previous_same_period)

    if mode == 'comparison':
        previous_period = query.get('period_previous')
        if not previous_period:
            return error_response('comparison period not recognized', query)
        if len(str(period)) == 4 or len(str(previous_period)) == 4:
            return error_response('comparison period not recognized', query)

        previous = _execute_summary(level, object_name, previous_period, get_session(session_id))
        if 'error' in previous:
            return error_response(previous['error'], query)

        response = ok_response(query, build_comparison_management_view(query, sanitize_payload(current), sanitize_payload(previous)))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, previous_period, mode)
            save_session_state(session_id, last_response_type='comparison', last_list_level=None, last_list_items=[], full_view=False)
            save_last_payload(session_id, response)
        return response

    explicit_target = query.get('target_level')
    if mode != 'comparison' and query.get('query_type') == 'summary' and explicit_target:
        drill_query = dict(query)
        drill_query['query_type'] = 'drill_down'
        drill_query['target_level'] = DEFAULT_NEXT_LEVEL.get(level) or explicit_target
        return _route_drill_query(drill_query, get_session(session_id), session_id)

    if query.get('query_type') == 'reasons':
        state = get_session_state(get_session(session_id))
        resolved_object_name = object_name or state.get('object_name')
        resolved_period = period or state.get('period')
        if not resolved_object_name or not resolved_period:
            return ok_response(query, {'type': 'reasons', 'object': None, 'error': 'no context', 'decomposition': []})
        response = ok_response(query, build_reasons_view(sanitize_payload(current)))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, resolved_object_name, resolved_period, query.get('period_previous'), 'diagnosis')
            save_session_state(session_id, last_response_type='reasons', full_view=False)
            save_last_payload(session_id, response)
        return response

    if query.get('query_type') == 'losses':
        target_level = DEFAULT_NEXT_LEVEL.get(level)
        if not target_level:
            return not_implemented_response(query, 'losses not supported for this level')

        source = _build_drill_from_scope(level, object_name, target_level, period, full_view=False, filter_payload=(get_session(session_id).get('filter') or {}))
        if 'error' in source:
            return error_response(source['error'], query)

        response = ok_response(query, build_losses_view_from_children(sanitize_payload(source)))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, query.get('period_previous'), 'diagnosis')
            list_items = _build_last_list_items(source.get('all_items') or source.get('items', []), target_level)
            _store_list_context(
                session_id,
                level,
                object_name,
                period,
                query.get('period_previous'),
                'diagnosis',
                target_level,
                response_type='object',
                list_items=list_items,
                full_view=False,
            )
            save_last_payload(session_id, response)
        return response

    return _route_signal_flow(query, current, session_id)


def _route_drill_query(query: Dict[str, Any], session_ctx: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    state = get_session_state(session_ctx)
    scope_level = query.get('level') or state.get('level')
    scope_object_name = query.get('object_name') or state.get('object_name')
    period = query.get('period_current') or state.get('period')
    target_level = query.get('target_level') or DEFAULT_NEXT_LEVEL.get(scope_level)
    if query.get('list_mode') or query.get('full_view'):
        target_level = _strict_next_level(scope_level)
    period_previous = query.get('period_previous') or state.get('period_previous')
    full_view = query.get('full_view', False)

    if not scope_level or not period:
        return {'status': 'error', 'reason': 'Нет активного объекта для анализа.'}
    if not target_level:
        return error_response('next drilldown level not available', query)

    payload = _build_drill_from_scope(scope_level, scope_object_name, target_level, period, full_view=full_view, filter_payload=(state.get('filter') or {}))
    if 'error' in payload:
        return error_response(payload['error'], query)

    summary_executor = SUMMARY_EXECUTORS.get(scope_level)
    if summary_executor is None:
        return not_implemented_response(query, 'base query not supported')
    current = _execute_summary(scope_level, scope_object_name, period, session_ctx)
    if 'error' in current:
        return error_response(current['error'], query)
    if scope_level == 'business':
        current = dict(current)
        current['object_name'] = 'Бизнес'
        current['level'] = 'business'

    current = _with_previous_metrics(current, _execute_summary(scope_level, scope_object_name, _previous_year_period(period), session_ctx) if _previous_year_period(period) and SUMMARY_EXECUTORS.get(scope_level) else None)
    if query.get('list_mode'):
        response = ok_response(query, build_list_view(sanitize_payload(current), sanitize_payload(payload)))
    else:
        response = ok_response(query, build_object_view(sanitize_payload(current), sanitize_payload(payload)))
    if response.get('status') == 'ok':
        _store_scope(session_id, scope_level, scope_object_name, period, period_previous, 'diagnosis')
        list_items = _build_last_list_items(payload.get('all_items') or payload.get('items', []), target_level)
        _store_list_context(
            session_id,
            scope_level,
            scope_object_name,
            period,
            period_previous,
            'diagnosis',
            target_level,
            response_type='object',
            list_items=list_items,
            full_view=full_view,
        )
        save_last_payload(session_id, response)
    return response


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    session_ctx = hydrate_session_from_last_payload(session_id, get_session(session_id))
    normalized = _normalize_message(message)

    if normalized in {'все причины', 'все причина', 'причины все'}:
        parsed = _build_query_from_short_command('причины', session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    elif normalized.isdigit():
        parsed = _build_query_from_numeric_selection(normalized, session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    elif _is_short_command(normalized):
        parsed = _build_query_from_short_command(normalized, session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    elif _is_full_view_command(normalized):
        parsed = _build_query_from_full_view(session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    else:
        clear_full_view_flag(session_id)
        parsed_by_intent = parse_query_intent(message)
        if parsed_by_intent.get('status') == 'ok' and (
            parsed_by_intent.get('query', {}).get('mode') == 'comparison'
            or parsed_by_intent.get('query', {}).get('query_type') in {'reasons', 'losses'}
        ):
            parsed = parsed_by_intent
        else:
            parsed = _resolve_direct_object_entry(message, session_ctx)
            if parsed.get('status') != 'ok':
                parsed = parsed_by_intent
                if parsed.get('status') != 'ok':
                    parsed = _fallback_parse_direct_object(message, session_ctx)
                    if parsed.get('status') != 'ok':
                        return parsed

    query = parsed['query']
    query_type = query.get('query_type', 'summary')

    if query_type == 'drill_down':
        return enforce_contract(_route_drill_query(query, get_session(session_id), session_id))

    return enforce_contract(_route_base_query(query, session_id))
