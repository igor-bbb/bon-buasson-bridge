from copy import deepcopy
import json
from pathlib import Path
from threading import Lock
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
from app.domain.filters import get_normalized_rows
from app.domain.summary import (
    get_business_summary,
    get_manager_top_summary,
    get_manager_summary,
    get_network_summary,
    get_category_summary,
    get_tmc_group_summary,
    get_sku_summary,
)
from app.presentation.contracts import error_response, not_implemented_response, ok_response
from app.presentation.views import (
    build_comparison_management_view,
    build_list_view,
    build_losses_view_from_children,
    build_object_view,
    build_reasons_view,
)
from app.query.entity_dictionary import normalize_entity_text
from app.query.parsing import normalize_user_message, parse_query_intent


SESSION_STORE: Dict[str, Dict[str, Any]] = {}
SESSION_LOCK = Lock()
SESSION_FILE = Path('/tmp/vectra_session_store.json')
MAX_LAST_LIST_ITEMS = 500

DEFAULT_NEXT_LEVEL = {
    'business': 'manager_top',
    'manager_top': 'manager',
    'manager': 'network',
    'network': 'sku',
}

FULL_VIEW_NEXT_LEVEL = dict(DEFAULT_NEXT_LEVEL)

SHORT_COMMAND_TARGETS = {
    'топы': 'manager_top',
    'дивизиональные менеджеры': 'manager_top',
    'дивизиональный менеджер': 'manager_top',
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
}

FULL_VIEW_COMMANDS = {'покажи все', 'все', 'полный список'}
BACK_COMMANDS = {'назад'}


def _blank_state() -> Dict[str, Any]:
    return {
        'scope_level': None,
        'scope_object_name': None,
        'period_current': None,
        'period_previous': None,
        'mode': 'management',
        'view_mode': 'drain',
        'filter': {},
        'last_list_level': None,
        'last_response_type': None,
        'last_list_items': [],
        'full_view': False,
        'last_payload': None,
        'stack': [],
    }



def _sanitize_session_payload(payload: Any) -> Dict[str, Any]:
    return payload if isinstance(payload, dict) else {}


def _load_session_store() -> Dict[str, Dict[str, Any]]:
    try:
        if not SESSION_FILE.exists():
            return {}
        raw = json.loads(SESSION_FILE.read_text(encoding='utf-8'))
        if not isinstance(raw, dict):
            return {}
        return {str(k): _sanitize_session_payload(v) for k, v in raw.items()}
    except Exception:
        return {}


def _persist_session_store() -> None:
    try:
        SESSION_FILE.write_text(json.dumps(SESSION_STORE, ensure_ascii=False), encoding='utf-8')
    except Exception:
        return None


def _hydrate_session_store() -> None:
    if SESSION_STORE:
        return
    SESSION_STORE.update(_load_session_store())


def _extract_list_items_from_response(response: Dict[str, Any], fallback_level: Optional[str]) -> List[Dict[str, Any]]:
    if not isinstance(response, dict):
        return []
    data = response.get('data') if isinstance(response.get('data'), dict) else response
    navigation = data.get('navigation') if isinstance(data, dict) else {}
    items = navigation.get('items') if isinstance(navigation, dict) else []
    next_level = navigation.get('next_level') if isinstance(navigation, dict) else None
    level = next_level or fallback_level
    if not isinstance(items, list) or not level:
        return []
    prepared = []
    for item in items:
        if not item:
            continue
        prepared.append({
            'object_name': item,
            'level': level,
            'normalized_name': normalize_entity_text(item),
        })
        if len(prepared) >= MAX_LAST_LIST_ITEMS:
            break
    return prepared


def get_session(session_id: str) -> Dict[str, Any]:
    with SESSION_LOCK:
        _hydrate_session_store()
        current = dict(_blank_state())
        current.update(SESSION_STORE.get(session_id, {}))
        return current


def update_session(session_id: str, data: Dict[str, Any]) -> None:
    with SESSION_LOCK:
        _hydrate_session_store()
        current = dict(_blank_state())
        current.update(SESSION_STORE.get(session_id, {}))
        current.update(data)
        SESSION_STORE[session_id] = current
        _persist_session_store()


def clear_full_view_flag(session_id: str) -> None:
    update_session(session_id, {'full_view': False})


def push_state(session_id: str) -> None:
    current = get_session(session_id)
    stack = list(current.get('stack') or [])
    snapshot = {
        'scope_level': current.get('scope_level'),
        'scope_object_name': current.get('scope_object_name'),
        'period_current': current.get('period_current'),
        'period_previous': current.get('period_previous'),
        'mode': current.get('mode'),
        'view_mode': current.get('view_mode'),
        'filter': deepcopy(current.get('filter') or {}),
        'last_list_level': current.get('last_list_level'),
        'last_response_type': current.get('last_response_type'),
        'last_list_items': deepcopy(current.get('last_list_items') or []),
        'full_view': bool(current.get('full_view', False)),
        'last_payload': deepcopy(current.get('last_payload')),
    }
    stack.append(snapshot)
    update_session(session_id, {'stack': stack})


def pop_state(session_id: str) -> Optional[Dict[str, Any]]:
    current = get_session(session_id)
    stack = list(current.get('stack') or [])
    if not stack:
        return None
    previous = stack.pop()
    previous['stack'] = stack
    with SESSION_LOCK:
        _hydrate_session_store()
        SESSION_STORE[session_id] = previous
        _persist_session_store()
    return previous


def get_session_state(session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return {
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
        'last_payload': session_ctx.get('last_payload'),
        'stack': session_ctx.get('stack') or [],
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
    if view_mode is not None:
        payload['view_mode'] = view_mode
    if filter_payload is not None:
        payload['filter'] = filter_payload
    if payload:
        update_session(session_id, payload)


def save_last_payload(session_id: str, payload: Dict[str, Any]) -> None:
    update_session(session_id, {'last_payload': payload})


def _get_latest_available_period() -> Optional[str]:
    try:
        periods = sorted({str(row.get('period')) for row in get_normalized_rows() if row.get('period')})
        return periods[-1] if periods else None
    except Exception:
        return None


def _build_filter_from_scope(level: str, object_name: Optional[str], period_current: str, existing_filter: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = dict(existing_filter or {})
    payload['period'] = period_current

    for key in ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']:
        if level == 'business':
            payload.pop(key, None)

    if level == 'manager_top':
        payload['manager_top'] = object_name
        payload.pop('manager', None)
        payload.pop('network', None)
        payload.pop('category', None)
        payload.pop('tmc_group', None)
        payload.pop('sku', None)

    if level == 'manager':
        payload['manager'] = object_name
        payload.pop('network', None)
        payload.pop('category', None)
        payload.pop('tmc_group', None)
        payload.pop('sku', None)

    if level == 'network':
        payload['network'] = object_name
        payload.pop('category', None)
        payload.pop('tmc_group', None)
        payload.pop('sku', None)

    if level == 'category':
        payload['category'] = object_name
        payload.pop('tmc_group', None)
        payload.pop('sku', None)

    if level == 'tmc_group':
        payload['tmc_group'] = object_name
        payload.pop('sku', None)

    if level == 'sku':
        payload['sku'] = object_name

    return payload


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
        })
        if len(prepared) >= MAX_LAST_LIST_ITEMS:
            break
    return prepared


def _store_scope(
    session_id: str,
    level: str,
    object_name: str,
    period_current: str,
    period_previous: Any,
    mode: str,
    existing_filter: Optional[Dict[str, Any]] = None,
    push_to_stack: bool = False,
) -> None:
    if push_to_stack:
        push_state(session_id)

    filter_payload = _build_filter_from_scope(level, object_name, period_current, existing_filter=existing_filter)
    save_session_state(
        session_id,
        level=level,
        object_name=object_name,
        period=period_current,
        period_previous=period_previous,
        mode='management' if mode == 'diagnosis' else mode,
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
    existing_filter: Optional[Dict[str, Any]] = None,
    push_to_stack: bool = False,
) -> None:
    if push_to_stack:
        push_state(session_id)

    filter_payload = _build_filter_from_scope(parent_level, parent_object_name, period_current, existing_filter=existing_filter)
    save_session_state(
        session_id,
        level=parent_level,
        object_name=parent_object_name,
        period=period_current,
        period_previous=period_previous,
        mode='management' if mode == 'diagnosis' else mode,
        view_mode='all' if full_view else 'drain',
        filter_payload=filter_payload,
        last_list_level=list_level,
        last_response_type=response_type,
        last_list_items=list_items or [],
        full_view=full_view,
    )


def sanitize_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return dict(payload)


def enforce_contract(response: Dict[str, Any]) -> Dict[str, Any]:
    """Return a clean summary contract for vectraQuery consumers.

    The integration layer expects the API to return the summary payload directly,
    not wrapped inside {status, query, data}. We still accept legacy wrapped
    responses internally, validate them, and unwrap successful ones.
    """
    if not isinstance(response, dict):
        return {'status': 'error', 'reason': 'invalid response'}

    standard_required = {'context', 'metrics', 'structure', 'drain_block', 'goal', 'navigation'}
    sku_required = {'context', 'metrics', 'structure', 'drain_block', 'goal', 'navigation'}

    # Already a direct summary contract.
    if standard_required.issubset(set(response.keys())) or sku_required.issubset(set(response.keys())):
        return response

    if response.get('status') != 'ok':
        return response

    data = response.get('data')
    if not isinstance(data, dict):
        return {'status': 'error', 'reason': 'invalid response data'}

    if standard_required.issubset(set(data.keys())) or sku_required.issubset(set(data.keys())):
        return data

    response_type = data.get('type')
    if response_type not in {'object', 'management', 'management_list', 'reasons', 'comparison', 'losses'}:
        return {'status': 'error', 'reason': 'invalid response type'}
    if response_type in {'management', 'management_list'} and ('metrics' not in data or 'commands' not in data):
        return {'status': 'error', 'reason': 'invalid management structure'}
    if response_type == 'reasons' and 'reasons' not in data:
        return {'status': 'error', 'reason': 'invalid reasons structure'}
    return data


SUMMARY_EXECUTORS = {
    'business': lambda obj, p, fp=None: get_business_summary(period=p),
    'manager_top': lambda obj, p, fp=None: get_manager_top_summary(manager_top=obj, period=p),
    'manager': lambda obj, p, fp=None: get_manager_summary(manager=obj, period=p),
    'network': lambda obj, p, fp=None: get_network_summary(network=obj, period=p),
    'category': lambda obj, p, fp=None: get_category_summary(category=obj, period=p),
    'tmc_group': lambda obj, p, fp=None: get_tmc_group_summary(tmc_group=obj, period=p),
    'sku': lambda obj, p, fp=None: get_sku_summary(sku=obj, period=p),
}


def _execute_summary(
    level: str,
    object_name: Optional[str],
    period: str,
    session_ctx: Optional[Dict[str, Any]] = None,
    explicit_filter: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    executor = SUMMARY_EXECUTORS.get(level)
    if executor is None:
        return {'error': 'base query not supported'}
    filter_payload = dict((session_ctx or {}).get('filter') or {})
    if explicit_filter:
        filter_payload.update({k: v for k, v in explicit_filter.items() if v is not None})
    filter_payload = _build_filter_from_scope(level, object_name, period, existing_filter=filter_payload)
    return executor(object_name, period, filter_payload)


def _previous_year_period(period: Optional[str]) -> Optional[str]:
    if not period or not isinstance(period, str):
        return None
    if ':' in period:
        start, end = period.split(':', 1)
        prev_start = _previous_year_period(start)
        prev_end = _previous_year_period(end)
        if prev_start and prev_end:
            return f'{prev_start}:{prev_end}'
        return None
    if len(period) == 7 and period[4] == '-':
        try:
            return f"{int(period[:4]) - 1:04d}-{period[5:7]}"
        except Exception:
            return None
    if len(period) == 4 and period.isdigit():
        return f"{int(period) - 1:04d}"
    return None


def _with_previous_metrics(payload: Dict[str, Any], previous_payload: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    enriched = dict(payload)
    if previous_payload and isinstance(previous_payload, dict):
        previous_metrics = ((previous_payload.get('metrics') or {}).get('object_metrics') or {})
        enriched['previous_object_metrics'] = previous_metrics
    return enriched


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


def _normalize_message(message: str) -> str:
    return normalize_user_message(message)


def _is_short_command(message: str) -> bool:
    return _normalize_message(message) in SHORT_COMMAND_TARGETS


def _is_full_view_command(message: str) -> bool:
    normalized = _normalize_message(message)
    if normalized in {'все причины', 'полные причины'}:
        return False
    return normalized in FULL_VIEW_COMMANDS


def _is_back_command(message: str) -> bool:
    return _normalize_message(message) in BACK_COMMANDS


def _is_full_reasons_command(message: str) -> bool:
    return _normalize_message(message) in {'все причины', 'полные причины'}


def _build_query_from_short_command(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    normalized = _normalize_message(message)
    if normalized not in SHORT_COMMAND_TARGETS:
        return {}

    state = get_session_state(session_ctx)
    target = SHORT_COMMAND_TARGETS[normalized]

    if target in {'reasons', 'losses'}:
        if not state.get('level') or not state.get('object_name') or not state.get('period'):
            return {'status': 'error', 'reason': 'Нет активного объекта для выполнения команды.'}
        return {
            'status': 'ok',
            'query': {
                'mode': 'diagnosis',
                'level': state.get('level'),
                'object_name': state.get('object_name'),
                'period_current': state.get('period'),
                'period_previous': state.get('period_previous'),
                'query_type': 'summary',
                'period': state.get('period'),
                'object': state.get('object_name'),
                'filter_payload': state.get('filter') or {},
            },
        }

    if not state.get('level') or not state.get('object_name') or not state.get('period'):
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
                'filter_payload': {'period': period},
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
            'filter_payload': state.get('filter') or {},
        },
    }


def _build_query_from_full_view(session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    state = get_session_state(session_ctx)
    level = state.get('level')
    object_name = state.get('object_name')
    period = state.get('period')

    if (not level or not object_name or not period) and isinstance(session_ctx.get('last_payload'), dict):
        data = (session_ctx.get('last_payload') or {}).get('data') or (session_ctx.get('last_payload') or {})
        level = level or data.get('level')
        object_name = object_name or data.get('object_name')
        period = period or data.get('period')

    if not level or not object_name or not period:
        return {'status': 'error', 'reason': 'Нет данных для отображения.'}

    return {
        'status': 'ok',
        'query': {
            'mode': 'diagnosis',
            'level': level,
            'object_name': object_name,
            'period_current': period,
            'period_previous': state.get('period_previous'),
            'query_type': 'summary',
            'period': period,
            'object': object_name,
            'filter_payload': state.get('filter') or {},
        },
    }


def _build_query_from_numeric_selection(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    if not message.isdigit():
        return {}
    state = get_session_state(session_ctx)
    items = state.get('last_list_items') or _extract_list_items_from_response(state.get('last_payload') or {}, state.get('last_list_level'))
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
                'filter_payload': _build_filter_from_scope(
                    selected.get('level'),
                    selected.get('object_name'),
                    state.get('period'),
                    existing_filter=state.get('filter') or {},
                ),
            },
        }
    return {'status': 'error', 'reason': 'Нет активного списка для выбора.'}


def _handle_back(session_id: str) -> Dict[str, Any]:
    restored = pop_state(session_id)
    if not restored:
        return {'status': 'error', 'reason': 'Назад недоступно.'}

    last_payload = restored.get('last_payload')
    if not last_payload:
        return {'status': 'error', 'reason': 'Назад недоступно.'}

    return enforce_contract(last_payload)


def _route_drill_query(query: Dict[str, Any], session_ctx: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    state = get_session_state(session_ctx)

    scope_level = query.get('level') or state.get('level')
    scope_object_name = query.get('object_name') or state.get('object_name')
    period = query.get('period_current') or state.get('period')
    target_level = query.get('target_level') or DEFAULT_NEXT_LEVEL.get(scope_level)
    period_previous = query.get('period_previous') or state.get('period_previous')
    full_view = bool(query.get('full_view', False))

    if not scope_level or not period:
        return {'status': 'error', 'reason': 'Нет активного объекта для анализа.'}
    if not target_level:
        return error_response('next drilldown level not available', query)

    source = _build_drill_from_scope(
        scope_level,
        scope_object_name,
        target_level,
        period,
        full_view=full_view,
        filter_payload=(query.get('filter_payload') or state.get('filter') or {}),
    )
    if 'error' in source:
        return error_response(source['error'], query)

    current = _execute_summary(
        scope_level,
        scope_object_name,
        period,
        get_session(session_id),
        explicit_filter=(query.get('filter_payload') or state.get('filter') or {}),
    )
    if 'error' in current:
        return error_response(current['error'], query)

    response = ok_response(query, build_list_view(sanitize_payload(current), sanitize_payload(source)))
    if response.get('status') == 'ok':
        list_items = _build_last_list_items(source.get('items', []), target_level) or _extract_list_items_from_response(response, target_level)
        _store_list_context(
            session_id,
            scope_level,
            scope_object_name,
            period,
            period_previous,
            'diagnosis',
            target_level,
            response_type='management_list',
            list_items=list_items,
            full_view=full_view,
            existing_filter=(current.get('filter') or query.get('filter_payload') or state.get('filter') or {}),
            push_to_stack=True,
        )
        save_last_payload(session_id, response)

    return response


def _route_signal_flow(query: Dict[str, Any], current: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    level = query.get('level')
    object_name = query.get('object_name')
    period = query.get('period_current')
    period_previous = query.get('period_previous')
    full_view = bool(query.get('full_view', False))

    target_level = DEFAULT_NEXT_LEVEL.get(level)
    drain_payload = None
    if target_level:
        source = _build_drill_from_scope(
            level,
            object_name,
            target_level,
            period,
            full_view=full_view,
            filter_payload=(current.get('filter') or query.get('filter_payload') or get_session_state(get_session(session_id)).get('filter') or {}),
        )
        if 'error' not in source:
            drain_payload = source

    previous_period = _previous_year_period(period)
    previous = None
    if previous_period and SUMMARY_EXECUTORS.get(level):
        try:
            previous = _execute_summary(
                level,
                object_name,
                previous_period,
                get_session(session_id),
                explicit_filter=(current.get('filter') or query.get('filter_payload')),
            )
            if 'error' in previous:
                previous = None
        except Exception:
            previous = None

    current = _with_previous_metrics(current, previous)
    response = ok_response(query, build_object_view(sanitize_payload(current), sanitize_payload(drain_payload) if drain_payload is not None else None))

    if response.get('status') == 'ok':
        _store_scope(
            session_id,
            level,
            object_name,
            period,
            period_previous,
            'diagnosis',
            existing_filter=(current.get('filter') or query.get('filter_payload')),
            push_to_stack=True,
        )
        if drain_payload is not None:
            list_items = _build_last_list_items(drain_payload.get('items', []), target_level) or _extract_list_items_from_response(response, target_level)
            save_session_state(
                session_id,
                last_response_type='object',
                last_list_level=target_level,
                last_list_items=list_items,
                full_view=False,
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

    if query.get('query_type') == 'losses' and level == 'sku':
        return not_implemented_response(query, 'losses not supported for this level')

    current = _execute_summary(level, object_name, period, get_session(session_id), explicit_filter=query.get('filter_payload'))
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
            previous_same_period = _execute_summary(level, object_name, previous_same_period_key, get_session(session_id), explicit_filter=query.get('filter_payload'))
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

        previous = _execute_summary(level, object_name, previous_period, get_session(session_id), explicit_filter=query.get('filter_payload'))
        if 'error' in previous:
            return error_response(previous['error'], query)

        response = ok_response(query, build_comparison_management_view(query, sanitize_payload(current), sanitize_payload(previous)))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, previous_period, mode, existing_filter=(current.get('filter') or query.get('filter_payload')), push_to_stack=True)
            save_session_state(session_id, last_response_type='comparison', last_list_level=None, last_list_items=[], full_view=False)
            save_last_payload(session_id, response)
        return response

    if query.get('query_type') == 'reasons':
        response = ok_response(query, build_reasons_view(sanitize_payload(current)))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, query.get('period_previous'), 'diagnosis', existing_filter=(current.get('filter') or query.get('filter_payload')), push_to_stack=True)
            save_session_state(session_id, last_response_type='reasons', full_view=False)
            save_last_payload(session_id, response)
        return response

    if query.get('query_type') == 'losses':
        target_level = DEFAULT_NEXT_LEVEL.get(level)
        if not target_level:
            return not_implemented_response(query, 'losses not supported for this level')

        source = _build_drill_from_scope(level, object_name, target_level, period, full_view=False, filter_payload=(current.get('filter') or query.get('filter_payload')))
        if 'error' in source:
            return error_response(source['error'], query)

        response = ok_response(query, build_losses_view_from_children(sanitize_payload(source)))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, query.get('period_previous'), 'diagnosis', push_to_stack=True)
            list_items = _build_last_list_items(source.get('items', []), target_level) or _extract_list_items_from_response(response, target_level)
            save_session_state(
                session_id,
                last_list_level=target_level,
                last_response_type='losses',
                last_list_items=list_items,
                full_view=False,
            )
            save_last_payload(session_id, response)
        return response

    return _route_signal_flow(query, current, session_id)


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    session_ctx = get_session(session_id)
    normalized = _normalize_message(message)

    if _is_back_command(normalized):
        return _handle_back(session_id)

    if normalized.isdigit():
        parsed = _build_query_from_numeric_selection(normalized, session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    elif _is_short_command(normalized):
        parsed = _build_query_from_short_command(normalized, session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    elif _is_full_reasons_command(normalized):
        parsed = _build_query_from_short_command('причины', session_ctx)
    elif _is_full_view_command(normalized):
        parsed = _build_query_from_full_view(session_ctx)
        if parsed.get('status') != 'ok':
            return parsed
    else:
        clear_full_view_flag(session_id)
        parsed = parse_query_intent(message)
        if parsed.get('status') != 'ok':
            return parsed

    query = parsed['query']
    query_type = query.get('query_type', 'summary')

    if query_type == 'drill_down':
        return enforce_contract(_route_drill_query(query, get_session(session_id), session_id))

    return enforce_contract(_route_base_query(query, session_id))
