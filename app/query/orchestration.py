from __future__ import annotations

import json
from pathlib import Path
from threading import Lock
from typing import Any, Dict, Optional

from app.domain.filters import get_normalized_rows
from app.domain.summary import (
    get_business_summary,
    get_manager_summary,
    get_manager_top_summary,
    get_network_summary,
    get_sku_summary,
)
from app.query.entity_dictionary import get_entity_dictionary, normalize_entity_text
from app.query.parsing import normalize_user_message, parse_query_intent

SESSION_STORE: Dict[str, Dict[str, Any]] = {}
SESSION_LOCK = Lock()
SESSION_FILE = Path('/tmp/vectra_session_store.json')

SUMMARY_EXECUTORS = {
    'business': lambda obj, period, previous_state=None, propagated_effect=None: get_business_summary(period=period, previous_state=previous_state),
    'manager_top': lambda obj, period, previous_state=None, propagated_effect=None: get_manager_top_summary(manager_top=obj, period=period, previous_state=previous_state, propagated_effect=propagated_effect),
    'manager': lambda obj, period, previous_state=None, propagated_effect=None: get_manager_summary(manager=obj, period=period, previous_state=previous_state, propagated_effect=propagated_effect),
    'network': lambda obj, period, previous_state=None, propagated_effect=None: get_network_summary(network=obj, period=period, previous_state=previous_state, propagated_effect=propagated_effect),
    'sku': lambda obj, period, previous_state=None, propagated_effect=None: get_sku_summary(sku=obj, period=period, previous_state=previous_state, propagated_effect=propagated_effect),
}


def _load_session_store() -> None:
    global SESSION_STORE
    if not SESSION_FILE.exists():
        return
    try:
        raw = json.loads(SESSION_FILE.read_text(encoding='utf-8'))
        if isinstance(raw, dict):
            SESSION_STORE = raw
    except Exception:
        SESSION_STORE = {}


def _persist_session_store() -> None:
    try:
        SESSION_FILE.write_text(json.dumps(SESSION_STORE, ensure_ascii=False), encoding='utf-8')
    except Exception:
        pass


def get_session(session_id: str) -> Dict[str, Any]:
    with SESSION_LOCK:
        if not SESSION_STORE:
            _load_session_store()
        return dict(SESSION_STORE.get(session_id, {}))


def update_session(session_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    with SESSION_LOCK:
        if not SESSION_STORE:
            _load_session_store()
        current = dict(SESSION_STORE.get(session_id, {}))
        current.update(payload)
        SESSION_STORE[session_id] = current
        _persist_session_store()
        return dict(current)


def _latest_period() -> Optional[str]:
    try:
        periods = sorted({str(r.get('period')) for r in get_normalized_rows() if r.get('period')})
        return periods[-1] if periods else None
    except Exception:
        return None


def _public_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    allowed = ['context', 'metrics', 'structure', 'drain_block', 'goal', 'focus_block', 'decision_block', 'navigation']
    return {key: payload[key] for key in allowed if key in payload}


def _previous_navigation_state(session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    return {
        'current_level': session_ctx.get('current_level'),
        'items': session_ctx.get('navigation_items', []),
    } if session_ctx.get('current_level') else {}


def _save_summary_state(session_id: str, summary: Dict[str, Any], child_level: Optional[str], propagated_effects: Optional[Dict[str, float]] = None) -> None:
    navigation = summary.get('navigation') or {}
    update_session(session_id, {
        'current_level': (summary.get('context') or {}).get('level'),
        'current_object_name': (summary.get('context') or {}).get('object_name'),
        'period': (summary.get('context') or {}).get('period'),
        'navigation_items': list(navigation.get('items') or []),
        'navigation_all': list(navigation.get('all') or []),
        'child_level': child_level,
        'propagated_effects': propagated_effects or {},
        'last_summary': _public_contract(summary),
    })


def _drill_level(level: str) -> Optional[str]:
    return {
        'business': 'manager_top',
        'manager_top': 'manager',
        'manager': 'network',
        'network': 'sku',
    }.get(level)


def _execute_summary(level: str, object_name: Optional[str], period: str, session_ctx: Optional[Dict[str, Any]] = None, propagated_effect: Optional[float] = None) -> Dict[str, Any]:
    executor = SUMMARY_EXECUTORS.get(level)
    if not executor:
        return {'error': f'unsupported level: {level}'}
    previous_state = _previous_navigation_state(session_ctx or {})
    return executor(object_name, period, previous_state=previous_state, propagated_effect=propagated_effect)


def _resolve_name_only_query(message: str) -> Optional[Dict[str, Any]]:
    period = _latest_period()
    if not period:
        return None
    entity_dictionary = get_entity_dictionary(period)
    normalized = normalize_entity_text(message)
    if not normalized:
        return None
    for level in ['manager_top', 'manager', 'network', 'sku']:
        canonical = entity_dictionary.get(level, {}).get('index', {}).get(normalized)
        if canonical:
            return {
                'level': level,
                'object_name': canonical,
                'period_current': period,
                'query_type': 'summary',
            }
    return None


def _build_error(reason: str) -> Dict[str, Any]:
    return {'status': 'error', 'reason': reason}


def _select_from_navigation(index_text: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    if not index_text.isdigit():
        return _build_error('invalid selection')
    items = list(session_ctx.get('navigation_items') or [])
    child_level = session_ctx.get('child_level')
    period = session_ctx.get('period')
    if not items or not child_level or not period:
        return _build_error('Нет активного списка для выбора')
    index = int(index_text) - 1
    if index < 0 or index >= len(items):
        return _build_error('Нет активного списка для выбора')
    object_name = items[index]
    propagated_effect = (session_ctx.get('propagated_effects') or {}).get(object_name)
    summary = _execute_summary(child_level, object_name, period, session_ctx=session_ctx, propagated_effect=propagated_effect)
    if summary.get('error'):
        return _build_error(summary['error'])
    child = _drill_level(child_level)
    propagated = {item.get('object_name'): abs(float(item.get('effect_money') or 0.0)) for item in summary.get('drain_block', [])}
    _save_summary_state(session_id=session_ctx.get('_session_id', 'default'), summary=summary, child_level=child, propagated_effects=propagated)
    return _public_contract(summary)


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    session_ctx = get_session(session_id)
    session_ctx['_session_id'] = session_id
    normalized = normalize_user_message(message)

    if normalized.isdigit():
        return _select_from_navigation(normalized, session_ctx)

    parsed = parse_query_intent(message)
    query: Optional[Dict[str, Any]] = None
    if parsed.get('status') == 'ok':
        query = parsed.get('query')
    else:
        fallback = _resolve_name_only_query(message)
        if fallback:
            query = fallback
        else:
            return parsed

    if not query:
        return _build_error('query not recognized')

    level = query.get('level') or 'business'
    period = query.get('period_current') or _latest_period()
    if not period:
        return _build_error('period not recognized')

    if query.get('query_type') == 'drill_down':
        # For direct drill commands, return current scope summary and preserve navigation.
        level = query.get('level') or 'business'
        object_name = query.get('object_name') or ('business' if level == 'business' else None)
    else:
        object_name = query.get('object_name') or ('business' if level == 'business' else None)

    if level != 'business' and not object_name:
        return _build_error('object not recognized')

    summary = _execute_summary(level, object_name, period, session_ctx=session_ctx)
    if summary.get('error'):
        return _build_error(summary['error'])

    child = _drill_level(level)
    propagated = {item.get('object_name'): abs(float(item.get('effect_money') or 0.0)) for item in summary.get('drain_block', [])}
    _save_summary_state(session_id=session_id, summary=summary, child_level=child, propagated_effects=propagated)
    return _public_contract(summary)
