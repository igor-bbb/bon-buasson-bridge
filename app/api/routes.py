import math

import hashlib
import re

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from app.config import EMPTY_SKU_LABEL, LOW_VOLUME_THRESHOLD, SHEET_URL
from app.models.request_models import VectraQueryRequest
from app.domain.summary import (
    get_business_summary,
    get_manager_top_summary,
    get_manager_summary,
    get_network_summary,
    get_sku_summary,
)
from app.query.entity_dictionary import get_entity_dictionary
from app.query.orchestration import orchestrate_vectra_query

router = APIRouter()


PUBLIC_SUMMARY_KEYS = ('context', 'goal', 'metrics', 'structure', 'drain_block', 'navigation', 'reasons_block', 'decision_block')


def _safe_float(value):
    try:
        value = float(value)
        if math.isfinite(value):
            return value
    except Exception:
        pass
    return 0.0


def _display_name(key: str) -> str:
    mapping = {
        'revenue': 'Оборот',
        'finrez_pre': 'Финрез до',
        'finrez_final': 'Финрез итог',
        'margin_percent': 'Маржа',
        'markup_percent': 'Наценка',
    }
    return mapping.get(key, str(key))


def _normalize_context(payload):
    context = dict((payload.get('context') or {}))
    level = context.get('level') or payload.get('level') or 'business'
    object_name = context.get('object_name') or payload.get('object_name') or 'Бизнес'
    result = {
        'level': level,
        'object_name': object_name,
        'period': context.get('period') or payload.get('period'),
        'parent_object': context.get('parent_object') or payload.get('parent_object'),
    }
    if level == 'network':
        agg = payload.get('aggregation_type') or payload.get('aggregation_level') or payload.get('grouping_type')
        if agg:
            result['aggregation_type'] = agg
    return result


def _normalize_goal(payload):
    goal = dict(payload.get('goal') or {})
    goal_type = str(goal.get('type') or '').strip().lower()
    if goal_type in {'close_gap', 'close'}:
        goal_type = 'close'
    elif goal_type in {'keep', 'hold'}:
        goal_type = 'hold'
    else:
        goal_type = 'hold'
    return {
        'type': goal_type,
        'effect_money': _safe_float(goal.get('effect_money', goal.get('value_money'))),
    }


def _normalize_metrics(payload):
    metrics = payload.get('metrics') or {}
    if isinstance(metrics, list):
        return metrics
    result = []
    for key, entry in metrics.items():
        if not isinstance(entry, dict):
            continue
        item = {'name': _display_name(key)}
        if 'fact_money' in entry or 'prev_year_money' in entry or 'delta_money' in entry:
            item.update({
                'fact_money': _safe_float(entry.get('fact_money')),
                'pg_money': _safe_float(entry.get('pg_money', entry.get('prev_year_money'))),
                'delta_money': _safe_float(entry.get('delta_money')),
                'delta_percent': _safe_float(entry.get('delta_percent')),
            })
            result.append(item)
        elif 'fact_percent' in entry or 'prev_year_percent' in entry:
            item.update({
                'fact_money': 0.0,
                'pg_money': 0.0,
                'delta_money': 0.0,
                'delta_percent': _safe_float(entry.get('delta_percent')),
                'fact_percent': _safe_float(entry.get('fact_percent')),
                'pg_percent': _safe_float(entry.get('pg_percent', entry.get('prev_year_percent'))),
            })
            result.append(item)
    return result


def _normalize_structure(payload):
    structure = payload.get('structure') or {}
    items = []
    source = structure.items() if isinstance(structure, dict) else []
    main_driver = None
    main_value = None
    for key, entry in source:
        if not isinstance(entry, dict):
            continue
        effect = _safe_float(entry.get('effect_money'))
        if main_value is None or effect < main_value:
            main_value = effect
            main_driver = key
    labels = {'markup': 'Наценка', 'retro': 'Ретро', 'logistics': 'Логистика', 'personnel': 'Персонал', 'other': 'Прочие'}
    for key, entry in source:
        if not isinstance(entry, dict):
            continue
        items.append({
            'name': labels.get(key, entry.get('name') or str(key)),
            'money': _safe_float(entry.get('money', entry.get('fact_money', entry.get('value_money')))),
            'percent': _safe_float(entry.get('percent', entry.get('fact_percent', entry.get('value_percent')))),
            'base_percent': _safe_float(entry.get('base_percent')),
            'effect_money': _safe_float(entry.get('effect_money')),
            'is_main_driver': key == main_driver,
        })
    return items


def _slugify(text):
    text = (text or '').strip().lower()
    text = re.sub(r'[^a-zA-Z0-9а-яА-ЯіІїЇєЄ_]+', '_', text)
    return text.strip('_') or 'item'


def _normalize_drain(payload):
    raw = payload.get('drain_block') or []
    items = []
    total = 0.0
    if isinstance(raw, dict) and 'items' in raw:
        raw_items = raw.get('items') or []
    else:
        raw_items = raw if isinstance(raw, list) else []
    level = ((_normalize_context(payload)).get('level') or 'object')
    next_level = ((payload.get('navigation') or {}).get('next_level') or 'item')
    for entry in raw_items:
        if not isinstance(entry, dict):
            continue
        eff = _safe_float(entry.get('effect_money'))
        total += eff
        name = entry.get('object_name') or ''
        oid = entry.get('object_id') or f"{next_level}:{_slugify(name)}"
        items.append({
            'object_name': name,
            'object_id': oid,
            'effect_money': eff,
        })
    return {'items': items[:3], 'total_effect': total}


def _normalize_navigation(payload):
    nav = payload.get('navigation') or {}
    actions = []
    raw_items = nav.get('items') or nav.get('vector') or []
    for idx, _ in enumerate(raw_items[:3], start=1):
        actions.append({'type': 'drilldown', 'id': idx})
    if nav.get('has_all') or 'all_block' in payload:
        actions.append({'type': 'all'})
    if nav.get('has_causes') or 'reasons_block' in payload:
        actions.append({'type': 'reasons'})
    if nav.get('has_back'):
        actions.append({'type': 'back'})
    return {'actions': actions}


def _normalize_reasons(payload):
    raw = payload.get('reasons_block')
    if not raw:
        return None
    result = []
    struct_map = {item['name']: item for item in _normalize_structure(payload)}
    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            nm = entry.get('name') or ''
            mapped = struct_map.get(nm) or struct_map.get(str(nm).title()) or None
            if mapped:
                item = dict(mapped)
            else:
                item = {
                    'name': nm,
                    'money': _safe_float(entry.get('money')),
                    'percent': _safe_float(entry.get('percent')),
                    'base_percent': _safe_float(entry.get('base_percent')),
                    'effect_money': _safe_float(entry.get('effect_money')),
                }
            if entry.get('description'):
                item['description'] = entry.get('description')
            result.append(item)
    return result


def _normalize_decision(payload):
    raw = payload.get('decision_block')
    return raw if raw else None


def public_summary(payload):
    if not isinstance(payload, dict):
        return payload
    if payload.get('status') == 'error':
        return {'message': 'нет данных'}
    result = {
        'context': _normalize_context(payload),
        'goal': _normalize_goal(payload),
        'metrics': _normalize_metrics(payload),
        'structure': _normalize_structure(payload),
        'drain_block': _normalize_drain(payload),
        'navigation': _normalize_navigation(payload),
    }
    reasons = _normalize_reasons(payload)
    if reasons is not None:
        result['reasons_block'] = reasons
    decision = _normalize_decision(payload)
    if decision is not None:
        result['decision_block'] = decision
    return result


def _sanitize_json_value(value):
    if isinstance(value, dict):
        return {str(k): _sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else 0.0
    return value

def json_response(payload):
    return JSONResponse(content=_sanitize_json_value(payload), media_type='application/json; charset=utf-8')


def _stable_session_id(request: VectraQueryRequest) -> str:
    raw = (getattr(request, 'session_id', None) or '').strip()
    return raw or 'default'


@router.get('/', summary='Root')
def root():
    return json_response({'status': 'ok'})


@router.get('/health', summary='Health')
def health():
    return json_response({
        'status': 'ok',
        'sheet_url_exists': bool(SHEET_URL),
        'low_volume_threshold': LOW_VOLUME_THRESHOLD,
        'empty_sku_policy': EMPTY_SKU_LABEL,
    })


@router.get('/business_summary', summary='Business Summary')
def business_summary(period: str):
    return json_response(public_summary(get_business_summary(period=period)))


@router.get('/manager_top_summary', summary='Manager Top Summary')
def manager_top_summary(manager_top: str, period: str):
    return json_response(public_summary(get_manager_top_summary(manager_top=manager_top, period=period)))


@router.get('/manager_summary', summary='Manager Summary')
def manager_summary(manager: str, period: str):
    return json_response(public_summary(get_manager_summary(manager=manager, period=period)))


@router.get('/network_summary', summary='Network Summary')
def network_summary(network: str, period: str):
    return json_response(public_summary(get_network_summary(network=network, period=period)))


@router.get('/sku_summary', summary='SKU Summary')
def sku_summary(sku: str, period: str):
    return json_response(public_summary(get_sku_summary(sku=sku, period=period)))


@router.post('/vectra/query', summary='Stateful VECTRA Query')
def vectra_query(request: VectraQueryRequest):
    session_id = _stable_session_id(request)
    return json_response(public_summary(orchestrate_vectra_query(request.message, session_id=session_id)))


@router.get('/meta/entities')
def meta_entities(period: str = ''):
    payload = get_entity_dictionary(period=period or None)
    return json_response({
        'status': 'ok',
        'period': period or None,
        'entity_counts': {
            key: len(value.get('canonical', []))
            for key, value in payload.items()
            if isinstance(value, dict) and 'canonical' in value
        },
    })
