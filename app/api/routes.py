import math

import hashlib

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



PUBLIC_TOP_LEVEL_KEYS = ('context', 'goal', 'metrics', 'structure', 'drain_block', 'navigation', 'reasons_block', 'decision_block')

STRUCTURE_NAME_MAP = {
    'markup': 'Наценка',
    'retro': 'Ретро',
    'logistics': 'Логистика',
    'personnel': 'Персонал',
    'other': 'Прочие',
    'Прочее': 'Прочие',
}


def _is_number(value):
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def _num(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except Exception:
        return default


def _normalize_context(payload):
    ctx = payload.get('context') or {}
    if not isinstance(ctx, dict):
        ctx = {}
    level = (ctx.get('level') or payload.get('level') or '').strip()
    object_name = ctx.get('object_name') or payload.get('object_name') or ''
    if level == 'business' and not object_name:
        object_name = 'Бизнес'
    parent_object = ctx.get('parent_object')
    if parent_object is None:
        parent_object = ctx.get('parent_name')
    out = {
        'level': level,
        'object_name': object_name or 'Бизнес',
        'period': ctx.get('period') or payload.get('period'),
        'parent_object': parent_object,
    }
    if level == 'network':
        agg = payload.get('aggregation_type') or payload.get('aggregation_level') or payload.get('grouping_type')
        if agg:
            out['aggregation_type'] = agg
    return out


def _normalize_goal(payload, metrics_out):
    goal = payload.get('goal') or {}
    if not isinstance(goal, dict):
        goal = {}
    ctx = payload.get('context') or {}
    level = (ctx.get('level') or payload.get('level') or '').strip()
    effect = goal.get('effect_money')
    if effect is None:
        effect = goal.get('value_money')
    # lock business goal to finrez_pre delta
    if level == 'business':
        for item in metrics_out:
            if item.get('name') == 'Финрез до':
                effect = item.get('delta_money', effect)
                break
    effect = _num(effect, 0.0)
    gtype = (goal.get('type') or '').strip().lower()
    if gtype in ('close_gap', 'close'):
        gtype = 'close'
    elif gtype in ('keep', 'hold'):
        gtype = 'hold'
    else:
        gtype = 'hold' if effect >= 0 else 'close'
    return {'type': gtype, 'effect_money': effect}


def _normalize_metrics(payload):
    raw = payload.get('metrics') or {}
    items = []
    ctx = payload.get('context') or {}
    level = (ctx.get('level') or payload.get('level') or '').strip().lower()
    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            name = entry.get('name') or ''
            normalized_name = STRUCTURE_NAME_MAP.get(str(name), str(name))
            if normalized_name == 'Финрез итог' and level != 'business':
                continue
            is_percent = ('fact_percent' in entry or 'pg_percent' in entry or str(normalized_name).strip().lower() in {'маржа', 'наценка'})
            if is_percent:
                items.append({
                    'name': normalized_name,
                    'fact_percent': _num(entry.get('fact_percent', entry.get('fact_money'))),
                    'pg_percent': _num(entry.get('pg_percent', entry.get('pg_money'))),
                    'delta_percent': _num(entry.get('delta_percent')),
                })
            else:
                items.append({
                    'name': normalized_name,
                    'fact_money': _num(entry.get('fact_money')),
                    'pg_money': _num(entry.get('pg_money')),
                    'delta_money': _num(entry.get('delta_money')),
                    'delta_percent': _num(entry.get('delta_percent')),
                })
        return items
    if not isinstance(raw, dict):
        return items

    mapping = [
        ('revenue', 'Оборот', 'money'),
        ('markup_percent', 'Наценка', 'percent'),
        ('finrez_pre', 'Финрез до', 'money'),
        ('margin_percent', 'Маржа', 'percent'),
        ('finrez_final', 'Финрез итог', 'money'),
    ]
    for key, title, kind in mapping:
        if title == 'Финрез итог' and level != 'business':
            continue
        entry = raw.get(key) or {}
        if not isinstance(entry, dict):
            continue
        if kind == 'percent':
            items.append({
                'name': title,
                'fact_percent': _num(entry.get('fact_percent')),
                'pg_percent': _num(entry.get('prev_year_percent') if 'prev_year_percent' in entry else entry.get('pg_percent')),
                'delta_percent': _num(entry.get('delta_percent')),
            })
        else:
            items.append({
                'name': title,
                'fact_money': _num(entry.get('fact_money')),
                'pg_money': _num(entry.get('prev_year_money') if 'prev_year_money' in entry else entry.get('pg_money')),
                'delta_money': _num(entry.get('delta_money')),
                'delta_percent': _num(entry.get('delta_percent')),
            })
    return items


def _normalize_structure(payload):
    raw = payload.get('structure') or {}
    items = []
    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            items.append({
                'name': entry.get('name'),
                'money': _num(entry.get('money', entry.get('fact_money', entry.get('value_money')))),
                'percent': _num(entry.get('percent', entry.get('fact_percent', entry.get('value_percent')))),
                'base_percent': _num(entry.get('base_percent')),
                'effect_money': _num(entry.get('effect_money')),
                'is_main_driver': bool(entry.get('is_main_driver', False)),
            })
        return items
    if not isinstance(raw, dict):
        return items
    order = ['markup', 'retro', 'logistics', 'personnel', 'other']
    main_driver = None
    for key in order:
        entry = raw.get(key) or {}
        if not isinstance(entry, dict):
            entry = {}
        effect = _num(entry.get('effect_money'))
        item = {
            'name': STRUCTURE_NAME_MAP.get(key, key),
            'money': _num(entry.get('fact_money', entry.get('value_money'))),
            'percent': _num(entry.get('fact_percent', entry.get('value_percent'))),
            'base_percent': _num(entry.get('base_percent')),
            'effect_money': effect,
            'is_main_driver': False,
        }
        items.append(item)
        if main_driver is None or effect < main_driver[1]:
            main_driver = (item['name'], effect)
    if main_driver:
        for item in items:
            item['is_main_driver'] = item['name'] == main_driver[0]
    return items


def _normalize_reasons(payload, structure_items):
    raw = payload.get('reasons_block')
    if raw is None:
        return None

    struct_map = {str(item.get('name')).lower(): item for item in structure_items}
    struct_map.setdefault('прочее', struct_map.get('прочие', {}))
    struct_map.setdefault('прочие', struct_map.get('прочее', {}))

    items = []

    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue

            raw_name = entry.get('name')
            name = STRUCTURE_NAME_MAP.get(str(raw_name), str(raw_name))

            if name in ("Прочее", "прочее"):
                name = "Прочие"

            base = struct_map.get(str(name).lower(), {})

            money = entry.get('money')
            percent = entry.get('percent')
            base_percent = entry.get('base_percent')
            effect_money = entry.get('effect_money', base.get('effect_money'))

            if (_num(money) == 0.0 and _num(effect_money) != 0.0 and base):
                money = base.get('money')

            if (_num(percent) == 0.0 and _num(effect_money) != 0.0 and base):
                percent = base.get('percent')

            if (_num(base_percent) == 0.0 and _num(effect_money) != 0.0 and base):
                base_percent = base.get('base_percent')

            items.append({
                'name': name,
                'money': _num(money, _num(base.get('money'))),
                'percent': _num(percent, _num(base.get('percent'))),
                'base_percent': _num(base_percent, _num(base.get('base_percent'))),
                'effect_money': _num(effect_money, _num(base.get('effect_money'))),
                'is_main_driver': bool(entry.get('is_main_driver', base.get('is_main_driver', False))),
            })

    return items
    return None


def _normalize_drain(payload):
    raw = payload.get('drain_block') or []
    items = []
    total = 0.0
    if isinstance(raw, dict):
        source_items = raw.get('items') or []
        total = _num(raw.get('total_effect'))
    else:
        source_items = raw if isinstance(raw, list) else []
    for idx, entry in enumerate(source_items, start=1):
        if not isinstance(entry, dict):
            continue
        eff = _num(entry.get('effect_money'))
        items.append({
            'object_name': entry.get('object_name'),
            'object_id': entry.get('object_id', idx),
            'effect_money': eff,
        })
    items = sorted(items, key=lambda x: x['effect_money'])[:3]
    if not total:
        total = sum(item['effect_money'] for item in items)
    return {'items': items, 'total_effect': total}


def _normalize_navigation(payload, drain):
    raw = payload.get('navigation') or {}
    actions = []
    if isinstance(raw, dict) and isinstance(raw.get('actions'), list):
        for action in raw['actions']:
            if not isinstance(action, dict):
                continue
            if action.get('type') == 'drilldown':
                target = action.get('target_id', action.get('id'))
                actions.append({'type': 'drilldown', 'target_id': target})
            elif action.get('type') in {'all', 'reasons', 'back'}:
                actions.append({'type': action.get('type')})
        return {'actions': actions}
    # backfill from legacy navigation/items
    items = []
    if isinstance(raw, dict):
        items = raw.get('vector') or raw.get('items') or []
    for idx, _ in enumerate(items[:3], start=1):
        actions.append({'type': 'drilldown', 'target_id': idx})
    if isinstance(raw, dict) and raw.get('has_all'):
        actions.append({'type': 'all'})
    if isinstance(raw, dict) and (raw.get('has_causes') or raw.get('has_reasons')):
        actions.append({'type': 'reasons'})
    if isinstance(raw, dict) and raw.get('has_back'):
        actions.append({'type': 'back'})
    if not actions and drain.get('items'):
        for item in drain['items']:
            actions.append({'type': 'drilldown', 'target_id': item.get('object_id')})
        actions.extend([{'type': 'all'}, {'type': 'reasons'}, {'type': 'back'}])
    return {'actions': actions}


def _normalize_decision(payload):
    decision = payload.get('decision_block')
    return decision if isinstance(decision, list) and decision else None


def public_summary(payload):
    if not isinstance(payload, dict):
        return payload
    metrics = _normalize_metrics(payload)
    structure = _normalize_structure(payload)
    drain = _normalize_drain(payload)
    response = {
        'context': _normalize_context(payload),
        'goal': _normalize_goal(payload, metrics),
        'metrics': metrics,
        'structure': structure,
        'drain_block': drain,
        'navigation': _normalize_navigation(payload, drain),
    }
    reasons = _normalize_reasons(payload, structure)
    if reasons is not None:
        response['reasons_block'] = reasons
    decision = _normalize_decision(payload)
    if decision is not None:
        response['decision_block'] = decision
    return response


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
