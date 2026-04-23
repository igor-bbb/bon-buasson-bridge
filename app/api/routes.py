import math
import logging
import json

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
logger = logging.getLogger(__name__)


PUBLIC_TOP_LEVEL_KEYS = ('context', 'goal', 'metrics', 'structure', 'drain_block', 'navigation', 'reasons_block', 'decision_block', 'kpi_block', 'structure_block', 'main_driver', 'drain_block_render', 'drain_total', 'navigation_block', 'goal_block')

STRUCTURE_NAME_MAP = {
    'markup': 'Наценка',
    'retro': 'Ретро',
    'logistics': 'Логистика',
    'personnel': 'Персонал',
    'other': 'Прочие',
    'Прочее': 'Прочие',
}

MANDATORY_RENDER_BLOCK_DEFAULTS = {
    'kpi_block': [],
    'structure_block': [],
    'drain_block_render': [],
    'navigation_block': [],
    'goal_block': '',
    'summary_block': '',
    'path': [],
}


def _ensure_vectra_query_render_contract(payload):
    if not isinstance(payload, dict):
        payload = {'status': 'error', 'reason': 'unknown_error'}
    for key, default in MANDATORY_RENDER_BLOCK_DEFAULTS.items():
        if key not in payload or payload.get(key) is None:
            payload[key] = list(default) if isinstance(default, list) else default
        elif isinstance(default, list) and not isinstance(payload.get(key), list):
            payload[key] = []
        elif isinstance(default, str) and not isinstance(payload.get(key), str):
            payload[key] = ''
    return payload


def _log_vectra_query_payload(session_id, payload):
    try:
        rendered = json.dumps(_sanitize_json_value(payload), ensure_ascii=False, separators=(',', ':'))
    except Exception:
        logger.exception('vectra_query_render_payload_failed session_id=%s', session_id)
        return
    logger.info('vectra_query_render_payload session_id=%s payload=%s', session_id, rendered)


def _is_number(value):
    return isinstance(value, (int, float)) and math.isfinite(float(value))


def _num(value, default=0.0):
    try:
        value = float(value)
        return value if math.isfinite(value) else default
    except Exception:
        return default


def _intnum(value, default=0):
    try:
        value = float(value)
        return int(round(value)) if math.isfinite(value) else default
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
    effect = goal.get('effect_money')
    if effect is None:
        effect = goal.get('value_money')

    primary = None
    for item in metrics_out:
        if item.get('is_primary'):
            primary = item
            break
    if primary is not None:
        if 'delta_money' in primary:
            effect = primary.get('delta_money', effect)
        elif 'delta_percent' in primary:
            effect = primary.get('delta_percent', effect)

    effect = _intnum(effect, 0)
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

    def _append_metric(item):
        items.append(item)

    def _coalesce(entry, *keys):
        for key in keys:
            if key in entry and entry.get(key) is not None:
                return entry.get(key)
        return None

    def _money_metric(name, entry):
        fact = _coalesce(entry, 'fact_money', 'value_money', 'money')
        base = _coalesce(entry, 'pg_money', 'prev_year_money', 'base_money')
        delta = _coalesce(entry, 'delta_money', 'effect_money')
        fact_num = _num(fact)
        base_num = _num(base)
        delta_num = _num(delta)
        if base is None and delta is not None:
            base_num = fact_num - delta_num
        if delta is None:
            delta_num = fact_num - base_num
        return {
            'name': name,
            'is_primary': name == 'Финрез до',
            'fact_money': fact_num,
            'pg_money': base_num,
            'delta_money': delta_num,
            'delta_percent': _num(entry.get('delta_percent')),
        }

    def _percent_metric(name, entry):
        fact = _coalesce(entry, 'fact_percent', 'value_percent', 'percent')
        base = _coalesce(entry, 'pg_percent', 'prev_year_percent', 'base_percent')
        delta = _coalesce(entry, 'delta_percent')
        fact_num = _num(fact)
        base_num = _num(base)
        delta_num = _num(delta)
        if base is None and delta is not None:
            base_num = fact_num - delta_num
        if delta is None:
            delta_num = fact_num - base_num
        return {
            'name': name,
            'is_primary': False,
            'fact_percent': fact_num,
            'pg_percent': base_num,
            'delta_percent': delta_num,
        }

    if isinstance(raw, list):
        for entry in raw:
            if not isinstance(entry, dict):
                continue
            name = str(entry.get('name') or '').strip()
            normalized_name = STRUCTURE_NAME_MAP.get(name, name)
            if normalized_name in {'Наценка', 'Markup', 'markup'}:
                continue
            if normalized_name == 'Финрез итог' and level != 'business':
                continue
            is_percent = any(k in entry for k in ('fact_percent', 'pg_percent', 'value_percent', 'percent')) or normalized_name in {'Маржа'}
            _append_metric(_percent_metric(normalized_name, entry) if is_percent else _money_metric(normalized_name, entry))
        return items

    if not isinstance(raw, dict):
        return items

    metric_sources = [
        (('revenue',), 'Оборот', 'money'),
        (('finrez_pre',), 'Финрез до', 'money'),
        (('margin_percent', 'margin_pre'), 'Маржа', 'percent'),
        (('finrez_final',), 'Финрез итог', 'money'),
    ]
    for keys, title, kind in metric_sources:
        if title == 'Финрез итог' and level != 'business':
            continue
        entry = None
        for key in keys:
            candidate = raw.get(key)
            if isinstance(candidate, dict):
                entry = candidate
                break
        if not isinstance(entry, dict):
            continue
        _append_metric(_percent_metric(title, entry) if kind == 'percent' else _money_metric(title, entry))
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
                'effect_money': _intnum(entry.get('effect_money')),
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
        effect = _intnum(entry.get('effect_money'))
        item = {
            'name': STRUCTURE_NAME_MAP.get(key, key),
            'money': _num(entry.get('fact_money', entry.get('value_money'))),
            'percent': _num(entry.get('fact_percent', entry.get('value_percent'))),
            'base_percent': _num(entry.get('base_percent')),
            'effect_money': _intnum(effect),
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
                'effect_money': _intnum(effect_money, _intnum(base.get('effect_money'))),
                'is_main_driver': bool(entry.get('is_main_driver', base.get('is_main_driver', False))),
            })

    return items
    return None


def _normalize_drain(payload):
    ctx = payload.get('context') or {}
    level = (ctx.get('level') or payload.get('level') or '').strip().lower()
    if level == 'sku':
        return {'items': [], 'total_effect': 0}
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
        effect = entry.get('effect_money')
        if effect is None:
            potential = entry.get('potential_money', entry.get('gap_loss_money'))
            if potential is not None:
                effect = -abs(_num(potential))
            else:
                finrez = ((entry.get('fact') or {}).get('finrez') if isinstance(entry.get('fact'), dict) else None)
                if finrez is not None and _num(finrez) < 0:
                    effect = _num(finrez)
        eff = _intnum(effect)
        items.append({
            'object_name': entry.get('object_name'),
            'object_id': entry.get('object_id', idx),
            'effect_money': eff,
        })
    items = sorted(items, key=lambda x: x['effect_money'])[:3]
    if not total:
        total = sum(item['effect_money'] for item in items)
    return {'items': items, 'total_effect': _intnum(total)}


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


ACTION_TEXT_MAP = {
    'reduce_personnel': 'Сократить персонал',
    'reduce_logistics': 'Снизить логистические потери',
    'reduce_retro': 'Сократить ретро',
    'reduce_other': 'Снизить прочие потери',
    'reduce_markup_gap': 'Исправить наценку',
    'personnel': 'Сократить персонал',
    'logistics': 'Снизить логистические потери',
    'retro': 'Сократить ретро',
    'other': 'Снизить прочие потери',
}


def _normalize_decision(payload):
    decision = payload.get('decision_block')
    if not isinstance(decision, list) or not decision:
        return None
    items = []
    for entry in decision:
        if not isinstance(entry, dict):
            continue
        text = entry.get('text')
        if not text:
            key = entry.get('action') or entry.get('metric') or ''
            text = ACTION_TEXT_MAP.get(str(key), str(key).replace('_', ' ').strip())
        items.append({
            'text': text,
            'effect_money': _intnum(entry.get('effect_money')),
        })
    return items or None


def public_summary(payload):
    if not isinstance(payload, dict):
        return _ensure_vectra_query_render_contract({'status': 'error', 'reason': 'unknown_error'})
    if payload.get('status') == 'error':
        return _ensure_vectra_query_render_contract({
            'status': 'error',
            'reason': payload.get('reason') or 'unknown_error',
        })
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
    return _ensure_vectra_query_render_contract(_attach_render_blocks(response, payload))




def _fmt_int(value):
    return str(_intnum(value))


def _fmt_signed_int(value):
    num = _intnum(value)
    return f'+{num}' if num > 0 else str(num)


def _fmt_percent(value):
    try:
        value = float(value)
        if not math.isfinite(value):
            value = 0.0
    except Exception:
        value = 0.0
    return f'{value:.2f}'


def _metric_render_values(item):
    if 'fact_money' in item or 'pg_money' in item or item.get('name') != 'Маржа':
        return _fmt_int(item.get('fact_money')), _fmt_int(item.get('pg_money')), _fmt_signed_int(item.get('delta_money'))
    return _fmt_percent(item.get('fact_percent')), _fmt_percent(item.get('pg_percent')), _fmt_signed_int(round(_num(item.get('delta_percent'))))


def _render_kpi_block(metrics):
    lines = []
    for item in metrics:
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        fact, base, delta = _metric_render_values(item)
        lines.append(f'{name} {fact} | {base} | {delta}')
    return lines


def _render_structure_block(structure):
    lines = []
    for item in structure:
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        percent = _fmt_percent(item.get('percent'))
        base_percent = _fmt_percent(item.get('base_percent'))
        effect_money = _fmt_signed_int(item.get('effect_money'))
        lines.append(f'{name} {percent} vs {base_percent} → {effect_money}')
    return lines


def _render_main_driver(structure):
    for item in structure:
        if item.get('is_main_driver'):
            return str(item.get('name') or '')
    return ''


def _render_drain_block(drain):
    lines = []
    for item in drain.get('items') or []:
        object_name = str(item.get('object_name') or '').strip()
        if not object_name:
            continue
        lines.append(f'{object_name} → {_fmt_signed_int(item.get("effect_money"))}')
    return lines


def _extract_navigation_names(payload, drain):
    raw = payload.get('navigation') or {}
    names = []
    seen = set()
    if isinstance(raw, dict):
        source_items = raw.get('vector') or raw.get('items') or []
        for item in source_items:
            name = str(item or '').strip()
            if name and name not in seen:
                names.append(name)
                seen.add(name)
    if not names:
        for item in drain.get('items') or []:
            name = str(item.get('object_name') or '').strip()
            if name and name not in seen:
                names.append(name)
                seen.add(name)
    return names


def _render_navigation_block(payload, navigation, drain):
    lines = []
    names = _extract_navigation_names(payload, drain)
    for idx, name in enumerate(names[:3], start=1):
        lines.append(f'{idx} — {name}')
    action_types = [a.get('type') for a in (navigation.get('actions') or []) if isinstance(a, dict)]
    if 'all' in action_types:
        lines.append('все — полный список')
    if 'reasons' in action_types:
        lines.append('причины — разбор')
    if 'back' in action_types:
        lines.append('назад — вверх')
    return lines


def _render_goal_block(goal):
    effect_money = _intnum(goal.get('effect_money'))
    goal_type = str(goal.get('type') or '').strip().lower()
    label = 'Удержать' if goal_type in {'hold', 'keep'} else 'Закрыть'
    return f'{label}: {abs(effect_money)}'


def _build_path(response, payload):
    direct = payload.get('path') if isinstance(payload, dict) else None
    if isinstance(direct, list) and direct:
        return [str(x) for x in direct if str(x).strip()]
    ctx = response.get('context') or {}
    path = ['Бизнес']
    name = str(ctx.get('object_name') or '').strip()
    if name and name != 'Бизнес' and name != 'Старт':
        path.append(name)
    return path


def _build_summary_block(response, payload):
    direct = payload.get('summary_block') if isinstance(payload, dict) else None
    if isinstance(direct, str) and direct.strip():
        return direct
    driver = response.get('main_driver') or ''
    if driver:
        return f'Основное давление на результат через {str(driver).lower()}.'
    return ''


def _attach_render_blocks(response, payload):
    metrics = response.get('metrics') or []
    structure = response.get('structure') or []
    drain = response.get('drain_block') or {'items': [], 'total_effect': 0}
    navigation = response.get('navigation') or {'actions': []}
    goal = response.get('goal') or {'type': 'hold', 'effect_money': 0}
    response['kpi_block'] = _render_kpi_block(metrics)
    response['structure_block'] = _render_structure_block(structure)
    response['main_driver'] = _render_main_driver(structure)
    response['drain_block_render'] = _render_drain_block(drain)
    response['drain_total'] = _intnum(drain.get('total_effect'))
    response['navigation_block'] = _render_navigation_block(payload, navigation, drain)
    response['goal_block'] = _render_goal_block(goal)
    response['path'] = _build_path(response, payload)
    response['summary_block'] = _build_summary_block(response, payload)
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
    logger.info('vectra_query_received session_id=%s message=%r', session_id, request.message)
    payload = orchestrate_vectra_query(request.message, session_id=session_id)
    logger.info('vectra_query_result session_id=%s status=%s reason=%s', session_id, payload.get('status'), payload.get('reason'))
    rendered_payload = _ensure_vectra_query_render_contract(public_summary(payload))
    render_only_payload = {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'path': rendered_payload.get('path', []),
        'summary_block': rendered_payload.get('summary_block', ''),
        'goal_block': rendered_payload.get('goal_block', ''),
        'kpi_block': rendered_payload.get('kpi_block', []),
        'structure_block': rendered_payload.get('structure_block', []),
        'main_driver': rendered_payload.get('main_driver', ''),
        'drain_block_render': rendered_payload.get('drain_block_render', []),
        'drain_total': rendered_payload.get('drain_total', 0),
        'navigation_block': rendered_payload.get('navigation_block', []),
    }
    logger.info(
        'vectra_query_render_contract session_id=%s has_kpi_block=%s has_structure_block=%s has_drain_block_render=%s has_navigation_block=%s has_goal_block=%s',
        session_id,
        'kpi_block' in render_only_payload,
        'structure_block' in render_only_payload,
        'drain_block_render' in render_only_payload,
        'navigation_block' in render_only_payload,
        'goal_block' in render_only_payload,
    )
    _log_vectra_query_payload(session_id, render_only_payload)
    return json_response(render_only_payload)


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
