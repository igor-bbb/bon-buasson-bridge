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
from app.query.orchestration import orchestrate_vectra_query, save_last_payload

router = APIRouter()
logger = logging.getLogger(__name__)


PUBLIC_TOP_LEVEL_KEYS = ('business_result_money', 'business_result_rating', 'opportunity_rating', 'business_reasons', 'priority_action', 'object_result_money', 'opportunity_money', 'navigation_money', 'net_drain_money', 'gross_loss_money', 'internal_drain_money', 'compare_base', 'context', 'metrics', 'structure', 'drain_block', 'all_block', 'navigation', 'reasons_block', 'decision_block', 'decision_block_render', 'reasons_block_render', 'kpi_block', 'structure_block', 'main_driver', 'drain_block_render', 'drain_total', 'navigation_block', 'summary_block', 'product_layer_block', 'path')

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
    'result_block': [],
    'summary_block': '',
    'product_layer_block': [],
    'path': [],
    'decision_block': [],
    'decision_block_render': [],
    'render_mode': '',
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
        elif isinstance(default, dict) and not isinstance(payload.get(key), dict):
            payload[key] = {}
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
            if normalized_name == 'Markup':
                normalized_name = 'Наценка'
            if normalized_name == 'markup':
                normalized_name = 'Наценка'
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
        (('markup_percent', 'markup'), 'Наценка', 'percent'),
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

            money = entry.get('money', entry.get('value_money', entry.get('fact_money')))
            percent = entry.get('percent', entry.get('value_percent', entry.get('fact_percent')))
            base_percent = entry.get('base_percent', entry.get('business_percent', entry.get('pg_percent')))
            effect_money = entry.get('effect_money', base.get('effect_money'))

            if (_num(money) == 0.0 and _num(effect_money) != 0.0 and base):
                money = base.get('money')

            if (_num(percent) == 0.0 and _num(effect_money) != 0.0 and base):
                percent = base.get('percent')

            if (_num(base_percent) == 0.0 and _num(effect_money) != 0.0 and base):
                base_percent = base.get('base_percent')

            percent_num = _num(percent, _num(base.get('percent')))
            base_percent_num = _num(base_percent, _num(base.get('base_percent')))
            delta_percent = percent_num - base_percent_num
            effect_num = _intnum(effect_money, _intnum(base.get('effect_money')))
            if effect_num < 0 and abs(delta_percent) >= 10:
                signal = 'критично'
            elif effect_num < 0:
                signal = 'риск'
            else:
                signal = 'норма'

            prev_money = entry.get('previous_money', entry.get('prev_money', entry.get('pg_money')))
            prev_percent = entry.get('previous_percent', entry.get('prev_percent', entry.get('pg_percent')))
            prev_percent_missing = bool(entry.get('previous_percent_missing')) or prev_percent is None
            prev_percent_num = None if prev_percent_missing else _num(prev_percent)
            delta_vs_prev = entry.get('delta_vs_previous_percent', entry.get('delta_vs_prev'))
            if delta_vs_prev is None and prev_percent_num is not None:
                delta_vs_prev = percent_num - prev_percent_num

            items.append({
                'name': name,
                'money': _num(money, _num(base.get('money'))),
                'percent': percent_num,
                'base_percent': base_percent_num,
                'previous_money': _num(prev_money),
                'previous_percent': prev_percent_num,
                'previous_percent_missing': prev_percent_missing,
                'previous_note': entry.get('previous_note', 'нет корректной базы' if prev_percent_missing else ''),
                'delta_percent': round(delta_percent, 2),
                'delta_vs_business_percent': round(delta_percent, 2),
                'delta_vs_previous_percent': None if delta_vs_prev is None else round(_num(delta_vs_prev), 2),
                'effect_money': effect_num,
                'signal': signal,
                'is_main_driver': bool(entry.get('is_main_driver', base.get('is_main_driver', False))),
            })

    return items
    return None


def _normalize_drain(payload):
    ctx = payload.get('context') or {}
    level = (ctx.get('level') or payload.get('level') or '').strip().lower()
    if level == 'sku':
        return {'items': [], 'total_effect': 0}

    nav = payload.get('navigation') if isinstance(payload.get('navigation'), dict) else {}
    mode = nav.get('mode') or payload.get('view_mode') or ''
    is_all_mode = mode == 'all'

    # Navigation Contract v1.2: rendered Top-3 and "все" must use the same
    # source list. Domain layer already builds all_block in the correct order
    # (navigation_money DESC), so API/render boundary must not re-sort or fill it.
    all_block = payload.get('all_block') if isinstance(payload.get('all_block'), list) else None
    raw = payload.get('drain_block')
    explicit_total = None

    if all_block is not None:
        source_items = all_block if is_all_mode else all_block[:3]
    else:
        if raw is None:
            raw = payload.get('items') or []
        if isinstance(raw, dict):
            source_items = raw.get('items') or []
            explicit_total = raw.get('total_effect')
        else:
            source_items = raw if isinstance(raw, list) else []
        if not is_all_mode:
            source_items = source_items[:3]

    items = []
    for idx, entry in enumerate(source_items, start=1):
        if not isinstance(entry, dict):
            continue
        navigation_money = entry.get('navigation_money')
        if navigation_money is None:
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
            navigation_money = abs(eff) if eff < 0 else 0
        nav_money = _intnum(navigation_money)
        items.append({
            'object_name': entry.get('object_name') or entry.get('name'),
            'object_id': entry.get('object_id', idx),
            'effect_money': -abs(nav_money),
            'navigation_money': nav_money,
        })

    if is_all_mode:
        total = -sum(_num(item.get('navigation_money')) for item in items)
    elif explicit_total is not None and all_block is None:
        total = _num(explicit_total)
    else:
        total = -sum(_num(item.get('navigation_money')) for item in items)
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
    # UX-only labels. Internal action codes stay unchanged.
    'raise_margin': 'Повысить наценку',
    'reduce_personnel': 'Снизить затраты на персонал',
    'reduce_logistics': 'Сократить логистические затраты',
    'reduce_retro': 'Снизить ретроусловия',
    'reduce_other': 'Снизить прочие затраты',
    'reduce_markup_gap': 'Повысить наценку',
    'markup': 'Повысить наценку',
    'margin': 'Повысить наценку',
    'personnel': 'Снизить затраты на персонал',
    'logistics': 'Сократить логистические затраты',
    'retro': 'Снизить ретроусловия',
    'other': 'Снизить прочие затраты',
}


DECISION_LEVELS = {'network'}
PRODUCT_LEVELS = {'category', 'tmc_group', 'sku'}


def _decision_action_text(action_key, level):
    base = ACTION_TEXT_MAP.get(str(action_key), str(action_key).replace('_', ' ').strip())
    if level in {'category', 'tmc_group'}:
        category_map = {
            'raise_margin': 'Повысить наценку',
            'reduce_retro': 'Снизить ретроусловия',
            'reduce_logistics': 'Сократить логистические затраты',
            'reduce_personnel': 'Снизить затраты на персонал',
            'reduce_other': 'Снизить прочие затраты',
            'markup': 'Повысить наценку',
            'margin': 'Повысить наценку',
            'retro': 'Снизить ретроусловия',
            'logistics': 'Сократить логистические затраты',
            'personnel': 'Снизить затраты на персонал',
            'other': 'Снизить прочие затраты',
        }
        return category_map.get(str(action_key), base)
    if level == 'sku':
        sku_map = {
            'raise_margin': 'Повысить наценку',
            'reduce_retro': 'Снизить ретроусловия',
            'reduce_logistics': 'Сократить логистические затраты',
            'reduce_personnel': 'Снизить затраты на персонал',
            'reduce_other': 'Снизить прочие затраты',
            'markup': 'Повысить наценку',
            'margin': 'Повысить наценку',
            'retro': 'Снизить ретроусловия',
            'logistics': 'Сократить логистические затраты',
            'personnel': 'Снизить затраты на персонал',
            'other': 'Снизить прочие затраты',
        }
        return sku_map.get(str(action_key), base)
    return base



def _is_product_layer_level(level):
    return str(level or '').strip().lower() in PRODUCT_LEVELS


def _metric_lookup(metrics, name):
    wanted = str(name or '').strip().lower()
    for item in metrics or []:
        if str(item.get('name') or '').strip().lower() == wanted:
            return item
    return {}


def _build_product_layer_block(response):
    """V1.3 Product Layer presentation for levels below Network.

    Uses only already available payload fields. Missing source data is shown
    explicitly instead of being calculated or invented.
    """
    metrics = response.get('metrics') or []
    margin = _metric_lookup(metrics, 'Маржа')
    revenue = _metric_lookup(metrics, 'Оборот')
    compare_base = str(response.get('compare_base') or ((response.get('context') or {}).get('compare_base')) or '').strip()

    price_text = 'данных нет'
    volume_text = 'данных нет'
    margin_text = 'данных нет'
    benchmark_text = 'данных нет'

    if margin:
        margin_text = _fmt_percent_value(margin.get('fact_percent'))
        base_margin = margin.get('pg_percent')
        if base_margin is not None:
            benchmark_text = f'SKU Benchmark: маржа базы {_fmt_percent_value(base_margin)}'
        elif compare_base and compare_base != 'product_baseline_missing':
            benchmark_text = 'SKU Benchmark: активен'
    elif compare_base and compare_base != 'product_baseline_missing':
        benchmark_text = 'SKU Benchmark: активен'

    # DATA contract currently has turnover, but not physical volume/price.
    # Revenue is kept in KPI; Product Layer does not invent price or volume.
    if not revenue:
        volume_text = 'данных нет'

    return [
        f'Цена: {price_text}',
        f'Маржа: {margin_text}',
        f'Объём: {volume_text}',
        benchmark_text,
    ]


def _build_product_priority_action_block(response):
    compare_base = str(response.get('compare_base') or ((response.get('context') or {}).get('compare_base')) or '').strip()
    metrics = response.get('metrics') or []
    margin = _metric_lookup(metrics, 'Маржа')
    if compare_base == 'product_baseline_missing':
        return ['Проверить SKU Benchmark → данных для базы нет']
    if margin and _num(margin.get('delta_percent')) < 0:
        return ['Улучшить маржу']
    return ['Проверить SKU Benchmark']

def _normalize_decision(payload):
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    if _is_product_layer_level(level):
        return None
    if level not in DECISION_LEVELS:
        return None

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
            text = _decision_action_text(key, level)
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
    nav_raw = payload.get('navigation') if isinstance(payload.get('navigation'), dict) else {}
    render_mode = 'list_only' if (nav_raw.get('mode') == 'all' or payload.get('view_mode') == 'all') else ('reasons' if (nav_raw.get('mode') == 'reasons' or payload.get('view_mode') == 'reasons') else '')
    context = _normalize_context(payload)
    level = str(context.get('level') or '').strip().lower()
    response = {
        'context': context,
        'path': payload.get('path') or [],
        'summary_block': payload.get('summary_block') or '',
        'result_block': payload.get('result_block') or [],
        'object_result_money': payload.get('object_result_money'),
        'opportunity_money': payload.get('opportunity_money'),
        'navigation_money': payload.get('navigation_money'),
        'net_drain_money': payload.get('net_drain_money'),
        'gross_loss_money': payload.get('gross_loss_money'),
        'internal_drain_money': payload.get('internal_drain_money'),
        'metrics': metrics,
        'structure': structure,
        'drain_block': drain,
        'all_block': payload.get('all_block') or [],
        'navigation': _normalize_navigation(payload, drain),
        'compare_base': payload.get('compare_base') or (payload.get('context') or {}).get('compare_base'),
        'render_mode': render_mode,
    }
    if level == 'business':
        response['business_result_money'] = payload.get('business_result_money')
        response['business_result_rating'] = payload.get('business_result_rating') or []
        response['opportunity_rating'] = payload.get('opportunity_rating') or []
        response['business_reasons'] = payload.get('business_reasons') or []
        response['priority_action'] = payload.get('priority_action')
    else:
        response['object_reasons'] = payload.get('object_reasons') or []
        response['priority_action'] = payload.get('priority_action')
    reasons = _normalize_reasons(payload, structure)
    ctx_level_for_contract = str((response.get('context') or {}).get('level') or '').strip().lower()
    if ctx_level_for_contract == 'business':
        for legacy_key in ('goal', 'goal_block', 'focus_money', 'coverage', 'coverage_percent', 'vector_block', 'path_goal', 'path_goal_money'):
            response.pop(legacy_key, None)
    elif ctx_level_for_contract:
        for legacy_key in ('goal', 'goal_block', 'focus_money', 'coverage', 'coverage_percent', 'vector_block', 'path_goal', 'path_goal_money'):
            response.pop(legacy_key, None)

    if reasons is not None and not _is_product_layer_level(ctx_level_for_contract):
        response['reasons_block'] = reasons
    elif _is_product_layer_level(ctx_level_for_contract):
        response['reasons_block'] = []
    decision = _normalize_decision(payload)
    if decision is not None:
        response['decision_block'] = decision
    rendered = _attach_render_blocks(response, payload)
    # V12.1: make summary explain positive object with opportunity money.
    # Compatibility aliases must not drive Presentation Layer.
    try:
        ctx_level = str((rendered.get('context') or {}).get('level') or '').strip().lower()
        obj_result = rendered.get('object_result_money')
        opportunity = rendered.get('opportunity_money')
        if ctx_level != 'business' and obj_result is not None and opportunity is not None:
            if _num(obj_result) > 0 and _num(opportunity) > 0:
                rendered['summary_block'] = 'Объект в целом даёт плюс, но внутри есть управляемые потери.'
            elif _num(obj_result) < 0 and _num(opportunity) > 0:
                rendered['summary_block'] = rendered.get('summary_block') or 'Объект в минусе и внутри есть управляемый дренаж.'
    except Exception:
        pass
    final_payload = _ensure_vectra_query_render_contract(rendered)
    final_level = str((final_payload.get('context') or {}).get('level') or '').strip().lower()
    if final_level:
        for legacy_key in ('goal', 'goal_block', 'focus_money', 'coverage', 'coverage_percent', 'vector_block', 'path_goal', 'path_goal_money'):
            final_payload.pop(legacy_key, None)
    return final_payload




def _fmt_int(value):
    num = _intnum(value)
    sign = '−' if num < 0 else ''
    return f"{sign}{abs(num):,}".replace(',', ' ')


def _fmt_signed_int(value):
    num = _intnum(value)
    if num > 0:
        return f"+{num:,}".replace(',', ' ')
    if num < 0:
        return f"−{abs(num):,}".replace(',', ' ')
    return '0'


def _fmt_percent(value):
    try:
        value = float(value)
        if not math.isfinite(value):
            value = 0.0
    except Exception:
        value = 0.0
    return f'{value:.2f}'


def _fmt_percent_value(value):
    return f'{_fmt_percent(value)}%'


def _fmt_pp_delta(value):
    num = _num(value)
    sign = '+' if num > 0 else ('−' if num < 0 else '')
    return f'{sign}{abs(num):.2f} п.п.'

def _metric_render_values(item):
    metric_name = str(item.get('name') or '').strip()
    if metric_name in {'Маржа', 'Наценка'}:
        return (
            _fmt_percent_value(item.get('fact_percent')),
            _fmt_percent_value(item.get('pg_percent')),
            _fmt_pp_delta(item.get('delta_percent')),
            'Δ',
        )
    return (
        _fmt_int(item.get('fact_money')),
        _fmt_int(item.get('pg_money')),
        _fmt_signed_int(item.get('delta_money')),
        'Δ к прошлому году',
    )


def _render_kpi_block(metrics):
    lines = []
    for item in metrics:
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        fact, base, delta, delta_label = _metric_render_values(item)
        lines.append(f'{name}: текущий период {fact} | прошлый год {base} | {delta_label} {delta}')
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


def _render_reasons_block(reasons):
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    sorted_reasons = sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99))
    lines = []
    for item in sorted_reasons:
        if not isinstance(item, dict):
            continue
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        money = _fmt_int(item.get('money', item.get('value_money')))
        percent = _fmt_percent(item.get('percent', item.get('value_percent')))
        base_percent = _fmt_percent(item.get('base_percent'))
        prev_money = _fmt_int(item.get('previous_money', item.get('prev_money')))
        prev_missing = bool(item.get('previous_percent_missing')) or item.get('previous_percent', item.get('prev_percent')) is None
        prev_percent = 'нет корректной базы' if prev_missing else _fmt_percent(item.get('previous_percent', item.get('prev_percent')))
        delta_b = _num(item.get('delta_vs_business_percent', item.get('delta_percent')))
        delta_p_raw = item.get('delta_vs_previous_percent', item.get('delta_vs_prev'))
        delta_p = None if delta_p_raw is None else _num(delta_p_raw)
        delta_b_text = f'+{delta_b:.2f}' if delta_b > 0 else f'{delta_b:.2f}'
        delta_p_text = 'нет корректной базы' if delta_p is None else (f'+{delta_p:.2f}' if delta_p > 0 else f'{delta_p:.2f}')
        effect = _fmt_signed_int(item.get('effect_money'))
        signal = str(item.get('signal') or '').strip() or 'норма'
        prev_line = f'прошлый год: {prev_money} грн ({prev_percent}%)'
        if prev_missing:
            prev_line = f'прошлый год: {prev_money} грн (нет корректной базы)'
        delta_prev_line = f'{delta_p_text} п.п. к прошлому году'
        if delta_p is None:
            delta_prev_line = 'нет корректной базы к прошлому году'
        lines.append(
            f'{name}\n'
            f'факт: {money} грн ({percent}%)\n'
            f'бизнес: {base_percent}%\n'
            f'{prev_line}\n\n'
            f'отклонение:\n'
            f'{delta_b_text} п.п. к бизнесу\n'
            f'{delta_prev_line}\n\n'
            f'эффект: {effect}\n'
            f'сигнал: {signal}'
        )
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
    for idx, name in enumerate(names, start=1):
        lines.append(f'{idx} — {name}')

    action_types = [a.get('type') for a in (navigation.get('actions') or []) if isinstance(a, dict)]
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    nav = payload.get('navigation') if isinstance(payload.get('navigation'), dict) else {}
    mode = nav.get('mode') or payload.get('view_mode') or ''

    # Force product navigation commands into every analytical screen.
    if level != 'sku' and mode != 'all':
        if 'all' in action_types or names:
            lines.append('все — полный список')
    if level == 'network':
        lines.append('причины — разбор контракта')
    elif level and not _is_product_layer_level(level):
        lines.append('причины — разбор')
    # v9: no separate 'искать' command; numeric navigation and 'все' are enough.
    if 'back' in action_types or level != 'business':
        lines.append('назад — вверх')

    # De-duplicate while preserving order.
    out = []
    seen = set()
    for line in lines:
        if line and line not in seen:
            out.append(line)
            seen.add(line)
    return out





def _metric_by_name(metrics, wanted):
    wanted_l = str(wanted).strip().lower()
    for item in metrics or []:
        if str(item.get('name') or '').strip().lower() == wanted_l:
            return item
    return {}


def _delta_money_for_metric(item):
    if not isinstance(item, dict):
        return 0
    if item.get('delta_money') is not None:
        return _num(item.get('delta_money'))
    if item.get('fact_money') is not None and item.get('pg_money') is not None:
        return _num(item.get('fact_money')) - _num(item.get('pg_money'))
    return 0


def _delta_percent_for_metric(item):
    if not isinstance(item, dict):
        return 0
    if item.get('delta_percent') is not None:
        return _num(item.get('delta_percent'))
    if item.get('fact_percent') is not None and item.get('pg_percent') is not None:
        return _num(item.get('fact_percent')) - _num(item.get('pg_percent'))
    return 0


def _build_kpi_summary(response):
    """V12.3: summary explains KPI behavior, not structure."""
    metrics = response.get('metrics') or []
    revenue = _metric_by_name(metrics, 'Оборот')
    finrez = _metric_by_name(metrics, 'Финрез до')
    margin = _metric_by_name(metrics, 'Маржа')

    rev_delta = _delta_money_for_metric(revenue)
    fin_delta = _delta_money_for_metric(finrez)
    margin_delta = _delta_percent_for_metric(margin)

    if rev_delta < 0 and fin_delta >= 0:
        return 'Оборот снизился, но финрез удержан за счёт более сильной маржи.'
    if rev_delta < 0 and fin_delta < 0 and margin_delta > 0:
        return 'Оборот просел, маржа выросла и частично компенсировала падение финреза.'
    if rev_delta > 0 and fin_delta > 0:
        return 'Оборот и финрез растут одновременно — объект усиливает результат.'
    if fin_delta < 0:
        return 'Финрез просел — нужен разбор источника потерь ниже.'
    if margin_delta > 0:
        return 'Маржа улучшилась относительно базы.'
    return response.get('summary_block') or ''


def _render_money_value(value):
    if value is None:
        return '—'
    return _fmt_signed_int(value)


def _render_rating_lines(items, money_key):
    lines = []
    for idx, item in enumerate([x for x in (items or []) if isinstance(x, dict)], start=1):
        name = str(item.get('object_name') or item.get('object') or item.get('name') or '').strip()
        if not name:
            continue
        lines.append(f'{idx}. {name} → {_render_money_value(item.get(money_key))}')
    return lines


def _action_display_label(action):
    if not isinstance(action, dict):
        return 'Приоритетное действие'
    code = str(action.get('action') or action.get('metric') or '').strip()
    text = str(action.get('text') or '').strip()
    if code in ACTION_TEXT_MAP:
        return ACTION_TEXT_MAP[code]
    if text in ACTION_TEXT_MAP:
        return ACTION_TEXT_MAP[text]
    return text or (code.replace('_', ' ').strip() if code else 'Приоритетное действие')


def _render_priority_action(response):
    action = response.get('priority_action')
    if not isinstance(action, dict) or not action:
        return []
    text = _action_display_label(action)
    effect = action.get('expected_effect_money')
    if effect is None:
        effect = action.get('effect_money')
    return [f'{text} → ожидаемый эффект {_render_money_value(effect)}']


def _render_business_result_block(response):
    return [f'Результат бизнеса: {_render_money_value(response.get("business_result_money"))}']


def _render_object_result_block(response):
    return [f'Результат объекта: {_render_money_value(response.get("object_result_money"))}']


def _render_opportunity_block(response):
    return [f'Потенциал возврата прибыли: {_render_money_value(response.get("opportunity_money"))}']


def _render_result_block(response):
    """V12 Presentation: business result, object result and opportunity are shown separately."""
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or response.get('level') or '').strip().lower()
    if level == 'business':
        return _render_business_result_block(response)

    lines = []
    lines.extend(_render_object_result_block(response))
    lines.extend(_render_opportunity_block(response))
    return lines


def _metric_fact_revenue(response):
    for item in response.get('metrics') or []:
        if str(item.get('name') or '').strip().lower() == 'оборот':
            return abs(_num(item.get('fact_money')))
    return 0.0


def _render_decision_block(response):
    priority_lines = _render_priority_action(response)
    if priority_lines:
        return priority_lines

    decision = response.get('decision_block')
    if isinstance(decision, list) and decision:
        lines = []
        for item in decision:
            if not isinstance(item, dict):
                continue
            text = _action_display_label(item)
            effect = item.get('expected_effect_money')
            if effect is None:
                effect = item.get('effect_money')
            lines.append(f'{text} → ожидаемый эффект {_render_money_value(effect)}')
        return lines
    return []


def _attach_render_blocks(response, payload):
    metrics = response.get('metrics') or []
    structure = response.get('structure') or []
    drain = response.get('drain_block') or {'items': [], 'total_effect': 0}
    navigation = response.get('navigation') or {'actions': []}
    render_mode = response.get('render_mode') or ''

    drain_total = _intnum(response.get('navigation_money') if response.get('navigation_money') is not None else drain.get('total_effect'))
    ctx_level_for_main_driver = str((response.get('context') or {}).get('level') or '').strip().lower()

    # In list-only mode, the screen is a navigation list, not an object analysis screen.
    ctx_level = str((response.get('context') or {}).get('level') or '').strip().lower()
    if render_mode == 'list_only':
        response['result_block'] = []
        response['kpi_block'] = []
        response['structure_block'] = []
        response['main_driver'] = ''
        response['summary_block'] = 'Полный список объектов текущего уровня.'
        response['decision_block_render'] = []
        response['business_result_rating_block'] = []
        response['opportunity_rating_block'] = []
        response['priority_action_block'] = []
        response['object_reasons_block'] = []
        response['product_layer_block'] = []
    else:
        response['kpi_block'] = _render_kpi_block(metrics)
        response['summary_block'] = _build_kpi_summary(response)
        response['result_block'] = _render_result_block(response)
        response['business_result_rating_block'] = _render_rating_lines(response.get('business_result_rating') or [], 'object_result_money')
        response['opportunity_rating_block'] = _render_rating_lines(response.get('opportunity_rating') or [], 'opportunity_money')
        if _is_product_layer_level(ctx_level_for_main_driver):
            response['structure_block'] = []
            response['main_driver'] = 'Product Layer'
            response['product_layer_block'] = _build_product_layer_block(response)
            response['priority_action_block'] = _build_product_priority_action_block(response)
            response['object_reasons_block'] = []
            response['reasons_block'] = []
            response['decision_block'] = []
        else:
            response['structure_block'] = _render_structure_block(structure)
            response['main_driver'] = _render_main_driver(structure)
            response['product_layer_block'] = []
            response['priority_action_block'] = _render_priority_action(response)
            response['object_reasons_block'] = _render_reasons_block(response.get('object_reasons') or [])

    ctx_level = str((response.get('context') or {}).get('level') or '').strip().lower()
    if ctx_level == 'sku':
        rendered_sku_drain = _render_drain_block(drain)
        if not rendered_sku_drain:
            response['drain_total'] = drain_total
        else:
            response['drain_total'] = drain_total
        response['drain_block_render'] = rendered_sku_drain
    else:
        response['drain_block_render'] = _render_drain_block(drain)
        response['drain_total'] = drain_total
    response['navigation_block'] = _render_navigation_block(payload, navigation, drain)
    if _is_product_layer_level(ctx_level):
        response['decision_block_render'] = list(response.get('priority_action_block') or [])
        response['reasons_block_render'] = []
    else:
        response['decision_block_render'] = _render_decision_block(response)
        response['reasons_block_render'] = _render_reasons_block(response.get('reasons_block') or [])
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


def _is_render_ready_payload(payload):
    if not isinstance(payload, dict):
        return False
    render_keys = {'kpi_block', 'structure_block', 'navigation_block', 'drain_block_render', 'result_block'}
    return bool(render_keys.intersection(payload.keys())) and isinstance(payload.get('context'), dict)


def _prepare_vectra_query_payload(payload):
    """Normalize only raw API/domain summaries.

    UI/state commands (все / причины / назад) may already return a final
    render-ready screen. Re-normalizing that screen through public_summary()
    breaks it because render screens do not carry the raw metrics contract.
    """
    if _is_render_ready_payload(payload):
        ready = dict(payload)
        ready.setdefault('status', 'ok')
        return _ensure_vectra_query_render_contract(_force_product_navigation(ready))
    return _ensure_vectra_query_render_contract(public_summary(payload))


def _force_product_navigation(payload):
    """Final product navigation guard for /vectra/query render-only payload.

    Keeps product commands visible even when upstream navigation was produced
    by an older/raw view. Does not change calculations, drain, vector or KPI.
    """
    if not isinstance(payload, dict) or payload.get('status') == 'error':
        return payload

    nav = payload.get('navigation_block') or []
    if not isinstance(nav, list):
        nav = []

    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    render_mode = str(payload.get('render_mode') or '').strip().lower()

    # A numeric line starts navigation to a concrete child object.
    has_numeric_items = any(str(line).strip()[:1].isdigit() for line in nav)
    out = []
    seen = set()

    def add(line):
        if line and line not in seen:
            out.append(line)
            seen.add(line)

    for line in nav:
        add(str(line))

    # Full list / object screen commands.
    if level != 'sku' and render_mode != 'list_only' and has_numeric_items:
        add('все — полный список')

    # Contract reasons are available only through Network. Product Layer does not expose Contract Reasons.
    if level == 'network':
        add('причины — разбор контракта')
    elif level and level not in {'start'} and not _is_product_layer_level(level):
        add('причины — разбор')

    # v9: no separate 'искать' command; numeric navigation and 'все' are enough.

    # Back should exist below business and in list/reasons modes.
    if level and level not in {'business', 'start'}:
        add('назад — вверх')
    elif render_mode in {'list_only', 'reasons'}:
        add('назад — вверх')

    payload['navigation_block'] = out
    return payload




@router.post('/vectra/query', summary='Stateful VECTRA Query')
def vectra_query(request: VectraQueryRequest):
    session_id = _stable_session_id(request)
    logger.info('vectra_query_received session_id=%s message=%r', session_id, request.message)
    
    # State/UI commands (все / причины / назад) are handled only inside
    # orchestration.py. routes.py is now only API/render boundary.

    payload = orchestrate_vectra_query(request.message, session_id=session_id)
    logger.info('vectra_query_result session_id=%s status=%s reason=%s', session_id, payload.get('status'), payload.get('reason'))
    rendered_payload = _prepare_vectra_query_payload(payload)
    render_only_payload = {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'compare_base': rendered_payload.get('compare_base'),
        'kpi_block': rendered_payload.get('kpi_block', []),
        'structure_block': rendered_payload.get('structure_block', []),
        'main_driver': rendered_payload.get('main_driver', ''),
        'drain_block_render': rendered_payload.get('drain_block_render', []),
        'drain_total': rendered_payload.get('drain_total', 0),
        'all_block': rendered_payload.get('all_block', []),
        'navigation_block': rendered_payload.get('navigation_block', []),
        'summary_block': rendered_payload.get('summary_block', ''),
        'result_block': rendered_payload.get('result_block', []),
        'path': rendered_payload.get('path', []),
        'reasons_block': rendered_payload.get('reasons_block', []),
        'reasons_block_render': rendered_payload.get('reasons_block_render', []),
        'decision_block': rendered_payload.get('decision_block', []),
        'decision_block_render': rendered_payload.get('decision_block_render', []),
        'business_result_rating_block': rendered_payload.get('business_result_rating_block', []),
        'opportunity_rating_block': rendered_payload.get('opportunity_rating_block', []),
        'priority_action_block': rendered_payload.get('priority_action_block', []),
        'object_reasons_block': rendered_payload.get('object_reasons_block', []),
        'product_layer_block': rendered_payload.get('product_layer_block', []),
        'render_mode': rendered_payload.get('render_mode', ''),
        'business_result_money': rendered_payload.get('business_result_money'),
        'object_result_money': rendered_payload.get('object_result_money'),
        'opportunity_money': rendered_payload.get('opportunity_money'),
        'navigation_money': rendered_payload.get('navigation_money'),
        'net_drain_money': rendered_payload.get('net_drain_money'),
        'gross_loss_money': rendered_payload.get('gross_loss_money'),
        'internal_drain_money': rendered_payload.get('internal_drain_money'),
    }
    render_only_payload = _force_product_navigation(render_only_payload)
    # Persist only analytical object/list screens at the API boundary.
    # UI display modes (все / причины) are produced by orchestration.py and
    # must not overwrite current_screen; otherwise the next «назад» would
    # return to a display mode instead of the object screen.
    if render_only_payload.get('status') != 'error' and render_only_payload.get('render_mode') not in {'list_only', 'reasons'}:
        try:
            save_last_payload(session_id, render_only_payload)
        except Exception:
            logger.exception('vectra_query_render_state_save_failed session_id=%s', session_id)
    logger.info(
        'vectra_query_render_contract session_id=%s has_kpi_block=%s has_structure_block=%s has_drain_block_render=%s has_navigation_block=%s has_result_block=%s',
        session_id,
        'kpi_block' in render_only_payload,
        'structure_block' in render_only_payload,
        'drain_block_render' in render_only_payload,
        'navigation_block' in render_only_payload,
        'result_block' in render_only_payload,
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
