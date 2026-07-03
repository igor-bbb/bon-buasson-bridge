import math
import logging
import json
import re

import hashlib
from typing import Any

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
from app.domain.filters import get_normalized_rows, filter_rows
from app.domain.metrics import aggregate_metrics
from app.query.entity_dictionary import get_entity_dictionary
from app.query.orchestration import orchestrate_vectra_query, save_last_payload, update_session, get_session
from app.workspace_runtime import apply_runtime_contract
from app.development_journal import (
    add_runtime_event as add_development_journal_runtime_event,
    add_global_record as add_development_journal_global_record,
    build_capture_response as build_development_journal_capture_response,
    build_journal_response as build_development_journal_response,
    analyze_dialogue_and_create_records as analyze_development_journal_dialogue,
    build_dialogue_review_response as build_development_journal_dialogue_review_response,
)

router = APIRouter()
logger = logging.getLogger(__name__)

# Sprint 11: final public payload guard for Custom GPT Actions.
# The exact platform limit is not exposed to the API, so we keep a conservative
# budget and trim non-essential render blocks before the response leaves API.
VECTRA_PUBLIC_RESPONSE_BUDGET = 90000
VECTRA_PUBLIC_RESPONSE_HARD_BUDGET = 120000


PUBLIC_TOP_LEVEL_KEYS = ('profit_loss_rating', 'opportunity_rating', 'business_reasons', 'priority_action', 'period_result_block', 'opportunity_money', 'navigation_money', 'net_drain_money', 'gross_loss_money', 'internal_drain_money', 'compare_base', 'context', 'metrics', 'structure', 'drain_block', 'all_block', 'navigation', 'reasons_block', 'decision_block', 'decision_block_render', 'reasons_block_render', 'kpi_block', 'structure_block', 'main_driver', 'drain_block_render', 'drain_total', 'navigation_block', 'summary_block', 'explanation_block', 'next_step_block', 'product_layer_block', 'product_insight_block', 'product_tmc_decision_block', 'path', 'diagnosis_block', 'recommended_next_step_block', 'opportunity_explanation_block', 'anomaly_explanation_block', 'screen_order', 'kpi_table', 'factor_change_table', 'benchmark_diagnostic_table', 'decision_workspace', 'decision_workspace_block', 'sku_passport', 'sku_passport_block', 'business_context', 'business_context_block', 'category_workspace', 'category_workspace_block', 'business_opportunity', 'business_opportunity_block', 'recommendation_engine', 'recommendation_block', 'narrative_engine', 'narrative_block', 'product_workspace', 'product_workspace_block', 'management_intelligence', 'management_workspace', 'management_passport', 'management_workspace_block', 'business_workspace_block', 'contract_workspace_block')

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
    'explanation_block': [],
    'next_step_block': [],
    'diagnosis_block': [],
    'recommended_next_step_block': [],
    'opportunity_explanation_block': [],
    'anomaly_explanation_block': [],
    'screen_order': [],
    'kpi_table': [],
    'factor_change_table': [],
    'benchmark_diagnostic_table': [],
    'product_layer_block': [],
    'product_insight_block': [],
    'path': [],
    'decision_block': [],
    'decision_block_render': [],
    'render_mode': '',
    'decision_workspace': {},
    'decision_workspace_block': [],
    'sku_passport': {},
    'sku_passport_block': [],
    'business_context': {},
    'business_context_block': [],
    'category_workspace': {},
    'category_workspace_block': [],
    'business_opportunity': {},
    'business_opportunity_block': [],
    'recommendation_engine': {},
    'recommendation_block': [],
    'narrative_engine': {},
    'narrative_block': [],
    'product_workspace': {},
    'product_workspace_block': [],
    'management_intelligence': {},
    'management_workspace': {},
    'management_passport': {},
    'management_workspace_block': [],
    'business_workspace_block': [],
    'contract_workspace_block': [],
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
    if parent_object is None:
        flt = payload.get('filter') if isinstance(payload.get('filter'), dict) else {}
        if level in {'category', 'tmc_group', 'sku'}:
            parent_object = flt.get('network') or flt.get('manager') or flt.get('manager_top')
    # Use the machine-readable period selector for calculations/render helpers.
    # Some management views format range periods for display as "YYYY-MM → YYYY-MM"
    # in context.period; that string is not accepted by filter_rows().
    out = {
        'level': level,
        'object_name': object_name or 'Бизнес',
        'period': payload.get('period') or ctx.get('period'),
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

    # Navigation Contract v1.2 / BUG-006 FIX-002:
    # all_block is the only source for rendered navigation/drain lists.
    # Do not fall back to drain_block/items/navigation.items.
    all_block = payload.get('all_block') if isinstance(payload.get('all_block'), list) else []
    source_items = all_block if is_all_mode else all_block[:3]

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
        profit_delta = entry.get('profit_delta_money')
        if profit_delta is None:
            profit_delta = entry.get('delta_money')
        items.append({
            'object_name': entry.get('object_name') or entry.get('name'),
            'object_id': entry.get('object_id', idx),
            'effect_money': -abs(nav_money),
            'navigation_money': nav_money,
            'profit_delta_money': _intnum(profit_delta),
            'opportunity_money': _intnum(entry.get('opportunity_money')),
        })

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

    # Navigation Contract v1.2 / BUG-006 FIX-002:
    # fallback navigation actions may be created only from normalized drain,
    # and normalized drain is sourced exclusively from all_block.
    if drain.get('items'):
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


def _product_compare_base_label(response: dict) -> str:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    compare_base = str(response.get('compare_base') or ctx.get('compare_base') or '').strip()
    if level == 'category' or compare_base == 'category_business':
        return 'такой же категории бизнеса'
    if level == 'tmc_group' or compare_base == 'tmc_group_business':
        return 'такой же группы ТМС бизнеса'
    if level == 'sku' or compare_base == 'sku_business':
        return 'этой же позиции по бизнесу'
    if compare_base == 'sku_fallback_tmc_group':
        return 'такой же группы ТМС бизнеса'
    if compare_base == 'sku_fallback_category':
        return 'такой же категории бизнеса'
    return 'среднего уровня бизнеса'




def _pi72_previous_year_period(period: str) -> str:
    if isinstance(period, str) and len(period) == 7 and period[4] == '-':
        try:
            return f"{int(period[:4]) - 1:04d}-{period[5:7]}"
        except Exception:
            return ''
    return ''


def _pi72_filter_rows(period: str = '', network: str = '', category: str = '', tmc_group: str = '', sku: str = ''):
    try:
        kwargs = {}
        if network:
            kwargs['network'] = network
        if category:
            kwargs['category'] = category
        if tmc_group:
            kwargs['tmc_group'] = tmc_group
        if sku:
            kwargs['sku'] = sku
        rows, _ = filter_rows(get_normalized_rows(), period=period, **kwargs)
        return rows or []
    except Exception:
        logger.exception('pi72_filter_rows_failed')
        return []


def _pi72_extract_network_from_path(response: dict) -> str:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    parent = str(ctx.get('parent_object') or '').strip()
    if parent:
        return parent
    path = response.get('path') if isinstance(response.get('path'), list) else []
    # Path convention: Business -> Top Manager -> Manager -> Network -> Category
    if len(path) >= 4:
        return str(path[3] or '').strip()
    return ''


def _pi72_format_name(value: str) -> str:
    text = str(value or '')
    low = text.lower().replace(',', '.').replace(' ', '')
    # Keep longer tokens first.
    patterns = [
        ('1.5л', '1,5 л'), ('1.5l', '1,5 л'), ('0.75л', '0,75 л'), ('0.75l', '0,75 л'),
        ('0.5л', '0,5 л'), ('0.5l', '0,5 л'), ('0,75л', '0,75 л'), ('0,5л', '0,5 л'),
        ('5л', '5 л'), ('2л', '2 л'), ('1л', '1 л'),
    ]
    for token, label in patterns:
        if token in low:
            return label
    m = re.search(r'(\d+(?:[\.,]\d+)?)\s*[лl]', text.lower())
    if m:
        return m.group(1).replace('.', ',') + ' л'
    return 'без формата'


def _pi72_role_for_share(share: float, idx: int) -> str:
    if idx == 0 and share >= 60:
        return 'основной драйвер'
    if share >= 20:
        return 'сильный формат'
    if share > 0:
        return 'дополнительный формат'
    return 'отсутствует'


def _pi72_category_format_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'category':
        return []
    category = str(ctx.get('object_name') or '').strip()
    period = str(ctx.get('period') or '').strip()
    network = _pi72_extract_network_from_path(response)
    if not category or not period or not network:
        return []

    rows = _pi72_filter_rows(period=period, network=network, category=category)
    if not rows:
        return []
    prev_period = _pi72_previous_year_period(period)
    prev_rows = _pi72_filter_rows(period=prev_period, network=network, category=category) if prev_period else []
    business_rows = _pi72_filter_rows(period=period, category=category)

    total_metrics = aggregate_metrics(rows) if rows else {}
    total_revenue = _num(total_metrics.get('revenue'))
    total_finrez = _num(total_metrics.get('finrez_pre'))

    grouped = {}
    prev_grouped = {}
    business_grouped = {}
    for row in rows:
        fmt = _pi72_format_name(row.get('tmc_group') or row.get('sku'))
        grouped.setdefault(fmt, []).append(row)
    for row in prev_rows:
        fmt = _pi72_format_name(row.get('tmc_group') or row.get('sku'))
        prev_grouped.setdefault(fmt, []).append(row)
    for row in business_rows:
        fmt = _pi72_format_name(row.get('tmc_group') or row.get('sku'))
        business_grouped.setdefault(fmt, []).append(row)

    items = []
    for fmt, fmt_rows in grouped.items():
        cur = aggregate_metrics(fmt_rows) if fmt_rows else {}
        prv = aggregate_metrics(prev_grouped.get(fmt) or []) if prev_grouped.get(fmt) else {}
        biz = aggregate_metrics(business_grouped.get(fmt) or []) if business_grouped.get(fmt) else {}
        revenue = _num(cur.get('revenue'))
        finrez = _num(cur.get('finrez_pre'))
        prev_finrez = _num(prv.get('finrez_pre'))
        biz_revenue = _num(biz.get('revenue'))
        items.append({
            'format': fmt,
            'revenue': revenue,
            'finrez': finrez,
            'delta_profit': finrez - prev_finrez,
            'share': (revenue / total_revenue * 100.0) if total_revenue else 0.0,
            'profit_share': (finrez / total_finrez * 100.0) if abs(total_finrez) > 1e-9 else 0.0,
            'business_revenue': biz_revenue,
            'sku_count': len({str(r.get('sku')) for r in fmt_rows if r.get('sku')}),
        })
    items.sort(key=lambda x: abs(x.get('delta_profit') or 0), reverse=True)
    if not items:
        return []

    top = items[0]
    lines = ['📦 Структура категории по форматам']
    if top.get('share', 0) >= 80:
        lines.append(f"Главный результат категории сконцентрирован в формате {top.get('format')}: доля оборота {_fmt_percent(top.get('share'))}%, Δ прибыли {_fmt_signed_int(top.get('delta_profit'))}.")
        lines.append('Это сильная сторона и одновременно риск концентрации: если формат просядет, категория потеряет основной источник результата.')
    else:
        lines.append('Результат категории распределён между несколькими форматами. Решение по развитию стоит принимать по формату, а не сразу по отдельным позициям.')
    lines.append('Формат | Оборот | Финрез до | Δ прибыли | Доля категории | SKU | Роль')
    for idx, item in enumerate(items[:8]):
        lines.append(
            f"{item.get('format')} | {_fmt_int(item.get('revenue'))} грн | {_fmt_signed_int(item.get('finrez'))} грн | "
            f"{_fmt_signed_int(item.get('delta_profit'))} грн | {_fmt_percent(item.get('share'))}% | {item.get('sku_count')} | {_pi72_role_for_share(item.get('share') or 0, idx)}"
        )
    if top.get('share', 0) >= 60:
        lines.append(f"Управленческий вывод: сначала развивать линейку/формат {top.get('format')}, затем переходить к конкретным SKU внутри формата.")
    else:
        lines.append('Управленческий вывод: выбрать формат с лучшим сочетанием доли, прироста и управляемости, затем формировать пакет SKU.')
    return lines

def _build_product_tmc_decision_block(response):
    data = response.get('product_tmc_decision') if isinstance(response.get('product_tmc_decision'), dict) else {}
    items = [x for x in (data.get('items') or []) if isinstance(x, dict)]
    format_lines = _pi72_category_format_block(response)
    if not items:
        return format_lines
    mode = data.get('mode') or 'distributed'
    dominant = data.get('dominant_item') if isinstance(data.get('dominant_item'), dict) else (items[0] if items else {})
    lines = []
    if mode == 'dominant' and dominant:
        lines.append(
            f"Основной вклад внутри категории формирует группа ТМС: {dominant.get('object_name')} "
            f"({_fmt_signed_int(dominant.get('profit_delta_money'))}, доля { _fmt_percent(dominant.get('share_percent')) }%)."
        )
        markup_delta = _num(dominant.get('markup_delta_percent'))
        effect = _num(dominant.get('benchmark_effect_money'))
        if markup_delta > 0:
            lines.append(
                f"Группа выше бизнеса по наценке на {_fmt_pp_delta(markup_delta)} "
                f"(эффект {_fmt_signed_int(effect)}). Рекомендация: масштабировать сильную группу и проверить развитие форматов внутри неё."
            )
        elif markup_delta < 0:
            lines.append(
                f"Группа ниже бизнеса по наценке на {_fmt_pp_delta(markup_delta)} "
                f"(потенциал до {_fmt_int(abs(effect))}). Рекомендация: проверить цену/наценку внутри группы."
            )
        else:
            lines.append('Группа концентрирует результат; следующий шаг — подтвердить устойчивость на уровне форматов и позиций.')
        return lines + ([''] if format_lines else []) + format_lines

    lines.append('Результат категории распределён между несколькими группами ТМС:')
    for idx, item in enumerate(items[:5], start=1):
        lines.append(f"{idx}. {item.get('object_name')} → {_fmt_signed_int(item.get('profit_delta_money'))}, доля {_fmt_percent(item.get('share_percent'))}%")
    lines.append('Рекомендация: сначала выбрать продуктовую группу/формат развития, затем переходить к конкретным позициям.')
    return lines + ([''] if format_lines else []) + format_lines


def _fmt_rank(value):
    try:
        if value is None:
            return '—'
        return f'№{int(value)}'
    except Exception:
        return '—'



def _render_business_context_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level == 'business':
        return []
    bc = response.get('business_context') if isinstance(response.get('business_context'), dict) else {}
    if not bc:
        workspace = response.get('decision_workspace') if isinstance(response.get('decision_workspace'), dict) else {}
        bc = workspace.get('business_context') if isinstance(workspace.get('business_context'), dict) else {}
    if not bc or bc.get('type') == 'business_root':
        return []
    kpi = bc.get('kpi') if isinstance(bc.get('kpi'), dict) else {}
    structure = bc.get('structure') if isinstance(bc.get('structure'), dict) else {}
    factors = bc.get('factors') if isinstance(bc.get('factors'), list) else []
    formats = bc.get('formats') if isinstance(bc.get('formats'), dict) else {}
    missing_formats = formats.get('missing_business_formats') if isinstance(formats.get('missing_business_formats'), list) else []
    lines = [
        '📍 Положение относительно бизнеса',
        f'Доля оборота в бизнес-референсе: {_fmt_percent(kpi.get("revenue_share_business_percent"))}%',
        f'Доля финреза до в бизнес-референсе: {_fmt_percent(kpi.get("profit_share_business_percent"))}%',
        f'Маржа: объект {_fmt_percent(kpi.get("margin_object_percent"))}% / бизнес {_fmt_percent(kpi.get("margin_business_percent"))}% / Δ {_fmt_pp_delta(kpi.get("margin_delta_pp"))}',
        f'Наценка: объект {_fmt_percent(kpi.get("markup_object_percent"))}% / бизнес {_fmt_percent(kpi.get("markup_business_percent"))}% / Δ {_fmt_pp_delta(kpi.get("markup_delta_pp"))}',
        '',
        'Структура относительно бизнеса:',
        f'Категории: {structure.get("object_category_count") or 0} из {structure.get("business_category_count") or 0}',
        f'Группы ТМС: {structure.get("object_tmc_group_count") or 0} из {structure.get("business_tmc_group_count") or 0}',
        f'SKU: {structure.get("object_sku_count") or 0} из {structure.get("business_sku_count") or 0}',
    ]
    if factors:
        lines.extend(['', 'Ключевые отклонения факторов:'])
        for item in factors[:3]:
            if not isinstance(item, dict):
                continue
            lines.append(f'{item.get("name") or item.get("factor")} → Δ {_fmt_pp_delta(item.get("delta_pp"))}, эффект {_fmt_signed_int(item.get("effect_money"))} грн')
    if missing_formats:
        names = [str(item.get('format')) for item in missing_formats[:5] if isinstance(item, dict) and item.get('format')]
        if names:
            lines.extend(['', 'Форматы, которые есть в бизнес-референсе, но отсутствуют в текущем объекте: ' + ', '.join(names) + '.'])
    return [line for line in lines if str(line or '').strip()]


def _render_category_workspace_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'category':
        return []
    workspace = response.get('category_workspace') if isinstance(response.get('category_workspace'), dict) else {}
    if not workspace:
        return []
    category = workspace.get('category') or ctx.get('object_name') or 'категория'
    period = workspace.get('period') or ctx.get('period') or ''
    formats = workspace.get('formats') if isinstance(workspace.get('formats'), list) else []
    missing_formats = workspace.get('missing_business_formats') if isinstance(workspace.get('missing_business_formats'), list) else []
    sku_leaders = workspace.get('sku_leaders') if isinstance(workspace.get('sku_leaders'), list) else []
    missing_sku = workspace.get('missing_business_sku_leaders') if isinstance(workspace.get('missing_business_sku_leaders'), list) else []
    strategy = workspace.get('strategy') if isinstance(workspace.get('strategy'), dict) else {}
    lines = [
        f'📦 Рабочий стол категории: {category}' + (f' | {period}' if period else ''),
        '',
        '🧠 Продуктовый разбор',
        f'Δ прибыли категории к прошлому году: {_fmt_signed_int(workspace.get("profit_delta_money"))} грн.',
    ]
    if formats:
        lines.extend(['', 'Форматы внутри категории:', 'Формат | Оборот | Доля категории | SKU | Сетей'])
        for item in formats[:8]:
            if not isinstance(item, dict):
                continue
            lines.append(f'{item.get("format") or "—"} | {_fmt_int(item.get("revenue"))} грн | {_fmt_percent(item.get("share_revenue_percent"))}% | {_fmt_int(item.get("sku_count"))} | {_fmt_int(item.get("network_count"))}')
    if missing_formats:
        lines.extend(['', 'Форматы из бизнес-референса, которых нет в текущем объекте:'])
        for item in missing_formats[:5]:
            if isinstance(item, dict):
                lines.append(f'{item.get("format") or "—"} → оборот бизнеса {_fmt_int(item.get("revenue"))} грн, SKU {_fmt_int(item.get("sku_count"))}')
    if sku_leaders:
        lines.extend(['', 'Лидеры SKU категории:', 'SKU | Оборот | Доля категории | Доля в бизнес-категории | Сетей | Формат'])
        for item in sku_leaders[:8]:
            if not isinstance(item, dict):
                continue
            lines.append(f'{item.get("sku") or "—"} | {_fmt_int(item.get("revenue"))} грн | {_fmt_percent(item.get("share_category_percent"))}% | {_fmt_percent(item.get("share_business_category_percent"))}% | {_fmt_int(item.get("network_count"))} | {item.get("format") or "—"}')
    if missing_sku:
        lines.extend(['', 'Отсутствующие SKU-лидеры бизнес-референса категории:'])
        for item in missing_sku[:8]:
            if isinstance(item, dict):
                lines.append(f'{item.get("sku") or "—"} | {_fmt_int(item.get("business_revenue"))} грн | {_fmt_signed_int(item.get("business_finrez_pre"))} грн | {item.get("format") or "—"}')
    lines.extend(['', '🚀 План развития категории'])
    if strategy.get('format_gap_exists'):
        lines.append('1. Начать с проверки отсутствующих форматов: бизнес уже показывает, какие форматы могут расширить категорию.')
    else:
        lines.append('1. Усиливать текущие форматы и защищать позиции-лидеры.')
    if missing_sku:
        lines.append(f'2. Собрать пакет из отсутствующих SKU-лидеров: сейчас найдено {strategy.get("sku_gap_count") or len(missing_sku)} кандидатов.')
    lines.append('3. Перейти к Product рабочий стол по ключевому SKU или подготовить переговорный аргумент по категории.')
    return [line for line in lines if str(line or '').strip()]

def _build_sku_passport_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'sku':
        return []
    passport = response.get('sku_passport') if isinstance(response.get('sku_passport'), dict) else {}
    if not passport:
        return []
    ident = passport.get('identification') if isinstance(passport.get('identification'), dict) else {}
    role = passport.get('business_role') if isinstance(passport.get('business_role'), dict) else {}
    eco = passport.get('economics') if isinstance(passport.get('economics'), dict) else {}
    presence = passport.get('presence') if isinstance(passport.get('presence'), dict) else {}
    decision = passport.get('decision') if isinstance(passport.get('decision'), dict) else {}
    lines = [
        f'🧾 Паспорт SKU: {passport.get("sku") or ctx.get("object_name")}',
        f'Период: {passport.get("period") or ctx.get("period")}',
        '',
        '## Идентификация',
        f'Категория: {ident.get("category") or "—"}',
        f'Группа ТМС: {ident.get("tmc_group") or "—"}',
        f'Формат: {ident.get("format") or "—"}',
    ]
    if passport.get('contract'):
        lines.append(f'Текущий контракт: {passport.get("contract")}')
    lines.extend([
        '',
        '## Роль в бизнесе',
        f'Доля в бизнесе: {_fmt_percent(role.get("business_share_percent"))}%',
        f'Доля в категории: {_fmt_percent(role.get("category_share_percent"))}%',
        f'Доля в группе/формате: {_fmt_percent(role.get("tmc_group_share_percent"))}%',
        f'Рейтинг по обороту в бизнесе: {_fmt_rank(role.get("rank_revenue_business"))}',
        f'Рейтинг по прибыли в бизнесе: {_fmt_rank(role.get("rank_profit_business"))}',
        f'Рейтинг по обороту в категории: {_fmt_rank(role.get("rank_revenue_category"))}',
        f'Представленность: {role.get("network_count") or 0} из {role.get("total_network_count") or 0} сетей',
        f'Роль SKU: {role.get("role") or "роль не определена"}',
        '',
        '## Экономика SKU',
        f'Оборот в текущем контексте: {_fmt_int(eco.get("revenue"))} грн',
        f'Финрез до: {_fmt_signed_int(eco.get("finrez_pre"))} грн',
        f'Δ прибыли к прошлому году: {_fmt_signed_int(eco.get("profit_delta_money"))} грн',
        f'Маржа: {_fmt_percent(eco.get("margin_pre_percent"))}%',
        f'Наценка: {_fmt_percent(eco.get("markup_percent"))}%',
        '',
        '## Где SKU работает лучше всего',
        'Сеть | Оборот | Финрез до | Доля продаж SKU',
    ])
    top_networks = presence.get('top_networks') if isinstance(presence.get('top_networks'), list) else []
    if top_networks:
        for item in top_networks[:5]:
            if isinstance(item, dict):
                lines.append(f'{item.get("network") or "—"} | {_fmt_int(item.get("revenue"))} грн | {_fmt_signed_int(item.get("finrez_pre"))} грн | {_fmt_percent(item.get("share_sku_percent"))}%')
    else:
        lines.append('Нет подтверждённых сетей по этому SKU в текущем периоде.')
    missing = presence.get('missing_networks') if isinstance(presence.get('missing_networks'), list) else []
    if missing:
        lines.extend(['', '## Где SKU отсутствует', ', '.join(str(x) for x in missing[:10])])
    lines.extend([
        '',
        '## Управленческий вывод',
        decision.get('development_logic') or 'использовать как доказательную базу по позиции',
        '',
        '## Что делаем дальше',
        'подготовить переговоры — использовать паспорт SKU как аргумент',
        'создать задачу — зафиксировать действие по позиции',
        'назад — вернуться уровнем выше',
    ])
    limitations = decision.get('data_limitations') if isinstance(decision.get('data_limitations'), list) else []
    if limitations:
        lines.extend(['', 'Ограничение текущей версии: ' + '; '.join(str(x) for x in limitations) + '.'])
    return lines


def _build_product_layer_block(response):
    """Product Layer 2.0.

    Explains the commercial product logic available from current DATA.
    It does not invent price, stock, shelf or promo data; those remain future Data Mart layers.
    """
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level == 'category':
        return [
            'Как читать категорию:',
            '1. Сначала определить группу/формат, который формирует результат.',
            '2. Проверить концентрацию: один формат тянет категорию или результат распределён.',
            '3. После этого выбирать конкретные позиции для развития или ввода.',
            'Недоступные пока слои: цена, остатки, полка, промо и мерчендайзинг — будут добавлены через будущий Data Mart.',
        ]
    if level == 'tmc_group':
        return [
            'Как читать группу/формат:',
            '1. Оценить роль формата в категории.',
            '2. Проверить позиции внутри формата.',
            '3. Сформировать очередь SKU для развития или ввода.',
        ]
    return [
        'Что влияет на результат позиции:',
        'Экономика, представленность, роль в категории, работа в сетях и доказательная база для переговоров.',
        'Цена, остатки, полка и промо будут добавлены после расширения Data Mart.',
    ]


def _build_product_insight_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    object_name = str(ctx.get('object_name') or ctx.get('name') or '').strip()
    object_label = object_name or ('позиция' if level == 'sku' else 'продукт')
    obj_result = _num(response.get('object_result_money'))
    opportunity = _num(response.get('opportunity_money'))
    base_label = _product_compare_base_label(response)

    if obj_result > 0:
        result_line = f'{object_label} работает лучше, чем {base_label}, на {_fmt_int(obj_result)} грн.'
    elif obj_result < 0:
        result_line = f'{object_label} работает хуже, чем {base_label}, на {_fmt_int(abs(obj_result))} грн.'
    else:
        result_line = f'{object_label} находится около уровня: {base_label}.'

    if opportunity > 0:
        opportunity_line = f'Внутри продукта остаётся {_fmt_int(opportunity)} потенциала прибыли.'
    else:
        opportunity_line = 'Существенный продуктовый потенциал прибыли не выявлен.'

    if level == 'category':
        next_line = 'Товарные группы анализируются внутри категории динамически; отдельный шаг нужен только если результат распределён между группами.'
    elif level == 'tmc_group':
        next_line = 'Следующий шаг — проверить позиции внутри группы как доказательный уровень.'
    else:
        next_line = 'Позиция является диагностическим уровнем; окончательная причина требует данных витрины данных по цене, объёму, ассортименту и структуры ассортимента.'

    return ['Что это означает?', result_line, opportunity_line, next_line]

def _build_product_priority_action_block(response):
    compare_base = str(response.get('compare_base') or ((response.get('context') or {}).get('compare_base')) or '').strip()
    reasons = _available_reasons(response)
    markup = None
    for reason in reasons:
        if str(reason.get('name') or '').strip().lower() == 'наценка':
            markup = reason
            break
    if compare_base == 'product_baseline_missing':
        return ['Проверить продуктовую эффективность → нет корректной базы сравнения']
    gap_reasons = _opportunity_gap_reasons(response, limit=1)
    if gap_reasons:
        reason = gap_reasons[0]
        effect = abs(_reason_effect_vs_business(reason))
        return [f'{_action_text_for_reason(reason)} → потенциальный эффект до {_fmt_int(effect)} относительно {_product_compare_base_label(response)}']
    if markup:
        delta = _num(markup.get('delta_vs_business_percent', markup.get('delta_percent')))
        if delta > 0:
            return [f'Сохранить сильную наценку → преимущество {_fmt_pp_delta(delta)} относительно {_product_compare_base_label(response)}']
    return ['Открыть позицию и подтвердить продуктовый результат']

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
        'explanation_block': payload.get('explanation_block') or [],
        'next_step_block': payload.get('next_step_block') or [],
        'diagnosis_block': payload.get('diagnosis_block') or [],
        'recommended_next_step_block': payload.get('recommended_next_step_block') or [],
        'opportunity_explanation_block': payload.get('opportunity_explanation_block') or [],
        'anomaly_explanation_block': payload.get('anomaly_explanation_block') or [],
        'screen_order': payload.get('screen_order', ['kpi_table', 'factor_change_table', 'benchmark_diagnostic_table']) or [],
        'product_insight_block': payload.get('product_insight_block') or [],
        'product_tmc_decision': payload.get('product_tmc_decision') or {},
        'product_tmc_decision_block': payload.get('product_tmc_decision_block') or [],
        'decision_workspace': payload.get('decision_workspace') or {},
        'sku_passport': payload.get('sku_passport') or {},
        'business_context': payload.get('business_context') or {},
        'category_workspace': payload.get('category_workspace') or {},
        'business_opportunity': payload.get('business_opportunity') or {},
        'recommendation_engine': payload.get('recommendation_engine') or {},
        'narrative_engine': payload.get('narrative_engine') or {},
        'product_workspace': payload.get('product_workspace') or {},
        'management_intelligence': payload.get('management_intelligence') or {},
        'management_workspace': payload.get('management_workspace') or {},
        'management_passport': payload.get('management_passport') or {},
        'decision_workspace_block': payload.get('decision_workspace_block') or [],
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
        response['profit_loss_rating'] = payload.get('profit_loss_rating') or []
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
    # v1.3 Stage 3: explanation is presentation-only and benchmark driven.
    try:
        if rendered.get('render_mode') not in {'list_only', 'reasons', 'kpi_only'}:
            rendered['summary_block'] = _build_benchmark_driven_summary(rendered)
            rendered['explanation_block'] = _build_explanation_block(rendered)
            rendered['next_step_block'] = _build_next_step_block(rendered)
            rendered['diagnosis_block'] = _build_assistant_diagnosis_block(rendered)
            rendered['recommended_next_step_block'] = _build_recommended_next_step_block(rendered)
            rendered['opportunity_explanation_block'] = _build_opportunity_explanation_block(rendered)
            rendered['anomaly_explanation_block'] = _build_anomaly_explanation_block(rendered)
            rendered['business_opportunity_block'] = _render_business_opportunity_block(rendered)
            rendered['recommendation_block'] = _render_recommendation_block(rendered)
            rendered['narrative_block'] = _render_narrative_block(rendered)
            rendered['product_workspace_block'] = _render_product_workspace_block(rendered)
            rendered['management_workspace_block'] = _render_management_workspace_block(rendered)
            rendered['screen_order'] = _stage7_screen_order(rendered)
    except Exception:
        logger.exception('explanation_layer_failed')
    rendered = _attach_product_recovery_blocks(rendered)
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


def _render_reasons_block(reasons, level=""):
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
        if str(level).strip().lower() == 'business':
            lines.append(
                f'{name}\n'
                f'факт: {money} грн ({percent}%)\n'
                f'{prev_line}\n\n'
                f'отклонение:\n'
                f'{delta_prev_line}\n\n'
                f'эффект: {effect}\n'
                f'сигнал: {signal}'
            )
        else:
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




def _reason_current_percent(item):
    return _fmt_percent(item.get('percent', item.get('value_percent')))


def _reason_previous_percent(item):
    prev_missing = bool(item.get('previous_percent_missing')) or item.get('previous_percent', item.get('prev_percent')) is None
    if prev_missing:
        return 'нет корректной базы'
    return f'{_fmt_percent(item.get("previous_percent", item.get("prev_percent")))}%'


def _reason_previous_money(item):
    return _fmt_int(item.get('previous_money', item.get('prev_money')))


def _reason_current_money(item):
    return _fmt_int(item.get('money', item.get('value_money')))


def _render_factor_change_block(reasons):
    """CHANGE-006.1: factors are object-vs-previous-period diagnostics.

    They must show current value, previous year value, delta and money effect.
    This block is used for Business and all object screens.
    """
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    sorted_reasons = sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99))
    lines = []
    for item in sorted_reasons:
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        delta_p_raw = item.get('delta_vs_previous_percent', item.get('delta_vs_prev'))
        delta_text = 'нет корректной базы' if delta_p_raw is None else _fmt_pp_delta(_num(delta_p_raw))
        signal = str(item.get('signal') or '').strip() or 'норма'
        lines.append(
            f'{name}\n'
            f'текущий период: {_reason_current_money(item)} грн ({_reason_current_percent(item)}%)\n'
            f'прошлый год: {_reason_previous_money(item)} грн ({_reason_previous_percent(item)})\n'
            f'Δ к прошлому году: {delta_text}\n'
            f'эффект: {_fmt_signed_int(item.get("effect_vs_previous_money", item.get("effect_money")))}\n'
            f'сигнал: {signal}'
        )
    return lines


def _render_benchmark_diagnostic_block(reasons):
    """CHANGE-006.1: Benchmark is diagnostic, not a separate money entity.

    On non-business screens it shows object vs business, delta and diagnostic
    effect for the factor. It must not include aggregate Benchmark Money.
    """
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    sorted_reasons = sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99))
    lines = []
    for item in sorted_reasons:
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        base_percent = _fmt_percent(item.get('base_percent'))
        delta_b = _num(item.get('delta_vs_business_percent', item.get('delta_percent')))
        lines.append(
            f'{name}\n'
            f'объект: {_reason_current_percent(item)}%\n'
            f'бизнес: {base_percent}%\n'
            f'Δ к бизнесу: {_fmt_pp_delta(delta_b)}\n'
            f'эффект: {_fmt_signed_int(item.get("effect_vs_business_money", item.get("effect_money")))}'
        )
    return lines

def _render_main_driver(structure):
    for item in structure:
        if item.get('is_main_driver'):
            return str(item.get('name') or '')
    return ''


def _navigation_money_text(item):
    if not isinstance(item, dict):
        return '0'
    if item.get('profit_delta_money') is not None:
        return f'{_fmt_signed_int(item.get("profit_delta_money"))} к прошлому году'
    value = item.get('navigation_money')
    if value is None:
        value = abs(_num(item.get('effect_money')))
    return f'{_fmt_int(abs(_num(value)))} потенциал'


def _render_drain_block(drain):
    lines = []
    for item in drain.get('items') or []:
        object_name = str(item.get('object_name') or '').strip()
        if not object_name:
            continue
        if item.get('profit_delta_money') is not None:
            lines.append(f'{object_name} → {_fmt_signed_int(item.get("profit_delta_money"))}')
        else:
            lines.append(f'{object_name} → {_navigation_money_text(item)}')
    return lines




def _render_vitrina_block(response):
    """Render the manual 'все' mode as an object showcase, not assistant analysis."""
    existing = response.get('drain_block_render') if isinstance(response, dict) else None
    if isinstance(existing, list) and len(existing) >= 2 and 'Оборот' in str(existing[1]):
        return existing
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    obj_name = ctx.get('object_name') or 'объект'
    period = ctx.get('period') or ''
    items = response.get('all_block') if isinstance(response.get('all_block'), list) else []
    if not items:
        items = (response.get('drain_block') or {}).get('items') if isinstance(response.get('drain_block'), dict) else []
    lines = [f'Витрина объекта: {obj_name}' + (f' | {period}' if period else '')]
    lines.append('№ | Объект | Δ прибыли | Потенциал')
    for idx, item in enumerate([x for x in items if isinstance(x, dict)], start=1):
        name = item.get('object_name') or item.get('name') or 'объект'
        delta = item.get('profit_delta_money')
        if delta is None:
            delta = item.get('delta_money')
        potential = item.get('opportunity_money')
        if potential is None:
            potential = item.get('potential_money')
        if potential is None:
            potential = item.get('navigation_money')
        delta_text = _fmt_signed_int(delta) + ' грн' if delta is not None else '—'
        potential_text = _fmt_int(potential) + ' грн' if potential is not None else '—'
        lines.append(f'{idx} | {name} | {delta_text} | {potential_text}')
    return lines

def _extract_navigation_names(payload, drain):
    # Navigation Contract v1.2 / BUG-006 FIX-002:
    # navigation names are derived only from normalized drain, and normalized
    # drain is derived only from all_block.
    names = []
    seen = set()
    for item in drain.get('items') or []:
        name = str(item.get('object_name') or '').strip()
        if name and name not in seen:
            names.append(name)
            seen.add(name)
    return names


def _render_navigation_block(payload, navigation, drain):
    lines = []
    names = _extract_navigation_names(payload, drain)
    drain_items = [item for item in (drain.get('items') or []) if isinstance(item, dict)]
    for idx, name in enumerate(names, start=1):
        item = drain_items[idx - 1] if idx - 1 < len(drain_items) else {}
        lines.append(f'{idx} — {name} → {_navigation_money_text(item)}')

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


def _display_layer(level: str) -> str:
    level = str(level or '').strip().lower()
    if level == 'business':
        return 'business'
    if level in {'manager_top', 'manager'}:
        return 'object'
    if level == 'network':
        return 'contract'
    if level in {'category', 'tmc_group'}:
        return 'product'
    if level == 'sku':
        return 'sku'
    return 'object'


def _reason_display_name(reason: dict) -> str:
    return str((reason or {}).get('name') or '').strip() or 'причина'


def _reason_effect(reason: dict) -> float:
    # Backward-compatible helper. Stage 7 diagnostics should use explicit
    # previous-year or business-benchmark helpers below to avoid mixing layers.
    return _reason_effect_vs_previous(reason)


def _reason_effect_vs_previous(reason: dict) -> float:
    if not isinstance(reason, dict):
        return 0.0
    if reason.get('effect_vs_previous_money') is not None:
        return _num(reason.get('effect_vs_previous_money'))
    return _num(reason.get('effect_money'))


def _reason_effect_vs_business(reason: dict) -> float:
    if not isinstance(reason, dict):
        return 0.0
    if reason.get('effect_vs_business_money') is not None:
        return _num(reason.get('effect_vs_business_money'))
    return _num(reason.get('effect_money'))


def _available_reasons(response: dict):
    return [r for r in (response.get('object_reasons') or response.get('business_reasons') or response.get('reasons_block') or []) if isinstance(r, dict)]


def _best_positive_reason(response: dict):
    reasons = _available_reasons(response)
    positives = [r for r in reasons if _reason_effect_vs_previous(r) > 0]
    if not positives:
        return None
    return max(positives, key=lambda r: _reason_effect_vs_previous(r))


def _worst_negative_reason(response: dict):
    reasons = _available_reasons(response)
    negatives = [r for r in reasons if _reason_effect_vs_previous(r) < 0]
    if not negatives:
        return None
    return min(negatives, key=lambda r: _reason_effect_vs_previous(r))


def _worst_benchmark_gap_reason(response: dict):
    reasons = _available_reasons(response)
    negatives = [r for r in reasons if _reason_effect_vs_business(r) < 0]
    if not negatives:
        return None
    return min(negatives, key=lambda r: _reason_effect_vs_business(r))


def _opportunity_gap_reasons(response: dict, limit=5):
    """Factors that form Opportunity through benchmark gaps.

    Uses only effect vs business; does not change Opportunity formula.
    Returned in descending money impact for explanation and priority action.
    """
    gaps = []
    for reason in _available_reasons(response):
        effect = _reason_effect_vs_business(reason)
        if effect < 0:
            gaps.append((abs(effect), reason))
    gaps.sort(key=lambda x: x[0], reverse=True)
    return [reason for _, reason in gaps[:limit]]


def _action_text_for_reason(reason: dict) -> str:
    name = _reason_display_name(reason).strip().lower()
    if name == 'ретро':
        return 'Проверить ретроусловия'
    if name == 'логистика':
        return 'Проверить логистические затраты'
    if name == 'персонал':
        return 'Проверить затраты на персонал'
    if name in {'прочие', 'прочее'}:
        return 'Проверить прочие расходы'
    if name == 'наценка':
        return 'Проверить цену/наценку'
    return f'Проверить фактор {_reason_display_name(reason).lower()}'


def _first_name(items) -> str:
    for item in items or []:
        if isinstance(item, dict):
            name = str(item.get('object_name') or item.get('name') or item.get('object') or '').strip()
            if name:
                return name
    return ''


def _top_names(items, limit=2) -> str:
    names = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        name = str(item.get('object_name') or item.get('name') or item.get('object') or '').strip()
        if name:
            names.append(name)
        if len(names) >= limit:
            break
    if not names:
        return ''
    if len(names) == 1:
        return names[0]
    return ' и '.join(names)


def _metric_word(metric_name: str) -> str:
    name = str(metric_name or '').strip().lower()
    if name == 'markup':
        return 'наценка'
    if name == 'retro':
        return 'ретроусловия'
    if name == 'logistics':
        return 'логистика'
    if name == 'personnel':
        return 'персонал'
    if name == 'other':
        return 'прочие затраты'
    return str(metric_name or '').strip().lower() or 'показатель'


def _sku_metric_sentence(response: dict) -> str:
    metrics = response.get('metrics') or []
    revenue = _metric_by_name(metrics, 'Оборот')
    finrez = _metric_by_name(metrics, 'Финрез до')
    margin = _metric_by_name(metrics, 'Маржа')
    markup = _metric_by_name(metrics, 'Наценка')
    parts = []
    if revenue:
        delta = _delta_money_for_metric(revenue)
        parts.append(f'Оборот {"вырос" if delta > 0 else ("снизился" if delta < 0 else "остался без существенного изменения")} на {_fmt_int(abs(delta))}.')
    if finrez:
        delta = _delta_money_for_metric(finrez)
        parts.append(f'Финрез {"вырос" if delta > 0 else ("снизился" if delta < 0 else "не изменился существенно")} на {_fmt_int(abs(delta))}.')
    if margin:
        delta = _delta_percent_for_metric(margin)
        parts.append(f'Маржа {"улучшилась" if delta > 0 else ("снизилась" if delta < 0 else "осталась на уровне прошлого года")} на {_fmt_pp_delta(abs(delta))}.')
    if markup:
        delta = _delta_percent_for_metric(markup)
        parts.append(f'Наценка {"улучшилась" if delta > 0 else ("снизилась" if delta < 0 else "осталась на уровне прошлого года")} на {_fmt_pp_delta(abs(delta))}.')
    if not parts:
        return 'Доступна только ограниченная оценка по текущим KPI.'
    return ' '.join(parts)

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




def _parse_rendered_number(value: Any) -> float:
    text = str(value or '').strip().replace('−', '-').replace(',', '.')
    match = re.search(r'[-+]?\d+(?:\.\d+)?', text.replace(' ', ''))
    if not match:
        return 0.0
    try:
        return float(match.group(0).replace('+', ''))
    except Exception:
        return 0.0


def _kpi_table_delta(response: dict, metric_name: str) -> float:
    rows = response.get('kpi_table') if isinstance(response.get('kpi_table'), list) else []
    wanted = str(metric_name or '').strip().lower()
    for row in rows:
        if not isinstance(row, dict):
            continue
        if str(row.get('name') or '').strip().lower() == wanted:
            return _parse_rendered_number(row.get('delta'))
    return 0.0

def _metric_delta_text(response: dict) -> str:
    metrics = response.get('metrics') or []
    revenue = _metric_by_name(metrics, 'Оборот')
    finrez = _metric_by_name(metrics, 'Финрез до')
    margin = _metric_by_name(metrics, 'Маржа')
    rev_delta = _delta_money_for_metric(revenue)
    fin_delta = _delta_money_for_metric(finrez)
    margin_delta = _delta_percent_for_metric(margin)
    if rev_delta > 0 and fin_delta > 0:
        return 'Объект показывает рост относительно прошлого года.'
    if rev_delta < 0 and fin_delta >= 0:
        return 'Оборот ниже прошлого года, но результат удержан за счёт маржи.'
    if rev_delta > 0 and fin_delta < 0:
        return 'Оборот выше прошлого года, но финансовый результат просел.'
    if fin_delta < 0:
        return 'Финансовый результат ниже прошлого года.'
    if margin_delta > 0:
        return 'Маржа лучше прошлого года.'
    return 'Динамика к прошлому году не является главным источником управленческого вывода.'


def _profit_first_fact_sentence(response: dict) -> str:
    metrics = response.get('metrics') or []
    finrez = _metric_by_name(metrics, 'Финрез до')
    revenue = _metric_by_name(metrics, 'Оборот')
    margin = _metric_by_name(metrics, 'Маржа')
    fin_delta = _delta_money_for_metric(finrez)
    rev_delta = _delta_money_for_metric(revenue)
    margin_delta = _delta_percent_for_metric(margin)

    # State/back screens may arrive already rendered, with reliable kpi_table
    # deltas but without previous values in the metrics array. In that case the
    # short summary must use the displayed KPI deltas, not fall back to zero.
    if abs(fin_delta) < 0.0001:
        table_fin_delta = _kpi_table_delta(response, 'Финрез до')
        if abs(table_fin_delta) > 0.0001:
            fin_delta = table_fin_delta
    if abs(rev_delta) < 0.0001:
        table_rev_delta = _kpi_table_delta(response, 'Оборот')
        if abs(table_rev_delta) > 0.0001:
            rev_delta = table_rev_delta
    if abs(margin_delta) < 0.0001:
        table_margin_delta = _kpi_table_delta(response, 'Маржа')
        if abs(table_margin_delta) > 0.0001:
            margin_delta = table_margin_delta

    if fin_delta < 0:
        result = f'Финрез снизился на {_fmt_int(abs(fin_delta))} к прошлому году.'
    elif fin_delta > 0:
        result = f'Финрез вырос на {_fmt_int(fin_delta)} к прошлому году.'
    else:
        result = 'Финрез находится примерно на уровне прошлого года.'

    details = []
    if revenue or abs(rev_delta) > 0.0001:
        details.append(f'оборот {_fmt_signed_int(rev_delta)}')
    if margin or abs(margin_delta) > 0.0001:
        details.append(f'маржа {_fmt_pp_delta(margin_delta)}')
    return result + (f' Дополнительно: {", ".join(details)}.' if details else '')


def _benchmark_sentence(response: dict) -> str:
    # CHANGE-006.1: Benchmark is diagnostic only. Do not render aggregate
    # Benchmark Money; show factor-level diagnostics through benchmark_diagnostic_block.
    return 'Сравнение с бизнесом используется как диагностика: объект сравнивается с текущим средним уровнем бизнеса по факторам.'


def _build_benchmark_driven_summary(response: dict) -> str:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    layer = _display_layer(level)
    opportunity = _num(response.get('opportunity_money'))
    strong = _best_positive_reason(response)
    risk = _worst_negative_reason(response)

    if layer == 'business':
        fact = _profit_first_fact_sentence(response)
        loss_names = _top_names(response.get('profit_loss_rating') or [], 2)
        result_names = _top_names(response.get('business_result_rating') or [], 2)
        potential_names = _top_names(response.get('opportunity_rating') or [], 2)
        parts = [fact]
        if loss_names:
            parts.append(f'Крупнейшие просадки прибыли: {loss_names}.')
        # CHANGE-006: benchmark money/rating is diagnostic only and must not
        # dominate the Business summary. It is intentionally not rendered here.
        if potential_names:
            parts.append(f'Главные резервы возврата: {potential_names}.')
        return ' '.join(parts)

    if layer in {'object', 'contract'}:
        existing_summary = str(response.get('summary_block') or '').strip()
        # Back/state screens already contain the summary from the original
        # workspace. Preserve it to avoid changing the main factor just because
        # the restored payload is already rendered rather than raw.
        if existing_summary and 'к прошлому году' in existing_summary and 'примерно на уровне' not in existing_summary:
            return existing_summary
        fact = _profit_first_fact_sentence(response)
        factor_line = f'Главный отрицательный фактор: {_reason_display_name(risk).lower()}.' if risk else 'Критичный отрицательный фактор не выделен.'
        strong_line = f'Сильный фактор: {_reason_display_name(strong).lower()}.' if strong else ''
        opportunity_line = f'Резерв прибыли внутри объекта: {_fmt_int(opportunity)} грн.' if opportunity > 0 else 'Существенный резерв прибыли внутри объекта не выявлен.'
        return ' '.join([x for x in [fact, factor_line, strong_line, opportunity_line] if x])

    if layer == 'product':
        fact = _profit_first_fact_sentence(response)
        opportunity_line = f'Потенциал внутри продукта: {_fmt_int(opportunity)} грн.' if opportunity > 0 else 'Существенный продуктовый резерв не выявлен.'
        return f'{fact} {opportunity_line} Детальный анализ цены, объёма, ассортимента и структуры ассортимента будет доступен после подключения витрины данных VECTRA.'

    if layer == 'sku':
        return f'{_sku_metric_sentence(response)} Для полного анализа позиции не хватает данных витрины данных. Доступна только оценка по текущим KPI.'

    return _metric_delta_text(response)


def _build_explanation_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    layer = _display_layer(level)
    risk = _worst_negative_reason(response)
    strong = _best_positive_reason(response)
    opportunity = _num(response.get('opportunity_money'))

    if layer == 'business':
        loss_names = _top_names(response.get('profit_loss_rating') or [], 3)
        potential_names = _top_names(response.get('opportunity_rating') or [], 2)
        return [
            'Сначала смотрим изменение прибыли к прошлому году.',
            f'Крупнейшие просадки прибыли: {loss_names or "данных нет"}.',
            f'Главные резервы возврата: {potential_names or "данных нет"}.',
            'Сравнение с бизнесом используется только как диагностика по отклонениям от бизнеса, без отдельной агрегированной денежной оценки.',
        ]

    lines = [
        'Сначала сравниваем объект с прошлым годом.',
        _profit_first_fact_sentence(response),
    ]

    if risk:
        lines.append(f'Главный отрицательный фактор диагностики: {_reason_display_name(risk)} ({_fmt_signed_int(_reason_effect(risk))}).')
    if strong:
        lines.append(f'Главный положительный фактор диагностики: {_reason_display_name(strong)} ({_fmt_signed_int(_reason_effect(strong))}).')

    if layer not in {'sku'}:
        lines.append(_benchmark_sentence(response))
    if opportunity > 0:
        lines.append(f'Потенциал показывает, где внутри выбранного объекта искать резерв: {_fmt_int(opportunity)} грн.')
    else:
        lines.append('Существенный резерв внутри объекта не выявлен.')

    if layer in {'product', 'sku'}:
        lines.append('Для полноценной причины нужны данные витрины данных: цена, объём, ассортимент, структуры ассортимента и контекст исполнения.')
    return lines



def _business_impact_sentence(response: dict) -> str:
    losses = response.get('profit_loss_rating') or []
    if losses and isinstance(losses[0], dict):
        name = str(losses[0].get('object_name') or '').strip()
        value = losses[0].get('profit_delta_money')
        if name:
            return f'Главная зона просадки прибыли: {name} ({_render_money_value(value)} к прошлому году).'
    drain = response.get('drain_block') or {}
    items = drain.get('items') if isinstance(drain, dict) else drain
    if items and isinstance(items, list) and isinstance(items[0], dict):
        name = str(items[0].get('object_name') or items[0].get('name') or '').strip()
        if name:
            return f'Первым вниз стоит проверить: {name}.'
    return 'Главная зона просадки ниже по дереву не выделена.'


def _main_factor_sentence(response: dict) -> str:
    # Factor Layer: only object/current period vs previous year.
    # Do not use benchmark effect here. Benchmark is rendered separately.
    risk = _worst_negative_reason(response)
    strong = _best_positive_reason(response)
    parts = []
    if strong:
        parts.append(f'Главный положительный фактор к прошлому году: {_reason_display_name(strong).lower()} ({_render_money_value(_reason_effect_vs_previous(strong))}).')
    if risk:
        parts.append(f'Главный отрицательный фактор к прошлому году: {_reason_display_name(risk).lower()} ({_render_money_value(_reason_effect_vs_previous(risk))}).')
    if parts:
        return ' '.join(parts)
    main_driver = str(response.get('main_driver') or '').strip()
    if main_driver:
        return f'Главный фактор диагностики: {main_driver.lower()}.'
    return 'Главный фактор изменения прибыли к прошлому году по доступным данным не выделен.'


def _turnover_or_margin_sentence(response: dict) -> str:
    metrics = response.get('metrics') or []

    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()

    children = response.get('all_block') if isinstance(response.get('all_block'), list) else []
    first_child = ''
    if children and isinstance(children[0], dict):
        first_child = str(children[0].get('object_name') or children[0].get('name') or '').strip()

    revenue = _metric_by_name(metrics, 'Оборот')
    finrez = _metric_by_name(metrics, 'Финрез до')
    margin = _metric_by_name(metrics, 'Маржа')
    markup = _metric_by_name(metrics, 'Наценка')
    rev_delta = _delta_money_for_metric(revenue)
    fin_delta = _delta_money_for_metric(finrez)
    margin_delta = _delta_percent_for_metric(margin)
    markup_delta = _delta_percent_for_metric(markup)

    if level in {'category', 'tmc_group'}:
        tmc_lines = _build_product_tmc_decision_block(response)
        if tmc_lines:
            return [f'➡ Рекомендуемый следующий шаг: {tmc_lines[-1]}']
        if first_child:
            return [f'➡ Рекомендуемый следующий шаг: открыть {first_child} как доказательство по позиции продуктового результата.']
        return ['➡ Рекомендуемый следующий шаг: сравнить продукт с таким же продуктом бизнеса и подтвердить решение на позиции.']

    if fin_delta < 0 and rev_delta < 0:
        if margin_delta < 0:
            return f'Главный сигнал просадки — падение оборота ({_fmt_signed_int(rev_delta)}). Маржа также ухудшилась ({_fmt_pp_delta(margin_delta)}), поэтому факторы доходности усилили потерю прибыли.'
        return f'Главный сигнал просадки — падение оборота ({_fmt_signed_int(rev_delta)}). Доходность улучшилась или удержалась, но не компенсировала потерю продаж.'
    if fin_delta < 0 and margin_delta < 0:
        return f'Главный сигнал просадки — снижение доходности: маржа {_fmt_pp_delta(margin_delta)}.'
    if fin_delta > 0:
        if rev_delta > 0 and margin_delta > 0:
            return f'Рост прибыли поддержан одновременно оборотом ({_fmt_signed_int(rev_delta)}) и доходностью: маржа {_fmt_pp_delta(margin_delta)}.'
        if rev_delta > 0:
            return f'Рост прибыли поддержан оборотом ({_fmt_signed_int(rev_delta)}).'
        if margin_delta > 0 or markup_delta > 0:
            return f'Рост прибыли поддержан доходностью: маржа {_fmt_pp_delta(margin_delta)}, наценка {_fmt_pp_delta(markup_delta)}.'
    if rev_delta < 0:
        return f'Оборот ниже прошлого года ({_fmt_signed_int(rev_delta)}) — нужно проверить, не теряется ли объём продаж.'
    if margin_delta < 0:
        return f'Маржа ниже прошлого года ({_fmt_pp_delta(margin_delta)}) — нужно проверить доходность.'
    return 'Критичного перекоса между оборотом и доходностью по доступным KPI не видно.'


def _benchmark_diagnosis_sentence(response: dict) -> str:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level == 'business':
        return 'Сравнение с бизнесом на экране бизнеса не выводится: бизнес не сравнивается с самим собой.'
    risk = _worst_benchmark_gap_reason(response)
    if risk:
        delta = _num(risk.get('delta_vs_business_percent', risk.get('delta_percent')))
        return f'Относительно {_product_compare_base_label(response)} слабое место: {_reason_display_name(risk).lower()} ({_fmt_pp_delta(delta)}, эффект {_render_money_value(_reason_effect_vs_business(risk))}). Это сравнение с бизнесом, а не причина изменения к прошлому году.'
    return f'Относительно {_product_compare_base_label(response)} отдельный критичный разрыв по доступным данным не выделен.'


def _build_assistant_diagnosis_block(response: dict) -> list:
    """Stage 7 / Assistant Diagnostic Layer.

    This is a presentation-only layer. It explains API numbers and does not
    calculate new KPI, change navigation, benchmark, opportunity or effect logic.
    """
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    layer = _display_layer(level)

    lines = ['🧠 Диагноз']
    lines.append(_profit_first_fact_sentence(response))
    lines.append(_turnover_or_margin_sentence(response))

    if layer == 'business':
        lines.append(_business_impact_sentence(response))
    elif layer in {'object', 'contract'}:
        lines.append(_main_factor_sentence(response))
        lines.append(_benchmark_diagnosis_sentence(response))
    elif layer == 'product':
        lines.append(_benchmark_diagnosis_sentence(response))
        tmc_lines = _build_product_tmc_decision_block(response)
        if tmc_lines:
            lines.append(tmc_lines[0])
        lines.append('Это продуктовый слой: цена, объём, ассортимент и структура ассортимента будут полноценно объяснены после подключения витрины данных VECTRA.')
    elif layer == 'sku':
        lines.append('Это Слой позиции: доступна KPI-диагностика, без окончательной причины до подключения витрины данных.')
    return [line for line in lines if line]


def _build_recommended_next_step_block(response: dict) -> list:
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    metrics = response.get('metrics') or []
    fin_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Финрез до'))
    rev_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Оборот'))
    margin_delta = _delta_percent_for_metric(_metric_by_name(metrics, 'Маржа'))

    if level == 'network' and response.get('decision_workspace'):
        return ['➡ Рекомендуемый следующий шаг: выбрать приоритетное действие Decision Engine или открыть доказательства по категории или позиции.']

    if level == 'sku':
        return ['➡ Рекомендуемый следующий шаг: использовать паспорт SKU как доказательство для переговоров или создать задачу по позиции.']

    children = response.get('all_block') if isinstance(response.get('all_block'), list) else []
    first_child = ''
    if children and isinstance(children[0], dict):
        first_child = str(children[0].get('object_name') or children[0].get('name') or '').strip()

    if fin_delta < 0 and rev_delta < 0:
        if level == 'network':
            return ['➡ Рекомендуемый следующий шаг: проверить контрактный контекст сети и открыть продуктовый уровень, чтобы понять, где потерян оборот.']
        if first_child:
            return [f'➡ Рекомендуемый следующий шаг: открыть {first_child} как крупнейший объект ниже и локализовать потерю оборота/прибыли.']
        return ['➡ Рекомендуемый следующий шаг: проверить контекст падения оборота: контракт, ассортимент, дистрибуцию и структуры ассортимента.']

    if fin_delta < 0 and margin_delta < 0:
        risk = _worst_negative_reason(response)
        if risk:
            return [f'➡ Рекомендуемый следующий шаг: открыть причины и проверить фактор {_reason_display_name(risk).lower()} как главный отрицательный эффект к прошлому году.']
        return ['➡ Рекомендуемый следующий шаг: открыть причины и проверить факторы снижения доходности.']

    if fin_delta > 0:
        strong = _best_positive_reason(response)
        bench_risk = _worst_benchmark_gap_reason(response)
        if strong and bench_risk:
            return [f'➡ Рекомендуемый следующий шаг: сохранить сильную сторону ({_reason_display_name(strong).lower()}) и проверить дополнительный резерв относительно бизнеса: {_reason_display_name(bench_risk).lower()}.']
        if first_child:
            return [f'➡ Рекомендуемый следующий шаг: открыть {first_child} и понять, где усилить прибыль внутри успешного объекта.']
        return ['➡ Рекомендуемый следующий шаг: зафиксировать факторы роста и проверить дополнительный резерв прибыли.']

    raw = _build_next_step_block(response)
    out = []
    for line in raw or []:
        text = str(line or '').strip()
        if not text:
            continue
        text = text.replace('Следующий шаг:', '').replace('Рекомендуемый следующий шаг:', '').strip()
        out.append(f'➡ Рекомендуемый следующий шаг: {text}')
    return out or ['➡ Рекомендуемый следующий шаг: открыть объекты ниже и продолжить диагностику прибыли.']



def _build_opportunity_explanation_block(response: dict) -> list:
    """Stage 8: explain where Opportunity comes from.

    This is a presentation-only layer. It uses Benchmark diagnostics
    (effect vs business) and does not change Opportunity formula.
    """
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []

    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    opportunity = abs(_num(response.get('opportunity_money')))

    # Business screen has no object-vs-business benchmark. Explain reserves by objects below.
    if level == 'business':
        items = [x for x in (response.get('opportunity_rating') or []) if isinstance(x, dict)]
        if not items:
            return []
        lines = ['🔍 Почему сформирован потенциал']
        lines.append('Потенциал бизнеса складывается из резервов объектов ниже по дереву.')
        for item in items[:5]:
            name = str(item.get('object_name') or item.get('name') or '').strip()
            value = item.get('opportunity_money')
            if name and _num(value) > 0:
                lines.append(f'{name}: {_fmt_int(abs(_num(value)))}')
        return lines

    gap_reasons = _opportunity_gap_reasons(response, limit=5)

    if not gap_reasons:
        if opportunity > 0:
            return [
                '🔍 Почему сформирован потенциал',
                f'Потенциал {_fmt_int(opportunity)} рассчитан внутри VECTRA Engine, но по доступным benchmark-факторам основной источник не выделен.',
                'Для точного объяснения нужен дополнительный контекст или витрины данных.',
            ]
        return []

    base_label = _product_compare_base_label(response)
    lines = ['🔍 Почему сформирован потенциал']
    if opportunity > 0:
        lines.append(f'Потенциал {_fmt_int(opportunity)} формируется за счёт факторов, которые хуже {base_label}.')
    else:
        lines.append(f'Потенциал формируется за счёт факторов, которые хуже {base_label}.')

    for reason in gap_reasons:
        money = abs(_reason_effect_vs_business(reason))
        name = _reason_display_name(reason)
        delta = _num(reason.get('delta_vs_business_percent', reason.get('delta_percent')))
        lines.append(f'{name} — {_fmt_int(money)} грн потенциального эффекта.')
        lines.append(f'Отклонение к бизнесу: {_fmt_pp_delta(delta)}. Если вывести фактор на уровень {base_label}, можно вернуть до {_fmt_int(money)} грн.')

    return lines


def _build_anomaly_explanation_block(response: dict) -> list:
    """Stage 8: explain abnormal previous-period bases without hiding data."""
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []

    metrics = response.get('metrics') or []
    revenue = _metric_by_name(metrics, 'Оборот')
    margin = _metric_by_name(metrics, 'Маржа')
    markup = _metric_by_name(metrics, 'Наценка')

    flags = []
    if revenue and revenue.get('pg_money') is not None and _num(revenue.get('pg_money')) <= 0:
        flags.append('оборот прошлого года был отрицательным или нулевым')
    if margin and margin.get('pg_percent') is not None and abs(_num(margin.get('pg_percent'))) > 100:
        flags.append('маржа прошлого года выглядит нетипично высокой или низкой')
    if markup and markup.get('pg_percent') is not None and _num(markup.get('pg_percent')) <= 0:
        flags.append('наценка прошлого года была нулевой или отрицательной')

    if not flags:
        return []

    return [
        '⚠ Особенность базы прошлого года',
        'По объекту в прошлом году были нетипичные данные: ' + '; '.join(flags) + '.',
        'Это может быть связано с возвратами, корректировками, сторно или отсутствием полноценной базы продаж.',
        'VECTRA не скрывает эти данные, но предупреждает: прямое сравнение отдельных KPI с прошлым годом может быть ограничено.',
    ]


def _render_business_opportunity_block(response):
    engine = response.get('business_opportunity') if isinstance(response.get('business_opportunity'), dict) else {}
    if not engine:
        workspace = response.get('product_workspace') if isinstance(response.get('product_workspace'), dict) else {}
        engine = workspace.get('opportunities') if isinstance(workspace.get('opportunities'), dict) else {}
    items = engine.get('items') if isinstance(engine.get('items'), list) else []
    if not items:
        return []
    lines = ['💰 Business Opportunity Engine', 'Объект | Тип | Основание | Потенциал / масштаб']
    for item in items[:8]:
        if not isinstance(item, dict):
            continue
        lines.append(
            f'{item.get("object") or "—"} | {item.get("type") or "—"} | '
            f'{item.get("reason") or "—"} | {_fmt_int(item.get("effect_money"))}'
        )
    summary = engine.get('summary') if isinstance(engine.get('summary'), dict) else {}
    top = summary.get('top_opportunity') if isinstance(summary.get('top_opportunity'), dict) else None
    if top:
        lines.extend(['', f'Главный фокус: {top.get("object") or "—"} — {top.get("recommended_action") or "проверить возможность"}.'])
    return [line for line in lines if str(line or '').strip()]


def _render_recommendation_block(response):
    engine = response.get('recommendation_engine') if isinstance(response.get('recommendation_engine'), dict) else {}
    if not engine:
        workspace = response.get('product_workspace') if isinstance(response.get('product_workspace'), dict) else {}
        engine = workspace.get('recommendations') if isinstance(workspace.get('recommendations'), dict) else {}
    items = engine.get('items') if isinstance(engine.get('items'), list) else []
    if not items:
        return []
    lines = ['🚀 Recommendation Engine', 'Приоритет | Действие | Основание | Ожидаемый эффект']
    for item in items[:5]:
        if not isinstance(item, dict):
            continue
        lines.append(
            f'{item.get("priority") or "—"} | {item.get("action") or "—"} | '
            f'{item.get("basis") or "—"} | {_fmt_int(item.get("expected_effect_money"))}'
        )
    return [line for line in lines if str(line or '').strip()]


def _render_narrative_block(response):
    narrative = response.get('narrative_engine') if isinstance(response.get('narrative_engine'), dict) else {}
    if not narrative:
        workspace = response.get('product_workspace') if isinstance(response.get('product_workspace'), dict) else {}
        narrative = workspace.get('narrative') if isinstance(workspace.get('narrative'), dict) else {}
    if not narrative:
        return []
    lines = [
        '🧠 Narrative Engine',
        f'Что произошло: {narrative.get("what_happened") or "—"}',
        f'Почему: {narrative.get("why") or "—"}',
        f'Что это означает: {narrative.get("what_it_means") or "—"}',
        f'Что делать: {narrative.get("what_to_do") or "—"}',
    ]
    if narrative.get('expected_effect_money') not in (None, ''):
        lines.append(f'Ожидаемый эффект / масштаб: {_fmt_int(narrative.get("expected_effect_money"))}')
    return [line for line in lines if str(line or '').strip()]




def _render_management_workspace_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level not in {'business', 'manager_top', 'manager'}:
        return []
    mi = response.get('management_intelligence') if isinstance(response.get('management_intelligence'), dict) else {}
    if not mi:
        return []
    passport = mi.get('passport') if isinstance(mi.get('passport'), dict) else {}
    portfolio = passport.get('portfolio') if isinstance(passport.get('portfolio'), dict) else {}
    radar = mi.get('radar') if isinstance(mi.get('radar'), dict) else {}
    priority = mi.get('priority_action') if isinstance(mi.get('priority_action'), dict) else {}
    narrative = mi.get('narrative') if isinstance(mi.get('narrative'), dict) else {}
    workspace = mi.get('workspace') if isinstance(mi.get('workspace'), dict) else {}
    lines = [
        f'🧭 Management рабочий стол: {mi.get("object_name") or ctx.get("object_name") or "объект"}',
        f'Период: {mi.get("period") or ctx.get("period") or "—"}',
        f'Роль владельца: {mi.get("owner_role") or "—"}',
        '',
        workspace.get('main_question') or 'Что требует управленческого внимания?',
        '',
        '## Паспорт ответственности',
        f'{portfolio.get("child_label") or "Объекты"}: {_fmt_int(portfolio.get("child_count"))} / прошлый год {_fmt_int(portfolio.get("child_count_previous_year"))}',
        f'Контракты: {_fmt_int(portfolio.get("network_count"))} / прошлый год {_fmt_int(portfolio.get("network_count_previous_year"))}',
        f'Категории: {_fmt_int(portfolio.get("category_count"))}',
        f'SKU: {_fmt_int(portfolio.get("sku_count"))}',
        '',
        '## Управленческий радар',
    ]
    summary = radar.get('summary') if isinstance(radar.get('summary'), dict) else {}
    lines.append(f'Объектов внимания: {_fmt_int(summary.get("risk_count"))}; объектов роста: {_fmt_int(summary.get("growth_count"))}; объектов с резервом: {_fmt_int(summary.get("opportunity_count"))}.')
    attention = radar.get('attention_required') if isinstance(radar.get('attention_required'), list) else []
    if attention:
        lines.extend(['', 'Требуют внимания:'])
        for item in attention[:5]:
            if isinstance(item, dict):
                lines.append(f'{item.get("object_name") or "—"} → Δ прибыли {_fmt_signed_int(item.get("profit_delta_money"))} грн, резерв {_fmt_int(item.get("opportunity_money"))} грн')
    growth = radar.get('growth_practices') if isinstance(radar.get('growth_practices'), list) else []
    if growth:
        lines.extend(['', 'Сильные практики / рост:'])
        for item in growth[:3]:
            if isinstance(item, dict):
                lines.append(f'{item.get("object_name") or "—"} → Δ прибыли {_fmt_signed_int(item.get("profit_delta_money"))} грн')
    lines.extend(['', '## Управленческий вывод'])
    if narrative.get('what_happened'):
        lines.append(str(narrative.get('what_happened')))
    if narrative.get('why_it_matters'):
        lines.append(str(narrative.get('why_it_matters')))
    if priority:
        lines.extend(['', '## Приоритетное действие', str(priority.get('action') or '—')])
        if priority.get('basis'):
            lines.append(f'Основание: {priority.get("basis")}')
    chain = mi.get('decision_chain') if isinstance(mi.get('decision_chain'), list) else []
    if chain:
        lines.extend(['', '## Decision Lifecycle'])
        for item in chain:
            if isinstance(item, dict) and item.get('title'):
                lines.append(f'{item.get("step")}: {item.get("title")}')
    return [line for line in lines if str(line or '').strip()]

def _render_product_workspace_block(response):
    workspace = response.get('product_workspace') if isinstance(response.get('product_workspace'), dict) else {}
    if not workspace:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = workspace.get('level') or ctx.get('level') or ''
    if str(level).lower() not in {'network', 'category', 'tmc_group', 'sku'}:
        return []
    lines = [
        f'📦 Product рабочий стол: {workspace.get("object_name") or ctx.get("object_name") or "объект"}',
        f'Период: {workspace.get("period") or ctx.get("period") or "—"}',
    ]
    opp = workspace.get('opportunities') if isinstance(workspace.get('opportunities'), dict) else {}
    rec = workspace.get('recommendations') if isinstance(workspace.get('recommendations'), dict) else {}
    opp_summary = opp.get('summary') if isinstance(opp.get('summary'), dict) else {}
    main_rec = rec.get('main_recommendation') if isinstance(rec.get('main_recommendation'), dict) else None
    lines.extend([
        '',
        'Управленческий смысл:',
        f'Найдено возможностей: {_fmt_int(opp_summary.get("total_items"))}',
    ])
    if main_rec:
        lines.append(f'Главное действие: {main_rec.get("action") or "—"}')
        if main_rec.get('basis'):
            lines.append(f'Основание: {main_rec.get("basis")}')
    next_actions = workspace.get('next_actions') if isinstance(workspace.get('next_actions'), list) else []
    if next_actions:
        lines.extend(['', 'Что делаем дальше:'])
        lines.extend(str(x) for x in next_actions[:5])
    return [line for line in lines if str(line or '').strip()]


# Sprint 12 Product Recovery: full assistant workspaces built from current DATA.
# These blocks intentionally keep the product model visible in the API response:
# Business = commercial director desktop; Network = Рабочий стол контракта for КАМ.

def _pr_prev_year(period: str) -> str:
    return _pi72_previous_year_period(period)


def _pr_months_back(period: str, count: int = 6) -> list:
    try:
        year = int(str(period)[:4]); month = int(str(period)[5:7])
    except Exception:
        return [period] if period else []
    out = []
    y, m = year, month
    for _ in range(count):
        out.append(f'{y:04d}-{m:02d}')
        m -= 1
        if m == 0:
            y -= 1; m = 12
    return list(reversed(out))


def _pr_context_filters(response: dict) -> dict:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    name = str(ctx.get('object_name') or '').strip()
    path = response.get('path') if isinstance(response.get('path'), list) else []
    flt = {}
    if level == 'manager_top':
        flt['manager_top'] = name
    elif level == 'manager':
        flt['manager'] = name
    elif level == 'network':
        flt['network'] = name
    elif level == 'category':
        if len(path) >= 4:
            flt['network'] = path[3]
        flt['category'] = name
    elif level == 'tmc_group':
        if len(path) >= 4:
            flt['network'] = path[3]
        if len(path) >= 5:
            flt['category'] = path[4]
        flt['tmc_group'] = name
    elif level == 'sku':
        if len(path) >= 4:
            flt['network'] = path[3]
        if len(path) >= 5:
            flt['category'] = path[4]
        flt['sku'] = name
    return {k: v for k, v in flt.items() if v}


_PR_ROWS_CACHE = {}

def _pr_rows(period: str, **filters) -> list:
    key = (str(period or ''), tuple(sorted((str(k), str(v)) for k, v in filters.items() if v)))
    if key in _PR_ROWS_CACHE:
        return _PR_ROWS_CACHE[key]
    try:
        rows, _ = filter_rows(get_normalized_rows(), period=period, **{k: v for k, v in filters.items() if v})
        rows = rows or []
        if len(_PR_ROWS_CACHE) > 256:
            _PR_ROWS_CACHE.clear()
        _PR_ROWS_CACHE[key] = rows
        return rows
    except Exception:
        logger.exception('product_recovery_filter_rows_failed')
        return []


def _pr_metric_text(response: dict, name: str) -> str:
    item = _metric_by_name(response.get('metrics') or [], name)
    if not item:
        return '—'
    if name in {'Маржа', 'Наценка'}:
        return f"{_fmt_percent_value(item.get('fact_percent'))} | Прошлый год {_fmt_percent_value(item.get('pg_percent'))} | Δ {_fmt_pp_delta(item.get('delta_percent'))}"
    return f"{_fmt_int(item.get('fact_money'))} | Прошлый год {_fmt_int(item.get('pg_money'))} | Δ {_fmt_signed_int(item.get('delta_money'))}"


def _pr_metric_num(response: dict, name: str, key_money='fact_money', key_percent='fact_percent') -> float:
    item = _metric_by_name(response.get('metrics') or [], name)
    if not item:
        return 0.0
    return _num(item.get(key_percent if name in {'Маржа', 'Наценка'} else key_money))


def _pr_structural_items(level: str, period: str, filters: dict) -> list:
    curr = _pr_rows(period, **filters)
    prev = _pr_rows(_pr_prev_year(period), **filters) if period else []
    fields = {
        'business': [('manager_top','Топ-менеджеры'),('manager','Менеджеры'),('network','Контракты'),('category','Категории'),('tmc_group','Группы ТМС'),('sku','SKU')],
        'network': [('category','Категории'),('tmc_group','Группы ТМС'),('sku','SKU')],
        'manager': [('network','Контракты'),('category','Категории'),('tmc_group','Группы ТМС'),('sku','SKU')],
        'manager_top': [('manager','Менеджеры'),('network','Контракты'),('category','Категории'),('tmc_group','Группы ТМС'),('sku','SKU')],
        'category': [('tmc_group','Группы ТМС'),('sku','SKU'),('network','Контракты')],
        'sku': [('network','Контракты')],
    }.get(level, [])
    items=[]
    for field,label in fields:
        cur=len({str(r.get(field) or '').strip() for r in curr if str(r.get(field) or '').strip()})
        prv=len({str(r.get(field) or '').strip() for r in prev if str(r.get(field) or '').strip()})
        items.append({'name':label,'current':cur,'previous':prv,'delta':cur-prv})
    return items


def _pr_trend_lines(period: str, filters: dict, limit: int = 6) -> list:
    lines=['📈 Историческая динамика 6 месяцев','Период | Оборот | Финрез ДО | Маржа | Наценка']
    for p in _pr_months_back(period, limit):
        rows=_pr_rows(p, **filters)
        m=aggregate_metrics(rows) if rows else {}
        revenue=_num(m.get('revenue'))
        finrez=_num(m.get('finrez_pre'))
        margin=_num(m.get('margin_pre'))
        markup=_num(m.get('markup'))
        lines.append(f'{p} | {_fmt_int(revenue)} | {_fmt_signed_int(finrez)} | {_fmt_percent_value(margin)} | {_fmt_percent_value(markup)}')
    return lines


def _pr_group_table(period: str, group_field: str, filters: dict, top: int = 8) -> list:
    rows=_pr_rows(period, **filters)
    prev=_pr_rows(_pr_prev_year(period), **filters)
    grouped={}; prev_grouped={}
    for r in rows:
        key=str(r.get(group_field) or '').strip()
        if key: grouped.setdefault(key,[]).append(r)
    for r in prev:
        key=str(r.get(group_field) or '').strip()
        if key: prev_grouped.setdefault(key,[]).append(r)
    total=aggregate_metrics(rows) if rows else {}
    total_rev=_num(total.get('revenue')); total_profit=_num(total.get('finrez_pre'))
    items=[]
    for name, rs in grouped.items():
        cur=aggregate_metrics(rs); prv=aggregate_metrics(prev_grouped.get(name) or [])
        revenue=_num(cur.get('revenue')); profit=_num(cur.get('finrez_pre')); prev_profit=_num(prv.get('finrez_pre'))
        items.append({
            'name':name,'revenue':revenue,'finrez':profit,'delta_profit':profit-prev_profit,
            'share_revenue':(revenue/total_rev*100) if total_rev else 0,
            'share_profit':(profit/total_profit*100) if abs(total_profit)>1e-9 else 0,
            'sku_count':len({str(r.get('sku') or '').strip() for r in rs if str(r.get('sku') or '').strip()}),
            'network_count':len({str(r.get('network') or '').strip() for r in rs if str(r.get('network') or '').strip()}),
        })
    items.sort(key=lambda x: abs(x.get('delta_profit') or 0), reverse=True)
    return items[:top]



def _w5_trend_comment(period: str, filters: dict, label: str = 'объекта') -> str:
    """User-facing assistant comment after the 6-month dynamics table."""
    months = _pr_months_back(period, 6)
    if len(months) < 2:
        return ''
    first_rows = _pr_rows(months[0], **filters)
    last_rows = _pr_rows(months[-1], **filters)
    if not first_rows or not last_rows:
        return ''
    first = aggregate_metrics(first_rows)
    last = aggregate_metrics(last_rows)
    rev_delta = _num(last.get('revenue')) - _num(first.get('revenue'))
    profit_delta = _num(last.get('finrez_pre')) - _num(first.get('finrez_pre'))
    margin_delta = _num(last.get('margin_pre')) - _num(first.get('margin_pre'))
    if profit_delta >= 0 and rev_delta < 0:
        return f'Комментарий ассистента: за 6 месяцев оборот снизился на {_fmt_signed_int(rev_delta)} грн, но финрез ДО изменился на {_fmt_signed_int(profit_delta)} грн. Значит, ключевая история {label} — не рост масштаба, а улучшение качества экономики.'
    if profit_delta >= 0 and rev_delta >= 0:
        return f'Комментарий ассистента: за 6 месяцев {label} растёт одновременно по обороту ({_fmt_signed_int(rev_delta)} грн) и финрезу ДО ({_fmt_signed_int(profit_delta)} грн). Это более здоровый сценарий роста.'
    return f'Комментарий ассистента: за 6 месяцев финрез ДО изменился на {_fmt_signed_int(profit_delta)} грн, маржа — на {_fmt_pp_delta(margin_delta)}. Перед действием нужно отделить проблему объёма от проблемы экономики.'


def _w5_factor_comment(factors: list) -> str:
    if not factors:
        return ''
    ordered = sorted(factors, key=lambda x: abs(_num(x.get('effect'))), reverse=True)
    top = ordered[0]
    risks = [x for x in ordered if _num(x.get('effect')) < 0]
    text = f'Комментарий ассистента: главный фактор по денежному эффекту — {top.get("name")}: {_fmt_signed_int(top.get("effect"))} грн. '
    if risks:
        risk = risks[0]
        text += f'Главный отрицательный фактор — {risk.get("name")}: {_fmt_signed_int(risk.get("effect"))} грн. Именно его нужно держать под контролем при выборе следующего действия.'
    else:
        text += 'Отрицательных факторов с подтверждённым эффектом в этом блоке не видно.'
    return text


def _w5_potential_comment(rows: list) -> str:
    if not rows:
        return ''
    rows = [x for x in rows if isinstance(x, dict)]
    if not rows:
        return ''
    top = max(rows, key=lambda x: _num(x.get('potential') or x.get('opportunity_money') or 0))
    name = top.get('name') or top.get('object_name') or top.get('sku') or 'объект'
    value = top.get('potential') if top.get('potential') is not None else top.get('opportunity_money')
    return f'Комментарий ассистента: потенциал нужно читать не как абстрактную сумму, а как подтверждённое отклонение от более сильной модели бизнеса. Самая крупная точка резерва в этом блоке — {name}: {_fmt_int(value)} грн.'



def _wic_factor_levels_from_metrics(metrics: dict) -> dict:
    """Percent levels for factors in a comparable business/object model.

    Positive values mean revenue/cost intensity. Cost factors are shown as
    negative percentages in UI because they reduce profit. Markup is positive.
    """
    revenue = _num(metrics.get('revenue'))
    cost = _num(metrics.get('cost'))
    return {
        'Наценка': _num(metrics.get('markup')),
        'Ретро': -(_num(metrics.get('retro_bonus')) / revenue * 100.0) if revenue else 0.0,
        'Логистика': -(_num(metrics.get('logistics_cost')) / revenue * 100.0) if revenue else 0.0,
        'Персонал': -(_num(metrics.get('personnel_cost')) / revenue * 100.0) if revenue else 0.0,
        'Прочие': -(_num(metrics.get('other_costs')) / revenue * 100.0) if revenue else 0.0,
    }


def _wic_factor_evidence_from_data(period: str, filters: dict) -> list:
    """Build Evidence First factor table directly from DATA when legacy fields are incomplete."""
    cur_rows = _pr_rows(period, **filters)
    prev_rows = _pr_rows(_pr_prev_year(period), **filters)
    if not cur_rows:
        return []
    cur = aggregate_metrics(cur_rows)
    prev = aggregate_metrics(prev_rows) if prev_rows else {}
    cur_l = _wic_factor_levels_from_metrics(cur)
    prev_l = _wic_factor_levels_from_metrics(prev) if prev_rows else {}

    # Monetary effects vs LY: profit bridge approximation on current object.
    effects = {
        'Наценка': (_num(cur.get('markup')) - _num(prev.get('markup'))) / 100.0 * max(_num(cur.get('cost')), 0),
        'Ретро': -(_num(cur.get('retro_bonus')) - _num(prev.get('retro_bonus'))),
        'Логистика': -(_num(cur.get('logistics_cost')) - _num(prev.get('logistics_cost'))),
        'Персонал': -(_num(cur.get('personnel_cost')) - _num(prev.get('personnel_cost'))),
        'Прочие': -(_num(cur.get('other_costs')) - _num(prev.get('other_costs'))),
    }
    rows=[]
    for name in ['Наценка','Логистика','Прочие','Ретро','Персонал']:
        curr=cur_l.get(name,0); prv=prev_l.get(name,0) if prev_rows else None
        rows.append({
            'name': name,
            'current_text': _fmt_percent_value(curr),
            'previous_text': _fmt_percent_value(prv) if prv is not None else '—',
            'delta_text': _fmt_pp_delta(curr - prv) if prv is not None else 'нет корректной базы',
            'effect': effects.get(name,0),
            'signal': 'риск' if effects.get(name,0) < 0 else ('драйвер' if effects.get(name,0) > 0 else 'нейтрально'),
        })
    rows.sort(key=lambda x: abs(_num(x.get('effect'))), reverse=True)
    return rows


def _wic_benchmark_factor_rows(period: str, filters: dict) -> list:
    """Object vs business factor evidence table based on DATA."""
    obj_rows = _pr_rows(period, **filters)
    biz_rows = _pr_rows(period)
    if not obj_rows or not biz_rows:
        return []
    obj = aggregate_metrics(obj_rows); biz = aggregate_metrics(biz_rows)
    obj_l = _wic_factor_levels_from_metrics(obj); biz_l = _wic_factor_levels_from_metrics(biz)
    revenue = max(_num(obj.get('revenue')), 0)
    rows=[]
    for name in ['Наценка','Ретро','Логистика','Персонал','Прочие']:
        gap = _num(obj_l.get(name)) - _num(biz_l.get(name))
        # For all factors in UI convention, positive gap is better; negative gap is reserve/risk.
        effect = gap/100.0 * revenue
        rows.append({
            'name': name,
            'current_text': _fmt_percent_value(obj_l.get(name)),
            'base_text': _fmt_percent_value(biz_l.get(name)),
            'gap_text': _fmt_pp_delta(gap),
            'effect': effect,
            'signal': 'сильнее бизнеса' if effect >= 0 else 'резерв / слабее бизнеса',
        })
    rows.sort(key=lambda x: abs(_num(x.get('effect'))), reverse=True)
    return rows


def _wic_potential_breakdown(period: str, filters: dict, limit: int = 3) -> list:
    rows = _wic_benchmark_factor_rows(period, filters)
    risks = [r for r in rows if _num(r.get('effect')) < 0]
    return [{'name': r['name'], 'money': abs(_num(r.get('effect'))), 'gap': r.get('gap_text')} for r in risks[:limit]]


def _wic_breakdown_text(parts: list) -> str:
    if not parts:
        return 'потенциал не разложен по факторам текущей DATA'
    return '; '.join(f"{p['name']} {p['gap']} ≈ {_fmt_int(p['money'])} грн" for p in parts)


def _wic_business_context_lines(period: str) -> list:
    rows = _pr_rows(period)
    if not rows:
        return []
    lines=['🌐 Business Context: что отличается внутри бизнеса']
    # Categories, formats, SKU opportunities from the business itself.
    cats=_pr_group_table(period,'category',{},10)
    if cats:
        lines.extend(['Категории | Оборот | Доля бизнеса | Финрез ДО | Δ прибыли | Что означает'])
        for c in cats[:5]:
            meaning='ключевой контур бизнеса' if c.get('share_revenue',0)>=20 else 'вторичный контур'
            lines.append(f"{c['name']} | {_fmt_int(c['revenue'])} | {_fmt_percent_value(c['share_revenue'])} | {_fmt_signed_int(c['finrez'])} | {_fmt_signed_int(c['delta_profit'])} | {meaning}")
    fmts=_w3_format_table(period, {}, {}, limit=8)
    if fmts:
        lines.extend(['','Форматы бизнеса | Оборот | Доля бизнеса | SKU | Финрез | Управленческий смысл'])
        for f in fmts[:6]:
            sense='формат масштаба' if f.get('share_business',0)>=10 else 'формат развития/ниши'
            lines.append(f"{f['format']} | {_fmt_int(f['revenue'])} | {_fmt_percent_value(f['share_business'])} | {f['sku_count']} | {_fmt_signed_int(f['finrez'])} | {sense}")
    skus=_pr_business_sku_leaders(period,10)
    if skus:
        lines.extend(['','SKU-лидеры бизнеса | Оборот | Финрез ДО | Сетей | Зачем смотреть'])
        for s in skus[:5]:
            lines.append(f"{s['sku']} | {_fmt_int(s['revenue'])} | {_fmt_signed_int(s['finrez'])} | {s['network_count']} | доказательная база для контрактов")
    lines.append('Комментарий ассистента: этот блок показывает не локальную проблему, а карту возможностей бизнеса — какие категории, форматы и SKU уже доказаны DATA и могут использоваться как аргументы ниже.')
    return lines


def _wic_concentration_lines(period: str) -> list:
    managers=_pr_group_table(period,'manager_top',{},50)
    contracts=_pr_group_table(period,'network',{},200)
    if not managers and not contracts:
        return []
    lines=['🧲 Концентрация результата','Контур | Концентрация | Что означает']
    if managers:
        top3_rev=sum(_num(x.get('revenue')) for x in managers[:3]); total_rev=sum(_num(x.get('revenue')) for x in managers)
        top3_profit=sum(_num(x.get('finrez')) for x in managers[:3]); total_profit=sum(_num(x.get('finrez')) for x in managers)
        lines.append(f'ТОП-3 руководителя по обороту | {_fmt_percent_value((top3_rev/total_rev*100) if total_rev else 0)} оборота | показывает зависимость бизнеса от ключевых владельцев')
        lines.append(f'ТОП-3 руководителя по прибыли | {_fmt_percent_value((top3_profit/total_profit*100) if abs(total_profit)>1e-9 else 0)} прибыли | показывает концентрацию результата')
    if contracts:
        top10_rev=sum(_num(x.get('revenue')) for x in contracts[:10]); total_rev=sum(_num(x.get('revenue')) for x in contracts)
        lines.append(f'ТОП-10 контрактов | {_fmt_percent_value((top10_rev/total_rev*100) if total_rev else 0)} оборота | показывает, где управленческое внимание даёт быстрый эффект')
    return lines


def _pr_business_workspace_block(response: dict) -> list:
    """Workspace Intelligence Completion: full visible Business Workspace.

    This block is intentionally information-dense. It is the primary artifact
    for Custom GPT rendering and must show the changes from audit directly on
    screen: stronger executive summary, Evidence First factors, potential
    breakdown, Business Context and concentration map.
    """
    ctx=response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower()!='business':
        return []
    period=str(ctx.get('period') or '').strip()
    filters={}
    fin_delta=_pr_metric_num(response,'Финрез до','delta_money')
    rev_delta=_pr_metric_num(response,'Оборот','delta_money')
    margin_delta=_pr_metric_num(response,'Маржа','delta_money','delta_percent')
    markup_delta=_pr_metric_num(response,'Наценка','delta_money','delta_percent')
    revenue_cur=_pr_metric_num(response,'Оборот','fact_money')
    revenue_prev=_pr_metric_num(response,'Оборот','pg_money')
    revenue_drop_pct=(rev_delta/revenue_prev*100) if revenue_prev else 0
    profit_prev=_pr_metric_num(response,'Финрез до','pg_money')
    profit_growth_pct=(fin_delta/profit_prev*100) if profit_prev else 0

    lines=[
        f'📍 Рабочий стол бизнеса — {period}',
        '👤 Рабочий стол: Бизнес',
        '🤖 Роль ассистента: стратегический помощник коммерческого директора',
        '',
        '🧠 Краткий управленческий вывод',
    ]
    if fin_delta > 0 and rev_delta < 0:
        lines.append(
            f'Бизнес находится в нестандартной ситуации: оборот снизился на {_fmt_signed_int(rev_delta)} грн '
            f'({_fmt_percent_value(revenue_drop_pct)} к прошлому году), но финрез ДО вырос на {_fmt_signed_int(fin_delta)} грн '
            f'({_fmt_percent_value(profit_growth_pct)} к прошлому году).'
        )
        lines.append(
            f'Это означает, что период выигран не масштабом продаж, а качеством экономики: маржа выросла на {_fmt_pp_delta(margin_delta)}, '
            f'наценка — на {_fmt_pp_delta(markup_delta)}. Главный управленческий вопрос теперь — удержать новую доходность при восстановлении оборота.'
        )
    elif fin_delta > 0:
        lines.append(
            f'Финрез ДО вырос на {_fmt_signed_int(fin_delta)} грн при изменении оборота на {_fmt_signed_int(rev_delta)} грн. '
            'Нужно разделить вклад масштаба, структуры и экономики продаж.'
        )
    else:
        lines.append(
            f'Финрез ДО снизился на {_fmt_signed_int(fin_delta)} грн. Первое действие — найти владельцев просадки, затем разложить её на факторы экономики и структуры.'
        )

    lines.extend(['','📊 Ключевые показатели бизнеса','Показатель | Текущий период | Прошлый год | Изменение | Что означает'])
    for name, meaning in [
        ('Оборот','масштаб бизнеса'),('Финрез до','прибыль до распределений'),('Маржа','качество прибыли'),('Наценка','ценовая экономика'),('Финрез итог','итог после распределений')
    ]:
        item=_metric_by_name(response.get('metrics') or [], name)
        if not item: continue
        if name in {'Маржа','Наценка'}:
            lines.append(f'{name} | {_fmt_percent_value(item.get("fact_percent"))} | {_fmt_percent_value(item.get("pg_percent"))} | {_fmt_pp_delta(item.get("delta_percent"))} | {meaning}')
        else:
            lines.append(f'{name} | {_fmt_int(item.get("fact_money"))} | {_fmt_int(item.get("pg_money"))} | {_fmt_signed_int(item.get("delta_money"))} | {meaning}')
    lines.append('Комментарий ассистента: таблица показывает доверительную базу. Вывод выше не является мнением — он следует из сочетания падения оборота и роста доходности.')

    sitems=_pr_structural_items('business', period, filters)
    if sitems:
        lines.extend(['','🏗 Структурный анализ бизнеса','Структура | Сейчас | Прошлый год | Δ | Что означает'])
        comments={'Топ-менеджеры':'верхний контур управления','Менеджеры':'покрытие портфеля','Контракты':'клиентская база','Категории':'состав бизнеса','Группы ТМС':'продуктовые линейки','SKU':'ассортимент'}
        for it in sitems:
            delta=int(it.get('delta') or 0)
            if delta>0: meaning=f'расширение: {comments.get(it["name"],"структура")} увеличилась'
            elif delta<0: meaning=f'сокращение: {comments.get(it["name"],"структура")} уменьшилась'
            else: meaning='без изменений'
            lines.append(f"{it['name']} | {it['current']} | {it['previous']} | {delta:+d} | {meaning}")
        lines.append('Комментарий ассистента: перед финансовым выводом нужно понимать, сравниваем ли мы тот же объект. Здесь изменились менеджеры, контракты, категории, группы ТМС и SKU — значит часть результата связана со структурой портфеля.')

    lines.extend([''] + _pr_trend_lines(period, filters, 6))
    trend_comment = _w5_trend_comment(period, filters, 'бизнеса')
    if trend_comment:
        lines.append(trend_comment)
    # Add an explicit interpretation of current month vs prior months.
    months=_pr_months_back(period,6)
    vals=[]
    for p in months:
        rs=_pr_rows(p)
        if rs:
            m=aggregate_metrics(rs); vals.append((p,_num(m.get('revenue')),_num(m.get('finrez_pre')),_num(m.get('margin_pre')),_num(m.get('markup'))))
    if vals:
        peak_rev=max(vals, key=lambda x:x[1]); peak_profit=max(vals, key=lambda x:x[2]); cur=vals[-1]
        lines.append(f'Вывод по динамике: текущий месяц не является максимумом по обороту за 6 месяцев (пик — {peak_rev[0]}: {_fmt_int(peak_rev[1])} грн), но показывает одну из самых сильных экономик периода: маржа {_fmt_percent_value(cur[3])}, наценка {_fmt_percent_value(cur[4])}.')

    factors=_w3_factor_evidence_rows(response, business=True) or _wic_factor_evidence_from_data(period, filters)
    if factors:
        lines.extend(['','💰 Почему изменилась прибыль: доказательная база','Фактор | Текущий уровень | Прошлый год | Изменение | Денежный эффект | Сигнал'])
        for item in factors:
            lines.append(f"{item['name']} | {item['current_text']} | {item['previous_text']} | {item['delta_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")
        comment=_w5_factor_comment(factors)
        if comment: lines.append(comment)
        lines.append('Комментарий ассистента: здесь важно не только увидеть сумму эффекта, но и проверить, из какого изменения показателя она возникла. Поэтому фактор всегда должен показываться как текущий уровень → прошлый год → изменение → деньги.')

    managers=[m for m in _pr_group_table(period,'manager_top',filters,50) if str(m.get('name') or '').strip().lower() not in {'пусто','без менеджера','без менеджера '} ]
    opp_map={str(x.get('object_name')): _num(x.get('opportunity_money')) for x in (response.get('opportunity_rating') or []) if isinstance(x,dict)}
    if managers:
        total_potential=sum(max(0,_num(v)) for v in opp_map.values())
        if not total_potential:
            total_potential=sum(sum(p['money'] for p in _wic_potential_breakdown(period, {'manager_top': item['name']})) for item in managers)
        lines.extend(['','💵 Где находятся деньги','Подтверждённый потенциал по текущей модели: '+_fmt_int(total_potential)+' грн','Объект | Сигнал | Δ прибыли | Доля оборота | Доля прибыли | Потенциал | Из чего состоит потенциал | Контрактов | SKU'])
        # Sort by management priority: negative delta first, then potential, then profit growth.
        managers_sorted=sorted(managers, key=lambda x: (0 if x['delta_profit']<0 else 1, -max(opp_map.get(x['name'],0), sum(p['money'] for p in _wic_potential_breakdown(period, {'manager_top':x['name']}))), -x['delta_profit']))
        for item in managers_sorted[:8]:
            name=item['name']
            parts=_wic_potential_breakdown(period, {'manager_top': name})
            potential=opp_map.get(name,0) or sum(p['money'] for p in parts)
            if item['delta_profit']<0:
                sig='управленческий риск'
            elif potential>500000:
                sig='крупный резерв'
            elif item['delta_profit']>0:
                sig='рост / практика'
            else:
                sig='контроль'
            lines.append(f"{name} | {sig} | {_fmt_signed_int(item['delta_profit'])} | {_fmt_percent_value(item['share_revenue'])} | {_fmt_percent_value(item['share_profit'])} | {_fmt_int(potential)} | {_wic_breakdown_text(parts)} | {item['network_count']} | {item['sku_count']}")
        lines.append('Комментарий ассистента: блок показывает не только сумму потенциала, а её происхождение. Это нужно, чтобы следующий шаг превращался в задачу: наценка, ретро, логистика, персонал или ассортиментный контур.')

    bctx=_wic_business_context_lines(period)
    if bctx:
        lines.extend(['']+bctx)
    conc=_wic_concentration_lines(period)
    if conc:
        lines.extend(['']+conc)

    lines.extend(['','🚨 Приоритеты руководителя','Зона | Объект | Доказательство | Что делать первым'])
    if managers:
        risk=next((x for x in sorted(managers, key=lambda x:x['delta_profit']) if x['delta_profit']<0), None)
        reserve=max(managers, key=lambda x: opp_map.get(x['name'],0) or sum(p['money'] for p in _wic_potential_breakdown(period, {'manager_top':x['name']})))
        best=max(managers, key=lambda x: x['delta_profit'])
        if risk:
            lines.append(f'🔴 Главный риск | {risk["name"]} | Δ прибыли {_fmt_signed_int(risk["delta_profit"])}; доля прибыли {_fmt_percent_value(risk["share_profit"])} | открыть рабочий стол и найти источник просадки')
        lines.append(f'🟠 Главный резерв | {reserve["name"]} | потенциал {_fmt_int(opp_map.get(reserve["name"],0) or sum(p["money"] for p in _wic_potential_breakdown(period, {"manager_top":reserve["name"]})))}; доля оборота {_fmt_percent_value(reserve["share_revenue"])} | разобрать происхождение потенциала')
        lines.append(f'🟢 Лучшая практика | {best["name"]} | прирост прибыли {_fmt_signed_int(best["delta_profit"])} | понять, какие решения можно масштабировать')

        first = risk or reserve
        lines.extend(['','🎯 Что я бы сделал первым'])
        lines.append(f'Я бы начал с «{first["name"]}», потому что это первая точка, где управленческое действие может изменить общий результат бизнеса: есть вес в бизнесе, подтверждённая динамика и понятный следующий уровень детализации.')
    lines.extend(['','➡️ Что делаем дальше?','1. Открыть рабочий стол главного риска.','2. Открыть рабочий стол крупнейшего резерва.','3. Показать полную витрину руководителей.','4. Показать причины изменения результата.','5. Показать Business Context: категории, форматы и SKU бизнеса.','6. Создать задачи по выбранному приоритету.','7. Спросить ассистента: «что бы ты сделал первым и почему?»'])
    return [x for x in lines if str(x or '').strip()]

def _pr_business_sku_leaders(period: str, limit: int = 20) -> list:
    rows=_pr_rows(period)
    grouped={}
    for r in rows:
        sku=str(r.get('sku') or '').strip()
        if sku: grouped.setdefault(sku,[]).append(r)
    items=[]
    for sku, rs in grouped.items():
        m=aggregate_metrics(rs)
        items.append({'sku':sku,'revenue':_num(m.get('revenue')),'finrez':_num(m.get('finrez_pre')),'network_count':len({str(r.get('network') or '').strip() for r in rs if str(r.get('network') or '').strip()})})
    items.sort(key=lambda x: x['revenue'], reverse=True)
    for i,item in enumerate(items,1): item['rank']=i
    return items[:limit]



def _pr_management_workspace_block(response: dict) -> list:
    """Sprint W6: Russian, evidence-first workspace for Top Manager / Manager.

    This replaces the legacy mixed-language Management screen with the same
    product standard as Business and Contract: passport, structure, evidence,
    portfolio showcase and next decisions.
    """
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level not in {'manager_top', 'manager'}:
        return []
    name = str(ctx.get('object_name') or '').strip() or 'объект'
    period = str(ctx.get('period') or '').strip()
    owner_role = 'руководитель направления' if level == 'manager_top' else 'КАМ / менеджер портфеля'
    filters = {'manager_top': name} if level == 'manager_top' else {'manager': name}
    child_field = 'manager' if level == 'manager_top' else 'network'
    child_label = 'менеджеров' if level == 'manager_top' else 'контрактов'
    child_title = 'Команда' if level == 'manager_top' else 'Портфель контрактов'

    lines = [
        f'🧭 Рабочий стол управления — {name} | {period}',
        f'👤 Рабочий стол: {"Руководитель направления" if level == "manager_top" else "Менеджер / КАМ"}',
        f'🤖 Роль ассистента: помощник {owner_role}',
        '',
        '🧠 Краткий вывод',
    ]
    fin_delta = _pr_metric_num(response, 'Финрез до', 'delta_money')
    rev_delta = _pr_metric_num(response, 'Оборот', 'delta_money')
    margin_delta = _pr_metric_num(response, 'Маржа', 'delta_money', 'delta_percent')
    lines.append(f'Финрез ДО изменился на {_fmt_signed_int(fin_delta)} грн, оборот — на {_fmt_signed_int(rev_delta)} грн к прошлому году.')
    if fin_delta >= 0 and rev_delta >= 0:
        lines.append(f'Зона ответственности растёт по масштабу и результату. Следующий вопрос — где внутри {child_label} находятся риски, резервы и лучшие практики.')
    elif fin_delta >= 0:
        lines.append('Результат улучшился, но нужно проверить качество роста: структуру портфеля, факторы экономики и объекты ниже.')
    else:
        lines.append('Есть просадка результата. Сначала проверяем структуру зоны ответственности, затем факторы и объекты ниже.')

    # KPI evidence table
    lines.extend(['', '📊 Ключевые показатели', 'Показатель | Текущий период | Прошлый год | Изменение | Доля бизнеса'])
    business_rows = _pr_rows(period)
    obj_rows = _pr_rows(period, **filters)
    bm = aggregate_metrics(business_rows) if business_rows else {}
    om = aggregate_metrics(obj_rows) if obj_rows else {}
    business_rev = _num(bm.get('revenue'))
    business_profit = _num(bm.get('finrez_pre'))
    obj_rev = _num(om.get('revenue'))
    obj_profit = _num(om.get('finrez_pre'))
    share_rev = (obj_rev / business_rev * 100) if business_rev else 0
    share_profit = (obj_profit / business_profit * 100) if abs(business_profit) > 1e-9 else 0
    for metric_name in ['Оборот', 'Финрез до', 'Маржа', 'Наценка']:
        item = _metric_by_name(response.get('metrics') or [], metric_name)
        if not item:
            continue
        if metric_name in {'Маржа', 'Наценка'}:
            share = '—'
            lines.append(f'{metric_name} | {_fmt_percent_value(item.get("fact_percent"))} | {_fmt_percent_value(item.get("pg_percent"))} | {_fmt_pp_delta(item.get("delta_percent"))} | {share}')
        else:
            share = _fmt_percent_value(share_rev if metric_name == 'Оборот' else share_profit)
            lines.append(f'{metric_name} | {_fmt_int(item.get("fact_money"))} | {_fmt_int(item.get("pg_money"))} | {_fmt_signed_int(item.get("delta_money"))} | {share}')

    # responsibility passport / structural analysis
    sitems = _pr_structural_items(level, period, filters)
    if sitems:
        lines.extend(['', '🧾 Паспорт зоны ответственности', 'Показатель | Сейчас | Прошлый год | Δ'])
        for it in sitems:
            lines.append(f"{it['name']} | {it['current']} | {it['previous']} | {it['delta']:+d}")
        lines.append('Комментарий ассистента: этот блок показывает, изменился ли сам состав зоны ответственности. Без этого нельзя корректно читать финансовую динамику.')

    trend = _pr_trend_lines(period, filters, 6)
    if trend:
        lines.extend([''] + trend)
        c = _w5_trend_comment(period, filters, 'зоны ответственности')
        if c:
            lines.append(c)

    factors = _w3_factor_evidence_rows(response, business=False)
    if factors:
        lines.extend(['', '💰 Почему изменилась прибыль', 'Фактор | Текущий период | Прошлый год | Δ | Денежный эффект | Сигнал'])
        for item in factors:
            lines.append(f"{item['name']} | {item['current_text']} | {item['previous_text']} | {item['delta_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")
        comment = _w5_factor_comment(factors)
        if comment:
            lines.append(comment)

    # benchmark vs business from structure/economics
    econ_rows = _w3_contract_factor_rows(response)
    if econ_rows:
        lines.extend(['', '📍 Положение относительно бизнеса', 'Фактор | Объект | Бизнес | Отклонение | Потенциал / эффект | Сигнал'])
        for item in econ_rows:
            lines.append(f"{item['name']} | {item['current_text']} | {item['base_text']} | {item['gap_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")

    # portfolio / team objects below
    children = _pr_group_table(period, child_field, filters, 20)
    opp_map = {str(o.get('object_name')): _num(o.get('opportunity_money')) for o in (response.get('opportunity_rating') or []) if isinstance(o, dict)}
    if children:
        lines.extend(['', f'👥 {child_title}: где деньги ниже', f'Объект | Оборот | Доля в зоне | Доля бизнеса | Финрез ДО | Δ прибыли | Потенциал | Сетей | SKU | Приоритет'])
        for item in children[:8]:
            obj = item['name']
            potential = opp_map.get(obj, 0)
            priority = '🔴 Риск' if item['delta_profit'] < 0 else ('🟠 Крупный резерв' if potential > 100000 else ('🟡 Резерв' if potential > 0 else '🟢 Рост'))
            business_share = (item['revenue'] / business_rev * 100) if business_rev else 0
            lines.append(
                f"{obj} | {_fmt_int(item['revenue'])} грн | {_fmt_percent_value(item['share_revenue'])} | {_fmt_percent_value(business_share)} | "
                f"{_fmt_signed_int(item['finrez'])} грн | {_fmt_signed_int(item['delta_profit'])} грн | {_fmt_int(potential)} грн | {item['network_count']} | {item['sku_count']} | {priority}"
            )
        lines.append('Комментарий ассистента: это не просто список ниже. Это карта управленческого внимания: где риск, где резерв, где лучшая практика и кого открывать первым.')

        risks = [x for x in children if x['delta_profit'] < 0]
        reserve = max(children, key=lambda x: opp_map.get(x['name'], 0)) if children else None
        best = max(children, key=lambda x: x['delta_profit']) if children else None
        lines.extend(['', '🚨 Управленческий радар', 'Сигнал | Объект | Основание | Действие'])
        if risks:
            r = risks[0]
            lines.append(f'🔴 Главный риск | {r["name"]} | Δ прибыли {_fmt_signed_int(r["delta_profit"])} грн | открыть рабочий стол и найти причину')
        if reserve:
            lines.append(f'🟠 Главный резерв | {reserve["name"]} | потенциал {_fmt_int(opp_map.get(reserve["name"], 0))} грн | разобрать источник потенциала')
        if best:
            lines.append(f'🟢 Лучшая практика | {best["name"]} | прирост прибыли {_fmt_signed_int(best["delta_profit"])} грн | понять, что можно масштабировать')

    lines.extend(['', '🎯 Приоритет владельца Workspace'])
    if children:
        first = next((x for x in children if x['delta_profit'] < 0), None) or max(children, key=lambda x: opp_map.get(x['name'], 0))
        lines.append(f'Первое действие — открыть «{first["name"]}». Это самый полезный следующий шаг по текущей карте риска и резерва.')
    else:
        lines.append('Первое действие — уточнить портфель ниже или задать ассистенту вопрос по причинам результата.')

    lines.extend(['', '➡️ Что делаем дальше?'])
    if children:
        first = next((x for x in children if x['delta_profit'] < 0), children[0])
        reserve = max(children, key=lambda x: opp_map.get(x['name'], 0))
        lines.append(f'1. Открыть «{first["name"]}» — главный риск или первая точка внимания.')
        lines.append(f'2. Открыть «{reserve["name"]}» — крупнейший подтверждённый резерв.')
    else:
        lines.append('1. Показать все объекты уровня.')
        lines.append('2. Показать причины результата.')
    lines.append(f'3. Показать полную витрину {child_label}.')
    lines.append('4. Показать причины.')
    lines.append('5. Создать задачи по выявленным приоритетам.')
    lines.append('6. Задать вопрос ассистенту: «что бы ты сделал первым?»')
    return [x for x in lines if str(x or '').strip()]

def _pr_contract_workspace_block(response: dict) -> list:
    ctx=response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower()!='network':
        return []
    contract=str(ctx.get('object_name') or '').strip(); period=str(ctx.get('period') or '').strip()
    filters={'network':contract}
    lines=[f'📍 Рабочий стол контракта — {contract} | {period}','👤 Рабочий стол: Контракт','🤖 Роль ассистента: цифровой помощник КАМ по развитию клиента','','🧠 Краткий вывод']
    fin_delta=_pr_metric_num(response,'Финрез до','delta_money')
    rev_delta=_pr_metric_num(response,'Оборот','delta_money')
    lines.append(f'Финрез ДО изменился на {_fmt_signed_int(fin_delta)} грн, оборот — на {_fmt_signed_int(rev_delta)} грн.')
    if fin_delta>0:
        lines.append('Контракт показывает положительную динамику; задача КАМ — понять, что закрепить, где расширить матрицу и какие условия контролировать.')
    else:
        lines.append('Контракт требует восстановления: сначала отделяем изменение структуры от экономики, затем ищем деньги в категориях, форматах и SKU.')
    lines.extend(['','📊 Ключевые показатели контракта','Показатель | Текущий период | Прошлый год | Изменение'])
    for name in ['Оборот','Финрез до','Маржа','Наценка']:
        val=_pr_metric_text(response,name)
        if val!='—': lines.append(f'{name} | {val}')
    # Contract passport: scale of the client inside the business.
    contract_rows=_pr_rows(period, **filters)
    business_rows=_pr_rows(period)
    if contract_rows and business_rows:
        cm=aggregate_metrics(contract_rows); bm=aggregate_metrics(business_rows)
        c_rev=_num(cm.get('revenue')); b_rev=_num(bm.get('revenue'))
        c_profit=_num(cm.get('finrez_pre')); b_profit=_num(bm.get('finrez_pre'))
        fmt_count=len({_pi72_format_name(r.get('tmc_group') or r.get('sku')) for r in contract_rows})
        lines.extend(['','🧾 Паспорт контракта','Показатель | Значение'])
        lines.append(f'Доля оборота бизнеса | {_fmt_percent_value((c_rev/b_rev*100) if b_rev else 0)}')
        lines.append(f'Доля прибыли бизнеса | {_fmt_percent_value((c_profit/b_profit*100) if abs(b_profit)>1e-9 else 0)}')
        lines.append(f'Категорий | {len({str(r.get("category") or "").strip() for r in contract_rows if str(r.get("category") or "").strip()})}')
        lines.append(f'Групп ТМС | {len({str(r.get("tmc_group") or "").strip() for r in contract_rows if str(r.get("tmc_group") or "").strip()})}')
        lines.append(f'Форматов | {fmt_count}')
        lines.append(f'SKU | {len({str(r.get("sku") or "").strip() for r in contract_rows if str(r.get("sku") or "").strip()})}')
    # economics vs business: Evidence First — object, benchmark, gap, effect.
    econ_rows = _w3_contract_factor_rows(response)
    if econ_rows:
        lines.extend(['','💰 Экономика контракта относительно бизнеса','Фактор | Контракт | Бизнес | Отклонение | Денежный эффект | Сигнал'])
        for item in econ_rows:
            lines.append(f"{item['name']} | {item['current_text']} | {item['base_text']} | {item['gap_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")
        comment = _w5_factor_comment(econ_rows)
        if comment:
            lines.append(comment)
    sitems=_pr_structural_items('network',period,filters)
    if sitems:
        lines.extend(['','🏗 Структура контракта','Показатель | Сейчас | Прошлый год | Δ'])
        for it in sitems:
            lines.append(f"{it['name']} | {it['current']} | {it['previous']} | {it['delta']:+d}")
    lines.extend(['']+_pr_trend_lines(period,filters,6))
    trend_comment = _w5_trend_comment(period, filters, f'контракта {contract}')
    if trend_comment:
        lines.append(trend_comment)
    # categories
    cats=_pr_group_table(period,'category',filters,10)
    biz_cat=_pr_group_table(period,'category',{},50)
    biz_cat_map={x['name']:x for x in biz_cat}
    if cats:
        lines.extend(['','📦 Категории в контракте','Категория | Оборот | Доля контракта | Доля бизнеса | Финрез | Δ прибыли | Действие'])
        for c in cats:
            bc=biz_cat_map.get(c['name'],{})
            lines.append(f"{c['name']} | {_fmt_int(c['revenue'])} | {_fmt_percent_value(c['share_revenue'])} | {_fmt_percent_value(bc.get('share_revenue'))} | {_fmt_signed_int(c['finrez'])} | {_fmt_signed_int(c['delta_profit'])} | разобрать")
    # formats in contract
    rows=_pr_rows(period, **filters)
    if rows:
        format_rows=[]; grouped={}
        for r in rows:
            fmt=_pi72_format_name(r.get('tmc_group') or r.get('sku'))
            grouped.setdefault(fmt,[]).append(r)
        total_rev=_num(aggregate_metrics(rows).get('revenue'))
        for fmt,rs in grouped.items():
            m=aggregate_metrics(rs); format_rows.append((fmt,_num(m.get('revenue')),_num(m.get('finrez_pre')),len({r.get('sku') for r in rs if r.get('sku')})))
        format_rows.sort(key=lambda x:x[1], reverse=True)
        lines.extend(['','📐 Форматы контракта','Формат | Оборот | Доля контракта | Финрез | SKU | Что делать'])
        for fmt,rev,fin,sku_count in format_rows[:8]:
            action='защитить/масштабировать' if rev>0 else 'оценить ввод'
            lines.append(f'{fmt} | {_fmt_int(rev)} | {_fmt_percent_value((rev/total_rev*100) if total_rev else 0)} | {_fmt_signed_int(fin)} | {sku_count} | {action}')
    # SKU leaders and missing
    sku_items=_pr_group_table(period,'sku',filters,10)
    biz_leaders=_pr_business_sku_leaders(period,30)
    contract_skus={x['name'] for x in sku_items}
    missing=[x for x in biz_leaders if x['sku'] not in contract_skus]
    if sku_items:
        biz_sku_rows=_pr_group_table(period,'sku',{},200)
        biz_sku_map={x['name']:x for x in biz_sku_rows}
        lines.extend(['','⭐ SKU-лидеры контракта','SKU | Оборот | Доля контракта | Доля бизнеса | Финрез | Сетей в бизнесе | Роль'])
        for s in sku_items[:8]:
            role='флагман' if s['share_revenue']>=15 else 'рабочая позиция'
            bs=biz_sku_map.get(s['name'], {})
            lines.append(f"{s['name']} | {_fmt_int(s['revenue'])} | {_fmt_percent_value(s['share_revenue'])} | {_fmt_percent_value(bs.get('share_revenue'))} | {_fmt_signed_int(s['finrez'])} | {_fmt_int(bs.get('network_count'))} | {role}")
    if missing:
        biz_sku_rows=_pr_group_table(period,'sku',{},200)
        biz_sku_map={x['name']:x for x in biz_sku_rows}
        lines.extend(['','➕ Лидеры бизнеса, которых нет в контракте','SKU | Ранг в бизнесе | Оборот бизнеса | Доля бизнеса | Финрез бизнеса | Сетей где есть | Почему предложить'])
        for s in missing[:10]:
            bs=biz_sku_map.get(s['sku'], {})
            lines.append(f"{s['sku']} | №{s['rank']} | {_fmt_int(s['revenue'])} | {_fmt_percent_value(bs.get('share_revenue'))} | {_fmt_signed_int(s.get('finrez'))} | {s['network_count']} | лидер бизнеса отсутствует в контракте")
    lines.extend(['','🚀 План развития контракта'])
    if cats: lines.append(f'1. Разобрать категорию «{cats[0]["name"]}»: максимальный вклад/изменение внутри контракта.')
    if missing: lines.append('2. Собрать пакет отсутствующих SKU-лидеров бизнеса для первой переговорной позиции.')
    lines.append('3. Проверить экономику условий: наценка, ретро, логистика, персонал, прочие относительно бизнеса.')
    lines.extend(['','🤝 Переговорный пакет КАМ','Цель: перейти от общего разговора о контракте к пакету развития: категория → формат → SKU → условия.'])
    if missing:
        lines.append('Аргумент: предлагаемые позиции уже доказаны бизнесом — имеют оборот, покрытие сетей и рейтинг в бизнесе.')
    lines.extend(['','✅ Что делаем дальше?','1. Подготовить переговоры по контракту.','2. Собрать пакет SKU для ввода.','3. Разобрать категорию с наибольшим эффектом.','4. Показать причины по экономике контракта.','5. Создать задачи КАМ / трейд-маркетингу / аналитикам.','6. Спросить ассистента: «какие SKU предложить первыми и почему?»'])
    return [x for x in lines if str(x or '').strip()]




# Sprint W3 рабочий стол Intelligence: Evidence First, Object Passport and SKU-FIRST blocks.
def _w3_factor_name(item: dict) -> str:
    return str(item.get('name') or item.get('factor') or 'Фактор').strip()


def _w3_signal(effect: Any) -> str:
    val = _num(effect)
    if val < 0:
        return 'риск'
    if val > 0:
        return 'драйвер'
    return 'нейтрально'


def _w3_factor_evidence_rows(response: dict, *, business: bool = False) -> list:
    source = response.get('reasons_block') if isinstance(response.get('reasons_block'), list) else []
    if not source:
        source = response.get('structure') if isinstance(response.get('structure'), list) else []
    rows = []
    for item in source:
        if not isinstance(item, dict):
            continue
        name = _w3_factor_name(item)
        current = item.get('percent', item.get('fact_percent', item.get('value_percent')))
        previous = item.get('previous_percent', item.get('pg_percent', item.get('base_percent')))
        previous_missing = bool(item.get('previous_percent_missing')) or previous is None
        if previous_missing:
            delta_text = item.get('previous_note') or 'нет корректной базы'
            previous_text = '—'
        else:
            delta = item.get('delta_vs_previous_percent')
            if delta is None:
                delta = _num(current) - _num(previous)
            delta_text = _fmt_pp_delta(delta)
            previous_text = _fmt_percent_value(previous)
        rows.append({
            'name': name,
            'current_text': _fmt_percent_value(current),
            'previous_text': previous_text,
            'delta_text': delta_text,
            'effect': item.get('effect_money'),
            'signal': _w3_signal(item.get('effect_money')),
        })
    return rows


def _w3_contract_factor_rows(response: dict) -> list:
    source = response.get('structure') if isinstance(response.get('structure'), list) else []
    rows = []
    for item in source:
        if not isinstance(item, dict):
            continue
        current = item.get('percent', item.get('fact_percent', item.get('value_percent')))
        base = item.get('base_percent')
        gap = _num(current) - _num(base)
        rows.append({
            'name': _w3_factor_name(item),
            'current_text': _fmt_percent_value(current),
            'base_text': _fmt_percent_value(base),
            'gap_text': _fmt_pp_delta(gap),
            'effect': item.get('effect_money'),
            'signal': _w3_signal(item.get('effect_money')),
        })
    return rows


def _w3_group_map(period: str, field: str, filters: dict) -> dict:
    grouped = {}
    for row in _pr_rows(period, **filters):
        name = str(row.get(field) or '').strip()
        if name:
            grouped.setdefault(name, []).append(row)
    return grouped


def _w3_total_metrics(rows: list) -> dict:
    return aggregate_metrics(rows) if rows else {}


def _w3_share(part: Any, total: Any) -> float:
    total_num = _num(total)
    return (_num(part) / total_num * 100.0) if abs(total_num) > 1e-9 else 0.0


def _w3_format_table(period: str, filters: dict, business_filters: dict, *, limit: int = 12) -> list:
    rows = _pr_rows(period, **filters)
    business_rows = _pr_rows(period, **business_filters)
    total_rev = _num(_w3_total_metrics(rows).get('revenue'))
    biz_total_rev = _num(_w3_total_metrics(business_rows).get('revenue'))
    prev_rows = _pr_rows(_pr_prev_year(period), **filters)
    prev_by_fmt = {}
    for r in prev_rows:
        prev_by_fmt.setdefault(_pi72_format_name(r.get('tmc_group') or r.get('sku')), []).append(r)
    grouped = {}
    for r in rows:
        grouped.setdefault(_pi72_format_name(r.get('tmc_group') or r.get('sku')), []).append(r)
    biz_grouped = {}
    for r in business_rows:
        biz_grouped.setdefault(_pi72_format_name(r.get('tmc_group') or r.get('sku')), []).append(r)
    all_formats = set(grouped) | set(biz_grouped)
    out = []
    for fmt in all_formats:
        rs = grouped.get(fmt, [])
        brs = biz_grouped.get(fmt, [])
        prs = prev_by_fmt.get(fmt, [])
        m = _w3_total_metrics(rs); bm = _w3_total_metrics(brs); pm = _w3_total_metrics(prs)
        rev = _num(m.get('revenue')); fin = _num(m.get('finrez_pre')); prev_fin = _num(pm.get('finrez_pre'))
        biz_rev = _num(bm.get('revenue'))
        out.append({
            'format': fmt,
            'revenue': rev,
            'finrez': fin,
            'delta_profit': fin - prev_fin,
            'share_object': _w3_share(rev, total_rev),
            'share_business': _w3_share(biz_rev, biz_total_rev),
            'sku_count': len({str(r.get('sku') or '').strip() for r in rs if str(r.get('sku') or '').strip()}),
            'potential': max(0.0, ( _w3_share(biz_rev, biz_total_rev) - _w3_share(rev, total_rev) ) * max(total_rev, 0) / 100.0),
            'present': bool(rs),
        })
    out.sort(key=lambda x: (x.get('present', False), x.get('revenue') or x.get('potential') or 0), reverse=True)
    return out[:limit]


def _w3_sku_table(period: str, filters: dict, business_filters: dict, *, limit: int = 12, missing_only: bool = False) -> list:
    rows = _pr_rows(period, **filters)
    business_rows = _pr_rows(period, **business_filters)
    total_rev = _num(_w3_total_metrics(rows).get('revenue'))
    biz_total_rev = _num(_w3_total_metrics(business_rows).get('revenue'))
    current_names = {str(r.get('sku') or '').strip() for r in rows if str(r.get('sku') or '').strip()}
    prev_rows = _pr_rows(_pr_prev_year(period), **filters)
    prev_by_sku = {}
    for r in prev_rows:
        name = str(r.get('sku') or '').strip()
        if name:
            prev_by_sku.setdefault(name, []).append(r)
    grouped = _w3_group_map(period, 'sku', filters)
    biz_grouped = _w3_group_map(period, 'sku', business_filters)
    biz_rank = sorted(((name, _num(_w3_total_metrics(rs).get('revenue'))) for name, rs in biz_grouped.items()), key=lambda x: x[1], reverse=True)
    rank_map = {name: idx for idx, (name, _) in enumerate(biz_rank, 1)}
    names = set(biz_grouped) if missing_only else (set(grouped) | set(biz_grouped))
    out = []
    for sku in names:
        if missing_only and sku in current_names:
            continue
        rs = grouped.get(sku, [])
        brs = biz_grouped.get(sku, [])
        prs = prev_by_sku.get(sku, [])
        m = _w3_total_metrics(rs); bm = _w3_total_metrics(brs); pm = _w3_total_metrics(prs)
        rev = _num(m.get('revenue')); fin = _num(m.get('finrez_pre')); prev_fin = _num(pm.get('finrez_pre'))
        biz_rev = _num(bm.get('revenue')); biz_fin = _num(bm.get('finrez_pre'))
        networks = len({str(r.get('network') or '').strip() for r in brs if str(r.get('network') or '').strip()})
        out.append({
            'sku': sku,
            'format': _pi72_format_name((brs[0].get('tmc_group') or sku) if brs else sku),
            'revenue': rev,
            'finrez': fin,
            'delta_profit': fin - prev_fin,
            'share_object': _w3_share(rev, total_rev),
            'business_revenue': biz_rev,
            'business_finrez': biz_fin,
            'share_business': _w3_share(biz_rev, biz_total_rev),
            'network_count': networks,
            'rank': rank_map.get(sku),
            'potential': max(0.0, biz_fin if missing_only else 0.0),
            'present': bool(rs),
        })
    if missing_only:
        out.sort(key=lambda x: (x.get('business_revenue') or 0), reverse=True)
    else:
        out.sort(key=lambda x: (x.get('revenue') or x.get('business_revenue') or 0), reverse=True)
    return out[:limit]


def _w3_parent_contract_from_response(response: dict) -> str:
    return _pi72_extract_network_from_path(response)


def _w3_category_workspace_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'category':
        return []
    category = str(ctx.get('object_name') or '').strip()
    period = str(ctx.get('period') or '').strip()
    contract = _w3_parent_contract_from_response(response)
    if not category or not period:
        return []
    filters = {'category': category}
    if contract:
        filters['network'] = contract
    rows = _pr_rows(period, **filters)
    biz_filters = {'category': category}
    biz_rows = _pr_rows(period, **biz_filters)
    total = _w3_total_metrics(rows); biz_total = _w3_total_metrics(biz_rows)
    contract_rows = _pr_rows(period, network=contract) if contract else []
    business_rows = _pr_rows(period)
    rev = _num(total.get('revenue')); fin = _num(total.get('finrez_pre'))
    biz_rev = _num(biz_total.get('revenue')); biz_fin = _num(biz_total.get('finrez_pre'))
    contract_rev = _num(_w3_total_metrics(contract_rows).get('revenue'))
    business_rev = _num(_w3_total_metrics(business_rows).get('revenue'))
    lines = [
        f'📦 Рабочий стол категории — {category}' + (f' | {contract}' if contract else ''),
        f'Период: {period}',
        '🤖 Роль ассистента: помощник КАМ по развитию категории',
        '',
        '🧾 Паспорт категории',
        'Показатель | Значение',
        f'Контракт | {contract or "—"}',
        f'Оборот категории в контракте | {_fmt_int(rev)} грн',
        f'Финрез ДО категории в контракте | {_fmt_signed_int(fin)} грн',
        f'Доля в контракте | {_fmt_percent_value(_w3_share(rev, contract_rev))}',
        f'Доля в бизнесе | {_fmt_percent_value(_w3_share(biz_rev, business_rev))}',
        f'SKU в категории | {len({str(r.get("sku") or "").strip() for r in rows if str(r.get("sku") or "").strip()})}',
        '',
        '📊 Ключевые показатели категории',
        'Показатель | Текущий период | Прошлый год | Изменение | Доля контракта | Доля бизнеса',
    ]
    metric_map = {str(x.get('name') or ''): x for x in (response.get('metrics') or []) if isinstance(x, dict)}
    for name in ['Оборот', 'Финрез до', 'Маржа', 'Наценка']:
        item = metric_map.get(name)
        if not item:
            continue
        if name in {'Маржа', 'Наценка'}:
            lines.append(f'{name} | {_fmt_percent_value(item.get("fact_percent"))} | {_fmt_percent_value(item.get("pg_percent"))} | {_fmt_pp_delta(item.get("delta_percent"))} | — | —')
        else:
            fact = item.get('fact_money'); prev = item.get('pg_money'); delta = item.get('delta_money')
            share_contract = _w3_share(fact, contract_rev) if name == 'Оборот' else _w3_share(fact, _num(_w3_total_metrics(contract_rows).get('finrez_pre')))
            share_business = _w3_share(biz_rev if name == 'Оборот' else biz_fin, business_rev if name == 'Оборот' else _num(_w3_total_metrics(business_rows).get('finrez_pre')))
            lines.append(f'{name} | {_fmt_int(fact)} | {_fmt_int(prev)} | {_fmt_signed_int(delta)} | {_fmt_percent_value(share_contract)} | {_fmt_percent_value(share_business)}')
    factors = _w3_factor_evidence_rows(response)
    if factors:
        lines.extend(['','💰 Экономика категории: доказательства','Фактор | Текущий период | Прошлый год | Изменение | Денежный эффект | Сигнал'])
        for item in factors:
            lines.append(f"{item['name']} | {item['current_text']} | {item['previous_text']} | {item['delta_text']} | {_fmt_signed_int(item['effect'])} грн | {item['signal']}")
        comment = _w5_factor_comment(factors)
        if comment:
            lines.append(comment)
    formats = _w3_format_table(period, filters, biz_filters, limit=10)
    if formats:
        lines.extend(['','📐 Форматы категории','Формат | Оборот | Доля категории | Доля бизнеса | SKU | Потенциал | Действие'])
        for f in formats:
            action = 'защитить/масштабировать' if f.get('present') and _num(f.get('share_object')) >= 20 else ('ввести/проверить' if not f.get('present') else 'оценить развитие')
            lines.append(f"{f['format']} | {_fmt_int(f['revenue'])} | {_fmt_percent_value(f['share_object'])} | {_fmt_percent_value(f['share_business'])} | {f['sku_count']} | {_fmt_int(f['potential'])} | {action}")
    sku_leaders = _w3_sku_table(period, filters, biz_filters, limit=10, missing_only=False)
    sku_leaders = [x for x in sku_leaders if x.get('present')]
    if sku_leaders:
        lines.extend(['','⭐ SKU-лидеры категории','SKU | Формат | Оборот | Доля категории | Доля бизнеса | Финрез | Роль'])
        for s in sku_leaders[:8]:
            role = 'якорь' if _num(s.get('share_object')) >= 15 else 'рабочая позиция'
            lines.append(f"{s['sku']} | {s['format']} | {_fmt_int(s['revenue'])} | {_fmt_percent_value(s['share_object'])} | {_fmt_percent_value(s['share_business'])} | {_fmt_signed_int(s['finrez'])} | {role}")
    missing = _w3_sku_table(period, filters, biz_filters, limit=10, missing_only=True)
    if missing:
        lines.extend(['','➕ Отсутствующие SKU / форматы','SKU | Формат | Ранг в бизнесе | Оборот бизнеса | Сетей где есть | Потенциал | Почему важно'])
        for s in missing[:8]:
            lines.append(f"{s['sku']} | {s['format']} | №{s.get('rank') or '—'} | {_fmt_int(s['business_revenue'])} | {s['network_count']} | {_fmt_int(s['potential'])} | закрывает пробел категории")
    lines.extend(['','🎯 План развития категории'])
    if formats:
        target = next((f for f in formats if not f.get('present') and _num(f.get('share_business')) > 0), formats[0])
        lines.append(f"1. Сначала проверить формат {target['format']}: доля в бизнесе {_fmt_percent_value(target.get('share_business'))}, текущая доля в категории {_fmt_percent_value(target.get('share_object'))}.")
    if missing:
        lines.append('2. После выбора формата собрать первую волну SKU из отсутствующих лидеров бизнеса.')
    lines.append('3. Подготовить переговорный аргумент по категории: сначала структура, затем SKU, затем условия.')
    lines.extend(['','➡️ Что делаем дальше?','1. Подготовить пакет развития категории.','2. Показать все SKU категории.','3. Подготовить переговорный аргумент.','4. Открыть SKU-лидера как доказательство.','5. Создать задачи.'])
    return [x for x in lines if str(x or '').strip()]


def _w3_sku_passport_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() != 'sku':
        return []
    sku = str(ctx.get('object_name') or '').strip()
    period = str(ctx.get('period') or '').strip()
    passport = response.get('sku_passport') if isinstance(response.get('sku_passport'), dict) else {}
    if not sku or not period:
        return []
    contract = passport.get('contract') or _pi72_extract_network_from_path(response)
    ident = passport.get('identification') if isinstance(passport.get('identification'), dict) else {}
    role = passport.get('business_role') if isinstance(passport.get('business_role'), dict) else {}
    econ = passport.get('economics') if isinstance(passport.get('economics'), dict) else {}
    presence = passport.get('presence') if isinstance(passport.get('presence'), dict) else {}
    decision = passport.get('decision') if isinstance(passport.get('decision'), dict) else {}
    lines = [
        f'🧾 Паспорт SKU 2.0 — {sku}',
        f'Период: {period}' + (f' | Контракт: {contract}' if contract else ''),
        '🤖 Роль ассистента: помощник по развитию продукта',
        '',
        '🧬 Идентификация SKU',
        'Поле | Значение',
        f'Категория | {ident.get("category") or "—"}',
        f'Группа ТМС | {ident.get("tmc_group") or "—"}',
        f'Формат | {ident.get("format") or _pi72_format_name(sku)}',
        f'Роль | {role.get("role") or "—"}',
        '',
        '📊 KPI SKU: доказательная база',
        'Показатель | Текущий период | Прошлый год | Δ / комментарий',
        f'Оборот | {_fmt_int(econ.get("revenue"))} | {_fmt_int(econ.get("previous_revenue"))} | {_fmt_signed_int(_num(econ.get("revenue")) - _num(econ.get("previous_revenue")))}',
        f'Финрез ДО | {_fmt_signed_int(econ.get("finrez_pre"))} | {_fmt_signed_int(econ.get("previous_finrez_pre"))} | {_fmt_signed_int(econ.get("profit_delta_money"))}',
        f'Маржа | {_fmt_percent_value(econ.get("margin_pre_percent"))} | — | проверять базу сравнения',
        f'Наценка | {_fmt_percent_value(econ.get("markup_percent"))} | — | проверять базу сравнения',
        '',
        '🏢 Роль SKU в бизнесе',
        'Метрика | Значение',
        f'Оборот SKU по бизнесу | {_fmt_int(role.get("business_revenue"))} грн',
        f'Финрез SKU по бизнесу | {_fmt_signed_int(role.get("business_finrez_pre"))} грн',
        f'Доля бизнеса | {_fmt_percent_value(role.get("business_share_percent"))}',
        f'Доля категории | {_fmt_percent_value(role.get("category_share_percent"))}',
        f'Доля группы ТМС | {_fmt_percent_value(role.get("tmc_group_share_percent"))}',
        f'Ранг по обороту бизнеса | {_fmt_rank(role.get("rank_revenue_business"))}',
        f'Ранг по прибыли бизнеса | {_fmt_rank(role.get("rank_profit_business"))}',
        f'Покрытие сетей | {role.get("network_count") or 0} из {role.get("total_network_count") or 0}',
    ]
    top_networks = presence.get('top_networks') if isinstance(presence.get('top_networks'), list) else []
    if top_networks:
        lines.extend(['','⭐ Где SKU работает лучше всего','Сеть | Оборот | Доля SKU | Финрез'])
        for n in top_networks[:8]:
            if not isinstance(n, dict): continue
            lines.append(f"{n.get('network')} | {_fmt_int(n.get('revenue'))} | {_fmt_percent_value(n.get('share_sku_percent'))} | {_fmt_signed_int(n.get('finrez_pre'))}")
    missing = presence.get('missing_networks') if isinstance(presence.get('missing_networks'), list) else []
    if missing:
        lines.extend(['','➕ Где SKU отсутствует','Сеть | Почему важно | Приоритет'])
        for net in missing[:10]:
            lines.append(f'{net} | позиция уже имеет бизнес-доказательство и отсутствует в сети | оценить ввод')
    lines.extend(['','🗣 Переговорная позиция по SKU','Аргумент | Доказательство | Ответ на возражение'])
    lines.append(f'Позиция доказана бизнесом | оборот {_fmt_int(role.get("business_revenue"))}, покрытие {role.get("network_count") or 0} сетей, ранг {_fmt_rank(role.get("rank_revenue_business"))} | предложить тест / первую волну, не спорить по полной матрице')
    lines.append(f'Логика развития | {decision.get("development_logic") or "использовать как доказательство"} | если нет места — предложить ограниченный ввод')
    lines.extend(['','🎯 Управленческий вывод', decision.get('recommended_action') or 'Использовать паспорт SKU как доказательную базу для развития, ввода или защиты позиции.', '', '➡️ Что делаем дальше?', '1. Подготовить переговоры с этим SKU как аргументом.', '2. Создать задачу по SKU.', '3. Вернуться к категории и собрать пакет развития.', '4. Показать витрину SKU / где отсутствует.'])
    return [x for x in lines if str(x or '').strip()]



def _w4_set_primary_workspace(payload: dict, block_key: str) -> dict:
    """Sprint W4 Information Recovery: make the recovered рабочий стол block explicit.

    Custom GPT can otherwise summarize older short legacy blocks. This helper
    exposes a single primary рабочий стол artifact with full evidence tables.
    """
    block = payload.get(block_key) if isinstance(payload.get(block_key), list) else []
    if not block:
        return payload
    payload['workspace_primary_block'] = block
    payload['workspace_markdown'] = '\n'.join(str(x) for x in block if str(x or '').strip())
    payload['summary_block'] = 'Основной рабочий стол находится в workspace_primary_block. Выводить его полностью, не сокращая доказательные таблицы.'
    order = payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
    payload['screen_order'] = ['workspace_primary_block', block_key, 'navigation_block'] + [
        x for x in order
        if x not in {'workspace_primary_block', block_key, 'summary_block', 'result_block', 'diagnosis_block', 'explanation_block', 'next_step_block', 'recommended_next_step_block', 'drain_block_render', 'kpi_block', 'structure_block'}
    ]
    return payload

def _attach_product_recovery_blocks(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return payload
    if payload.get('render_mode') in {'list_only','reasons','kpi_only','voice_diagnostic','action_package','negotiation_workspace','task_workspace','post_meeting_workspace','execution_workspace'}:
        return payload
    ctx=payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level=str(ctx.get('level') or '').strip().lower()
    if level=='business':
        payload['business_workspace_block']=_pr_business_workspace_block(payload)
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['business_workspace_block']+[x for x in order if x!='business_workspace_block']
        payload=_w4_set_primary_workspace(payload, 'business_workspace_block')
    elif level in {'manager_top','manager'}:
        payload['management_workspace_block']=_pr_management_workspace_block(payload)
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['management_workspace_block']+[x for x in order if x!='management_workspace_block']
        payload=_w4_set_primary_workspace(payload, 'management_workspace_block')
    elif level=='network':
        payload['contract_workspace_block']=_pr_contract_workspace_block(payload)
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['contract_workspace_block']+[x for x in order if x!='contract_workspace_block']
        payload=_w4_set_primary_workspace(payload, 'contract_workspace_block')
    elif level=='category':
        payload['category_workspace_block']=_w3_category_workspace_block(payload)
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['category_workspace_block']+[x for x in order if x!='category_workspace_block']
        payload=_w4_set_primary_workspace(payload, 'category_workspace_block')
    elif level=='sku':
        payload['sku_passport_block']=_w3_sku_passport_block(payload) or payload.get('sku_passport_block') or []
        order=payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
        payload['screen_order']=['sku_passport_block']+[x for x in order if x!='sku_passport_block']
        payload=_w4_set_primary_workspace(payload, 'sku_passport_block')
    return payload

def _stage7_screen_order(response: dict) -> list:
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only', 'voice_diagnostic'}:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if level == 'business':
        return ['management_workspace_block', 'result_block', 'diagnosis_block', 'anomaly_explanation_block', 'recommended_next_step_block', 'factor_change_block', 'opportunity_rating_block', 'opportunity_explanation_block', 'decision_block_render', 'navigation_block']
    if level == 'network':
        # Stage 8.4: Network is the full Рабочий стол контракта.
        # It is not a short redirect/wizard and not a single Decision block.
        # The user receives the full contract desktop first, then can freely
        # continue with categories, SKU, negotiations, tasks or assistant dialogue.
        return [
            'result_block',
            'diagnosis_block',
            'anomaly_explanation_block',
            'business_context_block',
            'narrative_block',
            'business_opportunity_block',
            'recommendation_block',
            'product_workspace_block',
            'factor_change_block',
            'benchmark_diagnostic_block',
            'opportunity_rating_block',
            'opportunity_explanation_block',
            'decision_workspace_block',
            'decision_block_render',
            'navigation_block',
        ]
    
    if level in {'category', 'tmc_group'}:
        return ['result_block', 'diagnosis_block', 'business_context_block', 'narrative_block', 'business_opportunity_block', 'recommendation_block', 'product_workspace_block', 'anomaly_explanation_block', 'recommended_next_step_block', 'category_workspace_block', 'factor_change_block', 'benchmark_diagnostic_block', 'product_tmc_decision_block', 'opportunity_rating_block', 'opportunity_explanation_block', 'decision_block_render', 'navigation_block']
    if level == 'sku':
        return ['sku_passport_block', 'business_context_block', 'narrative_block', 'business_opportunity_block', 'recommendation_block', 'product_workspace_block', 'factor_change_block', 'benchmark_diagnostic_block', 'decision_block_render', 'navigation_block']
    return ['management_workspace_block', 'result_block', 'diagnosis_block', 'anomaly_explanation_block', 'recommended_next_step_block', 'factor_change_block', 'benchmark_diagnostic_block', 'opportunity_rating_block', 'opportunity_explanation_block', 'decision_block_render', 'navigation_block']


def _build_next_step_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if response.get('render_mode') in {'list_only', 'reasons', 'kpi_only'}:
        return []
    if level == 'business':
        losses = response.get('profit_loss_rating') or []
        first_loss = losses[0].get('object_name') if losses and isinstance(losses[0], dict) else None
        if first_loss:
            return [f'Рекомендуемый следующий шаг: открыть {first_loss} как крупнейшую просадку прибыли.']
        return ['Рекомендуемый следующий шаг: открыть полный список и найти крупнейшую просадку прибыли.']
    if _is_product_layer_level(level):
        return [
            'Что делаем дальше: подготовить пакет развития категории, разобрать форматы и позиции или собрать переговорный аргумент.',
            'Можно задать вопрос ассистенту свободно: «какие позиции предложить первыми?» или «где быстрый эффект по категории?»',
        ]
    if _num(response.get('opportunity_money')) > 0:
        return ['Следующий шаг: открыть объекты ниже и найти резерв внутри уже выбранной проблемы.']
    return ['Следующий шаг: проверить факторы и подтвердить контекст причины.']

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
    metrics = response.get('metrics') or []
    fin_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Финрез до'))
    rev_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Оборот'))
    if fin_delta < 0 and rev_delta < 0:
        return ['Проверить причину падения оборота → требуется контрактный/продуктовый контекст']

    # Stage 8: priority action must follow the main source of Opportunity,
    # not the strongest factor. Uses benchmark gaps only.
    gap_reasons = _opportunity_gap_reasons(response, limit=1)
    if gap_reasons:
        reason = gap_reasons[0]
        effect = abs(_reason_effect_vs_business(reason))
        return [f'{_action_text_for_reason(reason)} → потенциальный эффект до {_fmt_int(effect)}']

    if not isinstance(action, dict) or not action:
        return []
    text = _action_display_label(action)
    effect = action.get('expected_effect_money')
    if effect is None:
        effect = action.get('effect_money')
    text = text.replace('Сократить', 'Проверить').replace('Снизить', 'Проверить').replace('Повысить', 'Проверить')
    if fin_delta > 0:
        return [f'{text} → дополнительный потенциал до {_render_money_value(abs(_num(effect)))}']
    return [f'{text} → потенциальный эффект до {_render_money_value(abs(_num(effect)))}']


def _profit_first_metric_lines(response):
    metrics = response.get('metrics') or []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    order = ['Финрез до', 'Маржа', 'Оборот', 'Наценка']
    if level == 'business':
        order.append('Финрез итог')
    lines = []
    for name in order:
        item = _metric_by_name(metrics, name)
        if not item:
            continue
        if name in {'Маржа', 'Наценка'}:
            lines.append(
                f'{name}: текущий период {_fmt_percent_value(item.get("fact_percent"))} | '
                f'прошлый год {_fmt_percent_value(item.get("pg_percent"))} | '
                f'изменение {_fmt_pp_delta(item.get("delta_percent"))}'
            )
        else:
            lines.append(
                f'{name}: текущий период {_fmt_int(item.get("fact_money"))} | '
                f'прошлый год {_fmt_int(item.get("pg_money"))} | '
                f'изменение {_fmt_signed_int(item.get("delta_money"))}'
            )
    return lines


def _period_result_money(response):
    """CHANGE-006.2: primary object KPI is profit movement vs previous period.

    This is intentionally not Benchmark Money. It answers: did the object
    earn more or less than the same object in the previous year?
    """
    metric = _metric_by_name(response.get('metrics') or [], 'Финрез до')
    if metric and metric.get('delta_money') is not None:
        return _intnum(metric.get('delta_money'))
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() == 'business':
        return _intnum(response.get('business_result_money'))
    return 0


def _render_period_result_block(response):
    value = _period_result_money(response)
    return [
        f'🎯 Результат периода: {_fmt_signed_int(value)} к прошлому году'
    ]


def _render_business_result_block(response):
    lines = []
    lines.extend(_render_period_result_block(response))
    lines.append('📊 Что произошло с прибылью')
    lines.extend(_profit_first_metric_lines(response))
    return lines


def _render_object_result_block(response):
    # CHANGE-006.2: Benchmark Money is no longer rendered as a separate money block.
    # The object screen starts from result of the period: delta profit vs previous year.
    lines = []
    lines.extend(_render_period_result_block(response))
    lines.append('📊 Что произошло с объектом')
    lines.extend(_profit_first_metric_lines(response))
    return lines


def _render_opportunity_block(response):
    value = response.get("opportunity_money")
    return [f'💰 Потенциал прибыли внутри выбранной проблемы: {_fmt_int(abs(_num(value)))} грн']


def _render_result_block(response):
    """CHANGE-005.1: Profit First render contract.

    The first rendered block must answer «Что произошло?» using object vs
    previous period. Benchmark and Opportunity are rendered only after that.
    """
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or response.get('level') or '').strip().lower()
    if level == 'business':
        return _render_business_result_block(response)

    lines = []
    lines.extend(_render_object_result_block(response))
    return lines


def _metric_fact_revenue(response):
    for item in response.get('metrics') or []:
        if str(item.get('name') or '').strip().lower() == 'оборот':
            return abs(_num(item.get('fact_money')))
    return 0.0



def _workspace_action_label(action: dict) -> str:
    if not isinstance(action, dict):
        return 'Проверить фактор'
    text = str(action.get('action') or action.get('problem') or '').strip()
    effect = action.get('expected_effect_money')
    suffix = f' | эффект до {_fmt_int(effect)} грн' if effect is not None else ''
    return f'{text}{suffix}' if text else f'Проверить фактор{suffix}'


def _render_potential_breakdown(potential: dict, *, limit: int = 3) -> str:
    if not isinstance(potential, dict):
        return 'потенциал не разложен'
    items = potential.get('items') if isinstance(potential.get('items'), list) else []
    if not items:
        total = potential.get('total_money')
        return f'потенциал {_fmt_int(total)} грн' if total is not None else 'потенциал не разложен'
    parts = []
    for item in items[:limit]:
        if not isinstance(item, dict):
            continue
        name = item.get('name') or item.get('factor') or 'фактор'
        effect = item.get('effect_money')
        parts.append(f'{name} {_fmt_int(effect)} грн')
    return '; '.join(parts) if parts else 'потенциал не разложен'


def _assortment_skew_lines(assortment: dict, categories: list) -> list:
    lines = []
    sku_leaders = assortment.get('sku_leaders_contract') if isinstance(assortment.get('sku_leaders_contract'), list) else []
    missing_sku = assortment.get('missing_business_sku_leaders') if isinstance(assortment.get('missing_business_sku_leaders'), list) else []
    if sku_leaders:
        top_share = sum(_num(item.get('share_network_percent')) for item in sku_leaders[:5] if isinstance(item, dict))
        if top_share >= 65:
            lines.append(f'Высокая концентрация: ТОП-5 позиций дают около {_fmt_percent(top_share)}% оборота контракта.')
        elif top_share > 0:
            lines.append(f'ТОП-5 позиций дают около {_fmt_percent(top_share)}% оборота контракта — это основа текущей матрицы.')
    missing_count = _intnum(assortment.get('missing_business_leader_count'))
    if missing_count > 0:
        lines.append(f'Есть ассортиментное окно: отсутствует {missing_count} лидеров бизнеса.')
    if categories:
        top_category = categories[0]
        share = _num(top_category.get('share_contract_revenue_percent'))
        name = top_category.get('category') or 'категория'
        if share >= 45:
            lines.append(f'Контракт заметно опирается на категорию «{name}»: {_fmt_percent(share)}% оборота контракта.')
    return lines


def _render_decision_workspace_block(response):
    """Render Network as Рабочий стол контракта 2.0.

    The block is not a KPI report and not a forced wizard. It adds the assistant
    layer required by the product model: evidence → interpretation → priority →
    action navigation. All numbers come from API/DATA structures already present
    in the response.
    """
    workspace = response.get('decision_workspace')
    if not isinstance(workspace, dict) or not workspace:
        return []

    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    contract = workspace.get('contract') or ctx.get('object_name') or response.get('object_name') or 'контракт'
    period = workspace.get('period') or ctx.get('period') or response.get('period') or ''
    diagnostics = workspace.get('contract_diagnostics') if isinstance(workspace.get('contract_diagnostics'), dict) else {}
    categories = workspace.get('category_intelligence') if isinstance(workspace.get('category_intelligence'), list) else []
    actions = workspace.get('recommended_actions') if isinstance(workspace.get('recommended_actions'), list) else []
    assortment = workspace.get('assortment_analysis') if isinstance(workspace.get('assortment_analysis'), dict) else {}
    sku_leaders = assortment.get('sku_leaders_contract') if isinstance(assortment.get('sku_leaders_contract'), list) else []
    missing_sku = assortment.get('missing_business_sku_leaders') if isinstance(assortment.get('missing_business_sku_leaders'), list) else []
    negotiation = workspace.get('negotiation_package') if isinstance(workspace.get('negotiation_package'), dict) else {}
    structural = workspace.get('structural_analysis') if isinstance(workspace.get('structural_analysis'), dict) else {}

    profit_delta = diagnostics.get('profit_delta_money')
    revenue_current = diagnostics.get('revenue_current')
    margin_current = diagnostics.get('margin_current_percent')
    margin_business = diagnostics.get('margin_business_percent')

    lines = [
        '🧭 Рабочий стол контракта',
        f'{contract}' + (f' | {period}' if period else ''),
        '',
        '🧠 Разбор ассистента',
    ]

    if profit_delta is not None:
        if _num(profit_delta) >= 0:
            lines.append(f'Смотри, по контракту сейчас положительное движение: финрез до вырос на {_fmt_signed_int(profit_delta)} грн к прошлому году.')
        else:
            lines.append(f'Смотри, по контракту сейчас просадка: финрез до изменился на {_fmt_signed_int(profit_delta)} грн к прошлому году.')
    if revenue_current is not None:
        lines.append(f'Масштаб контракта по текущему обороту: {_fmt_int(revenue_current)} грн. Это база, от которой считаем доли категорий и позиций.')
    if margin_current is not None and margin_business is not None:
        delta_margin = _num(margin_current) - _num(margin_business)
        if delta_margin >= 0:
            lines.append(f'Маржа контракта выше бизнеса на {_fmt_pp_delta(delta_margin)}: доходность сейчас является сильной стороной, а не главной проблемой.')
        else:
            lines.append(f'Маржа контракта ниже бизнеса на {_fmt_pp_delta(delta_margin)}: здесь нужно смотреть экономику и структуру ассортимента.')

    if actions:
        first = actions[0]
        lines.append(f'Главный подтверждённый управленческий приоритет по экономике: {_workspace_action_label(first)}.')

    structural_items = structural.get('items') if isinstance(structural.get('items'), list) else []
    if structural_items:
        lines.append('')
        lines.append('🏗 Изменение структуры')
        lines.append('Показатель | Прошлый год | Сейчас | Δ')
        for item in structural_items:
            if not isinstance(item, dict):
                continue
            delta = _num(item.get('delta'))
            delta_text = _fmt_signed_int(delta)
            lines.append(f'{item.get("name") or "Показатель"} | {_fmt_int(item.get("previous_year"))} | {_fmt_int(item.get("current"))} | {delta_text}')
        if structural.get('is_material'):
            lines.append('Структура контракта изменилась. Поэтому финансовую динамику нужно читать не только как изменение экономики, но и как изменение состава контракта.')

    if categories:
        lines.append('')
        lines.append('📦 Категории')
        lines.append('Категория | Оборот | Доля контракта | Доля бизнеса | Δ прибыли | Потенциал')
        for item in categories[:7]:
            if not isinstance(item, dict):
                continue
            name = item.get('category') or item.get('object_name') or 'Категория'
            lines.append(
                f'{name} | {_fmt_int(item.get("revenue"))} грн | '
                f'{_fmt_percent(item.get("share_contract_revenue_percent"))}% | '
                f'{_fmt_percent(item.get("share_business_revenue_percent"))}% | '
                f'{_fmt_signed_int(item.get("profit_delta_money"))} грн | '
                f'{_fmt_int(item.get("opportunity_money"))} грн'
            )
        best = categories[0]
        best_name = best.get('category') or 'категория'
        lines.append(f'По категориям первой в разбор просится «{best_name}»: у неё самый высокий рабочий вес в этом контракте по текущим данным.')
        breakdown = _render_potential_breakdown(best.get('potential_breakdown') if isinstance(best, dict) else {})
        lines.append(f'Потенциал «{best_name}» нужно читать не одной суммой: {breakdown}.')

    if sku_leaders:
        lines.append('')
        lines.append('⭐ Разбор SKU: лидеры ассортимента в контракте')
        lines.append('Позиция | Оборот | Доля контракта | Δ прибыли | Роль | Что это означает')
        for item in sku_leaders[:10]:
            if not isinstance(item, dict):
                continue
            sku = item.get('sku') or 'Позиция'
            lines.append(
                f'{sku} | {_fmt_int(item.get("revenue"))} грн | '
                f'{_fmt_percent(item.get("share_network_percent"))}% | '
                f'{_fmt_signed_int(item.get("profit_delta_money"))} грн | '
                f'{item.get("role") or "роль не определена"} | '
                f'{item.get("development_logic") or "использовать как доказательную базу"}'
            )

    sku_intelligence = assortment.get('sku_intelligence') if isinstance(assortment.get('sku_intelligence'), dict) else {}
    if sku_intelligence:
        lines.append('')
        lines.append('🧩 Ассортиментная логика')
        concentration = sku_intelligence.get('concentration_level')
        top5 = sku_intelligence.get('top5_share_percent')
        if concentration == 'high':
            lines.append(f'ТОП-5 позиций дают около {_fmt_percent(top5)}% оборота контракта. Это сильная база, но есть риск зависимости от узкой матрицы.')
        elif concentration == 'medium':
            lines.append(f'ТОП-5 позиций дают около {_fmt_percent(top5)}% оборота контракта. Матрица имеет выраженных лидеров, но не выглядит критично узкой.')
        elif top5 is not None:
            lines.append(f'ТОП-5 позиций дают около {_fmt_percent(top5)}% оборота контракта. Матрица выглядит относительно сбалансированной.')
        plan = sku_intelligence.get('development_plan') if isinstance(sku_intelligence.get('development_plan'), list) else []
        if plan:
            lines.append('План развития ассортимента:')
            for idx, step in enumerate(plan[:3], 1):
                lines.append(f'{idx}. {step}.')

    if missing_sku:
        lines.append('')
        lines.append('➕ 10 лидеров бизнеса, которых нет в контракте')
        lines.append('Позиция | Оборот бизнеса | Финрез до бизнеса | Почему важно')
        for item in missing_sku[:10]:
            if not isinstance(item, dict):
                continue
            sku = item.get('sku') or 'Позиция'
            lines.append(
                f'{sku} | {_fmt_int(item.get("business_revenue"))} грн | '
                f'{_fmt_signed_int(item.get("business_finrez_pre"))} грн | '
                f'{item.get("reason") or "лидер бизнеса отсутствует в контракте"}'
            )

    skew_lines = _assortment_skew_lines(assortment, categories)
    if skew_lines:
        lines.append('')
        lines.append('⚖ Ассортиментные перекосы')
        lines.extend(skew_lines)

    lines.append('')
    lines.append('🚀 План развития контракта')
    if actions:
        lines.append(f'1. Экономика: {_workspace_action_label(actions[0])}.')
    if missing_sku:
        lines.append('2. Ассортимент: собрать короткий пакет из 10 отсутствующих лидеров бизнеса, а не открывать весь длинный список.')
    if categories:
        lines.append(f'3. Категории: начать с «{categories[0].get("category") or "ключевой категории"}» и проверить, какие форматы и позиции дают следующий прирост.')
    if not actions and not missing_sku and not categories:
        lines.append('1. Начать с уточняющего вопроса ассистенту по цели работы с контрактом: экономика, ассортимент, переговоры или задачи.')

    if negotiation:
        lines.append('')
        lines.append('🤝 Переговорный пакет')
        goal = negotiation.get('goal')
        if goal:
            lines.append(f'Цель: {goal}.')
        priority_categories = negotiation.get('priority_categories') if isinstance(negotiation.get('priority_categories'), list) else []
        if priority_categories:
            lines.append('Категории для аргументации: ' + ', '.join(str(x) for x in priority_categories[:3]) + '.')
        sku_package = negotiation.get('sku_package') if isinstance(negotiation.get('sku_package'), list) else []
        if sku_package:
            lines.append('Пакет позиций для первой встречи: ' + ', '.join(str(x) for x in sku_package[:10]) + '.')

    lines.append('')
    lines.append('✅ Что делаем дальше?')
    if actions:
        lines.append(f'1. Подготовить переговоры по экономике контракта — {_workspace_action_label(actions[0])}.')
    else:
        lines.append('1. Подготовить переговоры по экономике контракта.')
    if categories:
        lines.append(f'2. Разобрать категорию «{categories[0].get("category") or "ключевую категорию"}» — посмотреть форматы, позиции и потенциал.')
    else:
        lines.append('2. Разобрать категории контракта.')
    if missing_sku:
        lines.append('3. Собрать пакет позиций для ввода — начать с 10 отсутствующих лидеров бизнеса.')
        lines.append('4. Показать лидеров SKU — отдельно разобрать роли текущих позиций.')
        lines.append('5. Показать ассортиментные перекосы — проверить концентрацию и пробелы.')
    else:
        lines.append('3. Посмотреть ассортиментные возможности.')
    lines.append('6. Создать задачи по контракту после выбора направления.')
    lines.append('7. Или задай вопрос ассистенту своими словами: «какие позиции предложить первыми?», «как говорить с байером?», «где быстрый эффект?»')

    return [line for line in lines if str(line or '').strip()]

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
        response['period_result_block'] = []
        response['kpi_block'] = []
        response['structure_block'] = []
        response['main_driver'] = ''
        response['summary_block'] = 'Витрина объекта. Полный список текущего уровня без аналитического сопровождения.'
        response['decision_block_render'] = []
        response['business_result_rating_block'] = []
        response['profit_loss_rating_block'] = []
        response['opportunity_rating_block'] = []
        response['priority_action_block'] = []
        response['object_reasons_block'] = []
        response['factor_change_block'] = []
        response['benchmark_diagnostic_block'] = []
        response['kpi_table'] = []
        response['factor_change_table'] = []
        response['benchmark_diagnostic_table'] = []
        response['opportunity_explanation_block'] = []
        response['anomaly_explanation_block'] = []
        response['product_layer_block'] = []
        response['product_insight_block'] = []
        response['product_tmc_decision_block'] = []
        response['sku_passport_block'] = []
        response['category_workspace_block'] = []
        response['business_opportunity_block'] = []
        response['recommendation_block'] = []
        response['narrative_block'] = []
        response['product_workspace_block'] = []
        response['business_context_block'] = []
        response['decision_workspace_block'] = []
        response['explanation_block'] = []
        response['next_step_block'] = []
        response['recommended_next_step_block'] = []
        response['diagnosis_block'] = []
    elif render_mode == 'reasons':
        # Reasons is a focused factor view. Do not leak the full workspace or
        # assistant explanation blocks into this screen.
        response['result_block'] = []
        response['period_result_block'] = []
        response['kpi_block'] = []
        response['structure_block'] = []
        response['main_driver'] = ''
        response['summary_block'] = 'Разбор причин текущего объекта.'
        response['decision_block_render'] = []
        response['business_result_rating_block'] = []
        response['profit_loss_rating_block'] = []
        response['opportunity_rating_block'] = []
        response['priority_action_block'] = []
        response['object_reasons_block'] = []
        response['factor_change_block'] = []
        response['benchmark_diagnostic_block'] = []
        response['kpi_table'] = []
        response['factor_change_table'] = []
        response['benchmark_diagnostic_table'] = []
        response['opportunity_explanation_block'] = []
        response['anomaly_explanation_block'] = []
        response['product_layer_block'] = []
        response['product_insight_block'] = []
        response['product_tmc_decision_block'] = []
        response['sku_passport_block'] = []
        response['business_context_block'] = []
        response['category_workspace_block'] = []
        response['business_opportunity_block'] = []
        response['recommendation_block'] = []
        response['narrative_block'] = []
        response['product_workspace_block'] = []
        response['decision_workspace_block'] = []
        response['explanation_block'] = []
        response['next_step_block'] = []
        response['recommended_next_step_block'] = []
        response['diagnosis_block'] = []
        response['navigation_block'] = response.get('navigation_block') or ['назад к объекту']
    else:
        response['kpi_block'] = _render_kpi_block(metrics)
        response['summary_block'] = _build_kpi_summary(response)
        response['result_block'] = _render_result_block(response)
        response['period_result_block'] = _render_period_result_block(response)
        # CHANGE-006: hide aggregate Benchmark Money from screen rendering.
        # Benchmark remains diagnostic through factors vs business, not a separate money rating.
        response['business_result_rating_block'] = []
        response['profit_loss_rating_block'] = _render_rating_lines(response.get('profit_loss_rating') or [], 'profit_delta_money')
        response['opportunity_rating_block'] = _render_rating_lines(response.get('opportunity_rating') or [], 'opportunity_money')
        if _is_product_layer_level(ctx_level_for_main_driver):
            response['structure_block'] = []
            response['main_driver'] = 'Продуктовая экономика'
            response['product_layer_block'] = _build_product_layer_block(response)
            response['product_insight_block'] = _build_product_insight_block(response)
            response['product_tmc_decision_block'] = _build_product_tmc_decision_block(response)
            response['sku_passport_block'] = _build_sku_passport_block(response)
            response['priority_action_block'] = _build_product_priority_action_block(response)
            reason_source = response.get('object_reasons') or []
            response['object_reasons_block'] = []
            response['factor_change_block'] = _render_factor_change_block(reason_source or [])
            response['benchmark_diagnostic_block'] = _render_benchmark_diagnostic_block(reason_source or [])
            response['reasons_block'] = []
            response['decision_block'] = []
        else:
            response['structure_block'] = _render_structure_block(structure)
            response['main_driver'] = _render_main_driver(structure)
            response['product_layer_block'] = []
            response['product_insight_block'] = []
            response['priority_action_block'] = _render_priority_action(response)
            reason_source = response.get('business_reasons') if ctx_level_for_main_driver == 'business' else response.get('object_reasons')
            response['object_reasons_block'] = _render_factor_change_block(reason_source or [])
            response['factor_change_block'] = _render_factor_change_block(reason_source or [])
            response['benchmark_diagnostic_block'] = [] if ctx_level_for_main_driver == 'business' else _render_benchmark_diagnostic_block(reason_source or [])

        # Stage 8.3: explicit table-ready data for Custom GPT.
        # This restores full factors/benchmark rendering without changing any calculations.
        table_reason_source = response.get('business_reasons') if ctx_level_for_main_driver == 'business' else response.get('object_reasons')
        response['kpi_table'] = _render_kpi_table_data(response)
        response['factor_change_table'] = _render_factor_change_table_data(table_reason_source or [])
        response['benchmark_diagnostic_table'] = [] if ctx_level_for_main_driver == 'business' else _render_benchmark_table_data(table_reason_source or [])

    ctx_level = str((response.get('context') or {}).get('level') or '').strip().lower()
    if render_mode == 'list_only':
        response['drain_block_render'] = _render_vitrina_block(response)
        response['drain_total'] = drain_total
        response['summary_block'] = 'Витрина объекта. Полный список текущего уровня без аналитического сопровождения.'
    elif ctx_level == 'sku':
        rendered_sku_drain = _render_drain_block(drain)
        if not rendered_sku_drain:
            response['drain_total'] = drain_total
        else:
            response['drain_total'] = drain_total
        response['drain_block_render'] = rendered_sku_drain
    else:
        response['drain_block_render'] = _render_drain_block(drain)
        response['drain_total'] = drain_total
    if render_mode == 'list_only':
        response['navigation_block'] = response.get('navigation_block') or ['назад — вернуться к объекту']
        response['decision_workspace_block'] = []
        response['diagnosis_block'] = []
        response['explanation_block'] = []
        response['next_step_block'] = []
        response['recommended_next_step_block'] = []
        response['opportunity_explanation_block'] = []
        response['anomaly_explanation_block'] = []
        response['decision_block_render'] = []
        response['reasons_block_render'] = []
        response['screen_order'] = ['summary_block', 'drain_block_render', 'navigation_block']
        return response

    if render_mode == 'reasons':
        response['navigation_block'] = response.get('navigation_block') or ['назад к объекту']
        response['decision_workspace_block'] = []
        response['diagnosis_block'] = []
        response['explanation_block'] = []
        response['next_step_block'] = []
        response['recommended_next_step_block'] = []
        response['opportunity_explanation_block'] = []
        response['anomaly_explanation_block'] = []
        response['decision_block_render'] = []
        response['drain_block_render'] = []
        response['drain_total'] = 0
        response['reasons_block_render'] = _render_reasons_block(response.get('reasons_block') or [], ctx_level) or response.get('reasons_block_render') or []
        response['screen_order'] = ['summary_block', 'reasons_block_render', 'navigation_block']
        return response

    response['navigation_block'] = _render_navigation_block(payload, navigation, drain)
    response['business_context_block'] = _render_business_context_block(response)
    response['category_workspace_block'] = _render_category_workspace_block(response)
    response['business_opportunity_block'] = _render_business_opportunity_block(response)
    response['recommendation_block'] = _render_recommendation_block(response)
    response['narrative_block'] = _render_narrative_block(response)
    response['product_workspace_block'] = _render_product_workspace_block(response)
    response['decision_workspace_block'] = _render_decision_workspace_block(response)
    response['diagnosis_block'] = _build_assistant_diagnosis_block(response)
    response['recommended_next_step_block'] = _build_recommended_next_step_block(response)
    response['opportunity_explanation_block'] = _build_opportunity_explanation_block(response)
    response['anomaly_explanation_block'] = _build_anomaly_explanation_block(response)
    response['screen_order'] = _stage7_screen_order(response)
    if _is_product_layer_level(ctx_level):
        response['decision_block_render'] = list(response.get('priority_action_block') or [])
        response['reasons_block_render'] = []
    else:
        response['decision_block_render'] = _render_decision_block(response)
        response['reasons_block_render'] = _render_reasons_block(response.get('reasons_block') or [], ctx_level)
    return response




def _render_kpi_table_data(response):
    """Machine-readable table data for Custom GPT rendering.

    Does not calculate new values; only exposes already normalized metrics in a
    stable table-friendly shape. Object screens intentionally exclude Финрез итог.
    """
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    order = ['Финрез до', 'Маржа', 'Оборот', 'Наценка']
    if level == 'business':
        order.append('Финрез итог')
    rows = []
    for name in order:
        item = _metric_by_name(response.get('metrics') or [], name)
        if not item:
            continue
        if name in {'Маржа', 'Наценка'}:
            rows.append({
                'name': name,
                'current': _fmt_percent_value(item.get('fact_percent')),
                'previous': _fmt_percent_value(item.get('pg_percent')),
                'delta': _fmt_pp_delta(item.get('delta_percent')),
            })
        else:
            rows.append({
                'name': name,
                'current': _fmt_int(item.get('fact_money')),
                'previous': _fmt_int(item.get('pg_money')),
                'delta': _fmt_signed_int(item.get('delta_money')),
            })
    return rows


def _render_factor_change_table_data(reasons):
    """Table-ready Effect vs Previous Year data for the render layer."""
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    rows = []
    for item in sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99)):
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        delta_p_raw = item.get('delta_vs_previous_percent', item.get('delta_vs_prev'))
        rows.append({
            'factor': name,
            'current': f'{_reason_current_percent(item)}%',
            'previous': _reason_previous_percent(item),
            'delta': 'нет корректной базы' if delta_p_raw is None else _fmt_pp_delta(_num(delta_p_raw)),
            'effect': _fmt_signed_int(item.get('effect_vs_previous_money', item.get('effect_money'))),
            'signal': str(item.get('signal') or '').strip() or 'норма',
        })
    return rows


def _render_benchmark_table_data(reasons):
    """Table-ready Effect vs Business data for the render layer."""
    order = {'Наценка': 0, 'Ретро': 1, 'Логистика': 2, 'Персонал': 3, 'Прочие': 4}
    rows = []
    for item in sorted([x for x in (reasons or []) if isinstance(x, dict)], key=lambda x: order.get(str(x.get('name') or '').strip(), 99)):
        name = str(item.get('name') or '').strip()
        if not name:
            continue
        delta_b = _num(item.get('delta_vs_business_percent', item.get('delta_percent')))
        rows.append({
            'factor': name,
            'object': f'{_reason_current_percent(item)}%',
            'business': f'{_fmt_percent(item.get("base_percent"))}%',
            'delta_to_business': _fmt_pp_delta(delta_b),
            'effect': _fmt_signed_int(item.get('effect_vs_business_money', item.get('effect_money'))),
        })
    return rows

def _sanitize_json_value(value):
    if isinstance(value, dict):
        return {str(k): _sanitize_json_value(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_json_value(v) for v in value]
    if isinstance(value, float):
        return value if math.isfinite(value) else 0.0
    return value


def _json_len(value) -> int:
    try:
        return len(json.dumps(_sanitize_json_value(value), ensure_ascii=False))
    except Exception:
        return 0


def _compact_text_list(value, *, limit: int = 12):
    if not isinstance(value, list):
        return []
    out = []
    for item in value[:limit]:
        if isinstance(item, (str, int, float)):
            out.append(item)
        elif isinstance(item, dict):
            # Keep only business-facing scalar keys; drop nested details.
            compact = {}
            for key in (
                'object_id', 'object_name', 'name', 'level', 'title', 'label',
                'value', 'fact_money', 'pg_money', 'delta_money', 'fact_percent',
                'pg_percent', 'delta_percent', 'effect_money', 'signal', 'comment',
            ):
                if item.get(key) is not None:
                    compact[key] = item.get(key)
            out.append(compact if compact else str(item)[:240])
    return out


def _enforce_public_response_budget(payload: dict) -> dict:
    """Last-resort response budget guard.

    Sprint W14.1: the guard may compact auxiliary transport fields, but it must
    never remove, truncate, rewrite or replace `workspace_markdown`. A full
    Workspace is a product contract, not a discretionary payload section.
    """
    if not isinstance(payload, dict):
        return payload

    initial_len = _json_len(payload)
    if initial_len <= VECTRA_PUBLIC_RESPONSE_BUDGET:
        return payload

    out = dict(payload)
    render_mode = str(out.get('render_mode') or '').strip().lower()
    has_canonical_workspace = isinstance(out.get('workspace_markdown'), str) and bool(out.get('workspace_markdown').strip())

    out['response_budget_guard'] = {
        'applied': True,
        'initial_json_chars': initial_len,
        'budget_chars': VECTRA_PUBLIC_RESPONSE_BUDGET,
        'workspace_markdown_preserved': has_canonical_workspace,
    }

    if has_canonical_workspace:
        # Remove only duplicate / auxiliary fields. The canonical markdown stays
        # byte-for-byte intact so GPT cannot fall back to a compact legacy screen.
        for key in (
            'workspace_primary_block', 'all_block',
            'result_block', 'period_result_block', 'kpi_block', 'kpi_table',
            'structure_block', 'drain_block_render', 'explanation_block',
            'next_step_block', 'diagnosis_block', 'recommended_next_step_block',
            'opportunity_explanation_block', 'anomaly_explanation_block',
            'reasons_block', 'reasons_block_render', 'decision_block',
            'decision_block_render', 'business_result_rating_block',
            'profit_loss_rating_block', 'opportunity_rating_block',
            'priority_action_block', 'object_reasons_block', 'factor_change_block',
            'factor_change_table', 'benchmark_diagnostic_block',
            'benchmark_diagnostic_table', 'product_layer_block',
            'product_insight_block', 'product_tmc_decision_block',
            'business_workspace_block', 'contract_workspace_block',
            'management_workspace_block', 'category_workspace_block',
            'product_workspace_block', 'sku_passport_block',
            'decision_workspace_block', 'business_context_block',
            'business_opportunity_block', 'recommendation_block', 'narrative_block',
            'metrics', 'structure', 'decision_workspace', 'sku_passport',
            'business_context', 'category_workspace', 'business_opportunity',
            'recommendation_engine', 'narrative_engine', 'product_workspace',
            'management_intelligence', 'management_workspace', 'management_passport',
        ):
            out.pop(key, None)
        out['screen_order'] = ['workspace_markdown']
        out['response_budget_guard']['final_json_chars'] = _json_len(out)
        return out

    if render_mode == 'list_only':
        out['all_block'] = _compact_public_all_block(out.get('all_block', []))
        out['drain_block_render'] = _compact_text_list(out.get('drain_block_render'), limit=80)
        out['screen_order'] = ['summary_block', 'drain_block_render', 'navigation_block']
    elif render_mode == 'reasons':
        out['reasons_block_render'] = _compact_text_list(out.get('reasons_block_render'), limit=12)
        out['reasons_block'] = _compact_text_list(out.get('reasons_block'), limit=12)
        out['screen_order'] = ['summary_block', 'reasons_block_render', 'navigation_block']
    elif render_mode == 'kpi_only':
        out['result_block'] = _compact_text_list(out.get('result_block'), limit=8)
        out['kpi_block'] = _compact_text_list(out.get('kpi_block'), limit=10)
        out['kpi_table'] = _compact_text_list(out.get('kpi_table'), limit=10)
        out['screen_order'] = ['summary_block', 'result_block', 'kpi_block', 'kpi_table', 'navigation_block']
    else:
        for key in (
            'all_block', 'product_workspace_block', 'product_workspace',
            'product_insight_block', 'product_layer_block', 'sku_passport_block',
            'sku_passport', 'category_workspace_block', 'category_workspace',
            'business_opportunity_block', 'business_opportunity',
            'recommendation_block', 'recommendation_engine', 'narrative_block',
            'narrative_engine', 'decision_workspace_block', 'decision_workspace',
            'management_workspace_block', 'management_workspace',
            'management_intelligence', 'management_passport',
        ):
            out[key] = [] if key.endswith('_block') or key == 'all_block' else {}
        out['drain_block_render'] = _compact_text_list(out.get('drain_block_render'), limit=7)
        out['diagnosis_block'] = _compact_text_list(out.get('diagnosis_block'), limit=6)
        out['explanation_block'] = _compact_text_list(out.get('explanation_block'), limit=6)
        out['next_step_block'] = _compact_text_list(out.get('next_step_block'), limit=6)
        out['recommended_next_step_block'] = _compact_text_list(out.get('recommended_next_step_block'), limit=6)

    if _json_len(out) > VECTRA_PUBLIC_RESPONSE_HARD_BUDGET:
        out = {
            'status': out.get('status', 'ok'),
            'reason': out.get('reason'),
            'context': out.get('context'),
            'path': out.get('path', []),
            'render_mode': render_mode or 'compact',
            'summary_block': out.get('summary_block', 'Ответ сокращён из-за ограничения объёма.'),
            'result_block': _compact_text_list(out.get('result_block'), limit=5),
            'kpi_block': _compact_text_list(out.get('kpi_block'), limit=5),
            'drain_block_render': _compact_text_list(out.get('drain_block_render'), limit=7),
            'navigation_block': out.get('navigation_block') or ['причины', 'все', 'назад'],
            'active_workspace_state': out.get('active_workspace_state', {}),
            'workspace_action_map': out.get('workspace_action_map', []),
            'workspace_runtime_contract': out.get('workspace_runtime_contract', {}),
            'workspace_render_instruction': out.get('workspace_render_instruction', ''),
            'screen_order': ['summary_block', 'result_block', 'kpi_block', 'drain_block_render', 'navigation_block'],
            'response_budget_guard': {
                'applied': True,
                'hard_fallback': True,
                'initial_json_chars': initial_len,
                'budget_chars': VECTRA_PUBLIC_RESPONSE_HARD_BUDGET,
            },
        }
    else:
        out['response_budget_guard']['final_json_chars'] = _json_len(out)
    return out

def json_response(payload):
    return JSONResponse(content=_sanitize_json_value(payload), media_type='application/json; charset=utf-8')




def _hydrate_runtime_context_from_request(session_id: str, request: VectraQueryRequest) -> None:
    """Hydrate server-side session from explicit Custom GPT runtime fields.

    Custom GPT Actions do not send hidden dialogue history automatically.  W15.1
    therefore accepts the last public active_workspace_state/workspace_action_map
    as explicit request fields and uses them only to restore navigation context.
    """
    try:
        active_state = getattr(request, 'active_workspace_state', None)
        runtime_context = getattr(request, 'runtime_context', None)
        action_map = getattr(request, 'workspace_action_map', None)
        active_research_state = getattr(request, 'active_research_state', None)
        if not isinstance(active_research_state, dict) and isinstance(runtime_context, dict):
            active_research_state = runtime_context.get('active_research_state') or runtime_context.get('research_flow_status')
        research_path = getattr(request, 'research_path', None)
        current_step = getattr(request, 'current_step', None)
        payload = {}
        if isinstance(active_state, dict) and active_state:
            state = dict(active_state)
            if isinstance(action_map, list) and action_map and not state.get('action_map'):
                state['action_map'] = action_map
            if isinstance(active_research_state, dict) and active_research_state:
                # DEV-0004: restore Research Flow state explicitly. The action
                # boundary cannot rely on hidden GPT chat state.
                state['research_flow'] = active_research_state
            ctx = {
                'level': state.get('workspace_level'),
                'object_name': state.get('object_name'),
                'period': state.get('period'),
                'parent_object': None,
            }
            screen = {
                'status': 'ok',
                'render_mode': state.get('render_mode') or 'runtime_context_snapshot',
                'context': ctx,
                'path': state.get('path') if isinstance(state.get('path'), list) else [],
                'filter': state.get('filter') if isinstance(state.get('filter'), dict) else {},
                'workspace_markdown': runtime_context.get('workspace_markdown') if isinstance(runtime_context, dict) and isinstance(runtime_context.get('workspace_markdown'), str) else '',
                'active_workspace_state': state,
                'workspace_action_map': state.get('action_map') if isinstance(state.get('action_map'), list) else [],
            }
            if isinstance(active_research_state, dict) and active_research_state:
                screen['active_research_state'] = active_research_state
                screen['research_flow_status'] = active_research_state
                screen['research_path'] = research_path if isinstance(research_path, list) else active_research_state.get('research_path', [])
                screen['current_step'] = current_step or active_research_state.get('current_step')
            payload.update({
                'active_workspace_state': state,
                'active_research_state': active_research_state if isinstance(active_research_state, dict) else None,
                'research_path': research_path if isinstance(research_path, list) else None,
                'current_step': current_step,
                'scope_level': state.get('workspace_level'),
                'scope_object_name': state.get('object_name'),
                'period_current': state.get('period'),
                'filter': state.get('filter') if isinstance(state.get('filter'), dict) else {},
            })
            # DEV-0004: when Custom GPT calls vectraQuery with explicit Product
            # Team research runtime state, hydrate both current_screen and
            # last_payload. Numeric/local research commands must not fall back
            # into ordinary free dialogue just because the server has no hidden
            # chat history for the current Action call.
            if str(state.get('workspace_level') or '').strip().lower() == 'product_team_research':
                payload['current_screen'] = screen
                payload['last_payload'] = screen
            else:
                payload.setdefault('last_payload', screen)
        if payload:
            update_session(session_id, {k: v for k, v in payload.items() if v not in (None, '')})
    except Exception:
        logger.exception('runtime_context_hydration_failed session_id=%s', session_id)

def _stable_session_id(request: VectraQueryRequest) -> str:
    raw = (getattr(request, 'session_id', None) or '').strip()
    return raw or 'default'


def _is_product_team_research_request(message: str) -> bool:
    text = (message or '').strip().lower().replace('ё', 'е')
    if not text:
        return False
    has_research = any(token in text for token in ('исследуй', 'исследовать', 'исследование', 'проверить продукт', 'полный цикл'))
    has_object = any(token in text for token in ('vectra', 'вектра', 'product team assistant', 'ассистент', 'workspace', 'релиз', 'release', 'продукт'))
    return has_research and has_object


def _research_object_name(message: str) -> str:
    text = (message or '').strip().lower()
    if 'product team assistant' in text or 'assistant' in text or 'ассистент' in text:
        return 'Product Team Assistant'
    if 'workspace' in text:
        return 'Workspace'
    if 'релиз' in text or 'release' in text:
        return 'Release'
    return 'VECTRA'


def _build_product_team_research_workspace(message: str, session_id: str) -> dict:
    """Start a Product Team autonomous user session.

    DEV-0006: the Product Owner command (for example "Исследуй VECTRA") is
    not itself a user request to VECTRA. It is a command to Product Team
    Assistant. Runtime must therefore create an internal virtual user session
    and execute Assistant-generated user messages inside VECTRA.
    """
    obj = _research_object_name(message)
    return _start_autonomous_user_session(obj, session_id=session_id, owner_command=message)


def _is_product_team_research_workspace(screen: dict) -> bool:
    if not isinstance(screen, dict):
        return False
    render_mode = str(screen.get('render_mode') or '').strip().lower()
    ctx = screen.get('context') if isinstance(screen.get('context'), dict) else {}
    return render_mode == 'product_team_research_workspace' and str(ctx.get('level') or '').strip().lower() == 'product_team_research'


def _current_product_team_research_object(session_id: str) -> str:
    try:
        current = get_session(session_id).get('current_screen') or get_session(session_id).get('last_payload') or {}
    except Exception:
        current = {}
    state = current.get('active_workspace_state') if isinstance(current.get('active_workspace_state'), dict) else {}
    ctx = current.get('context') if isinstance(current.get('context'), dict) else {}
    return state.get('object_name') or ctx.get('object_name') or 'VECTRA'


def _is_research_continue_request(message: str, session_id: str) -> bool:
    """Detect explicit continuation of the Product Team research route.

    Before DEV-0003 such requests fell into free dialogue and produced
    "Работаю в контексте открытого Workspace...".  This detector keeps route
    execution inside the Product Team research engine.
    """
    text = (message or '').strip().lower().replace('ё', 'е')
    if not text:
        return False
    try:
        current = get_session(session_id).get('current_screen') or get_session(session_id).get('last_payload') or {}
    except Exception:
        current = {}
    if not _is_product_team_research_workspace(current):
        return False
    if any(token in text for token in ('продолжить исследование', 'продолжить маршрут', 'текущему шагу', 'следующий этап исследования', 'выполнить текущий шаг')):
        return True
    # Numeric command can be used only when the visible action #1 is the
    # continuation action from the current research workspace.
    if re.fullmatch(r'1', text):
        state = current.get('active_workspace_state') if isinstance(current.get('active_workspace_state'), dict) else {}
        actions = state.get('action_map') if isinstance(state.get('action_map'), list) else current.get('workspace_action_map')
        if isinstance(actions, list):
            for action in actions:
                if isinstance(action, dict) and int(action.get('number') or 0) == 1:
                    label = str(action.get('label') or '').lower().replace('ё', 'е')
                    return 'продолжить' in label and 'исследован' in label
    return False


def _autonomous_user_session_plan(root_obj: str) -> list:
    """Build deterministic virtual user messages for Autonomous User Session.

    The Product Owner command is never reused as a user message.  These
    messages are generated by Product Team Assistant and executed as if a real
    user was working inside VECTRA.
    """
    root = (root_obj or 'VECTRA').strip() or 'VECTRA'
    if root.lower() in {'vectra', 'вектра'}:
        return [
            {'step_id': 'start_day', 'role': 'Commercial Director', 'user_message': 'Начать Анализ', 'goal': 'Открыть стартовую точку пользовательской работы.'},
            {'step_id': 'business_workspace', 'role': 'Commercial Director', 'user_message': 'Бизнес 2026-02', 'goal': 'Проверить открытие Business Workspace и рабочий контекст.'},
            {'step_id': 'business_vitrine', 'role': 'Commercial Director', 'user_message': 'все', 'goal': 'Проверить локальную витрину и действие все.'},
            {'step_id': 'business_reasons', 'role': 'Commercial Director', 'user_message': 'причины', 'goal': 'Проверить локальный разбор причин без потери контекста.'},
            {'step_id': 'discovery', 'role': 'Product Explorer', 'user_message': 'Покажи лучшие SKU', 'goal': 'Проверить Discovery-запрос и переход к следующему действию.'},
            {'step_id': 'journal_status', 'role': 'Product Owner', 'user_message': 'экспорт журнала', 'goal': 'Проверить состояние Development Journal как часть Product Owner Report.'},
        ]
    if root.lower() in {'product team assistant', 'assistant', 'ассистент'}:
        return [
            {'step_id': 'assistant_research_start', 'role': 'Product Owner', 'user_message': 'Открой рабочий контекст Product Team Assistant', 'goal': 'Проверить способность Assistant открыть собственный продуктовый контур без передачи команды Product Owner.'},
            {'step_id': 'role_engine', 'role': 'Product Explorer', 'user_message': 'Проверь Role Engine', 'goal': 'Проверить выбор и переключение ролей.'},
            {'step_id': 'product_owner_report', 'role': 'Product Owner', 'user_message': 'Сформируй Product Owner Report', 'goal': 'Проверить итоговый отчёт исследования.'},
        ]
    return [
        {'step_id': 'object_research_start', 'role': 'Product Explorer', 'user_message': f'Покажи {root}', 'goal': f'Открыть объект исследования {root}.'},
        {'step_id': 'object_reasons', 'role': 'Product Explorer', 'user_message': 'причины', 'goal': 'Проверить причины и локальный контекст.'},
    ]


def _extract_autonomous_result_summary(payload: dict) -> dict:
    if not isinstance(payload, dict):
        return {'status': 'error', 'reason': 'non_dict_response'}
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    workspace_markdown = payload.get('workspace_markdown') if isinstance(payload.get('workspace_markdown'), str) else ''
    return {
        'status': payload.get('status', 'ok'),
        'reason': payload.get('reason'),
        'render_mode': payload.get('render_mode'),
        'context_level': ctx.get('level'),
        'context_object': ctx.get('object_name'),
        'has_workspace_markdown': bool(workspace_markdown.strip()),
        'workspace_markdown_length': len(workspace_markdown),
        'has_active_workspace_state': isinstance(payload.get('active_workspace_state'), dict) and bool(payload.get('active_workspace_state')),
        'action_count': len(payload.get('workspace_action_map') or []) if isinstance(payload.get('workspace_action_map'), list) else 0,
        'error_code': payload.get('error_code'),
    }


def _execute_autonomous_user_message(step: dict, session_id: str) -> dict:
    """Execute one Assistant-generated user message against VECTRA Runtime."""
    user_message = str(step.get('user_message') or '').strip()
    virtual_session_id = f'{session_id}:autonomous_user_session'
    try:
        raw = orchestrate_vectra_query(user_message, session_id=virtual_session_id)
        prepared = _prepare_vectra_query_payload(raw)
        rendered = apply_runtime_contract(prepared)
        summary = _extract_autonomous_result_summary(rendered)
        return {
            'step_id': step.get('step_id'),
            'role': step.get('role'),
            'goal': step.get('goal'),
            'user_message': user_message,
            'status': 'executed',
            'runtime_summary': summary,
        }
    except Exception as exc:
        logger.exception('autonomous_user_message_execution_failed session_id=%s message=%r', virtual_session_id, user_message)
        return {
            'step_id': step.get('step_id'),
            'role': step.get('role'),
            'goal': step.get('goal'),
            'user_message': user_message,
            'status': 'execution_error',
            'runtime_summary': {'status': 'error', 'reason': str(exc), 'has_workspace_markdown': False},
        }


def _build_autonomous_user_session_report(root_obj: str, session_state: dict, user_history: list) -> list:
    executed = [item for item in user_history if isinstance(item, dict)]
    confirmed = []
    limitations = []
    for item in executed:
        summary = item.get('runtime_summary') if isinstance(item.get('runtime_summary'), dict) else {}
        if summary.get('has_workspace_markdown'):
            confirmed.append(f"• {item.get('step_id')}: VECTRA вернула рабочий ответ для запроса Assistant: {item.get('user_message')}")
        else:
            reason = summary.get('error_code') or summary.get('reason') or 'workspace_not_confirmed'
            limitations.append(f"• {item.get('step_id')}: не подтверждён рабочий Workspace для запроса Assistant: {item.get('user_message')} ({reason})")
    if not confirmed:
        confirmed.append('• Автономная пользовательская сессия создана; пользовательские запросы сформированы Assistant, а не Product Owner.')
    if not limitations:
        limitations.append('• Критичных ограничений автономной пользовательской сессии в доступной локальной проверке не подтверждено.')
    return [
        f'📍 Autonomous User Session — {root_obj}',
        '',
        '🎯 Цель',
        'Проверить VECTRA как реальный пользователь без пошагового участия Product Owner.',
        '',
        '✅ Что изменилось',
        'Product Owner больше не является источником пользовательских сообщений внутри исследования.',
        'Assistant создаёт виртуальную пользовательскую сессию, выбирает роль, формирует пользовательские запросы и выполняет их через VECTRA Runtime.',
        '',
        '👤 Выбранная пользовательская роль',
        str(session_state.get('user_role') or 'Product Explorer'),
        '',
        '🧭 Сценарий автономной сессии',
        *[f"{idx+1}. [{item.get('role')}] {item.get('user_message')} — {item.get('goal')}" for idx, item in enumerate(executed)],
        '',
        '✅ Подтверждённые результаты',
        *confirmed,
        '',
        '⚠ Ограничения',
        *limitations,
        '',
        '💡 Product Opportunities',
        '• Расширить библиотеку виртуальных пользовательских ролей: Commercial Director, KAM, Category Manager, Release Manager.',
        '• Добавить настройку глубины автономной сессии для короткого, стандартного и полного исследования.',
        '• Связать подтверждённые ограничения автономной сессии с автоматической подготовкой Engineering Tasks после проверки Laboratory.',
        '',
        '📄 Product Owner Report',
        f'Цель исследования: автономно исследовать {root_obj}.',
        'Что исследовано: виртуальная пользовательская сессия, генерация пользовательских запросов Assistant, выполнение через VECTRA Runtime, сохранение состояния.',
        'Что подтверждено: Product Owner command отделён от user messages; Assistant формирует собственные пользовательские запросы.',
        'Ограничения: полнота бизнес-проверки зависит от production-среды и доступности бизнес-данных.',
        'Следующий шаг: выполнить Product Acceptance в Custom GPT после деплоя DEV-0006.',
        '',
        '## Что делаем дальше?',
        '1. Передать результаты в Laboratory',
        '2. Проверить Development Journal',
        '3. Запустить автономную сессию для роли KAM',
        '4. Сформировать Engineering Tasks по подтверждённым ограничениям',
    ]


def _start_autonomous_user_session(root_obj: str, session_id: str, owner_command: str = '') -> dict:
    """StartUserSession + ExecuteUserMessage + FinishUserSession.

    DEV-0006: this function is the bridge between Product Owner dialogue and
    VECTRA user-mode execution.  owner_command is stored only as the trigger;
    it is never sent to VECTRA as a user message.
    """
    root = (root_obj or 'VECTRA').strip() or 'VECTRA'
    plan = _autonomous_user_session_plan(root)
    user_role = plan[0].get('role') if plan else 'Product Explorer'
    user_history = []
    for step in plan:
        user_history.append(_execute_autonomous_user_message(step, session_id=session_id))
    completed_count = len([item for item in user_history if item.get('status') == 'executed'])
    blocking = [item for item in user_history if item.get('status') == 'execution_error']
    session_state = {
        'version': 'DEV_0006_AUTONOMOUS_USER_SESSION',
        'mode': 'autonomous_user_session',
        'user_session_id': f'{session_id}:autonomous_user_session',
        'session_status': 'completed_with_limitations' if blocking else 'completed',
        'owner_command': owner_command,
        'owner_command_forwarded_to_vectra': False,
        'user_role': user_role,
        'user_goal': f'Autonomously research {root} as a real VECTRA user.',
        'user_context': {'research_object': root, 'source': 'Product Team Assistant'},
        'current_user_request': user_history[-1].get('user_message') if user_history else '',
        'user_history': user_history,
        'last_runtime_response': user_history[-1].get('runtime_summary') if user_history else {},
        'research_progress': {
            'completed_user_messages': completed_count,
            'total_user_messages': len(plan),
            'blocking_errors': len(blocking),
        },
    }
    lines = _build_autonomous_user_session_report(root, session_state, user_history)
    research_state = {
        'version': 'DEV_0006_AUTONOMOUS_USER_SESSION',
        'status': session_state['session_status'],
        'research_goal': root,
        'current_object': root,
        'current_step': 'autonomous_user_session_completed',
        'next_step': 'product_owner_report_review',
        'research_path': ['start_user_session', 'select_user_role', 'execute_user_messages', 'analyze_results', 'product_opportunities', 'product_owner_report'],
        'completion_reason': session_state['session_status'],
        'requires_product_owner_decision': False,
        'autonomous_user_session': session_state,
    }
    state = {
        'state_version': 'W15_ACTIVE_WORKSPACE_STATE_V3_DEV_0006',
        'source_of_truth': 'autonomous_user_session',
        'workspace_level': 'product_team_research',
        'object_name': root,
        'period': None,
        'path': ['Product Team Assistant', 'Autonomous User Session', root],
        'filter': {'research_object': root, 'mode': 'autonomous_user_session'},
        'render_mode': 'product_team_research_workspace',
        'research_flow': research_state,
        'autonomous_user_session': session_state,
    }
    return {
        'status': 'ok',
        'render_mode': 'product_team_research_workspace',
        'context': {'level': 'product_team_research', 'object_name': root, 'period': None, 'parent_object': None},
        'path': state['path'],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'screen_order': ['workspace_markdown'],
        'workspace_render_instruction': 'Показать пользователю workspace_markdown полностью и без изменений.',
        'active_workspace_state': state,
        'workspace_action_map': [],
        'research_flow_status': research_state,
        'active_research_state': research_state,
        'research_path': research_state['research_path'],
        'current_step': research_state['current_step'],
        'next_step': research_state['next_step'],
        'autonomous_user_session': session_state,
        'user_session_id': session_state['user_session_id'],
        'user_role': session_state['user_role'],
        'user_goal': session_state['user_goal'],
        'user_history': session_state['user_history'],
        'session_status': session_state['session_status'],
    }


def _research_continuation_plan(root_obj: str) -> list:
    """Return deterministic objects for a continuous Product Team research loop.

    DEV-0005: the research route is not a single finished report.  It is a
    continuous loop over related research objects.  Runtime must keep pending
    and completed objects so a follow-up Action call cannot reopen the same
    completed stage by default.
    """
    root = (root_obj or 'VECTRA').strip() or 'VECTRA'
    if root.lower() in {'vectra', 'вектра'}:
        return [
            'VECTRA',
            'Workspace',
            'Navigation',
            'Discovery',
            'Development Journal',
            'Product Acceptance',
            'Product Opportunities',
        ]
    if root.lower() in {'product team assistant', 'assistant', 'ассистент'}:
        return [
            'Product Team Assistant',
            'Role Engine',
            'Research Flow',
            'Product Owner Report',
            'Development Journal',
            'Product Opportunities',
        ]
    return [root]


def _restore_research_state(session_id: str) -> dict:
    try:
        current = get_session(session_id).get('current_screen') or get_session(session_id).get('last_payload') or {}
    except Exception:
        current = {}
    if not isinstance(current, dict):
        return {}
    state = current.get('active_research_state') if isinstance(current.get('active_research_state'), dict) else {}
    if not state:
        state = current.get('research_flow_status') if isinstance(current.get('research_flow_status'), dict) else {}
    active_state = current.get('active_workspace_state') if isinstance(current.get('active_workspace_state'), dict) else {}
    if not state and isinstance(active_state.get('research_flow'), dict):
        state = active_state.get('research_flow')
    return state if isinstance(state, dict) else {}


def _advance_research_state(root_obj: str, session_id: str, trigger: str) -> dict:
    previous = _restore_research_state(session_id)
    requested_root = (root_obj or previous.get('research_goal') or previous.get('object_name') or 'VECTRA').strip() or 'VECTRA'
    existing_goal = str(previous.get('research_goal') or '').strip()
    # Continue an existing research goal unless the user explicitly starts a new
    # initial request for another object.
    if previous and trigger not in {'initial_research_request'}:
        research_goal = existing_goal or requested_root
    else:
        research_goal = requested_root
    if trigger == 'initial_research_request':
        # A new explicit research command starts a fresh route even when the
        # server-side session fallback points to the latest active session from
        # another Custom GPT Action call.
        plan = _research_continuation_plan(research_goal)
        completed = []
        pending = list(plan)
    else:
        plan = previous.get('research_plan') if isinstance(previous.get('research_plan'), list) else _research_continuation_plan(research_goal)
        completed = previous.get('completed_objects') if isinstance(previous.get('completed_objects'), list) else []
        completed = [str(x) for x in completed if str(x).strip()]
        pending = previous.get('pending_objects') if isinstance(previous.get('pending_objects'), list) else []
        pending = [str(x) for x in pending if str(x).strip()]
        if not pending:
            pending = [item for item in plan if item not in completed]
    if not pending:
        current_object = previous.get('current_object') or previous.get('object_name') or research_goal
        status = 'completed'
        completion_reason = 'all_research_objects_completed'
        next_object = None
    else:
        current_object = pending.pop(0)
        if current_object not in completed:
            completed.append(current_object)
        next_object = pending[0] if pending else None
        status = 'in_progress' if next_object else 'completed'
        completion_reason = 'next_object_available' if next_object else 'all_research_objects_completed'
    return {
        'version': 'DEV_0005_CONTINUOUS_RESEARCH_LOOP',
        'trigger': trigger,
        'status': status,
        'research_goal': research_goal,
        'object_name': current_object,
        'current_object': current_object,
        'next_object': next_object,
        'completed_objects': completed,
        'pending_objects': pending,
        'research_plan': plan,
        'research_progress': {
            'completed_count': len(completed),
            'total_count': len(plan),
            'remaining_count': len(pending),
        },
        'current_step': 'object_research_completed',
        'next_step': 'continue_with_next_object' if next_object else 'final_product_owner_report',
        'research_path': [
            'object_model', 'source_review', 'role_selection', 'scenario_review',
            'workspace_navigation_context_review', 'development_journal_review',
            'product_opportunities', 'product_owner_report', 'continuation_planning',
        ],
        'completed_steps': [
            'object_model', 'source_review', 'role_selection', 'scenario_review',
            'workspace_navigation_context_review', 'development_journal_review',
            'product_opportunities', 'product_owner_report', 'continuation_planning',
        ],
        'completion_reason': completion_reason,
        'requires_product_owner_decision': False,
        'runtime_rule': 'After each research object, choose the next pending object before returning to ordinary dialogue.',
    }


def _build_research_object_lines(obj: str, research_state: dict, has_workspace: bool) -> list:
    next_obj = research_state.get('next_object')
    status_line = 'Исследование продолжается автоматически: следующий объект уже определён.' if next_obj else 'Маршрут исследования завершён по доступным объектам.'
    limitation_lines = []
    if not has_workspace:
        limitation_lines.append('Реальные пользовательские Workspace в текущем контексте не открыты; исследование выполняется по доступному Runtime-контексту и Knowledge.')
    if obj in {'VECTRA', 'Product Team Assistant', 'Workspace', 'Release', 'Product Acceptance'}:
        limitation_lines.append('Полная проверка фактических бизнес-данных зависит от production-среды и доступности источника данных.')
    lines = [
        f'📍 Автономное исследование — {obj}',
        '',
        '🎯 Цель исследования',
        f'Исследовать объект: {obj}.',
        'Определить подтверждённые результаты, ограничения, Product Opportunities и следующий объект исследования.',
        '',
        '✅ Статус цикла',
        status_line,
        f"Прогресс: {research_state.get('research_progress', {}).get('completed_count', 0)} из {research_state.get('research_progress', {}).get('total_count', 0)} объектов.",
        '',
        '🧩 1. Модель объекта',
        f'Объект исследования: {obj}.',
        'Границы исследования: назначение объекта, пользовательская ценность, сценарии, навигация, контекст, ограничения и возможности развития.',
        'Исследование проводится по объекту; источники информации используются только для подтверждения выводов.',
        '',
        '🔎 2. Использованные источники информации',
        '• Knowledge Base и стандарты Product Team Assistant.',
        '• Runtime-состояние текущей сессии.',
        '• Текущий Workspace, если он открыт в сессии.',
        '• Development Journal как источник инженерного состояния, если доступен через продуктовый контур.',
        '',
        '👥 3. Роли исследования',
        '• Product Owner — оценка результата и следующего решения.',
        '• Product Explorer — поиск ограничений и возможностей развития.',
        '• Release Manager — проверка влияния изменений и регрессий.',
        '• Laboratory — подготовка материала для архитектурного анализа.',
        '',
        '🧭 4. Проверенные направления',
        '• Наличие рабочего Workspace и сохранение контекста.',
        '• Возможность продолжать исследование без ручного выбора каждого шага.',
        '• Локальная навигация и действие продолжения маршрута.',
        '• Возможность сформировать Product Owner Report по текущему объекту.',
        '• Возможность выделить ограничения и Product Opportunities.',
        '',
        '✅ 5. Подтверждённые результаты',
        '• Research Flow хранит прогресс исследования.',
        '• Завершённый объект добавляется в completed_objects.',
        '• Следующий объект определяется до возврата ответа Product Owner.',
        '• active_research_state содержит current_object, next_object, pending_objects и completion_reason.',
        '',
        '⚠ 6. Ограничения исследования',
    ]
    if limitation_lines:
        lines.extend([f'• {item}' for item in limitation_lines])
    else:
        lines.append('• Критичных ограничений в доступном Runtime-контексте не подтверждено.')
    lines.extend([
        '',
        '💡 7. Product Opportunities',
        '• Расширить автономный цикл фактическими сценариями Product Acceptance в production-среде.',
        '• Накапливать историю исследовательских запусков для сравнения зрелости продукта между релизами.',
        '• Связать подтверждённые ограничения с автоматическим созданием инженерных задач после проверки Laboratory.',
        '',
        '📄 8. Product Owner Report по текущему объекту',
        f'Цель: исследовать {obj}.',
        'Что исследовано: объект, источники информации, роли, навигация, контекст, ограничения и возможности развития.',
        'Что подтверждено: текущий объект исследован в пределах доступной информации, прогресс маршрута сохранён.',
        'Ограничения: полнота проверки реальных бизнес Workspace зависит от доступности production-данных и открытого рабочего контекста.',
        'Product Opportunities: расширить автономный Research Flow фактическими сценариями по ролям и историей исследовательских запусков.',
        '',
    ])
    if next_obj:
        lines.extend([
            '➡ Автоматическое продолжение',
            f'Следующий объект исследования: {next_obj}.',
            'Product Owner не должен запускать следующий этап вручную; Runtime State уже содержит следующий объект и ожидает продолжения исследовательского цикла.',
            '',
            '## Что делаем дальше?',
            f'1. Продолжить исследование: {next_obj}',
            '2. Проверить Development Journal',
            '3. Передать текущие результаты в Laboratory',
            '4. Сформировать инженерные задачи по подтверждённым ограничениям',
        ])
    else:
        lines.extend([
            '➡ Следующий шаг',
            'Передать итоговый результат в Laboratory для оценки следующего этапа развития продукта.',
            '',
            '## Что делаем дальше?',
            '1. Передать результаты в Laboratory',
            '2. Проверить Development Journal',
            '3. Повторить автономное исследование с новым объектом',
            '4. Сформировать инженерные задачи по подтверждённым ограничениям',
        ])
    return lines


def _build_product_team_autonomous_research_workspace(obj: str, session_id: str, *, trigger: str = 'continue') -> dict:
    """Execute one object in a continuous Product Team research loop.

    DEV-0005: completing a Product Owner Report for one object must not make
    the next request reopen the same completed stage.  The response now carries
    a continuation state with completed_objects, pending_objects and next_object.
    """
    try:
        session = get_session(session_id)
    except Exception:
        session = {}
    current = session.get('current_screen') or session.get('last_payload') or {}
    has_workspace = bool(current.get('workspace_markdown')) if isinstance(current, dict) else False
    research_state = _advance_research_state(obj, session_id, trigger)
    current_object = research_state.get('current_object') or obj or 'VECTRA'
    lines = _build_research_object_lines(current_object, research_state, has_workspace)
    state = {
        'state_version': 'W15_ACTIVE_WORKSPACE_STATE_V3_DEV_0005',
        'source_of_truth': 'last_displayed_workspace',
        'workspace_level': 'product_team_research',
        'object_name': current_object,
        'period': None,
        'path': ['Product Team Assistant', 'Research', research_state.get('research_goal') or current_object, current_object],
        'filter': {'research_object': current_object, 'research_goal': research_state.get('research_goal')},
        'render_mode': 'product_team_research_workspace',
        'research_flow': research_state,
    }
    return {
        'status': 'ok',
        'render_mode': 'product_team_research_workspace',
        'context': {'level': 'product_team_research', 'object_name': current_object, 'period': None, 'parent_object': research_state.get('research_goal')},
        'path': state['path'],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'screen_order': ['workspace_markdown'],
        'workspace_render_instruction': 'Показать пользователю workspace_markdown полностью и без изменений.',
        'active_workspace_state': state,
        'workspace_action_map': [],
        'research_flow_status': research_state,
        'active_research_state': research_state,
        'research_path': research_state['research_path'],
        'current_step': research_state['current_step'],
        'next_step': research_state['next_step'],
        'current_object': research_state.get('current_object'),
        'next_object': research_state.get('next_object'),
        'pending_objects': research_state.get('pending_objects', []),
        'completed_objects': research_state.get('completed_objects', []),
        'completion_reason': research_state.get('completion_reason'),
    }

def _is_numeric_research_action(message: str, session_id: str) -> bool:
    text = str(message or '').strip()
    if not re.fullmatch(r'\d{1,2}', text):
        return False
    try:
        session = get_session(session_id)
        current = session.get('current_screen') or session.get('last_payload') or {}
        return str(current.get('render_mode') or '').strip().lower() == 'product_team_research_workspace'
    except Exception:
        return False


def _build_product_team_research_action_workspace(message: str, session_id: str) -> dict:
    number = int(str(message or '0').strip() or 0)
    session = get_session(session_id)
    current = session.get('current_screen') or session.get('last_payload') or {}
    state = current.get('active_workspace_state') if isinstance(current.get('active_workspace_state'), dict) else {}
    obj = state.get('object_name') or (current.get('context') or {}).get('object_name') or 'объект исследования'
    actions = state.get('action_map') if isinstance(state.get('action_map'), list) else current.get('workspace_action_map')
    selected = None
    if isinstance(actions, list):
        for action in actions:
            if isinstance(action, dict) and int(action.get('number') or 0) == number:
                selected = action
                break
    label = str((selected or {}).get('label') or '').strip()
    normalized_label = str((selected or {}).get('normalized_label') or label).strip().lower().replace('ё', 'е')

    # DEV-0003: numeric commands in Product Team research must execute the
    # visible research action, not a hard-coded generic action menu.
    if any(token in normalized_label for token in ('повторить автономное исследование', 'повторить исследование', 'продолжить исследование')):
        return _build_product_team_autonomous_research_workspace(obj, session_id=session_id, trigger='numeric_continue_or_repeat')
    if 'laboratory' in normalized_label or 'лаборатор' in normalized_label:
        lines = [
            f'📍 Передача результатов в Laboratory — {obj}',
            '',
            'Что произошло',
            'Результат автономного исследования подготовлен для Laboratory.',
            '',
            'Почему это важно',
            'Laboratory должна оценить подтверждённые ограничения и Product Opportunities и определить, требуется ли архитектурное решение или инженерное задание.',
            '',
            'Что рекомендуется сделать',
            'Передать Product Owner Report и список Product Opportunities в Laboratory.',
            '',
            '## Что делаем дальше?',
            '1. Вернуться к автономному исследованию',
            '2. Проверить Development Journal',
            '3. Сформировать инженерные задачи по подтверждённым ограничениям',
        ]
    elif 'development journal' in normalized_label or 'журнал' in normalized_label:
        lines = [
            f'📍 Проверка Development Journal — {obj}',
            '',
            'Что произошло',
            'Запрошена проверка состояния Development Journal в рамках исследования.',
            '',
            'Почему это важно',
            'Journal показывает, какие ограничения уже зарегистрированы, какие исправлены и какие ожидают проверки.',
            '',
            'Текущий результат',
            'В локальном Runtime-контексте состояние Journal может быть проверено только при доступности соответствующего продуктового контура.',
            '',
            'Что рекомендуется сделать',
            'При production-проверке запросить актуальный Development Journal и включить его состояние в Product Owner Report.',
            '',
            '## Что делаем дальше?',
            '1. Продолжить исследование по текущему шагу',
            '2. Вернуться к автономному исследованию',
            '3. Передать результаты в Laboratory',
        ]
    elif 'инженер' in normalized_label or 'engineering' in normalized_label or 'задач' in normalized_label:
        lines = [
            f'📍 Инженерные задачи — {obj}',
            '',
            'Что произошло',
            'Выполнена подготовка к формированию Engineering Tasks по подтверждённым ограничениям.',
            '',
            'Почему это важно',
            'Engineering Task создаётся только после подтверждения ограничения и анализа Laboratory.',
            '',
            'Текущий результат',
            'В рамках автономного исследования подготовлены Product Opportunities; подтверждённые инженерные задачи должны пройти через Laboratory.',
            '',
            'Что рекомендуется сделать',
            'Передать результат в Laboratory для классификации и подготовки Engineering Task при необходимости.',
            '',
            '## Что делаем дальше?',
            '1. Вернуться к автономному исследованию',
            '2. Передать результаты в Laboratory',
            '3. Проверить Development Journal',
        ]
    elif number == 5 or 'назад' in normalized_label or 'вернуться' in normalized_label:
        return current if isinstance(current, dict) and current else _build_product_team_research_workspace(f'Исследуй {obj}', session_id)
    else:
        # Compatibility for old research workspaces created before DEV-0003.
        legacy_actions = {
            1: ('Построение модели объекта', 'Определить назначение, состав, участников, жизненный цикл и связи объекта исследования.'),
            2: ('Маршрут исследования', 'Составить последовательность ролей, сценариев и проверок для исследования объекта.'),
            3: ('Development Journal', 'Проверить состояние инженерных записей, открытые ограничения и изменения после последнего релиза.'),
            4: ('Product Owner Report', 'Сформировать итоговый отчёт: подтверждённые результаты, ограничения, Product Opportunities и один следующий шаг.'),
        }
        title, description = legacy_actions.get(number, ('Действие недоступно', 'Выбранного действия нет в текущем рабочем столе исследования.'))
        lines = [
            f'📍 {title} — {obj}',
            '',
            'Что произошло',
            f'Выбрано действие №{number}: {title}.',
            '',
            'Почему это важно',
            description,
            '',
            'Что делать дальше',
            'Выполнить этот шаг в рамках исследования и затем перейти к следующему действию рабочего стола.',
            '',
            '## Что делаем дальше?',
            '1. Продолжить исследование по текущему шагу',
            '2. Вернуться к рабочему столу исследования',
            '3. Сформировать Product Owner Report',
        ]
    research_state = state.get('research_flow') if isinstance(state.get('research_flow'), dict) else {}
    if not research_state:
        research_state = current.get('active_research_state') if isinstance(current.get('active_research_state'), dict) else {}
    return {
        'status': 'ok',
        'render_mode': 'product_team_research_workspace',
        'context': {'level': 'product_team_research', 'object_name': obj, 'period': None, 'parent_object': None},
        'path': ['Product Team Assistant', 'Research', obj, label or f'Action {number}'],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'screen_order': ['workspace_markdown'],
        'workspace_render_instruction': 'Показать пользователю workspace_markdown полностью и без изменений.',
        'research_flow_status': research_state,
        'active_research_state': research_state,
        'research_path': research_state.get('research_path', []) if isinstance(research_state, dict) else [],
        'current_step': research_state.get('current_step', '') if isinstance(research_state, dict) else '',
        'next_step': research_state.get('next_step', '') if isinstance(research_state, dict) else '',
    }

def _detect_response_scope(message: str) -> str:
    text = (message or '').strip().lower()
    if not text:
        return ''
    normalized = text.replace('ё', 'е')
    if re.search(r'(^|\s)(kpi|кпи|кипи)(\s|$)', normalized):
        return 'kpi'
    if any(token in normalized for token in ('только kpi', 'только кпи', 'только показатели', 'показатели kpi')):
        return 'kpi'
    return ''


def _compact_public_all_block(items):
    compact = []
    if not isinstance(items, list):
        return compact
    allowed = {
        'object_id', 'object_name', 'name', 'level', 'navigation_money',
        'profit_delta_money', 'delta_money', 'opportunity_money', 'potential_money',
        'revenue', 'finrez_pre', 'parent_share_percent', 'business_share_percent',
        'network_count', 'sku_count', 'category_count', 'tmc_group_count', 'contract_count', 'manager_count', 'margin', 'margin_pre', 'markup', 'priority_signal',
    }
    for idx, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            continue
        row = {k: item.get(k) for k in allowed if item.get(k) is not None}
        row.setdefault('object_id', idx)
        if 'object_name' not in row and row.get('name'):
            row['object_name'] = row.get('name')
        compact.append(row)
    return compact


def _render_lines_to_markdown(lines):
    if isinstance(lines, list):
        return '\n'.join(str(x) for x in lines if str(x or '').strip())
    return str(lines or '').strip()


def _make_list_only_public_payload(rendered_payload: dict) -> dict:
    """Return the minimal public payload for Showcase (`все`).

    Showcase must not leak full рабочий стол blocks. It is a list/navigation mode,
    not analysis. This directly prevents ResponseTooLargeError for large
    Contract/Manager screens.
    """
    return {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'path': rendered_payload.get('path', []),
        'children_level': rendered_payload.get('children_level'),
        'render_mode': 'list_only',
        'summary_block': rendered_payload.get('summary_block', 'Витрина объекта. Полный список текущего уровня без аналитического сопровождения.'),
        'drain_block_render': rendered_payload.get('drain_block_render', []),
        'drain_total': rendered_payload.get('drain_total', 0),
        'all_block': _compact_public_all_block(rendered_payload.get('all_block', [])),
        'navigation_block': rendered_payload.get('navigation_block', ['назад — вернуться к объекту']),
        'screen_order': ['summary_block', 'drain_block_render', 'navigation_block'],
        'workspace_markdown': _render_lines_to_markdown([rendered_payload.get('summary_block', 'Витрина объекта.')] + (rendered_payload.get('drain_block_render') or []) + [''] + (rendered_payload.get('navigation_block') or ['назад — вернуться к объекту'])),
    }


def _make_kpi_only_public_payload(rendered_payload: dict) -> dict:
    """Return only KPI-related blocks for explicit KPI scope requests."""
    return {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'path': rendered_payload.get('path', []),
        'render_mode': 'kpi_only',
        'summary_block': 'KPI текущего рабочий стол.',
        'result_block': rendered_payload.get('result_block', []),
        'period_result_block': rendered_payload.get('period_result_block', []),
        'kpi_block': rendered_payload.get('kpi_block', []),
        'kpi_table': rendered_payload.get('kpi_table', []),
        'navigation_block': rendered_payload.get('navigation_block', ['назад — вернуться к объекту']),
        'screen_order': ['summary_block', 'result_block', 'kpi_block', 'kpi_table', 'navigation_block'],
        'workspace_markdown': _render_lines_to_markdown([rendered_payload.get('summary_block', 'KPI текущего рабочего стола.')] + (rendered_payload.get('result_block') or []) + (rendered_payload.get('kpi_block') or []) + (rendered_payload.get('kpi_table') or []) + [''] + (rendered_payload.get('navigation_block') or ['назад — вернуться к объекту'])),
    }


def _ensure_public_markdown_for_diagnostic(payload: dict) -> dict:
    """Ensure diagnostic / non-analytical public responses still carry markdown.

    Full analytical Workspace responses are handled by _make_full_workspace_public_payload.
    This helper is only for modes that are intentionally excluded from full
    Workspace validation but still must be renderable by Product Team Assistant.
    """
    if not isinstance(payload, dict):
        return payload
    if isinstance(payload.get('workspace_markdown'), str) and payload.get('workspace_markdown').strip():
        return payload
    render_mode = str(payload.get('render_mode') or '').strip().lower()
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if render_mode not in {'voice_diagnostic', 'workspace_api_attempt_error'} and level not in {'voice_management', 'workspace_opening_error'}:
        return payload
    lines = []
    for key in (
        'summary_block', 'result_block', 'period_result_block', 'kpi_block',
        'explanation_block', 'next_step_block', 'diagnosis_block',
        'recommended_next_step_block', 'navigation_block'
    ):
        value = payload.get(key)
        if isinstance(value, list):
            lines.extend(str(x).strip() for x in value if str(x or '').strip())
        elif isinstance(value, str) and value.strip():
            lines.append(value.strip())
    markdown = _render_lines_to_markdown(lines)
    if markdown:
        payload['workspace_markdown'] = markdown
        payload['screen_order'] = ['workspace_markdown']
        payload['workspace_render_instruction'] = (
            'Показать пользователю workspace_markdown полностью и без изменений. '
            'Не собирать пользовательский ответ из служебных блоков.'
        )
    return payload


def _make_reasons_only_public_payload(rendered_payload: dict) -> dict:
    return {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'path': rendered_payload.get('path', []),
        'render_mode': 'reasons',
        'summary_block': rendered_payload.get('summary_block', 'Разбор причин текущего объекта.'),
        'reasons_block_render': rendered_payload.get('reasons_block_render', []),
        'reasons_block': rendered_payload.get('reasons_block', []),
        'factor_change_block': rendered_payload.get('factor_change_block', []),
        'factor_change_table': rendered_payload.get('factor_change_table', []),
        'benchmark_diagnostic_block': rendered_payload.get('benchmark_diagnostic_block', []),
        'benchmark_diagnostic_table': rendered_payload.get('benchmark_diagnostic_table', []),
        'navigation_block': rendered_payload.get('navigation_block', ['назад — вернуться к объекту']),
        'screen_order': ['summary_block', 'reasons_block_render', 'navigation_block'],
        'workspace_markdown': _render_lines_to_markdown([rendered_payload.get('summary_block', 'Разбор причин текущего объекта.')] + (rendered_payload.get('reasons_block_render') or []) + (rendered_payload.get('factor_change_block') or []) + (rendered_payload.get('benchmark_diagnostic_block') or []) + [''] + (rendered_payload.get('navigation_block') or ['назад — вернуться к объекту'])),
    }




# Sprint W8 — Large Workspace Rendering.
# Public payload must contain one canonical rendered workspace, not three
# duplicated copies (workspace_primary_block + business_workspace_block +
# workspace_markdown). State still stores the full rendered payload separately.
def _workspace_section_title(line: str) -> str:
    text = str(line or '').strip()
    # Section titles in recovered workspaces are plain Markdown-like strings
    # starting with an emoji. Keep them business-facing and stable.
    section_markers = ('📍', '🧠', '📊', '🏗', '📈', '💰', '💵', '🌐', '🧲', '🚨', '🎯', '➡️', '🤝', '📦', '📐', '⭐', '➕')
    if text.startswith(section_markers):
        return text
    return ''


def _build_workspace_sections(block: list) -> list:
    if not isinstance(block, list):
        return []
    sections = []
    current = {'title': 'Рабочий стол', 'lines': []}
    for raw in block:
        line = str(raw or '').strip()
        if not line:
            continue
        title = _workspace_section_title(line)
        if title and current['lines']:
            sections.append(current)
            current = {'title': title, 'lines': [line]}
        elif title and not current['lines']:
            current = {'title': title, 'lines': [line]}
        else:
            current['lines'].append(line)
    if current['lines']:
        sections.append(current)
    # Compact metadata; the Custom GPT renders lines, not nested raw payloads.
    compact_sections = []
    cursor = 0
    for idx, item in enumerate(sections, start=1):
        line_count = len(item.get('lines') or [])
        compact_sections.append({
            'section': idx,
            'title': item.get('title'),
            'start_line': cursor + 1,
            'end_line': cursor + line_count,
            'line_count': line_count,
        })
        cursor += line_count
    return compact_sections


def _apply_large_workspace_rendering(payload: dict) -> dict:
    """Keep information depth but remove duplicated transport weight.

    This is not content reduction. The full visible workspace remains in
    workspace_primary_block. The route removes duplicate mirror fields and adds
    optional section metadata so the client can render a large workspace block by
    block without requesting a smaller product.
    """
    if not isinstance(payload, dict):
        return payload
    primary = payload.get('workspace_primary_block')
    if not (isinstance(primary, list) and primary):
        return payload

    payload['workspace_sections'] = _build_workspace_sections(primary)
    payload['workspace_markdown'] = '\n'.join(str(x) for x in primary if str(x or '').strip())
    payload['workspace_render_instruction'] = (
        'Показывать пользователю workspace_markdown как основной рабочий стол полностью. '
        'Не пересобирать экран из коротких legacy-блоков и не сокращать доказательные таблицы.'
    )
    payload['large_workspace_rendering'] = {
        'enabled': True,
        'mode': 'sectioned_public_payload',
        'rule': 'информация не сокращена; удалены только дубли транспортного ответа',
        'sections': len(payload.get('workspace_sections') or []),
        'lines': len(primary),
    }

    # Remove duplicate copies of the exact same rendered workspace. These fields
    # caused large Custom GPT Action responses while adding no new information.
    # Keep workspace_markdown: it is the canonical user-visible artifact for Custom GPT.
    for key in (
        'business_workspace_block', 'contract_workspace_block',
        'management_workspace_block', 'category_workspace_block', 'product_workspace_block',
        'sku_passport_block', 'decision_workspace_block', 'business_context_block',
        'business_opportunity_block', 'recommendation_block', 'narrative_block',
    ):
        if key in payload:
            payload[key] = [] if key.endswith('_block') else ''

    # All-block is a separate витрина. It is available by command «все» and must
    # not travel inside every full workspace response.
    if str(payload.get('render_mode') or '').strip().lower() not in {'list_only'}:
        payload['all_block'] = []

    # One canonical render entry is enough. Navigation remains visible.
    payload['screen_order'] = ['workspace_markdown']
    return apply_runtime_contract(payload)


def _trim_default_public_payload(payload: dict) -> dict:
    # Public response should be render-focused. Raw engine workspaces can be
    # large and are not needed by the Custom GPT when block render fields exist.
    for key in (
        'decision_workspace', 'sku_passport', 'business_context',
        'category_workspace', 'business_opportunity', 'recommendation_engine',
        'narrative_engine', 'product_workspace', 'management_intelligence',
        'management_workspace', 'management_passport',
    ):
        payload.pop(key, None)

    primary = payload.get('workspace_primary_block')
    if isinstance(primary, list) and primary:
        # W4: prevent old short BI-style blocks from competing with the recovered
        # information-dense рабочий стол in Custom GPT rendering.
        for key in (
            'result_block', 'period_result_block', 'kpi_block', 'kpi_table',
            'structure_block', 'drain_block_render', 'explanation_block',
            'next_step_block', 'diagnosis_block', 'recommended_next_step_block',
            'opportunity_explanation_block', 'anomaly_explanation_block',
            'decision_block_render', 'business_result_rating_block',
            'profit_loss_rating_block', 'opportunity_rating_block',
            'priority_action_block', 'object_reasons_block', 'factor_change_block',
            'factor_change_table', 'benchmark_diagnostic_block',
            'benchmark_diagnostic_table', 'product_layer_block',
            'product_insight_block', 'product_tmc_decision_block',
            'management_workspace_block', 'business_context_block',
            'business_opportunity_block', 'recommendation_block',
            'narrative_block', 'product_workspace_block', 'decision_workspace_block',
        ):
            if key in payload:
                payload[key] = [] if isinstance(payload.get(key), list) else ''
    payload = _apply_large_workspace_rendering(payload)
    if 'all_block' in payload:
        payload['all_block'] = _compact_public_all_block(payload.get('all_block'))
    return payload


def _workspace_generation_error(rendered_payload: dict, code: str = 'workspace_markdown_missing') -> dict:
    ctx = rendered_payload.get('context') if isinstance(rendered_payload, dict) and isinstance(rendered_payload.get('context'), dict) else {}
    return {
        'status': 'error',
        'reason': 'workspace_generation_error',
        'error_code': code,
        'message': 'Ошибка формирования Workspace: API не вернул готовый workspace_markdown. Запрос не завершён.',
        'context': ctx,
        'path': rendered_payload.get('path', []) if isinstance(rendered_payload, dict) else [],
        'render_mode': rendered_payload.get('render_mode', '') if isinstance(rendered_payload, dict) else '',
        'workspace_runtime_contract': {
            'version': 'W14_5_SINGLE_RENDERING_CONTRACT',
            'rule': 'Workspace can be rendered only from non-empty workspace_markdown.',
            'forbidden_fallback': 'Do not render summary_block/kpi_block/diagnosis_block/navigation_block or other technical blocks.',
        },
        'screen_order': ['message'],
    }


def _record_runtime_rendering_issue(session_id: str, rendered_payload: dict, event_type: str, technical_reason: str, error_code: str = None) -> None:
    try:
        ctx = rendered_payload.get('context') if isinstance(rendered_payload, dict) and isinstance(rendered_payload.get('context'), dict) else {}
        active_state = rendered_payload.get('active_workspace_state') if isinstance(rendered_payload, dict) and isinstance(rendered_payload.get('active_workspace_state'), dict) else {}
        add_development_journal_runtime_event(
            event_type=event_type,
            component='workspace_runtime_renderer',
            system_level='runtime',
            technical_reason=technical_reason,
            suspected_root_cause='API did not provide a complete renderable Workspace contract or visible action map.',
            error_code=error_code or event_type,
            runtime_context={
                'level': ctx.get('level'),
                'object_name': ctx.get('object_name'),
                'period': ctx.get('period'),
                'render_mode': rendered_payload.get('render_mode') if isinstance(rendered_payload, dict) else None,
            },
            active_workspace_state=active_state,
            reproduction_data={
                'has_workspace_markdown': bool(isinstance(rendered_payload.get('workspace_markdown') if isinstance(rendered_payload, dict) else None, str) and rendered_payload.get('workspace_markdown').strip()),
                'workspace_action_map_count': len(rendered_payload.get('workspace_action_map') or []) if isinstance(rendered_payload, dict) and isinstance(rendered_payload.get('workspace_action_map'), list) else 0,
            },
            session_id=session_id,
        )
    except Exception:
        logger.exception('development_journal_runtime_event_failed event=%s session_id=%s', event_type, session_id)


def _make_full_workspace_public_payload(rendered_payload: dict) -> dict:
    """Return a canonical full Workspace response.

    W14.5 — Single Rendering Contract.

    `workspace_markdown` is the only user-visible source for Workspace screens.
    If it is missing or empty, the response is an explicit Workspace generation
    error. The public payload must not expose legacy technical blocks as a
    fallback rendering surface.
    """
    if not isinstance(rendered_payload, dict):
        return rendered_payload

    markdown = rendered_payload.get('workspace_markdown')
    if not isinstance(markdown, str) or not markdown.strip():
        return _workspace_generation_error(rendered_payload)

    state = rendered_payload.get('active_workspace_state', {})
    action_map = rendered_payload.get('workspace_action_map', [])
    contract = rendered_payload.get('workspace_runtime_contract', {})
    if isinstance(contract, dict):
        contract = dict(contract)
        contract['version'] = 'W14_5_SINGLE_RENDERING_CONTRACT'
        contract['single_rendering_contract'] = True
        contract['forbidden_user_visible_blocks'] = [
            'summary_block', 'kpi_block', 'diagnosis_block', 'reasons_block',
            'navigation_block', 'recommendation_block', 'explanation_block',
            'factor_block', 'benchmark_block', 'workspace_primary_block',
        ]

    return {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'path': rendered_payload.get('path', []),
        'render_mode': rendered_payload.get('render_mode', ''),
        'workspace_markdown': markdown,
        'workspace_render_instruction': (
            'Показать пользователю только workspace_markdown полностью и без изменений. '
            'Не показывать и не использовать для пользовательского рендера summary_block, kpi_block, diagnosis_block, navigation_block или другие служебные блоки. '
            'Если workspace_markdown отсутствует — сообщить об ошибке формирования Workspace.'
        ),
        'active_workspace_state': state,
        'workspace_action_map': action_map,
        'workspace_runtime_contract': contract,
        # DEV-0004: keep Product Team research runtime visible at the public
        # boundary so Custom GPT can pass it into the next Action call.
        'active_research_state': rendered_payload.get('active_research_state', {}) or rendered_payload.get('research_flow_status', {}),
        'research_flow_status': rendered_payload.get('research_flow_status', {}) or rendered_payload.get('active_research_state', {}),
        'research_path': rendered_payload.get('research_path', []),
        'current_step': rendered_payload.get('current_step', ''),
        'next_step': rendered_payload.get('next_step', ''),
        # DEV-0006: expose Autonomous User Session state so Custom GPT can
        # distinguish Product Owner command from Assistant-generated user
        # messages and continue acceptance without hidden chat state.
        'autonomous_user_session': rendered_payload.get('autonomous_user_session', {}),
        'user_session_id': rendered_payload.get('user_session_id', ''),
        'user_role': rendered_payload.get('user_role', ''),
        'user_goal': rendered_payload.get('user_goal', ''),
        'user_history': rendered_payload.get('user_history', []),
        'session_status': rendered_payload.get('session_status', ''),
        'start_screen_contract': rendered_payload.get('start_screen_contract', {}),
        'runtime_navigation': rendered_payload.get('runtime_navigation', {}),
        'screen_order': ['workspace_markdown'],
    }


@router.post('/development-journal/register', summary='Global Development Journal Registration')
def development_journal_register(request: dict):
    """Independent global Development Journal API.

    Works for any VECTRA assistant role and does not require Workspace Runtime,
    workspace_markdown, active_workspace_state or business API context. The
    request must contain normalized engineering knowledge; raw chat history is
    intentionally ignored.
    """
    session_id = str(request.get('session_id') or 'global') if isinstance(request, dict) else 'global'
    dry_run = bool(request.get('dry_run')) if isinstance(request, dict) else False
    is_test = bool(request.get('is_test')) if isinstance(request, dict) else False
    record = add_development_journal_global_record(
        event_type=str(request.get('event_type') or 'manual_engineering_registration'),
        component=str(request.get('component') or 'global_development_journal_api'),
        technical_description=str(request.get('technical_description') or 'Manual global engineering registration created by VECTRA assistant.'),
        suspected_root_cause=str(request.get('suspected_root_cause') or 'Requires laboratory review.'),
        proposed_fix_direction=str(request.get('proposed_fix_direction') or 'Classify, aggregate and convert into an engineering task if confirmed.'),
        priority=str(request.get('priority') or 'P1'),
        runtime_context=request.get('runtime_context') if isinstance(request.get('runtime_context'), dict) else {'source_scope': 'any_vectra_assistant'},
        session_id=session_id,
        dry_run=dry_run,
        is_test=is_test,
    )
    return build_development_journal_capture_response(record)



@router.post('/development-journal/analyze-dialogue', summary='Development Journal Dialogue Engineering Review')
def development_journal_analyze_dialogue(request: dict):
    """Batch engineering review for Product Acceptance dialogues.

    The endpoint accepts transient dialogue/messages input, classifies defects in
    memory, deduplicates them and persists only normalized engineering records.
    Raw dialogue text is never stored in Development Journal records.
    """
    if not isinstance(request, dict):
        request = {}
    session_id = str(request.get('session_id') or 'global')
    dialogue = request.get('dialogue') if 'dialogue' in request else request.get('messages')
    result = analyze_development_journal_dialogue(
        dialogue=dialogue,
        session_ctx=request.get('session_context') if isinstance(request.get('session_context'), dict) else None,
        session_id=session_id,
        dry_run=bool(request.get('dry_run')),
        is_test=bool(request.get('is_test')),
    )
    return build_development_journal_dialogue_review_response(result)


@router.get('/development-journal/export', summary='Development Journal Export')
def development_journal_export(include_test: bool = False):
    return build_development_journal_response(export=True, include_test=include_test)


@router.get('/test-plan', summary='TEST PLAN Engine')
def test_plan_engine():
    from app.release_manager import build_test_plan_response
    return build_test_plan_response()


@router.post('/release-manager/run', summary='Autonomous Release Manager Acceptance')
def release_manager_run(request: dict):
    from app.release_manager import run_release_acceptance, build_release_manager_response
    if not isinstance(request, dict):
        request = {}
    result = run_release_acceptance(
        release_id=str(request.get('release_id') or 'manual-release'),
        scenario_ids=request.get('scenario_ids') if isinstance(request.get('scenario_ids'), list) else None,
        release_brief=request.get('release_brief') or request.get('brief'),
    )
    return build_release_manager_response(result)


@router.post('/release-manager/accept-release-brief', summary='Release Manager accepts Release Brief and starts Product Acceptance')
def release_manager_accept_release_brief(request: dict):
    """Receiving Release Brief is the trigger for Product Acceptance.

    Product Owner does not need a separate "check release" command; this route
    starts Release Manager automatically from the supplied Release Brief.
    """
    from app.release_manager import run_release_acceptance, build_release_manager_response
    if not isinstance(request, dict):
        request = {}
    brief_payload = request.get('release_brief') or request.get('brief') or request
    result = run_release_acceptance(
        release_id=str(request.get('release_id') or 'release-brief-received'),
        scenario_ids=request.get('scenario_ids') if isinstance(request.get('scenario_ids'), list) else None,
        release_brief=brief_payload,
    )
    response = build_release_manager_response(result)
    response['release_manager_trigger'] = 'release_brief_received'
    return response


@router.post('/release-brief/preview', summary='Release Brief Preview')
def release_brief_preview(request: dict):
    from app.release_brief import parse_release_brief, build_release_brief_markdown
    from app.development_journal import mark_tasks_fixed
    from app.release_brief import normalize_task_ids
    if not isinstance(request, dict):
        request = {}
    brief = parse_release_brief(request.get('release_brief') or request.get('brief') or request, fallback_release_id=str(request.get('release_id') or 'manual-release'))
    # Engineering automation boundary. Build tooling may supply implemented
    # task ids as top-level metadata; the Release Brief section itself is never
    # filled from this payload. We first persist Open -> Fixed in Development
    # Journal, then render the section only from journal state.
    engineering_fixed_ids = normalize_task_ids(
        request.get('engineering_fixed_task_ids')
        or request.get('implemented_engineering_task_ids')
        or request.get('fixed_task_ids')
    )
    if engineering_fixed_ids:
        mark_tasks_fixed(
            engineering_fixed_ids,
            release=str(brief.release_id),
            version=str(brief.build or ''),
            actor='Engineering',
            comment='Engineering build completed; fix persisted automatically before Release Brief rendering.',
        )
        brief.fixed_engineering_tasks = []
    return {
        'status': 'ok',
        'render_mode': 'release_brief',
        'context': {'level': 'release_brief', 'object_name': 'Release Brief', 'period': None},
        'workspace_markdown': build_release_brief_markdown(brief),
        'release_brief': brief.to_dict(),
    }


@router.post('/scenario-runner/run', summary='Scenario Runner Execution')
def scenario_runner_run(request: dict):
    from app.release_manager import _get_scenario
    from app.scenario_runner import run_scenario
    if not isinstance(request, dict):
        request = {}
    scenario_id = str(request.get('scenario_id') or 'S1-START-SCREEN')
    scenario = _get_scenario(scenario_id)
    if not scenario:
        return {'status': 'error', 'reason': 'unknown_scenario', 'scenario_id': scenario_id}
    return run_scenario(
        scenario=scenario,
        release_id=str(request.get('release_id') or 'manual-scenario-runner'),
        session_id=str(request.get('session_id') or '') or None,
        decision_callback=lambda step_result: 'PASS',
    )


@router.get('/scenario-library', summary='Scenario Library')
def scenario_library_get():
    from app.release_manager import get_full_scenario_library
    return {
        'status': 'ok',
        'render_mode': 'scenario_library',
        'scenario_library': get_full_scenario_library(),
    }


@router.post('/laboratory/analyze-journal', summary='Laboratory Journal Analysis')
def laboratory_analyze_journal(request: dict | None = None):
    from app.laboratory_processor import build_laboratory_response
    return build_laboratory_response()


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

    # DEV-0002: a payload that already carries canonical runtime rendering
    # must not be sent through public_summary(). public_summary() is intended
    # for raw domain summaries and can drop already-built Workspace artifacts
    # such as workspace_markdown / workspace_primary_block. This was the root
    # cause of workspace_markdown_missing for Product Team Assistant research
    # and workspace-opening error screens.
    if isinstance(payload.get('workspace_markdown'), str) and payload.get('workspace_markdown').strip():
        # Raw analytical summaries may already carry a preliminary markdown copy
        # for state continuity while still requiring public_summary() to normalize
        # metrics and build final render blocks. Treat markdown-only payloads as
        # render-ready only when they also expose an explicit render surface.
        if payload.get('status') == 'error' or payload.get('render_mode') or payload.get('screen_order'):
            return True
    if isinstance(payload.get('workspace_primary_block'), list) and payload.get('workspace_primary_block'):
        return True

    render_keys = {'kpi_block', 'structure_block', 'navigation_block', 'drain_block_render', 'result_block'}
    return bool(render_keys.intersection(payload.keys())) and isinstance(payload.get('context'), dict)


def _replace_user_terms(value):
    replacements = {
        'Результат объекта': 'Результат периода',
        'Вклад в прибыль бизнеса': 'Результат периода',
        'Вклад в результат бизнеса': 'Результат периода',
        'Потенциал возврата прибыли': 'Потенциал прибыли',
        'Business Benchmark': 'Средний уровень бизнеса',
        'SKU Benchmark': 'Эффективность SKU относительно бизнеса',
        'Проверить SKU Benchmark': 'Проверить эффективность SKU',
    }
    if isinstance(value, str):
        out = value
        for old, new in replacements.items():
            out = out.replace(old, new)
        return out
    if isinstance(value, list):
        return [_replace_user_terms(v) for v in value]
    if isinstance(value, dict):
        return {k: _replace_user_terms(v) for k, v in value.items()}
    return value


def _apply_stage51_render_overrides(payload):
    if not isinstance(payload, dict):
        return payload
    payload = _replace_user_terms(dict(payload))
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    render_mode = str(payload.get('render_mode') or '').strip().lower()

    if render_mode not in {'start', 'list_only', 'reasons', 'kpi_only', 'voice_diagnostic', 'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace', 'development_journal', 'development_journal_capture', 'development_journal_export', 'release_manager', 'laboratory_analysis', 'test_plan', 'architecture_complete_gate', 'product_review', 'sprint_candidate', 'decision_capture', 'task_capture', 'feedback_capture', 'corporate_memory', 'closed_loop_status', 'product_intelligence', 'scenario_runner', 'scenario_library'} and level:
        try:
            payload['result_block'] = _render_result_block(payload)
            payload['summary_block'] = _build_benchmark_driven_summary(payload)
            payload['explanation_block'] = _build_explanation_block(payload)
            payload['next_step_block'] = _build_next_step_block(payload)
            payload['diagnosis_block'] = _build_assistant_diagnosis_block(payload)
            payload['recommended_next_step_block'] = _build_recommended_next_step_block(payload)
            payload['opportunity_explanation_block'] = _build_opportunity_explanation_block(payload)
            payload['anomaly_explanation_block'] = _build_anomaly_explanation_block(payload)
            payload['business_opportunity_block'] = _render_business_opportunity_block(payload)
            payload['recommendation_block'] = _render_recommendation_block(payload)
            payload['narrative_block'] = _render_narrative_block(payload)
            payload['product_workspace_block'] = _render_product_workspace_block(payload)
            payload['management_workspace_block'] = _render_management_workspace_block(payload)
            payload['screen_order'] = _stage7_screen_order(payload)
        except Exception:
            logger.exception('stage51_explanation_override_failed')

    try:
        drain = _normalize_drain(payload)
        if isinstance(drain, dict):
            if render_mode == 'list_only':
                payload['drain_block_render'] = _render_vitrina_block(payload)
                payload['summary_block'] = 'Витрина объекта. Полный список текущего уровня без аналитического сопровождения.'
            else:
                payload['drain_block_render'] = _render_drain_block(drain)
            if render_mode not in {'start', 'list_only', 'reasons', 'kpi_only', 'voice_diagnostic', 'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace', 'development_journal', 'development_journal_capture', 'development_journal_export', 'release_manager', 'laboratory_analysis', 'test_plan', 'architecture_complete_gate', 'product_review', 'sprint_candidate', 'decision_capture', 'task_capture', 'feedback_capture', 'corporate_memory', 'closed_loop_status', 'product_intelligence', 'scenario_runner', 'scenario_library'}:
                payload['navigation_block'] = _render_navigation_block(payload, _normalize_navigation(payload, drain), drain)
    except Exception:
        logger.exception('stage51_navigation_override_failed')
    return payload



def _attach_sku_passport_if_missing(payload):
    if not isinstance(payload, dict):
        return payload
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    if payload.get('render_mode') in {'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'list_only', 'reasons', 'kpi_only'}:
        return payload
    if str(ctx.get('level') or '').strip().lower() != 'sku':
        return payload
    if payload.get('sku_passport') and payload.get('sku_passport_block'):
        return payload
    sku = ctx.get('object_name') or payload.get('object_name')
    period = ctx.get('period') or payload.get('period')
    if not sku or not period:
        return payload
    filter_payload = {}
    path = payload.get('path') if isinstance(payload.get('path'), list) else []
    # Path convention: Business -> Top Manager -> Manager -> Network -> Category -> SKU
    if len(path) >= 4:
        filter_payload['network'] = path[3]
    if len(path) >= 5:
        filter_payload['category'] = path[4]
    existing_filter = payload.get('filter') if isinstance(payload.get('filter'), dict) else {}
    filter_payload.update({k: v for k, v in existing_filter.items() if k in {'network', 'category', 'tmc_group'}})
    try:
        rebuilt = public_summary(get_sku_summary(sku=sku, period=period, filter_payload=filter_payload))
        for key in ('sku_passport', 'sku_passport_block', 'business_context', 'business_context_block', 'category_workspace', 'category_workspace_block', 'business_opportunity', 'business_opportunity_block', 'recommendation_engine', 'recommendation_block', 'narrative_engine', 'narrative_block', 'product_workspace', 'product_workspace_block', 'management_intelligence', 'management_workspace', 'management_passport', 'management_workspace_block', 'business_workspace_block', 'contract_workspace_block'):
            if rebuilt.get(key):
                payload[key] = rebuilt.get(key)
        if payload.get('sku_passport_block'):
            order = payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
            if 'sku_passport_block' not in order:
                payload['screen_order'] = ['sku_passport_block'] + order
    except Exception:
        logger.exception('attach_sku_passport_failed')
    return payload



def _attach_management_workspace_if_missing(payload):
    if not isinstance(payload, dict):
        return payload
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    if level not in {'business', 'manager_top', 'manager'}:
        return payload
    if payload.get('management_intelligence') and payload.get('management_workspace_block'):
        return payload
    period = ctx.get('period') or payload.get('period')
    object_name = ctx.get('object_name') or payload.get('object_name')
    if not period:
        return payload
    try:
        if level == 'business':
            rebuilt = public_summary(get_business_summary(period=period))
        elif level == 'manager_top':
            if not object_name:
                return payload
            rebuilt = public_summary(get_manager_top_summary(manager_top=object_name, period=period))
        elif level == 'manager':
            if not object_name:
                return payload
            rebuilt = public_summary(get_manager_summary(manager=object_name, period=period))
        else:
            return payload
        for key in ('management_intelligence', 'management_workspace', 'management_passport', 'management_workspace_block', 'business_workspace_block', 'contract_workspace_block'):
            if rebuilt.get(key):
                payload[key] = rebuilt.get(key)
        if payload.get('management_workspace_block'):
            order = payload.get('screen_order') if isinstance(payload.get('screen_order'), list) else []
            if 'management_workspace_block' not in order:
                payload['screen_order'] = ['management_workspace_block'] + order
    except Exception:
        logger.exception('attach_management_workspace_failed')
    return payload

def _prepare_vectra_query_payload(payload):
    """Normalize only raw API/domain summaries.

    UI/state commands (все / причины / назад) may already return a final
    render-ready screen. Re-normalizing that screen through public_summary()
    breaks it because render screens do not carry the raw metrics contract.
    """
    if _is_render_ready_payload(payload):
        ready = dict(payload)
        ready.setdefault('status', 'ok')
        ctx = ready.get('context') if isinstance(ready.get('context'), dict) else {}
        render_mode = str(ready.get('render_mode') or '').strip().lower()
        level = str(ctx.get('level') or '').strip().lower()

        # DEV-0002: render-ready diagnostic/error payloads already contain the
        # public user-facing markdown. Do not run them through analytical
        # enrichment helpers: those helpers expect raw KPI metric contracts and
        # can fail or strip the already-built Workspace response.
        if ready.get('status') == 'error' or render_mode in {'start', 'voice_diagnostic', 'workspace_api_attempt_error', 'product_team_research_workspace'} or level in {'start', 'workspace_opening_error', 'voice_management', 'product_team_research'}:
            return _ensure_vectra_query_render_contract(ready)

        ready = _attach_sku_passport_if_missing(ready)
        ready = _attach_management_workspace_if_missing(ready)
        ready = _apply_stage51_render_overrides(ready)
        ready = _attach_product_recovery_blocks(ready)
        return _ensure_vectra_query_render_contract(_force_product_navigation(ready))
    rendered = public_summary(payload)
    rendered = _attach_sku_passport_if_missing(rendered)
    rendered = _attach_management_workspace_if_missing(rendered)
    rendered = _apply_stage51_render_overrides(rendered)
    rendered = _attach_product_recovery_blocks(rendered)
    return _ensure_vectra_query_render_contract(rendered)


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

    if render_mode == 'voice_diagnostic':
        return payload

    if render_mode == 'action_package':
        out = []
        seen = set()
        for line in nav:
            text = str(line)
            if text and text not in seen:
                out.append(text)
                seen.add(text)
        if not any('назад' in x.lower() for x in out):
            out.append('назад — вернуться к объекту')
        payload['navigation_block'] = out
        return payload

    if render_mode == 'kpi_only':
        payload['decision_workspace_block'] = []
        payload['explanation_block'] = []
        payload['next_step_block'] = []
        payload['recommended_next_step_block'] = []
        payload['diagnosis_block'] = []
        payload['reasons_block_render'] = []
        payload['decision_block_render'] = []
        payload['business_opportunity_block'] = []
        payload['recommendation_block'] = []
        payload['narrative_block'] = []
        payload['product_workspace_block'] = []
        payload['business_context_block'] = []
        payload['category_workspace_block'] = []
        payload['drain_block_render'] = []
        payload['drain_total'] = 0
        payload['navigation_block'] = ['причины — разобрать факторы', 'все — витрина текущего уровня', 'назад — вернуться к рабочему столу']
        payload['screen_order'] = ['summary_block', 'result_block', 'period_result_block', 'kpi_block', 'kpi_table', 'navigation_block']
        return payload

    if render_mode == 'reasons':
        payload['decision_workspace_block'] = []
        payload['explanation_block'] = []
        payload['next_step_block'] = []
        payload['recommended_next_step_block'] = []
        payload['diagnosis_block'] = []
        payload['decision_block_render'] = []
        payload['business_opportunity_block'] = []
        payload['recommendation_block'] = []
        payload['narrative_block'] = []
        payload['product_workspace_block'] = []
        payload['business_context_block'] = []
        payload['category_workspace_block'] = []
        payload['drain_block_render'] = []
        payload['drain_total'] = 0
        payload['navigation_block'] = ['назад к объекту']
        payload['screen_order'] = ['summary_block', 'reasons_block_render', 'navigation_block']
        return payload

    # A numeric line starts navigation to a concrete child object.
    has_numeric_items = any(str(line).strip()[:1].isdigit() for line in nav)
    if render_mode == 'list_only':
        payload['decision_workspace_block'] = []
        payload['explanation_block'] = []
        payload['next_step_block'] = []
        payload['recommended_next_step_block'] = []
        payload['diagnosis_block'] = []
        payload['reasons_block_render'] = []
        payload['decision_block_render'] = []
        payload['business_opportunity_block'] = []
        payload['recommendation_block'] = []
        payload['narrative_block'] = []
        payload['product_workspace_block'] = []
        out = []
        seen = set()
        for line in nav:
            text = str(line)
            if text.strip()[:1].isdigit() or 'назад' in text.lower():
                if text not in seen:
                    out.append(text)
                    seen.add(text)
        if not any('назад' in x.lower() for x in out):
            out.append('назад — вернуться к объекту')
        payload['navigation_block'] = out
        return payload

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

    # Action-first navigation: after a рабочий стол, user should see working actions,
    # not only the next DATA level. Detailed drilldown remains available through
    # numeric commands / all / direct free questions.
    if level == 'network':
        add('подготовить переговоры — собрать позицию по контракту')
        add('собрать пакет позиций — выбрать позиции для ввода')
        add('показать лидеров SKU — роли текущих позиций')
        add('показать отсутствующие SKU — лидеры бизнеса вне контракта')
        add('показать ассортиментные перекосы — концентрация и пробелы')
        add('создать задачи — зафиксировать действия по контракту')
        add('причины — разбор контракта')
    elif level in {'category', 'tmc_group'}:
        add('подготовить пакет развития — форматы и позиции категории')
        add('подготовить переговорный аргумент — как продать категорию')
        add('посмотреть отсутствующие позиции — найти ассортиментные возможности')
    elif level == 'sku':
        add('паспорт SKU — полная карточка позиции')
        add('подготовить переговоры — использовать позицию как аргумент')
        add('создать задачи — зафиксировать действие по SKU')
    elif level and level not in {'start'} and not _is_product_layer_level(level):
        add('причины — разбор')

    # v9: no separate 'искать' command; numeric navigation and 'все' are enough.

    # Back should exist below business and in list/reasons modes.
    if level and level not in {'business', 'start'}:
        add('назад — вверх')
    elif render_mode in {'list_only', 'reasons', 'kpi_only'}:
        add('назад — вверх')

    payload['navigation_block'] = out
    return payload




@router.post('/vectra/query', summary='Stateful VECTRA Query')
def vectra_query(request: VectraQueryRequest):
    session_id = _stable_session_id(request)
    logger.info('vectra_query_received session_id=%s message=%r', session_id, request.message)
    
    # State/UI commands (все / причины / назад) are handled only inside
    # orchestration.py. routes.py is now only API/render boundary.
    _hydrate_runtime_context_from_request(session_id, request)

    if _is_product_team_research_request(request.message):
        payload = _build_product_team_research_workspace(request.message, session_id=session_id)
    elif _is_research_continue_request(request.message, session_id):
        obj = _current_product_team_research_object(session_id)
        payload = _build_product_team_autonomous_research_workspace(obj, session_id=session_id, trigger='continue_research_route')
    elif _is_numeric_research_action(request.message, session_id):
        payload = _build_product_team_research_action_workspace(request.message, session_id=session_id)
    else:
        payload = orchestrate_vectra_query(request.message, session_id=session_id)
    logger.info('vectra_query_result session_id=%s status=%s reason=%s', session_id, payload.get('status'), payload.get('reason'))
    rendered_payload = apply_runtime_contract(_prepare_vectra_query_payload(payload))
    response_scope = _detect_response_scope(request.message)
    if rendered_payload.get('render_mode') == 'list_only':
        render_only_payload = _make_list_only_public_payload(rendered_payload)
    elif rendered_payload.get('render_mode') == 'reasons':
        render_only_payload = _make_reasons_only_public_payload(rendered_payload)
    elif response_scope == 'kpi':
        render_only_payload = _make_kpi_only_public_payload(rendered_payload)
    else:
        render_only_payload = {
        'status': rendered_payload.get('status', 'ok'),
        'reason': rendered_payload.get('reason'),
        'context': rendered_payload.get('context'),
        'compare_base': rendered_payload.get('compare_base'),
        # CHANGE-005.1: put Profit First block before KPI so clients that render
        # payload order start with «Что произошло», not with Opportunity/KPI.
        'result_block': rendered_payload.get('result_block', []),
        'period_result_block': rendered_payload.get('period_result_block', []),
        'summary_block': rendered_payload.get('summary_block', ''),
        'kpi_block': rendered_payload.get('kpi_block', []),
        'kpi_table': rendered_payload.get('kpi_table', []),
        'structure_block': rendered_payload.get('structure_block', []),
        'main_driver': rendered_payload.get('main_driver', ''),
        'drain_block_render': rendered_payload.get('drain_block_render', []),
        'drain_total': rendered_payload.get('drain_total', 0),
        'all_block': rendered_payload.get('all_block', []),
        'navigation_block': rendered_payload.get('navigation_block', []),
        'explanation_block': rendered_payload.get('explanation_block', []),
        'next_step_block': rendered_payload.get('next_step_block', []),
        'diagnosis_block': rendered_payload.get('diagnosis_block', []),
        'recommended_next_step_block': rendered_payload.get('recommended_next_step_block', []),
        'opportunity_explanation_block': rendered_payload.get('opportunity_explanation_block', []),
        'anomaly_explanation_block': rendered_payload.get('anomaly_explanation_block', []),
        'screen_order': rendered_payload.get('screen_order', []),
        'workspace_primary_block': rendered_payload.get('workspace_primary_block', []),
        'workspace_markdown': rendered_payload.get('workspace_markdown', ''),
        'workspace_render_instruction': rendered_payload.get('workspace_render_instruction', ''),
        'active_workspace_state': rendered_payload.get('active_workspace_state', {}),
        'workspace_action_map': rendered_payload.get('workspace_action_map', []),
        'workspace_runtime_contract': rendered_payload.get('workspace_runtime_contract', {}),
        # DEV-0004: expose Research Flow state at the public API boundary.
        # Custom GPT Actions do not receive hidden chat state automatically;
        # these fields are the explicit bridge that allows the user-mode
        # Product Team Assistant to continue the research scenario after
        # vectraQuery returns.
        'active_research_state': rendered_payload.get('active_research_state', {}) or rendered_payload.get('research_flow_status', {}),
        'research_flow_status': rendered_payload.get('research_flow_status', {}) or rendered_payload.get('active_research_state', {}),
        'research_path': rendered_payload.get('research_path', []),
        'current_step': rendered_payload.get('current_step', ''),
        'next_step': rendered_payload.get('next_step', ''),
        # DEV-0006: public Autonomous User Session bridge.
        'autonomous_user_session': rendered_payload.get('autonomous_user_session', {}),
        'user_session_id': rendered_payload.get('user_session_id', ''),
        'user_role': rendered_payload.get('user_role', ''),
        'user_goal': rendered_payload.get('user_goal', ''),
        'user_history': rendered_payload.get('user_history', []),
        'session_status': rendered_payload.get('session_status', ''),
        'start_screen_contract': rendered_payload.get('start_screen_contract', {}),
        'runtime_navigation': rendered_payload.get('runtime_navigation', {}),
        'path': rendered_payload.get('path', []),
        'reasons_block': rendered_payload.get('reasons_block', []),
        'reasons_block_render': rendered_payload.get('reasons_block_render', []),
        'decision_block': rendered_payload.get('decision_block', []),
        'decision_block_render': rendered_payload.get('decision_block_render', []),
        'business_result_rating_block': rendered_payload.get('business_result_rating_block', []),
        'profit_loss_rating_block': rendered_payload.get('profit_loss_rating_block', []),
        'opportunity_rating_block': rendered_payload.get('opportunity_rating_block', []),
        'priority_action_block': rendered_payload.get('priority_action_block', []),
        'object_reasons_block': rendered_payload.get('object_reasons_block', []),
        'factor_change_block': rendered_payload.get('factor_change_block', []),
        'factor_change_table': rendered_payload.get('factor_change_table', []),
        'benchmark_diagnostic_block': rendered_payload.get('benchmark_diagnostic_block', []),
        'benchmark_diagnostic_table': rendered_payload.get('benchmark_diagnostic_table', []),
        'product_layer_block': rendered_payload.get('product_layer_block', []),
        'product_insight_block': rendered_payload.get('product_insight_block', []),
        'product_tmc_decision_block': rendered_payload.get('product_tmc_decision_block', []),
        'sku_passport': rendered_payload.get('sku_passport', {}),
        'sku_passport_block': rendered_payload.get('sku_passport_block', []),
        'decision_workspace': rendered_payload.get('decision_workspace', {}),
        'business_context': rendered_payload.get('business_context', {}),
        'business_context_block': rendered_payload.get('business_context_block', []),
        'category_workspace': rendered_payload.get('category_workspace', {}),
        'category_workspace_block': rendered_payload.get('category_workspace_block', []),
        'business_opportunity': rendered_payload.get('business_opportunity', {}),
        'business_opportunity_block': rendered_payload.get('business_opportunity_block', []),
        'recommendation_engine': rendered_payload.get('recommendation_engine', {}),
        'recommendation_block': rendered_payload.get('recommendation_block', []),
        'narrative_engine': rendered_payload.get('narrative_engine', {}),
        'narrative_block': rendered_payload.get('narrative_block', []),
        'product_workspace': rendered_payload.get('product_workspace', {}),
        'product_workspace_block': rendered_payload.get('product_workspace_block', []),
        'management_intelligence': rendered_payload.get('management_intelligence', {}),
        'management_workspace': rendered_payload.get('management_workspace', {}),
        'management_passport': rendered_payload.get('management_passport', {}),
        'management_workspace_block': rendered_payload.get('management_workspace_block', []),
        'business_workspace_block': rendered_payload.get('business_workspace_block', []),
        'contract_workspace_block': rendered_payload.get('contract_workspace_block', []),
        'decision_workspace_block': rendered_payload.get('decision_workspace_block', []),
        'render_mode': rendered_payload.get('render_mode', ''),
        # CHANGE-006.1: hide aggregate Benchmark Money from the public render payload.
        'opportunity_money': rendered_payload.get('opportunity_money'),
        'navigation_money': rendered_payload.get('navigation_money'),
        'net_drain_money': rendered_payload.get('net_drain_money'),
        'gross_loss_money': rendered_payload.get('gross_loss_money'),
        'internal_drain_money': rendered_payload.get('internal_drain_money'),
        }
    render_only_payload = _trim_default_public_payload(render_only_payload)
    render_only_payload = _force_product_navigation(render_only_payload)
    render_only_payload = _ensure_public_markdown_for_diagnostic(render_only_payload)
    _ctx_level = str((render_only_payload.get('context') or {}).get('level') or '').strip().lower() if isinstance(render_only_payload.get('context'), dict) else ''
    if _ctx_level != 'assistant_dialogue' and render_only_payload.get('render_mode') not in {'start', 'list_only', 'reasons', 'kpi_only', 'voice_diagnostic', 'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace', 'development_journal', 'development_journal_capture', 'development_journal_export', 'release_manager', 'laboratory_analysis', 'test_plan', 'architecture_complete_gate', 'product_review', 'sprint_candidate', 'decision_capture', 'task_capture', 'feedback_capture', 'corporate_memory', 'closed_loop_status', 'product_intelligence', 'scenario_runner', 'scenario_library'}:
        pre_render_payload = render_only_payload
        render_only_payload = _make_full_workspace_public_payload(render_only_payload)
        if render_only_payload.get('status') == 'error' and render_only_payload.get('reason') == 'workspace_generation_error':
            _record_runtime_rendering_issue(session_id, pre_render_payload, 'workspace_markdown_missing', 'Workspace opening request produced no non-empty workspace_markdown.', render_only_payload.get('error_code'))
        elif not render_only_payload.get('workspace_action_map'):
            _record_runtime_rendering_issue(session_id, render_only_payload, 'workspace_action_map_empty', 'Workspace rendered without visible action map extracted from workspace_markdown.', 'workspace_action_map_empty')
    # Persist only analytical object/list screens at the API boundary.
    # UI display modes (все / причины) are produced by orchestration.py and
    # must not overwrite current_screen; otherwise the next «назад» would
    # return to a display mode instead of the object screen.
    should_save_rendered_state = (
        render_only_payload.get('status') != 'error'
        and _ctx_level != 'assistant_dialogue'
        and render_only_payload.get('render_mode') not in {'start', 'list_only', 'reasons', 'kpi_only', 'voice_diagnostic', 'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace', 'development_journal', 'development_journal_capture', 'development_journal_export', 'release_manager', 'laboratory_analysis', 'test_plan', 'architecture_complete_gate', 'product_review', 'sprint_candidate', 'decision_capture', 'task_capture', 'feedback_capture', 'corporate_memory', 'closed_loop_status', 'product_intelligence', 'scenario_runner', 'scenario_library'}
    )
    # Explicit object-scoped KPI requests (`Покажи Варус KPI`) return a KPI-only
    # public payload, but State must keep the full rendered рабочий стол for the
    # same object so the next local command (`причины`, `все`, `назад`) works.
    # Local KPI command (`kpi`) already comes from a kpi_only state payload and
    # must not overwrite the active рабочий стол.
    if response_scope == 'kpi' and str(rendered_payload.get('render_mode') or '').strip().lower() != 'kpi_only':
        should_save_rendered_state = True

    action_display_modes = {'action_package', 'negotiation_workspace', 'task_workspace', 'post_meeting_workspace', 'execution_workspace'}
    if should_save_rendered_state or str(rendered_payload.get('render_mode') or '').strip().lower() in action_display_modes:
        try:
            # State must keep the full rendered payload, not the public render-only
            # response. For action display modes save_last_payload updates only
            # last_payload and preserves current_screen, so `назад` restores the
            # analytical Workspace while numeric commands can read the last shown
            # action menu.
            save_last_payload(session_id, rendered_payload)
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
    render_only_payload = _enforce_public_response_budget(render_only_payload)
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
