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


PUBLIC_TOP_LEVEL_KEYS = ('profit_loss_rating', 'opportunity_rating', 'business_reasons', 'priority_action', 'period_result_block', 'opportunity_money', 'navigation_money', 'net_drain_money', 'gross_loss_money', 'internal_drain_money', 'compare_base', 'context', 'metrics', 'structure', 'drain_block', 'all_block', 'navigation', 'reasons_block', 'decision_block', 'decision_block_render', 'reasons_block_render', 'kpi_block', 'structure_block', 'main_driver', 'drain_block_render', 'drain_total', 'navigation_block', 'summary_block', 'explanation_block', 'next_step_block', 'product_layer_block', 'product_insight_block', 'product_tmc_decision_block', 'path', 'diagnosis_block', 'recommended_next_step_block', 'opportunity_explanation_block', 'anomaly_explanation_block', 'screen_order')

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
    'product_layer_block': [],
    'product_insight_block': [],
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
        return 'этого же SKU по бизнесу'
    if compare_base == 'sku_fallback_tmc_group':
        return 'такой же группы ТМС бизнеса'
    if compare_base == 'sku_fallback_category':
        return 'такой же категории бизнеса'
    return 'среднего уровня бизнеса'


def _build_product_tmc_decision_block(response):
    data = response.get('product_tmc_decision') if isinstance(response.get('product_tmc_decision'), dict) else {}
    items = [x for x in (data.get('items') or []) if isinstance(x, dict)]
    if not items:
        return []
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
                f"(эффект {_fmt_signed_int(effect)}). Рекомендация: масштабировать сильную группу и проверить возможность расширения ассортимента."
            )
        elif markup_delta < 0:
            lines.append(
                f"Группа ниже бизнеса по наценке на {_fmt_pp_delta(markup_delta)} "
                f"(потенциал до {_fmt_int(abs(effect))}). Рекомендация: проверить цену/наценку внутри группы."
            )
        else:
            lines.append('Группа концентрирует результат; следующий шаг — подтвердить устойчивость на уровне SKU.')
        return lines

    lines.append('Результат категории распределён между несколькими группами ТМС:')
    for idx, item in enumerate(items[:5], start=1):
        lines.append(f"{idx}. {item.get('object_name')} → {_fmt_signed_int(item.get('profit_delta_money'))}, доля {_fmt_percent(item.get('share_percent'))}%")
    lines.append('Рекомендация: при необходимости открыть группу ТМС для детализации, но не делать её обязательным шагом навигации.')
    return lines


def _build_product_layer_block(response):
    """V1.3 Stage 4.0 Product Layer framework.

    Presentation-only scaffold for product analysis below Network. It does
    not calculate price, volume, assortment or mix; it explains that these
    dimensions will be populated by the future VECTRA Data Mart.
    """
    return [
        'Что влияет на результат продукта?',
        'Цена — данные будут доступны после подключения VECTRA Data Mart.',
        'Объём — данные будут доступны после подключения VECTRA Data Mart.',
        'Ассортимент — данные будут доступны после подключения VECTRA Data Mart.',
        'SKU Mix — данные будут доступны после подключения VECTRA Data Mart.',
    ]


def _build_product_insight_block(response):
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    object_name = str(ctx.get('object_name') or ctx.get('name') or '').strip()
    object_label = object_name or ('SKU' if level == 'sku' else 'продукт')
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
        next_line = 'TMC Group анализируется внутри категории динамически; отдельный шаг нужен только если результат распределён между группами.'
    elif level == 'tmc_group':
        next_line = 'Следующий шаг — проверить SKU внутри группы как доказательный уровень.'
    else:
        next_line = 'SKU является диагностическим уровнем; окончательная причина требует данных Data Mart по цене, объёму, ассортименту и SKU Mix.'

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
    return ['Открыть SKU и подтвердить продуктовый результат']

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
        'screen_order': payload.get('screen_order') or [],
        'product_insight_block': payload.get('product_insight_block') or [],
        'product_tmc_decision': payload.get('product_tmc_decision') or {},
        'product_tmc_decision_block': payload.get('product_tmc_decision_block') or [],
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
        if rendered.get('render_mode') not in {'list_only', 'reasons'}:
            rendered['summary_block'] = _build_benchmark_driven_summary(rendered)
            rendered['explanation_block'] = _build_explanation_block(rendered)
            rendered['next_step_block'] = _build_next_step_block(rendered)
            rendered['diagnosis_block'] = _build_assistant_diagnosis_block(rendered)
            rendered['recommended_next_step_block'] = _build_recommended_next_step_block(rendered)
            rendered['opportunity_explanation_block'] = _build_opportunity_explanation_block(rendered)
            rendered['anomaly_explanation_block'] = _build_anomaly_explanation_block(rendered)
            rendered['screen_order'] = _stage7_screen_order(rendered)
    except Exception:
        logger.exception('explanation_layer_failed')
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

    if fin_delta < 0:
        result = f'Финрез снизился на {_fmt_int(abs(fin_delta))} к прошлому году.'
    elif fin_delta > 0:
        result = f'Финрез вырос на {_fmt_int(fin_delta)} к прошлому году.'
    else:
        result = 'Финрез находится примерно на уровне прошлого года.'

    details = []
    if revenue:
        details.append(f'оборот {_fmt_signed_int(rev_delta)}')
    if margin:
        details.append(f'маржа {_fmt_pp_delta(margin_delta)}')
    return result + (f' Дополнительно: {", ".join(details)}.' if details else '')


def _benchmark_sentence(response: dict) -> str:
    # CHANGE-006.1: Benchmark is diagnostic only. Do not render aggregate
    # Benchmark Money; show factor-level diagnostics through benchmark_diagnostic_block.
    return 'Benchmark используется как диагностика: объект сравнивается с текущим средним уровнем бизнеса по факторам.'


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
        fact = _profit_first_fact_sentence(response)
        factor_line = f'Главный отрицательный фактор: {_reason_display_name(risk).lower()}.' if risk else 'Критичный отрицательный фактор не выделен.'
        strong_line = f'Сильный фактор: {_reason_display_name(strong).lower()}.' if strong else ''
        opportunity_line = f'Резерв прибыли внутри объекта: {_fmt_int(opportunity)} грн.' if opportunity > 0 else 'Существенный резерв прибыли внутри объекта не выявлен.'
        return ' '.join([x for x in [fact, factor_line, strong_line, opportunity_line] if x])

    if layer == 'product':
        fact = _profit_first_fact_sentence(response)
        opportunity_line = f'Потенциал внутри продукта: {_fmt_int(opportunity)} грн.' if opportunity > 0 else 'Существенный продуктовый резерв не выявлен.'
        return f'{fact} {opportunity_line} Детальный анализ цены, объёма, ассортимента и SKU Mix будет доступен после подключения VECTRA Data Mart.'

    if layer == 'sku':
        return f'{_sku_metric_sentence(response)} Для полного анализа SKU не хватает данных Data Mart. Доступна только оценка по текущим KPI.'

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
            'Profit First: сначала смотрим изменение прибыли к прошлому году.',
            f'Крупнейшие просадки прибыли: {loss_names or "данных нет"}.',
            f'Главные резервы возврата: {potential_names or "данных нет"}.',
            'Benchmark используется только как диагностика по отклонениям от бизнеса, без отдельной агрегированной денежной оценки.',
        ]

    lines = [
        'Profit First: сначала объект сравнивается сам с собой прошлого периода.',
        _profit_first_fact_sentence(response),
    ]

    if risk:
        lines.append(f'Главный отрицательный фактор диагностики: {_reason_display_name(risk)} ({_fmt_signed_int(_reason_effect(risk))}).')
    if strong:
        lines.append(f'Главный положительный фактор диагностики: {_reason_display_name(strong)} ({_fmt_signed_int(_reason_effect(strong))}).')

    if layer not in {'sku'}:
        lines.append(_benchmark_sentence(response))
    if opportunity > 0:
        lines.append(f'Opportunity показывает, где внутри выбранного объекта искать резерв: {_fmt_int(opportunity)} грн.')
    else:
        lines.append('Существенный Opportunity-резерв внутри объекта не выявлен.')

    if layer in {'product', 'sku'}:
        lines.append('Для полноценной причины нужны данные Data Mart: цена, объём, ассортимент, SKU Mix и контекст исполнения.')
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
            return [f'➡ Рекомендуемый следующий шаг: открыть {first_child} как SKU-доказательство продуктового результата.']
        return ['➡ Рекомендуемый следующий шаг: сравнить продукт с таким же продуктом бизнеса и подтвердить решение на SKU.']

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
        return 'Benchmark на Business Screen не выводится: бизнес не сравнивается с самим собой.'
    risk = _worst_benchmark_gap_reason(response)
    if risk:
        delta = _num(risk.get('delta_vs_business_percent', risk.get('delta_percent')))
        return f'Относительно {_product_compare_base_label(response)} слабое место: {_reason_display_name(risk).lower()} ({_fmt_pp_delta(delta)}, эффект {_render_money_value(_reason_effect_vs_business(risk))}). Это benchmark-диагностика, а не причина изменения к прошлому году.'
    return f'Относительно {_product_compare_base_label(response)} отдельный критичный разрыв по доступным данным не выделен.'


def _build_assistant_diagnosis_block(response: dict) -> list:
    """Stage 7 / Assistant Diagnostic Layer.

    This is a presentation-only layer. It explains API numbers and does not
    calculate new KPI, change navigation, benchmark, opportunity or effect logic.
    """
    if response.get('render_mode') in {'list_only', 'reasons', 'voice_diagnostic'}:
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
        lines.append('Это Product Layer: цена, объём, ассортимент и SKU Mix будут полноценно объяснены после подключения VECTRA Data Mart.')
    elif layer == 'sku':
        lines.append('Это SKU Layer: доступна KPI-диагностика, без окончательной причины до подключения Data Mart.')
    return [line for line in lines if line]


def _build_recommended_next_step_block(response: dict) -> list:
    if response.get('render_mode') in {'list_only', 'reasons', 'voice_diagnostic'}:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    metrics = response.get('metrics') or []
    fin_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Финрез до'))
    rev_delta = _delta_money_for_metric(_metric_by_name(metrics, 'Оборот'))
    margin_delta = _delta_percent_for_metric(_metric_by_name(metrics, 'Маржа'))

    if level == 'sku':
        return ['➡ Рекомендуемый следующий шаг: перейти к причинам и проверить доступные факторные сигналы SKU.']

    children = response.get('all_block') if isinstance(response.get('all_block'), list) else []
    first_child = ''
    if children and isinstance(children[0], dict):
        first_child = str(children[0].get('object_name') or children[0].get('name') or '').strip()

    if fin_delta < 0 and rev_delta < 0:
        if level == 'network':
            return ['➡ Рекомендуемый следующий шаг: проверить контрактный контекст сети и открыть продуктовый уровень, чтобы понять, где потерян оборот.']
        if first_child:
            return [f'➡ Рекомендуемый следующий шаг: открыть {first_child} как крупнейший объект ниже и локализовать потерю оборота/прибыли.']
        return ['➡ Рекомендуемый следующий шаг: проверить контекст падения оборота: контракт, ассортимент, дистрибуцию и SKU mix.']

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
    if response.get('render_mode') in {'list_only', 'reasons', 'voice_diagnostic'}:
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
                'Для точного объяснения нужен дополнительный контекст или Data Mart.',
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
    if response.get('render_mode') in {'list_only', 'reasons', 'voice_diagnostic'}:
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

def _stage7_screen_order(response: dict) -> list:
    if response.get('render_mode') in {'list_only', 'reasons', 'voice_diagnostic'}:
        return []
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    if str(ctx.get('level') or '').strip().lower() == 'business':
        return ['result_block', 'diagnosis_block', 'anomaly_explanation_block', 'recommended_next_step_block', 'factor_change_block', 'opportunity_rating_block', 'opportunity_explanation_block', 'decision_block_render', 'navigation_block']
    
    if str(ctx.get('level') or '').strip().lower() in {'category', 'tmc_group'}:
        return ['result_block', 'diagnosis_block', 'anomaly_explanation_block', 'recommended_next_step_block', 'factor_change_block', 'benchmark_diagnostic_block', 'product_tmc_decision_block', 'opportunity_rating_block', 'opportunity_explanation_block', 'decision_block_render', 'navigation_block']
    return ['result_block', 'diagnosis_block', 'anomaly_explanation_block', 'recommended_next_step_block', 'factor_change_block', 'benchmark_diagnostic_block', 'opportunity_rating_block', 'opportunity_explanation_block', 'decision_block_render', 'navigation_block']


def _build_next_step_block(response: dict) -> list:
    ctx = response.get('context') if isinstance(response.get('context'), dict) else {}
    level = str(ctx.get('level') or '').strip().lower()
    if response.get('render_mode') in {'list_only', 'reasons'}:
        return []
    if level == 'business':
        losses = response.get('profit_loss_rating') or []
        first_loss = losses[0].get('object_name') if losses and isinstance(losses[0], dict) else None
        if first_loss:
            return [f'Рекомендуемый следующий шаг: открыть {first_loss} как крупнейшую просадку прибыли.']
        return ['Рекомендуемый следующий шаг: открыть полный список и найти крупнейшую просадку прибыли.']
    if _is_product_layer_level(level):
        return ['Следующий шаг: открыть SKU и проверить, где именно изменилась прибыль продукта.']
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
    order = ['Финрез до', 'Маржа', 'Оборот', 'Наценка', 'Финрез итог']
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
        response['summary_block'] = 'Полный список объектов текущего уровня.'
        response['decision_block_render'] = []
        response['business_result_rating_block'] = []
        response['profit_loss_rating_block'] = []
        response['opportunity_rating_block'] = []
        response['priority_action_block'] = []
        response['object_reasons_block'] = []
        response['factor_change_block'] = []
        response['benchmark_diagnostic_block'] = []
        response['opportunity_explanation_block'] = []
        response['anomaly_explanation_block'] = []
        response['product_layer_block'] = []
        response['product_insight_block'] = []
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

    if render_mode not in {'list_only', 'reasons', 'voice_diagnostic'} and level:
        try:
            payload['result_block'] = _render_result_block(payload)
            payload['summary_block'] = _build_benchmark_driven_summary(payload)
            payload['explanation_block'] = _build_explanation_block(payload)
            payload['next_step_block'] = _build_next_step_block(payload)
            payload['diagnosis_block'] = _build_assistant_diagnosis_block(payload)
            payload['recommended_next_step_block'] = _build_recommended_next_step_block(payload)
            payload['opportunity_explanation_block'] = _build_opportunity_explanation_block(payload)
            payload['anomaly_explanation_block'] = _build_anomaly_explanation_block(payload)
            payload['screen_order'] = _stage7_screen_order(payload)
        except Exception:
            logger.exception('stage51_explanation_override_failed')

    try:
        drain = _normalize_drain(payload)
        if isinstance(drain, dict):
            payload['drain_block_render'] = _render_drain_block(drain)
            if render_mode not in {'list_only', 'reasons', 'voice_diagnostic'}:
                payload['navigation_block'] = _render_navigation_block(payload, _normalize_navigation(payload, drain), drain)
    except Exception:
        logger.exception('stage51_navigation_override_failed')
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
        ready = _apply_stage51_render_overrides(ready)
        return _ensure_vectra_query_render_contract(_force_product_navigation(ready))
    rendered = public_summary(payload)
    rendered = _apply_stage51_render_overrides(rendered)
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

    # Contract reasons are available only through Network. Product economics view does not expose contract reasons.
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
        # CHANGE-005.1: put Profit First block before KPI so clients that render
        # payload order start with «Что произошло», not with Opportunity/KPI.
        'result_block': rendered_payload.get('result_block', []),
        'period_result_block': rendered_payload.get('period_result_block', []),
        'summary_block': rendered_payload.get('summary_block', ''),
        'kpi_block': rendered_payload.get('kpi_block', []),
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
        'benchmark_diagnostic_block': rendered_payload.get('benchmark_diagnostic_block', []),
        'product_layer_block': rendered_payload.get('product_layer_block', []),
        'product_insight_block': rendered_payload.get('product_insight_block', []),
        'product_tmc_decision_block': rendered_payload.get('product_tmc_decision_block', []),
        'render_mode': rendered_payload.get('render_mode', ''),
        # CHANGE-006.1: hide aggregate Benchmark Money from the public render payload.
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
    if render_only_payload.get('status') != 'error' and render_only_payload.get('render_mode') not in {'list_only', 'reasons', 'voice_diagnostic'}:
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
