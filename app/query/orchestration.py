from copy import deepcopy
import logging
import json
import time
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
from app.domain.filters import get_normalized_rows, filter_rows
from app.domain.metrics import aggregate_metrics
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
from app.domain.normalization import parse_period_from_text
from app.query.entity_dictionary import normalize_entity_text, normalize_fuzzy_text
from app.query.parsing import normalize_user_message, parse_query_intent
from app.query.entity_resolution import resolve_entity, _canonical_values
from app.query.entity_dictionary import get_entity_dictionary


SESSION_STORE: Dict[str, Dict[str, Any]] = {}
SESSION_LOCK = Lock()
SESSION_FILE = Path('/tmp/vectra_session_store.json')
MAX_LAST_LIST_ITEMS = 500

logger = logging.getLogger(__name__)

DEFAULT_NEXT_LEVEL = {
    'business': 'manager_top',
    'manager_top': 'manager',
    'manager': 'network',
    'network': 'category',
    'category': 'sku',
    'tmc_group': 'sku',
}

FULL_VIEW_NEXT_LEVEL = dict(DEFAULT_NEXT_LEVEL)


HIERARCHY_ORDER = [
    "business",
    "manager_top",
    "manager",
    "network",
    "category",
    "tmc_group",
    "sku",
]


def _resolve_next_level_from_payload(scope_level: Optional[str], payload: Optional[Dict[str, Any]] = None) -> Optional[str]:
    data = payload if isinstance(payload, dict) else {}
    explicit = data.get('children_level') or data.get('next_level')
    if explicit:
        return explicit
    if scope_level == 'network':
        grouping_type = data.get('grouping_type') or data.get('aggregation_level')
        if grouping_type in {'category', 'tmc_group'}:
            return grouping_type
    return DEFAULT_NEXT_LEVEL.get(scope_level)


def _coerce_target_level(scope_level: Optional[str], target_level: Optional[str], payload: Optional[Dict[str, Any]] = None) -> Optional[str]:
    if not target_level:
        return None
    if not scope_level:
        return target_level

    next_level = _resolve_next_level_from_payload(scope_level, payload)
    if not next_level:
        return target_level

    # Never skip mandatory hierarchy.
    # business -> manager_top -> manager -> network -> category/tmc_group -> sku
    if scope_level in {'business', 'manager_top', 'manager'} and target_level != next_level:
        return next_level

    if scope_level == 'network':
        if target_level in {'sku', 'category', 'tmc_group'}:
            return next_level
        return next_level if target_level != next_level else target_level

    if scope_level in {'category', 'tmc_group'} and target_level != 'sku':
        return 'sku'

    return target_level

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




VOICE_MANAGEMENT_PREFIXES = (
    'покажи', 'где', 'какие', 'какой', 'какая', 'разбери', 'проанализируй',
    'что делать', 'сформируй', 'найди', 'кто', 'лучшие', 'худшие'
)

VOICE_REASON_KEYWORDS = {
    'ретро': 'ретро',
    'логистик': 'логистика',
    'персонал': 'персонал',
    'проч': 'прочие',
    'нацен': 'наценка',
    'марж': 'маржа',
    'причин': 'причины',
}

VOICE_OBJECT_ALIASES = {
    'труш': 'Труш Максим',
    'труша': 'Труш Максим',
    'трушу': 'Труш Максим',
    'оптторг': 'Оптторг - 15',
    'оптторг-15': 'Оптторг - 15',
    'оптторг 15': 'Оптторг - 15',
}


def _voice_clean_text(value: Any) -> str:
    return str(value or '').replace('?', ' ').replace('.', ' ').replace(',', ' ').replace('!', ' ').strip()


def _voice_title(value: str) -> str:
    value = _voice_clean_text(value)
    norm = normalize_entity_text(value)
    if not norm:
        return ''
    for key, canonical in VOICE_OBJECT_ALIASES.items():
        if key in norm:
            return canonical
    return value[:1].upper() + value[1:]


def _extract_voice_object(message: str, intent_type: str) -> str:
    text = _voice_clean_text(message)
    norm = normalize_entity_text(text)

    if intent_type == 'action_request' and ' по ' in f' {norm} ':
        tail = norm.split(' по ', 1)[1]
        return _voice_title(tail)

    for prefix in ('разбери', 'проанализируй'):
        if norm.startswith(prefix):
            return _voice_title(norm[len(prefix):].strip())

    # object/rating/reason requests may not have a concrete object yet.
    return ''


def _detect_voice_management_intent(message: str, normalized: str) -> Optional[Dict[str, Any]]:
    """Classify free managerial requests without executing analysis.

    Stage 5.0 creates only a routing/diagnostic layer. It must not change
    navigation, all_block, calculations, reasons, state or benchmark logic.
    """
    raw_normalized = normalize_entity_text(message)
    intent_text = raw_normalized or normalized
    if not normalized and not raw_normalized:
        return None
    if normalized.isdigit() or normalized in FULL_VIEW_COMMANDS or normalized in BACK_COMMANDS:
        return None
    if normalized in {'причины', 'причина', 'разбор', 'разбор причин', 'начать анализ', 'start analysis'}:
        return None
    if _is_short_command(normalized) or _is_full_reasons_command(normalized) or _is_full_view_command(normalized):
        return None

    intent_type = None
    rating_level_words = ('менедж', 'сет', 'мереж', 'sku', 'скю', 'товар', 'позици')
    rating_words = ('рейтинг', 'топ', 'лучшие', 'лучших', 'худшие', 'худших', 'просад', 'паден', 'низкая', 'низкие', 'высокая', 'высокие')

    if intent_text.startswith(('разбери ', 'проанализируй ')) or normalized.startswith(('разбери ', 'проанализируй ')):
        intent_type = 'object_analysis'
    elif intent_text.startswith('что делать') or normalized.startswith('что делать'):
        intent_type = 'action_request'
    elif (intent_text.startswith(('сформируй', 'создай')) or normalized.startswith(('сформируй', 'создай'))) and any(word in intent_text for word in ('задач', 'план', 'действ')):
        intent_type = 'task_request'
    elif any(word in normalized for word in rating_words) and any(word in normalized for word in rating_level_words):
        intent_type = 'rating_request'
    elif 'рейтинг' in normalized or 'топ' in normalized or normalized.startswith(('лучшие', 'худшие')):
        intent_type = 'rating_request'
    elif (intent_text.startswith(('где', 'покажи', 'найди', 'какие', 'какой', 'какая')) or normalized.startswith(('где', 'покажи', 'найди', 'какие', 'какой', 'какая'))) and any(k in intent_text for k in VOICE_REASON_KEYWORDS):
        intent_type = 'reason_request'
    elif intent_text.startswith(VOICE_MANAGEMENT_PREFIXES) or normalized.startswith(VOICE_MANAGEMENT_PREFIXES):
        # Keep a safe generic bucket for managerial free text. The diagnostic
        # response is explicit that full analysis is not enabled yet.
        intent_type = 'object_analysis'

    if not intent_type:
        return None

    obj = _extract_voice_object(message, intent_type)
    reason = ''
    for key, label in VOICE_REASON_KEYWORDS.items():
        if key in normalized:
            reason = label
            break

    return {
        'intent_type': intent_type,
        'object_name': obj,
        'reason_name': reason,
    }


def _build_voice_management_diagnostic(message: str, intent: Dict[str, Any]) -> Dict[str, Any]:
    intent_type = intent.get('intent_type') or 'unknown'
    object_name = intent.get('object_name') or ''
    reason_name = intent.get('reason_name') or ''

    lines = [
        f'Распознан режим: {intent_type}',
    ]
    if object_name:
        lines.append(f'Объект: {object_name}')
    if reason_name:
        lines.append(f'Причина: {reason_name}')
    lines.extend([
        '',
        'Voice Management Layer подготовлен.',
        'Полный свободный анализ будет подключён на следующих этапах.',
    ])

    return {
        'status': 'ok',
        'context': {'level': 'voice_management', 'object_name': 'Свободный управленческий запрос', 'period': None, 'parent_object': None},
        'path': ['Voice Management Layer'],
        'render_mode': 'voice_diagnostic',
        'summary_block': 'Свободный управленческий запрос распознан.',
        'kpi_block': lines,
        'structure_block': [],
        'main_driver': '',
        'drain_block_render': [],
        'drain_total': 0,
        'all_block': [],
        'navigation_block': [],
        'explanation_block': [f'Запрос: {message}'],
        'next_step_block': ['Следующий шаг: подключить обработчик этого intent на следующих этапах.'],
    }



VOICE_RATING_LEVEL_LABELS = {
    'manager_top': 'топ-менеджеры',
    'manager': 'менеджеры',
    'network': 'сети',
    'sku': 'SKU',
}

VOICE_RATING_FIELD_BY_LEVEL = {
    'manager_top': 'manager_top',
    'manager': 'manager',
    'network': 'network',
    'sku': 'sku',
}

VOICE_RATING_SUMMARY_BY_LEVEL = {
    'manager_top': get_manager_top_summary,
    'manager': get_manager_summary,
    'network': get_network_summary,
    'sku': get_sku_summary,
}

VOICE_RATING_TITLE_LABELS = {
    'manager_top': {'plain': 'топ-менеджеры', 'genitive': 'топ-менеджеров'},
    'manager': {'plain': 'менеджеры', 'genitive': 'менеджеров'},
    'network': {'plain': 'сети', 'genitive': 'сетей'},
    'sku': {'plain': 'SKU', 'genitive': 'SKU'},
}


def _voice_num(value: Any) -> float:
    try:
        return float(value or 0.0)
    except Exception:
        return 0.0


def _voice_fmt_money(value: Any, signed: bool = False) -> str:
    num = int(round(_voice_num(value)))
    if signed:
        if num > 0:
            return f"+{num:,}".replace(',', ' ')
        if num < 0:
            return f"−{abs(num):,}".replace(',', ' ')
    if num < 0:
        return f"−{abs(num):,}".replace(',', ' ')
    return f"{num:,}".replace(',', ' ')


def _voice_fmt_percent(value: Any) -> str:
    return f"{_voice_num(value):.2f}%"


def _voice_fmt_pp(value: Any, signed: bool = True) -> str:
    num = _voice_num(value)
    sign = '+' if signed and num > 0 else '−' if signed and num < 0 else ''
    return f"{sign}{abs(num):.2f} п.п."


def _voice_latest_period(rows: List[Dict[str, Any]]) -> Optional[str]:
    periods = sorted({str(r.get('period') or '') for r in rows if r.get('period')})
    return periods[-1] if periods else None


def _voice_previous_year_period(period: str) -> Optional[str]:
    if not period or not isinstance(period, str) or len(period) != 7 or period[4] != '-':
        return None
    try:
        return f"{int(period[:4]) - 1:04d}-{period[5:7]}"
    except Exception:
        return None


def _voice_period_from_message_or_state(message: str, session_ctx: Optional[Dict[str, Any]], rows: List[Dict[str, Any]]) -> Optional[str]:
    parsed = parse_period_from_text(message)
    if parsed:
        return parsed
    if isinstance(session_ctx, dict):
        for key in ('period_current', 'current_period'):
            value = session_ctx.get(key)
            if value:
                return str(value)
        screen = session_ctx.get('current_screen') or session_ctx.get('last_payload') or {}
        if isinstance(screen, dict):
            ctx = screen.get('context') if isinstance(screen.get('context'), dict) else {}
            value = ctx.get('period') or screen.get('period')
            if value:
                return str(value)
    return _voice_latest_period(rows)




VOICE_OPEN_LEVEL_LABELS = {
    'manager_top': 'Top Manager',
    'manager': 'Manager',
    'network': 'Network',
    'category': 'Product Layer',
    'tmc_group': 'TMC Group',
    'sku': 'SKU',
}


def _voice_compact(value: Any) -> str:
    return ''.join(normalize_fuzzy_text(value).split())


def _voice_level_hint(message: str) -> Optional[str]:
    norm = normalize_entity_text(message)
    if any(token in norm for token in ('топ менеджер', 'топ-менеджер', 'top manager', 'manager top', 'руководитель направления')):
        return 'manager_top'
    if any(token in norm for token in ('как менеджер', 'по менеджеру', 'менеджера', 'manager')) and not any(token in norm for token in ('топ менеджер', 'топ-менеджер')):
        return 'manager'
    if any(token in norm for token in ('сеть ', 'сети ', 'network', 'контракт')):
        return 'network'
    if any(token in norm for token in ('категор', 'product layer', 'продукт')):
        return 'category'
    if any(token in norm for token in ('sku', 'скю', 'товар', 'позици')):
        return 'sku'
    return None




def _voice_business_scope_hint(message: str) -> bool:
    norm = normalize_entity_text(message)
    return any(token in norm for token in (
        'по бизнесу', 'в бизнесе', 'бизнес ', 'бизнесу', 'весь бизнес', 'по всему бизнесу', 'общий бизнес'
    ))

def _voice_entity_candidates(message: str, dictionary: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Return all canonical entity names explicitly mentioned in the message.

    This is intentionally conservative. It is used only for ambiguity checks
    before opening an object from a free voice request.
    """
    msg_norm = normalize_fuzzy_text(message)
    msg_compact = _voice_compact(message)
    level_hint = _voice_level_hint(message)
    candidates: List[Dict[str, Any]] = []

    for level in ['sku', 'network', 'manager', 'manager_top', 'category', 'tmc_group']:
        if level_hint and level != level_hint:
            continue
        for name in _canonical_values(dictionary, level):
            name_norm = normalize_fuzzy_text(name)
            if not name_norm:
                continue
            name_compact = _voice_compact(name)
            if name_norm in msg_norm or name_compact in msg_compact:
                candidates.append({
                    'level': level,
                    'name': name,
                    'score': len(name_compact),
                })

    if not candidates:
        return []

    # Keep the most specific textual match. This prevents a category like
    # "Вода" from stealing a full SKU request containing the same word.
    best = max(c.get('score', 0) for c in candidates)
    return [c for c in candidates if c.get('score', 0) == best]


def _voice_role_clarification(candidates: List[Dict[str, Any]], period: str) -> Dict[str, Any]:
    name = candidates[0].get('name') if candidates else 'объект'
    lines = [f'Объект «{name}» найден на нескольких уровнях.', 'Уточните, какой экран открыть:']
    seen = set()
    idx = 1
    for cand in candidates:
        key = (cand.get('level'), cand.get('name'))
        if key in seen:
            continue
        seen.add(key)
        label = VOICE_OPEN_LEVEL_LABELS.get(cand.get('level'), cand.get('level'))
        lines.append(f'{idx}. {cand.get("name")} как {label}')
        idx += 1
    lines.append('')
    lines.append('Например: «Покажи Труш Максим как топ-менеджера 2026-02» или «Покажи Труш Максим как менеджера 2026-02».')
    return {
        'status': 'ok',
        'context': {'level': 'voice_management', 'object_name': name, 'period': period, 'parent_object': None},
        'path': ['Voice Object Opening Layer'],
        'render_mode': 'voice_diagnostic',
        'summary_block': 'Нужно уточнить уровень объекта.',
        'kpi_block': lines,
        'structure_block': [],
        'main_driver': '',
        'drain_block_render': [],
        'drain_total': 0,
        'all_block': [],
        'navigation_block': ['назад — вверх'],
        'explanation_block': [],
        'next_step_block': ['После уточнения VECTRA откроет полный экран объекта.'],
    }


def _resolve_voice_entity_for_opening(message: str, dictionary: Dict[str, Any], period: str) -> Dict[str, Any]:
    candidates = _voice_entity_candidates(message, dictionary)
    if candidates:
        # Same person/object can exist on different levels. Do not choose
        # arbitrarily unless the user provided a level hint.
        unique_levels = {(c.get('level'), c.get('name')) for c in candidates}
        level_hint = _voice_level_hint(message)
        if len(unique_levels) > 1 and not level_hint:
            return {'clarification': _voice_role_clarification(candidates, period)}
        chosen = candidates[0]
        return {'entity_type': chosen.get('level'), 'entity_name': chosen.get('name')}
    return resolve_entity(message, dictionary)

VOICE_OPEN_PARENT_FIELDS = {
    'manager_top': [],
    'manager': ['manager_top'],
    'network': ['manager_top', 'manager'],
    'category': ['manager_top', 'manager', 'network'],
    'tmc_group': ['manager_top', 'manager', 'network', 'category'],
    'sku': ['manager_top', 'manager', 'network', 'category'],
}


def _voice_row_value(row: Dict[str, Any], field: str) -> str:
    return str(row.get(field) or '').strip()


def _voice_same_entity(left: Any, right: Any) -> bool:
    return normalize_entity_text(left) == normalize_entity_text(right)


def _voice_period_rows(rows: List[Dict[str, Any]], period: str) -> List[Dict[str, Any]]:
    period_rows, _ = filter_rows(rows, period=period)
    return period_rows


def _voice_matching_rows(rows: List[Dict[str, Any]], level: str, object_name: str, period: str) -> List[Dict[str, Any]]:
    period_rows = _voice_period_rows(rows, period)
    return [row for row in period_rows if _voice_same_entity(row.get(level), object_name)]


def _voice_filter_matches_rows(base_filter: Dict[str, Any], rows: List[Dict[str, Any]]) -> bool:
    if not base_filter or not rows:
        return False
    for row in rows:
        ok = True
        for field in ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']:
            value = base_filter.get(field)
            if value and not _voice_same_entity(row.get(field), value):
                ok = False
                break
        if ok:
            return True
    return False


def _voice_group_parent_paths(level: str, rows: List[Dict[str, Any]]) -> Dict[tuple, List[Dict[str, Any]]]:
    parent_fields = VOICE_OPEN_PARENT_FIELDS.get(level, [])
    grouped: Dict[tuple, List[Dict[str, Any]]] = {}
    for row in rows:
        key = tuple(_voice_row_value(row, field) for field in parent_fields)
        grouped.setdefault(key, []).append(row)
    return grouped


def _voice_path_filter_from_key(level: str, object_name: str, period: str, key: tuple) -> Dict[str, Any]:
    payload: Dict[str, Any] = {'period': period}
    for field, value in zip(VOICE_OPEN_PARENT_FIELDS.get(level, []), key):
        if value:
            payload[field] = value
    payload[level] = object_name
    return payload


def _voice_open_clarification(level: str, object_name: str, period: str, grouped: Dict[tuple, List[Dict[str, Any]]]) -> Dict[str, Any]:
    parent_fields = VOICE_OPEN_PARENT_FIELDS.get(level, [])
    variants = []
    for key, chunk in grouped.items():
        metrics = aggregate_metrics(chunk)
        label_parts = [value for value in key if value]
        label = ' → '.join(['Бизнес'] + label_parts + [object_name])
        variants.append((float(metrics.get('revenue') or 0.0), label))
    variants.sort(key=lambda x: x[0], reverse=True)
    lines = [f'Объект «{object_name}» найден в нескольких ветках.', 'Уточните, какую ветку открыть:']
    for idx, (_, label) in enumerate(variants[:10], start=1):
        lines.append(f'{idx}. {label}')
    if parent_fields:
        lines.append('')
        lines.append('Например: укажите менеджера или откройте объект через нужную ветку.')
    return {
        'status': 'ok',
        'context': {'level': 'voice_management', 'object_name': object_name, 'period': period, 'parent_object': None},
        'path': ['Voice Object Opening Layer'],
        'render_mode': 'voice_diagnostic',
        'summary_block': 'Нужно уточнить объект.',
        'kpi_block': lines,
        'structure_block': [],
        'main_driver': '',
        'drain_block_render': [],
        'drain_total': 0,
        'all_block': [],
        'navigation_block': ['назад — вверх'],
        'explanation_block': [],
        'next_step_block': ['После уточнения VECTRA откроет полный экран объекта.'],
    }


def _resolve_voice_open_filter(
    level: str,
    object_name: str,
    period: str,
    rows: List[Dict[str, Any]],
    session_ctx: Optional[Dict[str, Any]] = None,
    message: str = '',
) -> Dict[str, Any]:
    """Resolve a direct voice object request into its real hierarchy path.

    Stage 6.2 must not append a found object to the current State Layer when
    that object does not belong to the active branch. This helper first checks
    whether the active branch is valid for the object; otherwise it rebuilds the
    parent path from DATA for the requested period.
    """
    matches = _voice_matching_rows(rows, level, object_name, period)
    if not matches:
        return {'filter_payload': {'period': period, level: object_name}}

    current_filter = dict((session_ctx or {}).get('filter') or {})
    current_filter.pop('period', None)
    parent_fields = VOICE_OPEN_PARENT_FIELDS.get(level, [])
    has_complete_parent_context = all(current_filter.get(field) for field in parent_fields)

    # BUG-025: Product Layer can be opened as an analytical business-level
    # product view. Example: "Покажи Вода 2026-02" should mean
    # "Вода по бизнесу" when there is no valid Network parent context.
    # If the user is already inside a complete branch (manager_top -> manager
    # -> network), keep contextual opening. If the user explicitly says
    # "по бизнесу", force the business-level product view.
    if level == 'category':
        if _voice_business_scope_hint(message) or not has_complete_parent_context:
            return {'filter_payload': {'period': period, 'category': object_name}}

    if has_complete_parent_context and _voice_filter_matches_rows(current_filter, matches):
        payload = {'period': period}
        for field in parent_fields:
            value = current_filter.get(field)
            if value:
                payload[field] = value
        payload[level] = object_name
        return {'filter_payload': payload}

    grouped = _voice_group_parent_paths(level, matches)
    if len(grouped) == 1:
        key = next(iter(grouped.keys()))
        return {'filter_payload': _voice_path_filter_from_key(level, object_name, period, key)}

    # Several real parent paths exist. Do not invent the manager context. Ask
    # for clarification instead of opening a misleading aggregate or zero page.
    return {'clarification': _voice_open_clarification(level, object_name, period, grouped)}


def _voice_rating_level(normalized: str) -> Optional[str]:
    if any(token in normalized for token in ('sku', 'скю', 'товар', 'позици')):
        return 'sku'
    if any(token in normalized for token in ('сет', 'мереж')):
        return 'network'
    if any(token in normalized for token in ('топ менеджер', 'топ-менеджер', 'дивизион')):
        return 'manager_top'
    if 'менедж' in normalized:
        return 'manager'
    return None


def _voice_is_margin_drop_request(normalized: str) -> bool:
    return 'марж' in normalized and any(token in normalized for token in (
        'просад', 'просели', 'паден', 'упала', 'упали', 'хуже к прошлому', 'снизил', 'снизилась'
    ))


def _voice_rating_metric(normalized: str) -> Dict[str, Any]:
    if _voice_is_margin_drop_request(normalized):
        return {'key': 'delta_margin_pre_pp', 'label': 'просадке маржи', 'kind': 'delta_margin'}
    if 'марж' in normalized:
        return {'key': 'margin_pre', 'label': 'марже', 'kind': 'percent'}
    if 'нацен' in normalized:
        return {'key': 'markup', 'label': 'наценке', 'kind': 'percent_markup'}
    if any(token in normalized for token in ('потенциал', 'возможност', 'где деньги')):
        return {'key': 'opportunity_money', 'label': 'потенциалу прибыли', 'kind': 'money'}
    if any(token in normalized for token in ('оборот', 'выруч', 'продаж')):
        return {'key': 'revenue', 'label': 'обороту', 'kind': 'money'}
    if any(token in normalized for token in ('прибыл', 'финрез', 'финансовый результат', 'результат')):
        return {'key': 'finrez_pre', 'label': 'вкладу в результат бизнеса', 'kind': 'money'}
    return {'key': 'finrez_pre', 'label': 'вкладу в результат бизнеса', 'kind': 'money'}


def _voice_rating_direction(normalized: str, metric_key: str) -> str:
    if metric_key == 'delta_margin_pre_pp':
        return 'asc'
    if any(token in normalized for token in ('худш', 'слаб', 'низк', 'минимальн', 'аутсайдер')):
        return 'asc'
    if any(token in normalized for token in ('лучш', 'топ', 'рейтинг', 'высок')):
        return 'desc'
    return 'desc'


def _voice_has_valid_percent_base(metrics: Dict[str, Any], metric_key: str) -> bool:
    if metric_key == 'margin_pre':
        return _voice_num(metrics.get('revenue')) > 0
    if metric_key == 'markup':
        return _voice_num(metrics.get('cost')) > 0
    return True


def _voice_percent_anomaly(metrics: Dict[str, Any], metric_key: str) -> bool:
    # Margin uses the exact VECTRA Engine value. Values >= 100% indicate
    # invalid/degenerate base for a margin rating and must not be presented as
    # a normal result. Markup may legitimately exceed 100%, so do not apply
    # this guard to markup.
    if metric_key == 'margin_pre' and abs(_voice_num(metrics.get('margin_pre'))) >= 100:
        return True
    return False


def _voice_object_summary(level: str, object_name: str, period: str) -> Dict[str, Any]:
    summary_fn = VOICE_RATING_SUMMARY_BY_LEVEL.get(level)
    if not summary_fn:
        return {}
    try:
        payload = summary_fn(object_name, period)
        return payload if isinstance(payload, dict) else {}
    except Exception as exc:
        logger.warning('Voice rating summary failed for %s=%s period=%s: %s', level, object_name, period, exc)
        return {}


def _voice_metrics_from_engine(level: str, object_name: str, period: str) -> Dict[str, Any]:
    payload = _voice_object_summary(level, object_name, period)
    metrics = payload.get('metrics_raw') if isinstance(payload.get('metrics_raw'), dict) else {}
    if metrics:
        return metrics
    # Last-resort empty dict. Do not calculate alternative percent metrics here:
    # Voice ratings must use the same VECTRA Engine metrics as object screens.
    return {}


def _voice_previous_metrics_from_engine(level: str, object_name: str, period: str) -> Dict[str, Any]:
    payload = _voice_object_summary(level, object_name, period)
    metrics = payload.get('previous_object_metrics') if isinstance(payload.get('previous_object_metrics'), dict) else {}
    return metrics or {}



def _voice_log_rating_metrics(source: str, object_name: str, metrics: Dict[str, Any], previous_metrics: Optional[Dict[str, Any]] = None) -> None:
    logger.debug(
        'voice_rating_metrics source=%s object=%s network=%s revenue=%s cost=%s finrez_pre=%s margin_pre=%s markup=%s delta_margin_pre_pp=%s',
        source,
        object_name,
        object_name,
        metrics.get('revenue'),
        metrics.get('cost'),
        metrics.get('finrez_pre'),
        metrics.get('margin_pre'),
        metrics.get('markup'),
        None if not previous_metrics else round(_voice_num(metrics.get('margin_pre')) - _voice_num(previous_metrics.get('margin_pre')), 2),
    )


def _voice_group_map(rows: List[Dict[str, Any]], group_field: str) -> Dict[str, List[Dict[str, Any]]]:
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in rows:
        name = str(row.get(group_field) or '').strip()
        if not name or name.lower() in {'total', 'итого', 'unknown'}:
            continue
        grouped.setdefault(name, []).append(row)
    return grouped


def _voice_group_rating_rows(
    rows: List[Dict[str, Any]],
    group_field: str,
    metric_spec: Dict[str, Any],
    direction: str,
    limit: int = 10,
    previous_rows: Optional[List[Dict[str, Any]]] = None,
    level: Optional[str] = None,
    period: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Build Voice ratings with single-pass aggregation.

    BUG-022: Voice Rating must not build full object screens, reasons,
    explanations, navigation, or drilldown for every object in a rating. It
    uses the same core aggregate_metrics helper as domain summaries, but only
    once per group in the already-loaded period rows.
    """
    metric_key = metric_spec.get('key') or 'finrez_pre'
    kind = metric_spec.get('kind') or 'money'
    grouped = _voice_group_map(rows, group_field)
    previous_grouped = _voice_group_map(previous_rows or [], group_field)

    items: List[Dict[str, Any]] = []
    for name, chunk in grouped.items():
        metrics = aggregate_metrics(chunk)
        previous_chunk = previous_grouped.get(name) or []
        previous_metrics = aggregate_metrics(previous_chunk) if previous_chunk else {}

        if not metrics:
            continue

        if kind == 'delta_margin':
            if not previous_metrics or _voice_num(previous_metrics.get('revenue')) <= 0 or _voice_num(metrics.get('revenue')) <= 0:
                continue
            current_margin = _voice_num(metrics.get('margin_pre'))
            previous_margin = _voice_num(previous_metrics.get('margin_pre'))
            if abs(current_margin) >= 100 or abs(previous_margin) >= 100:
                continue
            value = current_margin - previous_margin
            # A просадка rating should show actual drops only. Growth belongs to
            # a separate "best dynamics" request, not to margin decline.
            if value >= 0:
                continue
        else:
            if kind.startswith('percent'):
                if not _voice_has_valid_percent_base(metrics, metric_key) or _voice_percent_anomaly(metrics, metric_key):
                    continue
            value = _voice_num(metrics.get(metric_key))

        # Do not present absent percentage data as 0.00%.
        if kind.startswith('percent') and value == 0 and not metrics.get(metric_key):
            continue

        items.append({
            'object_name': name,
            'metric_value': value,
            'metrics': metrics,
            'previous_metrics': previous_metrics,
        })

    reverse = direction == 'desc'
    if metric_spec.get('kind') == 'delta_margin':
        # Most negative first: -8.2, -6.4, -4.1.
        items.sort(key=lambda x: (x.get('metric_value') or 0.0, x.get('object_name') or ''))
    else:
        items.sort(key=lambda x: (x.get('metric_value') or 0.0, x.get('object_name') or ''), reverse=reverse)
    return items[:limit]


def _voice_rating_title(level: str, metric_spec: Dict[str, Any], direction: str, normalized: str, limit: int) -> str:
    label = metric_spec.get('label') or 'результату'
    labels = VOICE_RATING_TITLE_LABELS.get(level, {'plain': 'объекты', 'genitive': 'объектов'})
    plain_label = labels.get('plain') or 'объекты'
    genitive_label = labels.get('genitive') or plain_label
    if metric_spec.get('kind') == 'delta_margin':
        return f'Топ {limit} {genitive_label} по просадке маржи'
    if 'худш' in normalized:
        prefix = 'Худшие'
        level_label = plain_label
    elif 'лучш' in normalized:
        prefix = 'Лучшие'
        level_label = plain_label
    elif 'топ' in normalized:
        prefix = f'Топ {limit}'
        level_label = genitive_label
    else:
        prefix = 'Рейтинг'
        level_label = genitive_label
    return f'{prefix} {level_label} по {label}'


def _voice_render_rating_item(idx: int, item: Dict[str, Any], metric_spec: Dict[str, Any]) -> List[str]:
    name = item.get('object_name') or 'Без названия'
    metric_key = metric_spec.get('key') or 'finrez_pre'
    kind = metric_spec.get('kind') or 'money'
    metrics = item.get('metrics') if isinstance(item.get('metrics'), dict) else {}
    previous_metrics = item.get('previous_metrics') if isinstance(item.get('previous_metrics'), dict) else {}
    value = item.get('metric_value')

    if kind == 'delta_margin':
        return [
            f'{idx}. {name}',
            f'маржа: {_voice_fmt_percent(metrics.get("margin_pre"))}',
            f'прошлый год: {_voice_fmt_percent(previous_metrics.get("margin_pre"))}',
            f'Δ: {_voice_fmt_pp(value)}',
        ]
    if kind.startswith('percent'):
        return [f'{idx}. {name} → {_voice_fmt_percent(value)}']
    return [f'{idx}. {name} → {_voice_fmt_money(value, signed=True)}']


def _voice_rating_summary(title: str, items: List[Dict[str, Any]], metric_spec: Dict[str, Any], direction: str) -> List[str]:
    if not items:
        return []
    first = items[0].get('object_name') or 'первый объект'
    second = items[1].get('object_name') if len(items) > 1 else ''
    names = f'{first} и {second}' if second else first
    kind = metric_spec.get('kind')
    if kind == 'delta_margin':
        summary = f'Наибольшая просадка маржи наблюдается у объектов {names}.'
        next_step = 'Следующий шаг: открыть объект с максимальной просадкой и проверить причины.'
    elif metric_spec.get('key') == 'finrez_pre':
        if direction == 'asc':
            summary = f'Минимальный вклад в результат бизнеса сейчас дают {names}.'
            next_step = 'Следующий шаг: разобрать худший объект и определить основной источник потерь.'
        else:
            summary = f'Основной вклад в результат бизнеса формируют {names}.'
            next_step = 'Следующий шаг: разобрать лидеров, чтобы понять сильные практики, или запросить худших менеджеров для поиска зоны риска.'
    else:
        summary = f'Первые позиции рейтинга: {names}.'
        next_step = 'Следующий шаг: открыть объект из рейтинга и проверить причины отклонения.'
    return [summary, '', next_step]




# ---------------------------------------------------------------------------
# STAGE 6.3 — VOICE ANALYTICAL LAYER / PHASE 1
# Intent by Meaning + Context First
# ---------------------------------------------------------------------------

ANALYTICAL_INTENT_LABELS = {
    'explain_current_object': 'Объяснение текущего объекта',
    'find_money': 'Где находятся деньги',
    'next_best_action': 'Что делать дальше',
    'teach_me': 'Объяснение логики VECTRA',
}


def _analytical_norm(message: str) -> str:
    return normalize_entity_text(message)


def _detect_voice_analytical_intent(message: str, normalized: str) -> Optional[Dict[str, Any]]:
    """Detect analytical intent by meaning, not by a single command phrase.

    Stage 6.3 deliberately treats user language as flexible. The user may say
    "почему", "раскрой", "странно", "куда копать" or "что делать". These are
    not separate commands; they are managerial meanings mapped to stable
    analytical intents.
    """
    text = _analytical_norm(message) or normalized
    if not text:
        return None
    if text.isdigit() or text in FULL_VIEW_COMMANDS or text in BACK_COMMANDS:
        return None
    if text in {'причины', 'причина', 'разбор', 'разбор причин', 'начать анализ', 'start analysis'}:
        return None

    teach_tokens = (
        'что такое потенциал', 'что означает потенциал', 'почему потенциал',
        'что такое вклад', 'что означает вклад', 'почему вклад', 'как считается вклад',
        'как считается потенциал', 'почему сравниваем', 'почему сравнение',
        'что означает этот показатель', 'что значит этот показатель', 'объясни показатель',
        'как считается', 'что означает', 'что значит'
    )
    action_tokens = (
        'что делать', 'что рекомендуешь', 'с чего начать', 'куда смотреть',
        'на что повлиять', 'как исправить', 'как вернуть деньги', 'где быстро исправить',
        'какой следующий шаг', 'следующий шаг'
    )
    money_tokens = (
        'где деньги', 'где лежат деньги', 'где резерв', 'где основной резерв',
        'где потенциал', 'где основной потенциал', 'куда копать', 'где теряем',
        'где теряется', 'кто тянет вниз', 'что сильнее всего влияет', 'что тянет вниз'
    )
    explain_tokens = (
        'почему', 'объясни', 'раскрой', 'покажи смысл', 'смысл', 'что произошло',
        'за счет чего', 'за счёт чего', 'в чем проблема', 'в чём проблема',
        'что не так', 'странно', 'не понял', 'не понимаю', 'разберись', 'разъясни'
    )

    if any(token in text for token in teach_tokens):
        return {'intent_type': 'teach_me'}
    if any(token in text for token in action_tokens):
        return {'intent_type': 'next_best_action'}
    if any(token in text for token in money_tokens):
        return {'intent_type': 'find_money'}
    if any(token in text for token in explain_tokens):
        return {'intent_type': 'explain_current_object'}
    return None


def _analytical_current_screen(session_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not isinstance(session_ctx, dict):
        return {}
    screen = session_ctx.get('current_screen') or session_ctx.get('last_payload') or {}
    return screen if isinstance(screen, dict) else {}


def _analytical_context(screen: Dict[str, Any]) -> Dict[str, Any]:
    ctx = screen.get('context') if isinstance(screen.get('context'), dict) else {}
    return {
        'level': str(ctx.get('level') or screen.get('level') or '').strip(),
        'object_name': str(ctx.get('object_name') or screen.get('object_name') or '').strip(),
        'period': ctx.get('period') or screen.get('period'),
    }


def _analytical_level_label(level: str) -> str:
    return {
        'business': 'Бизнес',
        'manager_top': 'Top Manager',
        'manager': 'Manager',
        'network': 'Contract Layer / сеть',
        'category': 'Product Layer / категория',
        'tmc_group': 'TMC Group',
        'sku': 'SKU',
    }.get(str(level or '').strip(), str(level or 'объект'))


def _analytical_metric(screen: Dict[str, Any], name: str) -> Dict[str, Any]:
    wanted = str(name or '').strip().lower()
    for item in screen.get('metrics') or []:
        if isinstance(item, dict) and str(item.get('name') or '').strip().lower() == wanted:
            return item
    return {}


def _analytical_money(value: Any, signed: bool = False) -> str:
    return f'{_voice_fmt_money(value, signed=signed)} грн'


def _analytical_percent(value: Any) -> str:
    try:
        return f'{float(value):.2f}%'
    except Exception:
        return '—'


def _analytical_pp(value: Any) -> str:
    return _voice_fmt_pp(value)


def _analytical_reasons(screen: Dict[str, Any]) -> List[Dict[str, Any]]:
    for key in ('reasons_block', 'object_reasons', 'business_reasons'):
        raw = screen.get(key)
        if isinstance(raw, list) and raw:
            return [r for r in raw if isinstance(r, dict)]
    return []


def _analytical_reason_name(reason: Dict[str, Any]) -> str:
    return str(reason.get('name') or 'Причина').strip()


def _analytical_reason_effect(reason: Dict[str, Any]) -> float:
    try:
        return float(reason.get('effect_money') or 0.0)
    except Exception:
        return 0.0


def _analytical_top_negative_reasons(screen: Dict[str, Any], limit: int = 3) -> List[Dict[str, Any]]:
    negatives = [r for r in _analytical_reasons(screen) if _analytical_reason_effect(r) < 0]
    negatives.sort(key=lambda r: _analytical_reason_effect(r))
    return negatives[:limit]


def _analytical_top_positive_reasons(screen: Dict[str, Any], limit: int = 2) -> List[Dict[str, Any]]:
    positives = [r for r in _analytical_reasons(screen) if _analytical_reason_effect(r) > 0]
    positives.sort(key=lambda r: _analytical_reason_effect(r), reverse=True)
    return positives[:limit]


def _analytical_next_items(screen: Dict[str, Any], limit: int = 3) -> List[Dict[str, Any]]:
    raw = screen.get('all_block') if isinstance(screen.get('all_block'), list) else []
    items = [x for x in raw if isinstance(x, dict)]
    return items[:limit]


def _analytical_item_money(item: Dict[str, Any]) -> Any:
    for key in ('navigation_money', 'potential_money', 'gap_loss_money', 'effect_money'):
        if item.get(key) is not None:
            value = item.get(key)
            if key == 'effect_money':
                try:
                    return abs(float(value))
                except Exception:
                    return value
            return value
    return 0


def _analytical_priority_lines(screen: Dict[str, Any]) -> List[str]:
    for key in ('priority_action_block', 'decision_block_render', 'next_step_block'):
        raw = screen.get(key)
        if isinstance(raw, list) and raw:
            return [str(x) for x in raw if str(x).strip()]
    action = screen.get('priority_action') if isinstance(screen.get('priority_action'), dict) else {}
    if action:
        label = action.get('text') or action.get('action') or 'Приоритетное действие'
        effect = action.get('expected_effect_money') or action.get('effect_money')
        return [f'{label} → ожидаемый эффект {_analytical_money(effect, signed=True)}']
    return []


def _analytical_response_shell(screen: Dict[str, Any], intent_type: str, lines: List[str], next_steps: Optional[List[str]] = None) -> Dict[str, Any]:
    ctx = _analytical_context(screen)
    title = ANALYTICAL_INTENT_LABELS.get(intent_type, 'Аналитический ответ')
    path = list(screen.get('path') or [])
    if not path:
        object_name = ctx.get('object_name') or 'Текущий объект'
        path = [object_name]
    path = path + ['Аналитика']
    return {
        'status': 'ok',
        'context': {
            'level': 'voice_management',
            'object_name': title,
            'period': ctx.get('period'),
            'parent_object': ctx.get('object_name'),
        },
        'path': path,
        'render_mode': 'voice_diagnostic',
        'summary_block': title,
        'kpi_block': lines,
        'structure_block': [],
        'main_driver': '',
        'drain_block_render': [],
        'drain_total': 0,
        'all_block': [],
        'navigation_block': ['назад — вверх'],
        'explanation_block': [],
        'next_step_block': next_steps or [],
    }


def _build_analytical_no_context_response(message: str) -> Dict[str, Any]:
    return {
        'status': 'ok',
        'context': {'level': 'voice_management', 'object_name': 'Аналитический запрос', 'period': None, 'parent_object': None},
        'path': ['Voice Analytical Layer'],
        'render_mode': 'voice_diagnostic',
        'summary_block': 'Нужен активный объект для анализа.',
        'kpi_block': [
            'Я понял аналитический запрос, но сейчас нет открытого объекта.',
            '',
            'Сначала откройте объект, например:',
            'Покажи Труш Максим как топ-менеджера 2026-02',
            'Покажи РУКАВИЧКА 2026-02',
        ],
        'structure_block': [],
        'main_driver': '',
        'drain_block_render': [],
        'drain_total': 0,
        'all_block': [],
        'navigation_block': [],
        'explanation_block': [f'Запрос: {message}'],
        'next_step_block': ['После открытия объекта можно спросить: «почему?», «где деньги?», «что делать?».'],
    }


def _build_explain_current_object_response(screen: Dict[str, Any]) -> Dict[str, Any]:
    ctx = _analytical_context(screen)
    level = ctx.get('level')
    object_name = ctx.get('object_name') or 'текущий объект'
    result_money = screen.get('business_result_money') if level == 'business' else screen.get('object_result_money')
    opportunity = screen.get('opportunity_money')
    margin = _analytical_metric(screen, 'Маржа')
    markup = _analytical_metric(screen, 'Наценка')
    negatives = _analytical_top_negative_reasons(screen, 3)
    positives = _analytical_top_positive_reasons(screen, 2)

    lines: List[str] = [
        '📍 Что произошло',
        f'Объект: {object_name}. Уровень: {_analytical_level_label(level)}.',
    ]
    if result_money is not None:
        label = 'Результат бизнеса' if level == 'business' else 'Вклад в результат бизнеса'
        lines.append(f'{label}: {_analytical_money(result_money, signed=True)}.')
    if opportunity is not None and level != 'business':
        lines.append(f'Потенциал прибыли внутри объекта: {_analytical_money(abs(_voice_num(opportunity)))}.')
    if screen.get('summary_block'):
        lines.extend(['', '📌 Краткий смысл', str(screen.get('summary_block'))])

    lines.extend(['', '📊 С чем сравниваем'])
    if level == 'business':
        lines.append('Business Screen сравнивается только с прошлым годом. Это показывает, что изменилось в бизнесе за год.')
    else:
        lines.append('VECTRA использует два сравнения: с прошлым годом и со средним уровнем бизнеса.')
        lines.append('Средний уровень бизнеса — это фактически сложившаяся эффективность компании за выбранный период.')
        if margin:
            lines.append(f'Маржа объекта: {_analytical_percent(margin.get("fact_percent"))}; изменение к прошлому году: {_analytical_pp(margin.get("delta_percent"))}.')
        if markup:
            lines.append(f'Наценка объекта: {_analytical_percent(markup.get("fact_percent"))}; изменение к прошлому году: {_analytical_pp(markup.get("delta_percent"))}.')

    lines.extend(['', '📉 Главные причины'])
    if negatives:
        for reason in negatives:
            name = _analytical_reason_name(reason)
            effect = _analytical_reason_effect(reason)
            base = reason.get('base_percent')
            fact = reason.get('percent') or reason.get('value_percent') or reason.get('fact_percent')
            if level == 'business':
                lines.append(f'{name}: эффект {_analytical_money(effect, signed=True)}.')
            else:
                lines.append(f'{name}: факт {_analytical_percent(fact)}, средний уровень бизнеса {_analytical_percent(base)}, эффект {_analytical_money(effect, signed=True)}.')
    else:
        lines.append('Критичный отрицательный фактор по доступным причинам не выделен.')
    if positives:
        strong = positives[0]
        lines.append(f'Главная сильная сторона: {_analytical_reason_name(strong)} ({_analytical_money(_analytical_reason_effect(strong), signed=True)}).')

    lines.extend(['', '💰 Денежный эффект'])
    if negatives:
        total_loss = sum(abs(_analytical_reason_effect(r)) for r in negatives)
        lines.append(f'Суммарный эффект основных отрицательных причин: {_analytical_money(total_loss)}.')
    elif result_money is not None:
        lines.append(f'Главный денежный ориентир экрана: {_analytical_money(result_money, signed=True)}.')

    lines.extend(['', '📌 Что это означает'])
    if level == 'business':
        lines.append('Это показывает, какие причины изменили общий результат бизнеса к прошлому году и куда нужно направить внимание CEO.')
    else:
        lines.append('Если объект хуже среднего уровня бизнеса по причине, VECTRA переводит отклонение в деньги. Так появляется управленческий эффект для возврата прибыли.')

    next_items = _analytical_next_items(screen, 1)
    next_steps = []
    if next_items:
        next_steps = [f'➡ Следующий шаг: открыть {next_items[0].get("object_name")}, где находится следующий слой потенциала.']
    else:
        next_steps = ['➡ Следующий шаг: перейти к причинам или уточнить показатель, который нужно раскрыть.']
    lines.extend(['', next_steps[0]])
    return _analytical_response_shell(screen, 'explain_current_object', lines, next_steps)


def _build_find_money_response(screen: Dict[str, Any]) -> Dict[str, Any]:
    ctx = _analytical_context(screen)
    object_name = ctx.get('object_name') or 'текущий объект'
    level = ctx.get('level')
    items = _analytical_next_items(screen, 5)
    opportunity = screen.get('opportunity_money')
    if level == 'business':
        items = [x for x in (screen.get('opportunity_rating') or []) if isinstance(x, dict)][:5]

    lines: List[str] = ['📍 Общий потенциал']
    if level == 'business':
        lines.append('На Business Screen VECTRA показывает рейтинг возможностей: где в бизнесе находится основной потенциал прибыли.')
    else:
        lines.append(f'Объект: {object_name}. Потенциал прибыли: {_analytical_money(abs(_voice_num(opportunity)))}.' if opportunity is not None else f'Объект: {object_name}.')

    lines.extend(['', '📊 Основные зоны'])
    if items:
        for idx, item in enumerate(items, start=1):
            name = item.get('object_name') or item.get('name') or 'объект'
            money = item.get('opportunity_money') if level == 'business' else _analytical_item_money(item)
            lines.append(f'{idx}. {name} → {_analytical_money(abs(_voice_num(money)))}')
    else:
        lines.append('Ниже по дереву нет доступного списка объектов. Возможно, это последний уровень детализации.')

    lines.extend(['', '📌 Куда идти дальше'])
    if items:
        first = items[0].get('object_name') or items[0].get('name')
        lines.append(f'Начать лучше с объекта «{first}», потому что он находится выше всего в текущем денежном маршруте.')
        next_steps = [f'➡ Следующий шаг: открыть {first}.']
    else:
        lines.append('Следующий шаг — открыть причины текущего объекта или задать вопрос по конкретному показателю.')
        next_steps = ['➡ Следующий шаг: причины — разбор.']
    lines.extend(['', next_steps[0]])
    return _analytical_response_shell(screen, 'find_money', lines, next_steps)


def _build_next_best_action_response(screen: Dict[str, Any]) -> Dict[str, Any]:
    ctx = _analytical_context(screen)
    object_name = ctx.get('object_name') or 'текущий объект'
    action_lines = _analytical_priority_lines(screen)
    negatives = _analytical_top_negative_reasons(screen, 1)
    lines: List[str] = [
        '📍 Главная возможность',
        f'Объект: {object_name}.',
    ]
    if negatives:
        r = negatives[0]
        lines.append(f'Главный источник потерь: {_analytical_reason_name(r)} ({_analytical_money(_analytical_reason_effect(r), signed=True)}).')
    else:
        lines.append('Критичный источник потерь по доступным причинам не выделен.')

    lines.extend(['', '📌 Действие'])
    if action_lines:
        lines.extend(action_lines)
    else:
        lines.append('Готовое действие в VECTRA Engine для этого уровня не найдено. Новые действия не генерирую без данных.')

    lines.extend(['', '📌 Что это означает'])
    lines.append('VECTRA не принимает решение вместо менеджера. Она показывает главный денежный рычаг и следующий шаг анализа.')

    next_items = _analytical_next_items(screen, 1)
    if next_items:
        first = next_items[0].get('object_name')
        next_steps = [f'➡ Следующий шаг: открыть {first} и проверить, где действие даст максимальный эффект.']
    else:
        next_steps = ['➡ Следующий шаг: перейти к причинам текущего объекта.']
    lines.extend(['', next_steps[0]])
    return _analytical_response_shell(screen, 'next_best_action', lines, next_steps)


def _build_teach_me_response(screen: Dict[str, Any], message: str) -> Dict[str, Any]:
    ctx = _analytical_context(screen)
    object_name = ctx.get('object_name') or 'текущий объект'
    text = _analytical_norm(message)
    lines: List[str] = ['📍 Что означает показатель']

    if 'потенциал' in text:
        lines.append('Потенциал прибыли — это деньги, которые находятся внутри объекта и помогают понять, куда идти дальше для возврата прибыли.')
        lines.append('Это не оценка менеджера и не итоговый результат объекта.')
        value = screen.get('opportunity_money')
        if value is not None:
            lines.append(f'На текущем экране потенциал прибыли: {_analytical_money(abs(_voice_num(value)))}.')
    elif 'вклад' in text or 'результат' in text:
        lines.append('Вклад в результат бизнеса показывает, лучше или хуже объект работает относительно среднего уровня бизнеса.')
        lines.append('Если вклад отрицательный — объект недозарабатывает относительно бизнеса. Если положительный — объект создаёт дополнительный результат.')
        value = screen.get('object_result_money')
        if value is not None:
            lines.append(f'На текущем экране вклад объекта: {_analytical_money(value, signed=True)}.')
    elif 'сравн' in text or 'бизнес' in text:
        lines.append('VECTRA сравнивает объект со средним уровнем бизнеса, потому что бизнес — это фактически сложившаяся эффективность компании за выбранный период.')
        lines.append('Это не план, не бюджет и не норматив. Это реальный уровень компании, с которым сравнивается объект.')
    else:
        lines.append(f'Текущий объект: {object_name}. VECTRA объясняет его через KPI, сравнение с прошлым годом, сравнение со средним уровнем бизнеса, причины и денежный эффект.')

    lines.extend(['', '📊 Как считается'])
    lines.append('1. DATA фильтруется по текущему объекту и периоду.')
    lines.append('2. VECTRA Engine агрегирует KPI и причины.')
    lines.append('3. Объект сравнивается с прошлым годом и, если это не Business Screen, со средним уровнем бизнеса.')
    lines.append('4. Отклонение переводится в денежный эффект.')

    lines.extend(['', '📌 Что это означает'])
    lines.append('Смысл не в том, чтобы оценить человека как хорошего или плохого. Смысл — показать, где можно вернуть прибыль и куда двигаться дальше.')
    next_steps = ['➡ Следующий шаг: спросить «где деньги?» или «что делать?», чтобы перейти от объяснения к действию.']
    lines.extend(['', next_steps[0]])
    return _analytical_response_shell(screen, 'teach_me', lines, next_steps)



# ---------------------------------------------------------------------------
# STAGE 6.3 — PHASE 1.1
# Analytical Response Builder
# ---------------------------------------------------------------------------

ANALYTICAL_BUILDER_SECTIONS = (
    '📍 Что произошло',
    '📊 С чем сравниваем',
    '📉 Главные причины',
    '💰 Денежный эффект',
    '📌 Что это означает',
    '➡ Следующий шаг',
)


def _analytical_reason_percent(reason: Dict[str, Any]) -> Any:
    for key in ('percent', 'value_percent', 'fact_percent'):
        if reason.get(key) is not None:
            return reason.get(key)
    return None


def _analytical_reason_base_percent(reason: Dict[str, Any]) -> Any:
    for key in ('base_percent', 'business_percent', 'benchmark_percent'):
        if reason.get(key) is not None:
            return reason.get(key)
    return None


def _analytical_reason_gap_pp(reason: Dict[str, Any]) -> Optional[float]:
    try:
        fact = round(float(_analytical_reason_percent(reason)), 6)
    except Exception:
        fact = None
    try:
        base = round(float(_analytical_reason_base_percent(reason)), 6)
    except Exception:
        base = None
    if fact is None or base is None:
        return None
    return round(fact - base, 2)


def _analytical_main_negative_reason(screen: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    negatives = _analytical_top_negative_reasons(screen, 1)
    return negatives[0] if negatives else None


def _analytical_screen_result_money(screen: Dict[str, Any], level: str) -> Any:
    return screen.get('business_result_money') if level == 'business' else screen.get('object_result_money')


def _analytical_builder_next_step(screen: Dict[str, Any], intent_type: str) -> str:
    items = _analytical_next_items(screen, 1)
    if intent_type == 'next_best_action':
        if items:
            name = items[0].get('object_name') or items[0].get('name') or 'следующий объект'
            return f'➡ Следующий шаг: открыть {name} и проверить, где действие даст максимальный эффект.'
        return '➡ Следующий шаг: перейти к причинам текущего объекта.'
    if intent_type == 'find_money':
        if items:
            name = items[0].get('object_name') or items[0].get('name') or 'первый объект'
            return f'➡ Следующий шаг: открыть {name}, потому что там находится следующий слой потенциала.'
        return '➡ Следующий шаг: перейти к причинам текущего объекта или уточнить показатель.'
    if intent_type == 'teach_me':
        return '➡ Следующий шаг: спросить «где деньги?» или «что делать?», чтобы перейти от объяснения к действию.'
    if items:
        name = items[0].get('object_name') or items[0].get('name') or 'следующий объект'
        return f'➡ Следующий шаг: открыть {name}, где находится следующий слой потенциала.'
    return '➡ Следующий шаг: перейти к причинам или уточнить показатель, который нужно раскрыть.'


def _analytical_builder_what_happened(screen: Dict[str, Any], level: str, object_name: str, intent_type: str, message: str) -> List[str]:
    result_money = _analytical_screen_result_money(screen, level)
    opportunity = screen.get('opportunity_money')
    lines = [f'Объект: {object_name}. Уровень: {_analytical_level_label(level)}.']

    if level == 'business':
        if result_money is not None:
            lines.append(f'Результат бизнеса к прошлому году: {_analytical_money(result_money, signed=True)}.')
        lines.append('Business Screen показывает состояние всего бизнеса и стартовую точку навигации по потенциалу прибыли.')
    else:
        if result_money is not None:
            lines.append(f'Вклад в результат бизнеса: {_analytical_money(result_money, signed=True)}.')
        if opportunity is not None:
            lines.append(f'Потенциал прибыли внутри объекта: {_analytical_money(abs(_voice_num(opportunity)))}.')
        if result_money is not None and _voice_num(result_money) < 0:
            lines.append('Объект недозарабатывает относительно среднего уровня бизнеса.')
        elif result_money is not None and _voice_num(result_money) > 0:
            lines.append('Объект работает лучше среднего уровня бизнеса по текущей модели VECTRA.')

    if intent_type == 'teach_me':
        text = _analytical_norm(message)
        if 'потенциал' in text:
            lines.append('Вы спрашиваете про потенциал прибыли: это деньги внутри объекта, которые показывают, куда идти дальше.')
        elif 'вклад' in text or 'результат' in text:
            lines.append('Вы спрашиваете про вклад: это оценка результата объекта относительно среднего уровня бизнеса.')
        elif 'сравн' in text or 'бизнес' in text:
            lines.append('Вы спрашиваете про сравнение: VECTRA использует бизнес как фактический эталон эффективности периода.')
    return lines


def _analytical_builder_comparison(screen: Dict[str, Any], level: str) -> List[str]:
    lines: List[str] = []
    margin = _analytical_metric(screen, 'Маржа')
    markup = _analytical_metric(screen, 'Наценка')
    main_reason = _analytical_main_negative_reason(screen)

    if level == 'business':
        lines.append('Business Screen сравнивается только с прошлым годом.')
        lines.append('Это показывает, что изменилось в бизнесе за год: оборот, финрез, маржа, наценка и причины бизнеса.')
        return lines

    lines.append('VECTRA использует два сравнения:')
    lines.append('1. С прошлым годом — чтобы понять, что изменилось.')
    lines.append('2. Со средним уровнем бизнеса — чтобы понять, насколько объект эффективен относительно компании.')
    lines.append('Средний уровень бизнеса — это не план и не норматив, а фактически сложившаяся эффективность бизнеса за выбранный период.')

    if margin:
        fact = margin.get('fact_percent')
        delta = margin.get('delta_percent')
        lines.append(f'Маржа объекта: {_analytical_percent(fact)}; изменение к прошлому году: {_analytical_pp(delta)}.')
    if markup:
        fact = markup.get('fact_percent')
        delta = markup.get('delta_percent')
        lines.append(f'Наценка объекта: {_analytical_percent(fact)}; изменение к прошлому году: {_analytical_pp(delta)}.')

    if main_reason:
        name = _analytical_reason_name(main_reason)
        fact = _analytical_reason_percent(main_reason)
        base = _analytical_reason_base_percent(main_reason)
        gap = _analytical_reason_gap_pp(main_reason)
        if fact is not None and base is not None:
            lines.append(f'По главной причине «{name}»: факт объекта {_analytical_percent(fact)}, средний уровень бизнеса {_analytical_percent(base)}.')
            if gap is not None:
                lines.append(f'Отклонение от бизнеса: {_analytical_pp(gap)}.')
    return lines


def _analytical_builder_reasons(screen: Dict[str, Any], level: str) -> List[str]:
    negatives = _analytical_top_negative_reasons(screen, 3)
    positives = _analytical_top_positive_reasons(screen, 1)
    lines: List[str] = []
    if negatives:
        for idx, reason in enumerate(negatives, start=1):
            name = _analytical_reason_name(reason)
            effect = _analytical_reason_effect(reason)
            fact = _analytical_reason_percent(reason)
            base = _analytical_reason_base_percent(reason)
            gap = _analytical_reason_gap_pp(reason)
            if level == 'business':
                lines.append(f'{idx}. {name}: эффект {_analytical_money(effect, signed=True)} к прошлому году.')
            elif fact is not None and base is not None:
                gap_text = f', отклонение {_analytical_pp(gap)}' if gap is not None else ''
                lines.append(f'{idx}. {name}: факт {_analytical_percent(fact)}, бизнес {_analytical_percent(base)}{gap_text}, эффект {_analytical_money(effect, signed=True)}.')
            else:
                lines.append(f'{idx}. {name}: эффект {_analytical_money(effect, signed=True)}.')
    else:
        lines.append('Критичный отрицательный фактор по доступным причинам не выделен.')
    if positives:
        reason = positives[0]
        lines.append(f'Сильная сторона: {_analytical_reason_name(reason)} ({_analytical_money(_analytical_reason_effect(reason), signed=True)}).')
    return lines


def _analytical_builder_money_effect(screen: Dict[str, Any], level: str, intent_type: str) -> List[str]:
    lines: List[str] = []
    negatives = _analytical_top_negative_reasons(screen, 3)
    result_money = _analytical_screen_result_money(screen, level)
    opportunity = screen.get('opportunity_money')

    if intent_type == 'find_money':
        if level == 'business':
            items = [x for x in (screen.get('opportunity_rating') or []) if isinstance(x, dict)][:5]
        else:
            items = _analytical_next_items(screen, 5)
        if opportunity is not None and level != 'business':
            lines.append(f'Потенциал текущего объекта: {_analytical_money(abs(_voice_num(opportunity)))}.')
        if items:
            for idx, item in enumerate(items, start=1):
                name = item.get('object_name') or item.get('name') or 'объект'
                money = item.get('opportunity_money') if level == 'business' else _analytical_item_money(item)
                lines.append(f'{idx}. {name} → {_analytical_money(abs(_voice_num(money)))}')
        else:
            lines.append('Ниже по дереву нет доступного списка объектов. Возможно, это последний уровень детализации.')
        return lines

    if negatives:
        total_loss = sum(abs(_analytical_reason_effect(r)) for r in negatives)
        lines.append(f'Суммарный эффект основных отрицательных причин: {_analytical_money(total_loss)}.')
        main = negatives[0]
        lines.append(f'Главный денежный фактор: {_analytical_reason_name(main)} → {_analytical_money(abs(_analytical_reason_effect(main)))}.')
    elif result_money is not None:
        lines.append(f'Главный денежный ориентир экрана: {_analytical_money(result_money, signed=True)}.')
    if opportunity is not None and level != 'business':
        lines.append(f'Потенциал прибыли показывает, сколько денег находится внутри объекта для дальнейшего разбора: {_analytical_money(abs(_voice_num(opportunity)))}.')
    return lines


def _analytical_builder_meaning(screen: Dict[str, Any], level: str, intent_type: str, message: str) -> List[str]:
    lines: List[str] = []
    action_lines = _analytical_priority_lines(screen)
    text = _analytical_norm(message)

    if intent_type == 'next_best_action':
        if action_lines:
            lines.append('VECTRA берёт действие из Decision Layer. Новые действия без расчёта не придумываются.')
            lines.extend(action_lines)
        else:
            lines.append('Готовое действие в VECTRA Engine для этого уровня не найдено. Новые действия не генерирую без данных.')
        return lines

    if intent_type == 'teach_me':
        if 'потенциал' in text:
            lines.append('Потенциал прибыли не является оценкой менеджера. Это навигационные деньги: они показывают, где внутри объекта искать следующий слой возврата прибыли.')
        elif 'вклад' in text or 'результат' in text:
            lines.append('Вклад в результат бизнеса показывает, лучше или хуже объект работает относительно среднего уровня бизнеса. Положительный вклад — объект создаёт дополнительный результат. Отрицательный — объект недозарабатывает относительно бизнеса.')
        elif 'сравн' in text or 'бизнес' in text:
            lines.append('Сравнение с бизнесом нужно, чтобы не смотреть только на прошлый год. Объект может расти к прошлому году, но всё равно быть хуже текущей эффективности бизнеса.')
        else:
            lines.append('VECTRA объясняет объект через KPI, сравнение с прошлым годом, сравнение со средним уровнем бизнеса, причины и денежный эффект.')
        return lines

    if level == 'business':
        lines.append('Для бизнеса смысл анализа — увидеть общую динамику, главный риск и маршрут к зонам потенциала.')
    else:
        lines.append('Смысл не в том, чтобы оценить человека как хорошего или плохого. Смысл — показать, где можно вернуть прибыль и куда двигаться дальше.')
        lines.append('Если объект хуже среднего уровня бизнеса по причине, VECTRA переводит отклонение в деньги. Так появляется управленческий эффект для возврата прибыли.')
    return lines


def _build_analytical_response(screen: Dict[str, Any], intent_type: str, message: str = '') -> Dict[str, Any]:
    """Single analytical response generator for Stage 6.3 Phase 1.1.

    All analytical intents must go through this builder so VECTRA answers in
    one consistent Explain → Teach → Lead structure and does not duplicate
    benchmark/reasons/action logic across separate handlers.
    """
    ctx = _analytical_context(screen)
    level = ctx.get('level')
    object_name = ctx.get('object_name') or 'текущий объект'

    sections = [
        ('📍 Что произошло', _analytical_builder_what_happened(screen, level, object_name, intent_type, message)),
        ('📊 С чем сравниваем', _analytical_builder_comparison(screen, level)),
        ('📉 Главные причины', _analytical_builder_reasons(screen, level)),
        ('💰 Денежный эффект', _analytical_builder_money_effect(screen, level, intent_type)),
        ('📌 Что это означает', _analytical_builder_meaning(screen, level, intent_type, message)),
    ]

    next_step = _analytical_builder_next_step(screen, intent_type)
    lines: List[str] = []
    for title, body in sections:
        lines.append(title)
        lines.extend([line for line in body if str(line).strip()])
        lines.append('')
    lines.append('➡ Следующий шаг')
    lines.append(next_step.replace('➡ Следующий шаг: ', ''))

    return _analytical_response_shell(screen, intent_type, lines, [next_step])

def _execute_voice_analytical_request(
    message: str,
    intent: Dict[str, Any],
    session_ctx: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    screen = _analytical_current_screen(session_ctx)
    if not screen:
        return _build_analytical_no_context_response(message)
    intent_type = intent.get('intent_type') or 'explain_current_object'
    return _build_analytical_response(screen, intent_type, message)


def _build_voice_rating_response(message: str, intent: Dict[str, Any], session_ctx: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    normalized = normalize_entity_text(message)
    level = _voice_rating_level(normalized)
    metric_spec = _voice_rating_metric(normalized)
    metric_key = metric_spec.get('key') or 'finrez_pre'
    direction = _voice_rating_direction(normalized, metric_key)
    limit = 10

    if not level:
        return {
            'status': 'ok',
            'context': {'level': 'voice_management', 'object_name': 'Рейтинг', 'period': None, 'parent_object': None},
            'path': ['Voice Management Layer', 'Рейтинг'],
            'render_mode': 'voice_diagnostic',
            'summary_block': 'Не удалось определить уровень рейтинга.',
            'kpi_block': ['Уточните уровень: менеджеры, сети или SKU.'],
            'structure_block': [],
            'main_driver': '',
            'drain_block_render': [],
            'drain_total': 0,
            'all_block': [],
            'navigation_block': ['назад — вверх'],
            'explanation_block': [],
            'next_step_block': ['Пример: Покажи лучшие сети по марже.'],
        }

    try:
        rows = get_normalized_rows()
    except Exception as exc:
        return {'status': 'error', 'reason': f'Не удалось загрузить данные VECTRA Engine: {exc}'}

    period = _voice_period_from_message_or_state(message, session_ctx, rows)
    if not period:
        return {'status': 'error', 'reason': 'Не удалось определить период для рейтинга.'}

    period_rows, meta = filter_rows(rows, period=period)
    if not period_rows:
        return {'status': 'error', 'reason': meta.get('empty_reason') if isinstance(meta, dict) else 'Нет данных за период.'}

    previous_rows: List[Dict[str, Any]] = []
    previous_period = _voice_previous_year_period(period)
    if previous_period:
        previous_rows, _ = filter_rows(rows, period=previous_period)

    group_field = VOICE_RATING_FIELD_BY_LEVEL[level]
    items = _voice_group_rating_rows(period_rows, group_field, metric_spec, direction, limit=limit, previous_rows=previous_rows, level=level, period=period)
    if not items:
        return {
            'status': 'ok',
            'context': {'level': 'voice_management', 'object_name': 'Рейтинг', 'period': period, 'parent_object': None},
            'path': ['Voice Management Layer', 'Рейтинг'],
            'render_mode': 'voice_diagnostic',
            'summary_block': 'По этому запросу нет объектов с корректной базой данных.',
            'kpi_block': ['данных нет'],
            'structure_block': [],
            'main_driver': '',
            'drain_block_render': [],
            'drain_total': 0,
            'all_block': [],
            'navigation_block': ['назад — вверх'],
            'explanation_block': ['Значения с некорректной базой не включены в рейтинг.'],
            'next_step_block': ['Проверьте период или уточните показатель.'],
        }

    title = _voice_rating_title(level, metric_spec, direction, normalized, limit)
    lines = [f'📊 {title}', '']
    for idx, item in enumerate(items, start=1):
        lines.extend(_voice_render_rating_item(idx, item, metric_spec))
        if metric_spec.get('kind') == 'delta_margin' and idx != len(items):
            lines.append('')

    summary_lines = _voice_rating_summary(title, items, metric_spec, direction)

    return {
        'status': 'ok',
        'context': {'level': 'voice_management', 'object_name': title, 'period': period, 'parent_object': None},
        'path': ['Voice Management Layer', title],
        'render_mode': 'voice_diagnostic',
        'summary_block': summary_lines[0] if summary_lines else '',
        'kpi_block': lines,
        'structure_block': [],
        'main_driver': '',
        'drain_block_render': [],
        'drain_total': 0,
        'all_block': [],
        'navigation_block': ['назад — вверх'],
        'explanation_block': [],
        'next_step_block': summary_lines[2:] if len(summary_lines) > 2 else ['Следующий шаг: открыть объект из рейтинга обычной навигацией или задать уточняющий запрос.'],
    }

def _execute_voice_management_request(
    message: str,
    intent: Dict[str, Any],
    session_ctx: Optional[Dict[str, Any]] = None,
    session_id: str = 'default',
) -> Dict[str, Any]:
    # Object opening has priority over ratings. This is required for role
    # disambiguation phrases like "Покажи Труш Максим как топ-менеджера":
    # the word "топ" must not send the request to Voice Rating Layer when
    # a concrete object is present.
    try:
        rows = get_normalized_rows()
        period = _voice_period_from_message_or_state(message, session_ctx, rows)
        entity = _resolve_voice_entity_for_opening(message, get_entity_dictionary(period), period)
        if entity.get('clarification'):
            return entity['clarification']
        level = entity.get('entity_type')
        object_name = entity.get('entity_name')
        if level and object_name and period:
            resolved = _resolve_voice_open_filter(level, object_name, period, rows, session_ctx=session_ctx, message=message)
            if resolved.get('clarification'):
                return resolved['clarification']
            filter_payload = resolved.get('filter_payload') or {'period': period, level: object_name}

            # A direct object opening starts a new analytical path. Keep the
            # period from the previous state if needed, but do not carry old
            # parent filters like Головченко -> РУКАВИЧКА when the object
            # belongs to another branch. The reconstructed filter_payload is
            # now the source of truth for the opened object path.
            update_session(session_id, {
                'filter': {},
                'current_screen': None,
                'last_payload': None,
                'last_list_items': [],
                'last_list_level': None,
                'full_view': False,
                'view_mode': 'drain',
                'stack': [],
            })

            query = {
                'level': level,
                'object_name': object_name,
                'period_current': period,
                'query_type': 'summary',
                'mode': 'diagnosis',
                'filter_payload': filter_payload,
                'voice_open': True,
            }
            return _route_base_query(query, session_id)
    except Exception as exc:
        logger.warning('voice object opening failed: %s', exc)

    if (intent or {}).get('intent_type') == 'rating_request':
        return _build_voice_rating_response(message, intent, session_ctx=session_ctx)

    return _build_voice_management_diagnostic(message, intent)

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
        # current_screen is the single source of truth for UI commands
        # (all / reasons / back). last_payload is kept as a legacy alias.
        'current_screen': None,
        'last_payload': None,
        'show_all': False,
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


def _latest_active_session_payload() -> Dict[str, Any]:
    candidates = [v for v in SESSION_STORE.values() if isinstance(v, dict) and (v.get('current_screen') or v.get('last_payload'))]
    if not candidates:
        return {}
    return max(candidates, key=lambda x: float(x.get('_updated_at') or 0))


def get_session(session_id: str) -> Dict[str, Any]:
    with SESSION_LOCK:
        _hydrate_session_store()
        current = dict(_blank_state())
        stored = SESSION_STORE.get(session_id, {})
        # Some clients create a fresh session_id for every message. When that
        # happens, UI-only commands (all/reasons/back) must still see the last
        # active screen instead of returning "нет данных".
        if not stored:
            stored = _latest_active_session_payload()
        current.update(stored)
        return current


def update_session(session_id: str, data: Dict[str, Any]) -> None:
    with SESSION_LOCK:
        _hydrate_session_store()
        current = dict(_blank_state())
        current.update(SESSION_STORE.get(session_id, {}))
        current.update(data)
        current['_updated_at'] = time.time()
        SESSION_STORE[session_id] = current
        _persist_session_store()


def clear_full_view_flag(session_id: str) -> None:
    update_session(session_id, {'full_view': False})


def push_state(session_id: str) -> None:
    current = get_session(session_id)
    # Back must restore real screens, not blank pre-start state.
    if not current.get('current_screen') and not current.get('last_payload'):
        return
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
        'current_screen': deepcopy(current.get('current_screen') or current.get('last_payload')),
        'last_payload': deepcopy(current.get('last_payload') or current.get('current_screen')),
        'show_all': bool(current.get('show_all', False)),
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
        previous['_updated_at'] = time.time()
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
        'current_screen': session_ctx.get('current_screen') or session_ctx.get('last_payload'),
        'last_payload': session_ctx.get('last_payload') or session_ctx.get('current_screen'),
        'show_all': bool(session_ctx.get('show_all', False)),
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
    show_all: Optional[bool] = None,
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
    if show_all is not None:
        payload['show_all'] = show_all
    if payload:
        update_session(session_id, payload)


def save_last_payload(session_id: str, payload: Dict[str, Any]) -> None:
    # Store the full render-ready screen atomically. UI commands (все/причины/назад)
    # must read exactly what /vectra/query returned last time. Do NOT pass a
    # render-ready screen through enforce_contract(): that validator expects the
    # raw summary contract and would convert a valid screen into
    # {status:error, reason:invalid response data}.
    if not isinstance(payload, dict):
        screen = payload
    elif _is_render_ready_screen(payload):
        screen = deepcopy(payload)
    else:
        screen = enforce_contract(payload)

    # State Layer stabilization: list/reasons are UI display modes, not real
    # analytical screens. They may update last_payload for display, but must not
    # overwrite current_screen; otherwise the next «назад» returns to the list
    # instead of the object screen.
    mode = str(screen.get('render_mode') or screen.get('view_mode') or '').strip().lower() if isinstance(screen, dict) else ''
    if mode in {'list_only', 'all', 'reasons'}:
        update_session(session_id, {'last_payload': screen})
        return
    update_session(session_id, {'last_payload': screen, 'current_screen': screen})


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


def _extract_item_effect_money(item: Dict[str, Any]) -> Optional[float]:
    if not isinstance(item, dict):
        return None
    for key in ('navigation_money', 'effect_money', 'potential_money', 'gap_loss_money'):
        value = item.get(key)
        if value is not None:
            try:
                return float(value)
            except Exception:
                pass
    impact = item.get('impact') if isinstance(item.get('impact'), dict) else {}
    for key in ('navigation_money', 'gap_loss_money', 'effect_money', 'potential_money'):
        value = impact.get(key)
        if value is not None:
            try:
                return float(value)
            except Exception:
                pass
    return None


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

    standard_required = {'context', 'metrics', 'structure', 'drain_block', 'navigation'}
    sku_required = {'context', 'metrics', 'structure', 'drain_block', 'navigation'}

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
    'business': lambda obj, p, fp=None: get_business_summary(period=p, filter_payload=fp),
    'manager_top': lambda obj, p, fp=None: get_manager_top_summary(manager_top=obj, period=p, filter_payload=fp),
    'manager': lambda obj, p, fp=None: get_manager_summary(manager=obj, period=p, filter_payload=fp),
    'network': lambda obj, p, fp=None: get_network_summary(network=obj, period=p, filter_payload=fp),
    'category': lambda obj, p, fp=None: get_category_summary(category=obj, period=p, filter_payload=fp),
    'tmc_group': lambda obj, p, fp=None: get_tmc_group_summary(tmc_group=obj, period=p, filter_payload=fp),
    'sku': lambda obj, p, fp=None: get_sku_summary(sku=obj, period=p, filter_payload=fp),
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
    if '..' in period:
        start, end = period.split('..', 1)
        prev_start = _previous_year_period(start)
        prev_end = _previous_year_period(end)
        if prev_start and prev_end:
            return f'{prev_start}..{prev_end}'
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

def _cannot_parse_message_response(message: str) -> Dict[str, Any]:
    logger.warning('cannot_parse_message raw_message=%r normalized_message=%r', message, _normalize_message(message))
    return {'status': 'error', 'reason': 'cannot_parse_message'}


def _is_period_only_message(message: str) -> bool:
    normalized = _normalize_message(message)
    parsed = parse_query_intent.__globals__['resolve_period_from_message'](normalized)
    period_current, _ = parsed
    if not period_current:
        return False
    compact = normalized.replace(' ', '')
    return compact == period_current


def _validate_parsed_query(message: str, parsed: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(parsed, dict) or parsed.get('status') != 'ok':
        return _cannot_parse_message_response(message)

    query = parsed.get('query') if isinstance(parsed.get('query'), dict) else {}
    period_current = query.get('period_current') or query.get('period')
    level = (query.get('level') or '').strip() if isinstance(query.get('level'), str) else query.get('level')

    if _is_period_only_message(message):
        query['level'] = 'business'
        query['object_name'] = 'business'
        query['object'] = 'business'
        level = 'business'

    if not period_current or not level:
        return _cannot_parse_message_response(message)

    parsed['query'] = query
    return parsed



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


def _is_search_command(message: str) -> bool:
    return False


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
                'view_mode': 'reasons',
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
    last_payload = (session_ctx.get('last_payload') or {}) if isinstance(session_ctx.get('last_payload'), dict) else {}
    data = last_payload.get('data') or last_payload

    if (not level or not object_name or not period) and isinstance(data, dict):
        context = data.get('context') if isinstance(data.get('context'), dict) else {}
        level = level or context.get('level') or data.get('level')
        object_name = object_name or context.get('object_name') or data.get('object_name')
        period = period or context.get('period') or data.get('period')

    if not level or not object_name or not period:
        return {'status': 'error', 'reason': 'Нет данных для отображения.'}

    target_level = state.get('last_list_level') or _resolve_next_level_from_payload(level, data)
    target_level = _coerce_target_level(level, target_level, data)
    if not target_level:
        return {'status': 'error', 'reason': 'Нет следующего уровня для отображения.'}

    return {
        'status': 'ok',
        'query': {
            'mode': 'diagnosis',
            'level': level,
            'object_name': object_name,
            'period_current': period,
            'period_previous': state.get('period_previous'),
            'query_type': 'drill_down',
            'target_level': target_level,
            'period': period,
            'object': object_name,
            'list_mode': True,
            'full_view': True,
            'filter_payload': state.get('filter') or {},
            'view_mode': 'all',
        },
    }


def _build_query_from_numeric_selection(message: str, session_ctx: Dict[str, Any]) -> Dict[str, Any]:
    if not message.isdigit():
        return {}
    state = get_session_state(session_ctx)
    # Navigation Contract v1.2 / BUG-006 FIX-002:
    # numeric drilldown may use only last_list_items generated from all_block.
    # Do not parse response text as a fallback navigation source.
    items = state.get('last_list_items') or []
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
                'view_mode': 'default',
            },
        }
    return {'status': 'error', 'reason': 'Навигация недоступна: отсутствует all_block'}




def _is_render_ready_screen(payload: Any) -> bool:
    return isinstance(payload, dict) and isinstance(payload.get('context'), dict) and any(
        key in payload for key in ('kpi_block', 'structure_block', 'navigation_block', 'drain_block_render', 'result_block')
    )


def _fmt_signed_int_local(value: Any) -> str:
    try:
        num = int(round(float(value or 0)))
    except Exception:
        num = 0
    if num > 0:
        return f'+{num:,}'.replace(',', ' ')
    if num < 0:
        return f'−{abs(num):,}'.replace(',', ' ')
    return '0'


def _extract_effect_money_local(item: Dict[str, Any]) -> Any:
    if not isinstance(item, dict):
        return 0
    for key in ('navigation_money', 'effect_money', 'potential_money', 'gap_loss_money'):
        if item.get(key) is not None:
            return item.get(key)
    return 0


def _render_all_screen_from_ready(screen: Dict[str, Any]) -> Dict[str, Any]:
    out = deepcopy(screen)
    block = out.get('all_block')
    if not isinstance(block, list):
        return {'status': 'error', 'reason': 'all_block_missing'}

    lines = []
    list_items = []
    next_level = out.get('children_level')
    nav = out.get('navigation') if isinstance(out.get('navigation'), dict) else {}
    if not next_level:
        next_level = nav.get('next_level')
    if not next_level:
        ctx = out.get('context') if isinstance(out.get('context'), dict) else {}
        next_level = DEFAULT_NEXT_LEVEL.get(str(ctx.get('level') or '').strip().lower())

    total = 0
    for idx, item in enumerate(block, start=1):
        if not isinstance(item, dict):
            continue
        name = item.get('object_name') or item.get('name')
        if not name:
            continue
        navigation_money = item.get('navigation_money')
        if navigation_money is None:
            effect = _extract_effect_money_local(item)
            try:
                eff_num = int(round(float(effect or 0)))
            except Exception:
                eff_num = 0
            navigation_money = abs(eff_num) if eff_num < 0 else 0
        try:
            nav_num = int(round(float(navigation_money or 0)))
        except Exception:
            nav_num = 0
        total += nav_num
        
        profit_delta = item.get('profit_delta_money')
        if profit_delta is not None:
            lines.append(f'{idx} — {name} → {_fmt_signed_int_local(profit_delta)} к прошлому году')
        else:
            lines.append(f'{idx} — {name} → {_fmt_signed_int_local(-nav_num)} потенциал')
        list_items.append({
            'object_name': name,
            'level': item.get('level') or next_level,
            'normalized_name': normalize_entity_text(str(name)),
            'navigation_money': nav_num,
        })

    out.update({
        'status': 'ok',
        'render_mode': 'list_only',
        'summary_block': 'Полный список объектов текущего уровня.',
        'kpi_block': [],
        'structure_block': [],
        'main_driver': '',
        'decision_block_render': [],
        'drain_block_render': lines,
        'drain_total': total,
        'navigation_block': ['причины — разбор', 'назад — вверх'],
    })
    out['_list_items_for_state'] = [i for i in list_items if i.get('level')]
    return out


def _render_reasons_screen_from_ready(screen: Dict[str, Any]) -> Dict[str, Any]:
    block = screen.get('reasons_block')
    render = screen.get('reasons_block_render')
    if (not isinstance(render, list) or not render) and isinstance(screen.get('structure_block'), list) and screen.get('structure_block'):
        render = list(screen.get('structure_block') or [])
    if (not isinstance(block, list) or not block) and (not isinstance(render, list) or not render):
        return {'status': 'error', 'reason': 'reasons_block_missing'}
    out = deepcopy(screen)
    out.update({
        'status': 'ok',
        'render_mode': 'reasons',
        'summary_block': 'Разбор причин текущего объекта.',
        'kpi_block': [],
        'structure_block': [],
        'main_driver': '',
        'drain_block_render': [],
        'drain_total': 0,
        'decision_block_render': [],
        'navigation_block': ['назад к объекту'],
        'reasons_block_render': render if isinstance(render, list) else out.get('reasons_block_render', []),
    })
    return out


def _direct_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    if isinstance(payload.get("data"), dict):
        return payload.get("data")
    return payload


def _structure_to_reasons_block(structure: Any) -> List[Dict[str, Any]]:
    """Build reasons from the current screen structure when the raw reasons
    block is missing. Reasons is a view of the current screen, not a new
    summary query, so it must not reset drilldown state.
    """
    if isinstance(structure, list):
        return [dict(item) for item in structure if isinstance(item, dict)]
    if not isinstance(structure, dict):
        return []
    names = {
        'markup': 'Наценка',
        'retro': 'Ретро',
        'logistics': 'Логистика',
        'personnel': 'Персонал',
        'other': 'Прочие',
    }
    result: List[Dict[str, Any]] = []
    for key in ['markup', 'retro', 'logistics', 'personnel', 'other']:
        value = structure.get(key)
        if not isinstance(value, dict):
            continue
        item = dict(value)
        item.setdefault('name', names.get(key, str(key)))
        result.append(item)
    return result



def _ensure_screen_blocks(data: Dict[str, Any]) -> Dict[str, Any]:
    """Normalize a stored screen before rendering UI-only modes.

    current_screen is the source of truth. This function only fills missing
    render blocks from the same screen payload; it never calls API and never
    switches object context.
    """
    if not isinstance(data, dict):
        return {}
    out = deepcopy(data)
    if not isinstance(out.get('all_block'), list):
        out['all_block'] = []
    if not isinstance(out.get('reasons_block'), list) or not out.get('reasons_block'):
        derived = _structure_to_reasons_block(out.get('structure'))
        if derived:
            out['reasons_block'] = derived
    if not isinstance(out.get('path'), list):
        inferred = _infer_path(out)
        if inferred:
            out['path'] = inferred
    return out

def _with_mode_payload(payload: Dict[str, Any], mode: str) -> Dict[str, Any]:
    # If state already holds the final rendered screen (normal /vectra/query
    # path), produce another final rendered screen and do not rebuild it as a raw
    # summary. This is the core fix for v5: UI commands are navigation modes.
    if _is_render_ready_screen(payload):
        if mode == "all":
            return _render_all_screen_from_ready(payload)
        if mode == "reasons":
            return _render_reasons_screen_from_ready(payload)
        return payload

    data = _ensure_screen_blocks(_direct_payload(payload))
    if not isinstance(data, dict):
        return {"status": "error", "reason": "Нет активного экрана"}

    navigation = dict(data.get("navigation") or {})
    navigation["mode"] = mode
    data["navigation"] = navigation

    if mode == "all":
        block = data.get("all_block")
        if not isinstance(block, list):
            return {"status": "error", "reason": "all_block_missing"}
        data["drain_block"] = block
        data["view_mode"] = "all"
        data["show_all"] = True
    elif mode == "reasons":
        block = data.get("reasons_block")
        if not isinstance(block, list) or not block:
            return {"status": "error", "reason": "reasons_block_missing"}
        data = {
            "status": "ok",
            "context": data.get("context") or {},
            "path": data.get("path") or [],
            "summary_block": "Разбор причин текущего объекта.",
            "structure": data.get("structure") or {},
            "metrics": data.get("metrics") or [],
            "reasons_block": block,
            "navigation": {"mode": "reasons", "back": True},
            "view_mode": "reasons",
        }
    else:
        navigation["mode"] = "default"
        data["navigation"] = navigation

    return data

def _handle_back(session_id: str) -> Dict[str, Any]:
    current = get_session(session_id)

    # UI display modes (все / причины) are not real drilldown steps.
    # Back from those modes must restore the same analytical object screen
    # and must not pop the drilldown stack.
    if current.get('view_mode') in {'all', 'reasons'}:
        screen = current.get('current_screen') or current.get('last_payload')
        save_session_state(session_id, view_mode='drain', show_all=False)
        if screen:
            restored_screen = deepcopy(screen) if _is_render_ready_screen(screen) else enforce_contract(screen)
            if isinstance(restored_screen, dict):
                restored_screen['render_mode'] = ''
            return restored_screen
        return {'status': 'error', 'reason': 'Назад недоступно.'}

    restored = pop_state(session_id)
    if not restored:
        screen = current.get('current_screen') or current.get('last_payload')
        if screen:
            return deepcopy(screen) if _is_render_ready_screen(screen) else enforce_contract(screen)
        return {'status': 'error', 'reason': 'Назад недоступно.'}

    screen = restored.get('current_screen') or restored.get('last_payload')
    if not screen:
        return {'status': 'error', 'reason': 'Назад недоступно.'}

    return deepcopy(screen) if _is_render_ready_screen(screen) else enforce_contract(screen)


def _route_drill_query(query: Dict[str, Any], session_ctx: Dict[str, Any], session_id: str) -> Dict[str, Any]:
    state = get_session_state(session_ctx)

    scope_level = query.get('level') or state.get('level')
    scope_object_name = query.get('object_name') or state.get('object_name')
    period = query.get('period_current') or state.get('period')
    target_level = _coerce_target_level(scope_level, query.get('target_level') or DEFAULT_NEXT_LEVEL.get(scope_level), state.get('last_payload') or {})
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
        child_level = _resolve_next_level_from_payload(scope_level, source) or target_level
        # List context is created from the canonical all_block rendered by build_list_view.
        # No response-text fallback is allowed for navigation.
        rendered_all_block = ((response.get('data') or {}).get('all_block') if isinstance(response.get('data'), dict) else [])
        list_items = _build_last_list_items(rendered_all_block if isinstance(rendered_all_block, list) else [], child_level)
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

    target_level = _coerce_target_level(level, _resolve_next_level_from_payload(level, current), current)
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
    view_payload = build_object_view(
        sanitize_payload(current),
        sanitize_payload(drain_payload) if drain_payload is not None else None
    )

    # Cleanup Stage 1: orchestration does not create Goal/Focus state.
    # Money, navigation, business and object meaning comes only from v1.2 contracts.
    for legacy_key in ('goal', 'goal_block', 'focus_money', 'path_goal_money', 'current_focus_money', 'selected_focus_money', 'vector_block', 'coverage_percent', 'coverage', 'path_goal'):
        view_payload.pop(legacy_key, None)

    response = ok_response(query, view_payload)

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
            child_level = _resolve_next_level_from_payload(level, drain_payload) or target_level
            # BUG-006 / Navigation Contract v1.2:
            # numeric drilldown choices must follow the same ordered all_block that renders "все".
            list_source = view_payload.get('all_block') if isinstance(view_payload.get('all_block'), list) else []
            list_items = _build_last_list_items(list_source, child_level)
            save_session_state(
                session_id,
                last_response_type='object',
                last_list_level=target_level,
                last_list_items=list_items,
                full_view=False,
                view_mode='default',
                show_all=False,
            )
        else:
            save_session_state(
                session_id,
                last_response_type='object',
                last_list_level=None,
                last_list_items=[],
                full_view=False,
                view_mode='default',
                show_all=False,
            )
        direct = enforce_contract(response)
        save_last_payload(session_id, direct)

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
        target_level = _coerce_target_level(level, _resolve_next_level_from_payload(level, current), current)
        if not target_level:
            return not_implemented_response(query, 'losses not supported for this level')

        source = _build_drill_from_scope(level, object_name, target_level, period, full_view=False, filter_payload=(current.get('filter') or query.get('filter_payload')))
        if 'error' in source:
            return error_response(source['error'], query)

        response = ok_response(query, build_losses_view_from_children(sanitize_payload(source)))
        if response.get('status') == 'ok':
            _store_scope(session_id, level, object_name, period, query.get('period_previous'), 'diagnosis', push_to_stack=True)
            child_level = _resolve_next_level_from_payload(level, source) or target_level
            list_items = _build_last_list_items(source.get('all_block', []), child_level)
            save_session_state(
                session_id,
                last_list_level=child_level,
                last_response_type='losses',
                last_list_items=list_items,
                full_view=False,
            )
            save_last_payload(session_id, response)
        return response

    response = _route_signal_flow(query, current, session_id)
    view_mode = query.get('view_mode') or 'default'
    if isinstance(response, dict) and response.get('status') == 'ok' and view_mode in {'all', 'reasons'}:
        direct = _with_mode_payload(response, view_mode)
        save_last_payload(session_id, direct)
        save_session_state(session_id, view_mode=view_mode)
        return direct
    return response





def _infer_path(payload: Dict[str, Any]) -> List[str]:
    flt = dict(payload.get('filter') or {})
    parts = ['Бизнес']
    for key in ['manager_top','manager','network','category','tmc_group','sku']:
        value = flt.get(key)
        if value:
            parts.append(str(value))
    return parts


def _attach_product_context(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict) or payload.get('status') == 'error':
        return payload
    enriched = dict(payload)
    if not isinstance(enriched.get('path'), list) or not enriched.get('path'):
        enriched['path'] = _infer_path(enriched)
    if 'summary_block' not in enriched:
        main_driver = ''
        structure = enriched.get('structure') or {}
        if isinstance(structure, dict):
            worst_name = None
            worst_effect = None
            for key, value in structure.items():
                if not isinstance(value, dict):
                    continue
                eff = value.get('effect_money')
                try:
                    eff = float(eff or 0)
                except Exception:
                    eff = 0.0
                if worst_effect is None or eff < worst_effect:
                    worst_effect = eff
                    worst_name = key
            mapping = {'markup':'наценку','retro':'ретро','logistics':'логистику','personnel':'персонал','other':'прочие расходы'}
            if worst_name:
                main_driver = mapping.get(worst_name, str(worst_name))
        enriched['summary_block'] = f'Основное давление на результат через {main_driver}.' if main_driver else ''
    return enriched

def _finalize_response(response: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(response, dict):
        return error_response('нет данных')
    if response.get('status') == 'ok':
        if _is_render_ready_screen(response):
            return response
        return _attach_product_context(enforce_contract(response))
    if response.get('status') == 'error':
        return {
            'status': 'error',
            'reason': response.get('reason') or 'unknown_error',
        }
    return error_response('нет данных')

def _show_start_screen() -> Dict[str, Any]:
    # VECTRA PRODUCT INSTRUCTION v3.0:
    # "Начать Анализ" is a local UI command. It must not trigger DATA/API
    # calculations and must render only the approved start prompt.
    return {
        "status": "ok",
        "render_mode": "start",
        "context": {"level": "start", "object_name": "Старт", "period": None, "parent_object": None},
        "path": ["Старт"],
        "summary_block": "📍 Старт",
        "kpi_block": [
            "Введите:",
            "• Бизнес 2026-02",
            "• Труш Максим 2026-02",
            "• Оптторг-15 2026-02",
            "• Покажи Варус 2026-02",
        ],
        "structure_block": [],
        "main_driver": "",
        "drain_block_render": [],
        "drain_total": 0,
        "navigation_block": [],
        "result_block": [],
        "period_result_block": [],
        "screen_order": ["summary_block", "kpi_block"],
    }


def orchestrate_vectra_query(message: str, session_id: str = 'default') -> Dict[str, Any]:
    session_ctx = get_session(session_id)
    normalized = _normalize_message(message)

    if normalized in {'начать анализ', 'start analysis'}:
        return _show_start_screen()

    if _is_back_command(normalized):
        return _finalize_response(_handle_back(session_id))

    if normalized.isdigit():
        parsed = _build_query_from_numeric_selection(normalized, session_ctx)
        if parsed.get('status') != 'ok':
            return _finalize_response(parsed)
    elif _is_search_command(normalized):
        last_payload = session_ctx.get('last_payload') or {}
        if last_payload:
            return _finalize_response(last_payload)
        return _finalize_response({'status': 'error', 'reason': 'Нет активного списка для поиска.'})
    elif normalized in {'причины', 'причина', 'разбор', 'разбор причин'}:
        screen = session_ctx.get('current_screen') or session_ctx.get('last_payload') or {}
        if screen:
            view = _with_mode_payload(screen, 'reasons')
            if view.get('status') != 'error':
                save_session_state(session_id, view_mode='reasons', show_all=False)
            return _finalize_response(view)
        return _finalize_response({'status': 'error', 'reason': 'Нет активного экрана для разбора причин.'})
    elif _is_short_command(normalized):
        parsed = _build_query_from_short_command(normalized, session_ctx)
        if parsed.get('status') != 'ok':
            return _finalize_response(parsed)
    elif _is_full_reasons_command(normalized):
        screen = session_ctx.get('current_screen') or session_ctx.get('last_payload') or {}
        if screen:
            view = _with_mode_payload(screen, 'reasons')
            if view.get('status') != 'error':
                save_session_state(session_id, view_mode='reasons', show_all=False)
            return _finalize_response(view)
        parsed = _build_query_from_short_command('причины', session_ctx)
    elif _is_full_view_command(normalized):
        screen = session_ctx.get('current_screen') or session_ctx.get('last_payload') or {}
        if not screen:
            return _finalize_response({'status': 'error', 'reason': 'Нет данных для отображения.'})
        view = _with_mode_payload(screen, 'all')
        if view.get('status') != 'error':
            list_items = view.pop('_list_items_for_state', None) if isinstance(view, dict) else None
            save_session_state(
                session_id,
                view_mode='all',
                show_all=True,
                last_list_items=list_items if list_items is not None else None,
            )
        return _finalize_response(view)
    else:
        analytical_intent = _detect_voice_analytical_intent(message, normalized)
        if analytical_intent:
            return _finalize_response(_execute_voice_analytical_request(message, analytical_intent, session_ctx=session_ctx))

        voice_intent = _detect_voice_management_intent(message, normalized)
        if voice_intent:
            return _finalize_response(_execute_voice_management_request(message, voice_intent, session_ctx=session_ctx, session_id=session_id))

        # A free-text query starts a new analytical path.
        # Do not carry an old business-drain vector into a direct manager/network entry.
        clear_full_view_flag(session_id)
        update_session(session_id, {
            'current_screen': None,
            'last_payload': None,
            'last_list_items': [],
            'last_list_level': None,
            'full_view': False,
            'view_mode': 'drain',
            'stack': [],
        })
        parsed = _validate_parsed_query(message, parse_query_intent(message))
        if parsed.get('status') != 'ok':
            return _finalize_response(parsed)

    query = parsed['query']
    query_type = query.get('query_type', 'summary')

    if query_type == 'drill_down':
        return _finalize_response(_route_drill_query(query, get_session(session_id), session_id))

    return _finalize_response(_route_base_query(query, session_id))
