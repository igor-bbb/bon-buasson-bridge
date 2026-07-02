"""Workspace Runtime Architecture helpers for VECTRA W14.

This module creates the contract between API and Custom GPT after
`workspace_markdown` became the canonical rendered Workspace.

Principles:
- workspace_markdown is immutable user-visible content;
- numeric commands are resolved against the last displayed menu;
- navigation_block is technical support, not the source of truth;
- Development Journal capture commands must be routed into the official API
  journal instead of being kept as chat notes.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional


def normalize_text(value: Any) -> str:
    text = str(value or '').strip().lower().replace('ё', 'е')
    text = re.sub(r'[*_`#>\[\]()]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


MENU_HEADERS = (
    'что делаем дальше',
    'следующие действия',
    'доступные действия',
    'что можно сделать дальше',
    'выберите направление',
    'action zone',
)

STOP_HEADERS = (
    '# ', '## ', '### ', '---', '⸻'
)


def _line_to_text(line: Any) -> str:
    return str(line or '').strip()


def _extract_markdown(payload: Dict[str, Any]) -> str:
    # W14.5 Single Rendering Contract:
    # only workspace_markdown is user-visible rendering authority.
    # Do not fall back to workspace_primary_block or technical blocks, because
    # that reintroduces GPT-side / runtime-side Workspace composition.
    markdown = payload.get('workspace_markdown')
    if isinstance(markdown, str) and markdown.strip():
        return markdown
    return ''


def _parse_numbered_action(line: str) -> Optional[Dict[str, Any]]:
    raw = _line_to_text(line)
    if not raw:
        return None
    # Supports: "1. Text", "1 — Text", "1) Text", "* **1** — Text".
    cleaned = raw.lstrip('-•* ').strip()
    cleaned = re.sub(r'^\*\*(\d+)\*\*', r'\1', cleaned)
    m = re.match(r'^(\d{1,2})\s*(?:[\.)\-—:]+)?\s*(.+)$', cleaned)
    if not m:
        return None
    number = int(m.group(1))
    text = m.group(2).strip(' -—:')
    if not text:
        return None
    return {
        'number': number,
        'label': text,
        'normalized_label': normalize_text(text),
        'source_line': raw,
        'action_type': classify_action_label(text),
    }


def _extract_menu_actions_from_markdown(markdown: str) -> List[Dict[str, Any]]:
    if not markdown:
        return []
    lines = markdown.splitlines()
    start = None
    for idx, line in enumerate(lines):
        norm = normalize_text(line)
        if any(h in norm for h in MENU_HEADERS):
            start = idx + 1
            break
    if start is None:
        # Last-resort: use numbered lines near the end of the Workspace only.
        tail = lines[-30:]
        parsed = [_parse_numbered_action(x) for x in tail]
        return [x for x in parsed if x][:10]

    actions: List[Dict[str, Any]] = []
    for line in lines[start:start + 40]:
        stripped = _line_to_text(line)
        if not stripped:
            continue
        # Stop at next clear markdown section after actions have started.
        if actions and (stripped.startswith('#') or stripped.startswith('---') or stripped.startswith('⸻')):
            break
        parsed = _parse_numbered_action(stripped)
        if parsed:
            actions.append(parsed)
        elif actions and not stripped.startswith(('-', '•', '*')) and re.match(r'^\D', stripped):
            # A normal paragraph after the action list likely means the menu ended.
            if len(actions) >= 2:
                break
    return actions[:20]


def classify_action_label(label: Any) -> str:
    norm = normalize_text(label)
    if 'назад' in norm or 'вернуться' in norm:
        return 'back'
    if any(x in norm for x in ('полная витрина', 'полную витрину', 'показать все', 'все объекты', 'полный список', 'витрина')):
        return 'show_all'
    if any(x in norm for x in ('причины', 'факторы', 'разобрать влияние')):
        return 'reasons'
    if any(x in norm for x in ('после встречи', 'завершить переговор', 'итог встреч')):
        return 'post_meeting'
    if any(x in norm for x in ('исполнен', 'исполнение', 'выполнени', 'контроль выполнения', 'перейти к исполн')):
        return 'execution'
    if any(x in norm for x in ('переговор', 'встреч', 'байер')):
        return 'negotiation'
    if any(x in norm for x in ('пакет sku', 'пакет позиц', 'пакет развития', 'собрать пакет')):
        return 'sku_package'
    if any(x in norm for x in ('отсутствующ', 'ассортиментн', 'лидеров sku', 'лидеры sku')):
        if 'лидер' in norm and 'отсутств' not in norm:
            return 'sku_leaders'
        return 'missing_sku'
    if any(x in norm for x in ('задач', 'задачи')):
        return 'tasks'
    if any(x in norm for x in ('sku-лидер', 'sku лидер', 'открыть sku', 'доказательн')):
        return 'sku_leader'
    if any(x in norm for x in ('категор', 'формат', 'рабочий стол', 'открыть', 'разобрать')):
        return 'open_child_or_context'
    if any(x in norm for x in ('вопрос', 'спросить')):
        return 'free_dialogue'
    return 'open_child_or_context'


def build_workspace_action_map(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    markdown = _extract_markdown(payload)
    actions = _extract_menu_actions_from_markdown(markdown)
    if actions:
        return actions

    # W14.5: navigation_block is internal API data. It must not become a
    # fallback source for user-visible command resolution when the visible
    # Workspace menu is missing. Missing visible menu is a Workspace composition
    # issue, not a reason to render hidden technical navigation.
    return []


def _payload_has_workspace_shape(payload: Dict[str, Any]) -> bool:
    if not isinstance(payload, dict) or payload.get('status') == 'error':
        return False
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    render_mode = str(payload.get('render_mode') or '').strip().lower()
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    if render_mode in {'start', 'voice_diagnostic'} or level in {'start'}:
        return False
    if render_mode in {'development_journal', 'development_journal_capture', 'development_journal_export', 'release_manager', 'laboratory_analysis', 'test_plan', 'scenario_runner', 'scenario_library'}:
        return False
    return bool(ctx or payload.get('workspace_primary_block') or payload.get('workspace_markdown') or render_mode)


def _append_render_lines(lines: List[str], value: Any) -> None:
    if value is None:
        return
    if isinstance(value, str):
        if value.strip():
            lines.append(value.strip())
        return
    if isinstance(value, list):
        for item in value:
            _append_render_lines(lines, item)
        return
    if isinstance(value, dict):
        # Runtime may receive already-normalized public rows (for example KPI
        # tables). Keep this API-side rendering simple and business-readable; do
        # not expose JSON or internal object structures to the Custom GPT.
        cleaned = []
        for key, val in value.items():
            if val in (None, '', [], {}):
                continue
            cleaned.append(f'{key}: {val}')
        if cleaned:
            lines.append(' | '.join(cleaned))
        return
    text = str(value).strip()
    if text:
        lines.append(text)


def _compose_workspace_markdown_from_render_blocks(payload: Dict[str, Any]) -> str:
    ordered_keys = (
        'summary_block',
        'result_block',
        'period_result_block',
        'kpi_block',
        'kpi_table',
        'structure_block',
        'diagnosis_block',
        'explanation_block',
        'opportunity_explanation_block',
        'anomaly_explanation_block',
        'drain_block_render',
        'product_layer_block',
        'product_insight_block',
        'business_context_block',
        'category_workspace_block',
        'product_workspace_block',
        'management_workspace_block',
        'contract_workspace_block',
        'decision_workspace_block',
        'sku_passport_block',
        'decision_block_render',
        'reasons_block_render',
        'factor_change_block',
        'benchmark_diagnostic_block',
        'next_step_block',
        'recommended_next_step_block',
        'navigation_block',
    )
    lines: List[str] = []
    for key in ordered_keys:
        _append_render_lines(lines, payload.get(key))
    # Remove consecutive duplicates while preserving order.
    out: List[str] = []
    seen_previous = None
    for line in lines:
        text = str(line).rstrip()
        if not text or text == seen_previous:
            continue
        out.append(text)
        seen_previous = text
    return '\n'.join(out).strip()


def _coerce_workspace_markdown(payload: Dict[str, Any]) -> None:
    """Create the canonical markdown at API-runtime level when renderer data exists.

    This is not a Custom GPT fallback. The API is responsible for returning
    `workspace_markdown`. If an internal renderer already produced public
    render blocks, Runtime promotes those API-produced blocks into the canonical
    public field before the response leaves Runtime.
    """
    markdown = payload.get('workspace_markdown')
    if isinstance(markdown, str) and markdown.strip():
        return
    primary = payload.get('workspace_primary_block')
    if isinstance(primary, list) and primary:
        lines = [str(x).rstrip() for x in primary if str(x or '').strip()]
        if lines:
            payload['workspace_markdown'] = '\n'.join(lines)
            return
    composed = _compose_workspace_markdown_from_render_blocks(payload)
    if composed:
        payload['workspace_markdown'] = composed


def _default_visible_actions(payload: Dict[str, Any]) -> List[str]:
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    render_mode = str(payload.get('render_mode') or '').strip().lower()

    if render_mode in {'negotiation_workspace'}:
        return ['собрать пакет позиций — подготовить SKU-пакет', 'создать задачи — зафиксировать действия', 'перейти к исполнению — контроль выполнения задач', 'назад — вернуться к рабочему столу объекта']
    if render_mode in {'action_package'}:
        return ['подготовить переговоры — собрать аргументы', 'создать задачи — зафиксировать ввод позиций', 'перейти к исполнению — контроль выполнения задач', 'назад — вернуться к рабочему столу объекта']
    if render_mode in {'task_workspace'}:
        return ['перейти к исполнению — контроль выполнения задач', 'после встречи — зафиксировать итог переговоров', 'подготовить переговоры — вернуться к позиции', 'назад — вернуться к рабочему столу объекта']
    if render_mode in {'execution_workspace'}:
        return ['после встречи — зафиксировать итог переговоров', 'создать задачи — обновить черновик задач', 'назад — вернуться к рабочему столу объекта']
    if render_mode in {'post_meeting_workspace'}:
        return ['создать задачи — сформировать черновик задач', 'перейти к исполнению — контроль выполнения задач', 'подготовить переговоры — вернуться к позиции', 'назад — вернуться к рабочему столу объекта']

    if level in {'network', 'contract'}:
        return ['подготовить переговоры — собрать переговорную позицию', 'собрать пакет позиций — подготовить SKU-пакет', 'создать задачи — зафиксировать действия', 'причины — разобрать факторы', 'все — открыть витрину текущего уровня', 'назад — вернуться назад']
    if level in {'category', 'tmc_group', 'format', 'sku', 'product'}:
        return ['собрать пакет позиций — подготовить пакет развития', 'подготовить переговоры — собрать аргументы', 'создать задачи — зафиксировать действия', 'причины — разобрать факторы', 'все — открыть витрину текущего уровня', 'назад — вернуться назад']
    if level in {'business', 'manager_top', 'top_manager', 'manager'}:
        return ['все — открыть витрину текущего уровня', 'причины — разобрать факторы', 'kpi — показать KPI', 'назад — вернуться назад']
    return ['причины — разобрать факторы', 'все — открыть витрину текущего уровня', 'назад — вернуться назад']


def _ensure_visible_action_menu(payload: Dict[str, Any]) -> None:
    markdown = payload.get('workspace_markdown')
    if not isinstance(markdown, str) or not markdown.strip():
        return
    if build_workspace_action_map(payload):
        return
    actions = _default_visible_actions(payload)
    if not actions:
        return
    menu_lines = ['', '## Что делаем дальше?']
    for idx, label in enumerate(actions, start=1):
        menu_lines.append(f'{idx}. {label}')
    payload['workspace_markdown'] = markdown.rstrip() + '\n' + '\n'.join(menu_lines)


def ensure_runtime_workspace_rendering(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Guarantee the runtime rendering contract for supported Workspaces.

    DEV-0002: supported Workspace responses must leave Runtime with non-empty
    workspace_markdown.
    DEV-0004: supported Workspace responses must have a visible menu from which
    workspace_action_map can be extracted.
    """
    if not _payload_has_workspace_shape(payload):
        return payload
    _coerce_workspace_markdown(payload)
    _ensure_visible_action_menu(payload)
    return payload


def infer_filter_from_payload(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Infer the stable object filter represented by the current Workspace.

    W15.0 Context Engine:
    active_workspace_state is the primary runtime context.  The filter makes
    the context executable for free dialogue, local commands and follow-up
    actions without forcing the user to repeat object/period.
    """
    if not isinstance(payload, dict):
        return {}
    existing = payload.get('filter')
    if isinstance(existing, dict) and existing:
        return dict(existing)

    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    period = ctx.get('period') or payload.get('period')
    path = payload.get('path') if isinstance(payload.get('path'), list) else []
    level = str(ctx.get('level') or payload.get('level') or '').strip().lower()
    obj = ctx.get('object_name') or payload.get('object_name')

    filt: Dict[str, Any] = {}
    if period:
        filt['period'] = period

    # Prefer explicit path because it preserves ownership chain.
    clean_path = [str(x).strip() for x in path if str(x).strip()]
    if clean_path and clean_path[0].lower() in {'бизнес', 'business'}:
        chain = clean_path[1:]
    else:
        chain = clean_path

    if level == 'business':
        return filt

    mapping = ['manager_top', 'manager', 'network', 'category', 'tmc_group', 'sku']
    for key, value in zip(mapping, chain):
        filt[key] = value

    # Fallback for direct screens whose path is incomplete.
    level_to_key = {
        'manager_top': 'manager_top', 'top_manager': 'manager_top',
        'manager': 'manager', 'network': 'network', 'contract': 'network',
        'category': 'category', 'tmc_group': 'tmc_group', 'format': 'tmc_group',
        'sku': 'sku', 'product': 'sku',
    }
    key = level_to_key.get(level)
    if key and obj and key not in filt:
        filt[key] = obj
    return filt


def build_active_workspace_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    actions = build_workspace_action_map(payload)
    state = {
        'state_version': 'W15_ACTIVE_WORKSPACE_STATE_V2',
        'source_of_truth': 'last_displayed_workspace',
        'workspace_level': ctx.get('level') or payload.get('level'),
        'object_name': ctx.get('object_name') or payload.get('object_name'),
        'period': ctx.get('period') or payload.get('period'),
        'path': payload.get('path') if isinstance(payload.get('path'), list) else [],
        'filter': infer_filter_from_payload(payload),
        'render_mode': payload.get('render_mode'),
        'action_map': actions,
        'numeric_command_rule': 'Resolve 1-N only against action_map from the last displayed workspace_markdown menu.',
        'context_rule': 'Use this active_workspace_state for all free dialogue and local commands until the user explicitly opens another object.',
    }
    return state


def apply_runtime_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    if payload.get('status') == 'error':
        return payload
    payload = ensure_runtime_workspace_rendering(payload)
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    render_mode = str(payload.get('render_mode') or '').strip().lower()
    # Attach to object workspaces and action workspaces; not necessary for pure start/diagnostic errors.
    if render_mode in {'start', 'voice_diagnostic'} or str(ctx.get('level') or '').strip().lower() == 'start':
        return payload
    state = build_active_workspace_state(payload)
    payload['active_workspace_state'] = state
    payload['workspace_action_map'] = state.get('action_map', [])
    payload['workspace_runtime_contract'] = {
        'version': 'W15_1_RUNTIME_NAVIGATION_CONTRACT',
        'rendering_authority': 'workspace_markdown is the only user-visible Workspace artifact.',
        'navigation_authority': 'numeric commands resolve only against active_workspace_state.action_map extracted from the visible workspace_markdown menu.',
        'state_authority': 'active_workspace_state is the primary source of context for free dialogue and local commands until explicit object change.',
        'gpt_role': ['render_workspace_markdown_verbatim', 'maintain_active_state', 'continue_conversation'],
        'gpt_must_not': ['rewrite_workspace', 'shorten_tables', 'reorder_sections', 'compose_workspace_from_blocks', 'render_summary_block', 'render_kpi_block', 'render_navigation_block'],
    }
    instruction = payload.get('workspace_render_instruction') or ''
    payload['workspace_render_instruction'] = (
        'Показать только workspace_markdown пользователю полностью и без изменений. '
        'Не показывать summary_block, kpi_block, diagnosis_block, navigation_block и другие служебные блоки. '
        'Если workspace_markdown отсутствует или пустой — считать Workspace не сформированным. '
        'После показа использовать active_workspace_state как источник контекста, а active_workspace_state.action_map как источник истины для цифровых команд 1-N. '
        'Если пользователь говорит «зафиксируй / запиши в журнал / это баг / это неудобно», вызвать API-команду Development Journal.'
    )
    if instruction and instruction not in payload['workspace_render_instruction']:
        payload['workspace_render_instruction_legacy'] = instruction
    return payload


def get_action_from_state(screen: Dict[str, Any], number: int) -> Optional[Dict[str, Any]]:
    if not isinstance(screen, dict) or number < 1:
        return None
    state = screen.get('active_workspace_state') if isinstance(screen.get('active_workspace_state'), dict) else {}
    actions = state.get('action_map') if isinstance(state.get('action_map'), list) else screen.get('workspace_action_map')
    if not isinstance(actions, list):
        return None
    for action in actions:
        if isinstance(action, dict) and int(action.get('number') or 0) == number:
            return action
    return None
