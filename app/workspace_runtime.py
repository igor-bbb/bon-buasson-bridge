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
)

STOP_HEADERS = (
    '# ', '## ', '### ', '---', '⸻'
)


def _line_to_text(line: Any) -> str:
    return str(line or '').strip()


def _extract_markdown(payload: Dict[str, Any]) -> str:
    markdown = payload.get('workspace_markdown')
    if isinstance(markdown, str) and markdown.strip():
        return markdown
    primary = payload.get('workspace_primary_block')
    if isinstance(primary, list):
        return '\n'.join(str(x) for x in primary if str(x or '').strip())
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
    if any(x in norm for x in ('полная витрина', 'полную витрину', 'показать все', 'все объекты', 'полный список', 'витрина')):
        return 'show_all'
    if any(x in norm for x in ('причины', 'факторы', 'разобрать влияние')):
        return 'reasons'
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

    # Fallback to navigation_block only if the Workspace did not publish a visible menu.
    nav = payload.get('navigation_block')
    out: List[Dict[str, Any]] = []
    if isinstance(nav, list):
        for idx, line in enumerate(nav, start=1):
            text = str(line or '').strip()
            if not text:
                continue
            # Split "команда — пояснение" and keep business-facing text.
            label = text
            if '—' in text:
                left, right = text.split('—', 1)
                if left.strip().isdigit():
                    label = right.strip()
                else:
                    label = text.strip()
            out.append({
                'number': idx,
                'label': label,
                'normalized_label': normalize_text(label),
                'source_line': text,
                'action_type': classify_action_label(label),
            })
    return out[:20]


def build_active_workspace_state(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    actions = build_workspace_action_map(payload)
    state = {
        'state_version': 'W14_ACTIVE_WORKSPACE_STATE_V1',
        'source_of_truth': 'last_displayed_workspace',
        'workspace_level': ctx.get('level') or payload.get('level'),
        'object_name': ctx.get('object_name') or payload.get('object_name'),
        'period': ctx.get('period') or payload.get('period'),
        'path': payload.get('path') if isinstance(payload.get('path'), list) else [],
        'render_mode': payload.get('render_mode'),
        'action_map': actions,
        'numeric_command_rule': 'Resolve 1-N against action_map from the last displayed Workspace menu before using navigation_block or all_block.',
    }
    return state


def apply_runtime_contract(payload: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(payload, dict):
        return payload
    if payload.get('status') == 'error':
        return payload
    ctx = payload.get('context') if isinstance(payload.get('context'), dict) else {}
    render_mode = str(payload.get('render_mode') or '').strip().lower()
    # Attach to object workspaces and action workspaces; not necessary for pure start/diagnostic errors.
    if render_mode in {'start', 'voice_diagnostic'} or str(ctx.get('level') or '').strip().lower() == 'start':
        return payload
    state = build_active_workspace_state(payload)
    payload['active_workspace_state'] = state
    payload['workspace_action_map'] = state.get('action_map', [])
    payload['workspace_runtime_contract'] = {
        'version': 'W14_WORKSPACE_RUNTIME_ARCHITECTURE_V1',
        'rendering_authority': 'workspace_markdown is immutable canonical user-visible Workspace.',
        'navigation_authority': 'numeric commands resolve against active_workspace_state.action_map from the displayed menu.',
        'state_authority': 'current_screen / active_workspace_state is the source of truth for local commands.',
        'gpt_role': ['render_workspace', 'maintain_active_state', 'resolve_navigation', 'continue_conversation'],
        'gpt_must_not': ['rewrite_workspace', 'shorten_tables', 'reorder_sections', 'replace_workspace_with_own_summary', 'resolve_numbers_from_hidden_navigation_block_first'],
    }
    instruction = payload.get('workspace_render_instruction') or ''
    payload['workspace_render_instruction'] = (
        'Показать workspace_markdown пользователю полностью и без изменений. '
        'Не пересобирать, не сокращать, не переставлять разделы и таблицы. '
        'После показа использовать active_workspace_state.action_map как источник истины для цифровых команд 1-N. '
        'Если пользователь говорит «зафиксируй / запиши в журнал / это баг / это неудобно», вызвать API-команду Development Journal, а не вести заметку в чате.'
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
