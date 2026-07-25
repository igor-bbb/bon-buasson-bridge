"""First User Experience routing contracts for VECTRA.

W14.3 protects two foundational Production scenarios:
1. opening the first Workspace must go through API orchestration;
2. Development Journal capture phrases must never fall through to analytics.

This module is intentionally small and dependency-light so the contract can be
used by orchestration and tests without creating another calculation path.
"""

from __future__ import annotations

import re
from typing import Any, Dict

from app.development_journal import is_capture_command, is_export_command, is_show_command, is_dry_run_command, is_lifecycle_command, is_dialogue_review_command


def normalize_message(value: Any) -> str:
    text = str(value or '').strip().lower().replace('ё', 'е')
    text = text.replace('—', '-').replace('–', '-')
    text = re.sub(r'\s+', ' ', text)
    return text


WORKSPACE_OPENING_ACTION_WORDS = (
    'покажи',
    'показать',
    'открой',
    'открыть',
    'разбери',
    'разобрать',
    'дай',
)

NON_WORKSPACE_LOCAL_COMMANDS = {
    'начать анализ',
    'start analysis',
    'все',
    'показать все',
    'полный список',
    'назад',
    'причины',
    'причина',
    'разбор',
    'разбор причин',
    'kpi',
    'кпи',
    'помощь',
}

JOURNAL_ROUTING_PHRASES = (
    'зафиксируй',
    'зафиксировать',
    'фиксируй',
    'запиши',
    'запиши в журнал',
    'записать в журнал',
    'добавь в развитие',
    'добавить в развитие',
    'это баг',
    'баг',
    'это ошибка',
    'ошибка',
    'это неудобно',
    'неудобно',
    'добавь в журнал',
    'надо исправить',
    'нужно исправить',
    'это нужно улучшить',
    'нужно улучшить',
)


def is_development_journal_intent(message: Any) -> bool:
    """Strict journal intent guard used before any analytics routing."""
    norm = normalize_message(message)
    if is_capture_command(message) or is_export_command(message) or is_show_command(message) or is_dry_run_command(message) or is_lifecycle_command(message) or is_dialogue_review_command(message):
        return True
    return any(
        norm == phrase
        or norm.startswith(phrase + ' ')
        or norm.startswith(phrase + ':')
        or norm.startswith(phrase + ' -')
        for phrase in JOURNAL_ROUTING_PHRASES
    )


def is_workspace_opening_intent(message: Any) -> bool:
    """Heuristic guard for requests that must reach API orchestration.

    It does not route the request itself and does not calculate anything. It only
    identifies user messages for which a local GPT-only answer is forbidden.
    """
    norm = normalize_message(message)
    if not norm or norm in NON_WORKSPACE_LOCAL_COMMANDS or norm.isdigit():
        return False
    if is_development_journal_intent(message):
        return False
    if re.search(r'\b20\d{2}[-./](0[1-9]|1[0-2])\b', norm):
        return True
    if re.search(r'\b(январ|феврал|март|апрел|май|мая|июн|июл|август|сентябр|октябр|ноябр|декабр)', norm):
        return True
    if norm.startswith(WORKSPACE_OPENING_ACTION_WORDS):
        return True
    if norm in {'бизнес'}:
        return True
    # Bare object names such as "Труш Максим" or "Варус" are intentionally
    # treated as workspace-opening candidates by the lower resolver.
    if len(norm) >= 3 and not norm.endswith('?') and '?' not in norm:
        analytical_words = ('почему', 'что ', 'где ', 'какие ', 'какой ', 'как ', 'стоит ли')
        if not norm.startswith(analytical_words):
            return True
    return False


def build_workspace_api_attempt_error(message: Any, reason: str) -> Dict[str, Any]:
    """Explicit API-attempt failure payload for Workspace intents.

    Used only after the request has entered the API/orchestration layer. This
    prevents silent local refusals and makes failures visible to Production.
    """
    return {
        'status': 'error',
        'reason': reason or 'workspace_api_attempt_failed',
        'render_mode': 'workspace_api_attempt_error',
        'api_attempted': True,
        'workspace_intent_detected': True,
        'workspace_primary_block': [
            '## Не удалось открыть рабочий стол',
            '',
            'Запрос был передан в API, но рабочий стол не был построен.',
            '',
            f'**Запрос:** {str(message or "").strip()}',
            f'**Причина:** {reason or "workspace_api_attempt_failed"}',
        ],
        'workspace_markdown': '\n'.join([
            '## Не удалось открыть рабочий стол',
            '',
            'Запрос был передан в API, но рабочий стол не был построен.',
            '',
            f'**Запрос:** {str(message or "").strip()}',
            f'**Причина:** {reason or "workspace_api_attempt_failed"}',
        ]),
        'context': {'level': 'workspace_opening_error', 'object_name': None, 'period': None},
        'navigation_block': [],
    }
