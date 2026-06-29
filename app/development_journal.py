"""Development Journal MVP for VECTRA.

W11.1 implements the first working contour between Production VECTRA and
engineering development: capture observations from the current Workspace, keep
an append-only journal, render it, and export it as a Product Review packet.

The module is deliberately file-backed for the current FastAPI/Render prototype.
A future sprint can replace the storage adapter with a database without changing
orchestration commands.
"""

from __future__ import annotations

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

JOURNAL_FILE = Path('/tmp/vectra_development_journal.json')
JOURNAL_LOCK = Lock()

CAPTURE_PREFIXES = (
    'зафиксируй',
    'зафиксировать',
    'запиши',
    'запиши в журнал',
    'записать в журнал',
    'записать',
    'добавь в развитие',
    'добавить в развитие',
    'это баг',
    'баг',
    'ошибка',
    'это ошибка',
    'это неудобно',
    'неудобно',
    'это нужно улучшить',
    'нужно улучшить',
    'надо улучшить',
)

SHOW_COMMANDS = {
    'журнал развития',
    'показать журнал развития',
    'открыть журнал развития',
    'development journal',
    'показать development journal',
}

EXPORT_COMMANDS = {
    'экспорт журнала',
    'экспортировать журнал',
    'экспорт журнала развития',
    'выгрузка журнала развития',
    'подготовить журнал развития',
    'экспортировать журнал развития',
    'выгрузить журнал развития',
    'получить журнал развития',
    'получить development journal',
}


def _normalize(value: Any) -> str:
    text = str(value or '').strip().lower().replace('ё', 'е')
    text = re.sub(r'\s+', ' ', text)
    return text


def is_capture_command(message: Any) -> bool:
    norm = _normalize(message)
    return any(norm == prefix or norm.startswith(prefix + ' ') or norm.startswith(prefix + ':') or norm.startswith(prefix + ' —') or norm.startswith(prefix + ' -') for prefix in CAPTURE_PREFIXES)


def is_show_command(message: Any) -> bool:
    return _normalize(message) in SHOW_COMMANDS


def is_export_command(message: Any) -> bool:
    return _normalize(message) in EXPORT_COMMANDS


def _read_records() -> List[Dict[str, Any]]:
    if not JOURNAL_FILE.exists():
        return []
    try:
        raw = json.loads(JOURNAL_FILE.read_text(encoding='utf-8'))
        if isinstance(raw, list):
            return [x for x in raw if isinstance(x, dict)]
    except Exception:
        return []
    return []


def _write_records(records: List[Dict[str, Any]]) -> None:
    JOURNAL_FILE.write_text(json.dumps(records, ensure_ascii=False, indent=2), encoding='utf-8')


def _next_id(records: List[Dict[str, Any]]) -> str:
    max_num = 0
    for item in records:
        raw = str(item.get('id') or '')
        m = re.search(r'(\d+)$', raw)
        if m:
            max_num = max(max_num, int(m.group(1)))
    return f'DEV-{max_num + 1:04d}'


def _extract_description(message: Any) -> str:
    text = str(message or '').strip()
    norm = _normalize(text)
    selected = ''
    for prefix in sorted(CAPTURE_PREFIXES, key=len, reverse=True):
        if norm == prefix:
            selected = text
            break
        if norm.startswith(prefix + ':') or norm.startswith(prefix + ' -') or norm.startswith(prefix + ' —') or norm.startswith(prefix + ' '):
            # Cut by original prefix length; then remove separators.
            selected = text[len(prefix):].strip(' :-—')
            break
    if not selected or _normalize(selected) in {_normalize(x) for x in CAPTURE_PREFIXES}:
        return 'Пользователь зафиксировал замечание по текущему рабочему столу без дополнительного описания.'
    return selected


def _classify(message: Any) -> str:
    norm = _normalize(message)
    if any(word in norm for word in ('баг', 'ошибк', 'не работает', 'сломал', 'падает', 'не открывает', 'не распозна')):
        return 'Engineering Bug'
    if any(word in norm for word in ('данных не хватает', 'не хватает данных', 'нет данных', 'нужно поле', 'нужны данные')):
        return 'Missing Data'
    if any(word in norm for word in ('архитектур', 'принцип', 'sources', 'источник', 'модель')):
        return 'Architecture Improvement'
    if any(word in norm for word in ('неудоб', 'непонят', 'экран', 'интерфейс', 'навигац')):
        return 'UX Improvement'
    if any(word in norm for word in ('улучш', 'добав', 'развит', 'идея')):
        return 'Product Improvement'
    return 'Product Observation'


def _priority(record_type: str, message: Any) -> str:
    norm = _normalize(message)
    if any(word in norm for word in ('критич', 'невозможно', 'не могу работать', 'блокер', 'срочно')):
        return 'High'
    if record_type in {'Engineering Bug', 'Missing Data', 'Architecture Improvement'}:
        return 'Medium'
    return 'Normal'


def _screen_context(session_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ctx = session_ctx or {}
    screen = ctx.get('current_screen') or ctx.get('last_payload') or {}
    screen_context = screen.get('context') if isinstance(screen, dict) else {}
    if not isinstance(screen_context, dict):
        screen_context = {}
    return {
        'level': screen_context.get('level') or ctx.get('scope_level'),
        'object_name': screen_context.get('object_name') or ctx.get('scope_object_name'),
        'period': screen_context.get('period') or ctx.get('period_current'),
        'parent_object': screen_context.get('parent_object'),
        'path': screen.get('path') if isinstance(screen, dict) else [],
        'render_mode': screen.get('render_mode') if isinstance(screen, dict) else None,
    }


def add_record(message: Any, session_ctx: Optional[Dict[str, Any]], session_id: str = 'default') -> Dict[str, Any]:
    with JOURNAL_LOCK:
        records = _read_records()
        record_type = _classify(message)
        record = {
            'id': _next_id(records),
            'type': record_type,
            'status': 'New',
            'priority': _priority(record_type, message),
            'description': _extract_description(message),
            'raw_message': str(message or '').strip(),
            'context': _screen_context(session_ctx),
            'session_id': session_id,
            'created_at': datetime.now(timezone.utc).isoformat(),
            'updated_at': datetime.now(timezone.utc).isoformat(),
            'source': 'Production VECTRA capture command',
            'history': [
                {
                    'at': datetime.now(timezone.utc).isoformat(),
                    'event': 'created',
                    'note': 'Created from Production capture command.',
                }
            ],
        }
        records.append(record)
        _write_records(records)
        return record


def list_records(status: Optional[str] = None) -> List[Dict[str, Any]]:
    with JOURNAL_LOCK:
        records = _read_records()
    if status:
        return [r for r in records if _normalize(r.get('status')) == _normalize(status)]
    return records


def _counts(records: List[Dict[str, Any]], key: str) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for r in records:
        label = str(r.get(key) or 'Unknown')
        out[label] = out.get(label, 0) + 1
    return out


def _fmt_context(ctx: Dict[str, Any]) -> str:
    if not isinstance(ctx, dict):
        return '—'
    parts = []
    level = ctx.get('level')
    obj = ctx.get('object_name')
    period = ctx.get('period')
    if level:
        parts.append(str(level))
    if obj:
        parts.append(str(obj))
    if period:
        parts.append(str(period))
    return ' / '.join(parts) if parts else '—'


def build_capture_response(record: Dict[str, Any]) -> Dict[str, Any]:
    lines = [
        f'## ✅ Записал в журнал развития',
        '',
        f'**ID:** {record.get("id")}',
        f'**Тип:** {record.get("type")}',
        f'**Приоритет:** {record.get("priority")}',
        f'**Контекст:** {_fmt_context(record.get("context") or {})}',
        '',
        'Запись сохранена. Можно продолжать работу в текущем рабочем столе.',
    ]
    return {
        'status': 'ok',
        'render_mode': 'development_journal_capture',
        'context': {'level': 'development_journal', 'object_name': 'Development Journal', 'period': None},
        'kpi_block': [],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': ['журнал развития — показать накопленные записи', 'экспорт журнала развития — подготовить пакет для инженерного чата'],
        'development_journal_record': record,
    }


def build_journal_response(export: bool = False) -> Dict[str, Any]:
    records = list_records()
    records_sorted = sorted(records, key=lambda r: str(r.get('created_at') or ''), reverse=True)
    counts_by_type = _counts(records, 'type')
    counts_by_status = _counts(records, 'status')
    title = '📤 Экспорт журнала развития VECTRA' if export else '📒 Журнал развития VECTRA'
    lines: List[str] = [
        f'# {title}',
        '',
        f'Всего записей: **{len(records)}**',
        '',
        '## Сводка по типам',
    ]
    if counts_by_type:
        for key, value in sorted(counts_by_type.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f'- **{key}:** {value}')
    else:
        lines.append('- Записей пока нет.')
    lines += ['', '## Сводка по статусам']
    if counts_by_status:
        for key, value in sorted(counts_by_status.items(), key=lambda x: (-x[1], x[0])):
            lines.append(f'- **{key}:** {value}')
    else:
        lines.append('- Записей пока нет.')

    lines += ['', '## Последние записи']
    if not records_sorted:
        lines.append('Записей пока нет. Во время работы в Production скажите: **зафиксируй** или **это баг: ...**')
    else:
        for r in records_sorted[:50 if not export else len(records_sorted)]:
            lines += [
                '',
                f'### {r.get("id")} — {r.get("type")}',
                f'- **Статус:** {r.get("status")}',
                f'- **Приоритет:** {r.get("priority")}',
                f'- **Контекст:** {_fmt_context(r.get("context") or {})}',
                f'- **Описание:** {r.get("description")}',
                f'- **Создано:** {r.get("created_at")}',
            ]
    if export:
        lines += [
            '',
            '## Инструкция для инженерного чата',
            'Использовать этот журнал как вход для Product Review. Сначала классифицировать записи на Engineering Bug / Product Gap / Synchronization Gap / Future Enhancement, затем сформировать утверждённый Sprint.',
        ]
    else:
        lines += [
            '',
            '## Доступные действия',
            '1. **экспорт журнала развития** — подготовить пакет для инженерного чата.',
            '2. **зафиксируй: ...** — добавить новую запись из текущего Workspace.',
        ]

    return {
        'status': 'ok',
        'render_mode': 'development_journal_export' if export else 'development_journal',
        'context': {'level': 'development_journal', 'object_name': 'Development Journal', 'period': None},
        'kpi_block': [],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': ['экспорт журнала развития — подготовить пакет для инженерного чата'],
        'development_journal': {'records_count': len(records), 'records': records_sorted if export else records_sorted[:50]},
    }
