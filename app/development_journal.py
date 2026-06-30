"""Development Journal for VECTRA.

File-backed production feedback journal with explicit capture/export routes,
Dry Run / Test support and a minimal lifecycle for safe Production smoke tests.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

JOURNAL_FILE = Path('/tmp/vectra_development_journal.json')
JOURNAL_LOCK = Lock()

CAPTURE_PREFIXES = (
    'зафиксируй', 'зафиксировать', 'фиксируй', 'запиши', 'запиши в журнал',
    'добавь в журнал', 'записать в журнал', 'записать', 'добавь в развитие',
    'добавить в развитие', 'это баг', 'баг', 'ошибка', 'это ошибка',
    'это неудобно', 'неудобно', 'это нужно улучшить', 'нужно улучшить',
    'надо улучшить', 'надо исправить', 'нужно исправить',
)

SHOW_COMMANDS = {
    'журнал развития', 'показать журнал развития', 'открыть журнал развития',
    'development journal', 'показать development journal',
}

EXPORT_COMMANDS = {
    'экспорт журнала', 'экспортировать журнал', 'экспорт журнала развития',
    'выгрузка журнала развития', 'подготовить журнал развития',
    'экспортировать журнал развития', 'выгрузить журнал развития',
    'получить журнал развития', 'получить development journal',
    'экспорт production журнала', 'экспорт production journal',
}

EXPORT_ALL_COMMANDS = {
    'экспорт журнала включая тест', 'экспорт журнала с тестами',
    'экспортировать журнал включая test', 'экспортировать весь журнал',
}

DRY_RUN_MARKERS = (
    'dry run', 'dry-run', 'дрaй ран', 'драй ран', 'сухой прогон',
    'тест журнала', 'проверка журнала', 'тестовая запись журнала',
    'sandbox journal', 'test journal', 'sandbox',
)

TEST_MARKERS = (
    ' test', ' тест', ' тестовая', ' тестовый', '[test]', '#test', 'пометить как test'
)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize(value: Any) -> str:
    text = str(value or '').strip().lower().replace('ё', 'е')
    text = text.replace('—', '-').replace('–', '-')
    text = re.sub(r'\s+', ' ', text)
    return text


def _match_prefix(norm: str, prefix: str) -> bool:
    return norm == prefix or norm.startswith(prefix + ' ') or norm.startswith(prefix + ':') or norm.startswith(prefix + ' -')


def is_capture_command(message: Any) -> bool:
    norm = _normalize(message)
    if is_dry_run_command(message):
        return True
    return any(_match_prefix(norm, prefix) for prefix in CAPTURE_PREFIXES)


def is_show_command(message: Any) -> bool:
    return _normalize(message) in SHOW_COMMANDS


def is_export_command(message: Any) -> bool:
    norm = _normalize(message)
    return norm in EXPORT_COMMANDS or norm in EXPORT_ALL_COMMANDS


def is_export_all_command(message: Any) -> bool:
    return _normalize(message) in EXPORT_ALL_COMMANDS


def is_dry_run_command(message: Any) -> bool:
    norm = _normalize(message)
    if not norm:
        return False
    if any(marker in norm for marker in DRY_RUN_MARKERS):
        return any(prefix in norm for prefix in CAPTURE_PREFIXES) or 'журнал' in norm or 'journal' in norm
    return False


def is_lifecycle_command(message: Any) -> bool:
    norm = _normalize(message)
    return bool(re.search(r'\bdev-?\d{1,6}\b', norm)) and any(
        word in norm for word in (
            'удалить', 'архивировать', 'архив', 'пометить', 'mark', 'test', 'production', 'восстановить'
        )
    )


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
        if _match_prefix(norm, prefix):
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


def _record_from_message(message: Any, session_ctx: Optional[Dict[str, Any]], session_id: str, records: List[Dict[str, Any]]) -> Dict[str, Any]:
    record_type = _classify(message)
    is_test = any(marker in f' {_normalize(message)} ' for marker in TEST_MARKERS) or is_dry_run_command(message)
    return {
        'id': _next_id(records),
        'type': record_type,
        'status': 'Test' if is_test else 'New',
        'priority': _priority(record_type, message),
        'description': _extract_description(message),
        'raw_message': str(message or '').strip(),
        'context': _screen_context(session_ctx),
        'session_id': session_id,
        'is_test': bool(is_test),
        'persisted': True,
        'created_at': _now(),
        'updated_at': _now(),
        'source': 'Production VECTRA capture command',
        'history': [{'at': _now(), 'event': 'created', 'note': 'Created from Production capture command.'}],
    }


def add_record(message: Any, session_ctx: Optional[Dict[str, Any]], session_id: str = 'default', dry_run: bool = False) -> Dict[str, Any]:
    with JOURNAL_LOCK:
        records = _read_records()
        record = _record_from_message(message, session_ctx, session_id, records)
        if dry_run or is_dry_run_command(message):
            record['id'] = f'DRYRUN-{record["id"]}'
            record['status'] = 'Dry Run'
            record['is_test'] = True
            record['persisted'] = False
            record['history'].append({'at': _now(), 'event': 'dry_run', 'note': 'Routing tested; record was not persisted.'})
            return record
        records.append(record)
        _write_records(records)
        return record


def list_records(status: Optional[str] = None, include_test: bool = True, include_archived: bool = True) -> List[Dict[str, Any]]:
    with JOURNAL_LOCK:
        records = _read_records()
    if status:
        records = [r for r in records if _normalize(r.get('status')) == _normalize(status)]
    if not include_test:
        records = [r for r in records if not r.get('is_test') and _normalize(r.get('status')) not in {'test', 'dry run'}]
    if not include_archived:
        records = [r for r in records if _normalize(r.get('status')) != 'archived']
    return records


def _find_record(records: List[Dict[str, Any]], record_id: str) -> Optional[Dict[str, Any]]:
    norm = _normalize(record_id).replace('dev', 'dev-')
    m = re.search(r'dev-?(\d{1,6})', norm)
    target = f'DEV-{int(m.group(1)):04d}' if m else record_id.upper()
    for record in records:
        if str(record.get('id') or '').upper() == target:
            return record
    return None


def update_record_lifecycle(message: Any) -> Dict[str, Any]:
    norm = _normalize(message)
    match = re.search(r'dev-?(\d{1,6})', norm)
    record_id = f'DEV-{int(match.group(1)):04d}' if match else ''
    with JOURNAL_LOCK:
        records = _read_records()
        record = _find_record(records, record_id)
        if not record:
            return build_lifecycle_response(None, action='not_found', record_id=record_id)
        if 'удалить' in norm:
            records = [r for r in records if str(r.get('id') or '').upper() != record_id]
            _write_records(records)
            return build_lifecycle_response({'id': record_id}, action='deleted', record_id=record_id)
        if 'архив' in norm:
            record['status'] = 'Archived'
            action = 'archived'
        elif 'test' in norm or 'тест' in norm:
            record['is_test'] = True
            record['status'] = 'Test'
            action = 'marked_test'
        elif 'production' in norm or 'рабоч' in norm:
            record['is_test'] = False
            if _normalize(record.get('status')) == 'test':
                record['status'] = 'New'
            action = 'marked_production'
        elif 'восстановить' in norm:
            record['status'] = 'New'
            action = 'restored'
        else:
            action = 'updated'
        record['updated_at'] = _now()
        record.setdefault('history', []).append({'at': _now(), 'event': action, 'note': f'Lifecycle command: {message}'})
        _write_records(records)
        return build_lifecycle_response(record, action=action, record_id=record_id)


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
    for key in ('level', 'object_name', 'period'):
        if ctx.get(key):
            parts.append(str(ctx.get(key)))
    return ' / '.join(parts) if parts else '—'


def build_capture_response(record: Dict[str, Any]) -> Dict[str, Any]:
    if not record.get('persisted', True):
        lines = [
            'Development Journal routing OK.',
            'Dry-run completed.',
            'Record not persisted.',
            '',
            f'**Test ID:** {record.get("id")}',
            f'**Тип:** {record.get("type")}',
            f'**Контекст:** {_fmt_context(record.get("context") or {})}',
        ]
    else:
        lines = [
            f'Записал в журнал развития. {record.get("id")}',
            '',
            '## ✅ Записал в журнал развития',
            '',
            f'**ID:** {record.get("id")}',
            f'**Тип:** {record.get("type")}',
            f'**Приоритет:** {record.get("priority")}',
            f'**Статус:** {record.get("status")}',
            f'**TEST:** {"да" if record.get("is_test") else "нет"}',
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
        'navigation_block': ['журнал развития — показать накопленные записи', 'экспорт журнала развития — экспорт Production-записей'],
        'development_journal_record': record,
    }


def build_lifecycle_response(record: Optional[Dict[str, Any]], action: str, record_id: str = '') -> Dict[str, Any]:
    action_map = {
        'deleted': 'Запись удалена.',
        'archived': 'Запись архивирована.',
        'marked_test': 'Запись помечена как TEST.',
        'marked_production': 'Запись помечена как Production.',
        'restored': 'Запись восстановлена.',
        'not_found': 'Запись не найдена.',
    }
    lines = [f'## Development Journal', action_map.get(action, 'Запись обновлена.')]
    if record_id:
        lines.append(f'**ID:** {record_id}')
    return {
        'status': 'ok' if action != 'not_found' else 'error',
        'reason': None if action != 'not_found' else 'journal_record_not_found',
        'render_mode': 'development_journal',
        'context': {'level': 'development_journal', 'object_name': 'Development Journal', 'period': None},
        'kpi_block': [],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': ['показать журнал развития', 'экспорт журнала развития'],
        'development_journal_record': record or {'id': record_id},
    }


def build_journal_response(export: bool = False, include_test: bool = False) -> Dict[str, Any]:
    records = list_records(include_test=include_test, include_archived=include_test)
    records_sorted = sorted(records, key=lambda r: str(r.get('created_at') or ''), reverse=True)
    counts_by_type = _counts(records, 'type')
    counts_by_status = _counts(records, 'status')
    title = '📤 Экспорт журнала развития VECTRA' if export else '📒 Журнал развития VECTRA'
    scope = 'Все записи, включая TEST/Archived' if include_test else 'Production-записи, TEST/Archived исключены'
    lines: List[str] = [
        f'# {title}', '', f'Режим: **{scope}**', f'Всего записей: **{len(records)}**', '', '## Сводка по типам'
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
        lines.append('Записей пока нет. Для проверки используйте: **dry run journal: это баг**')
    else:
        for r in records_sorted[:50 if not export else len(records_sorted)]:
            lines += [
                '', f'### {r.get("id")} — {r.get("type")}',
                f'- **Статус:** {r.get("status")}',
                f'- **TEST:** {"да" if r.get("is_test") else "нет"}',
                f'- **Приоритет:** {r.get("priority")}',
                f'- **Контекст:** {_fmt_context(r.get("context") or {})}',
                f'- **Описание:** {r.get("description")}',
                f'- **Создано:** {r.get("created_at")}',
            ]
    if export:
        lines += ['', '## Инструкция для инженерного чата', 'Использовать только Production-записи как вход для Product Review. TEST и Archived не включаются в Sprint Planning.']
    else:
        lines += ['', '## Доступные действия', '1. **экспорт журнала развития** — экспорт Production-записей.', '2. **экспорт журнала включая тест** — служебная проверка.', '3. **dry run journal: это баг** — smoke-test без сохранения.']
    return {
        'status': 'ok',
        'render_mode': 'development_journal_export' if export else 'development_journal',
        'context': {'level': 'development_journal', 'object_name': 'Development Journal', 'period': None},
        'kpi_block': [],
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': ['экспорт журнала развития — Production export', 'dry run journal: это баг — тест маршрута без записи'],
        'development_journal': {'records_count': len(records), 'records': records_sorted if export else records_sorted[:50], 'include_test': include_test},
    }
