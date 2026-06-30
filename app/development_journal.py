"""Development Journal Architecture v2.0 for VECTRA.

The journal is internal engineering memory. It stores normalized technical
records and aggregated runtime problems, not user chat messages.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

JOURNAL_FILE = Path('/tmp/vectra_development_journal_v2.json')
LEGACY_JOURNAL_FILE = Path('/tmp/vectra_development_journal.json')
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
    'dry run', 'dry-run', 'драй ран', 'сухой прогон', 'тест журнала',
    'проверка журнала', 'тестовая запись журнала', 'sandbox journal',
    'test journal', 'sandbox',
)

TEST_MARKERS = (' test', ' тест', ' тестовая', ' тестовый', '[test]', '#test', 'пометить как test')

RUNTIME_EVENT_PRIORITIES = {
    'workspace_markdown_missing': 'P0',
    'workspace_generation_error': 'P0',
    'active_workspace_state_lost': 'P0',
    'runtime_contract_violation': 'P0',
    'navigation_contract_violation': 'P0',
    'workspace_action_map_empty': 'P0',
    'context_lost': 'P0',
    'ambiguous_command_routing': 'P0',
    'workspace_incomplete': 'P0',
    'runtime_internal_error': 'P0',
    'intent_resolution_failure': 'P1',
    'cannot_parse_message': 'P1',
    'user_experience_signal': 'P1',
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _normalize(value: Any) -> str:
    text = str(value or '').strip().lower().replace('ё', 'е')
    text = text.replace('—', '-').replace('–', '-')
    text = re.sub(r'\s+', ' ', text)
    return text


def _stable_hash(value: Any) -> str:
    try:
        raw = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
    except Exception:
        raw = str(value)
    return hashlib.sha256(raw.encode('utf-8')).hexdigest()[:16]


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
        word in norm for word in ('удалить', 'архивировать', 'архив', 'пометить', 'mark', 'test', 'production', 'восстановить')
    )


def _read_records() -> List[Dict[str, Any]]:
    source = JOURNAL_FILE if JOURNAL_FILE.exists() else LEGACY_JOURNAL_FILE
    if not source.exists():
        return []
    try:
        raw = json.loads(source.read_text(encoding='utf-8'))
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


def _screen_context(session_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ctx = session_ctx or {}
    screen = ctx.get('current_screen') or ctx.get('last_payload') or {}
    screen_context = screen.get('context') if isinstance(screen, dict) else {}
    if not isinstance(screen_context, dict):
        screen_context = {}
    active_state = screen.get('active_workspace_state') if isinstance(screen, dict) and isinstance(screen.get('active_workspace_state'), dict) else {}
    return {
        'level': screen_context.get('level') or ctx.get('scope_level') or active_state.get('workspace_level'),
        'object_name': screen_context.get('object_name') or ctx.get('scope_object_name') or active_state.get('object_name'),
        'period': screen_context.get('period') or ctx.get('period_current') or active_state.get('period'),
        'parent_object': screen_context.get('parent_object'),
        'path': screen.get('path') if isinstance(screen, dict) else [],
        'render_mode': screen.get('render_mode') if isinstance(screen, dict) else None,
    }


def _active_state(session_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    ctx = session_ctx or {}
    screen = ctx.get('current_screen') or ctx.get('last_payload') or {}
    if isinstance(screen, dict) and isinstance(screen.get('active_workspace_state'), dict):
        return screen.get('active_workspace_state') or {}
    return {}


def _message_signal(message: Any) -> Dict[str, Any]:
    """Sanitized user-signal metadata. Never store raw message text."""
    norm = _normalize(message)
    return {
        'normalized_intent_hint': _classify_user_signal(norm),
        'message_hash': _stable_hash(norm),
        'length': len(str(message or '')),
    }


def _classify_user_signal(norm: str) -> str:
    if any(word in norm for word in ('баг', 'ошибк', 'не работает', 'сломал', 'падает', 'не открывает', 'не распозна')):
        return 'engineering_failure_reported_by_user'
    if any(word in norm for word in ('неудоб', 'непонят', 'экран', 'интерфейс', 'навигац')):
        return 'ux_friction_reported_by_user'
    if any(word in norm for word in ('данных не хватает', 'не хватает данных', 'нет данных', 'нужно поле', 'нужны данные')):
        return 'missing_data_reported_by_user'
    if any(word in norm for word in ('улучш', 'добав', 'развит', 'идея')):
        return 'product_improvement_reported_by_user'
    return 'user_feedback_signal'


def _classify(message: Any) -> str:
    return {
        'engineering_failure_reported_by_user': 'Engineering Bug',
        'ux_friction_reported_by_user': 'UX Improvement',
        'missing_data_reported_by_user': 'Missing Data',
        'product_improvement_reported_by_user': 'Product Improvement',
    }.get(_classify_user_signal(_normalize(message)), 'Product Observation')


def _priority(record_type: str, message: Any) -> str:
    norm = _normalize(message)
    if any(word in norm for word in ('критич', 'невозможно', 'не могу работать', 'блокер', 'срочно')):
        return 'P0'
    if record_type in {'Engineering Bug', 'Missing Data', 'Architecture Improvement'}:
        return 'P1'
    return 'P2'


def _technical_reason_from_user_signal(message: Any) -> str:
    signal = _classify_user_signal(_normalize(message))
    mapping = {
        'engineering_failure_reported_by_user': 'User experience signal indicates expected scenario failed or produced incorrect result.',
        'ux_friction_reported_by_user': 'User experience signal indicates friction, unclear flow or interface mismatch.',
        'missing_data_reported_by_user': 'User experience signal indicates required data was absent from current Workspace context.',
        'product_improvement_reported_by_user': 'User experience signal indicates product capability gap or improvement request.',
        'user_feedback_signal': 'User requested Development Journal capture without explicit technical details.',
    }
    return mapping.get(signal, mapping['user_feedback_signal'])


def _fingerprint_record(record: Dict[str, Any]) -> str:
    ctx = record.get('runtime_context') if isinstance(record.get('runtime_context'), dict) else {}
    basis = {
        'event_type': record.get('event_type'),
        'component': record.get('component'),
        'system_level': record.get('system_level'),
        'error_code': record.get('error_code'),
        'technical_reason': record.get('technical_reason'),
        'level': ctx.get('level'),
        'render_mode': ctx.get('render_mode'),
    }
    return _stable_hash(basis)


def _base_record(
    *,
    records: List[Dict[str, Any]],
    event_type: str,
    component: str,
    system_level: str,
    technical_reason: str,
    suspected_root_cause: str,
    priority: str,
    runtime_context: Optional[Dict[str, Any]] = None,
    active_workspace_state: Optional[Dict[str, Any]] = None,
    error_code: Optional[str] = None,
    reproduction_data: Optional[Dict[str, Any]] = None,
    source: str = 'Runtime',
    is_test: bool = False,
    status: str = 'Open',
) -> Dict[str, Any]:
    record = {
        'id': _next_id(records),
        'schema_version': 'DevelopmentJournalRecord/v2.0',
        'event_type': event_type,
        'type': event_type,
        'component': component,
        'system_level': system_level,
        'technical_reason': technical_reason,
        'suspected_root_cause': suspected_root_cause,
        'priority': priority,
        'runtime_context': runtime_context or {},
        'context': runtime_context or {},
        'active_workspace_state': active_workspace_state or {},
        'error_code': error_code,
        'reproduction_data': reproduction_data or {},
        'source': source,
        'status': 'Test' if is_test else status,
        'is_test': bool(is_test),
        'persisted': True,
        'occurrence_count': 1,
        'first_seen_at': _now(),
        'last_seen_at': _now(),
        'created_at': _now(),
        'updated_at': _now(),
        'history': [{'at': _now(), 'event': 'created', 'note': 'Created as normalized engineering record.'}],
    }
    record['fingerprint'] = _fingerprint_record(record)
    return record


def _aggregate_or_append(records: List[Dict[str, Any]], record: Dict[str, Any]) -> Dict[str, Any]:
    fingerprint = record.get('fingerprint') or _fingerprint_record(record)
    for existing in records:
        if existing.get('fingerprint') == fingerprint and not existing.get('is_test') and not record.get('is_test'):
            existing['occurrence_count'] = int(existing.get('occurrence_count') or 1) + 1
            existing['last_seen_at'] = _now()
            existing['updated_at'] = _now()
            existing['status'] = existing.get('status') or 'Open'
            existing['runtime_context'] = record.get('runtime_context') or existing.get('runtime_context') or {}
            existing['context'] = existing.get('runtime_context') or {}
            existing['active_workspace_state'] = record.get('active_workspace_state') or existing.get('active_workspace_state') or {}
            existing['reproduction_data'] = record.get('reproduction_data') or existing.get('reproduction_data') or {}
            existing.setdefault('history', []).append({'at': _now(), 'event': 'aggregated_occurrence', 'note': 'Repeated event aggregated into existing engineering record.'})
            return existing
    records.append(record)
    return record


def add_runtime_event(
    event_type: str,
    component: str,
    system_level: str = 'runtime',
    technical_reason: str = '',
    suspected_root_cause: str = '',
    priority: Optional[str] = None,
    runtime_context: Optional[Dict[str, Any]] = None,
    active_workspace_state: Optional[Dict[str, Any]] = None,
    error_code: Optional[str] = None,
    reproduction_data: Optional[Dict[str, Any]] = None,
    session_id: str = 'default',
    dry_run: bool = False,
    is_test: bool = False,
) -> Dict[str, Any]:
    with JOURNAL_LOCK:
        records = _read_records()
        record = _base_record(
            records=records,
            event_type=event_type,
            component=component,
            system_level=system_level,
            technical_reason=technical_reason or event_type,
            suspected_root_cause=suspected_root_cause or 'Requires laboratory review.',
            priority=priority or RUNTIME_EVENT_PRIORITIES.get(event_type, 'P1'),
            runtime_context=runtime_context or {},
            active_workspace_state=active_workspace_state or {},
            error_code=error_code,
            reproduction_data={**(reproduction_data or {}), 'session_id_hash': _stable_hash(session_id)},
            source='Runtime Event Detector',
            is_test=is_test,
        )
        if dry_run:
            record['id'] = f'DRYRUN-{record["id"]}'
            record['status'] = 'Dry Run'
            record['is_test'] = True
            record['persisted'] = False
            record['history'].append({'at': _now(), 'event': 'dry_run', 'note': 'Routing tested; record was not persisted.'})
            return record
        saved = _aggregate_or_append(records, record)
        _write_records(records)
        return saved


def add_record(message: Any, session_ctx: Optional[Dict[str, Any]], session_id: str = 'default', dry_run: bool = False) -> Dict[str, Any]:
    """Manual user signal capture, converted to technical record without raw quotes."""
    record_type = _classify(message)
    priority = _priority(record_type, message)
    runtime_ctx = _screen_context(session_ctx)
    active_state = _active_state(session_ctx)
    signal = _message_signal(message)
    is_test = any(marker in f' {_normalize(message)} ' for marker in TEST_MARKERS) or is_dry_run_command(message)
    return add_runtime_event(
        event_type='user_experience_signal',
        component='production_gpt_runtime',
        system_level='user_experience',
        technical_reason=_technical_reason_from_user_signal(message),
        suspected_root_cause='Manual feedback command detected; laboratory must classify root cause using runtime context.',
        priority=priority,
        runtime_context=runtime_ctx,
        active_workspace_state=active_state,
        error_code=signal.get('normalized_intent_hint'),
        reproduction_data=signal,
        session_id=session_id,
        dry_run=dry_run or is_dry_run_command(message),
        is_test=is_test,
    )


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
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': ['показать журнал развития', 'экспорт журнала развития'],
        'development_journal_record': record or {'id': record_id},
    }


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
            record['status'] = 'Archived'; action = 'archived'
        elif 'test' in norm or 'тест' in norm:
            record['is_test'] = True; record['status'] = 'Test'; action = 'marked_test'
        elif 'production' in norm or 'рабоч' in norm:
            record['is_test'] = False
            if _normalize(record.get('status')) == 'test':
                record['status'] = 'Open'
            action = 'marked_production'
        elif 'восстановить' in norm:
            record['status'] = 'Open'; action = 'restored'
        else:
            action = 'updated'
        record['updated_at'] = _now()
        record.setdefault('history', []).append({'at': _now(), 'event': action, 'note': 'Lifecycle command applied.'})
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
            f'**Тип события:** {record.get("event_type")}',
            f'**Компонент:** {record.get("component")}',
            f'**Контекст:** {_fmt_context(record.get("runtime_context") or record.get("context") or {})}',
        ]
    else:
        lines = [
            f'Записал в журнал развития. {record.get("id")}',
            '',
            '## ✅ Инженерная запись создана',
            '',
            f'**ID:** {record.get("id")}',
            f'**Тип события:** {record.get("event_type")}',
            f'**Компонент:** {record.get("component")}',
            f'**Приоритет:** {record.get("priority")}',
            f'**Повторов:** {record.get("occurrence_count", 1)}',
            f'**Контекст:** {_fmt_context(record.get("runtime_context") or record.get("context") or {})}',
        ]
    return {
        'status': 'ok',
        'render_mode': 'development_journal_capture',
        'context': {'level': 'development_journal', 'object_name': 'Development Journal', 'period': None},
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': ['журнал развития — показать накопленные записи', 'экспорт журнала развития — экспорт Production-записей'],
        'development_journal_record': record,
    }


def build_journal_response(export: bool = False, include_test: bool = False) -> Dict[str, Any]:
    records = list_records(include_test=include_test, include_archived=include_test)
    records_sorted = sorted(records, key=lambda r: str(r.get('last_seen_at') or r.get('created_at') or ''), reverse=True)
    counts_by_type = _counts(records, 'event_type')
    counts_by_status = _counts(records, 'status')
    title = '📤 Экспорт Development Journal VECTRA' if export else '📒 Development Journal VECTRA'
    scope = 'Все записи, включая TEST/Archived' if include_test else 'Production-записи, TEST/Archived исключены'
    lines: List[str] = [
        f'# {title}', '', f'Режим: **{scope}**', f'Всего инженерных проблем: **{len(records)}**', '', '## Сводка по типам событий'
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
    lines += ['', '## Инженерные записи']
    if not records_sorted:
        lines.append('Записей пока нет. Для smoke-test используйте: **dry run journal: это баг**')
    else:
        for r in records_sorted[:50 if not export else len(records_sorted)]:
            ctx = r.get('runtime_context') or r.get('context') or {}
            lines += [
                '', f'### {r.get("id")} — {r.get("event_type") or r.get("type")}',
                f'- **Статус:** {r.get("status")}',
                f'- **TEST:** {"да" if r.get("is_test") else "нет"}',
                f'- **Приоритет:** {r.get("priority")}',
                f'- **Компонент:** {r.get("component")}',
                f'- **Повторов:** {r.get("occurrence_count", 1)}',
                f'- **Контекст:** {_fmt_context(ctx)}',
                f'- **Техническая причина:** {r.get("technical_reason")}',
                f'- **Предполагаемая корневая причина:** {r.get("suspected_root_cause")}',
                f'- **Первое возникновение:** {r.get("first_seen_at") or r.get("created_at")}',
                f'- **Последнее возникновение:** {r.get("last_seen_at") or r.get("updated_at")}',
            ]
    if export:
        lines += ['', '## Инструкция для лаборатории', 'Использовать Production-записи как единственный вход для Product Review. TEST, Dry Run и Archived не включаются в Sprint Planning.']
    else:
        lines += ['', '## Доступные действия', '1. **экспорт журнала развития** — экспорт Production-записей.', '2. **экспорт журнала включая тест** — служебная проверка.', '3. **dry run journal: это баг** — smoke-test без сохранения.']
    return {
        'status': 'ok',
        'render_mode': 'development_journal_export' if export else 'development_journal',
        'context': {'level': 'development_journal', 'object_name': 'Development Journal', 'period': None},
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': ['экспорт журнала развития — Production export', 'dry run journal: это баг — тест маршрута без записи'],
        'development_journal': {'records_count': len(records), 'records': records_sorted if export else records_sorted[:50], 'include_test': include_test},
    }
