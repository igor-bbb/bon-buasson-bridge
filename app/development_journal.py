"""Development Journal Architecture v2.0 for VECTRA.

The journal is internal engineering memory. It stores normalized technical
records and aggregated runtime problems, not user chat messages.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

from app.assistant_runtime.durable_runtime_state import read_json_state, write_json_state

_ASSISTANT_REPOSITORY_ROOT = Path(
    os.getenv('VECTRA_ASSISTANT_REPOSITORY_PATH', 'assistant_repository')
).resolve()
JOURNAL_FILE = Path(os.getenv(
    'VECTRA_DEVELOPMENT_JOURNAL_PATH',
    str(_ASSISTANT_REPOSITORY_ROOT / 'runtime' / 'development' / 'development_journal.json'),
)).resolve()
CONTINUITY_FILE = Path(os.getenv(
    'VECTRA_DEVELOPMENT_JOURNAL_CONTINUITY_PATH',
    str(JOURNAL_FILE.with_name('development_journal_continuity.json')),
)).resolve()
LEGACY_JOURNAL_FILE = Path('/tmp/vectra_development_journal.json')
LEGACY_V2_JOURNAL_FILE = Path('/tmp/vectra_development_journal_v2.json')
JOURNAL_LOCK = Lock()

CONTINUITY_RELEASE_ID = 'DEVELOPMENT-JOURNAL-CONTINUITY-001'
CONTINUITY_CONTRACT_VERSION = 'DevelopmentJournalContinuity/v1.0'

CAPTURE_PREFIXES = (
    'зафиксируй в журнале', 'зафиксируй в журнал', 'запиши в журнале',
    'зафиксируй', 'зафиксировать', 'фиксируй', 'запиши', 'запиши в журнал',
    'добавь в журнал', 'записать в журнал', 'записать', 'добавь в развитие',
    'добавить в развитие', 'это баг', 'баг', 'ошибка', 'это ошибка',
    'это неудобно', 'неудобно', 'это нужно улучшить', 'нужно улучшить',
    'надо улучшить', 'надо исправить', 'нужно исправить',
)

SHOW_COMMANDS = {
    'журнал развития', 'показать журнал развития', 'открыть журнал развития',
    'открой журнал развития', 'покажи журнал развития', 'показать журнал',
    'открыть журнал', 'открой журнал', 'покажи журнал',
    'development journal', 'показать development journal',
}

EXPORT_COMMANDS = {
    'экспорт журнала', 'экспортировать журнал', 'экспортируй журнал',
    'экспорт журнала развития', 'экспортировать журнал развития', 'экспортируй журнал развития',
    'выгрузка журнала развития', 'выгрузить журнал развития', 'выгрузи журнал развития',
    'подготовить журнал развития', 'получить журнал развития', 'получи журнал развития',
    'получить development journal', 'экспорт production журнала', 'экспорт production journal',
}

EXPORT_ALL_COMMANDS = {
    'экспорт журнала включая тест', 'экспорт журнала с тестами',
    'экспортировать журнал включая test', 'экспортировать весь журнал',
}

DIALOGUE_REVIEW_COMMANDS = {
    'проанализируй диалог и зафиксируй все выявленные дефекты',
    'проанализировать диалог и зафиксировать все выявленные дефекты',
    'проанализируй сессию и зафиксируй все выявленные дефекты',
    'проанализируй текущий диалог и зафиксируй дефекты',
    'проанализируй диалог и зафиксируй дефекты',
    'анализ диалога и фиксация дефектов',
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


ENGINEERING_TASK_STATUSES = {
    'open': 'Open',
    'in progress': 'In Progress',
    'in_progress': 'In Progress',
    'в работе': 'In Progress',
    'fixed': 'Fixed',
    'исправлена': 'Fixed',
    'исправлено': 'Fixed',
    'awaiting verification': 'Awaiting Verification',
    'awaiting_verification': 'Awaiting Verification',
    'ожидает проверки': 'Awaiting Verification',
    'closed': 'Closed',
    'закрыта': 'Closed',
    'закрыто': 'Closed',
    'archived': 'Archived',
}

ENGINEERING_CLOSED_STATUSES = {'closed', 'archived'}
ENGINEERING_NON_OPEN_STATUSES = {'closed', 'archived', 'logged'}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _canonical_status(status: Any) -> str:
    norm = _normalize(status)
    return ENGINEERING_TASK_STATUSES.get(norm, str(status or 'Open').strip() or 'Open')


def _status_event(
    *,
    status: Any,
    actor: str,
    source: str,
    release: Optional[str] = None,
    version: Optional[str] = None,
    comment: Optional[str] = None,
    timestamp: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        'status': _canonical_status(status),
        'timestamp': timestamp or _now(),
        'release': release or None,
        'version': version or None,
        'actor': actor or source or 'System',
        'source': source or actor or 'System',
        'comment': comment or '',
    }


def _ensure_status_history(record: Dict[str, Any]) -> List[Dict[str, Any]]:
    history = record.get('status_history')
    if not isinstance(history, list):
        created_at = record.get('created_at') or record.get('first_seen_at') or _now()
        source = str(record.get('source') or 'Runtime')
        release = None
        runtime_context = record.get('runtime_context')
        if isinstance(runtime_context, dict):
            release = runtime_context.get('release_id')
        history = [_status_event(
            status=record.get('status') or 'Open',
            actor=source,
            source=source,
            release=release,
            comment='Initial lifecycle status reconstructed from record metadata.',
            timestamp=str(created_at),
        )]
        record['status_history'] = history
    return history


def _append_status_change(
    record: Dict[str, Any],
    *,
    status: Any,
    actor: str,
    source: str,
    release: Optional[str] = None,
    version: Optional[str] = None,
    comment: Optional[str] = None,
) -> Dict[str, Any]:
    new_status = _canonical_status(status)
    current_status = _canonical_status(record.get('status') or 'Open')
    event = _status_event(
        status=new_status, actor=actor, source=source,
        release=release, version=version, comment=comment,
    )
    _ensure_status_history(record).append(event)
    record['status'] = new_status
    record['current_status'] = new_status
    record['updated_at'] = event['timestamp']
    if new_status == 'Fixed':
        record['fixed_at'] = event['timestamp']
        record['fixed_release'] = release
        record['fixed_version'] = version
    elif new_status == 'Awaiting Verification':
        record['awaiting_verification_at'] = event['timestamp']
        record['verification_release'] = release
        record['fixed_release'] = record.get('fixed_release') or release
        record['fixed_version'] = record.get('fixed_version') or version
    elif new_status == 'Closed':
        record['closed_at'] = event['timestamp']
        record['confirmed_at'] = event['timestamp']
        record['confirmed_release'] = release or record.get('verification_release') or record.get('fixed_release')
    record.setdefault('history', []).append({
        'at': event['timestamp'],
        'event': 'status_changed',
        'from': current_status,
        'to': new_status,
        'release': release,
        'version': version,
        'actor': actor,
        'source': source,
        'note': comment or '',
    })
    return record


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
    if is_dry_run_command(message) or is_dialogue_review_command(message):
        return True
    return any(_match_prefix(norm, prefix) for prefix in CAPTURE_PREFIXES)


def is_global_capture_command(message: Any) -> bool:
    norm = _normalize(message)
    return any(_match_prefix(norm, prefix) for prefix in (
        'зафиксируй в журнале',
        'зафиксируй в журнал',
        'запиши в журнале',
    ))


def is_show_command(message: Any) -> bool:
    norm = _normalize(message)
    if norm in SHOW_COMMANDS:
        return True
    # Stabilization S1: common spoken variants must not fall through to Workspace Runtime.
    return ('журнал' in norm or 'journal' in norm) and any(x in norm for x in ('показ', 'покажи', 'откр', 'открой', 'получ')) and not any(x in norm for x in ('экспорт', 'выгруз'))


def is_export_command(message: Any) -> bool:
    norm = _normalize(message)
    if norm in EXPORT_COMMANDS or norm in EXPORT_ALL_COMMANDS:
        return True
    return ('журнал' in norm or 'journal' in norm) and any(x in norm for x in ('экспорт', 'экспортир', 'экспортируй', 'выгруз', 'выгрузи'))


def is_export_all_command(message: Any) -> bool:
    norm = _normalize(message)
    return norm in EXPORT_ALL_COMMANDS or (is_export_command(message) and any(x in norm for x in ('тест', 'test', 'весь', 'все')) )


def is_dialogue_review_command(message: Any) -> bool:
    norm = _normalize(message)
    return norm in DIALOGUE_REVIEW_COMMANDS


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
        word in norm for word in ('удалить', 'архивировать', 'архив', 'закрыть', 'close', 'closed', 'reopen', 'переоткрыть', 'пометить', 'mark', 'test', 'production', 'восстановить', 'в работу', 'in progress', 'fixed', 'исправлено', 'исправлена', 'ожидает проверки', 'awaiting verification')
    )


def _read_records() -> List[Dict[str, Any]]:
    records, diagnostic = read_json_state(JOURNAL_FILE, list, list)
    if diagnostic.get('status') == 'HOLD':
        raise RuntimeError(
            'Development Journal repository is unavailable: '
            f"primary={diagnostic.get('primary_error')}; backup={diagnostic.get('backup_error')}"
        )
    if diagnostic.get('status') != 'EMPTY':
        clean = [x for x in records if isinstance(x, dict)]
        _ensure_continuity_state(clean)
        return clean

    # One-time backward-compatible migration. The persistent repository always
    # wins; /tmp is read only when the canonical file and its backup are absent.
    for legacy in (LEGACY_V2_JOURNAL_FILE, LEGACY_JOURNAL_FILE):
        if not legacy.exists():
            continue
        try:
            migrated = json.loads(legacy.read_text(encoding='utf-8'))
        except Exception:
            continue
        if isinstance(migrated, list):
            clean = [x for x in migrated if isinstance(x, dict)]
            _write_records(clean)
            return clean
    _ensure_continuity_state([])
    return []


def _write_records(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    diagnostic = write_json_state(JOURNAL_FILE, records)
    readback, readback_diagnostic = read_json_state(JOURNAL_FILE, list, list)
    if readback_diagnostic.get('status') not in {'PASS', 'RECOVERED'} or readback != records:
        raise RuntimeError('Development Journal write/readback verification failed.')
    continuity = _ensure_continuity_state(records)
    return {
        **diagnostic,
        'readback_verified': True,
        'repository_path': str(JOURNAL_FILE),
        'continuity_status': continuity.get('status'),
        'next_sequence': continuity.get('next_sequence'),
    }


def _record_sequence(record_id: Any) -> int:
    match = re.search(r'(\d+)$', str(record_id or ''))
    return int(match.group(1)) if match else 0


def _default_continuity_state() -> Dict[str, Any]:
    now = _now()
    return {
        'contract_version': CONTINUITY_CONTRACT_VERSION,
        'release_id': CONTINUITY_RELEASE_ID,
        'status': 'ACTIVE',
        'next_sequence': 1,
        'reserved_record_ids': [],
        'unrecoverable_record_ids': [],
        'created_at': now,
        'updated_at': now,
    }


def _ensure_continuity_state(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    state, diagnostic = read_json_state(CONTINUITY_FILE, _default_continuity_state, dict)
    if diagnostic.get('status') == 'HOLD':
        raise RuntimeError(
            'Development Journal continuity repository is unavailable: '
            f"primary={diagnostic.get('primary_error')}; backup={diagnostic.get('backup_error')}"
        )
    normalized = dict(state or {})
    normalized.setdefault('contract_version', CONTINUITY_CONTRACT_VERSION)
    normalized.setdefault('release_id', CONTINUITY_RELEASE_ID)
    normalized['status'] = 'ACTIVE'
    normalized.setdefault('created_at', _now())
    reserved = {
        str(value).upper()
        for value in normalized.get('reserved_record_ids') or []
        if str(value or '').strip()
    }
    reserved.update(
        str(record.get('id')).upper()
        for record in records
        if isinstance(record, dict) and record.get('id')
    )
    unrecoverable = {
        str(value).upper()
        for value in normalized.get('unrecoverable_record_ids') or []
        if str(value or '').strip()
    }
    max_sequence = max(
        [_record_sequence(value) for value in reserved] +
        [_record_sequence(record.get('id')) for record in records if isinstance(record, dict)] +
        [0]
    )
    normalized['next_sequence'] = max(
        int(normalized.get('next_sequence') or 1),
        max_sequence + 1,
    )
    normalized['reserved_record_ids'] = sorted(reserved)
    normalized['unrecoverable_record_ids'] = sorted(unrecoverable)
    changed = normalized != state or diagnostic.get('status') == 'EMPTY'
    if changed:
        normalized['updated_at'] = _now()
        write_diagnostic = write_json_state(CONTINUITY_FILE, normalized)
        if write_diagnostic.get('status') != 'PASS':
            raise RuntimeError('Development Journal continuity write failed.')
    return normalized


def _next_id(records: List[Dict[str, Any]]) -> str:
    continuity = _ensure_continuity_state(records)
    return f'DEV-{int(continuity.get("next_sequence") or 1):04d}'


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


def _impact_from_priority(priority: str) -> str:
    p = str(priority or '').upper()
    if p == 'P0':
        return 'Blocks Product Acceptance or breaks the core user scenario.'
    if p == 'P1':
        return 'Degrades an approved scenario and requires engineering correction.'
    return 'Creates UX/product friction but does not block the main scenario.'


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
        'subsystem': record.get('subsystem'),
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
    subsystem: Optional[str] = None,
    proposed_fix_direction: Optional[str] = None,
) -> Dict[str, Any]:
    record = {
        'id': _next_id(records),
        'schema_version': 'DevelopmentJournalRecord/v2.1',
        'record_kind': 'engineering_task',
        'event_type': event_type,
        'type': event_type,
        'component': component,
        'system_level': system_level,
        'subsystem': subsystem or system_level,
        'technical_reason': technical_reason,
        'suspected_root_cause': suspected_root_cause,
        'proposed_fix_direction': proposed_fix_direction or 'Classify and fix in the owning component.',
        'priority': priority,
        'runtime_context': runtime_context or {},
        'context': runtime_context or {},
        'active_workspace_state': active_workspace_state or {},
        'error_code': error_code,
        'reproduction_data': reproduction_data or {},
        # Stabilization S1 — structured engineering defect contract.
        # These fields make Development Journal the single source for confirmed
        # engineering defects, independent from QA reports or raw dialogues.
        'scenario': (reproduction_data or {}).get('scenario') or (runtime_context or {}).get('scenario') or '',
        'expected_behavior': (reproduction_data or {}).get('expected_behavior') or '',
        'actual_behavior': (reproduction_data or {}).get('actual_behavior') or technical_reason,
        'impact': (reproduction_data or {}).get('impact') or _impact_from_priority(priority),
        'severity': (reproduction_data or {}).get('severity') or priority,
        'reproducibility': (reproduction_data or {}).get('reproducibility') or 'unknown',
        'is_regression': bool((reproduction_data or {}).get('is_regression', False)),
        'related_record_ids': list((reproduction_data or {}).get('related_record_ids') or []),
        'source': source,
        'status': 'Test' if is_test else _canonical_status(status),
        'current_status': 'Test' if is_test else _canonical_status(status),
        'is_test': bool(is_test),
        'persisted': True,
        'occurrence_count': 1,
        'first_seen_at': _now(),
        'last_seen_at': _now(),
        'created_at': _now(),
        'updated_at': _now(),
        'history': [{'at': _now(), 'event': 'created', 'note': 'Created as normalized engineering record.'}],
    }
    if not is_test:
        record['status_history'] = [_status_event(
            status=record['status'],
            actor=source,
            source=source,
            release=(runtime_context or {}).get('release_id') if isinstance(runtime_context, dict) else None,
            version=(runtime_context or {}).get('version') if isinstance(runtime_context, dict) else None,
            comment='Engineering task created.',
            timestamp=record['created_at'],
        )]
    record['fingerprint'] = _fingerprint_record(record)
    return record


def _aggregate_or_append(records: List[Dict[str, Any]], record: Dict[str, Any]) -> Dict[str, Any]:
    # Product Acceptance check records are audit history. They must never be
    # deduplicated into engineering defects, otherwise the journal loses the
    # chronology of Release Manager activity.
    if _is_acceptance_check_record(record):
        records.append(record)
        return record
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
            # Keep the latest confirmed scenario metadata while preserving links.
            for meta_key in ('scenario', 'expected_behavior', 'actual_behavior', 'impact', 'severity', 'reproducibility'):
                if record.get(meta_key):
                    existing[meta_key] = record.get(meta_key)
            existing['is_regression'] = bool(existing.get('is_regression') or record.get('is_regression'))
            links = set(existing.get('related_record_ids') or [])
            links.update(record.get('related_record_ids') or [])
            if record.get('id'):
                links.add(str(record.get('id')))
            existing['related_record_ids'] = sorted(x for x in links if x and x != existing.get('id'))
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
    subsystem: Optional[str] = None,
    proposed_fix_direction: Optional[str] = None,
    status: str = 'Open',
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
            subsystem=subsystem,
            proposed_fix_direction=proposed_fix_direction,
            status=status,
        )
        if event_type == 'release_manager_acceptance_check':
            record['record_kind'] = 'product_acceptance_check'
            record['status'] = status or 'Logged'
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



def add_global_record(
    *,
    event_type: str = 'manual_engineering_registration',
    component: str = 'vectra_global_assistant',
    technical_description: str = 'Manual global engineering registration requested by VECTRA assistant.',
    suspected_root_cause: str = 'Requires laboratory review.',
    proposed_fix_direction: str = 'Classify, aggregate and convert into an engineering task if confirmed.',
    priority: str = 'P1',
    runtime_context: Optional[Dict[str, Any]] = None,
    session_id: str = 'default',
    dry_run: bool = False,
    is_test: bool = False,
) -> Dict[str, Any]:
    """Global Development Journal API record creation.

    This route is independent from Workspace Runtime. It accepts already
    normalized engineering knowledge and never requires workspace_markdown or
    active_workspace_state.
    """
    return add_runtime_event(
        event_type=event_type,
        component=component,
        system_level='global_development_journal',
        technical_reason=technical_description,
        suspected_root_cause=suspected_root_cause,
        priority=priority,
        runtime_context=runtime_context or {'source_scope': 'any_vectra_assistant'},
        active_workspace_state={},
        error_code=event_type,
        reproduction_data={
            'source': 'manual_engineering_registration',
            'session_id_hash': _stable_hash(session_id),
        },
        session_id=session_id,
        dry_run=dry_run,
        is_test=is_test,
        subsystem='global_development_journal',
        proposed_fix_direction=proposed_fix_direction,
    )



def add_release_acceptance_record(
    *,
    release_id: str,
    scenarios_executed: int,
    result: str,
    defects_found: int,
    session_id: str = 'default',
    release_brief: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Persist Release Manager acceptance check result in Development Journal.

    This is a journal entry about performed acceptance, not an open engineering
    defect. It allows Laboratory to see release-check history while Product
    Owner Report keeps old engineering tasks separate from current release
    quality.
    """
    return add_runtime_event(
        event_type='release_manager_acceptance_check',
        component='release_manager',
        system_level='release_manager',
        subsystem='product_acceptance',
        technical_reason='Release Manager completed Product Acceptance and recorded check result.',
        suspected_root_cause='Not applicable: this is a release check record, not a defect.',
        proposed_fix_direction='No engineering fix required unless linked defect records exist.',
        priority='P2',
        runtime_context={
            'release_id': release_id,
            'scenarios_executed': scenarios_executed,
            'result': result,
            'defects_found': defects_found,
            'release_brief': release_brief or {},
        },
        active_workspace_state={},
        error_code='release_acceptance_completed',
        reproduction_data={
            'scenario': 'Product Acceptance',
            'expected_behavior': 'Release Manager executes the selected acceptance program and records the check result.',
            'actual_behavior': f'Product Acceptance completed with result {result}; defects found: {defects_found}.',
            'impact': 'Provides audit trail of release checks for Laboratory and engineering governance.',
            'severity': 'P2',
            'reproducibility': 'release_check_record',
            'is_regression': False,
            'release_id': release_id,
            'scenarios_executed': scenarios_executed,
            'defects_found': defects_found,
        },
        session_id=session_id,
        status='Logged',
    )


def update_engineering_task_status(
    record_id: str,
    status: str,
    *,
    actor: str,
    source: str,
    release: Optional[str] = None,
    version: Optional[str] = None,
    comment: Optional[str] = None,
    allow_release_manager_only_close: bool = True,
) -> Dict[str, Any]:
    """Update one engineering task lifecycle status and persist status history."""
    canonical = _canonical_status(status)
    if canonical == 'Closed' and allow_release_manager_only_close and _normalize(source) != 'release manager':
        return {
            'status': 'error',
            'reason': 'closed_status_requires_release_manager',
            'record_id': record_id,
            'requested_status': canonical,
        }
    with JOURNAL_LOCK:
        records = _read_records()
        record = _find_record(records, record_id)
        if not record:
            return {'status': 'error', 'reason': 'journal_record_not_found', 'record_id': record_id}
        if _is_acceptance_check_record(record):
            return {'status': 'error', 'reason': 'product_acceptance_record_has_no_engineering_lifecycle', 'record_id': record_id}
        _append_status_change(
            record,
            status=canonical,
            actor=actor,
            source=source,
            release=release,
            version=version,
            comment=comment,
        )
        _write_records(records)
        return {'status': 'ok', 'record': record}



def mark_tasks_fixed(
    task_ids: List[str],
    *,
    release: str,
    version: Optional[str] = None,
    actor: str = 'Engineering',
    comment: str = 'Engineering implemented the fix in code.',
) -> Dict[str, Any]:
    """Mark engineering tasks as Fixed for a release.

    This is the Engineering-side automatic lifecycle update used before Release
    Brief generation. The task is not closed here; Release Manager is the only
    actor allowed to close it after Product Acceptance.
    """
    fixed: List[str] = []
    missing: List[str] = []
    with JOURNAL_LOCK:
        records = _read_records()
        for task_id in task_ids or []:
            record = _find_record(records, str(task_id))
            if not record or _is_acceptance_check_record(record):
                missing.append(str(task_id))
                continue
            _append_status_change(
                record,
                status='Fixed',
                actor=actor,
                source='Engineering',
                release=release,
                version=version,
                comment=comment,
            )
            fixed.append(str(record.get('id')))
        _write_records(records)
    return {'status': 'ok', 'fixed_ids': sorted(set(fixed)), 'missing_ids': sorted(set(missing))}


def list_fixed_engineering_tasks_for_release(release: str, include_awaiting: bool = True) -> List[Dict[str, Any]]:
    """Return tasks fixed by the specified release.

    Includes tasks still in Fixed plus, by default, Awaiting Verification tasks
    that were already advanced after Release Brief generation. Closed tasks are
    intentionally excluded because Product Acceptance already confirmed them.
    """
    rel = str(release or '').strip()
    if not rel:
        return []
    wanted_statuses = {'fixed'} | ({'awaiting verification', 'awaiting_verification'} if include_awaiting else set())
    out: List[Dict[str, Any]] = []
    for record in list_records(include_test=False, include_archived=False):
        if not _is_engineering_task_record(record):
            continue
        if _normalize(record.get('status')) not in wanted_statuses:
            continue
        if str(record.get('fixed_release') or record.get('verification_release') or '').strip() == rel:
            out.append(record)
    return sorted(out, key=lambda r: str(r.get('id') or ''))


def list_fixed_engineering_task_ids_for_release(release: str, include_awaiting: bool = True) -> List[str]:
    return [str(r.get('id')) for r in list_fixed_engineering_tasks_for_release(release, include_awaiting=include_awaiting) if r.get('id')]

def mark_tasks_awaiting_verification(
    task_ids: List[str],
    *,
    release: str,
    version: Optional[str] = None,
    actor: str = 'Release Brief',
) -> Dict[str, Any]:
    updated: List[str] = []
    missing: List[str] = []
    with JOURNAL_LOCK:
        records = _read_records()
        for task_id in task_ids or []:
            record = _find_record(records, str(task_id))
            if not record or _is_acceptance_check_record(record):
                missing.append(str(task_id))
                continue
            _append_status_change(
                record,
                status='Awaiting Verification',
                actor=actor,
                source='Engineering',
                release=release,
                version=version,
                comment='Release Brief formed; fix is waiting for Release Manager verification.',
            )
            updated.append(str(record.get('id')))
        _write_records(records)
    return {'status': 'ok', 'updated_ids': sorted(set(updated)), 'missing_ids': sorted(set(missing))}


def close_verified_tasks(
    task_ids: List[str],
    *,
    release: str,
    actor: str = 'Release Manager',
    comment: str = 'Product Acceptance passed; fix confirmed by Release Manager.',
) -> Dict[str, Any]:
    closed: List[str] = []
    missing: List[str] = []
    with JOURNAL_LOCK:
        records = _read_records()
        for task_id in task_ids or []:
            record = _find_record(records, str(task_id))
            if not record or _is_acceptance_check_record(record):
                missing.append(str(task_id))
                continue
            _append_status_change(
                record,
                status='Closed',
                actor=actor,
                source='Release Manager',
                release=release,
                version=record.get('fixed_version'),
                comment=comment,
            )
            closed.append(str(record.get('id')))
            try:
                from app.release_manager import upsert_regression_scenario_from_journal_record
                regression = upsert_regression_scenario_from_journal_record(record)
                if regression:
                    record.setdefault('history', []).append({
                        'at': _now(),
                        'event': 'regression_scenario_upserted',
                        'note': f"Regression scenario {regression.get('id')} added or updated.",
                    })
            except Exception:
                record.setdefault('history', []).append({
                    'at': _now(),
                    'event': 'regression_scenario_upsert_failed',
                    'note': 'Defect was closed, but regression scenario update failed and must be reviewed.',
                })
        _write_records(records)
    return {'status': 'ok', 'closed_ids': sorted(set(closed)), 'missing_ids': sorted(set(missing))}


def reject_verified_tasks(
    task_ids: List[str],
    *,
    release: str,
    actor: str = 'Release Manager',
    comment: str = 'Product Acceptance did not confirm the fix; task returned to Open.',
) -> Dict[str, Any]:
    reopened: List[str] = []
    missing: List[str] = []
    with JOURNAL_LOCK:
        records = _read_records()
        for task_id in task_ids or []:
            record = _find_record(records, str(task_id))
            if not record or _is_acceptance_check_record(record):
                missing.append(str(task_id))
                continue
            _append_status_change(
                record,
                status='Open',
                actor=actor,
                source='Release Manager',
                release=release,
                version=record.get('fixed_version'),
                comment=comment,
            )
            reopened.append(str(record.get('id')))
        _write_records(records)
    return {'status': 'ok', 'reopened_ids': sorted(set(reopened)), 'missing_ids': sorted(set(missing))}


def create_development_request(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """Create the single cross-environment development record.

    Laboratory may register a confirmed product gap, but it cannot approve its
    implementation.  The same DEV id is retained through Owner, Engineering,
    release and Product Verification stages.
    """
    payload = payload if isinstance(payload, dict) else {}
    record = add_global_record(
        event_type=str(payload.get('event_type') or 'development_request'),
        component=str(payload.get('component') or 'vectra_laboratory'),
        technical_description=str(payload.get('confirmed_gap') or payload.get('technical_description') or 'Confirmed VECTRA development gap.'),
        suspected_root_cause=str(payload.get('evidence_summary') or payload.get('suspected_root_cause') or 'Requires Engineering review.'),
        proposed_fix_direction=str(payload.get('proposal') or payload.get('proposed_fix_direction') or 'Prepare one bounded engineering increment.'),
        priority=str(payload.get('priority') or 'P1'),
        runtime_context={
            **(payload.get('runtime_context') if isinstance(payload.get('runtime_context'), dict) else {}),
            'source_environment': str(payload.get('source_environment') or 'Laboratory'),
            'research_id': payload.get('research_id'),
            'evidence_ids': list(payload.get('evidence_ids') or []),
        },
        session_id=str(payload.get('session_id') or 'laboratory'),
        dry_run=bool(payload.get('dry_run')),
        is_test=bool(payload.get('is_test')),
    )
    if record.get('persisted', True) and not record.get('is_test'):
        with JOURNAL_LOCK:
            records = _read_records()
            stored = _find_record(records, str(record.get('id') or ''))
            if stored:
                stored['record_kind'] = 'development_request'
                stored.setdefault('owner_decision', {'status': 'PENDING'})
                stored.setdefault('engineering', {'status': 'NOT_STARTED'})
                stored.setdefault('verification', {'status': 'NOT_STARTED'})
                stored['bridge_schema_version'] = 'VECTRADevelopmentBridge/v1.0'
                stored['repository_path'] = 'runtime/development/development_journal.json'
                stored['updated_at'] = _now()
                _write_records(records)
                record = stored
    return {'status': 'ok', 'operation_type': 'create_development_request', 'record_id': record.get('id'), 'record': record, 'readback_status': 'PASS'}


def record_owner_decision(record_id: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    decision = _normalize(payload.get('decision'))
    if decision not in {'approve', 'approved', 'утвердить', 'утверждено', 'reject', 'rejected', 'отклонить', 'отклонено'}:
        return {'status': 'error', 'operation_type': 'record_owner_decision', 'failure_reason': 'explicit_owner_decision_required', 'record_id': record_id}
    approved = decision in {'approve', 'approved', 'утвердить', 'утверждено'}
    if not bool(payload.get('product_owner_approval') or payload.get('confirmed_by_product_owner')):
        return {'status': 'error', 'operation_type': 'record_owner_decision', 'failure_reason': 'product_owner_confirmation_required', 'record_id': record_id}
    with JOURNAL_LOCK:
        records = _read_records()
        record = _find_record(records, record_id)
        if not record:
            return {'status': 'error', 'operation_type': 'record_owner_decision', 'failure_reason': 'journal_record_not_found', 'record_id': record_id}
        now = _now()
        record['owner_decision'] = {'status': 'APPROVED' if approved else 'REJECTED', 'decided_at': now, 'comment': str(payload.get('comment') or ''), 'actor': 'Product Owner'}
        _append_status_change(record, status='Open' if approved else 'Archived', actor='Product Owner', source='Product Owner', comment=str(payload.get('comment') or ('Implementation approved.' if approved else 'Development request rejected.')))
        _write_records(records)
        return {'status': 'ok', 'operation_type': 'record_owner_decision', 'record_id': record_id, 'owner_decision': record['owner_decision'], 'readback_status': 'PASS', 'record': record}


def update_development_execution(record_id: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    stage = _normalize(payload.get('stage') or payload.get('status'))
    stage_map = {
        'in progress': 'In Progress', 'in_progress': 'In Progress', 'в работе': 'In Progress',
        'implemented': 'Fixed', 'fixed': 'Fixed', 'реализовано': 'Fixed',
        'awaiting verification': 'Awaiting Verification', 'awaiting_verification': 'Awaiting Verification', 'ожидает проверки': 'Awaiting Verification',
    }
    status = stage_map.get(stage)
    if not status:
        return {'status': 'error', 'operation_type': 'update_development_execution', 'failure_reason': 'unsupported_engineering_stage', 'record_id': record_id}
    with JOURNAL_LOCK:
        records = _read_records()
        record = _find_record(records, record_id)
        if not record:
            return {'status': 'error', 'operation_type': 'update_development_execution', 'failure_reason': 'journal_record_not_found', 'record_id': record_id}
        if (record.get('owner_decision') or {}).get('status') != 'APPROVED':
            return {'status': 'error', 'operation_type': 'update_development_execution', 'failure_reason': 'owner_approval_required', 'record_id': record_id}
        _append_status_change(record, status=status, actor='Engineering', source='Engineering', release=payload.get('release_id'), version=payload.get('version'), comment=str(payload.get('comment') or 'Engineering stage updated.'))
        record['engineering'] = {'status': stage.upper().replace(' ', '_'), 'release_id': payload.get('release_id'), 'version': payload.get('version'), 'commit_sha': payload.get('commit_sha'), 'updated_at': _now()}
        _write_records(records)
        return {'status': 'ok', 'operation_type': 'update_development_execution', 'record_id': record_id, 'readback_status': 'PASS', 'record': record}


def record_development_verification(record_id: str, payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    verdict = str(payload.get('verdict') or '').strip().upper()
    if verdict not in {'PASS', 'FAIL'}:
        return {'status': 'error', 'operation_type': 'record_development_verification', 'failure_reason': 'pass_or_fail_verdict_required', 'record_id': record_id}
    with JOURNAL_LOCK:
        records = _read_records()
        record = _find_record(records, record_id)
        if not record:
            return {'status': 'error', 'operation_type': 'record_development_verification', 'failure_reason': 'journal_record_not_found', 'record_id': record_id}
        if _canonical_status(record.get('status')) != 'Awaiting Verification':
            return {'status': 'error', 'operation_type': 'record_development_verification', 'failure_reason': 'record_not_awaiting_verification', 'record_id': record_id}
        release_id = str(payload.get('release_id') or record.get('verification_release') or record.get('fixed_release') or '')
        record['verification'] = {'status': verdict, 'release_id': release_id, 'verified_at': _now(), 'evidence': list(payload.get('evidence') or []), 'actor': 'Laboratory'}
        _append_status_change(record, status='Closed' if verdict == 'PASS' else 'Open', actor='Laboratory', source='Release Manager', release=release_id, comment=str(payload.get('comment') or ('Product Verification passed.' if verdict == 'PASS' else 'Product Verification failed; returned to Engineering.')))
        _write_records(records)
        return {'status': 'ok', 'operation_type': 'record_development_verification', 'record_id': record_id, 'verdict': verdict, 'readback_status': 'PASS', 'record': record}


def get_development_bridge(record_id: Optional[str] = None, *, only_new: bool = False, limit: int = 50) -> Dict[str, Any]:
    records = list_records(include_test=False, include_archived=True)
    records = [
        r for r in records
        if r.get('bridge_schema_version')
        or r.get('record_kind') in {'development_request', 'data_loss_tombstone'}
    ]
    continuity = get_development_journal_continuity_status()
    if record_id:
        record = _find_record(records, record_id)
        return {
            'status': 'ok' if record else 'not_found',
            'operation_type': 'get_development_bridge',
            'record_id': record_id,
            'record': record,
            'repository_path': str(JOURNAL_FILE),
            'repository_source_of_truth': continuity.get('source_of_truth'),
            'continuity_status': continuity.get('status'),
            'readback_status': 'PASS' if record else 'FAIL',
        }
    if only_new:
        records = [r for r in records if (r.get('owner_decision') or {}).get('status') == 'PENDING']
    records = sorted(records, key=lambda r: str(r.get('updated_at') or r.get('created_at') or ''), reverse=True)[:max(1, min(int(limit or 50), 200))]
    return {
        'status': 'ok',
        'operation_type': 'get_development_bridge',
        'records_count': len(records),
        'records': records,
        'repository_path': str(JOURNAL_FILE),
        'repository_source_of_truth': continuity.get('source_of_truth'),
        'continuity_status': continuity.get('status'),
        'readback_status': 'PASS',
    }


def get_development_journal_continuity_status() -> Dict[str, Any]:
    with JOURNAL_LOCK:
        records = _read_records()
        state = _ensure_continuity_state(records)
    try:
        from app.assistant_runtime.repository_persistence import persistence_backend
        backend = persistence_backend()
    except Exception:
        backend = 'file'
    return {
        **state,
        'records_count': len(records),
        'repository_path': str(JOURNAL_FILE),
        'continuity_repository_path': str(CONTINUITY_FILE),
        'repository_backend': backend,
        'source_of_truth': 'database' if backend == 'database' else 'filesystem',
        'durable_across_deploys': backend == 'database',
        'readback_status': 'PASS',
        'read_only_query_mutates_journal': False,
    }


def register_unrecoverable_development_record(
    record_id: str,
    *,
    migration_id: str,
    evidence: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Register an explicit loss tombstone without reconstructing missing facts."""
    normalized_id = str(record_id or '').strip().upper()
    if not re.fullmatch(r'DEV-\d{4,6}', normalized_id):
        raise ValueError('record_id must match DEV-NNNN')
    with JOURNAL_LOCK:
        records = _read_records()
        existing = _find_record(records, normalized_id)
        if existing:
            if existing.get('record_kind') == 'data_loss_tombstone':
                state = _ensure_continuity_state(records)
                lost_ids = set(state.get('unrecoverable_record_ids') or [])
                if normalized_id not in lost_ids:
                    lost_ids.add(normalized_id)
                    state['unrecoverable_record_ids'] = sorted(lost_ids)
                    state['updated_at'] = _now()
                    write_json_state(CONTINUITY_FILE, state)
            recovery_status = (
                'UNRECOVERABLE_LOSS_RECORDED'
                if existing.get('record_kind') == 'data_loss_tombstone'
                else 'HISTORICAL_RECORD_AVAILABLE'
            )
            return {
                'status': 'PASS',
                'record_id': normalized_id,
                'recovery_status': recovery_status,
                'record': existing,
                'created': False,
                'readback_status': 'PASS',
            }
        now = _now()
        tombstone = {
            'id': normalized_id,
            'schema_version': 'DevelopmentJournalRecord/v2.1',
            'bridge_schema_version': 'VECTRADevelopmentBridge/v1.0-loss-tombstone',
            'record_kind': 'data_loss_tombstone',
            'event_type': 'development_journal_history_loss',
            'title': f'Невосстановимая потеря исторической записи {normalized_id}',
            'confirmed_gap': (
                'Историческая запись отсутствует в текущем каноническом хранилище; '
                'её исходное содержимое не реконструировалось.'
            ),
            'technical_reason': (
                'Ранее подтверждённая запись не сохранилась при переходе со '
                'временной файловой системы на постоянное хранилище.'
            ),
            'suspected_root_cause': (
                'Database persistence was enabled after the historical record '
                'had already been lost with an ephemeral deployment.'
            ),
            'proposed_fix_direction': 'Preserve all subsequent journal state in the canonical database repository.',
            'priority': 'P0',
            'status': 'Lost',
            'current_status': 'Lost',
            'is_test': False,
            'persisted': True,
            'source': 'Controlled Runtime Repository migration',
            'source_environment': 'Production',
            'created_at': now,
            'updated_at': now,
            'owner_decision': {'status': 'NOT_APPLICABLE'},
            'engineering': {'status': 'DATA_LOSS_RECORDED'},
            'verification': {'status': 'FAIL', 'verdict': 'FAIL'},
            'data_recovery': {
                'status': 'UNRECOVERABLE',
                'original_content_restored': False,
                'silent_reconstruction_performed': False,
                'migration_id': migration_id,
                'evidence': evidence or {},
                'recorded_at': now,
            },
            'history': [{
                'at': now,
                'event': 'unrecoverable_data_loss_recorded',
                'note': 'Loss recorded explicitly; historical business content was not reconstructed.',
            }],
        }
        records.append(tombstone)
        _write_records(records)
        state = _ensure_continuity_state(records)
        lost_ids = set(state.get('unrecoverable_record_ids') or [])
        lost_ids.add(normalized_id)
        state['unrecoverable_record_ids'] = sorted(lost_ids)
        state['updated_at'] = _now()
        write_json_state(CONTINUITY_FILE, state)
        readback = _find_record(_read_records(), normalized_id)
        return {
            'status': 'PASS' if readback else 'FAIL',
            'record_id': normalized_id,
            'recovery_status': 'UNRECOVERABLE_LOSS_RECORDED',
            'record': readback,
            'created': True,
            'next_sequence': state.get('next_sequence'),
            'readback_status': 'PASS' if readback else 'FAIL',
        }

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
        'closed': 'Запись закрыта.',
        'reopened': 'Запись переоткрыта.',
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
        if any(x in norm for x in ('закрыть', 'закрыто', 'close', 'closed')):
            _append_status_change(record, status='Closed', actor='Release Manager', source='Release Manager', comment='Lifecycle command applied.')
            action = 'closed'
            try:
                from app.release_manager import upsert_regression_scenario_from_journal_record
                regression = upsert_regression_scenario_from_journal_record(record)
                if regression:
                    record.setdefault('history', []).append({
                        'at': _now(),
                        'event': 'regression_scenario_upserted',
                        'note': f"Regression scenario {regression.get('id')} added or updated.",
                    })
            except Exception:
                record.setdefault('history', []).append({
                    'at': _now(),
                    'event': 'regression_scenario_upsert_failed',
                    'note': 'Defect was closed, but regression scenario update failed and must be reviewed.',
                })
        elif any(x in norm for x in ('в работу', 'in progress')):
            _append_status_change(record, status='In Progress', actor='Engineering', source='Engineering', comment='Engineering accepted task into work.')
            action = 'in_progress'
        elif any(x in norm for x in ('fixed', 'исправлено', 'исправлена')):
            _append_status_change(record, status='Fixed', actor='Engineering', source='Engineering', comment='Engineering marked fix as implemented.')
            action = 'fixed'
        elif any(x in norm for x in ('ожидает проверки', 'awaiting verification')):
            _append_status_change(record, status='Awaiting Verification', actor='Engineering', source='Engineering', comment='Fix is waiting for Release Manager verification.')
            action = 'awaiting_verification'
        elif any(x in norm for x in ('reopen', 'reopened', 'переоткрыть', 'открыть повторно', 'вернуть в работу')):
            _append_status_change(record, status='Open', actor='Release Manager', source='Release Manager', comment='Task reopened after lifecycle command.')
            action = 'reopened'
        elif 'архив' in norm:
            _append_status_change(record, status='Archived', actor='Engineering', source='Engineering', comment='Task archived by lifecycle command.')
            action = 'archived'
        elif 'test' in norm or 'тест' in norm:
            record['is_test'] = True; record['status'] = 'Test'; record['current_status'] = 'Test'; action = 'marked_test'
        elif 'production' in norm or 'рабоч' in norm:
            record['is_test'] = False
            if _normalize(record.get('status')) == 'test':
                _append_status_change(record, status='Open', actor='Engineering', source='Engineering', comment='Task restored to Production from TEST.')
            action = 'marked_production'
        elif 'восстановить' in norm:
            _append_status_change(record, status='Open', actor='Engineering', source='Engineering', comment='Task restored by lifecycle command.')
            action = 'restored'
        else:
            action = 'updated'
            record['updated_at'] = _now()
            record.setdefault('history', []).append({'at': _now(), 'event': action, 'note': 'Lifecycle command applied.'})
        _write_records(records)
        return build_lifecycle_response(record, action=action, record_id=record_id)



def _status_norm(record: Dict[str, Any]) -> str:
    return _normalize(record.get('status'))


def _event_type_norm(record: Dict[str, Any]) -> str:
    return _normalize(record.get('event_type') or record.get('type'))


def _is_acceptance_check_record(record: Dict[str, Any]) -> bool:
    if not isinstance(record, dict):
        return False
    if _normalize(record.get('record_kind')) == 'product_acceptance_check':
        return True
    return _event_type_norm(record) == 'release_manager_acceptance_check'


def _is_engineering_task_record(record: Dict[str, Any]) -> bool:
    if not isinstance(record, dict) or _is_acceptance_check_record(record):
        return False
    if record.get('is_test') or _status_norm(record) in {'test', 'dry run', 'dry-run'}:
        return False
    return True


def _is_open_engineering_task_record(record: Dict[str, Any]) -> bool:
    return _is_engineering_task_record(record) and _status_norm(record) not in ENGINEERING_NON_OPEN_STATUSES


def _is_closed_engineering_task_record(record: Dict[str, Any]) -> bool:
    return _is_engineering_task_record(record) and _status_norm(record) in ENGINEERING_CLOSED_STATUSES


def get_journal_views(include_test: bool = False) -> Dict[str, Any]:
    records = list_records(include_test=include_test, include_archived=include_test)
    acceptance_checks = [r for r in records if _is_acceptance_check_record(r)]
    engineering_tasks = [r for r in records if _is_engineering_task_record(r)]
    open_tasks = [r for r in engineering_tasks if _is_open_engineering_task_record(r)]
    closed_tasks = [r for r in engineering_tasks if _is_closed_engineering_task_record(r)]
    return {
        'records': records,
        'acceptance_checks': acceptance_checks,
        'engineering_tasks': engineering_tasks,
        'open_engineering_tasks': open_tasks,
        'closed_engineering_tasks': closed_tasks,
        'summary': {
            'total_records': len(records),
            'release_manager_checks': len(acceptance_checks),
            'open_engineering_tasks': len(open_tasks),
            'closed_engineering_tasks': len(closed_tasks),
        },
    }


def list_open_engineering_tasks(include_test: bool = False) -> List[Dict[str, Any]]:
    return get_journal_views(include_test=include_test).get('open_engineering_tasks') or []

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
        # W14.7: user-facing manual/global registration response must stay simple.
        # The journal stores engineering detail internally; the user should not
        # receive technical routing content.
        lines = ['Записал в журнал развития.']
    return {
        'status': 'ok',
        'render_mode': 'development_journal_capture',
        'context': {'level': 'development_journal', 'object_name': 'Development Journal', 'period': None},
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': ['журнал развития — показать накопленные записи', 'экспорт журнала развития — экспорт Production-записей'],
        'development_journal_record': record,
    }


def _fmt_acceptance_status(record: Dict[str, Any]) -> str:
    ctx = record.get('runtime_context') if isinstance(record.get('runtime_context'), dict) else {}
    rep = record.get('reproduction_data') if isinstance(record.get('reproduction_data'), dict) else {}
    return str(ctx.get('result') or rep.get('result') or record.get('status') or '—')


def _acceptance_release(record: Dict[str, Any]) -> str:
    ctx = record.get('runtime_context') if isinstance(record.get('runtime_context'), dict) else {}
    rep = record.get('reproduction_data') if isinstance(record.get('reproduction_data'), dict) else {}
    return str(ctx.get('release_id') or rep.get('release_id') or '—')


def _acceptance_scenarios(record: Dict[str, Any]) -> Any:
    ctx = record.get('runtime_context') if isinstance(record.get('runtime_context'), dict) else {}
    rep = record.get('reproduction_data') if isinstance(record.get('reproduction_data'), dict) else {}
    return ctx.get('scenarios_executed') or rep.get('scenarios_executed') or 0


def _acceptance_defects(record: Dict[str, Any]) -> Any:
    ctx = record.get('runtime_context') if isinstance(record.get('runtime_context'), dict) else {}
    rep = record.get('reproduction_data') if isinstance(record.get('reproduction_data'), dict) else {}
    return ctx.get('defects_found') or rep.get('defects_found') or 0


def build_journal_response(export: bool = False, include_test: bool = False) -> Dict[str, Any]:
    views = get_journal_views(include_test=include_test)
    records = views['records']
    acceptance_checks = sorted(
        views['acceptance_checks'],
        key=lambda r: str(r.get('created_at') or r.get('last_seen_at') or ''),
        reverse=True,
    )
    open_tasks = sorted(
        views['open_engineering_tasks'],
        key=lambda r: str(r.get('last_seen_at') or r.get('created_at') or ''),
        reverse=True,
    )
    closed_tasks = sorted(
        views['closed_engineering_tasks'],
        key=lambda r: str(r.get('updated_at') or r.get('last_seen_at') or ''),
        reverse=True,
    )
    summary = views['summary']
    title = '📤 Экспорт Development Journal VECTRA' if export else '📒 Development Journal VECTRA'
    scope = 'Все записи, включая TEST/Archived' if include_test else 'Production-записи, TEST/Archived исключены'
    lines: List[str] = [
        f'# {title}',
        '',
        f'Режим: **{scope}**',
        '',
        '## Общая статистика',
        f'- **Всего записей:** {summary["total_records"]}',
        f'- **Проверок Release Manager:** {summary["release_manager_checks"]}',
        f'- **Открытых инженерных задач:** {summary["open_engineering_tasks"]}',
        f'- **Закрытых инженерных задач:** {summary["closed_engineering_tasks"]}',
        '',
        '## История Product Acceptance',
    ]
    if not acceptance_checks:
        lines.append('Проверок Release Manager пока нет.')
    else:
        for idx, r in enumerate(acceptance_checks[:50 if not export else len(acceptance_checks)], start=1):
            lines += [
                '',
                f'### Проверка №{idx}',
                f'- **Дата:** {r.get("created_at") or r.get("first_seen_at") or "—"}',
                f'- **Release:** {_acceptance_release(r)}',
                f'- **Проверено:** {_acceptance_scenarios(r)} сценария',
                f'- **Новых дефектов:** {_acceptance_defects(r)}',
                f'- **Статус:** {_fmt_acceptance_status(r)}',
            ]
    lines += ['', '## Открытые инженерные задачи']
    if not open_tasks:
        lines.append('Открытых инженерных задач нет.')
    else:
        for r in open_tasks[:50 if not export else len(open_tasks)]:
            ctx = r.get('runtime_context') or r.get('context') or {}
            lines += [
                '', f'### {r.get("id")} — {r.get("event_type") or r.get("type")}',
                f'- **Статус:** {r.get("status")}',
                f'- **Приоритет:** {r.get("priority")}',
                f'- **Компонент:** {r.get("component")}',
                f'- **Подсистема:** {r.get("subsystem")}',
                f'- **Описание:** {r.get("technical_reason")}',
                f'- **Влияние:** {r.get("impact") or "—"}',
                f'- **Предполагаемая причина:** {r.get("suspected_root_cause")}',
                f'- **Направление исправления:** {r.get("proposed_fix_direction")}',
                f'- **Повторов:** {r.get("occurrence_count", 1)}',
                f'- **История повторений:** первое {r.get("first_seen_at") or r.get("created_at")}; последнее {r.get("last_seen_at") or r.get("updated_at")}',
                f'- **Сценарий:** {r.get("scenario") or "—"}',
                f'- **Ожидалось:** {r.get("expected_behavior") or "—"}',
                f'- **Фактически:** {r.get("actual_behavior") or "—"}',
                f'- **Повторяемость:** {r.get("reproducibility") or "—"}',
                f'- **Регрессия:** {"да" if r.get("is_regression") else "нет"}',
                f'- **Связи:** {", ".join(r.get("related_record_ids") or []) or "—"}',
                f'- **Контекст:** {_fmt_context(ctx)}',
                f'- **Исправлен релизом:** {r.get("fixed_release") or "—"}',
                f'- **Ожидает проверки с:** {r.get("awaiting_verification_at") or "—"}',
            ]
    if include_test or export:
        lines += ['', '## Закрытые инженерные задачи']
        if not closed_tasks:
            lines.append('Закрытых инженерных задач нет.')
        else:
            for r in closed_tasks[:50 if not export else len(closed_tasks)]:
                lines += ['', f'### {r.get("id")} — {r.get("event_type") or r.get("type")}', f'- **Текущий статус:** {r.get("status")}', f'- **Исправлено релизом:** {r.get("fixed_release") or "—"}', f'- **Подтверждено релизом:** {r.get("confirmed_release") or "—"}', f'- **Закрыто:** {r.get("closed_at") or r.get("updated_at") or "—"}', f'- **Компонент:** {r.get("component")}', f'- **Описание:** {r.get("technical_reason")}']
    if export:
        lifecycle_tasks = sorted(open_tasks + closed_tasks, key=lambda r: str(r.get('id') or ''))
        lines += ['', '## История изменения статусов']
        if not lifecycle_tasks:
            lines.append('История статусов отсутствует: инженерных задач нет.')
        else:
            for r in lifecycle_tasks:
                _ensure_status_history(r)
                lines += ['', f'### {r.get("id")} — текущий статус: {r.get("status") or "—"}']
                for event in r.get('status_history') or []:
                    lines.append(
                        f'- **{event.get("timestamp") or "—"}** → {event.get("status") or "—"}; '
                        f'релиз: {event.get("release") or "—"}; '
                        f'версия: {event.get("version") or "—"}; '
                        f'источник: {event.get("source") or event.get("actor") or "—"}; '
                        f'комментарий: {event.get("comment") or "—"}'
                    )
        lines += [
            '',
            '## Инструкция для Laboratory',
            'Использовать для анализа только раздел **Открытые инженерные задачи**.',
            'Раздел **История Product Acceptance** является журналом выполненных проверок Release Manager и не считается списком дефектов.',
        ]
    else:
        lines += ['', '## Доступные действия', '1. **экспорт журнала развития** — экспорт Production-журнала.', '2. **экспорт журнала включая тест** — служебная проверка.', '3. **dry run journal: это баг** — smoke-test без сохранения.']
    return {
        'status': 'ok',
        'render_mode': 'development_journal_export' if export else 'development_journal',
        'context': {'level': 'development_journal', 'object_name': 'Development Journal', 'period': None},
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': ['экспорт журнала развития — Production export', 'анализ журнала — Laboratory анализ открытых инженерных задач'],
        'development_journal': {
            'schema_version': 'DevelopmentJournalExport/v2.2-lifecycle',
            'summary': summary,
            'records_count': len(records),
            'acceptance_checks_count': len(acceptance_checks),
            'open_engineering_tasks_count': len(open_tasks),
            'closed_engineering_tasks_count': len(closed_tasks),
            'acceptance_checks': acceptance_checks if export else acceptance_checks[:50],
            'open_engineering_tasks': open_tasks if export else open_tasks[:50],
            'closed_engineering_tasks': closed_tasks if export else closed_tasks[:50],
            'include_test': include_test,
        },
    }

# ---------------------------------------------------------------------------
# W14.8 — Dialogue Engineering Review
# ---------------------------------------------------------------------------

_DIALOGUE_DEFECT_RULES = [
    {
        'event_type': 'workspace_markdown_missing_or_incomplete',
        'type': 'Engineering Bug',
        'component': 'workspace_runtime',
        'subsystem': 'workspace_rendering',
        'priority': 'P0',
        'patterns': ('workspace_markdown', 'не сформирован', 'пустой экран', 'не открыл экран', 'урезан', 'урезанный', 'сокращаешь ответы'),
        'technical_reason': 'Workspace rendering contract is violated or the user receives an incomplete workspace instead of full workspace_markdown.',
        'suspected_root_cause': 'Renderer or runtime falls back to partial blocks, trims output incorrectly, or does not preserve the API-composed workspace_markdown.',
        'proposed_fix_direction': 'Enforce single workspace_markdown rendering, block fallback composition from service blocks, and add regression tests for full-screen rendering.',
    },
    {
        'event_type': 'navigation_state_context_loss',
        'type': 'Engineering Bug',
        'component': 'state_layer',
        'subsystem': 'navigation_runtime',
        'priority': 'P0',
        'patterns': ('назад', 'контекст', 'потерял', 'не тот контекст', 'рукавичка', 'нули', 'перешел не туда'),
        'technical_reason': 'Navigation or state context is lost, causing commands to resolve against the wrong active workspace or parent object.',
        'suspected_root_cause': 'Short commands, object openings or back navigation read stale last_payload/current_screen state or incorrectly reuse parent filters.',
        'proposed_fix_direction': 'Make active_workspace_state the only source for local commands, persist stack snapshots, and test back/reasons/all without API context drift.',
    },
    {
        'event_type': 'numeric_action_misrouting',
        'type': 'UX Bug',
        'component': 'workspace_action_map',
        'subsystem': 'navigation_runtime',
        'priority': 'P1',
        'patterns': ('цифра 3', 'команда 3', 'нажал 3', 'третий пункт', 'не тот пункт', 'пошел на'),
        'technical_reason': 'Numeric user command is resolved against an invisible or stale action map instead of the visible menu.',
        'suspected_root_cause': 'workspace_action_map does not match rendered workspace_markdown menu or is not refreshed after screen changes.',
        'proposed_fix_direction': 'Generate numeric actions from the visible “Что делаем дальше” menu and add tests for all numbered options.',
    },
    {
        'event_type': 'all_command_incomplete_registry',
        'type': 'Product Bug',
        'component': 'list_view',
        'subsystem': 'all_command',
        'priority': 'P1',
        'patterns': ('все', 'показать все', 'полный список', 'не показывает все', 'только вода', 'без напитки', 'нет напитки'),
        'technical_reason': 'The “all” command returns an incomplete registry for the current object level.',
        'suspected_root_cause': 'Current level, parent filter or list grouping is resolved incorrectly; full registry is built from partial screen data instead of DATA/API.',
        'proposed_fix_direction': 'Resolve “all” strictly from active workspace state and DATA grouping for the current level; add regression tests for category/SKU completeness.',
    },
    {
        'event_type': 'dialogue_command_missing_api_route',
        'type': 'Engineering Bug',
        'component': 'development_journal',
        'subsystem': 'dialogue_engineering_review',
        'priority': 'P1',
        'patterns': ('проанализируй диалог', 'выявленные дефекты', 'пакетный анализ', 'engineering review'),
        'technical_reason': 'Development Journal lacks or must route the global dialogue engineering review command independently from workspace runtime.',
        'suspected_root_cause': 'Only single-record capture exists; no batch dialogue review layer is available for Product Acceptance sessions.',
        'proposed_fix_direction': 'Add a global Development Journal dialogue-review route that accepts sanitized dialogue context, deduplicates issues and writes unique engineering records.',
    },
    {
        'event_type': 'six_month_dynamics_missing',
        'type': 'Product Bug',
        'component': 'workspace_composition',
        'subsystem': 'trend_analysis',
        'priority': 'P1',
        'patterns': ('динамика', 'полгода', '6 месяцев', 'шесть месяцев', 'только один месяц', 'один динамик'),
        'technical_reason': 'Workspace lacks required multi-month dynamics and shows only a single-month snapshot.',
        'suspected_root_cause': 'Workspace composition still uses old one-period KPI contract instead of the approved trend/dynamics block.',
        'proposed_fix_direction': 'Add 6–12 month trend blocks where required by Workspace Definitions and make current month only the endpoint of the trend.',
    },
    {
        'event_type': 'cannot_parse_or_voice_opening_failure',
        'type': 'Engineering Bug',
        'component': 'intent_router',
        'subsystem': 'voice_object_opening',
        'priority': 'P1',
        'patterns': ('cannot_parse', 'не распозна', 'не понял запрос', 'не смог открыть', 'покажи варус', 'покажи труш', 'открой'),
        'technical_reason': 'Free-form object opening or analytical command is not resolved into the expected workspace route.',
        'suspected_root_cause': 'Intent router or entity resolver does not map natural language object requests to the same route as structured navigation.',
        'proposed_fix_direction': 'Unify voice/free-form opening with canonical workspace query resolution and add entity regression cases.',
    },
    {
        'event_type': 'development_journal_export_or_persistence_gap',
        'type': 'Engineering Bug',
        'component': 'development_journal',
        'subsystem': 'journal_export',
        'priority': 'P1',
        'patterns': ('экспорт журнала', 'журнал не запис', 'не попало в журнал', 'получить журнал', 'dry run journal', 'тест маршрута'),
        'technical_reason': 'Development Journal capture/export route is ambiguous or does not reliably show persisted production records.',
        'suspected_root_cause': 'Test, dry-run and production journal records are not clearly separated or export route does not include expected records.',
        'proposed_fix_direction': 'Keep TEST/Dry Run out of Production Export, expose explicit include_test export, and add persistence/export smoke tests.',
    },
    {
        'event_type': 'ux_unclear_acceptance_flow',
        'type': 'UX Problem',
        'component': 'assistant_experience',
        'subsystem': 'product_acceptance',
        'priority': 'P2',
        'patterns': ('не понимаю', 'непонятно', 'как оценивать', 'что надо сделать', 'как работать', 'что дальше'),
        'technical_reason': 'User cannot clearly understand Product Acceptance state, next step or whether a sprint defect is open/closed.',
        'suspected_root_cause': 'Assistant response lacks explicit acceptance status, evidence of performed checks, or next action contract.',
        'proposed_fix_direction': 'Standardize acceptance-state responses with status, evidence, unresolved defects and next production test command.',
    },
]


def _iter_dialogue_text(dialogue: Any) -> List[str]:
    """Return raw text only for in-memory analysis; never persist returned text."""
    if dialogue is None:
        return []
    if isinstance(dialogue, str):
        return [dialogue]
    if isinstance(dialogue, dict):
        values = []
        for key in ('content', 'text', 'message', 'user_message', 'assistant_message'):
            if dialogue.get(key):
                values.append(str(dialogue.get(key)))
        if isinstance(dialogue.get('messages'), list):
            values.extend(_iter_dialogue_text(dialogue.get('messages')))
        return values
    if isinstance(dialogue, list):
        values: List[str] = []
        for item in dialogue:
            values.extend(_iter_dialogue_text(item))
        return values
    return [str(dialogue)]


def _session_trace_texts(session_ctx: Optional[Dict[str, Any]]) -> List[str]:
    """Extract sanitized trace descriptions from session context if available."""
    if not isinstance(session_ctx, dict):
        return []
    texts: List[str] = []
    for key in ('dialogue_trace', 'engineering_trace', 'runtime_events'):
        value = session_ctx.get(key)
        if isinstance(value, list):
            for item in value[-200:]:
                if isinstance(item, dict):
                    # Only engineering metadata is used from stored trace.
                    parts = [str(item.get(k) or '') for k in ('intent_hint', 'event_type', 'render_mode', 'reason', 'error_code')]
                    texts.append(' '.join(parts))
                else:
                    texts.append(str(item))
    screen = session_ctx.get('current_screen') or session_ctx.get('last_payload') or {}
    if isinstance(screen, dict):
        texts.append(' '.join(str(screen.get(k) or '') for k in ('status', 'reason', 'render_mode')))
    return texts


def _dialogue_runtime_context(session_ctx: Optional[Dict[str, Any]], session_id: str, text_count: int) -> Dict[str, Any]:
    ctx = _screen_context(session_ctx)
    ctx.update({
        'source_scope': 'dialogue_engineering_review',
        'session_id_hash': _stable_hash(session_id),
        'analyzed_items_count': int(text_count),
    })
    return ctx


def _rule_matches(rule: Dict[str, Any], norm_blob: str) -> bool:
    return any(_normalize(pattern) in norm_blob for pattern in rule.get('patterns') or [])


def _generic_defect_from_blob(norm_blob: str) -> Optional[Dict[str, Any]]:
    if not norm_blob:
        return None
    if any(token in norm_blob for token in ('баг', 'ошибка', 'не работает', 'сломал', 'не смог', 'проблема')):
        return {
            'event_type': 'unclassified_dialogue_engineering_defect',
            'type': 'Engineering Bug',
            'component': 'unclassified_runtime',
            'subsystem': 'dialogue_engineering_review',
            'priority': 'P2',
            'technical_reason': 'Dialogue review detected an engineering defect signal that did not match a specialized classifier.',
            'suspected_root_cause': 'The issue requires laboratory triage; classifier could not safely infer a more specific component.',
            'proposed_fix_direction': 'Review sanitized runtime context, classify the owning component, and convert the record into a precise engineering task.',
        }
    return None


def analyze_dialogue_and_create_records(
    dialogue: Any = None,
    session_ctx: Optional[Dict[str, Any]] = None,
    session_id: str = 'default',
    dry_run: bool = False,
    is_test: bool = False,
) -> Dict[str, Any]:
    """Analyze a Product Acceptance dialogue and persist unique engineering records.

    Raw dialogue text is used only in memory for classification. Journal records
    store normalized engineering knowledge, hashes/counts and runtime context —
    never user quotes or chat transcript.
    """
    texts = _iter_dialogue_text(dialogue) + _session_trace_texts(session_ctx)
    norm_blob = _normalize(' '.join(texts))
    runtime_ctx = _dialogue_runtime_context(session_ctx, session_id, len(texts))
    active_state = _active_state(session_ctx)

    candidates: List[Dict[str, Any]] = []
    for rule in _DIALOGUE_DEFECT_RULES:
        if _rule_matches(rule, norm_blob):
            candidates.append(dict(rule))
    generic = _generic_defect_from_blob(norm_blob)
    if generic and not candidates:
        candidates.append(generic)

    # If command is executed with no accessible dialogue history, persist one
    # actionable record for the missing integration instead of pretending that
    # the whole chat was visible to the API.
    if not candidates:
        candidates.append({
            'event_type': 'dialogue_history_not_supplied_to_api',
            'type': 'Architecture Defect',
            'component': 'custom_gpt_action_contract',
            'subsystem': 'dialogue_engineering_review',
            'priority': 'P1',
            'technical_reason': 'Dialogue review command was executed, but the API received no analyzable dialogue history or only empty sanitized session metadata.',
            'suspected_root_cause': 'Custom GPT action contract does not pass the current conversation or runtime trace to the Development Journal dialogue-review API.',
            'proposed_fix_direction': 'Update Custom GPT action usage so the command sends the current conversation as transient input, or persist sanitized engineering trace per turn.',
        })

    # Dedupe inside the batch before writing; journal-level aggregation handles
    # repeats across sessions.
    unique: Dict[str, Dict[str, Any]] = {}
    for item in candidates:
        key = _stable_hash({
            'event_type': item.get('event_type'),
            'component': item.get('component'),
            'subsystem': item.get('subsystem'),
            'technical_reason': item.get('technical_reason'),
        })
        unique.setdefault(key, item)

    saved: List[Dict[str, Any]] = []
    for item in unique.values():
        record = add_runtime_event(
            event_type=str(item.get('event_type') or 'dialogue_engineering_defect'),
            component=str(item.get('component') or 'development_journal'),
            system_level='dialogue_engineering_review',
            technical_reason=str(item.get('technical_reason') or 'Dialogue engineering defect detected.'),
            suspected_root_cause=str(item.get('suspected_root_cause') or 'Requires engineering triage.'),
            priority=str(item.get('priority') or 'P1'),
            runtime_context={**runtime_ctx, 'defect_type': item.get('type'), 'subsystem': item.get('subsystem')},
            active_workspace_state=active_state,
            error_code=str(item.get('event_type') or 'dialogue_engineering_defect'),
            reproduction_data={
                'source': 'dialogue_engineering_review',
                'dialogue_hash': _stable_hash(norm_blob),
                'raw_dialogue_persisted': False,
            },
            session_id=session_id,
            dry_run=dry_run,
            is_test=is_test,
            subsystem=str(item.get('subsystem') or 'dialogue_engineering_review'),
            proposed_fix_direction=str(item.get('proposed_fix_direction') or 'Classify and fix in the owning component.'),
        )
        saved.append(record)

    return {
        'records': saved,
        'unique_count': len(saved),
        'analyzed_items_count': len(texts),
        'dry_run': dry_run,
    }


def build_dialogue_review_response(result: Dict[str, Any]) -> Dict[str, Any]:
    count = int(result.get('unique_count') or len(result.get('records') or []))
    if result.get('dry_run'):
        lines = [
            'Development Journal dialogue review routing OK.',
            f'Обнаружено: {count} инженерных проблем.',
            'Dry-run completed. Records not persisted.',
        ]
    else:
        lines = [
            'Проанализировал диалог.',
            f'Обнаружено: {count} инженерных проблем.',
            'Все записи сохранены в Development Journal.',
        ]
    return {
        'status': 'ok',
        'render_mode': 'development_journal_capture',
        'context': {'level': 'development_journal', 'object_name': 'Development Journal', 'period': None},
        'workspace_primary_block': lines,
        'workspace_markdown': '\n\n'.join(lines),
        'navigation_block': ['экспорт журнала развития — проверить общий engineering backlog'],
        'development_journal_review': {
            'records_count': count,
            'analyzed_items_count': result.get('analyzed_items_count'),
            'record_ids': [r.get('id') for r in result.get('records') or []],
        },
    }
