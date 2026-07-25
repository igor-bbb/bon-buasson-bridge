"""Runtime scaffold for VECTRA Closed-Loop Corporate Intelligence.

W13 implements the first production-visible control layer for the final
architecture: decisions, tasks, feedback and corporate memory are represented as
runtime records and can be inspected from Production. This is intentionally a
file-backed prototype adapter; a future Data Mart/database can replace storage
without changing the public command contract.
"""

from __future__ import annotations

import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from threading import Lock
from typing import Any, Dict, List, Optional

STORE_FILE = Path('/tmp/vectra_corporate_intelligence.json')
STORE_LOCK = Lock()

DECISION_COMMANDS = {
    'создать решение', 'зафиксировать решение', 'решение', 'decision',
}
TASK_COMMANDS = {
    'создать задачу', 'создай задачу', 'сформировать задачу', 'task',
}
TASK_SHOW_COMMANDS = {
    'задачи', 'показать задачи', 'мои задачи', 'task workspace', 'открыть задачи',
}
FEEDBACK_COMMANDS = {
    'зафиксировать результат', 'результат выполнено', 'feedback', 'обратная связь', 'закрыть задачу',
}
MEMORY_COMMANDS = {
    'корпоративная память', 'показать корпоративную память', 'knowledge', 'knowledge engine', 'память компании',
}
CLOSED_LOOP_COMMANDS = {
    'closed loop', 'замкнутый цикл', 'статус замкнутого цикла', 'closed-loop status',
}
PRODUCT_INTELLIGENCE_COMMANDS = {
    'product intelligence', 'продуктовый интеллект', 'анализ развития продукта',
}


def _norm(value: Any) -> str:
    text = str(value or '').strip().lower().replace('ё', 'е')
    return re.sub(r'\s+', ' ', text)


def _starts_with_any(message: Any, commands: set[str]) -> bool:
    norm = _norm(message)
    return any(norm == c or norm.startswith(c + ' ') or norm.startswith(c + ':') or norm.startswith(c + ' —') or norm.startswith(c + ' -') for c in commands)


def detect_corporate_intelligence_command(message: Any) -> Optional[str]:
    if _starts_with_any(message, DECISION_COMMANDS):
        return 'create_decision'
    if _starts_with_any(message, TASK_COMMANDS):
        return 'create_task'
    if _norm(message) in TASK_SHOW_COMMANDS:
        return 'show_tasks'
    if _starts_with_any(message, FEEDBACK_COMMANDS):
        return 'capture_feedback'
    if _norm(message) in MEMORY_COMMANDS:
        return 'show_memory'
    if _norm(message) in CLOSED_LOOP_COMMANDS:
        return 'closed_loop_status'
    if _norm(message) in PRODUCT_INTELLIGENCE_COMMANDS:
        return 'product_intelligence'
    return None


def _read_store() -> Dict[str, List[Dict[str, Any]]]:
    if not STORE_FILE.exists():
        return {'decisions': [], 'tasks': [], 'feedback': [], 'memory': []}
    try:
        raw = json.loads(STORE_FILE.read_text(encoding='utf-8'))
        if isinstance(raw, dict):
            return {
                'decisions': [x for x in raw.get('decisions', []) if isinstance(x, dict)],
                'tasks': [x for x in raw.get('tasks', []) if isinstance(x, dict)],
                'feedback': [x for x in raw.get('feedback', []) if isinstance(x, dict)],
                'memory': [x for x in raw.get('memory', []) if isinstance(x, dict)],
            }
    except Exception:
        pass
    return {'decisions': [], 'tasks': [], 'feedback': [], 'memory': []}


def _write_store(store: Dict[str, List[Dict[str, Any]]]) -> None:
    STORE_FILE.write_text(json.dumps(store, ensure_ascii=False, indent=2), encoding='utf-8')


def _next_id(items: List[Dict[str, Any]], prefix: str) -> str:
    max_num = 0
    for item in items:
        raw = str(item.get('id') or '')
        m = re.search(r'(\d+)$', raw)
        if m:
            max_num = max(max_num, int(m.group(1)))
    return f'{prefix}-{max_num + 1:04d}'


def _extract_tail(message: Any, commands: set[str], fallback: str) -> str:
    text = str(message or '').strip()
    norm = _norm(text)
    for cmd in sorted(commands, key=len, reverse=True):
        if norm == cmd:
            return fallback
        if norm.startswith(cmd + ':') or norm.startswith(cmd + ' -') or norm.startswith(cmd + ' —') or norm.startswith(cmd + ' '):
            return text[len(cmd):].strip(' :-—') or fallback
    return fallback


def _workspace_context(session_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
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
        'render_mode': screen.get('render_mode') if isinstance(screen, dict) else None,
    }


def _fmt_context(ctx: Dict[str, Any]) -> str:
    parts = [str(ctx.get(k) or '').strip() for k in ('level', 'object_name', 'period')]
    parts = [p for p in parts if p]
    return ' / '.join(parts) if parts else '—'


def _payload(lines: List[str], mode: str, extra: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = {
        'status': 'ok',
        'render_mode': mode,
        'context': {'level': 'corporate_intelligence', 'object_name': 'VECTRA Corporate Intelligence', 'period': None},
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': [
            'замкнутый цикл — статус Decision → Task → Feedback → Learning',
            'создать решение: ... — зафиксировать управленческое решение',
            'создать задачу: ... — создать задачу по текущему Workspace',
            'задачи — показать Task Workspace',
            'корпоративная память — показать накопленные знания',
        ],
    }
    if extra:
        payload.update(extra)
    return payload


def _create_decision(message: Any, session_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    with STORE_LOCK:
        store = _read_store()
        now = datetime.now(timezone.utc).isoformat()
        ctx = _workspace_context(session_ctx)
        decision = {
            'id': _next_id(store['decisions'], 'DEC'),
            'status': 'Draft',
            'title': _extract_tail(message, DECISION_COMMANDS, 'Управленческое решение по текущему Workspace'),
            'context': ctx,
            'expected_effect': None,
            'owner': None,
            'due_date': None,
            'created_at': now,
            'updated_at': now,
            'source': 'Production command',
        }
        store['decisions'].append(decision)
        _write_store(store)
    lines = [
        '# ✅ Решение зафиксировано', '',
        f'**ID:** {decision["id"]}',
        f'**Статус:** {decision["status"]}',
        f'**Контекст:** {_fmt_context(ctx)}',
        f'**Суть:** {decision["title"]}', '',
        'Следующий шаг: **создать задачу** или уточнить ожидаемый эффект и владельца.',
    ]
    return _payload(lines, 'decision_capture', {'decision_record': decision})


def _create_task(message: Any, session_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    with STORE_LOCK:
        store = _read_store()
        now = datetime.now(timezone.utc).isoformat()
        ctx = _workspace_context(session_ctx)
        task = {
            'id': _next_id(store['tasks'], 'TASK'),
            'status': 'Draft',
            'priority': 'Medium',
            'title': _extract_tail(message, TASK_COMMANDS, 'Задача по текущему Workspace'),
            'context': ctx,
            'basis': 'Created from current Workspace context',
            'expected_effect': None,
            'owner': None,
            'due_date': None,
            'created_at': now,
            'updated_at': now,
        }
        store['tasks'].append(task)
        _write_store(store)
    lines = [
        '# ✅ Задача создана', '',
        f'**ID:** {task["id"]}',
        f'**Статус:** {task["status"]}',
        f'**Приоритет:** {task["priority"]}',
        f'**Контекст:** {_fmt_context(ctx)}',
        f'**Задача:** {task["title"]}', '',
        'Задача создана как черновик. Следующий уровень реализации — назначение ответственного, срока и ожидаемого эффекта.',
    ]
    return _payload(lines, 'task_capture', {'task_record': task})


def _capture_feedback(message: Any, session_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    with STORE_LOCK:
        store = _read_store()
        now = datetime.now(timezone.utc).isoformat()
        ctx = _workspace_context(session_ctx)
        feedback = {
            'id': _next_id(store['feedback'], 'FB'),
            'status': 'Captured',
            'description': _extract_tail(message, FEEDBACK_COMMANDS, 'Результат зафиксирован по текущему Workspace'),
            'context': ctx,
            'confirmed_by_data': False,
            'created_at': now,
        }
        memory = {
            'id': _next_id(store['memory'], 'MEM'),
            'type': 'Feedback Observation',
            'status': 'Pending Validation',
            'description': feedback['description'],
            'context': ctx,
            'source_feedback_id': feedback['id'],
            'created_at': now,
        }
        store['feedback'].append(feedback)
        store['memory'].append(memory)
        _write_store(store)
    lines = [
        '# ✅ Результат зафиксирован', '',
        f'**Feedback:** {feedback["id"]}',
        f'**Corporate Memory:** {memory["id"]}',
        f'**Контекст:** {_fmt_context(ctx)}', '',
        'Запись добавлена в корпоративную память как наблюдение. Она станет подтверждённым знанием только после проверки эффектом в DATA.',
    ]
    return _payload(lines, 'feedback_capture', {'feedback_record': feedback, 'memory_record': memory})


def _show_tasks() -> Dict[str, Any]:
    store = _read_store()
    tasks = sorted(store['tasks'], key=lambda r: str(r.get('created_at') or ''), reverse=True)
    by_status = Counter(str(t.get('status') or 'Unknown') for t in tasks)
    lines = ['# 📌 Task Workspace', '', f'Всего задач: **{len(tasks)}**', '', '## Статусы']
    if by_status:
        for status, count in by_status.most_common():
            lines.append(f'- **{status}:** {count}')
    else:
        lines.append('- Задач пока нет.')
    lines += ['', '## Последние задачи']
    if tasks:
        lines += ['| ID | Статус | Приоритет | Контекст | Задача |', '|---|---|---|---|---|']
        for t in tasks[:50]:
            lines.append(f'| {t.get("id")} | {t.get("status")} | {t.get("priority")} | {_fmt_context(t.get("context") or {})} | {t.get("title")} |')
    else:
        lines.append('Скажите **создать задачу: ...** в нужном Workspace.')
    return _payload(lines, 'task_workspace', {'tasks': tasks[:50]})


def _show_memory() -> Dict[str, Any]:
    store = _read_store()
    memory = sorted(store['memory'], key=lambda r: str(r.get('created_at') or ''), reverse=True)
    by_status = Counter(str(m.get('status') or 'Unknown') for m in memory)
    lines = ['# 🧠 Corporate Memory / Knowledge Engine', '', f'Всего записей: **{len(memory)}**', '', '## Статусы знаний']
    if by_status:
        for status, count in by_status.most_common():
            lines.append(f'- **{status}:** {count}')
    else:
        lines.append('- Знаний пока нет.')
    lines += ['', '## Последние записи']
    if memory:
        lines += ['| ID | Тип | Статус | Контекст | Содержание |', '|---|---|---|---|---|']
        for m in memory[:50]:
            lines.append(f'| {m.get("id")} | {m.get("type")} | {m.get("status")} | {_fmt_context(m.get("context") or {})} | {m.get("description")} |')
    else:
        lines.append('Корпоративная память начнёт наполняться после фиксации результатов и подтверждения эффектов.')
    return _payload(lines, 'corporate_memory', {'corporate_memory': memory[:50]})


def _closed_loop_status() -> Dict[str, Any]:
    store = _read_store()
    lines = [
        '# 🔁 Closed-Loop Intelligence Status', '',
        '## Контур управления', '',
        '| Этап | Runtime-состояние | Количество | Следующий шаг |',
        '|---|---|---:|---|',
        f'| Decision | Реестр решений доступен | {len(store["decisions"])} | связывать решение с задачами |',
        f'| Action / Task | Task Workspace доступен | {len(store["tasks"])} | добавить ответственных, сроки, статусы |',
        f'| Feedback | Фиксация результата доступна | {len(store["feedback"])} | подтверждать эффект по DATA |',
        f'| Learning / Memory | Corporate Memory доступна | {len(store["memory"])} | утверждать знания после проверки |',
        '',
        '## Статус Architecture Complete', '',
        '**Runtime-контур замкнутого цикла создан как MVP.** Полная зрелость требует постоянного хранилища, прав доступа и автоматической проверки эффекта по DATA.',
    ]
    return _payload(lines, 'closed_loop_status', {'closed_loop': {k: len(v) for k, v in store.items()}})


def _product_intelligence() -> Dict[str, Any]:
    try:
        from app.development_journal import list_records as list_dev_records
        dev_records = list_dev_records()
    except Exception:
        dev_records = []
    by_type = Counter(str(r.get('type') or 'Unknown') for r in dev_records)
    by_context = Counter(_fmt_context(r.get('context') or {}) for r in dev_records)
    lines = ['# 🧩 Product Intelligence', '', f'Записей Development Journal: **{len(dev_records)}**', '', '## Повторяющиеся типы']
    if by_type:
        for t, c in by_type.most_common(10):
            lines.append(f'- **{t}:** {c}')
    else:
        lines.append('- Записей пока нет.')
    lines += ['', '## Горячие зоны продукта']
    if by_context:
        for ctx, c in by_context.most_common(10):
            lines.append(f'- **{ctx}:** {c}')
    else:
        lines.append('- Недостаточно данных.')
    lines += ['', '## Вывод', '', 'Product Intelligence готовит Product Review на основе Development Journal, но дедупликация и автоматическая оценка влияния остаются следующим уровнем зрелости.']
    return _payload(lines, 'product_intelligence')


def handle_corporate_intelligence_command(kind: str, message: Any, session_ctx: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if kind == 'create_decision':
        return _create_decision(message, session_ctx)
    if kind == 'create_task':
        return _create_task(message, session_ctx)
    if kind == 'show_tasks':
        return _show_tasks()
    if kind == 'capture_feedback':
        return _capture_feedback(message, session_ctx)
    if kind == 'show_memory':
        return _show_memory()
    if kind == 'closed_loop_status':
        return _closed_loop_status()
    if kind == 'product_intelligence':
        return _product_intelligence()
    return _payload(['# Неизвестная команда корпоративного интеллекта'], 'corporate_intelligence_error')
