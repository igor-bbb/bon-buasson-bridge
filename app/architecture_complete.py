"""Architecture Complete gate and Product Intelligence helpers for VECTRA.

W12 does not replace business engines. It adds a single architectural control
surface inside Production VECTRA so the product can be audited as one system
before further development moves through Development Journal.
"""

from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from typing import Any, Dict, List

ARCHITECTURE_COMMANDS = {
    'архитектурный аудит',
    'полный архитектурный аудит',
    'аудит архитектуры',
    'architecture audit',
    'architecture complete',
    'статус архитектуры',
    'проверить архитектуру',
    'architecture status',
    'финальная архитектура',
    'финальный архитектурный статус',
    'architecture complete status',
}

PRODUCT_REVIEW_COMMANDS = {
    'product review',
    'продукт ревью',
    'product review журнала',
    'подготовить product review',
    'подготовить обзор журнала',
    'обзор журнала развития',
}

SPRINT_PLAN_COMMANDS = {
    'подготовить sprint',
    'подготовить спринт',
    'сформировать sprint',
    'сформировать спринт',
    'сформировать sprint из журнала',
    'сформировать спринт из журнала',
}


def _norm(value: Any) -> str:
    text = str(value or '').strip().lower().replace('ё', 'е')
    return re.sub(r'\s+', ' ', text)


def is_architecture_command(message: Any) -> bool:
    return _norm(message) in ARCHITECTURE_COMMANDS


def is_product_review_command(message: Any) -> bool:
    return _norm(message) in PRODUCT_REVIEW_COMMANDS


def is_sprint_plan_command(message: Any) -> bool:
    return _norm(message) in SPRINT_PLAN_COMMANDS


ARCHITECTURE_REGISTRY: List[Dict[str, Any]] = [
    {
        'component': 'Product Model',
        'status': 'Implemented as product contract / requires continuous protection',
        'evidence': 'Workspace-first response model, role-aware screens, Development Journal capture.',
        'gap': 'Sources, Knowledge, Instruction and API still require synchronization review after each sprint.',
        'priority': 'High',
    },
    {
        'component': 'Workspace Architecture',
        'status': 'Partially implemented',
        'evidence': 'Business/Object/Contract/Category/SKU renderers exist and are exposed through workspace_markdown.',
        'gap': 'Not every Workspace yet satisfies full Workspace Definitions: evidence tables, shares, context and future layers vary by level.',
        'priority': 'High',
    },
    {
        'component': 'Business Context Engine',
        'status': 'Partially implemented',
        'evidence': 'Business context, categories, formats, SKU leaders and concentration are rendered in Business Workspace.',
        'gap': 'Needs unified context object reused by all Workspaces rather than screen-specific rendering logic.',
        'priority': 'High',
    },
    {
        'component': 'Assistant Architecture / Role-Adaptive Assistant',
        'status': 'Partially implemented',
        'evidence': 'Workspace texts contain role-specific language and current role labels.',
        'gap': 'Role behavior is still mostly template-based; no central assistant-role policy engine exists yet.',
        'priority': 'High',
    },
    {
        'component': 'Contract Workspace',
        'status': 'Partially implemented',
        'evidence': 'KPI, economy, categories, formats, SKU leaders, missing SKU and negotiation blocks exist.',
        'gap': 'Needs stricter mandatory fields: contract/business factor comparison, shares, potential and proof columns everywhere.',
        'priority': 'High',
    },
    {
        'component': 'Category Workspace',
        'status': 'Partially implemented',
        'evidence': 'Category KPI, format structure, leaders and missing SKU are rendered.',
        'gap': 'Needs full category vs business economy table and stronger format/SKU proof model.',
        'priority': 'Medium',
    },
    {
        'component': 'Product / SKU Workspace',
        'status': 'Partially implemented',
        'evidence': 'SKU opening and SKU workspace renderer exist.',
        'gap': 'SKU Passport is not yet complete as system atom: role, coverage, absence, proof, negotiation and memory are incomplete.',
        'priority': 'High',
    },
    {
        'component': 'Decision Engine',
        'status': 'Partially implemented',
        'evidence': 'Decision summaries and factors exist in domain/summary and rendered decision blocks.',
        'gap': 'Decision lifecycle is not yet formalized as object with owner, expected effect, status and task conversion.',
        'priority': 'High',
    },
    {
        'component': 'Negotiation Engine',
        'status': 'Partially implemented',
        'evidence': 'Negotiation preparation screens and action commands exist.',
        'gap': 'Needs structured negotiation object: goals, concession limits, risk, expected effect, post-meeting outcome.',
        'priority': 'Medium',
    },
    {
        'component': 'Task Engine',
        'status': 'MVP implemented / needs production hardening',
        'evidence': 'Task Workspace, create-task command and file-backed task records exist as runtime scaffold.',
        'gap': 'Requires persistent database, assignment workflow, deadlines, reminders and DATA-based effect validation.',
        'priority': 'High',
    },
    {
        'component': 'Knowledge Engine / Corporate Memory',
        'status': 'MVP implemented / pending validation workflow',
        'evidence': 'Corporate Memory command and feedback-to-memory records exist as runtime scaffold.',
        'gap': 'Requires approval flow, decision/task linkage and reuse of confirmed knowledge inside Workspace context.',
        'priority': 'Medium',
    },
    {
        'component': 'Navigation Model / Conversation Model',
        'status': 'Partially implemented',
        'evidence': 'State layer, back/all/reasons, numeric actions, full path and object opening exist.',
        'gap': 'Needs unified available_actions as single source of truth across every screen and every action mode.',
        'priority': 'High',
    },
    {
        'component': 'Closed-Loop Intelligence',
        'status': 'MVP implemented / runtime loop available',
        'evidence': 'Decision, Task, Feedback and Corporate Memory commands are connected as the first closed-loop runtime scaffold.',
        'gap': 'Requires durable storage, owners, lifecycle enforcement, automatic DATA feedback and Learning validation.',
        'priority': 'High',
    },
    {
        'component': 'Product Intelligence / Development Journal',
        'status': 'MVP implemented / Product Review available',
        'evidence': 'Capture, show, export, Product Review and Sprint Candidate commands exist.',
        'gap': 'Needs deduplication, record resolution lifecycle and direct integration with engineering environment.',
        'priority': 'High',
    },
    {
        'component': 'Production / Development Model',
        'status': 'MVP implemented / manual bridge',
        'evidence': 'Production capture, journal export, product review and sprint candidate form the first Production→Development loop.',
        'gap': 'Direct integration remains manual until platform-level connector is introduced.',
        'priority': 'Medium',
    },
]


def _payload(lines: List[str], mode: str, extra: Dict[str, Any] | None = None) -> Dict[str, Any]:
    payload = {
        'status': 'ok',
        'render_mode': mode,
        'context': {'level': 'architecture', 'object_name': 'VECTRA Architecture', 'period': None},
        'workspace_primary_block': lines,
        'workspace_markdown': '\n'.join(lines),
        'navigation_block': [
            'product review — подготовить Product Review по Development Journal',
            'сформировать спринт — подготовить Sprint-кандидат из журнала',
            'экспорт журнала развития — выгрузить записи для инженерного чата',
        ],
    }
    if extra:
        payload.update(extra)
    return payload


def build_architecture_audit_response() -> Dict[str, Any]:
    total = len(ARCHITECTURE_REGISTRY)
    by_status = Counter(item['status'] for item in ARCHITECTURE_REGISTRY)
    high = [x for x in ARCHITECTURE_REGISTRY if x.get('priority') == 'High']
    lines = [
        '# 🧭 VECTRA Architecture Complete Gate',
        '',
        '## Статус',
        '',
        '**Architecture Complete: MVP-контур реализован, полная промышленная зрелость ещё не подтверждена.**',
        '',
        'Причина: утверждённая архитектура теперь имеет runtime-контуры для Workspace, Assistant, Navigation, Development Journal, Product Review, Sprint Candidate, Decision, Task, Feedback и Corporate Memory. Однако часть контуров работает как MVP и требует промышленного хранилища, прав доступа, автоматической проверки эффекта по DATA и синхронизации Knowledge/Instruction/API.',
        '',
        '## Сводка',
        '',
        f'- Архитектурных компонентов проверено: **{total}**',
        f'- Высокий приоритет доработки: **{len(high)}**',
        '',
        '## Статусы',
    ]
    for status, count in by_status.most_common():
        lines.append(f'- **{status}:** {count}')
    lines += [
        '',
        '## Полный аудит компонентов',
        '',
        '| Компонент | Статус | Что есть | Что нужно закрыть | Приоритет |',
        '|---|---|---|---|---|',
    ]
    for item in ARCHITECTURE_REGISTRY:
        lines.append(
            f'| {item["component"]} | {item["status"]} | {item["evidence"]} | {item["gap"]} | {item["priority"]} |'
        )
    lines += [
        '',
        '## Управленческий вывод',
        '',
        'VECTRA переведена в состояние **Architecture Complete MVP**: основные контуры финальной архитектуры представлены в Production и могут использоваться для непрерывного развития. Следующий шаг — не новое проектирование, а Product Review, промышленное усиление MVP-контуров и закрытие записей Development Journal через Sprint.',
        '',
        '## Что делать дальше',
        '',
        '1. **product review** — разобрать Development Journal и выделить подтверждённые изменения.',
        '2. **сформировать спринт** — подготовить инженерный пакет из подтверждённых записей.',
        '3. **экспорт журнала развития** — передать журнал в инженерный чат.',
    ]
    return _payload(lines, 'architecture_complete_gate', {'architecture_registry': ARCHITECTURE_REGISTRY})


def _record_context(record: Dict[str, Any]) -> str:
    ctx = record.get('context') or {}
    if not isinstance(ctx, dict):
        return '—'
    parts = [str(ctx.get(k) or '').strip() for k in ('level', 'object_name', 'period')]
    parts = [p for p in parts if p]
    return ' / '.join(parts) if parts else '—'


def build_product_review_response(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    records = [r for r in records if isinstance(r, dict)]
    by_type = Counter(str(r.get('type') or 'Unknown') for r in records)
    by_context = Counter(_record_context(r) for r in records)
    high = [r for r in records if str(r.get('priority') or '').lower() == 'high']
    medium = [r for r in records if str(r.get('priority') or '').lower() == 'medium']
    lines = [
        '# 📋 Product Review — Development Journal',
        '',
        f'Всего записей в журнале: **{len(records)}**',
        f'Высокий приоритет: **{len(high)}**',
        f'Средний приоритет: **{len(medium)}**',
        '',
        '## Сводка по типам',
    ]
    if by_type:
        for t, c in by_type.most_common():
            lines.append(f'- **{t}:** {c}')
    else:
        lines.append('- Записей пока нет.')
    lines += ['', '## Где чаще всего возникают замечания']
    for ctx, count in by_context.most_common(10):
        lines.append(f'- **{ctx}:** {count}')
    if not by_context:
        lines.append('- Недостаточно записей.')
    lines += [
        '',
        '## Рекомендуемая классификация для Sprint',
        '',
        '| ID | Тип | Приоритет | Контекст | Суть | Рекомендация |',
        '|---|---|---|---|---|---|',
    ]
    for r in records[:100]:
        priority = r.get('priority') or 'Normal'
        rec = 'включить в ближайший Sprint' if priority in {'High', 'Medium'} or r.get('type') in {'Engineering Bug', 'Missing Data'} else 'рассмотреть / объединить'
        lines.append(f'| {r.get("id")} | {r.get("type")} | {priority} | {_record_context(r)} | {r.get("description")} | {rec} |')
    lines += [
        '',
        '## Следующее действие',
        '',
        'Команда **сформировать спринт** подготовит Sprint-кандидат из записей журнала.',
    ]
    return _payload(lines, 'product_review', {'product_review': {'records_count': len(records)}})


def build_sprint_plan_response(records: List[Dict[str, Any]]) -> Dict[str, Any]:
    records = [r for r in records if isinstance(r, dict)]
    selected = [
        r for r in records
        if str(r.get('priority') or '').lower() in {'high', 'medium'} or r.get('type') in {'Engineering Bug', 'Missing Data', 'Architecture Improvement'}
    ]
    if not selected:
        selected = records[:20]
    lines = [
        '# 🛠 Sprint Candidate — из Development Journal',
        '',
        '## Цель спринта',
        '',
        'Закрыть подтверждённые замечания Production без ручного разбора экранов в инженерном чате.',
        '',
        f'Кандидатов в Sprint: **{len(selected)}**',
        '',
        '## Состав Sprint',
        '',
        '| ID | Тип | Приоритет | Контекст | Что исправить / улучшить | Критерий готовности |',
        '|---|---|---|---|---|---|',
    ]
    for r in selected:
        done = 'поведение в Production соответствует ожидаемому, запись можно закрыть после проверки'
        lines.append(f'| {r.get("id")} | {r.get("type")} | {r.get("priority")} | {_record_context(r)} | {r.get("description")} | {done} |')
    lines += [
        '',
        '## Definition of Done',
        '',
        '1. Изменения реализованы в коде.',
        '2. Реальные проверки выполнены инструментально.',
        '3. CHANGE LOG и TEST PLAN подготовлены отдельно от deploy-ready ZIP.',
        '4. После деплоя Product Owner проверяет сценарии в Production.',
        '5. Подтверждённые исправления закрываются в Development Journal.',
    ]
    return _payload(lines, 'sprint_candidate', {'sprint_candidate': {'items': selected}})
