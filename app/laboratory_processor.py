"""Laboratory Processor for VECTRA Stabilization S1.

Laboratory no longer analyzes raw dialogues. Its only input is Development
Journal Production records. It groups confirmed defects, estimates root causes
and produces one consolidated Engineering Task package.
"""
from __future__ import annotations

from typing import Any, Dict, List, Tuple
from collections import defaultdict

from app.development_journal import list_open_engineering_tasks


def _norm(value: Any) -> str:
    return str(value or '').strip().lower()


def _component_key(record: Dict[str, Any]) -> Tuple[str, str]:
    return (str(record.get('component') or 'unknown'), str(record.get('subsystem') or 'unknown'))


def _priority_rank(priority: Any) -> int:
    order = {'P0': 0, 'P1': 1, 'P2': 2, 'P3': 3}
    return order.get(str(priority or 'P2').upper(), 2)


def _highest_priority(records: List[Dict[str, Any]]) -> str:
    if not records:
        return 'P2'
    return min((str(r.get('priority') or 'P2').upper() for r in records), key=_priority_rank)


def _root_cause_for(component: str, subsystem: str, records: List[Dict[str, Any]]) -> str:
    text = ' '.join(_norm(r.get('event_type')) + ' ' + _norm(r.get('technical_reason')) + ' ' + _norm(r.get('suspected_root_cause')) for r in records)
    if 'action' in text or 'navigation' in text or 'workspace_action_map' in text or 'назад' in text:
        return 'Вероятная причина: несогласованность Command Routing, workspace_action_map и сохранённого Runtime State.'
    if 'journal' in component or 'development_journal' in component or 'журнал' in text:
        return 'Вероятная причина: Development Journal route или lifecycle/export contract не полностью изолирован от Runtime.'
    if 'workspace_markdown' in text or 'render' in text:
        return 'Вероятная причина: Renderer допускает возврат технического payload без полноценного пользовательского workspace_markdown.'
    if 'context' in text or 'state' in text:
        return 'Вероятная причина: active_workspace_state или snapshot восстанавливается неполно и даёт дрейф контекста.'
    return 'Вероятная причина требует инженерной проверки владельца подсистемы; Laboratory не нашла более точный общий паттерн.'


def _fix_direction(component: str, subsystem: str, records: List[Dict[str, Any]]) -> str:
    if 'runtime' in subsystem or 'navigation' in subsystem or 'command' in subsystem:
        return 'Унифицировать маршрутизацию команд, источник action_map и snapshot restore; добавить регрессию полного пользовательского маршрута.'
    if 'journal' in subsystem or 'journal' in component:
        return 'Изолировать Journal routes от Workspace Runtime, проверить capture/show/export/lifecycle на любом активном Workspace.'
    if 'render' in subsystem:
        return 'Ввести guard: каждый Workspace возвращает workspace_markdown либо явную диагностическую ошибку.'
    return 'Исправить владельца подсистемы и закрепить сценарий в TEST PLAN Engine.'


def analyze_development_journal() -> Dict[str, Any]:
    records = list_open_engineering_tasks(include_test=False)
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = defaultdict(list)
    for record in records:
        grouped[_component_key(record)].append(record)

    findings: List[Dict[str, Any]] = []
    for (component, subsystem), items in grouped.items():
        items_sorted = sorted(items, key=lambda r: _priority_rank(r.get('priority')))
        findings.append({
            'component': component,
            'subsystem': subsystem,
            'records_count': len(items_sorted),
            'record_ids': [r.get('id') for r in items_sorted],
            'priority': _highest_priority(items_sorted),
            'root_cause': _root_cause_for(component, subsystem, items_sorted),
            'fix_direction': _fix_direction(component, subsystem, items_sorted),
            'scenarios': sorted({str(r.get('scenario') or '').strip() for r in items_sorted if r.get('scenario')}),
        })
    findings.sort(key=lambda f: (_priority_rank(f.get('priority')), -int(f.get('records_count') or 0), f.get('component') or ''))

    return {
        'status': 'ok',
        'records_analyzed': len(records),
        'groups_count': len(findings),
        'findings': findings,
        'engineering_task': build_engineering_task(findings),
    }


def build_engineering_task(findings: List[Dict[str, Any]]) -> Dict[str, Any]:
    tasks: List[Dict[str, Any]] = []
    for index, finding in enumerate(findings, start=1):
        tasks.append({
            'id': f'LAB-TASK-{index:03d}',
            'priority': finding.get('priority'),
            'component': finding.get('component'),
            'subsystem': finding.get('subsystem'),
            'title': f"Stabilize {finding.get('component')} / {finding.get('subsystem')}",
            'description': finding.get('root_cause'),
            'fix_direction': finding.get('fix_direction'),
            'acceptance_criteria': [
                'All linked Development Journal records are closed or explicitly deferred.',
                'Affected scenarios pass Release Manager TEST PLAN.',
                'No new regression appears in Runtime, Command Routing or Development Journal routes.',
            ],
            'linked_records': finding.get('record_ids') or [],
        })
    return {
        'title': 'Engineering Task — Stabilization from Development Journal',
        'tasks': tasks,
        'mandatory_test_plan': 'Run TEST_PLAN_STABILIZATION_S1 after implementation and export Development Journal.',
    }


def build_laboratory_response(result: Dict[str, Any] | None = None) -> Dict[str, Any]:
    result = result or analyze_development_journal()
    lines = [
        '# Laboratory — Анализ Development Journal',
        '',
        f"Проанализировано открытых инженерных задач: **{result.get('records_analyzed')}**",
        f"Групп проблем: **{result.get('groups_count')}**",
        '',
        '## Root Cause Groups',
    ]
    if not result.get('findings'):
        lines.append('Активных Production-дефектов нет. Инженерное ТЗ не требуется.')
    for finding in result.get('findings') or []:
        lines += [
            '',
            f"### {finding.get('component')} / {finding.get('subsystem')} — {finding.get('priority')}",
            f"Записей: {finding.get('records_count')} ({', '.join(finding.get('record_ids') or [])})",
            f"Корневая причина: {finding.get('root_cause')}",
            f"Направление исправления: {finding.get('fix_direction')}",
        ]
    task = result.get('engineering_task') or {}
    lines += ['', '## Консолидированное инженерное ТЗ']
    for item in task.get('tasks') or []:
        lines += [
            '',
            f"### {item.get('id')} — {item.get('title')} — {item.get('priority')}",
            f"Компонент: {item.get('component')} / {item.get('subsystem')}",
            f"Описание: {item.get('description')}",
            f"Исправление: {item.get('fix_direction')}",
            f"Связанные записи: {', '.join(item.get('linked_records') or [])}",
            'Acceptance Criteria:',
        ]
        for ac in item.get('acceptance_criteria') or []:
            lines.append(f'- {ac}')
    lines += ['', f"Обязательный TEST PLAN: **{task.get('mandatory_test_plan')}**"]
    return {
        'status': 'ok',
        'render_mode': 'laboratory_analysis',
        'context': {'level': 'laboratory', 'object_name': 'Laboratory', 'period': None},
        'workspace_markdown': '\n'.join(lines),
        'workspace_primary_block': lines,
        'navigation_block': ['release manager — повторить тест план', 'экспорт журнала развития — проверить backlog'],
        'laboratory': result,
    }
