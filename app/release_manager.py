"""Autonomous Release Manager for VECTRA Stabilization S1.

This module executes the approved technical TEST PLAN against the public
query orchestration layer and records only confirmed defects in Development
Journal. It does not create product decisions and does not depend on Product
Owner manual acceptance.
"""
from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

from app.development_journal import (
    add_runtime_event,
    list_records,
    add_release_acceptance_record,
    mark_tasks_awaiting_verification,
    close_verified_tasks,
    reject_verified_tasks,
)
from pathlib import Path
import json

from app.release_brief import ReleaseBrief, parse_release_brief, scenario_ids_from_release_brief

REGRESSION_LIBRARY_FILE = Path('/tmp/vectra_scenario_library_regression.json')


@dataclass(frozen=True)
class ScenarioStep:
    message: str
    expected_render_modes: Tuple[str, ...] = ()
    expected_status: str = "ok"
    must_have_workspace_markdown: bool = True
    must_not_reason: Tuple[str, ...] = ()


@dataclass(frozen=True)
class Scenario:
    id: str
    title: str
    goal: str
    subsystem: str
    priority: str
    steps: Tuple[ScenarioStep, ...]
    depends_on: Tuple[str, ...] = field(default_factory=tuple)


SCENARIO_LIBRARY: Tuple[Scenario, ...] = (
    Scenario(
        id="S1-START-SCREEN",
        title="Start Screen local entry",
        goal="Start Screen opens locally and does not require analytical workspace.",
        subsystem="start_screen",
        priority="P0",
        steps=(ScenarioStep("Начать Анализ", expected_render_modes=("start", "start_screen",), must_have_workspace_markdown=True),),
    ),
    Scenario(
        id="S1-CONTRACT-FLOW",
        title="Contract → Negotiation → SKU Package → Task → Execution → Back",
        goal="Full management transition works without state loss.",
        subsystem="runtime_navigation",
        priority="P0",
        steps=(
            ScenarioStep("Покажи Варус 2026-02", expected_render_modes=("", "workspace", "contract_workspace"), must_have_workspace_markdown=True),
            ScenarioStep("Подготовить переговоры", expected_render_modes=("negotiation_workspace",), must_have_workspace_markdown=True),
            ScenarioStep("Собрать пакет SKU", expected_render_modes=("action_package",), must_have_workspace_markdown=True),
            ScenarioStep("Создать задачи", expected_render_modes=("task_workspace",), must_have_workspace_markdown=True),
            ScenarioStep("Перейти к исполнению", expected_render_modes=("execution_workspace",), must_have_workspace_markdown=True),
            ScenarioStep("назад", expected_status="ok", must_have_workspace_markdown=True),
        ),
    ),
    Scenario(
        id="S1-LOCAL-COMMANDS",
        title="Local commands after Contract Workspace",
        goal="назад/все/причины resolve from active workspace state.",
        subsystem="command_routing",
        priority="P0",
        steps=(
            ScenarioStep("Покажи Варус 2026-02", expected_status="ok", must_have_workspace_markdown=True),
            ScenarioStep("все", expected_render_modes=("list_only",), must_have_workspace_markdown=True),
            ScenarioStep("причины", expected_render_modes=("reasons",), must_have_workspace_markdown=True),
            ScenarioStep("назад", expected_status="ok", must_have_workspace_markdown=True),
        ),
    ),
    Scenario(
        id="S1-JOURNAL-COMMANDS",
        title="Development Journal command routing",
        goal="Journal commands bypass Workspace Runtime from any state.",
        subsystem="development_journal",
        priority="P0",
        steps=(
            ScenarioStep("Покажи Варус 2026-02", expected_status="ok", must_have_workspace_markdown=True),
            ScenarioStep("dry run journal: это баг", expected_render_modes=("development_journal_capture",), must_have_workspace_markdown=True),
            ScenarioStep("Показать журнал развития", expected_render_modes=("development_journal",), must_have_workspace_markdown=True),
            ScenarioStep("Экспорт журнала развития", expected_render_modes=("development_journal_export",), must_have_workspace_markdown=True),
        ),
    ),
)


TEST_PLAN: Dict[str, Any] = {
    "id": "TEST_PLAN_STABILIZATION_S1",
    "status": "ACTIVE",
    "scope": [
        "Command Routing",
        "Runtime Navigation",
        "Workspace Rendering",
        "Development Journal",
        "Regression",
    ],
    "scenario_ids": [s.id for s in SCENARIO_LIBRARY],
}


def _get_scenario(scenario_id: str) -> Optional[Scenario]:
    for scenario in SCENARIO_LIBRARY:
        if scenario.id == scenario_id:
            return scenario
    return None


def _render_mode(payload: Dict[str, Any]) -> str:
    return str(payload.get("render_mode") or "").strip()


def _workspace_markdown(payload: Dict[str, Any]) -> str:
    return str(payload.get("workspace_markdown") or "").strip()


def _evaluate_step(payload: Dict[str, Any], step: ScenarioStep) -> Tuple[bool, str]:
    status = str(payload.get("status") or "").strip().lower()
    if status != step.expected_status:
        return False, f"Expected status {step.expected_status}, got {status or 'empty'}."
    reason = str(payload.get("reason") or "")
    for blocked in step.must_not_reason:
        if blocked and blocked in reason:
            return False, f"Blocked reason appeared: {blocked}."
    if step.expected_render_modes:
        mode = _render_mode(payload)
        # Empty string is accepted because historical analytical workspaces often
        # rely on workspace_markdown contract rather than a named mode.
        if mode not in step.expected_render_modes:
            return False, f"Expected render_mode in {step.expected_render_modes}, got {mode or 'empty'}."
    if step.must_have_workspace_markdown and not _workspace_markdown(payload):
        return False, "Expected non-empty workspace_markdown."
    return True, "PASS"


def _record_defect(
    *,
    scenario: Scenario,
    step: ScenarioStep,
    actual: str,
    payload: Dict[str, Any],
    release_id: str,
    session_id: str,
) -> Dict[str, Any]:
    return add_runtime_event(
        event_type="release_manager_confirmed_defect",
        component=scenario.subsystem,
        system_level="release_manager",
        subsystem=scenario.subsystem,
        technical_reason=f"Release Manager scenario failed: {scenario.title} / command step.",
        suspected_root_cause="Confirmed by autonomous TEST PLAN execution; requires Laboratory root-cause analysis.",
        proposed_fix_direction="Laboratory must group this defect with related journal entries and generate engineering task.",
        priority=scenario.priority,
        runtime_context={
            "release_id": release_id,
            "scenario_id": scenario.id,
            "scenario": scenario.title,
            "command_hash_present": True,
            "render_mode": payload.get("render_mode"),
            "reason": payload.get("reason"),
        },
        active_workspace_state=payload.get("active_workspace_state") if isinstance(payload.get("active_workspace_state"), dict) else {},
        error_code=str(payload.get("reason") or "scenario_failed"),
        reproduction_data={
            "scenario": scenario.title,
            "expected_behavior": step.expected_status + (f" / {step.expected_render_modes}" if step.expected_render_modes else ""),
            "actual_behavior": actual,
            "impact": "Blocks or degrades autonomous Product Acceptance for approved Runtime scenario.",
            "severity": scenario.priority,
            "reproducibility": "confirmed_by_release_manager",
            "is_regression": True,
            "release_id": release_id,
            "scenario_id": scenario.id,
            "step_message_hash_only": True,
        },
        session_id=session_id,
    )




def _load_regression_scenarios() -> List[Dict[str, Any]]:
    if not REGRESSION_LIBRARY_FILE.exists():
        return []
    try:
        raw = json.loads(REGRESSION_LIBRARY_FILE.read_text(encoding='utf-8'))
        return [x for x in raw if isinstance(x, dict)] if isinstance(raw, list) else []
    except Exception:
        return []


def _write_regression_scenarios(items: List[Dict[str, Any]]) -> None:
    REGRESSION_LIBRARY_FILE.write_text(json.dumps(items, ensure_ascii=False, indent=2), encoding='utf-8')


def upsert_regression_scenario_from_journal_record(record: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Create/update permanent regression scenario metadata after defect closure.

    The executable scenario is linked to an approved Scenario Library id when the
    journal record was produced by Release Manager. Raw user messages are not
    persisted in the scenario file.
    """
    if not isinstance(record, dict):
        return None
    runtime_context = record.get('runtime_context') if isinstance(record.get('runtime_context'), dict) else {}
    reproduction = record.get('reproduction_data') if isinstance(record.get('reproduction_data'), dict) else {}
    scenario_id = str(runtime_context.get('scenario_id') or reproduction.get('scenario_id') or '').strip()
    if not scenario_id:
        return None
    base = _get_scenario(scenario_id)
    items = _load_regression_scenarios()
    scenario_record = {
        'id': f'REG-{scenario_id}',
        'source_journal_record_id': record.get('id'),
        'base_scenario_id': scenario_id,
        'title': f'Regression guard for {base.title if base else scenario_id}',
        'goal': 'Prevent recurrence of a confirmed and fixed Product Acceptance defect.',
        'subsystem': record.get('subsystem') or (base.subsystem if base else 'unknown'),
        'priority': record.get('priority') or (base.priority if base else 'P1'),
        'status': 'ACTIVE',
        'included_in_regression_suite': True,
        'updated_at': record.get('updated_at'),
    }
    for idx, existing in enumerate(items):
        if existing.get('id') == scenario_record['id']:
            existing.update(scenario_record)
            _write_regression_scenarios(items)
            return existing
    items.append(scenario_record)
    _write_regression_scenarios(items)
    return scenario_record


def get_regression_scenario_ids() -> List[str]:
    out: List[str] = []
    for item in _load_regression_scenarios():
        if item.get('included_in_regression_suite') and item.get('base_scenario_id'):
            out.append(str(item.get('base_scenario_id')))
    return sorted(set(out))


def get_full_scenario_library() -> Dict[str, Any]:
    return {
        'base_scenarios': [
            {
                'id': s.id,
                'title': s.title,
                'goal': s.goal,
                'subsystem': s.subsystem,
                'priority': s.priority,
                'steps': len(s.steps),
                'depends_on': list(s.depends_on),
            }
            for s in SCENARIO_LIBRARY
        ],
        'regression_scenarios': _load_regression_scenarios(),
        'regression_suite_ids': get_regression_scenario_ids(),
    }


def _is_open_engineering_task_record(record: Dict[str, Any]) -> bool:
    """Open engineering tasks only; release check records are not technical debt."""
    event_type = str(record.get("event_type") or record.get("type") or "").strip().lower()
    if event_type == "release_manager_acceptance_check":
        return False
    status = str(record.get("status") or "").strip().lower()
    if status in {"closed", "archived", "test", "dry run", "dry-run", "logged"}:
        return False
    if record.get("is_test"):
        return False
    return True


def _journal_open_snapshot() -> Dict[str, Any]:
    records = [r for r in list_records(include_test=False, include_archived=False) if _is_open_engineering_task_record(r)]
    ids = sorted(str(r.get("id")) for r in records if r.get("id"))
    return {"open_count": len(records), "open_ids": ids, "open_id_set": set(ids)}

def run_release_acceptance(release_id: str = "manual-release", scenario_ids: Optional[List[str]] = None, release_brief: Any = None) -> Dict[str, Any]:
    """Execute the autonomous technical acceptance matrix via Scenario Runner.

    Release Manager owns TEST PLAN selection and PASS/FAIL decisions. Scenario
    Runner only executes commands and returns payloads.
    """
    from app.scenario_runner import run_scenario

    available_ids = [s.id for s in SCENARIO_LIBRARY]
    brief = parse_release_brief(release_brief, fallback_release_id=release_id)
    if not release_id or release_id == "manual-release":
        release_id = brief.release_id or "manual-release"
    fixed_task_ids = list(getattr(brief, 'fixed_engineering_tasks', []) or [])
    awaiting_update = mark_tasks_awaiting_verification(
        fixed_task_ids,
        release=str(release_id),
        version=str(brief.build or ''),
        actor='Release Brief',
    ) if fixed_task_ids else {'status': 'ok', 'updated_ids': [], 'missing_ids': []}
    brief_selected = scenario_ids_from_release_brief(brief, available_ids)
    selected = scenario_ids or brief_selected or available_ids
    # Permanent regression scenarios are automatically included after defect closure.
    selected = sorted(set(list(selected) + get_regression_scenario_ids()))
    release_id = str(release_id or "manual-release")
    session_id = f"release-manager-{release_id}-{uuid.uuid4().hex[:8]}"
    journal_before = _journal_open_snapshot()
    scenario_results: List[Dict[str, Any]] = []
    journal_records: List[Dict[str, Any]] = []
    scenario_runner_invocations: List[Dict[str, Any]] = []

    for scenario_id in selected:
        scenario = _get_scenario(scenario_id)
        if not scenario:
            scenario_results.append({"scenario_id": scenario_id, "status": "SKIPPED", "reason": "unknown_scenario"})
            continue
        step_results: List[Dict[str, Any]] = []
        scenario_ok = True

        def release_manager_decision(step_result: Dict[str, Any]) -> str:
            nonlocal scenario_ok, step_results, journal_records
            idx = int(step_result.get('step') or 0)
            step = scenario.steps[idx - 1]
            payload = step_result.get('payload') if isinstance(step_result.get('payload'), dict) else {}
            ok, detail = _evaluate_step(payload, step)
            step_results.append({
                "step": idx,
                "status": "PASS" if ok else "FAIL",
                "detail": detail,
                "render_mode": payload.get("render_mode"),
                "reason": payload.get("reason"),
                "workspace_markdown_present": bool(_workspace_markdown(payload)),
                "executed_by": "ScenarioRunner",
            })
            if ok:
                return "PASS"
            scenario_ok = False
            rec = _record_defect(
                scenario=scenario,
                step=step,
                actual=detail,
                payload=payload,
                release_id=release_id,
                session_id=session_id,
            )
            journal_records.append({"id": rec.get("id"), "fingerprint": rec.get("fingerprint")})
            return "FAIL"

        runner_result = run_scenario(
            scenario=scenario,
            release_id=release_id,
            session_id=session_id,
            decision_callback=release_manager_decision,
        )
        invocation = runner_result.get("scenario_runner_invocation") if isinstance(runner_result, dict) else None
        if isinstance(invocation, dict):
            scenario_runner_invocations.append(invocation)
        scenario_results.append({
            "scenario_id": scenario.id,
            "title": scenario.title,
            "status": "PASS" if scenario_ok else "FAIL",
            "steps": step_results,
            "runner": {"status": runner_result.get('status'), "steps_executed": runner_result.get('steps_executed'), "scenario_runner_invocation": invocation},
        })

    passed = sum(1 for r in scenario_results if r.get("status") == "PASS")
    failed = sum(1 for r in scenario_results if r.get("status") == "FAIL")
    acceptance_check_record = add_release_acceptance_record(
        release_id=release_id,
        scenarios_executed=len(scenario_results),
        result="PASS" if failed == 0 else "REOPENED",
        defects_found=len(journal_records),
        session_id=session_id,
        release_brief=brief.to_dict(),
    )
    if fixed_task_ids:
        if failed == 0:
            fixed_task_verification = close_verified_tasks(
                fixed_task_ids,
                release=str(release_id),
                actor='Release Manager',
                comment='Product Acceptance passed; fixed engineering task confirmed and closed.',
            )
        else:
            fixed_task_verification = reject_verified_tasks(
                fixed_task_ids,
                release=str(release_id),
                actor='Release Manager',
                comment='Product Acceptance failed; fixed engineering task was not confirmed by Release Manager.',
            )
    else:
        fixed_task_verification = {'status': 'ok', 'closed_ids': [], 'reopened_ids': [], 'missing_ids': []}
    journal_after = _journal_open_snapshot()
    created_ids_current = sorted(
        rid for rid in journal_after.get("open_ids", [])
        if rid not in journal_before.get("open_id_set", set())
    )
    previously_open_count = max(0, int(journal_after.get("open_count") or 0) - len(created_ids_current))
    journal_status = {
        "new_records_count": len(created_ids_current),
        "new_record_ids": created_ids_current,
        "open_records_total": int(journal_after.get("open_count") or 0),
        "previously_open_count": previously_open_count,
        "created_in_current_check_count": len(created_ids_current),
    }
    return {
        "status": "ok" if failed == 0 else "failed",
        "release_id": release_id,
        "test_plan_id": TEST_PLAN["id"],
        "release_brief": brief.to_dict(),
        "fixed_engineering_tasks": fixed_task_ids,
        "awaiting_verification_update": awaiting_update,
        "fixed_task_verification": fixed_task_verification,
        "scenario_runner": "enabled",
        "scenario_runner_invocations": scenario_runner_invocations,
        "scenarios_total": len(scenario_results),
        "scenarios_passed": passed,
        "scenarios_failed": failed,
        "journal_records_created": journal_records,
        "release_acceptance_record": {"id": acceptance_check_record.get("id"), "status": acceptance_check_record.get("status")},
        "development_journal_status": journal_status,
        "scenario_results": scenario_results,
        "regression_suite_ids": get_regression_scenario_ids(),
    }

def _make_product_owner_report(result: Dict[str, Any]) -> Dict[str, Any]:
    total = int(result.get("scenarios_total") or 0)
    passed = int(result.get("scenarios_passed") or 0)
    failed = int(result.get("scenarios_failed") or 0)
    defects = result.get("journal_records_created") or []
    journal_status = result.get("development_journal_status") if isinstance(result.get("development_journal_status"), dict) else {}
    recommendation = "релиз можно использовать" if failed == 0 else "релиз требует доработки"
    criticality = "по текущему релизу проблем не обнаружено" if failed == 0 else "в текущем релизе найдены проблемы, которые нужно исправить"
    checked_titles = [str(s.get("title") or s.get("scenario_id")) for s in (result.get("scenario_results") or [])]
    return {
        "summary": "Проверка завершена.",
        "what_was_checked": checked_titles,
        "what_works": f"Успешно пройдено {passed} из {total} проверок.",
        "problems_found": "Проблем не обнаружено." if failed == 0 else f"Обнаружено проблемных сценариев: {failed}.",
        "criticality": criticality,
        "recommendation": recommendation,
        "development_journal_records": [x.get("id") for x in defects if isinstance(x, dict) and x.get("id")],
        "fixed_engineering_tasks_checked": list(result.get("fixed_engineering_tasks") or []),
        "fixed_task_verification": result.get("fixed_task_verification") or {},
        "development_journal_status": {
            "new_records_count": int(journal_status.get("new_records_count") or 0),
            "new_record_ids": list(journal_status.get("new_record_ids") or []),
            "open_records_total": int(journal_status.get("open_records_total") or 0),
            "previously_open_count": int(journal_status.get("previously_open_count") or 0),
            "created_in_current_check_count": int(journal_status.get("created_in_current_check_count") or 0),
        },
    }


def build_release_manager_response(result: Dict[str, Any]) -> Dict[str, Any]:
    """Return Product Owner friendly report plus machine-readable journal data.

    The visible workspace_markdown intentionally avoids engineering wording.
    Technical details remain in release_manager and Development Journal for Laboratory.
    """
    report = _make_product_owner_report(result)
    lines = [
        "# Проверка релиза",
        "",
        f"Релиз: **{result.get('release_id')}**",
        "",
        "## Короткий итог",
        report["summary"],
        "",
        "## Что было проверено",
    ]
    for title in report["what_was_checked"]:
        lines.append(f"- {title}")
    lines += [
        "",
        "## Что работает",
        str(report["what_works"]),
        "",
        "## Найденные проблемы",
        str(report["problems_found"]),
        "",
        "## Статус Журнала развития",
    ]
    journal_status = report.get("development_journal_status") or {}
    new_ids = journal_status.get("new_record_ids") or []
    lines += [
        f"Новых записей создано: **{journal_status.get('new_records_count', 0) or 'нет'}**",
        f"Идентификаторы новых записей: **{', '.join(new_ids) if new_ids else 'нет'}**",
        f"Всего открытых инженерных записей: **{journal_status.get('open_records_total', 0)}**",
        "",
        "Из них:",
        f"- ранее существовали: **{journal_status.get('previously_open_count', 0)}**",
        f"- созданы в ходе текущей проверки: **{journal_status.get('created_in_current_check_count', 0)}**",
        "",
    ]
    if int(journal_status.get('open_records_total') or 0) > 0 and int(journal_status.get('created_in_current_check_count') or 0) == 0:
        lines += [
            "По текущему релизу проблем не обнаружено.",
            "",
            "При этом в Журнале развития имеются открытые инженерные задачи.",
            "Для их анализа обратитесь в Laboratory.",
            "",
        ]
    elif int(journal_status.get('open_records_total') or 0) > 0:
        lines += [
            "В Журнале развития есть открытые инженерные задачи.",
            "Release Manager их не анализирует — для анализа обратитесь в Laboratory.",
            "",
        ]
    lines += [
        "## Насколько это критично",
        str(report["criticality"]),
        "",
        "## Рекомендация",
        f"**{report['recommendation']}**",
    ]
    if report.get("development_journal_records"):
        lines += [
            "",
            "Технические записи уже сохранены в Журнале развития и доступны лаборатории.",
        ]
    return {
        "status": "ok" if result.get("scenarios_failed") == 0 else "failed",
        "render_mode": "product_owner_release_report",
        "context": {"level": "release_manager", "object_name": "Release Manager", "period": None},
        "workspace_markdown": "\n".join(lines),
        "workspace_primary_block": lines,
        "navigation_block": ["анализ журнала — передать технические записи в Laboratory", "экспорт журнала развития — получить инженерный backlog"],
        "product_owner_report": report,
        "development_journal_records": result.get("journal_records_created") or [],
        "release_manager": result,
    }

def build_test_plan_response() -> Dict[str, Any]:
    lines = ["# TEST PLAN Engine", "", f"ID: **{TEST_PLAN['id']}**", "", "## Сценарии"]
    for s in SCENARIO_LIBRARY:
        lines.append(f"- **{s.id}** — {s.title} / {s.subsystem} / {s.priority}")
    regression = get_regression_scenario_ids()
    if regression:
        lines += ["", "## Regression Suite"]
        for sid in regression:
            lines.append(f"- **{sid}** — active regression guard")
    return {
        "status": "ok",
        "render_mode": "test_plan",
        "context": {"level": "test_plan", "object_name": "TEST PLAN", "period": None},
        "workspace_markdown": "\n".join(lines),
        "workspace_primary_block": lines,
        "navigation_block": ["release manager — выполнить полный TEST PLAN"],
        "test_plan": {**TEST_PLAN, "regression_suite_ids": regression},
    }
