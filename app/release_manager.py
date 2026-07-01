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

from app.development_journal import add_runtime_event
from pathlib import Path
import json

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

def run_release_acceptance(release_id: str = "manual-release", scenario_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Execute the autonomous technical acceptance matrix via Scenario Runner.

    Release Manager owns TEST PLAN selection and PASS/FAIL decisions. Scenario
    Runner only executes commands and returns payloads.
    """
    from app.scenario_runner import run_scenario

    selected = scenario_ids or [s.id for s in SCENARIO_LIBRARY]
    # Permanent regression scenarios are automatically included after defect closure.
    selected = sorted(set(list(selected) + get_regression_scenario_ids()))
    release_id = str(release_id or "manual-release")
    session_id = f"release-manager-{release_id}-{uuid.uuid4().hex[:8]}"
    scenario_results: List[Dict[str, Any]] = []
    journal_records: List[Dict[str, Any]] = []

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
        scenario_results.append({
            "scenario_id": scenario.id,
            "title": scenario.title,
            "status": "PASS" if scenario_ok else "FAIL",
            "steps": step_results,
            "runner": {"status": runner_result.get('status'), "steps_executed": runner_result.get('steps_executed')},
        })

    passed = sum(1 for r in scenario_results if r.get("status") == "PASS")
    failed = sum(1 for r in scenario_results if r.get("status") == "FAIL")
    return {
        "status": "ok" if failed == 0 else "failed",
        "release_id": release_id,
        "test_plan_id": TEST_PLAN["id"],
        "scenario_runner": "enabled",
        "scenarios_total": len(scenario_results),
        "scenarios_passed": passed,
        "scenarios_failed": failed,
        "journal_records_created": journal_records,
        "scenario_results": scenario_results,
        "regression_suite_ids": get_regression_scenario_ids(),
    }

def build_release_manager_response(result: Dict[str, Any]) -> Dict[str, Any]:
    lines = [
        "# Release Manager — Product Acceptance",
        "",
        f"Релиз: **{result.get('release_id')}**",
        f"TEST PLAN: **{result.get('test_plan_id')}**",
        f"Сценариев: **{result.get('scenarios_total')}**",
        f"PASS: **{result.get('scenarios_passed')}**",
        f"FAIL: **{result.get('scenarios_failed')}**",
        "",
        "## Результаты сценариев",
    ]
    for scenario in result.get("scenario_results") or []:
        lines.append(f"- **{scenario.get('scenario_id')}** — {scenario.get('status')} — {scenario.get('title')}")
    if result.get("journal_records_created"):
        lines += ["", "## Development Journal", "Подтверждённые дефекты записаны в Development Journal:"]
        for rec in result.get("journal_records_created") or []:
            lines.append(f"- {rec.get('id')}")
    else:
        lines += ["", "## Development Journal", "Новых подтверждённых дефектов не зафиксировано."]
    return {
        "status": "ok" if result.get("scenarios_failed") == 0 else "failed",
        "render_mode": "release_manager",
        "context": {"level": "release_manager", "object_name": "Release Manager", "period": None},
        "workspace_markdown": "\n".join(lines),
        "workspace_primary_block": lines,
        "navigation_block": ["анализ журнала — передать журнал в Laboratory", "экспорт журнала развития — проверить записи"],
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
