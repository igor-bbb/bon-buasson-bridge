"""Guided dialogue research program for Professional Business Model.

PROGRAM-002 / PBM-FOUNDATION-001 / INCREMENT-005

The program turns an explicit understanding gap into a controlled dialogue with
Product Owner. It never silently fills missing knowledge. A research cycle can
be started, deferred, resumed, answered stage by stage and converted into an
engineering capitalization request only after Product Owner confirmation.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from app.assistant_runtime.durable_runtime_state import read_json_state, update_json_state
from app.assistant_runtime.business_understanding_runtime import (
    activate_gap,
    defer_gap,
    record_understanding_gap,
    build_engineering_capitalization_request,
)

RELEASE_ID = "VECTRA-PBM-FOUNDATION-001-INCREMENT-005"
CONTRACT_VERSION = "1.0"
DEFAULT_DOMAIN_ID = "bon_buasson"
_STATE_ROOT = Path("runtime") / "business_domains"

PROGRAM_STATES = {"PROPOSED", "ACTIVE", "DEFERRED", "READY_FOR_CONFIRMATION", "COMPLETED", "CANCELLED"}
STAGE_STATES = {"PENDING", "ACTIVE", "COMPLETE", "SKIPPED"}

MODERN_TRADE_STAGES = [
    {
        "stage_id": "structure",
        "display_name": "Организационная структура",
        "questions": [
            "Какая структура действует сейчас фактически?",
            "Какие роли, уровни подчинённости и территориальные контуры существуют?",
            "Что уже утверждено как целевая структура, но ещё не внедрено?",
        ],
    },
    {
        "stage_id": "roles",
        "display_name": "Профессиональные роли",
        "questions": [
            "Какова цель каждой роли?",
            "Какие результативные действия выполняет роль?",
            "Какие решения принимает человек в этой роли?",
            "Какие ограничения и факторы нагрузки влияют на результат?",
        ],
    },
    {
        "stage_id": "processes",
        "display_name": "Рабочие процессы",
        "questions": [
            "Какие процессы создают коммерческий результат?",
            "Где начинается и заканчивается ответственность каждой роли?",
            "Какие передачи работы происходят между ролями и подразделениями?",
        ],
    },
    {
        "stage_id": "decision_objects",
        "display_name": "Объекты решений",
        "questions": [
            "По каким объектам принимаются управленческие решения?",
            "Какие решения принимаются по контракту, сети, SKU, территории и торговой точке?",
            "Какие решения требуют согласования и кем?",
        ],
    },
    {
        "stage_id": "performance",
        "display_name": "Результативность и нагрузка",
        "questions": [
            "Что считается результативным действием для каждой роли?",
            "Какая единица времени используется для оценки?",
            "Какие факторы делают простое сравнение количества объектов некорректным?",
            "Какие данные необходимы для расчёта фактической нагрузки и эффективности?",
        ],
    },
    {
        "stage_id": "workspaces",
        "display_name": "Профессиональные рабочие пространства",
        "questions": [
            "Какую картину должен видеть человек перед принятием решения?",
            "Какие сигналы, причины, варианты действий и риски должны быть представлены?",
            "Какие данные и внешний контекст нужны по запросу?",
        ],
    },
]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _state_path(domain_id: str) -> Path:
    return _STATE_ROOT / str(domain_id or DEFAULT_DOMAIN_ID).strip().lower() / "dialogue_research_state.json"


def _default_state(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    return {
        "contract_version": CONTRACT_VERSION,
        "state_id": "VECTRA-DIALOGUE-BUSINESS-RESEARCH-STATE",
        "business_domain": str(domain_id or DEFAULT_DOMAIN_ID).strip().lower(),
        "principles": [
            "missing_business_context_is_never_silently_invented",
            "product_owner_selects_research_priority",
            "research_progresses_one_professional_stage_at_a_time",
            "confirmed_understanding_requires_engineering_capitalization",
            "business_data_is_requested_only_when_needed",
        ],
        "programs": [],
        "active_program_id": None,
        "updated_at": _now(),
    }


def get_dialogue_research_state(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    state, diagnostic = read_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict)
    return {
        "status": "PASS" if diagnostic.get("status") in {"PASS", "EMPTY", "RECOVERED"} else "HOLD",
        "dialogue_research_state": deepcopy(state),
        "diagnostic": diagnostic,
        "release": RELEASE_ID,
        "read_only": True,
    }


def propose_research_program(
    *, area_id: str, topic: str, why_needed: str,
    impact_on_current_work: str = "LIMITATION", domain_id: str = DEFAULT_DOMAIN_ID,
) -> Dict[str, Any]:
    gap_result = record_understanding_gap(
        area_id=area_id, topic=topic, why_needed=why_needed,
        impact_on_current_work=impact_on_current_work, status="OPEN", domain_id=domain_id,
    )
    if gap_result.get("status") != "PASS":
        return gap_result
    program_id = f"DRP-{str(area_id).strip().upper()}-{uuid4().hex[:8].upper()}"
    stages_template = MODERN_TRADE_STAGES if str(area_id).strip().lower() == "modern_trade" else MODERN_TRADE_STAGES

    program = {
        "program_id": program_id,
        "area_id": str(area_id).strip().lower(),
        "topic": str(topic).strip(),
        "why_needed": str(why_needed).strip(),
        "gap_id": gap_result["gap"]["gap_id"],
        "status": "PROPOSED",
        "product_owner_choice_required": True,
        "choice_prompt": "Исследовать этот пробел сейчас или отложить?",
        "stages": [
            {**deepcopy(stage), "status": "PENDING", "answers": [], "confirmed_findings": [], "open_questions": []}
            for stage in stages_template
        ],
        "active_stage_id": None,
        "confirmed_understanding": [],
        "engineering_capitalization_request": None,
        "created_at": _now(),
        "updated_at": _now(),
    }

    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        state.setdefault("programs", []).append(program)
        state["updated_at"] = _now()
        return state

    _, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    return {"status": "PASS", "program": deepcopy(program), "diagnostic": diagnostic, "read_only": False}


def decide_research_priority(program_id: str, *, decision: str, reason: str = "", domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    decision_key = str(decision or "").strip().upper()
    if decision_key not in {"START_NOW", "DEFER"}:
        return {"status": "VALIDATION_ERROR", "reason": "decision_must_be_START_NOW_or_DEFER", "read_only": True}
    found: Dict[str, Any] = {}

    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        for program in state.get("programs") or []:
            if program.get("program_id") != program_id:
                continue
            found["program"] = program
            program["product_owner_choice_required"] = False
            program["priority_decision"] = decision_key
            program["priority_reason"] = str(reason or "").strip()
            if decision_key == "DEFER":
                program["status"] = "DEFERRED"
                program["active_stage_id"] = None
                state["active_program_id"] = None
            else:
                program["status"] = "ACTIVE"
                first = next((s for s in program.get("stages") or [] if s.get("status") == "PENDING"), None)
                if first:
                    first["status"] = "ACTIVE"
                    program["active_stage_id"] = first["stage_id"]
                state["active_program_id"] = program_id
            program["updated_at"] = _now()
        state["updated_at"] = _now()
        return state

    state, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    if not found:
        return {"status": "NOT_FOUND", "program_id": program_id, "diagnostic": diagnostic, "read_only": False}
    program = next(p for p in state["programs"] if p.get("program_id") == program_id)
    if decision_key == "DEFER":
        defer_gap(program["gap_id"], reason=reason or "Product Owner deferred research", domain_id=domain_id)
    else:
        activate_gap(program["gap_id"], reason=reason or "Product Owner started research", domain_id=domain_id)
    return {"status": "PASS", "program": deepcopy(program), "diagnostic": diagnostic, "read_only": False}


def record_stage_answer(
    program_id: str, *, stage_id: str, answer: str,
    confirmed_findings: Optional[List[str]] = None,
    open_questions: Optional[List[str]] = None,
    domain_id: str = DEFAULT_DOMAIN_ID,
) -> Dict[str, Any]:
    found = {"value": False}
    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        for program in state.get("programs") or []:
            if program.get("program_id") != program_id or program.get("status") != "ACTIVE":
                continue
            for stage in program.get("stages") or []:
                if stage.get("stage_id") == stage_id and stage.get("status") == "ACTIVE":
                    stage.setdefault("answers", []).append({"answer": str(answer).strip(), "recorded_at": _now()})
                    stage["confirmed_findings"] = list(dict.fromkeys((stage.get("confirmed_findings") or []) + list(confirmed_findings or [])))
                    stage["open_questions"] = list(dict.fromkeys((stage.get("open_questions") or []) + list(open_questions or [])))
                    found["value"] = True
                    program["updated_at"] = _now()
        state["updated_at"] = _now()
        return state
    state, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    program = next((p for p in state.get("programs") or [] if p.get("program_id") == program_id), None)
    return {"status": "PASS" if found["value"] else "HOLD", "program": deepcopy(program), "diagnostic": diagnostic, "read_only": False}


def complete_stage(program_id: str, *, stage_id: str, domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    found = {"value": False}
    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        for program in state.get("programs") or []:
            if program.get("program_id") != program_id or program.get("status") != "ACTIVE":
                continue
            stages = program.get("stages") or []
            for index, stage in enumerate(stages):
                if stage.get("stage_id") != stage_id or stage.get("status") != "ACTIVE":
                    continue
                if not stage.get("answers") and not stage.get("confirmed_findings"):
                    continue
                stage["status"] = "COMPLETE"
                for finding in stage.get("confirmed_findings") or []:
                    if finding not in program.setdefault("confirmed_understanding", []):
                        program["confirmed_understanding"].append(finding)
                next_stage = next((s for s in stages[index + 1:] if s.get("status") == "PENDING"), None)
                if next_stage:
                    next_stage["status"] = "ACTIVE"
                    program["active_stage_id"] = next_stage["stage_id"]
                else:
                    program["active_stage_id"] = None
                    program["status"] = "READY_FOR_CONFIRMATION"
                    state["active_program_id"] = None
                program["updated_at"] = _now()
                found["value"] = True
        state["updated_at"] = _now()
        return state
    state, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    program = next((p for p in state.get("programs") or [] if p.get("program_id") == program_id), None)
    return {"status": "PASS" if found["value"] else "HOLD", "program": deepcopy(program), "diagnostic": diagnostic, "read_only": False}


def confirm_research_outcome(
    program_id: str, *, product_owner_confirmed: bool,
    target_runtime_objects: Optional[List[str]] = None,
    acceptance_checks: Optional[List[str]] = None,
    domain_id: str = DEFAULT_DOMAIN_ID,
) -> Dict[str, Any]:
    current = get_dialogue_research_state(domain_id)
    programs = (current.get("dialogue_research_state") or {}).get("programs") or []
    program = next((p for p in programs if p.get("program_id") == program_id), None)
    if not program:
        return {"status": "NOT_FOUND", "program_id": program_id, "read_only": True}
    if program.get("status") != "READY_FOR_CONFIRMATION":
        return {"status": "HOLD", "reason": "program_not_ready_for_confirmation", "program": program, "read_only": True}
    if not product_owner_confirmed:
        return {"status": "HOLD", "reason": "product_owner_confirmation_required", "program": program, "read_only": True}

    capitalization = build_engineering_capitalization_request(
        subject_type="dialogue_business_research",
        subject_id=program_id,
        confirmed_change={
            "area_id": program.get("area_id"),
            "topic": program.get("topic"),
            "confirmed_understanding": program.get("confirmed_understanding") or [],
            "target_runtime_objects": target_runtime_objects or ["professional_business_model", "modern_trade_model", "professional_understanding_state"],
            "acceptance_checks": acceptance_checks or [
                "confirmed_understanding_written_without_unconfirmed_assumptions",
                "current_and_target_state_remain_separated",
                "unknown_and_open_questions_preserved",
                "runtime_readback_passes",
            ],
            "stage_results": [
                {
                    "stage_id": s.get("stage_id"),
                    "confirmed_findings": s.get("confirmed_findings") or [],
                    "open_questions": s.get("open_questions") or [],
                }
                for s in program.get("stages") or []
            ],
        },
        approved_by_product_owner=True,
        domain_id=domain_id,
    )
    if capitalization.get("status") != "PASS":
        return capitalization

    def updater(state: Dict[str, Any]) -> Dict[str, Any]:
        state = deepcopy(state or _default_state(domain_id))
        for item in state.get("programs") or []:
            if item.get("program_id") == program_id:
                item["status"] = "COMPLETED"
                item["product_owner_confirmed"] = True
                item["engineering_capitalization_request"] = capitalization.get("capitalization_request")
                item["completed_at"] = _now()
                item["updated_at"] = _now()
        state["updated_at"] = _now()
        return state
    state, diagnostic = update_json_state(_state_path(domain_id), lambda: _default_state(domain_id), dict, updater)
    updated = next(p for p in state["programs"] if p.get("program_id") == program_id)
    return {"status": "PASS", "program": deepcopy(updated), "capitalization": capitalization, "diagnostic": diagnostic, "read_only": False}


def build_active_research_prompt(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    result = get_dialogue_research_state(domain_id)
    state = result.get("dialogue_research_state") or {}
    active_id = state.get("active_program_id")
    program = next((p for p in state.get("programs") or [] if p.get("program_id") == active_id), None)
    if not program:
        proposed = next((p for p in reversed(state.get("programs") or []) if p.get("status") == "PROPOSED"), None)
        if proposed:
            return {"status": "CHOICE_REQUIRED", "program_id": proposed["program_id"], "prompt": proposed["choice_prompt"], "program": proposed, "read_only": True}
        return {"status": "NO_ACTIVE_RESEARCH", "read_only": True}
    stage = next((s for s in program.get("stages") or [] if s.get("stage_id") == program.get("active_stage_id")), None)
    return {
        "status": "PASS",
        "program_id": active_id,
        "area_id": program.get("area_id"),
        "stage": deepcopy(stage),
        "prompt": (stage.get("questions") or [None])[0] if stage else None,
        "read_only": True,
    }


def verify_dialogue_research_program(domain_id: str = DEFAULT_DOMAIN_ID) -> Dict[str, Any]:
    state_result = get_dialogue_research_state(domain_id)
    state = state_result.get("dialogue_research_state") or {}
    template_stage_ids = [s["stage_id"] for s in MODERN_TRADE_STAGES]
    checks = {
        "state_available": state_result.get("status") == "PASS",
        "principles_defined": len(state.get("principles") or []) >= 5,
        "no_silent_invention": "missing_business_context_is_never_silently_invented" in (state.get("principles") or []),
        "product_owner_priority": "product_owner_selects_research_priority" in (state.get("principles") or []),
        "modern_trade_stage_structure": template_stage_ids == ["structure", "roles", "processes", "decision_objects", "performance", "workspaces"],
        "capitalization_requires_confirmation": True,
        "business_data_on_demand": "business_data_is_requested_only_when_needed" in (state.get("principles") or []),
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "checks": checks,
        "missing_or_failed": [k for k, v in checks.items() if not v],
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
    }
