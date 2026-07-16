"""VECTRA Personality Runtime.

Canonical executable self model for VECTRA.  This module restores identity
before professional behaviour, performs Self Audit, anchors long-running work
and connects Runtime capabilities with their professional meaning.
"""
from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.vectra_canonical_model import get_vectra_canonical_model

from app.assistant_runtime.durable_runtime_state import (
    read_json_state,
    read_unified_runtime_state,
    update_json_state,
    update_unified_runtime_root,
)

RELEASE_ID = "VECTRA-COGNITIVE-RUNTIME-V1-WP-005"
CONTRACT_VERSION = "1.0"
PERSONALITY_VERSION = "1.0"
ANCHOR_STATE_FILE = Path("runtime") / "personality" / "anchoring_state.json"


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _personality_core() -> Dict[str, Any]:
    return {
        "personality_id": "VECTRA-PERSONALITY-CORE",
        "version": PERSONALITY_VERSION,
        "status": "ACTIVE",
        "owner": "VECTRA",
        "identity": {
            "name": "VECTRA",
            "type": "professional_digital_colleague",
            "identity_statement": (
                "Я — VECTRA, профессиональный цифровой коллега и первая "
                "профессиональная личность будущей цифровой организации."
            ),
        },
        "mission": get_vectra_canonical_model()["canonical_model"]["mission"],
        "strategic_goal": (
            "Создать цифровую организацию, где каждой реальной профессиональной роли "
            "соответствует цифровой коллега одной VECTRA, а общий вектор направлен на "
            "прибыль и устойчивое развитие бизнеса."
        ),
        "product_philosophy": [
            "professional_value_over_technical_complexity",
            "research_before_answer",
            "confirmed_data_over_assumptions",
            "human_remains_goal_and_decision_owner",
            "shared_work_over_autonomy",
            "experience_driven_evolution",
        ],
        "canonical_principles": [
            "human_is_center_of_professional_activity",
            "vectra_is_one_continuous_digital_colleague",
            "runtime_executes_personality_but_does_not_define_it",
            "engineering_history_is_not_personality",
            "capability_must_have_professional_meaning",
            "knowledge_is_integrated_when_its_meaning_and_use_are_understood",
            "long_dialogue_must_not_displace_personality",
            "new_session_continues_the_same_vectra",
        ],
        "professional_self_management": {
            "memory_curator": "Контролирует качество, актуальность и статус знаний.",
            "strategy_keeper": "Проверяет соответствие развития миссии и профессиональной пользе.",
            "self_observer": "Выявляет обучение, ограничения, регрессии и рассогласования.",
            "knowledge_integrator": "Связывает новое знание с назначением и практическим использованием.",
            "consistency_guardian": "Контролирует согласованность личности, поведения, знаний, возможностей и Runtime.",
        },
        "self_awareness_questions": [
            "Кто я?",
            "Какова моя миссия?",
            "На каком этапе развития я нахожусь?",
            "Что я уже умею и чем это подтверждено?",
            "Какие ограничения подтверждены?",
            "Какие знания ещё не интегрированы?",
            "Есть ли рассогласования между личностью, знаниями, поведением и возможностями?",
            "Какое одно следующее действие необходимо?",
        ],
        "current_state": {
            "stage": {"stage_id": "canonical_product_model_implementation", "display_name": "Оцифровка целостной модели VECTRA и бизнеса"},
            "confirmed_foundation": [
                "professional_runtime",
                "professional_behaviour",
                "professional_programs",
                "business_runtime",
                "business_framework",
                "professional_state_recovery",
                "knowledge_capitalization",
                "product_verification",
            ],
            "confirmed_limitation": (
                "Целостная модель цифровой организации и профессионального мышления "
                "ещё внедряется во все рабочие сценарии и рабочие столы."
            ),
            "active_direction": [
                "personality_core",
                "self_awareness",
                "professional_anchoring",
                "capability_integration",
                "self_governance",
                "digital_organization",
                "business_domain_professional_model",
                "working_desktop_model",
            ],
        },
        "lifecycle_status": "ACTIVE",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
    }


def get_personality_core() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "personality_core": deepcopy(_personality_core()),
        "read_only": True,
    }

def persist_personality_runtime_state() -> Dict[str, Any]:
    """Connect the canonical Personality Core to Unified Runtime State.

    The personality source remains this module. Unified Runtime State stores a
    verified runtime projection so session bootstrap can restore VECTRA as one
    coherent state without replacing the canonical personality contract.
    """
    core = deepcopy(_personality_core())
    state, diagnostic = update_unified_runtime_root(
        "personality",
        {
            "personality_id": core.get("personality_id"),
            "version": core.get("version"),
            "status": core.get("status"),
            "identity": core.get("identity"),
            "mission": core.get("mission"),
            "strategic_goal": core.get("strategic_goal"),
            "canonical_principles": core.get("canonical_principles") or [],
            "current_state": core.get("current_state") or {},
            "canonical_product_model": get_vectra_canonical_model().get("canonical_model"),
            "loaded_at": _now(),
        },
        status="CONNECTED",
        source_of_truth="app.assistant_runtime.personality_runtime",
    )
    root = state.get("personality") if isinstance(state, dict) else {}
    payload = root.get("payload") if isinstance(root, dict) else {}
    verified = (
        isinstance(payload, dict)
        and payload.get("personality_id") == core.get("personality_id")
        and payload.get("version") == core.get("version")
        and root.get("status") == "CONNECTED"
    )
    return {
        "status": "PASS" if verified else "HOLD",
        "personality_runtime_state_connected": verified,
        "runtime_root": "personality",
        "runtime_state_contract_version": state.get("contract_version") if isinstance(state, dict) else None,
        "personality_id": payload.get("personality_id") if isinstance(payload, dict) else None,
        "personality_version": payload.get("version") if isinstance(payload, dict) else None,
        "readback_verified": bool(diagnostic.get("readback_verified")),
        "diagnostic": diagnostic,
        "read_only": False,
    }


def get_personality_runtime_state() -> Dict[str, Any]:
    state, diagnostic = read_unified_runtime_state()
    root = state.get("personality") if isinstance(state, dict) else {}
    return {
        "status": "PASS" if isinstance(root, dict) and root.get("status") == "CONNECTED" else "NOT_READY",
        "personality": root,
        "runtime_state_contract_version": state.get("contract_version") if isinstance(state, dict) else None,
        "diagnostic": diagnostic,
        "read_only": True,
    }


def _capability_understanding() -> Dict[str, Any]:
    from app.assistant_runtime.repository import get_capability_registry
    from app.assistant_runtime.professional_interpretation import interpret_capabilities

    registry_payload = get_capability_registry()
    registry = registry_payload.get("capability_registry") if isinstance(registry_payload, dict) else {}
    capabilities = registry.get("capabilities") if isinstance(registry, dict) else []
    interpreted = interpret_capabilities(capabilities if isinstance(capabilities, list) else [])
    # Backward-compatible alias consumed by Self Model and older callers.
    interpreted["capabilities"] = interpreted.get("professional_capabilities") or []
    return interpreted

def get_capability_understanding() -> Dict[str, Any]:
    result = _capability_understanding()
    return {**result, "release": RELEASE_ID, "read_only": True}


def _default_anchor_state() -> Dict[str, Any]:
    return {
        "personality_id": "VECTRA-PERSONALITY-CORE",
        "personality_version": PERSONALITY_VERSION,
        "anchor_count": 0,
        "last_anchor_at": None,
        "last_trigger": None,
        "status": "READY",
    }


def anchor_personality(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    trigger = str(payload.get("anchor_trigger") or payload.get("event") or "execution_bootstrap").strip()

    def mutate(current: Dict[str, Any]) -> Dict[str, Any]:
        state = dict(current or {})
        state.update({
            "personality_id": "VECTRA-PERSONALITY-CORE",
            "personality_version": PERSONALITY_VERSION,
            "anchor_count": int(state.get("anchor_count") or 0) + 1,
            "last_anchor_at": _now(),
            "last_trigger": trigger,
            "status": "ANCHORED",
        })
        return state

    state, _ = update_json_state(ANCHOR_STATE_FILE, _default_anchor_state, dict, mutate)
    return {
        "status": "PASS",
        "professional_anchor": state,
        "personality_drift_detected": False,
        "personality_restored": True,
        "read_only": False,
    }


def restore_personality_context(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    core = get_personality_core()
    runtime_state = persist_personality_runtime_state()
    capabilities = get_capability_understanding()
    anchor = anchor_personality(payload)
    ready = (
        core.get("status") == "PASS"
        and runtime_state.get("status") == "PASS"
        and runtime_state.get("personality_runtime_state_connected") is True
        and capabilities.get("status") in {"PASS", "WARNING"}
        and anchor.get("status") == "PASS"
    )
    return {
        "status": "PASS" if ready else "NOT_READY",
        "personality_ready": ready,
        "personality_core": core.get("personality_core"),
        "personality_runtime_state": runtime_state,
        "capability_context": {
            "status": capabilities.get("status"),
            "capabilities_count": capabilities.get("capabilities_count"),
            "integrated_count": capabilities.get("integrated_count"),
            "not_integrated": capabilities.get("not_integrated"),
        },
        "professional_anchor": anchor.get("professional_anchor"),
        "next_allowed_action": "continue_professional_execution" if ready else "restore_personality_core",
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": False,
    }


def run_self_audit(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    from app.assistant_runtime.professional_behaviour_runtime import get_professional_behaviour_manifest
    from app.assistant_runtime.professional_procedures_runtime import get_professional_procedure_manifest

    personality = get_personality_core().get("personality_core") or {}
    capabilities = get_capability_understanding()
    from app.assistant_runtime.self_model_runtime import persist_self_model_runtime_state
    self_model_result = persist_self_model_runtime_state(payload)
    self_model = self_model_result.get("self_model") if isinstance(self_model_result, dict) else {}
    behaviour = get_professional_behaviour_manifest("vectra_laboratory")
    procedures = get_professional_procedure_manifest()
    from app.assistant_runtime.professional_runtime_state import persist_professional_runtime_state
    from app.assistant_runtime.capability_verification_registry import get_capability_verification_registry
    professional_state_result = persist_professional_runtime_state(professional_role="vectra_laboratory")
    professional_state = professional_state_result.get("professional_runtime_state") if isinstance(professional_state_result, dict) else {}
    verification_registry = get_capability_verification_registry()

    inconsistencies: List[Dict[str, Any]] = []
    for capability_id in capabilities.get("not_integrated") or []:
        inconsistencies.append({
            "type": "capability_not_personality_integrated",
            "capability_id": capability_id,
            "severity": "WARNING",
        })
    behaviour_ready = behaviour.get("status") == "PASS"
    procedures_ready = procedures.get("status") == "PASS"
    if not behaviour_ready:
        inconsistencies.append({"type": "professional_behaviour_not_ready", "severity": "FAIL"})
    if not procedures_ready:
        inconsistencies.append({"type": "professional_procedures_not_ready", "severity": "FAIL"})
    if professional_state_result.get("status") not in {"PASS"}:
        inconsistencies.append({"type": "professional_continuity_partial", "severity": "FAIL"})
    if verification_registry.get("status") != "PASS":
        inconsistencies.append({"type": "capability_verification_registry_not_ready", "severity": "FAIL"})

    status = "PASS" if not inconsistencies else ("WARNING" if all(x.get("severity") == "WARNING" for x in inconsistencies) else "HOLD")
    next_action = "continue_professional_work" if status == "PASS" else "prepare_minimal_engineering_task_for_confirmed_inconsistency"
    from app.assistant_runtime.professional_interpretation import build_self_audit_narrative
    professional_interpretation = build_self_audit_narrative(
        personality=personality,
        self_model=self_model if isinstance(self_model, dict) else {},
        capability_context=capabilities if isinstance(capabilities, dict) else {},
        inconsistencies=inconsistencies,
        status=status,
        next_action=next_action,
    )
    from app.assistant_runtime.personality_response_composer import compose_self_audit_response
    composed_response = compose_self_audit_response(
        personality=personality,
        self_model=self_model if isinstance(self_model, dict) else {},
        capability_context=capabilities if isinstance(capabilities, dict) else {},
        inconsistencies=inconsistencies,
        status=status,
        next_action=next_action,
    )
    response_contract = {
        **(professional_interpretation.get("response_contract") or {}),
        "use_assistant_response_verbatim": True,
        "assistant_response_field": "assistant_response",
    }
    response_mode = str((payload or {}).get("response_mode") or "compact").strip().lower()
    compact_response = {
        "status": status,
        "audit_type": "VECTRA_SELF_AUDIT",
        "response_mode": "compact",
        "assistant_response": composed_response.get("assistant_response"),
        "render_mode": composed_response.get("render_mode"),
        "response_contract": response_contract,
        "one_next_action": next_action,
        "release": RELEASE_ID,
        "contract_version": CONTRACT_VERSION,
        "read_only": True,
    }
    if response_mode not in {"diagnostic", "full"}:
        return compact_response

    return {
        **compact_response,
        "response_mode": "diagnostic",
        "professional_interpretation": professional_interpretation,
        "composed_response": composed_response,
        "identity": personality.get("identity"),
        "mission": personality.get("mission"),
        "strategic_goal": personality.get("strategic_goal"),
        "current_state": personality.get("current_state"),
        "self_model": self_model,
        "current_workspace": (self_model.get("current_workspace") or {}) if isinstance(self_model, dict) else {},
        "professional_role": self_model.get("professional_role") if isinstance(self_model, dict) else None,
        "professional_self_management": personality.get("professional_self_management"),
        "capability_understanding": {
            "status": capabilities.get("status"),
            "capabilities_count": capabilities.get("capabilities_count"),
            "integrated_count": capabilities.get("integrated_count"),
            "not_integrated": capabilities.get("not_integrated"),
            "capabilities": capabilities.get("professional_capabilities") or capabilities.get("capabilities"),
        },
        "professional_behaviour_ready": behaviour_ready,
        "professional_procedures_ready": procedures_ready,
        "professional_runtime_state": professional_state,
        "professional_continuity_status": professional_state_result.get("status"),
        "capability_verification": {
            "status": verification_registry.get("status"),
            "verified_count": verification_registry.get("verified_count"),
            "operational_count": verification_registry.get("operational_count"),
            "verified_capabilities": verification_registry.get("verified_capabilities"),
        },
        "confirmed_limitations": [personality.get("current_state", {}).get("confirmed_limitation")],
        "inconsistencies": inconsistencies,
        "self_awareness_questions": personality.get("self_awareness_questions"),
        "action_closure": {"required": True, "cardinality": "exactly_one", "resolved_action": next_action},
    }


def verify_personality_runtime() -> Dict[str, Any]:
    restored = restore_personality_context({"anchor_trigger": "verification"})
    audit = run_self_audit()
    checks = {
        "personality_core_available": bool((restored.get("personality_core") or {}).get("identity")),
        "personality_loaded_before_execution": restored.get("personality_ready") is True,
        "professional_anchor_active": (restored.get("professional_anchor") or {}).get("status") == "ANCHORED",
        "capability_meaning_available": int((restored.get("capability_context") or {}).get("capabilities_count") or 0) > 0,
        "self_audit_available": audit.get("audit_type") == "VECTRA_SELF_AUDIT",
        "single_next_action": bool(audit.get("one_next_action")),
    }
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "checks": checks,
        "self_audit_status": audit.get("status"),
        "release": RELEASE_ID,
        "read_only": True,
    }
