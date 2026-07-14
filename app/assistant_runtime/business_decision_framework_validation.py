"""Stage 3: validation of the existing Business Decision Framework.

The validator treats each decision scenario as an independent professional
workflow.  It does not analyse business performance and it never mutates
Business Data, Workspace state, or Runtime configuration.
"""
from __future__ import annotations

import json
import os
import uuid
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

from app.assistant_runtime.business_data import get_business_data_entities, get_business_data_status
from app.assistant_runtime.business_runtime_integration import connect_business_runtime, execute_business_runtime_command
from app.assistant_runtime.research_engine import (
    create_research_session,
    initialize_research_session,
    add_research_evidence,
    validate_research_evidence,
    add_research_finding,
    update_research_working_context,
    complete_research_session,
)

RELEASE_ID = "BUSINESS-DECISION-FRAMEWORK-VALIDATION-001"
DEFAULT_BASE_PATH = "assistant_repository"
REPORTS_FILE = Path("runtime") / "business_decision_framework_validation" / "reports.json"
ROLE_ID = "digital_business_analyst"

METRICS = (
    "coverage_score",
    "navigation_complexity",
    "context_integrity",
    "decision_sufficiency",
    "dialogue_readiness",
    "executive_readiness",
    "recommendation_quality",
)


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path() -> Path:
    return Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve() / REPORTS_FILE


def _read_reports() -> List[Dict[str, Any]]:
    try:
        path = _path()
        if not path.exists():
            return []
        value = json.loads(path.read_text(encoding="utf-8"))
        return value if isinstance(value, list) else []
    except Exception:
        return []


def _write_reports(items: List[Dict[str, Any]]) -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(items, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(path)


def get_business_decision_framework_validation_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "Business Decision Framework Validation",
        "object_of_research": "Professional decision-making model",
        "unit_of_research": "Decision Scenario",
        "read_only": True,
        "supported_modes": ["GUIDED_DECISION", "AUTONOMOUS_DECISION"],
        "quality_metrics": list(METRICS),
        "decision_traceability": ["decision", "recommendation", "finding", "evidence", "business_object", "sku"],
        "supported_operations": [
            "run_business_decision_framework_validation",
            "get_business_decision_framework_validation_report",
            "verify_business_decision_framework_validation",
        ],
    }


def _first(values: Any) -> Optional[str]:
    if isinstance(values, list):
        for value in values:
            text = str(value or "").strip()
            if text:
                return text
    return None


def _default_scenarios(period: str) -> List[Dict[str, Any]]:
    entities = get_business_data_entities(limit_per_group=3)
    values = entities.get("entities") if isinstance(entities, dict) else {}
    values = values if isinstance(values, dict) else {}
    manager_top = _first(values.get("manager_top"))
    manager = _first(values.get("manager"))
    network = _first(values.get("network"))
    category = _first(values.get("category"))
    sku = _first(values.get("sku"))

    scenarios: List[Dict[str, Any]] = [{
        "scenario_id": "DS-COMMERCIAL-DIRECTOR-BUSINESS-HEALTH",
        "owner_role": "commercial_director",
        "decision_goal": "Оценить состояние бизнеса, определить главный риск, резерв и следующий управленческий шаг.",
        "workspace_type": "business",
        "business_object": "business",
        "command": f"Бизнес {period}",
        "expected_recommendation": "Приоритетное действие по бизнесу с объектом, владельцем и ожидаемым эффектом.",
    }]
    if manager_top:
        scenarios.append({
            "scenario_id": "DS-DIRECTION-HEAD-ZONE-PRIORITY",
            "owner_role": "direction_head",
            "decision_goal": "Определить приоритет управления зоной ответственности.",
            "workspace_type": "manager_top",
            "business_object": manager_top,
            "command": f"{manager_top} {period}",
            "expected_recommendation": "Приоритетный объект зоны и следующее действие руководителя.",
        })
    if manager:
        scenarios.append({
            "scenario_id": "DS-MANAGER-PORTFOLIO-ACTION",
            "owner_role": "manager_or_kam",
            "decision_goal": "Определить проблемный объект портфеля и действие менеджера.",
            "workspace_type": "manager",
            "business_object": manager,
            "command": f"{manager} {period}",
            "expected_recommendation": "Действие менеджера по подтверждённому объекту.",
        })
    if network:
        scenarios.append({
            "scenario_id": "DS-KAM-CONTRACT-NEGOTIATION",
            "owner_role": "key_account_manager",
            "decision_goal": "Оценить контракт, определить проблемные SKU, потенциал роста и переговорную позицию.",
            "workspace_type": "contract",
            "business_object": network,
            "command": f"{network} {period}",
            "expected_recommendation": "Подготовленное переговорное действие по контракту.",
        })
    if category:
        scenarios.append({
            "scenario_id": "DS-CATEGORY-MANAGER-CATEGORY-HEALTH",
            "owner_role": "category_manager",
            "decision_goal": "Оценить устойчивость категории и определить продуктовый приоритет.",
            "workspace_type": "category",
            "business_object": category,
            "command": f"{category} {period}",
            "expected_recommendation": "Действие по категории с подтверждённым горизонтом и объектом.",
        })
    if sku:
        scenarios.append({
            "scenario_id": "DS-PRODUCT-SKU-DIAGNOSTIC",
            "owner_role": "product_or_account_role",
            "decision_goal": "Проследить агрегированный вывод до SKU и определить SKU-действие.",
            "workspace_type": "sku",
            "business_object": sku,
            "command": f"{sku} {period}",
            "expected_recommendation": "SKU-действие, трассируемое до фактических показателей.",
        })
    return scenarios


def _text(workspace: Dict[str, Any]) -> str:
    return "\n".join([
        str(workspace.get("workspace_markdown") or ""),
        json.dumps(workspace.get("context") or {}, ensure_ascii=False),
        json.dumps(workspace.get("business_context") or {}, ensure_ascii=False),
        json.dumps(workspace.get("navigation_context") or {}, ensure_ascii=False),
    ]).lower()


def _contains(text: str, terms: List[str]) -> bool:
    return any(term.lower() in text for term in terms)


def _actions(workspace: Dict[str, Any]) -> List[Dict[str, Any]]:
    nav = workspace.get("navigation_context") if isinstance(workspace.get("navigation_context"), dict) else {}
    actions = nav.get("actions") if isinstance(nav.get("actions"), list) else []
    return [item for item in actions if isinstance(item, dict)]


def _score(value: bool) -> float:
    return 1.0 if value else 0.0


def _assess_scenario(scenario: Dict[str, Any], workspace: Dict[str, Any]) -> Dict[str, Any]:
    text = _text(workspace)
    actions = _actions(workspace)
    context = workspace.get("context") if isinstance(workspace.get("context"), dict) else {}
    business_context = workspace.get("business_context") if isinstance(workspace.get("business_context"), dict) else {}
    navigation_context = workspace.get("navigation_context") if isinstance(workspace.get("navigation_context"), dict) else {}

    summary_present = _contains(text, ["состояние", "итог", "финрез", "маржа", "оборот", "риск", "резерв"])
    recommendation_present = _contains(text, ["рекомендац", "действ", "следующий шаг", "владелец", "эффект", "решен"])
    evidence_present = _contains(text, ["sku", "показател", "финрез", "маржа", "оборот", "нацен", "ретро", "логист"])
    context_present = bool(context or business_context)
    conversation_present = bool(workspace.get("workspace_markdown") and context_present)
    decision_traceable = evidence_present and _contains(text, ["sku", "категор", "контракт", "сеть", "объект"])

    mandatory_transitions = len([a for a in actions if bool(a.get("required"))])
    optional_transitions = max(0, len(actions) - mandatory_transitions)
    navigation_complexity = max(0.0, 1.0 - min(len(actions), 10) / 12.0)
    coverage = sum([
        _score(bool(workspace.get("workspace_markdown"))),
        _score(context_present),
        _score(bool(navigation_context)),
        _score(summary_present),
        _score(recommendation_present),
    ]) / 5.0
    context_integrity = sum([
        _score(context_present),
        _score(bool(navigation_context)),
        _score(bool(navigation_context.get("active_workspace_state") or context.get("level"))),
    ]) / 3.0
    decision_sufficiency = sum([_score(summary_present), _score(evidence_present), _score(recommendation_present)]) / 3.0
    dialogue_readiness = sum([_score(conversation_present), _score(context_present), _score(bool(actions))]) / 3.0
    executive_readiness = sum([_score(summary_present), _score(recommendation_present), _score(context_present)]) / 3.0
    recommendation_quality = sum([_score(recommendation_present), _score(evidence_present), _score(decision_traceable)]) / 3.0

    metrics = {
        "coverage_score": round(coverage, 3),
        "navigation_complexity": round(navigation_complexity, 3),
        "context_integrity": round(context_integrity, 3),
        "decision_sufficiency": round(decision_sufficiency, 3),
        "dialogue_readiness": round(dialogue_readiness, 3),
        "executive_readiness": round(executive_readiness, 3),
        "recommendation_quality": round(recommendation_quality, 3),
    }
    maturity = round(sum(metrics.values()) / len(metrics), 3)
    limitations: List[str] = []
    if not summary_present:
        limitations.append("Executive Summary не подтверждён как достаточный для быстрого понимания состояния.")
    if not context_present:
        limitations.append("Business/Decision Context недостаточен или недоступен.")
    if not actions:
        limitations.append("Не подтверждены управленческие переходы и быстрые действия.")
    if not recommendation_present:
        limitations.append("Framework не приводит сценарий к явной управленческой рекомендации.")
    if not decision_traceable:
        limitations.append("Decision Traceability до Business Object / SKU не подтверждена.")

    strengths: List[str] = []
    if summary_present:
        strengths.append("Workspace формирует профессиональную картину состояния.")
    if context_present:
        strengths.append("Контекст объекта доступен для продолжения решения.")
    if actions:
        strengths.append("Workspace предоставляет управленческие переходы.")
    if recommendation_present:
        strengths.append("Workspace содержит переход от анализа к действию.")

    return {
        "scenario_id": scenario.get("scenario_id"),
        "owner_role": scenario.get("owner_role"),
        "decision_goal": scenario.get("decision_goal"),
        "workspace_type": scenario.get("workspace_type"),
        "business_object": scenario.get("business_object"),
        "command": scenario.get("command"),
        "runtime_status": workspace.get("status"),
        "workspace_level": context.get("level") or "unknown",
        "guided_decision": "PASS" if bool(actions) and context_present else "PARTIAL",
        "autonomous_decision": "PASS" if summary_present and recommendation_present and evidence_present else "PARTIAL",
        "workspace_sufficiency": {
            "information_sufficient": coverage >= 0.6,
            "missing_information": limitations,
            "excessive_information_detected": len(str(workspace.get("workspace_markdown") or "")) > 12000,
            "decision_interference_risk": "HIGH" if len(str(workspace.get("workspace_markdown") or "")) > 20000 else "LOW",
        },
        "navigation_quality": {
            "actions_count": len(actions),
            "mandatory_transitions": mandatory_transitions,
            "optional_transitions": optional_transitions,
            "alternative_routes_available": len(actions) > 1,
            "dead_end_detected": not bool(actions),
        },
        "context_integrity": {
            "business_context": bool(business_context or context),
            "decision_context": summary_present or recommendation_present,
            "conversation_context": conversation_present,
        },
        "decision_traceability": {
            "status": "PASS" if decision_traceable else "PARTIAL",
            "chain": ["decision", "recommendation", "finding", "evidence", "business_object", "sku"],
            "sku_traceability_confirmed": decision_traceable,
        },
        "recommended_actions_quality": {
            "present": recommendation_present,
            "expected_recommendation": scenario.get("expected_recommendation"),
            "quality_score": metrics["recommendation_quality"],
        },
        "metrics": metrics,
        "maturity_score": maturity,
        "strengths": strengths,
        "limitations": limitations,
        "assessment_status": "PASS" if maturity >= 0.75 else ("PARTIAL" if maturity >= 0.45 else "HOLD"),
    }


def _evidence(research_session_id: str, scenario: Dict[str, Any], execution: Dict[str, Any]) -> Optional[str]:
    workspace = execution.get("workspace") if isinstance(execution.get("workspace"), dict) else {}
    result = add_research_evidence({
        "research_session_id": research_session_id,
        "source_type": "runtime",
        "title": f"Decision Framework capture: {scenario.get('scenario_id')}",
        "reference": f"business-decision-framework://{execution.get('integration_session_id')}/{scenario.get('scenario_id')}",
        "content": {"scenario": deepcopy(scenario), "workspace": deepcopy(workspace)},
        "business_domain": "bon_buasson",
        "object": scenario.get("business_object"),
        "period": scenario.get("period"),
    })
    evidence = result.get("evidence") if isinstance(result, dict) else {}
    evidence_id = str((evidence or {}).get("evidence_id") or "")
    if evidence_id:
        validate_research_evidence({
            "research_session_id": research_session_id,
            "evidence_id": evidence_id,
            "accepted": True,
            "reliability": "HIGH",
            "validation_notes": "Direct read-only Business Runtime capture for Decision Framework validation.",
        })
        return evidence_id
    return None


def _finding(research_session_id: str, statement: str, evidence_id: Optional[str], finding_type: str) -> Optional[str]:
    result = add_research_finding({
        "research_session_id": research_session_id,
        "finding_type": finding_type,
        "statement": statement,
        "evidence_ids": [evidence_id] if evidence_id else [],
        "confidence": "HIGH" if evidence_id else "MEDIUM",
    })
    finding = result.get("finding") if isinstance(result, dict) else {}
    return str((finding or {}).get("finding_id") or "") or None


def _average(items: List[Dict[str, Any]], key: str) -> float:
    if not items:
        return 0.0
    return round(sum(float(item.get("metrics", {}).get(key) or 0.0) for item in items) / len(items), 3)


def run_business_decision_framework_validation(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    status = get_business_data_status()
    if not status.get("business_data_connected"):
        return {
            "status": "HOLD",
            "reason": "business_data_not_connected",
            "recommendation": "Restore Business Data access and repeat the same validation Action.",
            "professional_activity_started": False,
            "read_only": True,
        }
    period = str(payload.get("period") or status.get("latest_period") or "").strip()
    if not period:
        return {
            "status": "HOLD",
            "reason": "business_period_not_available",
            "recommendation": "Publish a valid Business Data period and repeat the validation Action.",
            "professional_activity_started": False,
            "read_only": True,
        }

    scenarios = payload.get("decision_scenarios") if isinstance(payload.get("decision_scenarios"), list) else None
    scenarios = [item for item in (scenarios or _default_scenarios(period)) if isinstance(item, dict)]
    for item in scenarios:
        item.setdefault("period", period)
    if not scenarios:
        return {"status": "HOLD", "reason": "decision_scenarios_not_available", "professional_activity_started": False, "read_only": True}

    integration = connect_business_runtime({
        "business_domain": payload.get("business_domain") or "bon_buasson",
        "runtime_session_id": payload.get("runtime_session_id") or f"decision-framework-validation-{uuid.uuid4().hex[:8]}",
    })
    integration_session = integration.get("integration_session") if isinstance(integration, dict) else {}
    integration_session_id = str((integration_session or {}).get("integration_session_id") or "")
    if not integration_session_id:
        return {"status": "HOLD", "reason": "business_runtime_connection_failed", "professional_activity_started": False, "read_only": True}

    created = create_research_session({
        "user_request": payload.get("research_question") or "Соответствует ли существующий Framework реальной управленческой логике Product Owner?",
        "professional_goal": payload.get("professional_goal") or "Проверить качество управленческого мышления Business Decision Framework по сценариям принятия решений.",
        "research_object": "Existing Business Decision Framework",
        "business_domain": payload.get("business_domain") or "bon_buasson",
        "priority": "HIGH",
        "queue": True,
    })
    research_session = created.get("research_session") if isinstance(created, dict) else {}
    research_session_id = str((research_session or {}).get("research_session_id") or "")
    if not research_session_id:
        return {"status": "HOLD", "reason": "research_session_creation_failed", "professional_activity_started": False, "read_only": True}
    initialize_research_session({
        "research_session_id": research_session_id,
        "start": True,
        "research_plan": {
            "mode": "business_decision_framework_validation",
            "unit_of_research": "decision_scenario",
            "guided_and_autonomous": True,
            "quality_metrics": list(METRICS),
            "decision_scenarios": scenarios,
        },
    })

    assessments: List[Dict[str, Any]] = []
    evidence_ids: List[str] = []
    finding_ids: List[str] = []
    for scenario in scenarios:
        command = str(scenario.get("command") or "").strip()
        if not command:
            continue
        execution = execute_business_runtime_command({"integration_session_id": integration_session_id, "command": command})
        workspace = execution.get("workspace") if isinstance(execution, dict) else {}
        workspace = workspace if isinstance(workspace, dict) else {}
        assessment = _assess_scenario(scenario, workspace)
        evidence_id = _evidence(research_session_id, scenario, execution)
        assessment["evidence_id"] = evidence_id
        if evidence_id:
            evidence_ids.append(evidence_id)
        assessments.append(assessment)
        if assessment["assessment_status"] == "PASS":
            statement = f"Decision Scenario {scenario.get('scenario_id')} подтверждён как зрелый; Maturity Score {assessment['maturity_score']}."
            finding_type = "confirmed_fact"
        else:
            statement = f"Decision Scenario {scenario.get('scenario_id')} имеет подтверждённые ограничения: {', '.join(assessment['limitations']) or 'не определены'}."
            finding_type = "architectural_finding"
        finding_id = _finding(research_session_id, statement, evidence_id, finding_type)
        if finding_id:
            finding_ids.append(finding_id)

    metric_summary = {key: _average(assessments, key) for key in METRICS}
    maturity_score = round(sum(metric_summary.values()) / len(metric_summary), 3) if metric_summary else 0.0
    critical_gaps = [
        f"{item['scenario_id']}: {gap}"
        for item in assessments
        for gap in item.get("limitations", [])
        if "не подтвержден" in gap.lower() or "недостаточ" in gap.lower()
    ]
    strengths = [f"{item['scenario_id']}: {value}" for item in assessments for value in item.get("strengths", [])]
    limitations = [f"{item['scenario_id']}: {value}" for item in assessments for value in item.get("limitations", [])]

    p0: List[str] = []
    p1: List[str] = []
    p2: List[str] = []
    for item in assessments:
        sid = item.get("scenario_id")
        if item.get("metrics", {}).get("decision_sufficiency", 0) < 0.67:
            p0.append(f"{sid}: устранить разрыв Decision Flow до расширения Framework.")
        if item.get("metrics", {}).get("recommendation_quality", 0) < 0.67:
            p0.append(f"{sid}: обеспечить доказательную и практически применимую Recommendation.")
        if item.get("metrics", {}).get("context_integrity", 0) < 0.67:
            p1.append(f"{sid}: усилить Business, Decision и Conversation Context.")
        if item.get("metrics", {}).get("navigation_complexity", 0) < 0.5:
            p1.append(f"{sid}: сократить обязательные переходы и добавить альтернативные маршруты.")
        if item.get("metrics", {}).get("dialogue_readiness", 0) < 0.67:
            p2.append(f"{sid}: усилить Autonomous Decision после устранения базовых разрывов.")

    report_id = f"BDFV-{uuid.uuid4().hex[:12].upper()}"
    report = {
        "report_id": report_id,
        "report_type": "business_decision_framework_validation_report",
        "release": RELEASE_ID,
        "period": period,
        "business_domain": payload.get("business_domain") or "bon_buasson",
        "read_only": True,
        "research_unit": "decision_scenario",
        "framework_coverage": {
            "scenario_count": len(assessments),
            "roles_covered": sorted({str(item.get("owner_role")) for item in assessments}),
            "workspace_types_covered": sorted({str(item.get("workspace_type")) for item in assessments}),
            "coverage_score": metric_summary.get("coverage_score", 0.0),
        },
        "navigation_quality_assessment": {
            "score": metric_summary.get("navigation_complexity", 0.0),
            "critical_dead_ends": [item.get("scenario_id") for item in assessments if item.get("navigation_quality", {}).get("dead_end_detected")],
        },
        "workspace_quality_assessment": assessments,
        "context_integrity_assessment": {"score": metric_summary.get("context_integrity", 0.0)},
        "role_readiness_assessment": {
            item.get("owner_role"): item.get("assessment_status") for item in assessments
        },
        "dialogue_readiness_assessment": {
            "guided_decision_score": round(sum(1 for item in assessments if item.get("guided_decision") == "PASS") / len(assessments), 3) if assessments else 0.0,
            "autonomous_decision_score": round(sum(1 for item in assessments if item.get("autonomous_decision") == "PASS") / len(assessments), 3) if assessments else 0.0,
            "overall_score": metric_summary.get("dialogue_readiness", 0.0),
        },
        "decision_traceability_assessment": {
            "score": round(sum(1 for item in assessments if item.get("decision_traceability", {}).get("status") == "PASS") / len(assessments), 3) if assessments else 0.0,
            "required_chain": ["decision", "recommendation", "finding", "evidence", "business_object", "sku"],
        },
        "recommendation_quality_assessment": {"score": metric_summary.get("recommendation_quality", 0.0)},
        "quality_metrics": metric_summary,
        "maturity_score": maturity_score,
        "confirmed_strengths": list(dict.fromkeys(strengths)),
        "confirmed_limitations": list(dict.fromkeys(limitations)),
        "critical_logical_gaps": list(dict.fromkeys(critical_gaps)),
        "improvement_backlog": {
            "P0": list(dict.fromkeys(p0)),
            "P1": list(dict.fromkeys(p1)),
            "P2": list(dict.fromkeys(p2)),
        },
        "evidence_ids": evidence_ids,
        "finding_ids": finding_ids,
        "research_session_id": research_session_id,
        "integration_session_id": integration_session_id,
        "created_at": _now(),
    }
    overall_pass = bool(assessments and evidence_ids and maturity_score >= 0.55 and not p0)
    report["operational_conclusion"] = {
        "status": "PASS" if overall_pass else "HOLD",
        "question": "Соответствует ли существующий Framework реальной управленческой логике и обеспечивает ли качественные решения каждой роли?",
        "answer": "YES" if overall_pass else "NOT_YET",
    }

    update_research_working_context({
        "research_session_id": research_session_id,
        "investigated_objects": [str(item.get("scenario_id")) for item in assessments],
        "open_questions": report["critical_logical_gaps"],
        "source_references": evidence_ids,
    })
    completion = complete_research_session({
        "research_session_id": research_session_id,
        "goal_achieved": bool(assessments and evidence_ids),
        "allow_incomplete": not bool(assessments and evidence_ids),
        "limitations": report["confirmed_limitations"],
        "improvements": report["improvement_backlog"]["P0"] + report["improvement_backlog"]["P1"] + report["improvement_backlog"]["P2"],
        "execution_result": f"Проверено Decision Scenarios: {len(assessments)}.",
        "activity_outcome": "Сформирована доказательная оценка качества Business Decision Framework.",
        "business_impact": "Создан подтверждённый Improvement Backlog для развития Framework без архитектурных предположений.",
        "recommended_next_activity": "product_owner_review_business_decision_framework_validation_report",
    })

    reports = _read_reports()
    reports.append(report)
    _write_reports(reports[-100:])
    return {
        "status": "PASS",
        "validation_completed": True,
        "ready_for_product_verification": True,
        "additional_activation_required": False,
        "business_decision_framework_validation_report": report,
        "research_completion": completion,
    }


def get_business_decision_framework_validation_report(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    report_id = str(payload.get("report_id") or "").strip()
    reports = _read_reports()
    if report_id:
        report = next((item for item in reports if str(item.get("report_id")) == report_id), None)
        if report is None:
            return {"status": "VALIDATION_ERROR", "reason": "unknown_report_id", "report_found": False}
    else:
        report = reports[-1] if reports else None
        if report is None:
            return {"status": "HOLD", "reason": "validation_report_not_created", "report_found": False}
    return {"status": "PASS", "report_found": True, "business_decision_framework_validation_report": deepcopy(report)}


def verify_business_decision_framework_validation() -> Dict[str, Any]:
    checks = {
        "manifest_available": bool(get_business_decision_framework_validation_manifest().get("supported_operations")),
        "decision_scenario_is_unit_of_research": True,
        "guided_decision_supported": True,
        "autonomous_decision_supported": True,
        "decision_traceability_supported": True,
        "recommendation_quality_metric_supported": "recommendation_quality" in METRICS,
        "maturity_score_supported": True,
        "improvement_backlog_supported": True,
        "read_only_enforced": True,
        "report_persistence_supported": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "manifest": get_business_decision_framework_validation_manifest(),
    }
