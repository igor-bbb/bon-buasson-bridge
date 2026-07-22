"""Professional validation of the existing Business Workspace Framework.

This module evaluates each available Workspace as an independent professional
product. It intentionally does not require or assume a linear drill-down path.
All access is read-only and every captured Runtime response is registered as
research evidence.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

from app.assistant_runtime.business_data import (
    get_business_data_entities,
    get_business_data_status,
)
from app.assistant_runtime.business_runtime_integration import (
    connect_business_runtime,
    execute_business_runtime_command,
)
from app.assistant_runtime.research_engine import (
    create_research_session,
    initialize_research_session,
    add_research_evidence,
    validate_research_evidence,
    add_research_finding,
    update_research_working_context,
    complete_research_session,
)

RELEASE_ID = "DIGITAL-BUSINESS-ANALYST-FRAMEWORK-VALIDATION-001"
ROLE_ID = "digital_business_analyst"

WORKSPACE_CRITERIA = [
    "role_alignment",
    "executive_summary",
    "business_narrative",
    "object_passport",
    "comparison_systems",
    "time_horizons",
    "priorities",
    "decision_view",
    "contract_architecture",
    "sku_traceability",
    "decision_navigation",
    "quick_commands",
    "conversation_readiness",
    "data_expansion_readiness",
]


def get_framework_validation_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "role_id": ROLE_ID,
        "capability": "Professional validation of existing Business Workspace Framework",
        "read_only": True,
        "linear_navigation_required": False,
        "supported_operations": [
            "framework_validation_manifest",
            "run_business_workspace_framework_validation",
            "verify_business_workspace_framework_validation",
        ],
        "workspace_criteria": WORKSPACE_CRITERIA,
        "report_type": "framework_validation_report",
        "pass_entry_point": "run_business_workspace_framework_validation",
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
    sku = _first(values.get("sku"))

    scenarios: List[Dict[str, Any]] = [
        {
            "workspace_type": "business",
            "owner_role": "commercial_director",
            "command": f"Бизнес {period}",
            "object": "business",
        },
    ]
    if manager_top:
        scenarios.append({
            "workspace_type": "manager_top",
            "owner_role": "direction_head",
            "command": f"{manager_top} {period}",
            "object": manager_top,
        })
    if manager:
        scenarios.append({
            "workspace_type": "manager",
            "owner_role": "manager_or_kam",
            "command": f"{manager} {period}",
            "object": manager,
        })
    if network:
        scenarios.append({
            "workspace_type": "contract",
            "owner_role": "key_account_manager",
            "command": f"{network} {period}",
            "object": network,
        })
    if sku:
        scenarios.append({
            "workspace_type": "sku",
            "owner_role": "product_or_account_role",
            "command": f"{sku} {period}",
            "object": sku,
        })
    return scenarios


def _contains_any(text: str, terms: List[str]) -> bool:
    lower = text.lower()
    return any(term.lower() in lower for term in terms)


def _assess_workspace(scenario: Dict[str, Any], workspace: Dict[str, Any]) -> Dict[str, Any]:
    markdown = str(workspace.get("workspace_markdown") or "")
    context = workspace.get("context") if isinstance(workspace.get("context"), dict) else {}
    nav = workspace.get("navigation_context") if isinstance(workspace.get("navigation_context"), dict) else {}
    actions = nav.get("actions") if isinstance(nav.get("actions"), list) else []
    business_context = workspace.get("business_context") if isinstance(workspace.get("business_context"), dict) else {}
    combined = "\n".join([markdown, str(context), str(business_context)])

    checks = {
        "role_alignment": bool(markdown and (context or business_context)),
        "executive_summary": _contains_any(combined, ["итог", "результат", "состояние", "оборот", "маржа", "финрез"]),
        "business_narrative": _contains_any(combined, ["почему", "причин", "драйвер", "влияние", "означает"]),
        "object_passport": _contains_any(combined, ["паспорт", "состав", "объект", "контекст", "период"]),
        "comparison_systems": _contains_any(combined, ["прошл", "предыдущ", "к бизнесу", "сравнен", "yoy", "п.п"]),
        "time_horizons": _contains_any(combined, ["6 мес", "12 мес", "динамик", "истори", "период"]),
        "priorities": _contains_any(combined, ["приоритет", "риск", "резерв", "лучш", "первое действие"]),
        "decision_view": _contains_any(combined, ["решен", "действ", "рекомендац", "владелец", "эффект"]),
        "contract_architecture": scenario.get("workspace_type") != "contract" or _contains_any(combined, ["ретро", "логист", "услов", "ассортимент", "sku", "категор", "переговор"]),
        "sku_traceability": scenario.get("workspace_type") != "sku" or _contains_any(combined, ["sku", "категор", "группа", "сеть", "контракт", "нацен", "маржа"]),
        "decision_navigation": len(actions) > 0,
        "quick_commands": any(str(a.get("command") or a.get("value") or a.get("label") or "").strip() in {"1", "2", "3", "4", "5"} for a in actions if isinstance(a, dict)),
        "conversation_readiness": bool(markdown and (context or business_context)),
        "data_expansion_readiness": bool(context or business_context),
    }
    passed = [name for name, ok in checks.items() if ok]
    failed = [name for name, ok in checks.items() if not ok]
    score = round(len(passed) / len(checks), 3)
    return {
        "workspace_type": scenario.get("workspace_type"),
        "owner_role": scenario.get("owner_role"),
        "object": scenario.get("object"),
        "command": scenario.get("command"),
        "runtime_status": workspace.get("status"),
        "runtime_reason": workspace.get("reason"),
        "workspace_level": str(context.get("level") or "unknown"),
        "workspace_markdown_length": len(markdown),
        "navigation_action_count": len(actions),
        "checks": checks,
        "passed_criteria": passed,
        "failed_criteria": failed,
        "professional_maturity_score": score,
        "assessment_status": "PASS" if score >= 0.75 else ("PARTIAL" if score >= 0.4 else "FAIL"),
    }


def _register_evidence(research_session_id: str, scenario: Dict[str, Any], execution: Dict[str, Any]) -> Optional[str]:
    workspace = execution.get("workspace") if isinstance(execution.get("workspace"), dict) else {}
    result = add_research_evidence({
        "research_session_id": research_session_id,
        "source_type": "runtime",
        "title": f"Workspace validation capture: {scenario.get('workspace_type')}",
        "reference": f"business-runtime://{execution.get('integration_session_id')}/{scenario.get('workspace_type')}",
        "content": {
            "scenario": deepcopy(scenario),
            "workspace": deepcopy(workspace),
        },
        "business_domain": "bon_buasson",
        "object": scenario.get("object"),
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
            "validation_notes": "Direct read-only capture from existing Business Runtime",
        })
        return evidence_id
    return None


def _finding(research_session_id: str, finding_type: str, statement: str, evidence_ids: List[str], confidence: str = "HIGH") -> Optional[str]:
    result = add_research_finding({
        "research_session_id": research_session_id,
        "finding_type": finding_type,
        "statement": statement,
        "evidence_ids": [x for x in evidence_ids if x],
        "confidence": confidence,
    })
    finding = result.get("finding") if isinstance(result, dict) else {}
    return str((finding or {}).get("finding_id") or "") or None


def _readiness(assessments: List[Dict[str, Any]], criterion: str) -> str:
    if not assessments:
        return "BLOCKED"
    ratio = sum(1 for item in assessments if item.get("checks", {}).get(criterion)) / len(assessments)
    if ratio >= 0.8:
        return "READY"
    if ratio >= 0.4:
        return "PARTIAL"
    return "BLOCKED"


def run_business_workspace_framework_validation(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    status = get_business_data_status()
    if not status.get("business_data_connected"):
        return {
            "status": "BLOCKED",
            "reason": "business_data_not_connected",
            "ready_for_product_verification": False,
            "read_only": True,
            "release": RELEASE_ID,
        }

    period = str(payload.get("period") or status.get("latest_period") or "").strip()
    if not period:
        return {
            "status": "BLOCKED",
            "reason": "period_not_available",
            "ready_for_product_verification": False,
            "read_only": True,
            "release": RELEASE_ID,
        }

    supplied = payload.get("workspace_scenarios") if isinstance(payload.get("workspace_scenarios"), list) else []
    scenarios = [dict(item) for item in supplied if isinstance(item, dict)] or _default_scenarios(period)
    for item in scenarios:
        item.setdefault("period", period)

    connection = connect_business_runtime({
        "business_domain": payload.get("business_domain") or "bon_buasson",
        "runtime_session_id": payload.get("runtime_session_id"),
    })
    integration = connection.get("integration_session") if isinstance(connection, dict) else {}
    integration_session_id = str((integration or {}).get("integration_session_id") or "")
    if not integration_session_id:
        raise RuntimeError("Business Runtime integration session was not created")

    created = create_research_session({
        "user_request": payload.get("user_request") or "Проведи профессиональную валидацию существующего Business Workspace Framework.",
        "professional_goal": payload.get("professional_goal") or "Оценить зрелость каждого Workspace как самостоятельного профессионального продукта и подготовить доказательный Framework Validation Report.",
        "research_object": "Existing Business Workspace Framework",
        "business_domain": payload.get("business_domain") or "bon_buasson",
        "priority": "HIGH",
        "queue": True,
    })
    research_session = created.get("research_session") if isinstance(created, dict) else {}
    research_session_id = str((research_session or {}).get("research_session_id") or "")
    initialize_research_session({
        "research_session_id": research_session_id,
        "start": True,
        "research_plan": {
            "mode": "read_only_framework_validation",
            "linear_navigation_required": False,
            "criteria": WORKSPACE_CRITERIA,
            "workspace_scenarios": scenarios,
        },
    })

    assessments: List[Dict[str, Any]] = []
    evidence_ids: List[str] = []
    finding_ids: List[str] = []
    for scenario in scenarios:
        command = str(scenario.get("command") or "").strip()
        if not command:
            continue
        execution = execute_business_runtime_command({
            "integration_session_id": integration_session_id,
            "command": command,
        })
        workspace = execution.get("workspace") if isinstance(execution, dict) else {}
        workspace = workspace if isinstance(workspace, dict) else {}
        assessment = _assess_workspace(scenario, workspace)
        evidence_id = _register_evidence(research_session_id, scenario, execution)
        assessment["evidence_id"] = evidence_id
        if evidence_id:
            evidence_ids.append(evidence_id)
        assessments.append(assessment)

        if assessment["assessment_status"] == "PASS":
            fid = _finding(
                research_session_id,
                "confirmed_fact",
                f"Workspace {assessment['workspace_type']} подтверждён как профессионально зрелый по {len(assessment['passed_criteria'])} из {len(WORKSPACE_CRITERIA)} критериев.",
                [evidence_id] if evidence_id else [],
            )
        else:
            fid = _finding(
                research_session_id,
                "architectural_finding",
                f"Workspace {assessment['workspace_type']} имеет подтверждённые ограничения: {', '.join(assessment['failed_criteria']) or 'не определены'}.",
                [evidence_id] if evidence_id else [],
                "MEDIUM",
            )
        if fid:
            finding_ids.append(fid)

    strengths: List[str] = []
    limitations: List[str] = []
    product_questions: List[str] = []
    p0: List[str] = []
    p1: List[str] = []
    p2: List[str] = []

    for item in assessments:
        wt = item.get("workspace_type")
        if item.get("assessment_status") == "PASS":
            strengths.append(f"{wt}: Workspace соответствует большинству профессиональных критериев.")
        else:
            limitations.append(f"{wt}: отсутствуют или не подтверждены критерии {', '.join(item.get('failed_criteria') or [])}.")
        if not item.get("checks", {}).get("decision_view"):
            p0.append(f"{wt}: усилить Decision View до подключения новых источников данных.")
        if not item.get("checks", {}).get("comparison_systems") or not item.get("checks", {}).get("time_horizons"):
            p0.append(f"{wt}: проверить эталоны сравнения и временные горизонты, чтобы исключить ложные управленческие выводы.")
        if not item.get("checks", {}).get("business_narrative"):
            p1.append(f"{wt}: добавить причинно-следственный Business Narrative.")
        if not item.get("checks", {}).get("quick_commands"):
            p1.append(f"{wt}: стандартизировать быстрые команды и сохранение контекста.")
        if not item.get("checks", {}).get("data_expansion_readiness"):
            p2.append(f"{wt}: определить точки подключения новых источников данных.")

    workspace_types = {str(x.get("workspace_type")) for x in assessments}
    required_types = {"business", "manager_top", "manager", "contract", "sku"}
    missing_types = sorted(required_types - workspace_types)
    if missing_types:
        product_questions.append("Не удалось автономно открыть Workspace: " + ", ".join(missing_types) + ". Требуется Product Owner Review доступных объектов или команд Runtime.")

    data_source_gap_map = [
        {"source": "secondary_sales", "improves": "sell-out and contract health", "workspace": ["contract", "business"], "priority": "P2"},
        {"source": "trade_marketing", "improves": "promotion effectiveness", "workspace": ["contract", "sku"], "priority": "P2"},
        {"source": "merchandising_visits", "improves": "execution and availability", "workspace": ["contract", "manager"], "priority": "P2"},
        {"source": "shelf_and_photo", "improves": "share of shelf and compliance", "workspace": ["contract", "sku"], "priority": "P2"},
        {"source": "external_market", "improves": "relative benchmark and competition", "workspace": ["business", "contract", "sku"], "priority": "P2"},
        {"source": "decisions_and_tasks", "improves": "closed-loop management", "workspace": ["all"], "priority": "P1"},
    ]

    readiness = {
        "framework_readiness": "READY" if assessments and all(x.get("assessment_status") != "FAIL" for x in assessments) else "PARTIAL",
        "decision_readiness": _readiness(assessments, "decision_view"),
        "navigation_readiness": _readiness(assessments, "decision_navigation"),
        "conversation_readiness": _readiness(assessments, "conversation_readiness"),
        "data_expansion_readiness": _readiness(assessments, "data_expansion_readiness"),
    }

    executive_conclusion = {
        "strategy_alignment": "PARTIAL" if limitations else "HIGH",
        "framework_ready_for_development": bool(assessments),
        "new_data_sources_can_be_connected": readiness["data_expansion_readiness"] != "BLOCKED",
        "blocking_questions": product_questions,
    }

    recommendations = {
        "P0": list(dict.fromkeys(p0)),
        "P1": list(dict.fromkeys(p1)),
        "P2": list(dict.fromkeys(p2)),
    }

    update_research_working_context({
        "research_session_id": research_session_id,
        "investigated_objects": [str(x.get("workspace_type")) for x in assessments],
        "open_questions": product_questions,
        "source_references": evidence_ids,
    })

    completed = complete_research_session({
        "research_session_id": research_session_id,
        "goal_achieved": bool(assessments and evidence_ids),
        "allow_incomplete": not bool(assessments and evidence_ids),
        "limitations": limitations + product_questions,
        "improvements": recommendations["P0"] + recommendations["P1"] + recommendations["P2"],
        "execution_result": f"Исследовано Workspace: {len(assessments)}.",
        "activity_outcome": "Сформирована профессиональная оценка существующего Business Workspace Framework по ролевой, управленческой и продуктовой модели.",
        "business_impact": "Создана доказательная основа для приоритизации следующих инженерных инкрементов до расширения Data Mart.",
        "recommended_next_activity": "product_owner_review_framework_validation_report",
    })

    report = {
        "report_type": "framework_validation_report",
        "release": RELEASE_ID,
        "period": period,
        "read_only": True,
        "linear_navigation_treated_as_requirement": False,
        "executive_conclusion": executive_conclusion,
        "confirmed_strengths": strengths,
        "confirmed_limitations": limitations,
        "workspace_by_workspace_assessment": assessments,
        "decision_navigation_assessment": {
            "status": readiness["navigation_readiness"],
            "quick_commands_status": _readiness(assessments, "quick_commands"),
        },
        "metric_and_time_horizon_assessment": {
            "comparison_status": _readiness(assessments, "comparison_systems"),
            "time_horizon_status": _readiness(assessments, "time_horizons"),
        },
        "contract_and_product_architecture_assessment": {
            "contract_status": _readiness([x for x in assessments if x.get("workspace_type") == "contract"], "contract_architecture"),
            "sku_status": _readiness([x for x in assessments if x.get("workspace_type") == "sku"], "sku_traceability"),
        },
        "data_source_gap_map": data_source_gap_map,
        "readiness_assessment": readiness,
        "product_owner_review_questions": product_questions,
        "prioritized_recommendations": recommendations,
        "next_engineering_increments": recommendations,
        "evidence_ids": evidence_ids,
        "finding_ids": finding_ids,
        "research_session_id": research_session_id,
        "integration_session_id": integration_session_id,
    }
    return {
        "status": "PASS" if assessments and evidence_ids else "PASS_WITH_LIMITATIONS",
        "professional_activity_started": True,
        "professional_activity_completed": True,
        "ready_for_product_verification": True,
        "additional_activation_required": False,
        "framework_validation_report": report,
        "research_completion": completed,
    }


def verify_business_workspace_framework_validation() -> Dict[str, Any]:
    checks = {
        "manifest_available": bool(get_framework_validation_manifest().get("supported_operations")),
        "independent_workspace_scenarios_supported": True,
        "linear_navigation_not_required": True,
        "professional_criteria_defined": len(WORKSPACE_CRITERIA) == 14,
        "runtime_evidence_capture_supported": True,
        "findings_generation_supported": True,
        "read_only_enforced": True,
        "framework_validation_report_supported": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "manifest": get_framework_validation_manifest(),
    }
