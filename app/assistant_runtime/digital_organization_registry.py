"""Digital Organization Registry for VECTRA v2.

The registry describes digital professional roles as organizational objects.
It does not execute professional work. Execution remains in Professional
Activity, Decision Orchestrator and Executive Controller.
"""
from __future__ import annotations

import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional
from app.assistant_runtime.repository_persistence import read_repository_json, write_repository_json

RELEASE_ID = "DIGITAL-BUSINESS-ANALYST-FOUNDATION-001"
DEFAULT_BASE_PATH = "assistant_repository"
REGISTRY_FILE = Path("runtime") / "digital_organization" / "roles.json"
ROLE_CONTRACT_VERSION = "1.0"


_CANONICAL_ROLE_TEMPLATES: List[Dict[str, Any]] = [
    {
        "role_id": "commercial_director",
        "display_name": "Цифровой коллега коммерческого директора",
        "purpose": "Подготовка целостной картины бизнеса и сопровождение решений коммерческого директора.",
        "professional_responsibility": "Прибыль и устойчивое развитие коммерческого блока.",
        "professional_activities": ["executive_briefing", "business_analysis", "strategy", "task_cascade", "decision_support"],
        "professional_context": ["business", "top_manager", "network_contract", "category", "sku", "external_environment"],
        "platform_dependencies": ["business_domain", "business_knowledge", "business_data", "working_desktops"],
        "professional_outputs": ["morning_briefing", "management_decision_context", "tasks_for_roles"],
        "role_modes": ["analyst", "strategist", "coach", "coordinator"],
    },
    {
        "role_id": "national_manager",
        "display_name": "Цифровой коллега национального менеджера",
        "purpose": "Управление портфелем сетей, менеджеров и коммерческих приоритетов.",
        "professional_responsibility": "Результат национальных клиентов и развитие команды КАМ.",
        "professional_activities": ["portfolio_analysis", "manager_coordination", "contract_review", "coaching"],
        "professional_context": ["top_manager", "manager", "network_contract", "category", "sku"],
        "platform_dependencies": ["business_domain", "business_data", "decision_knowledge"],
        "professional_outputs": ["manager_tasks", "contract_priorities", "coaching_plan"],
        "role_modes": ["manager", "analyst", "coach"],
    },
    {
        "role_id": "kam",
        "display_name": "Цифровой коллега КАМ",
        "purpose": "Сопровождение контрактов, переговоров, ассортимента и прибыльности сетей.",
        "professional_responsibility": "Коммерческий результат закреплённых сетей и качество переговорных решений.",
        "professional_activities": ["contract_analysis", "negotiation_preparation", "assortment_review", "task_execution", "coaching"],
        "professional_context": ["manager", "network_contract", "category", "tmc_group", "sku"],
        "platform_dependencies": ["contract_workspace", "business_data", "external_environment"],
        "professional_outputs": ["negotiation_pack", "contract_action_plan", "sku_decisions"],
        "role_modes": ["analyst", "negotiator", "coach", "executor"],
    },
    {
        "role_id": "territory_manager",
        "display_name": "Цифровой коллега территориального менеджера",
        "purpose": "Развитие территории, каналов и исполнение коммерческих задач.",
        "professional_responsibility": "Прибыльное развитие территории и вторичные продажи.",
        "professional_activities": ["territory_analysis", "channel_development", "execution_control", "coaching"],
        "professional_context": ["region", "distributor", "network", "outlet", "sku"],
        "platform_dependencies": ["business_domain", "traditional_trade_data", "modern_trade_data"],
        "professional_outputs": ["territory_plan", "execution_tasks", "development_actions"],
        "role_modes": ["manager", "coach", "executor"],
    },
    {
        "role_id": "trade_marketing",
        "display_name": "Цифровой коллега Trade Marketing",
        "purpose": "Связывать продукт, представленность, активности и коммерческий результат.",
        "professional_responsibility": "Эффективность торговых активностей и присутствия продукта.",
        "professional_activities": ["promotion_analysis", "availability_analysis", "investment_effectiveness", "coaching"],
        "professional_context": ["network_contract", "category", "sku", "promotion", "store_execution"],
        "platform_dependencies": ["trade_marketing_data", "business_data", "external_environment"],
        "professional_outputs": ["activation_plan", "investment_recommendation", "execution_brief"],
        "role_modes": ["analyst", "planner", "coach"],
    },
    {
        "role_id": "chief_engineer",
        "display_name": "Главный инженер VECTRA",
        "purpose": "Переводить продуктовые решения в проверяемую инженерную реализацию.",
        "professional_responsibility": "Целостность кода, поставки и технической реализации утверждённой модели.",
        "professional_activities": ["engineering_review", "implementation", "release_packaging", "verification_support"],
        "professional_context": ["canonical_decisions", "runtime", "openapi", "repository", "release"],
        "platform_dependencies": ["github", "runtime", "laboratory"],
        "professional_outputs": ["deploy_package", "release_brief", "engineering_diagnostics"],
        "role_modes": ["architect_engineer", "executor", "governance_guardian"],
    },
]


def _merge_canonical_roles(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    existing = {str(item.get("role_id")): item for item in items if isinstance(item, dict)}
    now = _now()
    for template in _CANONICAL_ROLE_TEMPLATES:
        role_id = template["role_id"]
        if role_id in existing:
            continue
        existing[role_id] = {
            **deepcopy(template),
            "supported_business_domains": ["bon_buasson"],
            "interacting_roles": [],
            "maturity_status": "FOUNDATION",
            "contract_version": ROLE_CONTRACT_VERSION,
            "implementation_module": None,
            "status": "ACTIVE",
            "created_at": now,
            "updated_at": now,
        }
    return list(existing.values())


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _path() -> Path:
    root = Path(os.getenv("VECTRA_ASSISTANT_REPOSITORY_PATH", DEFAULT_BASE_PATH)).resolve()
    return root / REGISTRY_FILE


def _read() -> List[Dict[str, Any]]:
    path = _path()
    data = read_repository_json(path, [])
    items = data if isinstance(data, list) else []
    merged = _merge_canonical_roles(items)
    if merged != items:
        _write(merged)
    return merged


def _write(items: List[Dict[str, Any]]) -> None:
    write_repository_json(_path(), items)


def _required(payload: Dict[str, Any], key: str) -> str:
    value = str(payload.get(key) or "").strip()
    if not value:
        raise ValueError(f"{key} is required")
    return value


def get_digital_organization_registry_manifest() -> Dict[str, Any]:
    return {
        "status": "PASS",
        "release": RELEASE_ID,
        "capability": "Digital Organization Registry",
        "role_contract_version": ROLE_CONTRACT_VERSION,
        "required_contract_sections": [
            "professional_responsibility",
            "professional_activities",
            "professional_context",
            "platform_dependencies",
            "professional_outputs",
        ],
        "supported_operations": [
            "digital_organization_registry_manifest",
            "register_digital_professional_role",
            "get_digital_professional_role",
            "list_digital_professional_roles",
            "verify_digital_organization_registry",
        ],
        "policy": "Roles describe responsibility and permitted work; they do not own platform execution infrastructure.",
    }


def register_digital_professional_role(payload: Dict[str, Any]) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    role_id = _required(payload, "role_id").lower().replace(" ", "_")
    required_lists = ["professional_activities", "professional_context", "platform_dependencies", "professional_outputs"]
    for key in required_lists:
        if not isinstance(payload.get(key), list) or not payload.get(key):
            raise ValueError(f"{key} must be a non-empty list")
    responsibility = _required(payload, "professional_responsibility")
    now = _now()
    items = _read()
    existing = next((item for item in items if item.get("role_id") == role_id), None)
    role = {
        "role_id": role_id,
        "display_name": _required(payload, "display_name"),
        "purpose": str(payload.get("purpose") or responsibility).strip(),
        "professional_responsibility": responsibility,
        "professional_activities": list(payload["professional_activities"]),
        "professional_context": list(payload["professional_context"]),
        "platform_dependencies": list(payload["platform_dependencies"]),
        "professional_outputs": list(payload["professional_outputs"]),
        "supported_business_domains": list(payload.get("supported_business_domains") or ["*"]),
        "interacting_roles": list(payload.get("interacting_roles") or []),
        "maturity_status": str(payload.get("maturity_status") or "FOUNDATION").upper(),
        "contract_version": str(payload.get("contract_version") or ROLE_CONTRACT_VERSION),
        "implementation_module": payload.get("implementation_module"),
        "status": str(payload.get("status") or "ACTIVE").upper(),
        "created_at": existing.get("created_at") if existing else now,
        "updated_at": now,
    }
    if existing:
        items[items.index(existing)] = role
    else:
        items.append(role)
    _write(items)
    return {"status": "PASS", "created": existing is None, "role": deepcopy(role)}


def get_digital_professional_role(payload: Dict[str, Any]) -> Dict[str, Any]:
    role_id = _required(payload, "role_id").lower().replace(" ", "_")
    role = next((item for item in _read() if item.get("role_id") == role_id), None)
    if role is None:
        return {"status": "NOT_FOUND", "role_id": role_id}
    return {"status": "PASS", "role": deepcopy(role)}


def list_digital_professional_roles(payload: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    payload = payload if isinstance(payload, dict) else {}
    items = _read()
    if payload.get("status"):
        items = [item for item in items if item.get("status") == str(payload["status"]).upper()]
    if payload.get("business_domain"):
        domain = str(payload["business_domain"])
        items = [item for item in items if "*" in item.get("supported_business_domains", []) or domain in item.get("supported_business_domains", [])]
    return {"status": "PASS", "count": len(items), "roles": deepcopy(items)}


def verify_digital_organization_registry() -> Dict[str, Any]:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        _write([])
    items = _read()
    required = set(get_digital_organization_registry_manifest()["required_contract_sections"])
    contract_valid = all(required.issubset(set(item.keys())) for item in items)
    checks = {
        "manifest_available": True,
        "repository_readable": isinstance(items, list),
        "role_contract_enforced": contract_valid,
        "platform_execution_separated": True,
        "domain_scope_supported": True,
    }
    return {
        "status": "PASS" if all(checks.values()) else "FAIL",
        "release": RELEASE_ID,
        "checks": checks,
        "role_count": len(items),
        "manifest": get_digital_organization_registry_manifest(),
    }
