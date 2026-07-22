"""Canonical Business Domain profile and professional business model.

The domain profile is semantic-first: it restores what the business is, how it
creates value, how it is investigated and how digital colleagues work before
Business Data are requested.
"""
from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional

RELEASE_ID = "BON-BUASSON-BUSINESS-DOMAIN-MODEL-002"
CANONICAL_DOMAIN_ID = "bon_buasson"

_BUSINESS_DOMAIN_PROFILES: Dict[str, Dict[str, Any]] = {
    CANONICAL_DOMAIN_ID: {
        "business_domain": CANONICAL_DOMAIN_ID,
        "display_name": "Бон Буассон",
        "profile_version": "1.1",
        "status": "ACTIVE",
        "business_passport": {
            "legal_scope": "Украинская производственно-коммерческая компания",
            "market": "Рынок безалкогольных напитков Украины",
            "business_type": "full_cycle_manufacturing_and_commercial_business",
            "public_fact_policy": (
                "Внешние факты о компании, брендах, производствах и рынке должны "
                "проверяться по актуальным внешним источникам и датироваться."
            ),
        },
        "business_identity": {
            "identity_statement": (
                "Бон Буассон — единая производственно-коммерческая система, создающая "
                "ценность через разработку, производство, продвижение, продажу и "
                "сопровождение собственного портфеля безалкогольных напитков."
            ),
            "main_management_object": "коммерческий результат компании",
            "value_creation": (
                "Результат согласованной работы продукта, производства, логистики, "
                "маркетинга, трейд-маркетинга, продаж, финансов и управления."
            ),
            "principles": [
                "business_is_one_interconnected_system",
                "profit_and_development_are_common_vector",
                "confirmed_data_over_assumptions",
                "causes_and_effects_over_isolated_metrics",
                "professional_knowledge_is_corporate_asset",
            ],
        },
        "business_operating_model": {
            "current_primary_research_area": "modern_trade",
            "directions": [
                "production",
                "planning",
                "procurement",
                "logistics",
                "warehousing",
                "modern_trade",
                "traditional_trade",
                "distribution",
                "trade_marketing",
                "marketing",
                "category_management",
                "finance",
                "management_accounting",
                "strategic_management",
            ],
            "management_levels": [
                "business",
                "strategic_management",
                "functional_management",
                "commercial_management",
                "channel_management",
                "category_management",
                "assortment_management",
                "sku_management",
            ],
        },
        "business_object_model": {
            "business_atom": "sku",
            "aggregation_path": [
                "sku",
                "tmc_group",
                "category",
                "contract",
                "manager",
                "top_manager",
                "business",
            ],
            "investigation_path": [
                "business",
                "top_manager",
                "manager",
                "contract",
                "category",
                "tmc_group",
                "sku",
            ],
            "meaning": (
                "Данные агрегируются от SKU вверх. Профессиональное исследование может "
                "начинаться с любого объекта и спускается к SKU для поиска первопричины."
            ),
            "passport_required_for_every_object": True,
            "contract_is_key_commercial_decision_object": True,
            "contract_branches": {
                "commercial_economics": [
                    "price", "retro_bonus", "logistics", "personnel", "service", "profitability"
                ],
                "product_development": [
                    "assortment", "category", "marketing", "trade_marketing", "availability", "sku"
                ],
            },
        },
        "working_desktop_model": {
            "definition": (
                "Профессиональный утренний брифинг и пространство принятия решений, "
                "а не статический дашборд."
            ),
            "modes": ["guided_review", "free_dialogue"],
            "must_prepare_before_display": [
                "period_state",
                "dynamics",
                "financial_effect",
                "causal_factors",
                "external_environment",
                "risks",
                "opportunities",
                "recommended_actions",
            ],
            "object_passports": {
                "business": "all_top_managers_and_total_result",
                "top_manager": "responsibility_scope_and_managers",
                "manager": "portfolio_and_contracts",
                "contract": "commercial_economics_and_product_development",
                "category": "category_role_and_contribution",
                "tmc_group": "product_group_contribution",
                "sku": "maximum_object_passport",
            },
        },
        "business_environment": {
            "required": True,
            "sources": [
                "official_company_sources",
                "customers_and_retail_networks",
                "competitors",
                "market_and_category_news",
                "regulation",
                "macroeconomics",
                "relevant_publications",
            ],
            "use": (
                "External context is combined with internal knowledge and Business Data "
                "when it can affect interpretation or a management decision."
            ),
            "fact_hypothesis_separation_required": True,
        },
        "digital_transformation": {
            "current_stage": "modern_trade_professional_model_and_digital_colleague",
            "method": [
                "external_research",
                "business_identity",
                "professional_research",
                "business_model",
                "data_connection",
                "digital_colleague",
                "decision_support",
                "knowledge_capitalization",
            ],
            "target": (
                "Создать цифровое профессиональное отражение всей компании и цифровую "
                "организацию, соответствующую реальным профессиональным ролям."
            ),
        },
        "digital_organization": {
            "one_vectra_identity": True,
            "role_specific_colleagues": True,
            "role_modes_include_coach": True,
            "top_down_task_flow": True,
            "bottom_up_learning": True,
            "shared_business_context": True,
            "common_vector": ["profit", "business_development"],
        },
        "root_business": {
            "object_id": "BUSINESS-BON-BUASSON",
            "display_name": "Бон Буассон",
            "object_type": "business",
            "lifecycle_status": "ACTIVE",
        },
        "segment_policy": {
            "classification": "business_segment",
            "public_as_business_root": False,
            "examples": ["Private Label", "Вода и напитки", "Слабоалкогольные напитки"],
        },
        "canonical_business_model": {
            "source": "assistant_repository/business_domains/bon_buasson/canonical_business_model.json",
            "evidence_classes": ["confirmed_facts", "professional_conclusions", "strategic_intentions", "unknowns"],
        },
        "modern_trade_model": {
            "source": "assistant_repository/business_domains/bon_buasson/modern_trade_model.json",
            "status": "ACTIVE_RESEARCH",
            "current_and_target_structure_separated": True,
        },
        "restoration_contract": {
            "restore_before_business_data": [
                "business_passport",
                "business_identity",
                "business_operating_model",
                "business_object_model",
                "working_desktop_model",
                "business_environment",
                "digital_transformation",
                "digital_organization",
                "canonical_business_model",
                "modern_trade_model",
            ],
            "business_data_on_demand": True,
        },
    }
}


def get_business_domain_profile(business_domain: str = CANONICAL_DOMAIN_ID) -> Optional[Dict[str, Any]]:
    profile = _BUSINESS_DOMAIN_PROFILES.get(str(business_domain or "").strip().lower())
    return deepcopy(profile) if profile else None


def get_business_domain_professional_model(business_domain: str = CANONICAL_DOMAIN_ID) -> Dict[str, Any]:
    profile = get_business_domain_profile(business_domain)
    if profile is None:
        return {"status": "NOT_FOUND", "business_domain": business_domain}
    from app.assistant_runtime.professional_business_model import get_professional_business_model_summary
    return {
        "status": "PASS",
        "business_domain": business_domain,
        "display_name": profile.get("display_name"),
        "professional_model": profile,
        "professional_business_model": get_professional_business_model_summary(business_domain),
        "restored_before_business_data": True,
        "read_only": True,
        "release": RELEASE_ID,
    }


def verify_business_domain_professional_model(business_domain: str = CANONICAL_DOMAIN_ID) -> Dict[str, Any]:
    profile = get_business_domain_profile(business_domain) or {}
    required = [
        "business_passport",
        "business_identity",
        "business_operating_model",
        "business_object_model",
        "working_desktop_model",
        "business_environment",
        "digital_transformation",
        "digital_organization",
        "root_business",
        "restoration_contract",
        "canonical_business_model",
        "modern_trade_model",
    ]
    checks = {key: isinstance(profile.get(key), dict) and bool(profile.get(key)) for key in required}
    object_model = profile.get("business_object_model") or {}
    checks["sku_is_atom"] = object_model.get("business_atom") == "sku"
    checks["aggregation_and_investigation_separated"] = bool(object_model.get("aggregation_path")) and bool(object_model.get("investigation_path"))
    checks["external_context_required"] = (profile.get("business_environment") or {}).get("required") is True
    return {
        "status": "PASS" if all(checks.values()) else "HOLD",
        "business_domain": business_domain,
        "checks": checks,
        "release": RELEASE_ID,
        "read_only": True,
    }


def get_business_root_registry() -> Dict[str, Any]:
    roots: List[Dict[str, Any]] = []
    for domain_id, profile in _BUSINESS_DOMAIN_PROFILES.items():
        root = profile.get("root_business") if isinstance(profile, dict) else None
        if not isinstance(root, dict):
            continue
        roots.append({
            "business_domain": domain_id,
            "root_business_id": root.get("object_id"),
            "display_name": root.get("display_name"),
            "lifecycle_status": root.get("lifecycle_status", "ACTIVE"),
            "supported_periods": "RUNTIME_DISCOVERED",
        })
    return {"status": "PASS", "release": RELEASE_ID, "registry_type": "business_root_registry", "roots": roots}


def validate_single_business_root(business_domain: str = CANONICAL_DOMAIN_ID) -> Dict[str, Any]:
    registry = get_business_root_registry()
    roots = [
        item for item in registry.get("roots", [])
        if item.get("business_domain") == business_domain and item.get("lifecycle_status") == "ACTIVE"
    ]
    if len(roots) != 1:
        return {
            "status": "HOLD",
            "reason": "multiple_business_roots_detected" if len(roots) > 1 else "business_root_missing",
            "business_domain": business_domain,
            "conflicting_roots": roots,
            "recommendation": "Check Business Domain Profile and Business Root Registry.",
        }
    return {"status": "PASS", "business_domain": business_domain, "root_business": deepcopy(roots[0])}
