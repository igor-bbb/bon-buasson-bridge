import json
from pathlib import Path

import pytest

from app.api import routes
from app.assistant_runtime.professional_knowledge_runtime import (
    build_professional_knowledge_context,
    evaluate_operational_capability_readiness,
    get_knowledge_influence_trace,
    verify_knowledge_influence,
)
from app.assistant_runtime.professional_runtime_state import build_professional_runtime_state
from app.assistant_runtime.repository import restore_professional_body_state


PK_002 = {
    "knowledge_id": "PK-002",
    "candidate_id": "KC-PK-002-OPERATIONAL-CAPABILITY-READINESS-001",
    "package_id": "KCAP-PK-002",
    "knowledge_type": "professional",
    "type": "professional",
    "title": "Критерий эксплуатационной доступности профессиональной способности цифрового коллеги",
    "content": (
        "Профессиональная способность цифрового коллеги считается эксплуатационно "
        "доступной только тогда, когда согласованы исполняемый Runtime, API, "
        "Capability Registry, Action Manifest и пользовательская маршрутизация."
    ),
    "description": (
        "Профессиональная способность цифрового коллеги считается эксплуатационно "
        "доступной только тогда, когда согласованы исполняемый Runtime, API, "
        "Capability Registry, Action Manifest и пользовательская маршрутизация."
    ),
    "status": "CAPITALIZED",
    "source": "Product Verification",
    "product_owner_approved": True,
    "professional_model_auto_update": False,
}


@pytest.fixture()
def isolated_repository(tmp_path, monkeypatch) -> Path:
    repository = tmp_path / "assistant_repository"
    knowledge_path = repository / "knowledge" / "professional_knowledge.json"
    knowledge_path.parent.mkdir(parents=True)
    knowledge_path.write_text(
        json.dumps([PK_002], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    monkeypatch.setenv("VECTRA_ASSISTANT_REPOSITORY_PATH", str(repository))
    return repository


def test_capitalized_knowledge_is_loaded_without_model_mutation(
    isolated_repository,
) -> None:
    result = build_professional_knowledge_context("PK-002")
    context = result["professional_knowledge_context"]

    assert result["status"] == "PASS"
    assert context["knowledge_ids"] == ["PK-002"]
    assert context["reasoning_input_ready"] is True
    assert context["professional_model_auto_update"] is False
    assert context["professional_model_changed"] is False


def test_shared_professional_knowledge_is_projected_into_multiple_roles(
    isolated_repository,
) -> None:
    laboratory = build_professional_knowledge_context(
        "PK-002",
        professional_role="vectra_laboratory",
    )
    engineering = build_professional_knowledge_context(
        "PK-002",
        professional_role="chief_engineer",
    )

    for result, role in (
        (laboratory, "vectra_laboratory"),
        (engineering, "chief_engineer"),
    ):
        context = result["professional_knowledge_context"]
        assert result["status"] == "PASS"
        assert context["professional_role"] == role
        assert context["role_projection_enforced"] is True
        assert context["knowledge"][0]["shared_across_roles"] is True
        assert context["knowledge"][0]["role_applicability_verified"] is True


def test_role_restricted_knowledge_is_not_applied_outside_role(
    isolated_repository,
) -> None:
    knowledge_path = isolated_repository / "knowledge" / "professional_knowledge.json"
    restricted = dict(PK_002)
    restricted["applicable_roles"] = ["digital_business_analyst"]
    knowledge_path.write_text(
        json.dumps([restricted], ensure_ascii=False, indent=2),
        encoding="utf-8",
    )

    allowed = build_professional_knowledge_context(
        "PK-002",
        professional_role="digital_business_analyst",
    )
    blocked = build_professional_knowledge_context(
        "PK-002",
        professional_role="chief_engineer",
    )

    assert allowed["status"] == "PASS"
    assert blocked["status"] == "NOT_FOUND"


def test_new_session_restoration_exposes_professional_knowledge_context(
    isolated_repository,
) -> None:
    professional_body = restore_professional_body_state()
    professional_state = build_professional_runtime_state()
    context = professional_state["professional_knowledge_context"]

    assert professional_body["professional_knowledge_count"] == 1
    assert professional_body["professional_knowledge"][0]["knowledge_id"] == "PK-002"
    assert context["knowledge_ids"] == ["PK-002"]
    assert context["reasoning_input_ready"] is True
    assert professional_state["integrity"]["professional_model_auto_update"] is False


def test_pk_002_changes_capability_readiness_and_records_influence(
    isolated_repository,
) -> None:
    result = evaluate_operational_capability_readiness(
        {
            "knowledge_id": "PK-002",
            "capability_id": "professional_knowledge_runtime_influence",
            "runtime_ready": True,
            "api_ready": True,
            "capability_registry_ready": True,
            "action_manifest_ready": False,
            "user_routing_ready": True,
            "verification_reference": "PV-KNOWLEDGE-INFLUENCE-001",
        }
    )

    assert result["status"] == "PASS"
    assert result["knowledge_applied"] is True
    assert result["professional_role"] == "vectra_laboratory"
    assert result["role_applicability_verified"] is True
    assert result["verdict"] == "NOT_OPERATIONALLY_AVAILABLE"
    assert result["failed_gates"] == ["action_manifest_ready"]
    assert result["professional_model_changed"] is False
    trace_id = result["influence_trace"]["trace_id"]

    readback = get_knowledge_influence_trace(trace_id=trace_id)
    verification = verify_knowledge_influence(trace_id, knowledge_id="PK-002")
    assert readback["status"] == "PASS"
    assert readback["events"][0]["knowledge_id"] == "PK-002"
    assert verification["status"] == "PASS"
    assert all(verification["checks"].values())


def test_all_five_gates_make_capability_operationally_available(
    isolated_repository,
) -> None:
    result = evaluate_operational_capability_readiness(
        {
            "knowledge_id": "PK-002",
            "capability_id": "professional_knowledge_runtime_influence",
            "runtime_ready": True,
            "api_ready": True,
            "capability_registry_ready": True,
            "action_manifest_ready": True,
            "user_routing_ready": True,
        }
    )

    assert result["verdict"] == "OPERATIONALLY_AVAILABLE"
    assert result["failed_gates"] == []
    assert result["verification_status"] == "PASS"


def test_uncapitalized_knowledge_cannot_influence_runtime(
    isolated_repository,
) -> None:
    result = evaluate_operational_capability_readiness(
        {
            "knowledge_id": "PK-404",
            "capability_id": "test-capability",
            "runtime_ready": True,
            "api_ready": True,
            "capability_registry_ready": True,
            "action_manifest_ready": True,
            "user_routing_ready": True,
        }
    )

    assert result["status"] == "BLOCKED"
    assert result["failure_reason"] == "capitalized_professional_knowledge_not_found"
    assert result["professional_model_changed"] is False


def test_facade_normalizes_first_class_influence_fields(monkeypatch) -> None:
    calls = []

    def handler(payload):
        calls.append(payload)
        return {
            "status": "PASS",
            "verification_status": "PASS",
            "verdict": "OPERATIONALLY_AVAILABLE",
        }

    monkeypatch.setattr(
        routes,
        "evaluate_vectra_operational_capability_readiness",
        handler,
    )
    monkeypatch.setattr(
        routes,
        "process_professional_response",
        lambda **kwargs: {
            "status": "PASS",
            "recommended_next_action": kwargs.get("next_action", ""),
            "self_governance": {},
        },
    )
    request = {
        "operation_type": "evaluate_operational_capability_readiness",
        "knowledge_id": "PK-002",
        "capability_id": "professional_knowledge_runtime_influence",
        "runtime_ready": True,
        "api_ready": True,
        "capability_registry_ready": True,
        "action_manifest_ready": True,
        "user_routing_ready": True,
        "verification_reference": "Product Verification",
    }

    response = routes.vectra_laboratory_facade_knowledge(request)
    body = json.loads(response.body.decode("utf-8"))

    assert body["status"] == "ok"
    assert body["operation_type"] == "evaluate_operational_capability_readiness"
    assert calls == [
        {
            "knowledge_id": "PK-002",
            "capability_id": "professional_knowledge_runtime_influence",
            "runtime_ready": True,
            "api_ready": True,
            "capability_registry_ready": True,
            "action_manifest_ready": True,
            "user_routing_ready": True,
            "verification_reference": "Product Verification",
        }
    ]


def test_openapi_keeps_one_facade_action_and_publishes_influence_contract() -> None:
    schema = routes._laboratory_facade_openapi_schema()
    request_schema = schema["paths"]["/vectra/laboratory/facade/knowledge"]["post"][
        "requestBody"
    ]["content"]["application/json"]["schema"]
    properties = request_schema["properties"]
    operations = set(properties["operation_type"]["enum"])

    assert {
        "get_professional_knowledge_context",
        "evaluate_operational_capability_readiness",
        "get_knowledge_influence_trace",
        "verify_knowledge_influence",
    } <= operations
    for gate in (
        "runtime_ready",
        "api_ready",
        "capability_registry_ready",
        "action_manifest_ready",
        "user_routing_ready",
    ):
        assert properties[gate]["type"] == "boolean"
    assert properties["package_id"]["type"] == "string"
    assert properties["professional_role"]["type"] == "string"
    assert routes._count_openapi_operations(schema) == 30


def test_restore_action_returns_bounded_role_aware_response(monkeypatch) -> None:
    huge_content = "К" * 10000
    knowledge = [
        {
            "knowledge_id": f"PK-{index:03d}",
            "title": f"Knowledge {index}",
            "content": huge_content,
            "status": "CAPITALIZED",
            "revision": 1,
            "content_checksum": f"checksum-{index}",
            "applicable_roles": ["*"],
            "shared_across_roles": True,
            "applied_professional_role": "vectra_laboratory",
            "role_applicability_verified": True,
        }
        for index in range(60)
    ]
    state = {
        "state_id": "VECTRA-PROFESSIONAL-RUNTIME-STATE",
        "professional_identity": {"role": "vectra_laboratory"},
        "professional_knowledge_context": {
            "context_id": "PKCTX-TEST",
            "status": "READY",
            "source_of_truth": "knowledge/professional_knowledge.json",
            "professional_role": "vectra_laboratory",
            "knowledge_count": 60,
            "knowledge_ids": [item["knowledge_id"] for item in knowledge],
            "knowledge": knowledge,
            "reasoning_input_ready": True,
            "role_projection_enforced": True,
        },
        "active_work": {
            "engineering_cycle": {
                "cycle_id": "EP-001",
                "title": "Memory continuity",
                "status": "ACTIVE",
                "large_internal_payload": huge_content,
            }
        },
        "decision_state": {
            "open_count": 50,
            "open_decisions": [{"payload": huge_content} for _ in range(50)],
        },
        "continuity_questions": {
            "where_am_i": "vectra_laboratory",
            "what_am_i_working_on": "Memory continuity",
            "what_is_next": "verify",
        },
    }
    monkeypatch.setattr(
        routes,
        "restore_vectra_professional_body_state",
        lambda: {
            "status": "PASS",
            "source_of_state": "Runtime Repository",
            "professional_identity": {"id": "VECTRA"},
            "professional_model": {"id": "MODEL"},
            "professional_knowledge": knowledge,
            "professional_knowledge_count": len(knowledge),
            "chat_memory_used_as_source": False,
        },
    )
    monkeypatch.setattr(
        routes,
        "get_vectra_business_domain_registry",
        lambda: {"business_domain_registry": {"domains": []}},
    )
    monkeypatch.setattr(
        routes,
        "restore_professional_continuity",
        lambda **_: {
            "status": "PASS",
            "recovery_type": "PROFESSIONAL_CONTINUITY_RECOVERY",
            "professional_runtime_state": state,
            "checks": {"state_readback_verified": True},
            "recommended_next_action": "verify",
            "chat_history_required": False,
            "release": "TEST",
        },
    )
    monkeypatch.setattr(
        routes,
        "get_organizational_memory_continuity_status",
        lambda: {
            "status": "PASS",
            "source_of_truth": "database",
            "durable_across_deploys": True,
            "failed_objects": [],
            "failure_reason": None,
        },
    )
    monkeypatch.setattr(
        routes,
        "process_professional_response",
        lambda **_: {
            "status": "PASS",
            "professional_context": {},
            "self_governance": {},
            "professional_runtime_state": {
                "state_id": state["state_id"],
                "status": "PASS",
            },
            "recommended_next_action": "verify",
        },
    )

    response = routes.vectra_laboratory_state_restore()
    body = json.loads(response.body.decode("utf-8"))

    assert body["response_size_status"] == "PASS"
    assert body["response_size_bytes"] <= 48000
    restored = body["result"]["professional_knowledge_context"]
    assert restored["knowledge_count"] == 60
    assert restored["knowledge_items_returned"] == 12
    assert restored["knowledge_items_truncated"] is True
    assert restored["professional_role"] == "vectra_laboratory"
    assert body["result"]["response_contract"]["full_state_preserved_in_runtime"] is True
