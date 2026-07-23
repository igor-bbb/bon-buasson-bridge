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
    assert routes._count_openapi_operations(schema) == 30
