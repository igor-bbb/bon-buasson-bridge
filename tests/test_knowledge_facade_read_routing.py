import json

import pytest
from fastapi.testclient import TestClient

from app.api import routes
from app.main import app


READ_OPERATION_CASES = [
    (
        "getVectraProfessionalKnowledge",
        {},
        "list_vectra_professional_knowledge",
        "/vectra/knowledge/professional",
    ),
    (
        "getVectraProfessionalKnowledgeOverview",
        {},
        "get_vectra_professional_knowledge_overview",
        "/vectra/knowledge/professional/overview",
    ),
    (
        "getVectraProfessionalKnowledgeById",
        {"knowledge_id": "PK-001"},
        "get_vectra_professional_knowledge",
        "/vectra/knowledge/professional/{knowledge_id}",
    ),
    (
        "verifyVectraProfessionalKnowledgeReadback",
        {"knowledge_id": "PK-001"},
        "verify_vectra_professional_knowledge_readback",
        "/vectra/knowledge/professional/{knowledge_id}/readback",
    ),
    (
        "getVectraDomainKnowledge",
        {"domain": "bon_buasson"},
        "get_vectra_domain_knowledge",
        "/vectra/domain/{domain}/knowledge",
    ),
    (
        "getVectraDomainKnowledgeOverview",
        {"domain": "bon_buasson"},
        "get_vectra_domain_knowledge_overview",
        "/vectra/domain/{domain}/knowledge/overview",
    ),
    (
        "getVectraDomainKnowledgeById",
        {"domain": "bon_buasson", "knowledge_id": "BK-001"},
        "get_vectra_domain_knowledge_by_id",
        "/vectra/domain/{domain}/knowledge/{knowledge_id}",
    ),
    (
        "verifyVectraDomainKnowledgeReadback",
        {"domain": "bon_buasson", "knowledge_id": "BK-001"},
        "verify_vectra_domain_knowledge_readback",
        "/vectra/domain/{domain}/knowledge/{knowledge_id}/readback",
    ),
]


def _response_payload(response) -> dict:
    return json.loads(response.body.decode("utf-8"))


client = TestClient(app)


@pytest.fixture(autouse=True)
def isolate_professional_pipeline(monkeypatch) -> None:
    monkeypatch.setattr(
        routes,
        "process_professional_response",
        lambda **kwargs: {
            "status": "PASS",
            "recommended_next_action": kwargs.get("next_action", ""),
            "self_governance": {},
        },
    )


@pytest.mark.parametrize(
    ("operation_type", "payload", "handler_name", "expected_endpoint"),
    READ_OPERATION_CASES,
)
def test_manifest_read_operation_routes_through_knowledge_facade(
    monkeypatch,
    operation_type,
    payload,
    handler_name,
    expected_endpoint,
) -> None:
    calls = []

    def handler(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return {"status": "ok", "verification_status": "PASS"}

    monkeypatch.setattr(routes, handler_name, handler)

    response = routes.vectra_laboratory_facade_knowledge(
        {"operation_type": operation_type, "payload": payload}
    )
    body = _response_payload(response)

    assert body["status"] == "ok"
    assert body["operation_type"] == operation_type
    assert body["internal_endpoint_called"] == expected_endpoint
    assert body["verification_status"] == "PASS"
    assert body["error"] is None
    assert len(calls) == 1


def test_unknown_knowledge_operation_remains_rejected() -> None:
    response = routes.vectra_laboratory_facade_knowledge(
        {"operation_type": "unknown_knowledge_operation"}
    )
    body = _response_payload(response)

    assert body["status"] == "error"
    assert body["verification_status"] == "FAIL"
    assert "Unsupported knowledge operation_type" in body["error"]["message"]


def test_openapi_publishes_every_manifest_read_alias() -> None:
    schema = routes._laboratory_facade_openapi_schema()
    request_schema = schema["paths"]["/vectra/laboratory/facade/knowledge"]["post"][
        "requestBody"
    ]["content"]["application/json"]["schema"]
    published_operations = set(request_schema["properties"]["operation_type"]["enum"])

    expected = {case[0] for case in READ_OPERATION_CASES}
    assert expected <= published_operations


def test_openapi_publishes_explicit_knowledge_id_contract() -> None:
    schema = routes._laboratory_facade_openapi_schema()
    request_schema = schema["paths"]["/vectra/laboratory/facade/knowledge"]["post"][
        "requestBody"
    ]["content"]["application/json"]["schema"]

    assert request_schema["properties"]["knowledge_id"]["type"] == "string"
    assert request_schema["properties"]["domain"]["type"] == "string"
    assert request_schema["properties"]["payload"]["properties"]["knowledge_id"]["type"] == "string"
    assert "object_type" not in request_schema["properties"]


def test_openapi_publishes_explicit_create_candidate_contract() -> None:
    schema = routes._laboratory_facade_openapi_schema()
    request_schema = schema["paths"]["/vectra/laboratory/facade/knowledge"]["post"][
        "requestBody"
    ]["content"]["application/json"]["schema"]
    properties = request_schema["properties"]

    assert properties["candidate_id"]["type"] == "string"
    assert properties["knowledge_id"]["type"] == "string"
    assert properties["knowledge_type"]["enum"] == ["professional", "business"]
    assert properties["title"]["type"] == "string"
    assert properties["content"]["type"] == "string"
    assert properties["source"]["type"] == "string"
    assert "Do not place this value in working_context" in properties["content"]["description"]


def test_http_action_routes_top_level_create_candidate_fields_once(
    monkeypatch,
) -> None:
    calls = []

    def handler(payload):
        calls.append(payload)
        return {
            "status": "ok",
            "candidate": {
                "candidate_id": payload["candidate_id"],
                "knowledge_id": payload["knowledge_id"],
                "knowledge_type": payload["knowledge_type"],
                "title": payload["title"],
                "content": payload["content"],
                "product_owner_approval": payload["product_owner_approval"],
            },
            "capitalization_allowed": True,
        }

    monkeypatch.setattr(routes, "create_vectra_knowledge_candidate", handler)
    candidate = {
        "candidate_id": "KC-PK-002-OPERATIONAL-CAPABILITY-READINESS-001",
        "knowledge_id": "PK-002",
        "knowledge_type": "professional",
        "title": "Критерий эксплуатационной доступности",
        "content": "Полный утверждённый текст кандидата знания.",
        "source": "Product Verification",
    }
    response = client.post(
        "/vectra/laboratory/facade/knowledge",
        json={
            "operation_type": "create_candidate",
            **candidate,
            "product_owner_approval": True,
            "working_context": "Контекст сессии не заменяет content.",
        },
    )
    body = response.json()

    assert response.status_code == 200
    assert body["status"] == "ok"
    assert body["operation_type"] == "create_candidate"
    assert len(calls) == 1
    for key, value in candidate.items():
        assert calls[0][key] == value
    assert calls[0]["product_owner_approval"] is True
    assert calls[0]["working_context"] == "Контекст сессии не заменяет content."
    assert body["result"]["candidate"]["content"] == candidate["content"]


def test_http_action_keeps_nested_create_candidate_payload_compatible(
    monkeypatch,
) -> None:
    calls = []
    monkeypatch.setattr(
        routes,
        "create_vectra_knowledge_candidate",
        lambda payload: calls.append(payload) or {"status": "ok"},
    )
    nested_payload = {
        "candidate_id": "KC-PK-LEGACY",
        "knowledge_id": "PK-LEGACY",
        "knowledge_type": "professional",
        "title": "Legacy candidate",
        "content": "Legacy nested content",
    }

    response = client.post(
        "/vectra/laboratory/facade/knowledge",
        json={
            "operation_type": "create_candidate",
            "payload": nested_payload,
            "product_owner_approval": True,
        },
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert calls == [{**nested_payload, "product_owner_approval": True}]


@pytest.mark.parametrize(
    ("operation_type", "knowledge_id", "domain", "handler_name"),
    [
        (
            "getVectraProfessionalKnowledgeById",
            "PK-001",
            None,
            "get_vectra_professional_knowledge",
        ),
        (
            "verifyVectraProfessionalKnowledgeReadback",
            "PK-001",
            None,
            "verify_vectra_professional_knowledge_readback",
        ),
        (
            "getVectraDomainKnowledgeById",
            "BK-001",
            "bon_buasson",
            "get_vectra_domain_knowledge_by_id",
        ),
        (
            "verifyVectraDomainKnowledgeReadback",
            "BK-001",
            "bon_buasson",
            "verify_vectra_domain_knowledge_readback",
        ),
    ],
)
def test_http_action_routes_top_level_knowledge_id(
    monkeypatch,
    operation_type,
    knowledge_id,
    domain,
    handler_name,
) -> None:
    calls = []

    def handler(*args, **kwargs):
        calls.append({"args": args, "kwargs": kwargs})
        return {
            "status": "ok",
            "knowledge_id": knowledge_id,
            "verification_status": "PASS",
            "readback_status": "PASS",
        }

    monkeypatch.setattr(routes, handler_name, handler)
    request = {"operation_type": operation_type, "knowledge_id": knowledge_id}
    if domain:
        request["domain"] = domain

    response = client.post("/vectra/laboratory/facade/knowledge", json=request)
    body = response.json()

    assert response.status_code == 200
    assert body["status"] == "ok"
    assert body["operation_type"] == operation_type
    assert body["verification_status"] == "PASS"
    assert len(calls) == 1
    assert calls[0]["kwargs"]["knowledge_id"] == knowledge_id
    if domain:
        assert calls[0]["kwargs"]["domain"] == domain


@pytest.mark.parametrize(
    "operation_type",
    [
        "getVectraProfessionalKnowledgeById",
        "verifyVectraProfessionalKnowledgeReadback",
        "getVectraDomainKnowledgeById",
        "verifyVectraDomainKnowledgeReadback",
    ],
)
def test_http_action_rejects_missing_knowledge_id_before_runtime_call(
    monkeypatch,
    operation_type,
) -> None:
    for handler_name in (
        "get_vectra_professional_knowledge",
        "verify_vectra_professional_knowledge_readback",
        "get_vectra_domain_knowledge_by_id",
        "verify_vectra_domain_knowledge_readback",
    ):
        monkeypatch.setattr(
            routes,
            handler_name,
            lambda *args, **kwargs: pytest.fail("Runtime handler must not be called"),
        )

    response = client.post(
        "/vectra/laboratory/facade/knowledge",
        json={"operation_type": operation_type},
    )
    body = response.json()

    assert response.status_code == 200
    assert body["status"] == "error"
    assert body["verification_status"] == "FAIL"
    assert "knowledge_id is required" in body["error"]["message"]


def test_http_action_rejects_object_type_with_controlled_error() -> None:
    response = client.post(
        "/vectra/laboratory/facade/knowledge",
        json={
            "operation_type": "getVectraProfessionalKnowledgeById",
            "object_type": "PK-001",
        },
    )
    body = response.json()

    assert response.status_code == 200
    assert body["status"] == "error"
    assert body["verification_status"] == "FAIL"
    assert "object_type is not supported" in body["error"]["message"]
    assert "knowledge_id" in body["next_recommended_action"]


def test_http_action_preserves_unknown_knowledge_id_for_runtime_readback(
    monkeypatch,
) -> None:
    calls = []

    def handler(*, knowledge_id):
        calls.append(knowledge_id)
        return {
            "status": "error",
            "knowledge_id": knowledge_id,
            "exists": False,
            "verification_status": "FAIL",
            "readback_status": "FAIL",
            "failure_reason": "knowledge_not_found",
        }

    monkeypatch.setattr(routes, "verify_vectra_professional_knowledge_readback", handler)
    response = client.post(
        "/vectra/laboratory/facade/knowledge",
        json={
            "operation_type": "verifyVectraProfessionalKnowledgeReadback",
            "knowledge_id": "PK-UNKNOWN",
        },
    )
    body = response.json()

    assert response.status_code == 200
    assert calls == ["PK-UNKNOWN"]
    assert body["verification_status"] == "FAIL"
    assert body["readback_status"] == "FAIL"


def test_openapi_action_budget_and_production_server_are_unchanged() -> None:
    schema = routes._laboratory_facade_openapi_schema()
    operation_count = sum(len(methods) for methods in schema["paths"].values())

    assert operation_count == 30
    assert schema["servers"] == [{"url": "https://bon-buasson-api.onrender.com"}]
