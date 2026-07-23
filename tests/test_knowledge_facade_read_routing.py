import json

import pytest

from app.api import routes


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


def test_openapi_action_budget_and_production_server_are_unchanged() -> None:
    schema = routes._laboratory_facade_openapi_schema()
    operation_count = sum(len(methods) for methods in schema["paths"].values())

    assert operation_count == 30
    assert schema["servers"] == [{"url": "https://bon-buasson-api.onrender.com"}]
