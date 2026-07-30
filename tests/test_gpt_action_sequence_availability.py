import json

from app.api import routes


def _body(response):
    return json.loads(response.body.decode("utf-8"))


def test_dedicated_sequence_execute_action_calls_runtime(monkeypatch):
    captured = {}

    def execute(payload):
        captured.update(payload)
        return {
            "status": "PASS",
            "verification_status": "PASS",
            "sequence_id": payload["sequence_id"],
        }

    monkeypatch.setattr(routes, "execute_vectra_registered_action_sequence", execute)
    response = routes.vectra_execute_registered_action_sequence_action({
        "sequence_id": "RAS-EP001-INC002-PV-001",
        "program_type": "product_verification",
        "steps": ["get_memory_overview", "list_memory_objects"],
        "response_mode": "step_summary",
    })
    body = _body(response)

    assert captured["steps"] == ["get_memory_overview", "list_memory_objects"]
    assert body["status"] == "ok"
    assert body["operation_type"] == "execute_registered_action_sequence"
    assert body["runtime_service_called"] == "runtime_action_sequence.execute_registered_action_sequence"
    assert body["internal_endpoint_called"] == "/vectra/laboratory/runtime/action-sequences/execute"
    assert body["result"]["sequence_id"] == "RAS-EP001-INC002-PV-001"


def test_dedicated_sequence_read_action_calls_runtime(monkeypatch):
    monkeypatch.setattr(
        routes,
        "get_vectra_registered_action_sequence",
        lambda payload: {
            "status": "PASS",
            "verification_status": "PASS",
            "runtime_state_restored": True,
            "sequence_id": payload["sequence_id"],
        },
    )
    body = _body(routes.vectra_get_registered_action_sequence_action({
        "sequence_id": "RAS-EP001-INC002-PV-001",
        "response_mode": "step_summary",
    }))

    assert body["status"] == "ok"
    assert body["operation_type"] == "get_registered_action_sequence"
    assert body["runtime_service_called"] == "runtime_action_sequence.get_registered_action_sequence"
    assert body["internal_endpoint_called"] == "/vectra/laboratory/runtime/action-sequences/get"
    assert body["result"]["runtime_state_restored"] is True


def test_openapi_publishes_two_unambiguous_sequence_actions_at_limit():
    schema = routes._laboratory_facade_openapi_schema()
    paths = schema["paths"]

    execute = paths["/vectra/laboratory/runtime/action-sequences/execute"]["post"]
    execute_schema = execute["requestBody"]["content"]["application/json"]["schema"]
    read = paths["/vectra/laboratory/runtime/action-sequences/get"]["post"]
    read_schema = read["requestBody"]["content"]["application/json"]["schema"]

    assert execute["operationId"] == "executeVectraRegisteredActionSequence"
    assert execute_schema["required"] == ["sequence_id", "program_type", "steps"]
    assert "operation_type" not in execute_schema["properties"]
    assert read["operationId"] == "getVectraRegisteredActionSequence"
    assert read_schema["required"] == ["sequence_id"]
    assert "operation_type" not in read_schema["properties"]
    assert routes._count_openapi_operations(schema) == 30
    assert schema["info"]["version"] == "VECTRA-GPT-ACTION-SEQUENCE-AVAILABILITY-001"


def test_displaced_diagnostics_remain_runtime_routes_but_not_gpt_actions():
    schema = routes._laboratory_facade_openapi_schema()

    assert "/vectra/laboratory/business-research/executions/verify" not in schema["paths"]
    assert "/vectra/laboratory/business-decision-framework/verify" not in schema["paths"]
    non_actions = {item["operation_id"] for item in schema["x-vectra-non-action-diagnostics"]}
    assert {
        "verify_business_research_execution",
        "verify_business_decision_framework_validation",
    } <= non_actions
