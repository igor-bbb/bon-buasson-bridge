from __future__ import annotations

from app.api import routes


def test_action_sequence_is_published_through_memory_facade_without_extra_actions():
    schema = routes._laboratory_facade_openapi_schema()
    memory = schema["paths"]["/vectra/laboratory/facade/memory"]["post"]
    request_schema = memory["requestBody"]["content"]["application/json"]["schema"]
    operations = request_schema["properties"]["operation_type"]["enum"]

    assert "execute_registered_action_sequence" in operations
    assert "get_registered_action_sequence" in operations
    assert routes._count_openapi_operations(schema) == 29
    assert schema["x-vectra-gpt-actions-operation-limit"] == {
        "limit": 30,
        "operation_count": 29,
        "safe_operation_count": 29,
        "headroom": 1,
        "status": "PASS",
    }


def test_action_sequence_does_not_consume_dedicated_public_routes():
    schema = routes._laboratory_facade_openapi_schema()
    paths = schema["paths"]
    operation_ids = {
        operation["operationId"]
        for methods in paths.values()
        for operation in methods.values()
    }

    assert "/vectra/laboratory/runtime/action-sequences/execute" not in paths
    assert "/vectra/laboratory/runtime/action-sequences/read" not in paths
    assert "executeVectraRegisteredActionSequence" not in operation_ids
    assert "getVectraRegisteredActionSequence" not in operation_ids


def test_action_sequence_contract_publishes_required_payload_fields():
    schema = routes._laboratory_facade_openapi_schema()
    memory = schema["paths"]["/vectra/laboratory/facade/memory"]["post"]
    request_schema = memory["requestBody"]["content"]["application/json"]["schema"]
    payload = request_schema["properties"]["payload"]["properties"]

    assert payload["sequence_id"]["type"] == "string"
    assert payload["program_type"]["type"] == "string"
    assert payload["steps"]["type"] == "array"
    assert set(payload["response_mode"]["enum"]) == {"compact", "step_summary", "diagnostic"}
