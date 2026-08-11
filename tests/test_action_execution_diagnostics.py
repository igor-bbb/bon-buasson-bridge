from app.assistant_runtime.action_execution_diagnostics import classify_action_error, execution_evidence


def test_error_classes_are_distinct():
    assert classify_action_error("Could not parse API call kwargs as JSON") == "Request serialization error"
    assert classify_action_error("upstream timed out") == "Upstream timeout"
    assert classify_action_error("unsupported operation") == "Action routing failure"
    assert classify_action_error("CSV source unavailable", runtime_service="business_data.query") == "Business Data failure"
    assert classify_action_error("connection refused") == "Runtime unavailable"


def test_execution_id_proves_request_reached_runtime():
    evidence = execution_evidence(operation_type="status", status="ok", runtime_service="runtime.status")
    assert evidence["action_execution_id"].startswith("AX-")
    assert evidence["request_reached_runtime"] is True
    assert evidence["error_class"] is None

