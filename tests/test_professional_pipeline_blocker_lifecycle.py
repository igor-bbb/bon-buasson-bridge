from app.assistant_runtime.professional_pipeline import process_professional_response


def _call(operation_type: str, status: str):
    return process_professional_response(
        operation_type=operation_type,
        runtime_service=f"runtime_action_sequence.{operation_type}",
        endpoint=f"/vectra/laboratory/runtime/action-sequences/{operation_type}",
        result={"status": status, "verification_status": status},
    )


def test_success_reverification_closes_only_matching_pipeline_blocker(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    first_failure = _call("execute_registered_action_sequence", "FAIL")
    second_failure = _call("get_registered_action_sequence", "FAIL")
    assert first_failure["self_governance"]["attention"]["open_blockers"] == 1
    assert second_failure["self_governance"]["attention"]["open_blockers"] == 2

    execute_pass = _call("execute_registered_action_sequence", "PASS")
    assert execute_pass["status"] == "HOLD"
    assert execute_pass["blocker_reconciliation"]["verified_blockers_count"] == 1
    assert execute_pass["self_governance"]["attention"]["open_blockers"] == 1
    assert execute_pass["self_governance"]["decision"] == "STOP_FOR_OPEN_ENGINEERING_BLOCKERS"

    get_pass = _call("get_registered_action_sequence", "PASS")
    assert get_pass["status"] == "PASS"
    assert get_pass["blocker_reconciliation"]["verified_blockers_count"] == 1
    assert get_pass["self_governance"]["attention"]["open_blockers"] == 0
    assert get_pass["self_governance"]["attention"]["stop_recommended"] is False
    assert get_pass["engineering_observation"] is None


def test_repeated_success_is_idempotent(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    _call("get_registered_action_sequence", "FAIL")
    first_pass = _call("get_registered_action_sequence", "PASS")
    second_pass = _call("get_registered_action_sequence", "PASS")

    assert first_pass["blocker_reconciliation"]["verified_blockers_count"] == 1
    assert second_pass["blocker_reconciliation"]["verified_blockers_count"] == 0
    assert second_pass["self_governance"]["attention"]["open_blockers"] == 0


def test_expected_negative_normative_trace_does_not_create_blocker(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = process_professional_response(
        operation_type="trace_normative_usage",
        runtime_service="architecture_registry_runtime.trace_normative_usage",
        endpoint="/vectra/laboratory/facade/memory",
        result={
            "status": "FAIL",
            "failure_reason": "normative_section_not_found",
            "section_found_in_canonical_content": False,
            "usage_confirmed": False,
            "read_only": True,
        },
    )

    assert result["status"] == "PASS"
    assert result["professional_context"]["result_status"] == "FAIL"
    assert result["professional_context"]["outcome_classification"] == "EXPECTED_NEGATIVE"
    assert result["self_governance"]["expected_negative_outcome"] is True
    assert result["self_governance"]["confirmed_blocker"] is False
    assert result["self_governance"]["decision"] == "CONTINUE_AFTER_EXPECTED_NEGATIVE_OUTCOME"
    assert result["engineering_observation"] is None
    assert result["self_governance"]["attention"]["open_blockers"] == 0


def test_unregistered_fail_reason_remains_blocking(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = process_professional_response(
        operation_type="trace_normative_usage",
        runtime_service="architecture_registry_runtime.trace_normative_usage",
        endpoint="/vectra/laboratory/facade/memory",
        result={"status": "FAIL", "failure_reason": "normative_source_integrity_failed"},
    )

    assert result["status"] == "HOLD"
    assert result["self_governance"]["expected_negative_outcome"] is False
    assert result["self_governance"]["confirmed_blocker"] is True
    assert result["engineering_observation"]["observation"]["type"] == "BLOCKER"
