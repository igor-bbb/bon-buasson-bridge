from app.assistant_runtime.professional_pipeline import process_professional_response
from app.assistant_runtime.self_governance_runtime import (
    record_engineering_blocker_owner_decision,
    verify_runtime_operation_blockers,
)


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


def test_navigation_defect_blocks_route_and_engineering_but_research_continues(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    defect = process_professional_response(
        operation_type="navigate_existing_business_workspace",
        runtime_service="digital_business_analyst.navigate_existing_business_workspace",
        endpoint="/vectra/laboratory/facade/memory",
        result={"status": "FAIL", "failure_reason": "nli_unresolved_message", "read_only": True},
    )

    assert defect["status"] == "RESEARCH_CONTINUE"
    assert defect["professional_context"]["outcome_classification"] == "RESEARCH_DEFECT"
    assert defect["self_governance"]["decision"] == "RECORD_DEFECT_AND_CONTINUE_INDEPENDENT_RESEARCH"
    assert defect["execution_gates"]["research"]["status"] == "CONTINUE"
    assert defect["execution_gates"]["current_route"]["status"] == "BLOCKED"
    assert defect["execution_gates"]["engineering"]["status"] == "HOLD"
    assert defect["execution_gates"]["engineering"]["product_owner_approval_required"] is True
    assert defect["engineering_observation"]["observation"]["type"] == "BLOCKER"


def test_open_candidate_does_not_block_journal_or_independent_read_only_route(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    process_professional_response(
        operation_type="navigate_existing_business_workspace",
        runtime_service="digital_business_analyst.navigate_existing_business_workspace",
        endpoint="/vectra/laboratory/facade/memory",
        result={"status": "FAIL", "failure_reason": "nli_unresolved_message", "read_only": True},
    )

    journal = process_professional_response(
        operation_type="create_product_observation",
        runtime_service="development_journal.create_development_request",
        endpoint="/vectra/laboratory/facade/product-review",
        result={"status": "PASS", "record_id": "DEV-0012", "readback_status": "PASS"},
    )
    other_route = process_professional_response(
        operation_type="get_canonical_workspace",
        runtime_service="canonical_workspace.get",
        endpoint="/vectra/query",
        result={"status": "PASS", "read_only": True, "workspace_type": "sku"},
    )

    assert journal["status"] == "RESEARCH_CONTINUE"
    assert journal["professional_context"]["operation_access"]["classification"] == "RESEARCH_GOVERNANCE_WRITE"
    assert journal["execution_gates"]["research"]["development_journal_allowed"] is True
    assert journal["execution_gates"]["engineering"]["status"] == "HOLD"
    assert other_route["status"] == "RESEARCH_CONTINUE"
    assert other_route["execution_gates"]["research"]["independent_routes_allowed"] is True
    assert other_route["execution_gates"]["current_route"]["status"] == "PASS"


def test_successful_read_only_query_cannot_reconcile_engineering_hold(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    failed_route = process_professional_response(
        operation_type="query",
        runtime_service="business_data_facade.query",
        endpoint="/vectra/query",
        result={"status": "FAIL", "failure_reason": "nli_unresolved_message", "read_only": True},
    )
    successful_independent_route = process_professional_response(
        operation_type="query",
        runtime_service="business_data_facade.query",
        endpoint="/vectra/query",
        result={
            "status": "PASS",
            "read_only": True,
            "action_type": "tasks",
            "object_id": "Аврора",
            "period": "2026-02",
        },
    )

    assert failed_route["execution_gates"]["engineering"]["status"] == "HOLD"
    assert successful_independent_route["status"] == "RESEARCH_CONTINUE"
    assert successful_independent_route["blocker_reconciliation"]["status"] == "NOT_APPLICABLE"
    assert successful_independent_route["blocker_reconciliation"]["verified_blockers_count"] == 0
    assert successful_independent_route["self_governance"]["attention"]["open_blockers"] == 1
    assert successful_independent_route["execution_gates"]["research"]["status"] == "CONTINUE"
    assert successful_independent_route["execution_gates"]["engineering"]["status"] == "HOLD"
    assert successful_independent_route["execution_gates"]["engineering"]["product_owner_approval_required"] is True
    assert successful_independent_route["execution_gates"]["engineering"]["protected_mutations_allowed"] is False

    protected_write = process_professional_response(
        operation_type="capitalize_confirmed_knowledge",
        runtime_service="knowledge_capitalization.capitalize_confirmed_knowledge",
        endpoint="/vectra/laboratory/facade/memory",
        result={"status": "PASS"},
    )
    assert protected_write["status"] == "HOLD"
    assert protected_write["execution_gates"]["engineering"]["status"] == "HOLD"
    assert protected_write["execution_gates"]["engineering"]["protected_mutations_allowed"] is False


def test_repeated_known_failure_recreates_durable_hold_before_independent_read(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    first_failure = process_professional_response(
        operation_type="query",
        runtime_service="business_data_facade.query",
        endpoint="/vectra/query",
        result={"status": "FAIL", "failure_reason": "nli_unresolved_message", "read_only": True},
    )
    assert first_failure["self_governance"]["attention"]["open_blockers"] == 1

    # Simulate stale Production state left by the previous release: the event
    # hash remains deduplicated, while its automatically created blocker was
    # already reconciled and closed.
    stale_reconciliation = verify_runtime_operation_blockers(
        operation_type="query",
        runtime_service="business_data_facade.query",
        endpoint="/vectra/query",
        verification_status="PASS",
    )
    assert stale_reconciliation["verified_blockers_count"] == 1
    assert stale_reconciliation["attention"]["open_blockers"] == 0

    repeated_failure = process_professional_response(
        operation_type="query",
        runtime_service="business_data_facade.query",
        endpoint="/vectra/query",
        result={"status": "FAIL", "failure_reason": "nli_unresolved_message", "read_only": True},
    )
    independent_workspace = process_professional_response(
        operation_type="get_canonical_workspace",
        runtime_service="canonical_workspace.get",
        endpoint="/vectra/query",
        result={
            "status": "PASS",
            "read_only": True,
            "workspace_type": "contract",
            "object_id": "Аврора",
            "period": "2026-02",
        },
    )

    assert repeated_failure["engineering_observation"] is not None
    assert repeated_failure["self_governance"]["attention"]["open_blockers"] == 1
    assert independent_workspace["status"] == "RESEARCH_CONTINUE"
    assert independent_workspace["blocker_reconciliation"]["reason"] == "research_safe_operation_cannot_reconcile_engineering_blocker"
    assert independent_workspace["blocker_reconciliation"]["verified_blockers_count"] == 0
    assert independent_workspace["execution_gates"]["engineering"]["status"] == "HOLD"
    assert independent_workspace["execution_gates"]["engineering"]["product_owner_approval_required"] is True
    assert independent_workspace["execution_gates"]["engineering"]["protected_mutations_allowed"] is False


def test_open_candidate_still_blocks_protected_engineering_mutation(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    process_professional_response(
        operation_type="navigate_existing_business_workspace",
        runtime_service="digital_business_analyst.navigate_existing_business_workspace",
        endpoint="/vectra/laboratory/facade/memory",
        result={"status": "FAIL", "failure_reason": "nli_unresolved_message", "read_only": True},
    )
    engineering = process_professional_response(
        operation_type="create_engineering_task",
        runtime_service="development_journal.record_owner_decision",
        endpoint="/vectra/laboratory/facade/product-review",
        result={"status": "PASS"},
    )

    assert engineering["status"] == "HOLD"
    assert engineering["professional_context"]["operation_access"]["classification"] == "PROTECTED_SYSTEM_MUTATION"
    assert engineering["execution_gates"]["engineering"]["status"] == "HOLD"
    assert engineering["execution_gates"]["engineering"]["protected_mutations_allowed"] is False


def test_approved_blocker_releases_pipeline_hold_for_bounded_engineering(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    defect = process_professional_response(
        operation_type="query",
        runtime_service="business_data_facade.query",
        endpoint="/vectra/query",
        result={"status": "ERROR", "failure_reason": "runtime_operation_failed", "read_only": True},
    )
    engineering_item_id = defect["engineering_observation"]["attention"]["blockers"][-1]["engineering_item_id"]
    approved = record_engineering_blocker_owner_decision(
        engineering_item_id,
        "APPROVED",
        product_owner_confirmed=True,
        comment="Approve bounded regression repair.",
    )
    assert approved["engineering_hold"] is False

    independent_read = process_professional_response(
        operation_type="get_canonical_workspace",
        runtime_service="canonical_workspace.get",
        endpoint="/vectra/query",
        result={"status": "PASS", "read_only": True},
    )
    engineering_step = process_professional_response(
        operation_type="update_development_execution",
        runtime_service="development_journal.update_development_execution",
        endpoint="/vectra/laboratory/facade/product-review",
        result={"status": "PASS", "record_id": approved["development_record_id"]},
    )

    assert independent_read["execution_gates"]["engineering"]["status"] == "PASS"
    assert independent_read["execution_gates"]["engineering"]["product_owner_approval_required"] is False
    assert independent_read["execution_gates"]["engineering"]["execution_scope"] == "APPROVED_BLOCKERS_ONLY"
    assert engineering_step["status"] != "HOLD"
    assert engineering_step["execution_gates"]["engineering"]["status"] == "PASS"
    assert engineering_step["execution_gates"]["engineering"]["approved_engineering_item_ids"] == [engineering_item_id]
