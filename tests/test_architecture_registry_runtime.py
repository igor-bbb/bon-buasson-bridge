import json
from pathlib import Path

from app.assistant_runtime.architecture_object_registry import AOMM_VERSION, AOT_VERSION, OBJECT_TYPES, RELATIONSHIP_TYPES
from app.assistant_runtime.architecture_registry_runtime import (
    ArchitectureRegistryRepository,
    REGISTRY_PATH,
    evaluate_object_compliance,
    evaluate_registry_compliance,
    execute_architecture_registry_operation,
    get_architecture_object,
    get_object_relationships,
    get_traceability,
    initialize_architecture_registry_runtime,
    list_architecture_objects,
    resolve_dependencies,
    search_architecture_objects,
    validate_registry,
    verify_architecture_object,
)


def test_registry_runtime_loads_permanent_repository():
    result = initialize_architecture_registry_runtime(force=True)
    assert result["status"] == "PASS"
    assert result["loaded"] is True
    assert result["registry_id"] == "VECTRA-ARCHITECTURE-REGISTRY-001"
    assert result["objects_count"] == 24
    assert result["integrity_status"] == "PASS"


def test_repository_uses_approved_aot_and_aomm_without_extension():
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    assert registry["aot_version"] == AOT_VERSION == "1.0"
    assert registry["aomm_version"] == AOMM_VERSION == "1.0"
    assert {obj["object_type"] for obj in registry["objects"]}.issubset(OBJECT_TYPES)
    assert {
        rel["relationship_type"]
        for obj in registry["objects"]
        for rel in obj["relationships"]
    }.issubset(RELATIONSHIP_TYPES)


def test_registry_validator_rejects_integrity_defect(tmp_path):
    registry = json.loads(REGISTRY_PATH.read_text(encoding="utf-8"))
    registry["objects"][0]["relationships"][0]["target_object_id"] = "MISSING"
    result = validate_registry(registry)
    assert result["status"] == "FAIL"
    assert any("dangling_relationship" in error for error in result["errors"])
    broken = tmp_path / "broken.json"
    broken.write_text(json.dumps(registry), encoding="utf-8")
    repository = ArchitectureRegistryRepository(broken)
    try:
        repository.load()
    except RuntimeError as exc:
        assert "architecture_registry_integrity_failed" in str(exc)
    else:
        raise AssertionError("Broken Registry must not load")


def test_get_and_list_architecture_objects():
    single = get_architecture_object({"object_id": "VECTRA-PROFESSIONAL-RUNTIME-001"})
    assert single["status"] == "PASS"
    assert single["object"]["object_type"] == "Runtime"
    assert single["compliance"]["computed"] is True
    listed = list_architecture_objects({"object_type": "Runtime"})
    assert listed["status"] == "PASS"
    assert listed["objects_count"] == 2
    assert all(item["object_type"] == "Runtime" for item in listed["objects"])


def test_search_supports_all_required_registry_criteria():
    cases = [
        ({"object_id": "VECTRA-BUSINESS-RUNTIME-001"}, "VECTRA-BUSINESS-RUNTIME-001"),
        ({"object_type": "Business Domain"}, "VECTRA-DOMAIN-BONBOASON-001"),
        ({"title": "Professional Runtime"}, "VECTRA-PROFESSIONAL-RUNTIME-001"),
        ({"owner": "Architecture Laboratory"}, "AOMM-POLICY-001"),
        ({"normative_source": "Universal Organization Model"}, "AOT-REGISTRY-001"),
        ({"relationship_type": "depends_on"}, "VECTRA-BUSINESS-RUNTIME-001"),
        ({"implementation": "professional_runtime_state.py"}, "VECTRA-PROFESSIONAL-RUNTIME-001"),
        ({"verification": "AUTOMATED"}, "AOMM-POLICY-001"),
    ]
    for filters, expected_id in cases:
        result = search_architecture_objects(filters)
        assert result["status"] == "PASS", filters
        assert expected_id in {item["object_id"] for item in result["results"]}, filters


def test_relationship_navigation_supports_incoming_outgoing_filter_and_path():
    outgoing = get_object_relationships({
        "object_id": "VECTRA-BUSINESS-RUNTIME-001",
        "direction": "outgoing",
        "relationship_type": "depends_on",
    })
    assert outgoing["status"] == "PASS"
    assert outgoing["relationships_count"] == 1
    assert outgoing["relationships"][0]["target_object_id"] == "VECTRA-PROFESSIONAL-RUNTIME-001"
    incoming = get_object_relationships({
        "object_id": "VECTRA-PROFESSIONAL-RUNTIME-001",
        "direction": "incoming",
    })
    assert "VECTRA-BUSINESS-RUNTIME-001" in {
        item["source_object_id"] for item in incoming["relationships"]
    }
    path = get_object_relationships({
        "object_id": "VECTRA-BUSINESS-RUNTIME-001",
        "target_object_id": "VECTRA-INVARIANT-ARCHITECTURE-001",
    })
    assert path["path_status"] == "PASS"
    assert path["path"][0] == "VECTRA-BUSINESS-RUNTIME-001"
    assert path["path"][-1] == "VECTRA-INVARIANT-ARCHITECTURE-001"


def test_traceability_is_dynamic_and_complete():
    result = get_traceability({"object_id": "VECTRA-BUSINESS-RUNTIME-001"})
    assert result["status"] == "PASS"
    assert result["traceability_status"] == "PASS"
    assert [item["stage"] for item in result["chain"]] == [
        "Normative Source",
        "Architecture Object",
        "Architecture Registry",
        "Runtime Mapping",
        "Implementation",
        "Verification",
        "Evidence",
        "Computed Compliance",
    ]
    assert all(result["traceability_checks"].values())


def test_dependency_resolution_returns_direct_reverse_transitive_and_critical():
    result = resolve_dependencies({"object_id": "VECTRA-BUSINESS-RUNTIME-001"})
    assert result["status"] == "PASS"
    assert result["direct_dependencies"] == ["VECTRA-PROFESSIONAL-RUNTIME-001"]
    assert result["transitive_dependencies"] == ["VECTRA-PROFESSIONAL-RUNTIME-001"]
    assert result["critical_dependencies"][0]["object_id"] == "VECTRA-PROFESSIONAL-RUNTIME-001"
    reverse = resolve_dependencies({"object_id": "VECTRA-PROFESSIONAL-RUNTIME-001"})
    assert reverse["reverse_dependencies"] == ["VECTRA-BUSINESS-RUNTIME-001"]


def test_verification_projection_uses_registered_mapping_and_evidence():
    result = verify_architecture_object({"object_id": "VECTRA-PROFESSIONAL-RUNTIME-001"})
    assert result["status"] == "PASS"
    assert result["verification_mapping"]["status"] == "AUTOMATED"
    assert result["tests_count"] == result["existing_tests_count"]
    assert result["missing_test_paths"] == []


def test_object_compliance_is_computed_not_manually_trusted():
    result = evaluate_object_compliance({"object_id": "VECTRA-PROFESSIONAL-RUNTIME-001"})
    assert result["status"] == "PASS"
    assert result["computed"] is True
    assert result["registered_compliance_status_ignored"] == "COMPUTED"


def test_registry_compliance_and_integrity_pass():
    result = evaluate_registry_compliance()
    assert result["status"] == "PASS"
    assert result["computed"] is True
    assert result["objects_count"] == 24
    assert result["objects_failed_count"] == 0
    assert result["integrity"]["status"] == "PASS"


def test_runtime_operation_dispatch_covers_complete_facade_contract():
    operations = {
        "get_architecture_registry_status": {},
        "get_architecture_object": {"object_id": "AOMM-POLICY-001"},
        "list_architecture_objects": {},
        "search_architecture_objects": {"object_type": "Runtime"},
        "get_object_relationships": {"object_id": "VECTRA-BUSINESS-RUNTIME-001"},
        "get_traceability": {"object_id": "AOMM-POLICY-001"},
        "resolve_dependencies": {"object_id": "VECTRA-BUSINESS-RUNTIME-001"},
        "verify_architecture_object": {"object_id": "AOMM-POLICY-001"},
        "evaluate_object_compliance": {"object_id": "AOMM-POLICY-001"},
        "evaluate_registry_compliance": {},
    }
    for operation_type, payload in operations.items():
        result = execute_architecture_registry_operation(operation_type, payload)
        assert result["status"] == "PASS", (operation_type, result)


def test_existing_runtime_facade_exposes_operations_without_new_public_action(monkeypatch):
    from app.api import routes

    schema = routes._memory_facade_operation_request_schema()
    enum = schema["properties"]["operation_type"]["enum"]
    expected = {
        "get_architecture_object",
        "list_architecture_objects",
        "search_architecture_objects",
        "get_object_relationships",
        "get_traceability",
        "resolve_dependencies",
        "verify_architecture_object",
        "evaluate_object_compliance",
        "evaluate_registry_compliance",
    }
    assert expected.issubset(enum)
    assert routes._count_openapi_operations(routes._laboratory_full_openapi_schema()) == 29

    monkeypatch.setattr(routes, "_verify_laboratory_api_key", lambda *_: None)
    response = routes.vectra_laboratory_facade_memory({
        "operation_type": "get_architecture_object",
        "payload": {"object_id": "AOMM-POLICY-001"},
    })
    body = json.loads(response.body)
    assert body["result"]["status"] == "PASS"
    assert body["result"]["object_id"] == "AOMM-POLICY-001"


def test_ep001_file_is_not_part_of_release_implementation():
    release_paths = {
        Path("app/assistant_runtime/architecture_registry_runtime.py"),
        Path("runtime/architecture_registry/architecture_registry.json"),
        Path("tests/test_architecture_registry_runtime.py"),
        Path("app/api/routes.py"),
        Path("app/main.py"),
    }
    assert Path("app/assistant_runtime/self_governance_runtime.py") not in release_paths
