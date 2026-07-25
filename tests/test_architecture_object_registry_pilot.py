import json
from pathlib import Path

from app.assistant_runtime.architecture_object_registry import (
    AOMM_VERSION, OBJECT_TYPES, RELATIONSHIP_TYPES, REQUIRED_FIELDS,
    build_validated_registry, evaluate_object_compliance,
    evaluate_registry_compliance, load_architecture_registry,
)


def test_pilot_contains_20_to_30_objects():
    registry = load_architecture_registry()
    assert 20 <= len(registry["objects"]) <= 30


def test_every_object_uses_single_aomm_schema():
    registry = load_architecture_registry()
    field_sets = []
    for obj in registry["objects"]:
        assert set(REQUIRED_FIELDS).issubset(obj)
        assert obj["version"] == AOMM_VERSION
        field_sets.append(set(obj))
    assert len({frozenset(fields) for fields in field_sets}) == 1


def test_taxonomy_is_closed_to_approved_types():
    registry = load_architecture_registry()
    assert {obj["object_type"] for obj in registry["objects"]}.issubset(OBJECT_TYPES)
    assert len({obj["object_type"] for obj in registry["objects"]}) >= 12


def test_relationship_taxonomy_and_targets_are_valid():
    registry = load_architecture_registry()
    ids = {obj["object_id"] for obj in registry["objects"]}
    for obj in registry["objects"]:
        for rel in obj["relationships"]:
            assert rel["relationship_type"] in RELATIONSHIP_TYPES
            assert rel["target_object_id"] in ids


def test_traceability_chain_is_complete_for_every_object():
    registry = load_architecture_registry()
    ids = {obj["object_id"] for obj in registry["objects"]}
    for obj in registry["objects"]:
        result = evaluate_object_compliance(obj, ids)
        assert result["status"] == "PASS", result
        assert all(result["traceability"].values())


def test_compliance_is_computed_not_manually_trusted():
    registry = load_architecture_registry()
    registry["objects"][0]["compliance_status"] = "PASS"
    registry["objects"][0]["verification"]["tests"] = []
    evaluation = evaluate_registry_compliance(registry)
    assert evaluation["status"] == "FAIL"
    assert evaluation["objects_failed_count"] == 1


def test_valid_registry_compliance_passes():
    result = evaluate_registry_compliance(load_architecture_registry())
    assert result["status"] == "PASS"
    assert result["objects_count"] == 24
    assert result["objects_passed_count"] == 24
    assert result["objects_failed_count"] == 0


def test_validated_registry_materializes_computed_status():
    registry = build_validated_registry()
    assert registry["compliance"]["status"] == "PASS"
    assert all(obj["compliance_status"] == "PASS" for obj in registry["objects"])


def test_registry_implementation_and_evidence_paths_exist():
    registry = load_architecture_registry()
    for obj in registry["objects"]:
        for path in obj["implementation"]["paths"]:
            assert Path(path).exists(), (obj["object_id"], path)
        for test_path in obj["verification"]["tests"]:
            assert Path(test_path).exists(), (obj["object_id"], test_path)


def test_invalid_special_object_shape_fails_schema():
    registry = load_architecture_registry()
    bad = dict(registry["objects"][0])
    bad["special_runtime_fields"] = {"forbidden": True}
    ids = {obj["object_id"] for obj in registry["objects"]}
    result = evaluate_object_compliance(bad, ids)
    assert result["status"] == "FAIL"
    assert "special_fields_forbidden:special_runtime_fields" in result["errors"]


def test_existing_memory_facade_exposes_pilot_without_new_action():
    from app.api import routes
    schema = routes._memory_facade_operation_request_schema()
    operations = schema["properties"]["operation_type"]["enum"]
    assert "get_architecture_registry_pilot" in operations
    assert "verify_architecture_registry_pilot" in operations
    assert routes._count_openapi_operations(routes._laboratory_full_openapi_schema()) == 30


def test_memory_facade_returns_runtime_registry_and_verification(monkeypatch):
    from app.api import routes
    monkeypatch.setattr(routes, "_verify_laboratory_api_key", lambda *_: None)
    response = routes.vectra_laboratory_facade_memory({"operation_type": "verify_architecture_registry_pilot"})
    payload = json.loads(response.body)
    result = payload["result"]
    assert result["status"] == "PASS"
    assert result["objects_count"] == 24
    assert result["objects_failed_count"] == 0
