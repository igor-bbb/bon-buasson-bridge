from __future__ import annotations

import json
from copy import deepcopy
from pathlib import Path
from typing import Any

AOMM_VERSION = "1.0"
AOT_VERSION = "1.0"
OBJECT_TYPES = frozenset({
    "Component", "Runtime", "Capability", "Requirement", "Contract", "Verification",
    "Registry", "Role", "Knowledge", "Decision", "Policy", "Invariant", "Interface",
    "Action", "Service", "Business Domain",
})
RELATIONSHIP_TYPES = frozenset({
    "depends_on", "implements", "verifies", "owns", "uses", "produces", "consumes",
    "extends", "specializes", "governed_by", "capitalizes", "maps_to", "derived_from",
})
REQUIRED_FIELDS = (
    "object_id", "object_type", "title", "description", "normative_source", "owner",
    "lifecycle_state", "relationships", "requirements", "implementation", "verification",
    "compliance_status", "evidence", "version",
)
REGISTRY_PATH = Path(__file__).with_name("architecture_registry_pilot.json")


def load_architecture_registry(path: Path | None = None) -> dict[str, Any]:
    target = path or REGISTRY_PATH
    return json.loads(target.read_text(encoding="utf-8"))


def validate_architecture_object(obj: dict[str, Any]) -> list[str]:
    errors: list[str] = []
    missing = [field for field in REQUIRED_FIELDS if field not in obj]
    if missing:
        errors.append(f"missing_fields:{','.join(missing)}")
    extra = sorted(set(obj) - set(REQUIRED_FIELDS))
    if extra:
        errors.append(f"special_fields_forbidden:{','.join(extra)}")
    if obj.get("object_type") not in OBJECT_TYPES:
        errors.append("unsupported_object_type")
    if obj.get("version") != AOMM_VERSION:
        errors.append("unsupported_aomm_version")
    for relationship in obj.get("relationships", []):
        if relationship.get("relationship_type") not in RELATIONSHIP_TYPES:
            errors.append("unsupported_relationship_type")
        if not relationship.get("target_object_id"):
            errors.append("relationship_target_required")
    return errors


def evaluate_object_compliance(obj: dict[str, Any], known_ids: set[str]) -> dict[str, Any]:
    errors = validate_architecture_object(obj)
    traceability_checks = {
        "normative_source": bool(obj.get("normative_source", {}).get("document") and obj.get("normative_source", {}).get("section")),
        "registry": bool(obj.get("object_id")),
        "runtime_mapping": bool(obj.get("implementation", {}).get("runtime_mapping")),
        "implementation": bool(obj.get("implementation", {}).get("paths")),
        "verification": bool(obj.get("verification", {}).get("tests")),
        "evidence": bool(obj.get("evidence")),
    }
    dangling = [
        rel.get("target_object_id") for rel in obj.get("relationships", [])
        if rel.get("target_object_id") not in known_ids
    ]
    if dangling:
        errors.append("dangling_relationship:" + ",".join(dangling))
    if not all(traceability_checks.values()):
        errors.append("incomplete_traceability")
    status = "PASS" if not errors else "FAIL"
    return {
        "status": status,
        "computed": True,
        "traceability": traceability_checks,
        "errors": errors,
    }


def evaluate_registry_compliance(registry: dict[str, Any]) -> dict[str, Any]:
    objects = registry.get("objects", [])
    ids = [obj.get("object_id") for obj in objects]
    duplicate_ids = sorted({item for item in ids if ids.count(item) > 1})
    known_ids = {item for item in ids if item}
    results = []
    for obj in objects:
        evaluation = evaluate_object_compliance(obj, known_ids)
        results.append({"object_id": obj.get("object_id"), **evaluation})
    passed = sum(item["status"] == "PASS" for item in results)
    failed = len(results) - passed
    registry_errors = []
    if duplicate_ids:
        registry_errors.append("duplicate_object_ids:" + ",".join(duplicate_ids))
    if not 20 <= len(objects) <= 30:
        registry_errors.append("pilot_object_count_out_of_range")
    status = "PASS" if failed == 0 and not registry_errors else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "aot_version": registry.get("aot_version"),
        "aomm_version": registry.get("aomm_version"),
        "objects_count": len(objects),
        "objects_passed_count": passed,
        "objects_failed_count": failed,
        "registry_errors": registry_errors,
        "object_results": results,
    }


def build_validated_registry(registry: dict[str, Any] | None = None) -> dict[str, Any]:
    result = deepcopy(registry or load_architecture_registry())
    evaluation = evaluate_registry_compliance(result)
    by_id = {item["object_id"]: item for item in evaluation["object_results"]}
    for obj in result.get("objects", []):
        obj["compliance_status"] = by_id[obj["object_id"]]["status"]
    result["compliance"] = evaluation
    return result


def get_architecture_registry_pilot() -> dict[str, Any]:
    registry = build_validated_registry()
    return {
        "status": registry["compliance"]["status"],
        "verification_status": registry["compliance"]["verification_status"],
        "registry_id": registry["registry_id"],
        "aot_version": registry["aot_version"],
        "aomm_version": registry["aomm_version"],
        "objects_count": registry["compliance"]["objects_count"],
        "compliance": registry["compliance"],
        "objects": registry["objects"],
    }
