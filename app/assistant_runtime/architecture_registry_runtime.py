from __future__ import annotations

import json
from collections import deque
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path
from threading import RLock
from typing import Any, Iterable

from app.assistant_runtime.architecture_object_registry import (
    AOMM_VERSION,
    AOT_VERSION,
    OBJECT_TYPES,
    RELATIONSHIP_TYPES,
    REQUIRED_FIELDS,
    evaluate_object_compliance as _evaluate_object_compliance,
    evaluate_registry_compliance as _evaluate_registry_compliance,
)

RELEASE_ID = "VECTRA-ARCHITECTURE-REGISTRY-001"
REGISTRY_PATH = Path("runtime/architecture_registry/architecture_registry.json")


class ArchitectureRegistryError(RuntimeError):
    """Controlled Architecture Registry Runtime error."""


@dataclass(frozen=True)
class RegistryRuntimeState:
    registry_id: str
    release_id: str
    objects_count: int
    loaded: bool
    integrity_status: str


class ArchitectureRegistryRepository:
    """Read-only Runtime repository for AOMM v1.0 architecture objects.

    Normative documents remain the source of architecture decisions.  This
    repository is only an executable projection of those registered decisions.
    """

    def __init__(self, path: Path = REGISTRY_PATH) -> None:
        self.path = path
        self._registry: dict[str, Any] | None = None
        self._by_id: dict[str, dict[str, Any]] = {}
        self._incoming: dict[str, list[dict[str, str]]] = {}
        self._lock = RLock()

    def load(self, *, force: bool = False) -> RegistryRuntimeState:
        with self._lock:
            if self._registry is not None and not force:
                return self.state()
            if not self.path.exists():
                raise ArchitectureRegistryError(f"architecture_registry_not_found:{self.path}")
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            validation = validate_registry(raw)
            if validation["status"] != "PASS":
                raise ArchitectureRegistryError(
                    "architecture_registry_integrity_failed:" + ";".join(validation["errors"])
                )
            self._registry = raw
            self._by_id = {obj["object_id"]: obj for obj in raw["objects"]}
            incoming: dict[str, list[dict[str, str]]] = {object_id: [] for object_id in self._by_id}
            for source in raw["objects"]:
                for rel in source.get("relationships", []):
                    incoming[rel["target_object_id"]].append({
                        "relationship_type": rel["relationship_type"],
                        "source_object_id": source["object_id"],
                    })
            self._incoming = incoming
            return self.state()

    def state(self) -> RegistryRuntimeState:
        registry = self._require_loaded()
        evaluation = _evaluate_registry_compliance(registry)
        return RegistryRuntimeState(
            registry_id=registry["registry_id"],
            release_id=registry["release_id"],
            objects_count=len(registry["objects"]),
            loaded=True,
            integrity_status=evaluation["status"],
        )

    def registry(self) -> dict[str, Any]:
        return deepcopy(self._require_loaded())

    def list_objects(self) -> list[dict[str, Any]]:
        return deepcopy(self._require_loaded()["objects"])

    def get(self, object_id: str) -> dict[str, Any] | None:
        self._require_loaded()
        obj = self._by_id.get(object_id)
        return deepcopy(obj) if obj else None

    def incoming(self, object_id: str) -> list[dict[str, str]]:
        self._require_loaded()
        return deepcopy(self._incoming.get(object_id, []))

    def known_ids(self) -> set[str]:
        self._require_loaded()
        return set(self._by_id)

    def _require_loaded(self) -> dict[str, Any]:
        if self._registry is None:
            self.load()
        assert self._registry is not None
        return self._registry


_REPOSITORY = ArchitectureRegistryRepository()


def initialize_architecture_registry_runtime(*, force: bool = False) -> dict[str, Any]:
    state = _REPOSITORY.load(force=force)
    return {
        "status": "PASS",
        "verification_status": "PASS",
        "runtime_component": "Architecture Registry Runtime",
        "registry_id": state.registry_id,
        "release_id": state.release_id,
        "loaded": state.loaded,
        "objects_count": state.objects_count,
        "integrity_status": state.integrity_status,
        "aot_version": AOT_VERSION,
        "aomm_version": AOMM_VERSION,
    }


def validate_registry(registry: dict[str, Any]) -> dict[str, Any]:
    errors: list[str] = []
    if registry.get("aot_version") != AOT_VERSION:
        errors.append("unsupported_aot_version")
    if registry.get("aomm_version") != AOMM_VERSION:
        errors.append("unsupported_aomm_version")
    objects = registry.get("objects")
    if not isinstance(objects, list) or not objects:
        errors.append("registry_objects_required")
        objects = []
    ids = [obj.get("object_id") for obj in objects if isinstance(obj, dict)]
    if len(ids) != len(set(ids)):
        errors.append("duplicate_object_ids")
    known_ids = {item for item in ids if item}
    for obj in objects:
        if not isinstance(obj, dict):
            errors.append("object_must_be_mapping")
            continue
        missing = sorted(set(REQUIRED_FIELDS) - set(obj))
        extra = sorted(set(obj) - set(REQUIRED_FIELDS))
        if missing:
            errors.append(f"{obj.get('object_id','unknown')}:missing_fields:{','.join(missing)}")
        if extra:
            errors.append(f"{obj.get('object_id','unknown')}:special_fields_forbidden:{','.join(extra)}")
        if obj.get("object_type") not in OBJECT_TYPES:
            errors.append(f"{obj.get('object_id','unknown')}:unsupported_object_type")
        if obj.get("version") != AOMM_VERSION:
            errors.append(f"{obj.get('object_id','unknown')}:unsupported_aomm_version")
        for rel in obj.get("relationships", []):
            if rel.get("relationship_type") not in RELATIONSHIP_TYPES:
                errors.append(f"{obj.get('object_id','unknown')}:unsupported_relationship_type")
            if rel.get("target_object_id") not in known_ids:
                errors.append(f"{obj.get('object_id','unknown')}:dangling_relationship")
    return {
        "status": "PASS" if not errors else "FAIL",
        "verification_status": "PASS" if not errors else "FAIL",
        "objects_count": len(objects),
        "errors": errors,
    }


def get_architecture_registry_runtime_status() -> dict[str, Any]:
    return initialize_architecture_registry_runtime()


def get_architecture_object(payload: dict[str, Any]) -> dict[str, Any]:
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        return _fail("object_id_required", "object_id is required")
    obj = _REPOSITORY.get(object_id)
    if obj is None:
        return _fail("architecture_object_not_found", f"Architecture object {object_id} was not found")
    compliance = _evaluate_object_compliance(obj, _REPOSITORY.known_ids())
    obj["compliance_status"] = compliance["status"]
    return _pass(object=obj, object_id=object_id, compliance=compliance)


def list_architecture_objects(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    objects = _REPOSITORY.list_objects()
    object_type = payload.get("object_type")
    owner = payload.get("owner")
    lifecycle_state = payload.get("lifecycle_state")
    if object_type:
        objects = [obj for obj in objects if obj["object_type"] == object_type]
    if owner:
        objects = [obj for obj in objects if _contains(obj.get("owner"), owner)]
    if lifecycle_state:
        objects = [obj for obj in objects if obj.get("lifecycle_state") == lifecycle_state]
    return _pass(
        objects_count=len(objects),
        objects=[_object_summary(obj) for obj in objects],
        filters={"object_type": object_type, "owner": owner, "lifecycle_state": lifecycle_state},
    )


def search_architecture_objects(payload: dict[str, Any]) -> dict[str, Any]:
    supported = {
        "object_id", "object_type", "title", "owner", "normative_source",
        "relationship_type", "implementation", "verification",
    }
    filters = {key: value for key, value in payload.items() if key in supported and value not in (None, "", [])}
    query = payload.get("query")
    if not filters and not query:
        return _fail("search_criteria_required", "At least one architecture search criterion is required")
    results = []
    for obj in _REPOSITORY.list_objects():
        if query and not _contains(obj, query):
            continue
        if all(_matches_filter(obj, key, value) for key, value in filters.items()):
            results.append(_object_summary(obj))
    return _pass(results_count=len(results), results=results, filters=filters, query=query)


def get_object_relationships(payload: dict[str, Any]) -> dict[str, Any]:
    object_id = str(payload.get("object_id") or "").strip()
    direction = str(payload.get("direction") or "all").lower()
    relationship_type = payload.get("relationship_type")
    if not object_id:
        return _fail("object_id_required", "object_id is required")
    obj = _REPOSITORY.get(object_id)
    if obj is None:
        return _fail("architecture_object_not_found", f"Architecture object {object_id} was not found")
    if direction not in {"incoming", "outgoing", "all"}:
        return _fail("unsupported_relationship_direction", "direction must be incoming, outgoing or all")
    outgoing = [
        {**rel, "source_object_id": object_id}
        for rel in obj.get("relationships", [])
        if not relationship_type or rel["relationship_type"] == relationship_type
    ]
    incoming = [
        {**rel, "target_object_id": object_id}
        for rel in _REPOSITORY.incoming(object_id)
        if not relationship_type or rel["relationship_type"] == relationship_type
    ]
    selected = outgoing if direction == "outgoing" else incoming if direction == "incoming" else outgoing + incoming
    related_ids = sorted({rel.get("target_object_id") or rel.get("source_object_id") for rel in selected} - {object_id})
    related = [_object_summary(_REPOSITORY.get(item)) for item in related_ids if _REPOSITORY.get(item)]
    result = _pass(
        object_id=object_id,
        direction=direction,
        relationship_type=relationship_type,
        relationships_count=len(selected),
        relationships=selected,
        related_objects=related,
    )
    target_id = payload.get("target_object_id")
    if target_id:
        result["path"] = _find_path(object_id, str(target_id), relationship_type)
        result["path_status"] = "PASS" if result["path"] else "NOT_FOUND"
    return result


def get_traceability(payload: dict[str, Any]) -> dict[str, Any]:
    object_id = str(payload.get("object_id") or "").strip()
    obj = _REPOSITORY.get(object_id)
    if obj is None:
        return _fail("architecture_object_not_found", f"Architecture object {object_id} was not found")
    compliance = _evaluate_object_compliance(obj, _REPOSITORY.known_ids())
    chain = [
        {"stage": "Normative Source", "value": deepcopy(obj["normative_source"])},
        {"stage": "Architecture Object", "value": object_id},
        {"stage": "Architecture Registry", "value": _REPOSITORY.state().registry_id},
        {"stage": "Runtime Mapping", "value": obj["implementation"].get("runtime_mapping")},
        {"stage": "Implementation", "value": deepcopy(obj["implementation"].get("paths", []))},
        {"stage": "Verification", "value": deepcopy(obj["verification"])},
        {"stage": "Evidence", "value": deepcopy(obj["evidence"])},
        {"stage": "Computed Compliance", "value": compliance["status"]},
    ]
    return _pass(
        object_id=object_id,
        traceability_status=compliance["status"],
        chain=chain,
        traceability_checks=compliance["traceability"],
    )


def resolve_dependencies(payload: dict[str, Any]) -> dict[str, Any]:
    object_id = str(payload.get("object_id") or "").strip()
    if _REPOSITORY.get(object_id) is None:
        return _fail("architecture_object_not_found", f"Architecture object {object_id} was not found")
    direct = _dependency_targets(object_id)
    reverse = sorted({rel["source_object_id"] for rel in _REPOSITORY.incoming(object_id) if rel["relationship_type"] == "depends_on"})
    transitive = _transitive_dependencies(object_id)
    critical_types = {"Runtime", "Component", "Registry", "Contract", "Invariant", "Service"}
    critical = []
    for dependency_id in transitive:
        dependency = _REPOSITORY.get(dependency_id)
        if not dependency:
            continue
        reverse_count = len([
            rel for rel in _REPOSITORY.incoming(dependency_id)
            if rel["relationship_type"] == "depends_on"
        ])
        if dependency["object_type"] in critical_types or reverse_count > 1:
            critical.append({
                "object_id": dependency_id,
                "object_type": dependency["object_type"],
                "reverse_dependency_count": reverse_count,
                "reason": "critical_architecture_type" if dependency["object_type"] in critical_types else "shared_dependency",
            })
    return _pass(
        object_id=object_id,
        direct_dependencies=direct,
        reverse_dependencies=reverse,
        transitive_dependencies=transitive,
        critical_dependencies=critical,
    )


def verify_architecture_object(payload: dict[str, Any]) -> dict[str, Any]:
    object_id = str(payload.get("object_id") or "").strip()
    obj = _REPOSITORY.get(object_id)
    if obj is None:
        return _fail("architecture_object_not_found", f"Architecture object {object_id} was not found")
    verification = deepcopy(obj["verification"])
    tests = verification.get("tests", [])
    evidence = [item for item in obj.get("evidence", []) if item.get("type") in {"source_path", "verification", "test"}]
    existing_tests = [path for path in tests if Path(path).exists()]
    status = "PASS" if tests and len(existing_tests) == len(tests) and verification.get("status") else "FAIL"
    return {
        "status": status,
        "verification_status": status,
        "object_id": object_id,
        "verification_mapping": verification,
        "verification_evidence": evidence,
        "tests_count": len(tests),
        "existing_tests_count": len(existing_tests),
        "missing_test_paths": sorted(set(tests) - set(existing_tests)),
        "error": None if status == "PASS" else {"code": "verification_mapping_incomplete", "message": "Verification mapping is incomplete."},
    }


def evaluate_object_compliance(payload: dict[str, Any]) -> dict[str, Any]:
    object_id = str(payload.get("object_id") or "").strip()
    obj = _REPOSITORY.get(object_id)
    if obj is None:
        return _fail("architecture_object_not_found", f"Architecture object {object_id} was not found")
    evaluation = _evaluate_object_compliance(obj, _REPOSITORY.known_ids())
    return {
        "status": evaluation["status"],
        "verification_status": evaluation["status"],
        "object_id": object_id,
        "computed": True,
        "registered_compliance_status_ignored": obj.get("compliance_status"),
        "traceability": evaluation["traceability"],
        "errors": evaluation["errors"],
        "error": None if evaluation["status"] == "PASS" else {"code": "object_compliance_failed", "message": "Computed object compliance failed."},
    }


def evaluate_registry_compliance(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    del payload
    registry = _REPOSITORY.registry()
    evaluation = _evaluate_registry_compliance(registry)
    return {
        **evaluation,
        "computed": True,
        "registry_id": registry["registry_id"],
        "release_id": registry["release_id"],
        "integrity": validate_registry(registry),
        "error": None if evaluation["status"] == "PASS" else {"code": "registry_compliance_failed", "message": "Computed registry compliance failed."},
    }


def execute_architecture_registry_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    operations = {
        "get_architecture_registry_status": lambda _: get_architecture_registry_runtime_status(),
        "get_architecture_object": get_architecture_object,
        "list_architecture_objects": list_architecture_objects,
        "search_architecture_objects": search_architecture_objects,
        "get_object_relationships": get_object_relationships,
        "get_traceability": get_traceability,
        "resolve_dependencies": resolve_dependencies,
        "verify_architecture_object": verify_architecture_object,
        "evaluate_object_compliance": evaluate_object_compliance,
        "evaluate_registry_compliance": evaluate_registry_compliance,
    }
    handler = operations.get(operation_type)
    if handler is None:
        return _fail("unsupported_architecture_registry_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload)


def _object_summary(obj: dict[str, Any] | None) -> dict[str, Any]:
    if obj is None:
        return {}
    return {
        "object_id": obj["object_id"],
        "object_type": obj["object_type"],
        "title": obj["title"],
        "owner": obj["owner"],
        "lifecycle_state": obj["lifecycle_state"],
    }


def _contains(value: Any, expected: Any) -> bool:
    needle = str(expected).casefold()
    if isinstance(value, dict):
        return any(_contains(item, expected) for item in value.values())
    if isinstance(value, (list, tuple, set)):
        return any(_contains(item, expected) for item in value)
    return needle in str(value).casefold()


def _matches_filter(obj: dict[str, Any], key: str, value: Any) -> bool:
    if key == "relationship_type":
        return any(_contains(rel.get("relationship_type"), value) for rel in obj.get("relationships", []))
    return _contains(obj.get(key), value)


def _dependency_targets(object_id: str) -> list[str]:
    obj = _REPOSITORY.get(object_id) or {}
    return sorted({
        rel["target_object_id"] for rel in obj.get("relationships", [])
        if rel["relationship_type"] == "depends_on"
    })


def _transitive_dependencies(object_id: str) -> list[str]:
    visited: set[str] = set()
    queue: deque[str] = deque(_dependency_targets(object_id))
    while queue:
        current = queue.popleft()
        if current in visited or current == object_id:
            continue
        visited.add(current)
        queue.extend(_dependency_targets(current))
    return sorted(visited)


def _find_path(source_id: str, target_id: str, relationship_type: str | None = None) -> list[str]:
    if source_id == target_id:
        return [source_id]
    queue: deque[tuple[str, list[str]]] = deque([(source_id, [source_id])])
    visited = {source_id}
    while queue:
        current, path = queue.popleft()
        obj = _REPOSITORY.get(current) or {}
        neighbours = [
            rel["target_object_id"] for rel in obj.get("relationships", [])
            if not relationship_type or rel["relationship_type"] == relationship_type
        ]
        neighbours.extend(
            rel["source_object_id"] for rel in _REPOSITORY.incoming(current)
            if not relationship_type or rel["relationship_type"] == relationship_type
        )
        for neighbour in sorted(set(neighbours)):
            if neighbour == target_id:
                return path + [neighbour]
            if neighbour not in visited:
                visited.add(neighbour)
                queue.append((neighbour, path + [neighbour]))
    return []


def _pass(**fields: Any) -> dict[str, Any]:
    return {"status": "PASS", "verification_status": "PASS", **fields, "error": None}


def _fail(code: str, message: str, **fields: Any) -> dict[str, Any]:
    return {
        "status": "FAIL",
        "verification_status": "FAIL",
        **fields,
        "error": {"code": code, "message": message},
    }
