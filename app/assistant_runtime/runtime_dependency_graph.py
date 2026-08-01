from __future__ import annotations

import json
import os
import re
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Callable

from app.assistant_runtime.architecture_registry_runtime import initialize_architecture_registry_runtime
from app.assistant_runtime.verification_runtime import initialize_verification_runtime
from app.assistant_runtime.execution_runtime import initialize_execution_runtime
from app.assistant_runtime.execution_orchestrator_runtime import initialize_execution_orchestrator
from app.assistant_runtime.session_runtime import initialize_session_runtime
from app.assistant_runtime.runtime_supervisor import initialize_runtime_supervisor
from app.assistant_runtime.runtime_recovery import initialize_runtime_recovery
from app.assistant_runtime.runtime_capability_registry import (
    get_runtime_capabilities,
    initialize_runtime_capability_registry,
)

RELEASE_ID = "VECTRA-RUNTIME-DEPENDENCY-GRAPH-001"
REPOSITORY_PATH = Path("runtime/runtime_dependency_graph/dependencies.json")
LOAD_ORDER = "AFTER_RUNTIME_CAPABILITY_REGISTRY"


class RuntimeDependencyGraphError(RuntimeError):
    pass


Publisher = tuple[str, Callable[..., dict[str, Any]]]
_PUBLISHERS: tuple[Publisher, ...] = (
    ("Architecture Registry Runtime", initialize_architecture_registry_runtime),
    ("Verification Runtime", initialize_verification_runtime),
    ("Execution Runtime", initialize_execution_runtime),
    ("Execution Orchestrator Runtime", initialize_execution_orchestrator),
    ("Session Runtime", initialize_session_runtime),
    ("Runtime Supervisor", initialize_runtime_supervisor),
    ("Runtime Recovery", initialize_runtime_recovery),
    ("Runtime Capability Registry", initialize_runtime_capability_registry),
)

# Translation of publisher-owned field names into canonical component identities.
# No relation is produced unless the source explicitly publishes the field as true.
_DEPENDENCY_FIELDS = {
    "architecture_registry_loaded": "Architecture Registry Runtime",
    "verification_runtime_loaded": "Verification Runtime",
    "execution_runtime_loaded": "Execution Runtime",
    "execution_orchestrator_loaded": "Execution Orchestrator Runtime",
    "session_runtime_loaded": "Session Runtime",
    "supervisor_available": "Runtime Supervisor",
}
_LOAD_ORDER_TARGETS = {
    "AFTER_VERIFICATION_RUNTIME": "Verification Runtime",
    "AFTER_EXECUTION_RUNTIME": "Execution Runtime",
    "AFTER_EXECUTION_ORCHESTRATOR_RUNTIME": "Execution Orchestrator Runtime",
    "AFTER_SESSION_RUNTIME": "Session Runtime",
    "AFTER_RUNTIME_SUPERVISOR": "Runtime Supervisor",
    "AFTER_RUNTIME_RECOVERY": "Runtime Recovery",
}


class DependencyGraphRepository:
    def __init__(self, path: Path = REPOSITORY_PATH) -> None:
        self.path = path
        self._lock = RLock()
        self._data: dict[str, Any] | None = None

    def load(self, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            if self._data is not None and not force:
                return deepcopy(self._data)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            if self.path.exists():
                value = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(value, dict) or not isinstance(value.get("dependencies"), list):
                    raise RuntimeDependencyGraphError("dependency_graph_repository_invalid")
                self._data = value
            else:
                self._data = self._empty()
                self._persist()
            return deepcopy(self._data)

    def replace(self, graph_id: str, dependencies: list[dict[str, Any]], graph_status: str) -> None:
        with self._lock:
            if self._data is None:
                self.load()
            assert self._data is not None
            previous = self._data.get("dependencies", [])
            now = _now()
            if previous != dependencies or self._data.get("graph_status") != graph_status:
                self._data.setdefault("build_history", []).append({
                    "graph_id": graph_id,
                    "built_at": now,
                    "graph_status": graph_status,
                    "dependencies_count": len(dependencies),
                })
            self._data["graph_id"] = graph_id
            self._data["dependencies"] = deepcopy(dependencies)
            self._data["graph_status"] = graph_status
            self._data["updated_at"] = now
            self._persist()

    def dependencies(self) -> list[dict[str, Any]]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return deepcopy(self._data["dependencies"])

    def state(self) -> dict[str, Any]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return {
            "repository_id": self._data["repository_id"],
            "graph_id": self._data.get("graph_id"),
            "loaded": True,
            "dependencies_count": len(self._data["dependencies"]),
            "history_count": len(self._data.get("build_history", [])),
            "derived_operational_data_only": True,
        }

    def _empty(self) -> dict[str, Any]:
        return {
            "repository_id": "VECTRA-RUNTIME-DEPENDENCY-GRAPH-REPOSITORY-001",
            "release_id": RELEASE_ID,
            "schema_version": "1.0",
            "graph_id": None,
            "graph_status": "EMPTY",
            "dependencies": [],
            "build_history": [],
            "updated_at": _now(),
        }

    def _persist(self) -> None:
        assert self._data is not None
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, self.path)


_REPOSITORY = DependencyGraphRepository()


def initialize_runtime_dependency_graph(*, force: bool = False) -> dict[str, Any]:
    _REPOSITORY.load(force=force)
    discovered = discover_runtime_dependencies()
    if discovered.get("status") != "PASS":
        return discovered
    graph = build_dependency_graph(discovered["publications"], discovered["capabilities"])
    if graph.get("status") != "PASS":
        return graph
    _REPOSITORY.replace(graph["graph_id"], graph["dependencies"], graph["graph_status"])
    return _pass(
        runtime_component="Runtime Dependency Graph",
        release_id=RELEASE_ID,
        loaded=True,
        load_order=LOAD_ORDER,
        graph_status=graph["graph_status"],
        connection_status="CONNECTED",
        runtime_capability_registry_loaded=True,
        nodes_count=graph["nodes_count"],
        dependencies_count=len(graph["dependencies"]),
        missing_dependencies=graph["missing_dependencies"],
        cycles=graph["cycles"],
        inferred_dependencies_created=False,
        normative_source=False,
        repository=_REPOSITORY.state(),
        evaluated_at=_now(),
    )


def discover_runtime_dependencies() -> dict[str, Any]:
    publications: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for publisher_name, publisher in _PUBLISHERS:
        try:
            publication = publisher(force=False)
        except TypeError:
            publication = publisher()
        except Exception as exc:
            rejected.append({"publisher": publisher_name, "reason": "publisher_unavailable", "detail": str(exc)})
            continue
        checked = validate_dependency_publication(publisher_name, publication)
        if checked.get("status") != "PASS":
            rejected.append({"publisher": publisher_name, "reason": checked.get("failure_reason")})
            continue
        publications.append(deepcopy(publication))
    capabilities_result = get_runtime_capabilities({"refresh": False})
    if capabilities_result.get("status") != "PASS":
        return _fail("runtime_capability_registry_unavailable", "Runtime Capability Registry did not publish capabilities")
    if rejected:
        return _fail("runtime_dependency_publication_rejected", "One or more Runtime dependency publications were not confirmed", rejected_publications=sorted(rejected, key=lambda x: x["publisher"]))
    publications.sort(key=lambda x: str(x.get("runtime_component")))
    capabilities = sorted(capabilities_result["capabilities"], key=lambda x: x["capability_id"])
    return _pass(publications=publications, capabilities=capabilities)


def validate_dependency_publication(publisher_name: str, publication: Any) -> dict[str, Any]:
    if not isinstance(publication, dict):
        return _fail("dependency_publication_invalid", "Publisher response must be an object")
    if publication.get("status") != "PASS" or publication.get("loaded") is not True:
        return _fail("dependency_publication_unconfirmed", "Publisher must be loaded and return PASS")
    if str(publication.get("runtime_component") or "").strip() != publisher_name:
        return _fail("dependency_publisher_identity_mismatch", "Published component identity does not match publisher")
    if not str(publication.get("release_id") or "").strip():
        return _fail("dependency_publication_version_missing", "Publisher release_id is required")
    return _pass(publisher=publisher_name)


def build_dependency_graph(publications: list[dict[str, Any]], capabilities: list[dict[str, Any]]) -> dict[str, Any]:
    now = _now()
    graph_id = "VECTRA-RUNTIME-DEPENDENCY-GRAPH-CURRENT"
    published_components = {str(item.get("runtime_component")) for item in publications}
    dependencies: list[dict[str, Any]] = []

    def add(source: str, target: str, publisher: str, published_at: str) -> None:
        dependency_id = "dependency." + re.sub(r"[^a-z0-9]+", ".", f"{source}.to.{target}".lower()).strip(".")
        dependencies.append({
            "graph_id": graph_id,
            "node_id": source,
            "dependency_id": dependency_id,
            "publisher": publisher,
            "source_component": source,
            "target_component": target,
            "published_at": published_at,
            "last_verified_at": now,
            "graph_status": "REGISTERED",
        })

    for publication in publications:
        source = str(publication["runtime_component"])
        published_at = str(publication.get("evaluated_at") or now)
        explicit_targets: set[str] = set()
        for field, target in _DEPENDENCY_FIELDS.items():
            if publication.get(field) is True:
                explicit_targets.add(target)
        load_target = _LOAD_ORDER_TARGETS.get(str(publication.get("load_order") or ""))
        if load_target:
            explicit_targets.add(load_target)
        for target in sorted(explicit_targets):
            add(source, target, source, published_at)

    for capability in capabilities:
        if capability.get("publication_status") != "PUBLISHED" or capability.get("registry_status") != "REGISTERED":
            return _fail("dependency_capability_publication_unconfirmed", "Capability relation is not confirmed by Runtime Capability Registry")
        source = str(capability["capability_id"])
        target = str(capability["runtime_component"])
        add(source, target, "Runtime Capability Registry", str(capability.get("published_at") or now))

    unique = {(item["source_component"], item["target_component"]): item for item in dependencies}
    dependencies = sorted(unique.values(), key=lambda x: (x["source_component"], x["target_component"], x["dependency_id"]))
    nodes = sorted({item["source_component"] for item in dependencies} | {item["target_component"] for item in dependencies})
    missing = sorted({item["target_component"] for item in dependencies if item["target_component"] not in published_components and not item["target_component"].startswith("runtime.")})
    cycles = _find_cycles(dependencies)
    graph_status = "INVALID" if missing or cycles else "READY"
    return _pass(graph_id=graph_id, graph_status=graph_status, nodes=nodes, nodes_count=len(nodes), dependencies=dependencies, missing_dependencies=missing, cycles=cycles)


def get_runtime_dependency_graph(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    if bool(payload.get("refresh")) or not _REPOSITORY.dependencies():
        state = initialize_runtime_dependency_graph(force=bool(payload.get("refresh")))
        if state.get("status") != "PASS":
            return state
    deps = _REPOSITORY.dependencies()
    nodes = sorted({x["source_component"] for x in deps} | {x["target_component"] for x in deps})
    return _pass(nodes_count=len(nodes), dependencies_count=len(deps), nodes=nodes, dependencies=deps, repository=_REPOSITORY.state())


def search_runtime_dependencies(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    query = str(payload.get("query") or "").strip().lower()
    source = str(payload.get("source_component") or "").strip()
    target = str(payload.get("target_component") or "").strip()
    results = []
    for item in _REPOSITORY.dependencies():
        searchable = " ".join(str(item.get(k) or "") for k in ("dependency_id", "publisher", "source_component", "target_component")).lower()
        if query and query not in searchable:
            continue
        if source and item["source_component"] != source:
            continue
        if target and item["target_component"] != target:
            continue
        results.append(item)
    results.sort(key=lambda x: (x["source_component"], x["target_component"], x["dependency_id"]))
    return _pass(results_count=len(results), results=results)


def trace_runtime_dependency(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    source = str(payload.get("source_component") or "").strip()
    target = str(payload.get("target_component") or "").strip()
    if not source or not target:
        return _fail("dependency_trace_endpoints_required", "source_component and target_component are required")
    adjacency: dict[str, list[str]] = {}
    for item in _REPOSITORY.dependencies():
        adjacency.setdefault(item["source_component"], []).append(item["target_component"])
    for values in adjacency.values():
        values.sort()
    queue: list[list[str]] = [[source]]
    visited = {source}
    while queue:
        path = queue.pop(0)
        node = path[-1]
        if node == target:
            return _pass(found=True, source_component=source, target_component=target, path=path, hops=len(path) - 1)
        for nxt in adjacency.get(node, []):
            if nxt not in visited:
                visited.add(nxt)
                queue.append(path + [nxt])
    return _pass(found=False, source_component=source, target_component=target, path=[], hops=0)


def verify_runtime_dependency(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    dependency_id = str(payload.get("dependency_id") or "").strip()
    if not dependency_id:
        return _fail("dependency_id_required", "dependency_id is required")
    registered = next((x for x in _REPOSITORY.dependencies() if x["dependency_id"] == dependency_id), None)
    if registered is None:
        return _fail("runtime_dependency_not_found", "Dependency is not registered")
    current = discover_runtime_dependencies()
    if current.get("status") != "PASS":
        return current
    rebuilt = build_dependency_graph(current["publications"], current["capabilities"])
    published = next((x for x in rebuilt.get("dependencies", []) if x["dependency_id"] == dependency_id), None)
    checks = {
        "published_relation_present": published is not None,
        "metadata_match": published is not None and all(published.get(k) == registered.get(k) for k in ("dependency_id", "publisher", "source_component", "target_component")),
        "derived_data_only": set(registered) == {"graph_id", "node_id", "dependency_id", "publisher", "source_component", "target_component", "published_at", "last_verified_at", "graph_status"},
        "manual_dependency_creation_disabled": True,
    }
    return _pass(dependency_id=dependency_id, verified=all(checks.values()), checks=checks, dependency=registered)


def get_runtime_dependency_graph_status(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return initialize_runtime_dependency_graph()


def execute_runtime_dependency_graph_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    handlers = {
        "get_runtime_dependency_graph": get_runtime_dependency_graph,
        "search_runtime_dependencies": search_runtime_dependencies,
        "trace_runtime_dependency": trace_runtime_dependency,
        "verify_runtime_dependency": verify_runtime_dependency,
        "get_runtime_dependency_graph_status": get_runtime_dependency_graph_status,
    }
    handler = handlers.get(operation_type)
    if handler is None:
        return _fail("unsupported_runtime_dependency_graph_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload or {})


def _find_cycles(dependencies: list[dict[str, Any]]) -> list[list[str]]:
    adjacency: dict[str, list[str]] = {}
    for item in dependencies:
        adjacency.setdefault(item["source_component"], []).append(item["target_component"])
    for values in adjacency.values():
        values.sort()
    cycles: set[tuple[str, ...]] = set()
    visiting: list[str] = []
    visited: set[str] = set()

    def walk(node: str) -> None:
        if node in visiting:
            cycle = visiting[visiting.index(node):] + [node]
            body = cycle[:-1]
            rotations = [tuple(body[i:] + body[:i]) for i in range(len(body))]
            canonical = min(rotations) if rotations else tuple()
            cycles.add(canonical + (canonical[0],) if canonical else tuple())
            return
        if node in visited:
            return
        visiting.append(node)
        for nxt in adjacency.get(node, []):
            walk(nxt)
        visiting.pop()
        visited.add(node)

    for node in sorted(adjacency):
        walk(node)
    return [list(cycle) for cycle in sorted(cycles)]


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pass(**payload: Any) -> dict[str, Any]:
    return {"status": "PASS", **payload}


def _fail(code: str, message: str, **payload: Any) -> dict[str, Any]:
    return {"status": "FAIL", "failure_reason": code, "message": message, **payload}
