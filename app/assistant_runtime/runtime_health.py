from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any

from app.assistant_runtime.runtime_supervisor import get_runtime_health as get_supervisor_health, initialize_runtime_supervisor
from app.assistant_runtime.runtime_recovery import initialize_runtime_recovery
from app.assistant_runtime.runtime_capability_registry import get_runtime_capabilities, initialize_runtime_capability_registry
from app.assistant_runtime.runtime_dependency_graph import get_runtime_dependency_graph, initialize_runtime_dependency_graph
from app.assistant_runtime.runtime_observability import get_runtime_observations, initialize_runtime_observability

RELEASE_ID = "VECTRA-RUNTIME-HEALTH-001"
REPOSITORY_PATH = Path("runtime/runtime_health/health_snapshots.json")
LOAD_ORDER = "AFTER_RUNTIME_OBSERVABILITY"
ALLOWED_FIELDS = {
    "health_snapshot_id", "health_status", "source_components", "source_objects",
    "generated_at", "trace_id", "repository_status",
}


class RuntimeHealthError(RuntimeError):
    pass


class HealthRepository:
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
                data = json.loads(self.path.read_text(encoding="utf-8"))
                if not isinstance(data, dict) or not isinstance(data.get("snapshots"), list):
                    raise RuntimeHealthError("health_repository_invalid")
                self._data = data
            else:
                self._data = self._empty()
                self._persist()
            return deepcopy(self._data)

    def append(self, snapshot: dict[str, Any]) -> None:
        if set(snapshot) != ALLOWED_FIELDS:
            raise RuntimeHealthError("health_snapshot_contains_unapproved_fields")
        with self._lock:
            if self._data is None:
                self.load()
            assert self._data is not None
            current = self._data["snapshots"]
            if not current or current[-1] != snapshot:
                current.append(deepcopy(snapshot))
            self._data["repository_status"] = "READY"
            self._data["updated_at"] = _now()
            self._persist()

    def snapshots(self) -> list[dict[str, Any]]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return deepcopy(self._data["snapshots"])

    def state(self) -> dict[str, Any]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return {
            "repository_id": self._data["repository_id"],
            "loaded": True,
            "repository_status": self._data.get("repository_status"),
            "snapshots_count": len(self._data["snapshots"]),
            "derived_operational_metadata_only": True,
        }

    def _empty(self) -> dict[str, Any]:
        return {
            "repository_id": "VECTRA-RUNTIME-HEALTH-REPOSITORY-001",
            "release_id": RELEASE_ID,
            "schema_version": "1.0",
            "repository_status": "EMPTY",
            "snapshots": [],
            "updated_at": _now(),
        }

    def _persist(self) -> None:
        assert self._data is not None
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, self.path)


_REPOSITORY = HealthRepository()


def initialize_runtime_health(*, force: bool = False) -> dict[str, Any]:
    _REPOSITORY.load(force=force)
    prerequisites = _validate_prerequisites(force=force)
    if prerequisites.get("status") != "PASS":
        return prerequisites
    factors = collect_published_health_factors()
    if factors.get("status") != "PASS":
        return factors
    snapshot = evaluate_derived_health(factors)
    validation = validate_health_snapshot(snapshot)
    if validation.get("status") != "PASS":
        return validation
    _REPOSITORY.append(snapshot)
    return _pass(
        runtime_component="Runtime Health",
        release_id=RELEASE_ID,
        loaded=True,
        load_order=LOAD_ORDER,
        health_status=snapshot["health_status"],
        connection_status="CONNECTED",
        runtime_observability_loaded=True,
        health_snapshot_id=snapshot["health_snapshot_id"],
        source_components=snapshot["source_components"],
        source_objects_count=len(snapshot["source_objects"]),
        own_normative_criteria=False,
        published_statuses_overridden=False,
        supervisor_decision_replaced=False,
        normative_source=False,
        repository=_REPOSITORY.state(),
        evaluated_at=snapshot["generated_at"],
    )


def _validate_prerequisites(*, force: bool = False) -> dict[str, Any]:
    checks = (
        ("Runtime Supervisor", initialize_runtime_supervisor),
        ("Runtime Recovery", initialize_runtime_recovery),
        ("Runtime Capability Registry", initialize_runtime_capability_registry),
        ("Runtime Dependency Graph", initialize_runtime_dependency_graph),
        ("Runtime Observability", initialize_runtime_observability),
    )
    failures = []
    for component, initializer in checks:
        result = initializer(force=force)
        if result.get("status") != "PASS" or result.get("loaded") is not True:
            failures.append({"component": component, "status": result.get("status"), "failure_reason": result.get("failure_reason")})
    if failures:
        return _fail("runtime_health_prerequisite_unavailable", "One or more approved Runtime sources are unavailable", failures=failures)
    return _pass()


def collect_published_health_factors() -> dict[str, Any]:
    supervisor = get_supervisor_health({})
    recovery = initialize_runtime_recovery()
    capabilities = get_runtime_capabilities({})
    dependencies = get_runtime_dependency_graph({})
    observations = get_runtime_observations({"limit": 1000})
    publications = {
        "Runtime Supervisor": supervisor,
        "Runtime Recovery": recovery,
        "Runtime Capability Registry": capabilities,
        "Runtime Dependency Graph": dependencies,
        "Runtime Observability": observations,
    }
    for component, result in publications.items():
        if not isinstance(result, dict) or result.get("status") != "PASS":
            return _fail("runtime_health_publication_unconfirmed", "Approved Runtime source did not publish PASS", source_component=component)
    source_objects = [
        f"supervisor_health:{supervisor.get('runtime_health', 'NOT_PUBLISHED')}",
        f"runtime_recovery:{recovery.get('release_id', 'NOT_PUBLISHED')}",
        f"runtime_capabilities:{capabilities.get('capabilities_count', len(capabilities.get('capabilities', [])))}",
        f"runtime_dependencies:{dependencies.get('dependencies_count', len(dependencies.get('dependencies', [])))}",
        f"runtime_observations:{observations.get('observations_count', len(observations.get('observations', [])))}",
    ]
    return _pass(
        supervisor_health_status=supervisor.get("runtime_health"),
        source_components=sorted(publications),
        source_objects=sorted(source_objects),
        publications=publications,
    )


def evaluate_derived_health(factors: dict[str, Any]) -> dict[str, Any]:
    # The derived value is a transparent projection of the normative Supervisor
    # publication. No threshold, weighting or replacement status is introduced.
    health_status = str(factors.get("supervisor_health_status") or "NOT_PUBLISHED")
    components = sorted(str(x) for x in factors.get("source_components", []))
    objects = sorted(str(x) for x in factors.get("source_objects", []))
    trace_basis = json.dumps({"health_status": health_status, "source_components": components, "source_objects": objects}, sort_keys=True, separators=(",", ":"))
    digest = hashlib.sha256(trace_basis.encode("utf-8")).hexdigest()[:20].upper()
    now = _now()
    return {
        "health_snapshot_id": f"HEALTH-{digest}",
        "health_status": health_status,
        "source_components": components,
        "source_objects": objects,
        "generated_at": now,
        "trace_id": f"TRACE-{digest}",
        "repository_status": "DERIVED",
    }


def validate_health_snapshot(snapshot: Any) -> dict[str, Any]:
    if not isinstance(snapshot, dict) or set(snapshot) != ALLOWED_FIELDS:
        return _fail("runtime_health_snapshot_invalid", "Health snapshot must contain only approved derived metadata")
    if not snapshot["health_status"] or not snapshot["source_components"] or not snapshot["source_objects"]:
        return _fail("runtime_health_snapshot_incomplete", "Health snapshot metadata is incomplete")
    return _pass(valid=True)


def get_runtime_health_status(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return initialize_runtime_health(force=bool((payload or {}).get("refresh")))


def get_runtime_health(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    if bool(payload.get("refresh")) or not _REPOSITORY.snapshots():
        state = initialize_runtime_health(force=bool(payload.get("refresh")))
        if state.get("status") != "PASS":
            return state
    snapshots = _REPOSITORY.snapshots()
    return _pass(health=snapshots[-1], repository=_REPOSITORY.state())


def search_runtime_health(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    query = str(payload.get("query") or "").strip().lower()
    status_filter = str(payload.get("health_status") or "").strip()
    results = []
    for snapshot in _REPOSITORY.snapshots():
        if status_filter and snapshot["health_status"] != status_filter:
            continue
        if query and query not in json.dumps(snapshot, ensure_ascii=False, sort_keys=True).lower():
            continue
        results.append(snapshot)
    results.sort(key=lambda x: (x["generated_at"], x["health_snapshot_id"]))
    return _pass(results_count=len(results), results=results)


def trace_runtime_health(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    snapshot_id = str(payload.get("health_snapshot_id") or "").strip()
    trace_id = str(payload.get("trace_id") or "").strip()
    if not snapshot_id and not trace_id:
        return _fail("runtime_health_trace_selector_required", "health_snapshot_id or trace_id is required")
    snapshot = next((x for x in _REPOSITORY.snapshots() if x["health_snapshot_id"] == snapshot_id or x["trace_id"] == trace_id), None)
    if snapshot is None:
        return _fail("runtime_health_snapshot_not_found", "Health snapshot is not available")
    return _pass(
        health_snapshot_id=snapshot["health_snapshot_id"],
        trace_id=snapshot["trace_id"],
        health_status=snapshot["health_status"],
        source_components=snapshot["source_components"],
        source_objects=snapshot["source_objects"],
    )


def verify_runtime_health(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    snapshot_id = str(payload.get("health_snapshot_id") or "").strip()
    snapshot = next((x for x in _REPOSITORY.snapshots() if x["health_snapshot_id"] == snapshot_id), None)
    if snapshot is None:
        return _fail("runtime_health_snapshot_not_found", "Health snapshot is not available")
    current = collect_published_health_factors()
    if current.get("status") != "PASS":
        return current
    rebuilt = evaluate_derived_health(current)
    checks = {
        "supervisor_status_projected_without_override": rebuilt["health_status"] == snapshot["health_status"],
        "source_components_match": rebuilt["source_components"] == snapshot["source_components"],
        "source_objects_match": rebuilt["source_objects"] == snapshot["source_objects"],
        "metadata_only": set(snapshot) == ALLOWED_FIELDS,
        "own_normative_criteria": False,
        "published_statuses_overridden": False,
        "supervisor_decision_replaced": False,
    }
    verified = all(checks[k] for k in ("supervisor_status_projected_without_override", "source_components_match", "source_objects_match", "metadata_only"))
    return _pass(health_snapshot_id=snapshot_id, verified=verified, checks=checks, health=snapshot)


def execute_runtime_health_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    handlers = {
        "get_runtime_health_status": get_runtime_health_status,
        "get_runtime_health": get_runtime_health,
        "search_runtime_health": search_runtime_health,
        "trace_runtime_health": trace_runtime_health,
        "verify_runtime_health": verify_runtime_health,
    }
    handler = handlers.get(operation_type)
    if handler is None:
        return _fail("unsupported_runtime_health_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload or {})


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pass(**payload: Any) -> dict[str, Any]:
    return {"status": "PASS", **payload}


def _fail(code: str, message: str, **payload: Any) -> dict[str, Any]:
    return {"status": "FAIL", "failure_reason": code, "message": message, **payload}
