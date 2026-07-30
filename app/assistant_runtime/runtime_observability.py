from __future__ import annotations

import hashlib
import json
import os
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any, Callable

from app.assistant_runtime.runtime_supervisor import (
    get_runtime_diagnostics,
    get_runtime_events,
    get_runtime_health,
    initialize_runtime_supervisor,
)
from app.assistant_runtime.runtime_recovery import (
    get_runtime_recovery_history,
    initialize_runtime_recovery,
)
from app.assistant_runtime.runtime_capability_registry import (
    get_runtime_capabilities,
    initialize_runtime_capability_registry,
)
from app.assistant_runtime.runtime_dependency_graph import (
    get_runtime_dependency_graph,
    initialize_runtime_dependency_graph,
)

RELEASE_ID = "VECTRA-RUNTIME-OBSERVABILITY-001"
REPOSITORY_PATH = Path("runtime/runtime_observability/observations.json")
LOAD_ORDER = "AFTER_RUNTIME_DEPENDENCY_GRAPH"


class RuntimeObservabilityError(RuntimeError):
    pass


class ObservabilityRepository:
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
                if not isinstance(value, dict) or not isinstance(value.get("observations"), list):
                    raise RuntimeObservabilityError("observability_repository_invalid")
                self._data = value
            else:
                self._data = self._empty()
                self._persist()
            return deepcopy(self._data)

    def replace(self, observations: list[dict[str, Any]], repository_status: str) -> None:
        with self._lock:
            if self._data is None:
                self.load()
            assert self._data is not None
            now = _now()
            previous = self._data.get("observations", [])
            if previous != observations or self._data.get("repository_status") != repository_status:
                self._data.setdefault("aggregation_history", []).append({
                    "aggregated_at": now,
                    "repository_status": repository_status,
                    "observations_count": len(observations),
                })
            self._data["observations"] = deepcopy(observations)
            self._data["repository_status"] = repository_status
            self._data["updated_at"] = now
            self._persist()

    def observations(self) -> list[dict[str, Any]]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return deepcopy(self._data["observations"])

    def state(self) -> dict[str, Any]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return {
            "repository_id": self._data["repository_id"],
            "loaded": True,
            "repository_status": self._data.get("repository_status"),
            "observations_count": len(self._data["observations"]),
            "history_count": len(self._data.get("aggregation_history", [])),
            "derived_operational_metadata_only": True,
        }

    def _empty(self) -> dict[str, Any]:
        return {
            "repository_id": "VECTRA-RUNTIME-OBSERVABILITY-REPOSITORY-001",
            "release_id": RELEASE_ID,
            "schema_version": "1.0",
            "repository_status": "EMPTY",
            "observations": [],
            "aggregation_history": [],
            "updated_at": _now(),
        }

    def _persist(self) -> None:
        assert self._data is not None
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, self.path)


_REPOSITORY = ObservabilityRepository()


def initialize_runtime_observability(*, force: bool = False) -> dict[str, Any]:
    _REPOSITORY.load(force=force)
    prerequisites = _validate_prerequisites(force=force)
    if prerequisites.get("status") != "PASS":
        return prerequisites
    discovered = discover_published_observability_data()
    if discovered.get("status") != "PASS":
        return discovered
    observations = aggregate_published_observability_data(discovered["published_objects"])
    validation = validate_observations(observations)
    if validation.get("status") != "PASS":
        return validation
    _REPOSITORY.replace(observations, "READY")
    counts: dict[str, int] = {}
    for item in observations:
        counts[item["observation_type"]] = counts.get(item["observation_type"], 0) + 1
    return _pass(
        runtime_component="Runtime Observability",
        release_id=RELEASE_ID,
        loaded=True,
        load_order=LOAD_ORDER,
        observability_status="READY",
        observations_count=len(observations),
        observation_types=dict(sorted(counts.items())),
        own_events_published=False,
        unofficial_sources_used=False,
        source_data_modified=False,
        source_data_interpreted=False,
        normative_source=False,
        repository=_REPOSITORY.state(),
        evaluated_at=_now(),
    )


def _validate_prerequisites(*, force: bool = False) -> dict[str, Any]:
    checks = (
        ("Runtime Supervisor", initialize_runtime_supervisor),
        ("Runtime Recovery", initialize_runtime_recovery),
        ("Runtime Capability Registry", initialize_runtime_capability_registry),
        ("Runtime Dependency Graph", initialize_runtime_dependency_graph),
    )
    failures = []
    for name, initializer in checks:
        result = initializer(force=force)
        if result.get("status") != "PASS" or result.get("loaded") is not True:
            failures.append({"component": name, "status": result.get("status"), "failure_reason": result.get("failure_reason")})
    if failures:
        return _fail("runtime_observability_prerequisite_unavailable", "One or more approved Runtime sources are unavailable", failures=failures)
    return _pass()


def discover_published_observability_data() -> dict[str, Any]:
    published: list[dict[str, Any]] = []
    calls: tuple[tuple[str, str, Callable[[dict[str, Any]], dict[str, Any]], str], ...] = (
        ("Runtime Supervisor", "runtime_event", get_runtime_events, "events"),
        ("Runtime Supervisor", "runtime_state", get_runtime_health, "single"),
        ("Runtime Supervisor", "runtime_diagnostic", get_runtime_diagnostics, "single"),
        ("Runtime Recovery", "runtime_state", lambda _: initialize_runtime_recovery(), "single"),
        ("Runtime Recovery", "runtime_event", get_runtime_recovery_history, "executions"),
        ("Runtime Capability Registry", "runtime_capability", get_runtime_capabilities, "capabilities"),
        ("Runtime Dependency Graph", "runtime_dependency", get_runtime_dependency_graph, "dependencies"),
    )
    for component, observation_type, publisher, collection_key in calls:
        try:
            result = publisher({})
        except TypeError:
            result = publisher()
        if not isinstance(result, dict) or result.get("status") != "PASS":
            return _fail("runtime_observability_publication_unconfirmed", "An approved Runtime source did not publish PASS", source_component=component, observation_type=observation_type)
        objects = [result] if collection_key == "single" else result.get(collection_key, [])
        if not isinstance(objects, list):
            return _fail("runtime_observability_publication_invalid", "Published collection must be a list", source_component=component, observation_type=observation_type)
        for obj in objects:
            if not isinstance(obj, dict):
                return _fail("runtime_observability_object_invalid", "Published Runtime object must be an object", source_component=component)
            published.append({"source_component": component, "observation_type": observation_type, "object": deepcopy(obj)})
    published.sort(key=lambda x: (x["source_component"], x["observation_type"], _stable_json(x["object"])))
    return _pass(published_objects=published, published_objects_count=len(published))


def aggregate_published_observability_data(published_objects: list[dict[str, Any]]) -> list[dict[str, Any]]:
    aggregated_at = _now()
    observations = []
    for entry in published_objects:
        obj = entry["object"]
        component = entry["source_component"]
        observation_type = entry["observation_type"]
        source_object = _source_object_id(component, observation_type, obj)
        published_at = _published_at(obj)
        correlation_id = _correlation_id(obj, source_object)
        digest = hashlib.sha256(f"{component}|{observation_type}|{source_object}".encode("utf-8")).hexdigest()[:20]
        observations.append({
            "observation_id": f"OBS-{digest.upper()}",
            "source_component": component,
            "source_object": source_object,
            "published_at": published_at,
            "aggregated_at": aggregated_at,
            "observation_type": observation_type,
            "correlation_id": correlation_id,
            "repository_status": "AGGREGATED",
        })
    unique = {item["observation_id"]: item for item in observations}
    return sorted(unique.values(), key=lambda x: (x["source_component"], x["observation_type"], x["source_object"], x["observation_id"]))


def validate_observations(observations: Any) -> dict[str, Any]:
    allowed = {"observation_id", "source_component", "source_object", "published_at", "aggregated_at", "observation_type", "correlation_id", "repository_status"}
    if not isinstance(observations, list):
        return _fail("runtime_observations_invalid", "Observations must be a list")
    for item in observations:
        if not isinstance(item, dict) or set(item) != allowed:
            return _fail("runtime_observation_metadata_invalid", "Repository may contain only approved derived metadata")
        if not all(str(item.get(key) or "").strip() for key in allowed):
            return _fail("runtime_observation_metadata_incomplete", "Observation metadata is incomplete")
    return _pass(valid=True, observations_count=len(observations))


def get_runtime_observability_status(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return initialize_runtime_observability(force=bool((payload or {}).get("refresh")))


def get_runtime_observations(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    if bool(payload.get("refresh")) or not _REPOSITORY.observations():
        state = initialize_runtime_observability(force=bool(payload.get("refresh")))
        if state.get("status") != "PASS":
            return state
    observations = _REPOSITORY.observations()
    limit = _safe_limit(payload.get("limit"), 1000)
    return _pass(observations_count=min(len(observations), limit), observations=observations[:limit], repository=_REPOSITORY.state())


def search_runtime_observations(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    query = str(payload.get("query") or "").strip().lower()
    supported = {"source_component", "source_object", "observation_type", "correlation_id", "repository_status"}
    filters = {key: str(value) for key, value in payload.items() if key in supported and value not in (None, "")}
    results = []
    for item in _REPOSITORY.observations():
        searchable = " ".join(str(item.get(k) or "") for k in sorted(item)).lower()
        if query and query not in searchable:
            continue
        if any(str(item.get(key)) != value for key, value in filters.items()):
            continue
        results.append(item)
    results.sort(key=lambda x: (x["source_component"], x["observation_type"], x["source_object"], x["observation_id"]))
    return _pass(results_count=len(results), results=results, filters=filters)


def trace_runtime_observation(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    observation_id = str(payload.get("observation_id") or "").strip()
    correlation_id = str(payload.get("correlation_id") or "").strip()
    if not observation_id and not correlation_id:
        return _fail("runtime_observation_trace_selector_required", "observation_id or correlation_id is required")
    observations = _REPOSITORY.observations()
    selected = next((x for x in observations if x["observation_id"] == observation_id), None) if observation_id else None
    if observation_id and selected is None:
        return _fail("runtime_observation_not_found", "Observation is not aggregated")
    resolved_correlation = correlation_id or str(selected["correlation_id"])
    related = [x for x in observations if x["correlation_id"] == resolved_correlation]
    related.sort(key=lambda x: (x["source_component"], x["observation_type"], x["source_object"], x["observation_id"]))
    return _pass(correlation_id=resolved_correlation, observations_count=len(related), observations=related)


def verify_runtime_observation(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    observation_id = str(payload.get("observation_id") or "").strip()
    if not observation_id:
        return _fail("observation_id_required", "observation_id is required")
    registered = next((x for x in _REPOSITORY.observations() if x["observation_id"] == observation_id), None)
    if registered is None:
        return _fail("runtime_observation_not_found", "Observation is not aggregated")
    current = discover_published_observability_data()
    if current.get("status") != "PASS":
        return current
    rebuilt = aggregate_published_observability_data(current["published_objects"])
    published = next((x for x in rebuilt if x["observation_id"] == observation_id), None)
    stable = ("observation_id", "source_component", "source_object", "observation_type", "correlation_id", "repository_status")
    checks = {
        "published_object_present": published is not None,
        "metadata_match": published is not None and all(published.get(k) == registered.get(k) for k in stable),
        "metadata_only": set(registered) == {"observation_id", "source_component", "source_object", "published_at", "aggregated_at", "observation_type", "correlation_id", "repository_status"},
        "own_events_published": False,
        "unofficial_sources_used": False,
    }
    return _pass(observation_id=observation_id, verified=all(value is True for key, value in checks.items() if key not in {"own_events_published", "unofficial_sources_used"}) and not checks["own_events_published"] and not checks["unofficial_sources_used"], checks=checks, observation=registered)


def execute_runtime_observability_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    handlers = {
        "get_runtime_observability_status": get_runtime_observability_status,
        "get_runtime_observations": get_runtime_observations,
        "search_runtime_observations": search_runtime_observations,
        "trace_runtime_observation": trace_runtime_observation,
        "verify_runtime_observation": verify_runtime_observation,
    }
    handler = handlers.get(operation_type)
    if handler is None:
        return _fail("unsupported_runtime_observability_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload or {})


def _source_object_id(component: str, observation_type: str, obj: dict[str, Any]) -> str:
    keys = (
        "event_id", "recovery_execution_id", "capability_id", "dependency_id",
        "supervisor_evaluation_id", "graph_id", "repository_id", "release_id",
    )
    for key in keys:
        value = obj.get(key)
        if value not in (None, ""):
            return f"{key}:{value}"
    digest = hashlib.sha256(_stable_json(obj).encode("utf-8")).hexdigest()[:20]
    return f"published_object:{component}:{observation_type}:{digest}"


def _correlation_id(obj: dict[str, Any], source_object: str) -> str:
    for key in ("correlation_id", "supervisor_evaluation_id", "recovery_plan_id", "graph_id", "publisher", "runtime_component"):
        value = obj.get(key)
        if value not in (None, ""):
            return f"{key}:{value}"
    return source_object


def _published_at(obj: dict[str, Any]) -> str:
    for key in ("published_at", "evaluated_at", "completed_at", "started_at", "created_at", "timestamp", "last_verified_at"):
        value = obj.get(key)
        if value not in (None, ""):
            return str(value)
    return "NOT_PUBLISHED_BY_SOURCE"


def _stable_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _safe_limit(value: Any, default: int) -> int:
    try:
        return max(1, min(int(value), 1000))
    except (TypeError, ValueError):
        return default


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pass(**payload: Any) -> dict[str, Any]:
    return {"status": "PASS", **payload}


def _fail(code: str, message: str, **payload: Any) -> dict[str, Any]:
    return {"status": "FAIL", "failure_reason": code, "message": message, **payload}
