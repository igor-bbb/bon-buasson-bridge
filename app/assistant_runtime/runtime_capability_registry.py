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

RELEASE_ID = "VECTRA-RUNTIME-CAPABILITY-REGISTRY-001"
REPOSITORY_PATH = Path("runtime/runtime_capability_registry/capabilities.json")
LOAD_ORDER = "AFTER_RUNTIME_RECOVERY"


class RuntimeCapabilityRegistryError(RuntimeError):
    pass


Publisher = tuple[str, Callable[..., dict[str, Any]]]

# These are existing Runtime status publishers. The Registry does not accept
# external/manual registration and does not define capability implementations.
_PUBLISHERS: tuple[Publisher, ...] = (
    ("Architecture Registry Runtime", initialize_architecture_registry_runtime),
    ("Verification Runtime", initialize_verification_runtime),
    ("Execution Runtime", initialize_execution_runtime),
    ("Execution Orchestrator Runtime", initialize_execution_orchestrator),
    ("Session Runtime", initialize_session_runtime),
    ("Runtime Supervisor", initialize_runtime_supervisor),
    ("Runtime Recovery", initialize_runtime_recovery),
)


class CapabilityRegistryRepository:
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
                if not isinstance(value, dict) or not isinstance(value.get("capabilities"), list):
                    raise RuntimeCapabilityRegistryError("capability_registry_repository_invalid")
                self._data = value
            else:
                self._data = self._empty()
                self._persist()
            return deepcopy(self._data)

    def replace(self, capabilities: list[dict[str, Any]]) -> None:
        with self._lock:
            if self._data is None:
                self.load()
            assert self._data is not None
            previous = {item.get("capability_id"): item for item in self._data.get("capabilities", []) if isinstance(item, dict)}
            history = self._data.setdefault("registration_history", [])
            now = _now()
            for item in capabilities:
                prior = previous.get(item["capability_id"])
                if prior != item:
                    history.append({
                        "capability_id": item["capability_id"],
                        "publisher": item["publisher"],
                        "registered_at": now,
                        "registry_status": item["registry_status"],
                    })
            self._data["capabilities"] = deepcopy(capabilities)
            self._data["updated_at"] = now
            self._persist()

    def capabilities(self) -> list[dict[str, Any]]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return deepcopy(self._data["capabilities"])

    def state(self) -> dict[str, Any]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return {
            "repository_id": self._data["repository_id"],
            "loaded": True,
            "capabilities_count": len(self._data["capabilities"]),
            "history_count": len(self._data.get("registration_history", [])),
            "metadata_only": True,
        }

    def _empty(self) -> dict[str, Any]:
        return {
            "repository_id": "VECTRA-RUNTIME-CAPABILITY-REGISTRY-REPOSITORY-001",
            "release_id": RELEASE_ID,
            "schema_version": "1.0",
            "capabilities": [],
            "registration_history": [],
            "updated_at": _now(),
        }

    def _persist(self) -> None:
        assert self._data is not None
        temp = self.path.with_suffix(self.path.suffix + ".tmp")
        temp.write_text(json.dumps(self._data, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(temp, self.path)


_REPOSITORY = CapabilityRegistryRepository()


def initialize_runtime_capability_registry(*, force: bool = False) -> dict[str, Any]:
    _REPOSITORY.load(force=force)
    discovery = discover_runtime_capabilities()
    if discovery.get("status") != "PASS":
        return discovery
    _REPOSITORY.replace(discovery["capabilities"])
    return _pass(
        runtime_component="Runtime Capability Registry",
        release_id=RELEASE_ID,
        loaded=True,
        load_order=LOAD_ORDER,
        registry_status="READY",
        publishers_count=len(_PUBLISHERS),
        capabilities_count=len(discovery["capabilities"]),
        manual_registration_supported=False,
        normative_source=False,
        repository=_REPOSITORY.state(),
        evaluated_at=_now(),
    )


def discover_runtime_capabilities() -> dict[str, Any]:
    capabilities: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for publisher_name, publisher in _PUBLISHERS:
        try:
            publication = publisher(force=False)
        except TypeError:
            publication = publisher()
        except Exception as exc:  # defensive boundary around publisher interfaces
            rejected.append({"publisher": publisher_name, "reason": "publisher_unavailable", "detail": str(exc)})
            continue
        validation = validate_capability_publication(publisher_name, publication)
        if validation.get("status") != "PASS":
            rejected.append({"publisher": publisher_name, "reason": validation.get("failure_reason"), "detail": validation.get("message")})
            continue
        capabilities.append(_metadata_from_publication(publisher_name, publication))
    capabilities.sort(key=lambda item: (item["capability_id"], item["version"], item["publisher"]))
    if rejected:
        return _fail("runtime_capability_publication_rejected", "One or more required Runtime publishers did not provide a confirmed publication", rejected_publications=sorted(rejected, key=lambda item: item["publisher"]))
    return _pass(capabilities=capabilities, capabilities_count=len(capabilities), rejected_publications=[])


def validate_capability_publication(publisher_name: str, publication: Any) -> dict[str, Any]:
    if not isinstance(publication, dict):
        return _fail("capability_publication_invalid", "Publisher response must be an object")
    if publication.get("status") != "PASS":
        return _fail("capability_publication_unconfirmed", "Publisher did not return PASS")
    if publication.get("loaded") is not True:
        return _fail("capability_publisher_not_loaded", "Publisher is not loaded")
    runtime_component = str(publication.get("runtime_component") or publisher_name).strip()
    if runtime_component != publisher_name:
        return _fail("capability_publisher_identity_mismatch", "Published Runtime component identity does not match the approved publisher")
    version = str(publication.get("release_id") or publication.get("registry_version") or publication.get("contract_version") or "").strip()
    if not version:
        return _fail("capability_publication_version_missing", "Publisher did not publish a version or release identifier")
    return _pass(publisher=publisher_name, publication_status="PUBLISHED", version=version)


def get_runtime_capabilities(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    refresh = bool(payload.get("refresh"))
    if refresh or not _REPOSITORY.capabilities():
        initialized = initialize_runtime_capability_registry(force=refresh)
        if initialized.get("status") != "PASS":
            return initialized
    items = _REPOSITORY.capabilities()
    return _pass(capabilities_count=len(items), capabilities=items, repository=_REPOSITORY.state())


def get_runtime_capability(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    capability_id = str(payload.get("capability_id") or "").strip()
    if not capability_id:
        return _fail("capability_id_required", "capability_id is required")
    item = next((entry for entry in _REPOSITORY.capabilities() if entry.get("capability_id") == capability_id), None)
    if item is None:
        return _fail("runtime_capability_not_found", f"Runtime capability {capability_id} is not registered")
    return _pass(capability=item)


def search_runtime_capabilities(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    query = str(payload.get("query") or "").strip().lower()
    publisher = str(payload.get("publisher") or "").strip()
    runtime_component = str(payload.get("runtime_component") or "").strip()
    publication_status = str(payload.get("publication_status") or "").strip()
    registry_status = str(payload.get("registry_status") or "").strip()
    results = []
    for item in _REPOSITORY.capabilities():
        searchable = " ".join(str(item.get(key) or "") for key in ("capability_id", "publisher", "runtime_component", "version")).lower()
        if query and query not in searchable:
            continue
        if publisher and item.get("publisher") != publisher:
            continue
        if runtime_component and item.get("runtime_component") != runtime_component:
            continue
        if publication_status and item.get("publication_status") != publication_status:
            continue
        if registry_status and item.get("registry_status") != registry_status:
            continue
        results.append(item)
    results.sort(key=lambda item: (item["capability_id"], item["version"], item["publisher"]))
    return _pass(results_count=len(results), results=results)


def verify_runtime_capability(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    capability_id = str(payload.get("capability_id") or "").strip()
    if not capability_id:
        return _fail("capability_id_required", "capability_id is required")
    current = discover_runtime_capabilities()
    if current.get("status") != "PASS":
        return current
    published = next((item for item in current["capabilities"] if item["capability_id"] == capability_id), None)
    registered = next((item for item in _REPOSITORY.capabilities() if item["capability_id"] == capability_id), None)
    if published is None or registered is None:
        return _fail("runtime_capability_unconfirmed", "Capability is not both published and registered")
    stable_fields = ("capability_id", "version", "publisher", "publication_status", "runtime_component", "registry_status")
    checks = {
        "publisher_confirmed": published["publication_status"] == "PUBLISHED",
        "metadata_match": all(published.get(key) == registered.get(key) for key in stable_fields),
        "metadata_only": set(registered) == {"capability_id", "version", "publisher", "publication_status", "runtime_component", "published_at", "last_verified_at", "registry_status"},
        "manual_registration_disabled": True,
    }
    return _pass(capability_id=capability_id, verified=all(checks.values()), checks=checks, capability=registered)


def get_runtime_capability_registry_status(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    return initialize_runtime_capability_registry()


def execute_runtime_capability_registry_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    operations = {
        "get_runtime_capabilities": get_runtime_capabilities,
        "get_runtime_capability": get_runtime_capability,
        "search_runtime_capabilities": search_runtime_capabilities,
        "verify_runtime_capability": verify_runtime_capability,
        "get_runtime_capability_registry_status": get_runtime_capability_registry_status,
    }
    handler = operations.get(operation_type)
    if handler is None:
        return _fail("unsupported_runtime_capability_registry_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload or {})


def _metadata_from_publication(publisher_name: str, publication: dict[str, Any]) -> dict[str, Any]:
    now = _now()
    version = str(publication.get("release_id") or publication.get("registry_version") or publication.get("contract_version"))
    capability_id = "runtime." + re.sub(r"[^a-z0-9]+", ".", publisher_name.lower()).strip(".")
    published_at = str(publication.get("evaluated_at") or now)
    return {
        "capability_id": capability_id,
        "version": version,
        "publisher": publisher_name,
        "publication_status": "PUBLISHED",
        "runtime_component": publisher_name,
        "published_at": published_at,
        "last_verified_at": now,
        "registry_status": "REGISTERED",
    }


def _now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _pass(**payload: Any) -> dict[str, Any]:
    return {"status": "PASS", **payload}


def _fail(code: str, message: str, **payload: Any) -> dict[str, Any]:
    return {"status": "FAIL", "failure_reason": code, "message": message, **payload}
