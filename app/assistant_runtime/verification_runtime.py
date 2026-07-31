from __future__ import annotations

from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from typing import Any
from uuid import uuid4

from app.assistant_runtime.architecture_registry_runtime import (
    evaluate_registry_compliance,
    get_architecture_object,
    initialize_architecture_registry_runtime,
    list_architecture_objects,
    verify_architecture_object,
)
from app.assistant_runtime.repository_persistence import (
    get_persistence_status,
    read_repository_json,
    write_repository_json,
)

RELEASE_ID = "VECTRA-VERIFICATION-RUNTIME-001"
RESULTS_PATH = Path("runtime/verification_runtime/verification_results.json")


class VerificationRuntimeError(RuntimeError):
    """Controlled Verification Runtime error."""


class VerificationRepository:
    def __init__(self, path: Path = RESULTS_PATH) -> None:
        self.path = path
        self._lock = RLock()
        self._data: dict[str, Any] | None = None

    def load(self, *, force: bool = False) -> dict[str, Any]:
        with self._lock:
            if self._data is not None and not force:
                return deepcopy(self._data)
            self.path.parent.mkdir(parents=True, exist_ok=True)
            missing = {"__vectra_verification_repository_missing__": True}
            stored = read_repository_json(self.path, missing)
            if stored == missing:
                self._data = {
                    "repository_id": "VECTRA-VERIFICATION-REPOSITORY-001",
                    "release_id": RELEASE_ID,
                    "schema_version": "2.0",
                    "results": [],
                    "updated_at": _now(),
                }
                self._persist()
            else:
                self._data = stored
                self._validate(self._data)
            return deepcopy(self._data)

    def save_result(self, result: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            data = self._require_loaded()
            data["results"].append(deepcopy(result))
            data["updated_at"] = _now()
            self._persist()
            return deepcopy(result)

    def results(self) -> list[dict[str, Any]]:
        return deepcopy(self._require_loaded()["results"])

    def latest_for_object(self, object_id: str) -> dict[str, Any] | None:
        matches = [r for r in self.results() if r.get("object_id") == object_id]
        return matches[-1] if matches else None

    def get(self, execution_id: str) -> dict[str, Any] | None:
        for result in self.results():
            if result.get("execution_id") == execution_id:
                return result
        return None

    def state(self) -> dict[str, Any]:
        data = self._require_loaded()
        results = data["results"]
        return {
            "repository_id": data["repository_id"],
            "results_count": len(results),
            "last_execution_id": results[-1]["execution_id"] if results else None,
            "loaded": True,
        }

    def _require_loaded(self) -> dict[str, Any]:
        if self._data is None:
            self.load()
        assert self._data is not None
        return self._data

    def _persist(self) -> None:
        assert self._data is not None
        persistence = write_repository_json(self.path, self._data)
        if persistence.get("status") != "PASS":
            raise VerificationRuntimeError("verification_result_persistence_failed")

    @staticmethod
    def _validate(data: dict[str, Any]) -> None:
        if not isinstance(data.get("results"), list):
            raise VerificationRuntimeError("verification_results_invalid")


_REPOSITORY = VerificationRepository()


def initialize_verification_runtime(*, force: bool = False) -> dict[str, Any]:
    registry = initialize_architecture_registry_runtime(force=force)
    repository = _REPOSITORY.load(force=force)
    persistence = get_persistence_status()
    return {
        "status": "PASS",
        "verification_status": "PASS",
        "runtime_component": "Verification Runtime",
        "release_id": RELEASE_ID,
        "loaded": True,
        "architecture_registry_loaded": registry.get("loaded") is True,
        "architecture_registry_id": registry.get("registry_id"),
        "verification_repository_id": repository["repository_id"],
        "results_count": len(repository["results"]),
        "persistence_backend": persistence.get("backend"),
        "source_of_truth": persistence.get("source_of_truth"),
        "durable_across_deploys": persistence.get("durable_across_deploys") is True,
        "error": None,
    }


def record_product_verification(payload: dict[str, Any]) -> dict[str, Any]:
    """Persist one final Product Verification result with durable readback.

    The function is intentionally internal: final verification producers call it
    automatically, while the existing public facade remains read-only for result
    inspection. Repeated collection of the same deployment result is idempotent.
    """
    release_id = str(payload.get("release_id") or "").strip()
    verdict = str(payload.get("verdict") or payload.get("verification_status") or "").strip().upper()
    if not release_id:
        return _fail("release_id_required", "release_id is required")
    if verdict not in {"PASS", "FAIL", "BLOCKED"}:
        return _fail("verification_verdict_invalid", "verdict must be PASS, FAIL, or BLOCKED")

    deployment_version = str(payload.get("deployment_version") or "").strip() or None
    verification_source = str(payload.get("verification_source") or "Product Verification").strip()
    idempotency_key = str(payload.get("idempotency_key") or "").strip() or "|".join(
        [release_id, deployment_version or "unknown-deployment", verification_source, verdict]
    )
    for existing in _REPOSITORY.results():
        if existing.get("execution_type") == "PRODUCT_VERIFICATION" and existing.get("idempotency_key") == idempotency_key:
            return _pass(
                execution_id=existing.get("execution_id"),
                release_id=existing.get("release_id"),
                verdict=existing.get("verdict"),
                persisted=True,
                duplicate=True,
                readback_status="PASS",
            )

    execution_id = str(payload.get("execution_id") or f"PV-{uuid4().hex[:12].upper()}")
    evidence = deepcopy(payload.get("evidence") if payload.get("evidence") is not None else {})
    result = {
        "execution_id": execution_id,
        "execution_type": "PRODUCT_VERIFICATION",
        "release_id": release_id,
        "status": verdict,
        "verification_status": verdict,
        "verdict": verdict,
        "evidence": evidence,
        "verification_evidence": deepcopy(evidence),
        "verification_source": verification_source,
        "runtime_source": str(payload.get("runtime_source") or verification_source),
        "deployment_version": deployment_version,
        "deployment_time": payload.get("deployment_time"),
        "timestamp": str(payload.get("timestamp") or _now()),
        "idempotency_key": idempotency_key,
        "failure_reason": payload.get("failure_reason"),
        "error": deepcopy(payload.get("error")),
    }
    _REPOSITORY.save_result(result)
    readback = _REPOSITORY.get(execution_id)
    if not isinstance(readback, dict) or readback.get("execution_id") != execution_id:
        return _fail("verification_result_readback_failed", "Product Verification result readback failed")
    return _pass(
        execution_id=execution_id,
        release_id=release_id,
        verdict=verdict,
        persisted=True,
        duplicate=False,
        readback_status="PASS",
    )


def verify_runtime_object(payload: dict[str, Any]) -> dict[str, Any]:
    object_id = str(payload.get("object_id") or "").strip()
    if not object_id:
        return _fail("object_id_required", "object_id is required")

    object_response = get_architecture_object({"object_id": object_id})
    if object_response.get("status") != "PASS":
        return object_response

    # The mapping is obtained exclusively through the Architecture Registry API.
    registry_projection = verify_architecture_object({"object_id": object_id})
    mapping = deepcopy(registry_projection.get("verification_mapping") or {})
    evidence = deepcopy(registry_projection.get("verification_evidence") or [])
    checks = _execute_mapping(mapping)
    status = _aggregate_status(checks)
    execution_id = str(payload.get("execution_id") or f"VER-{uuid4().hex[:12].upper()}")
    result = {
        "execution_id": execution_id,
        "execution_type": "OBJECT_VERIFICATION",
        "object_id": object_id,
        "status": status,
        "verification_status": status,
        "verification_mapping": mapping,
        "verification_evidence": evidence + _runtime_evidence(checks),
        "runtime_source": "ArchitectureRegistryRuntime.verify_architecture_object",
        "timestamp": _now(),
        "checks": checks,
        "aggregation": _aggregate(checks),
        "error": None if status == "PASS" else {
            "code": "runtime_object_verification_failed",
            "message": "One or more Verification Mapping checks failed.",
        },
    }
    _REPOSITORY.save_result(result)
    return deepcopy(result)


def verify_registry(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    listed = list_architecture_objects({})
    object_ids = [item["object_id"] for item in listed.get("objects", [])]
    object_results = [verify_runtime_object({"object_id": object_id}) for object_id in object_ids]
    counts = _aggregate([{"status": item.get("status", "FAIL")} for item in object_results])
    registry_compliance = evaluate_registry_compliance({})
    status = "PASS" if counts["fail_count"] == 0 and registry_compliance.get("status") == "PASS" else "FAIL"
    execution_id = str(payload.get("execution_id") or f"VER-REG-{uuid4().hex[:10].upper()}")
    result = {
        "execution_id": execution_id,
        "execution_type": "REGISTRY_VERIFICATION",
        "registry_id": initialize_architecture_registry_runtime().get("registry_id"),
        "status": status,
        "verification_status": status,
        "timestamp": _now(),
        "runtime_source": "ArchitectureRegistryRuntime",
        "objects_count": len(object_results),
        "object_results": [
            {
                "object_id": item.get("object_id"),
                "execution_id": item.get("execution_id"),
                "verification_status": item.get("verification_status"),
            }
            for item in object_results
        ],
        "aggregation": counts,
        "verification_evidence": [{
            "type": "registry_compliance",
            "value": registry_compliance.get("status"),
        }],
        "compliance_integration": {
            "status": registry_compliance.get("status"),
            "computed": registry_compliance.get("computed") is True,
        },
        "error": None if status == "PASS" else {
            "code": "registry_verification_failed",
            "message": "Registry Verification or Compliance failed.",
        },
    }
    _REPOSITORY.save_result(result)
    return deepcopy(result)


def get_verification_status(payload: dict[str, Any]) -> dict[str, Any]:
    execution_id = str(payload.get("execution_id") or "").strip()
    object_id = str(payload.get("object_id") or "").strip()
    result = _REPOSITORY.get(execution_id) if execution_id else _REPOSITORY.latest_for_object(object_id) if object_id else None
    if result is None:
        return _fail("verification_result_not_found", "Verification result was not found")
    return _pass(
        execution_id=result["execution_id"],
        execution_type=result["execution_type"],
        object_id=result.get("object_id"),
        registry_id=result.get("registry_id"),
        verification_status=result["verification_status"],
        timestamp=result["timestamp"],
        aggregation=deepcopy(result.get("aggregation")),
    )


def list_verification_results(payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    results = _filter_results(_REPOSITORY.results(), payload)
    limit = max(1, min(int(payload.get("limit") or 100), 500))
    results = results[-limit:]
    return _pass(results_count=len(results), results=[_summary(item) for item in results])


def search_verification_results(payload: dict[str, Any]) -> dict[str, Any]:
    criteria = {key: value for key, value in payload.items() if key in {
        "execution_id", "execution_type", "object_id", "status", "runtime_source"
    } and value not in (None, "")}
    if not criteria:
        return _fail("search_criteria_required", "At least one verification search criterion is required")
    results = _filter_results(_REPOSITORY.results(), criteria)
    return _pass(results_count=len(results), results=[_summary(item) for item in results], filters=criteria)


def get_verification_evidence(payload: dict[str, Any]) -> dict[str, Any]:
    execution_id = str(payload.get("execution_id") or "").strip()
    if not execution_id:
        return _fail("execution_id_required", "execution_id is required")
    result = _REPOSITORY.get(execution_id)
    if result is None:
        return _fail("verification_result_not_found", "Verification result was not found")
    evidence = deepcopy(result.get("verification_evidence") if result.get("verification_evidence") is not None else [])
    return _pass(
        execution_id=execution_id,
        object_id=result.get("object_id"),
        release_id=result.get("release_id"),
        verdict=result.get("verdict"),
        verification_status=result.get("verification_status"),
        evidence_count=len(evidence),
        verification_mapping=deepcopy(result.get("verification_mapping")),
        verification_evidence=evidence,
        runtime_source=result.get("runtime_source"),
        timestamp=result.get("timestamp"),
    )


def execute_verification_runtime_operation(operation_type: str, payload: dict[str, Any] | None = None) -> dict[str, Any]:
    payload = payload or {}
    operations = {
        "get_verification_runtime_status": lambda _: initialize_verification_runtime(),
        "get_verification_status": get_verification_status,
        "list_verification_results": list_verification_results,
        "verify_runtime_object": verify_runtime_object,
        "verify_registry": verify_registry,
        "get_verification_evidence": get_verification_evidence,
        "search_verification_results": search_verification_results,
    }
    handler = operations.get(operation_type)
    if handler is None:
        return _fail("unsupported_verification_runtime_operation", f"Unsupported operation_type: {operation_type}")
    return handler(payload)


def get_latest_runtime_verification(object_id: str) -> dict[str, Any] | None:
    return _REPOSITORY.latest_for_object(object_id)


def _execute_mapping(mapping: dict[str, Any]) -> list[dict[str, Any]]:
    tests = mapping.get("tests") or []
    checks: list[dict[str, Any]] = []
    if not tests:
        return [{"check_type": "verification_mapping", "target": None, "status": "FAIL", "reason": "tests_required"}]
    for path_value in tests:
        exists = Path(path_value).exists()
        checks.append({
            "check_type": "test_path_exists",
            "target": path_value,
            "status": "PASS" if exists else "FAIL",
            "reason": None if exists else "test_path_not_found",
        })
    checks.append({
        "check_type": "mapping_status_published",
        "target": mapping.get("status"),
        "status": "PASS" if mapping.get("status") else "FAIL",
        "reason": None if mapping.get("status") else "mapping_status_required",
    })
    return checks


def _runtime_evidence(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [{"type": "runtime_verification_check", "value": deepcopy(check)} for check in checks]


def _aggregate_status(checks: list[dict[str, Any]]) -> str:
    return "FAIL" if any(item.get("status") == "FAIL" for item in checks) else "WARNING" if any(item.get("status") == "WARNING" for item in checks) else "PASS"


def _aggregate(checks: list[dict[str, Any]]) -> dict[str, Any]:
    pass_count = sum(item.get("status") == "PASS" for item in checks)
    fail_count = sum(item.get("status") == "FAIL" for item in checks)
    warning_count = sum(item.get("status") == "WARNING" for item in checks)
    return {
        "total_count": len(checks),
        "pass_count": pass_count,
        "fail_count": fail_count,
        "warning_count": warning_count,
        "status": "FAIL" if fail_count else "WARNING" if warning_count else "PASS",
    }


def _filter_results(results: list[dict[str, Any]], filters: dict[str, Any]) -> list[dict[str, Any]]:
    selected = []
    for item in results:
        if all(str(value).casefold() in str(item.get(key, "")).casefold() for key, value in filters.items() if key != "limit"):
            selected.append(item)
    return selected


def _summary(item: dict[str, Any]) -> dict[str, Any]:
    summary = {
        "execution_id": item.get("execution_id"),
        "execution_type": item.get("execution_type"),
        "object_id": item.get("object_id"),
        "registry_id": item.get("registry_id"),
        "verification_status": item.get("verification_status"),
        "timestamp": item.get("timestamp"),
        "aggregation": deepcopy(item.get("aggregation")),
    }
    if item.get("execution_type") == "PRODUCT_VERIFICATION":
        summary.update({
            "release_id": item.get("release_id"),
            "verdict": item.get("verdict"),
            "evidence": deepcopy(item.get("evidence")),
            "verification_source": item.get("verification_source"),
            "deployment_version": item.get("deployment_version"),
            "failure_reason": item.get("failure_reason"),
        })
    return summary


def _pass(**kwargs: Any) -> dict[str, Any]:
    return {"status": "PASS", "verification_status": "PASS", **kwargs, "error": None}


def _fail(code: str, message: str) -> dict[str, Any]:
    return {
        "status": "FAIL",
        "verification_status": "FAIL",
        "error": {"code": code, "message": message},
    }


def _now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
