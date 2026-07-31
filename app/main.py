import os
import threading
from datetime import datetime, timezone
from copy import deepcopy

from fastapi import FastAPI

from app.api.routes import router, _laboratory_full_openapi_schema

PUBLIC_RUNTIME_URL = (
    os.getenv("VECTRA_PUBLIC_RUNTIME_URL")
    or os.getenv("VECTRA_RUNTIME_URL")
    or os.getenv("RENDER_EXTERNAL_URL")
    or "https://bon-buasson-api.onrender.com"
)

app = FastAPI(
    title="VECTRA CORE v2 MVP",
    servers=[{"url": PUBLIC_RUNTIME_URL}],
)
app.include_router(router)


def _vectra_action_openapi_schema() -> dict:
    """Return the official GPT Actions OpenAPI at the standard /openapi.json URL.

    FastAPI's default schema exposes the whole internal Runtime surface. For Product
    Owner deployment and GPT Actions import, /openapi.json must be a compact,
    import-safe Action schema with a valid production servers.url. The detailed
    Laboratory OpenAPI remains available through its dedicated endpoints.
    """
    schema = deepcopy(_laboratory_full_openapi_schema())
    schema["servers"] = [{"url": PUBLIC_RUNTIME_URL}]
    schema.setdefault("info", {})["x-vectra-openapi-source"] = "official_gpt_actions_schema"
    schema["x-vectra-root-openapi"] = {
        "status": "GPT_ACTIONS_READY",
        "standard_url": "/openapi.json",
        "production_url": PUBLIC_RUNTIME_URL,
        "release_fix": "VECTRA-GPT-ACTION-AVAILABILITY-001",
        "previous_release_fix": "OPENAPI-SERVERS-HOTFIX-0001",
    }
    return schema


app.openapi = _vectra_action_openapi_schema


_warmup_lock = threading.Lock()
_warmup_thread = None
_warmup_state = {
    "status": "NOT_STARTED",
    "started_at": None,
    "completed_at": None,
    "failure_reason": None,
}


def get_vectra_warmup_state() -> dict:
    """Return a stable process-local view of background Runtime restoration."""
    with _warmup_lock:
        return deepcopy(_warmup_state)


def _set_warmup_state(status: str, *, failure_reason: str | None = None) -> None:
    now = datetime.now(timezone.utc).isoformat()
    with _warmup_lock:
        _warmup_state["status"] = status
        if status == "STARTING":
            _warmup_state["started_at"] = now
            _warmup_state["completed_at"] = None
        elif status in {"READY", "FAILED"}:
            _warmup_state["completed_at"] = now
        _warmup_state["failure_reason"] = failure_reason


def _warmup_vectra_runtime_sync():
    from app.query.entity_dictionary import refresh_entity_dictionary
    from app.data.loader import get_csv_text  # 🔴 ДОБАВИЛИ
    from app.assistant_runtime.repository_persistence import (
        STARTUP_HOTFIX_RELEASE_ID,
    )

    try:
        from app.assistant_runtime.repository import ensure_repository
        from app.assistant_runtime.repository_migrations import (
            reconcile_development_journal_continuity,
            reconcile_lost_pk002_candidate,
        )

        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=repository_sync status=STARTED",
            flush=True,
        )
        ensure_repository()
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=repository_sync status=PASS",
            flush=True,
        )

        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=repository_migration status=STARTED",
            flush=True,
        )
        migration = reconcile_lost_pk002_candidate()
        if migration.get("status") != "PASS":
            raise RuntimeError("Runtime Repository migration failed")
        journal_migration = reconcile_development_journal_continuity()
        if journal_migration.get("status") != "PASS":
            raise RuntimeError("Development Journal continuity migration failed")
        from app.assistant_runtime.organizational_memory_continuity import (
            verify_and_update_organizational_memory_continuity,
        )
        continuity = verify_and_update_organizational_memory_continuity(
            deployment_id=(
                os.getenv("RENDER_GIT_COMMIT")
                or os.getenv("RENDER_SERVICE_ID")
                or os.getenv("VECTRA_RELEASE_ID")
                or "local-startup"
            )
        )
        if continuity.get("status") != "PASS" or continuity.get("readback_status") != "PASS":
            raise RuntimeError(
                "Organizational Memory continuity verification failed: "
                f"{continuity.get('failure_reason') or continuity.get('failed_objects')}"
            )
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=repository_migration_and_memory_continuity status=PASS",
            flush=True,
        )

        # VECTRA-ARCHITECTURE-REGISTRY-001: load and validate the permanent
        # Architecture Registry before Runtime becomes ready.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=architecture_registry status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.architecture_registry_runtime import (
            initialize_architecture_registry_runtime,
        )
        registry_state = initialize_architecture_registry_runtime(force=True)
        if registry_state.get("status") != "PASS" or registry_state.get("integrity_status") != "PASS":
            raise RuntimeError("Architecture Registry Runtime initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=architecture_registry status=PASS",
            flush=True,
        )

        # VECTRA-VERIFICATION-RUNTIME-001: load the permanent Verification
        # Runtime after Architecture Registry is available.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=verification_runtime status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.verification_runtime import initialize_verification_runtime
        verification_state = initialize_verification_runtime(force=True)
        if verification_state.get("status") != "PASS" or not verification_state.get("architecture_registry_loaded"):
            raise RuntimeError("Verification Runtime initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=verification_runtime status=PASS",
            flush=True,
        )

        # VECTRA-EXECUTION-RUNTIME-001: load Execution Runtime only after
        # Verification Runtime is ready.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=execution_runtime status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.execution_runtime import initialize_execution_runtime
        execution_state = initialize_execution_runtime(force=True)
        if (
            execution_state.get("status") != "PASS"
            or not execution_state.get("architecture_registry_loaded")
            or not execution_state.get("verification_runtime_loaded")
        ):
            raise RuntimeError("Execution Runtime initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=execution_runtime status=PASS",
            flush=True,
        )

        # VECTRA-EXECUTION-ORCHESTRATOR-001: load Orchestrator only after
        # Execution Runtime is ready.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=execution_orchestrator status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.execution_orchestrator_runtime import initialize_execution_orchestrator
        orchestrator_state = initialize_execution_orchestrator(force=True)
        if (
            orchestrator_state.get("status") != "PASS"
            or not orchestrator_state.get("architecture_registry_loaded")
            or not orchestrator_state.get("verification_runtime_loaded")
            or not orchestrator_state.get("execution_runtime_loaded")
        ):
            raise RuntimeError("Execution Orchestrator initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=execution_orchestrator status=PASS",
            flush=True,
        )

        # VECTRA-SESSION-RUNTIME-001: load Session Runtime only after
        # Execution Orchestrator Runtime is ready.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=session_runtime status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.session_runtime import initialize_session_runtime
        session_state = initialize_session_runtime(force=True)
        if (
            session_state.get("status") != "PASS"
            or not session_state.get("architecture_registry_loaded")
            or not session_state.get("verification_runtime_loaded")
            or not session_state.get("execution_runtime_loaded")
            or not session_state.get("execution_orchestrator_loaded")
        ):
            raise RuntimeError("Session Runtime initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=session_runtime status=PASS",
            flush=True,
        )

        # VECTRA-RUNTIME-SUPERVISOR-001: load Runtime Supervisor only after
        # Session Runtime is ready. The Supervisor reads published statuses only.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_supervisor status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.runtime_supervisor import initialize_runtime_supervisor
        supervisor_state = initialize_runtime_supervisor(force=True)
        if supervisor_state.get("status") != "PASS" or not supervisor_state.get("loaded"):
            raise RuntimeError("Runtime Supervisor initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_supervisor status=PASS",
            flush=True,
        )

        # VECTRA-RUNTIME-RECOVERY-001: load Runtime Recovery only after
        # Runtime Supervisor is available. Recovery executes registered procedures only.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_recovery status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.runtime_recovery import initialize_runtime_recovery
        recovery_state = initialize_runtime_recovery(force=True)
        if recovery_state.get("status") != "PASS" or not recovery_state.get("loaded") or not recovery_state.get("supervisor_available"):
            raise RuntimeError("Runtime Recovery initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_recovery status=PASS",
            flush=True,
        )

        # VECTRA-RUNTIME-CAPABILITY-REGISTRY-001: load only after Runtime Recovery.
        # The Registry indexes metadata published by existing Runtime components.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_capability_registry status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.runtime_capability_registry import initialize_runtime_capability_registry
        capability_registry_state = initialize_runtime_capability_registry(force=True)
        if capability_registry_state.get("status") != "PASS" or not capability_registry_state.get("loaded"):
            raise RuntimeError("Runtime Capability Registry initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_capability_registry status=PASS",
            flush=True,
        )

        # VECTRA-RUNTIME-DEPENDENCY-GRAPH-001: load only after Capability Registry.
        # The graph contains only relations explicitly published by Runtime components.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_dependency_graph status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.runtime_dependency_graph import initialize_runtime_dependency_graph
        dependency_graph_state = initialize_runtime_dependency_graph(force=True)
        if dependency_graph_state.get("status") != "PASS" or not dependency_graph_state.get("loaded"):
            raise RuntimeError("Runtime Dependency Graph initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_dependency_graph status=PASS",
            flush=True,
        )

        # VECTRA-RUNTIME-OBSERVABILITY-001: aggregate only officially
        # published Runtime data after Runtime Dependency Graph is ready.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_observability status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.runtime_observability import initialize_runtime_observability
        observability_state = initialize_runtime_observability(force=True)
        if observability_state.get("status") != "PASS" or not observability_state.get("loaded"):
            raise RuntimeError("Runtime Observability initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_observability status=PASS",
            flush=True,
        )

        # VECTRA-RUNTIME-HEALTH-001: derive consolidated health only from
        # approved published Runtime sources after Observability is ready.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_health status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.runtime_health import initialize_runtime_health
        health_state = initialize_runtime_health(force=True)
        if health_state.get("status") != "PASS" or not health_state.get("loaded"):
            raise RuntimeError("Runtime Health initialization failed")
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_health status=PASS",
            flush=True,
        )

        # 🔴 preload DATA
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=data_preload status=STARTED",
            flush=True,
        )
        get_csv_text()

        # 🔴 preload dictionary
        refresh_entity_dictionary()
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=data_preload status=PASS",
            flush=True,
        )

        # GENESIS-0002: after successful runtime startup/deploy, persist the
        # official Runtime Snapshot for VECTRA Laboratory Product Verification.
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_snapshot status=STARTED",
            flush=True,
        )
        from app.assistant_runtime.observability import create_startup_runtime_snapshot
        create_startup_runtime_snapshot()
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=runtime_snapshot status=PASS",
            flush=True,
        )

        _set_warmup_state("READY")
        print("✅ VECTRA warmed up: DATA + dictionary + Runtime Snapshot loaded", flush=True)

    except Exception as e:
        _set_warmup_state("FAILED", failure_reason=f"{type(e).__name__}: {e}")
        print(
            f"❌ VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            f"status=FAIL error_type={type(e).__name__} error={e}",
            flush=True,
        )
        # The HTTP listener must stay available so Render and Runtime diagnostics
        # can observe the failed restoration. The Runtime itself remains non-ready.


@app.on_event('startup')
def warmup_vectra_runtime():
    """Open the HTTP port immediately and restore the heavy Runtime in background.

    Render scans for the bound port while FastAPI startup handlers are running.
    The complete VECTRA restoration can take several minutes, so doing it inline
    makes an otherwise healthy deploy fail with ``Port scan timeout``. A single
    daemon worker preserves the approved restoration order without blocking the
    ASGI server from accepting health and diagnostic traffic.
    """
    global _warmup_thread
    with _warmup_lock:
        if _warmup_thread is not None and _warmup_thread.is_alive():
            return
        _warmup_state.update(
            {
                "status": "STARTING",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "completed_at": None,
                "failure_reason": None,
            }
        )
        _warmup_thread = threading.Thread(
            target=_warmup_vectra_runtime_sync,
            name="vectra-runtime-warmup",
            daemon=True,
        )
        _warmup_thread.start()
    print(
        "VECTRA startup phase=http_listener status=READY "
        "runtime_warmup_status=STARTING",
        flush=True,
    )
