import os
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
        "release_fix": "OPENAPI-SERVERS-HOTFIX-0001",
    }
    return schema


app.openapi = _vectra_action_openapi_schema


@app.on_event('startup')
def warmup_vectra_runtime():
    from app.query.entity_dictionary import refresh_entity_dictionary
    from app.data.loader import get_csv_text  # 🔴 ДОБАВИЛИ
    from app.assistant_runtime.repository_persistence import (
        STARTUP_HOTFIX_RELEASE_ID,
        database_persistence_enabled,
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

        print("✅ VECTRA warmed up: DATA + dictionary + Runtime Snapshot loaded", flush=True)

    except Exception as e:
        print(
            f"❌ VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            f"status=FAIL error_type={type(e).__name__} error={e}",
            flush=True,
        )
        if database_persistence_enabled():
            raise
