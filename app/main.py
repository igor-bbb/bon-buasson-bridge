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
        from app.assistant_runtime.repository_migrations import reconcile_lost_pk002_candidate

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
        print(
            f"VECTRA startup [{STARTUP_HOTFIX_RELEASE_ID}] "
            "phase=repository_migration status=PASS",
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
