from fastapi import FastAPI

from app.api.routes import router

app = FastAPI(title="VECTRA CORE v2 MVP")
app.include_router(router)


@app.on_event('startup')
def warmup_vectra_runtime():
    from app.query.entity_dictionary import refresh_entity_dictionary
    from app.data.loader import get_csv_text  # 🔴 ДОБАВИЛИ

    try:
        # 🔴 preload DATA
        get_csv_text()

        # 🔴 preload dictionary
        refresh_entity_dictionary()

        # GENESIS-0002: after successful runtime startup/deploy, persist the
        # official Runtime Snapshot for VECTRA Laboratory Product Verification.
        from app.assistant_runtime.observability import create_startup_runtime_snapshot
        create_startup_runtime_snapshot()

        print("✅ VECTRA warmed up: DATA + dictionary + Runtime Snapshot loaded")

    except Exception as e:
        print(f"❌ Warmup error: {e}")
