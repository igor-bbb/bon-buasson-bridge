from fastapi import FastAPI

from app.api.routes import router
from app.runtime import ensure_runtime_ready

app = FastAPI(title="VECTRA CORE v2 MVP")
app.include_router(router)


@app.on_event('startup')
def warmup_vectra_runtime():
    ok = ensure_runtime_ready(force=True, retries=3)
    if ok:
        print('✅ VECTRA warmed up: DATA + rows + normalized cache + dictionary loaded')
    else:
        from app.runtime import get_runtime_status
        print(f"❌ Warmup error: {get_runtime_status().get('error')}")
