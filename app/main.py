from fastapi import FastAPI

from app.api.routes import router

app = FastAPI(title="VECTRA CORE v2 MVP")
app.include_router(router)


@app.on_event('startup')
def warmup_vectra_runtime():
    from app.query.entity_dictionary import refresh_entity_dictionary

    try:
        refresh_entity_dictionary()
    except Exception:
        pass
