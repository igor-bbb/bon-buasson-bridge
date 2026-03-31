from fastapi import FastAPI

from app.api.routes import router

app = FastAPI(title="VECTRA CORE v2 MVP")
app.include_router(router)
