from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.auth import get_app_secret
from app.config import config
from app.db import get_engine, shutdown_engine
from app.routes.accounts import router as accounts_router
from app.routes.api_keys import router as api_keys_router
from app.routes.artifacts import router as artifacts_router
from app.routes.auth import router as auth_router
from app.routes.avatars import router as avatars_router
from app.routes.collaborators import router as collaborators_router
from app.routes.files import router as files_router
from app.routes.logs import router as logs_router
from app.routes.media import router as media_router
from app.routes.organizations import router as orgs_router
from app.routes.projects import router as projects_router
from app.routes.runs import router as runs_router
from app.routes.scalars import router as scalars_router
from app.routes.users import router as users_router
from app.storage import get_storage
from app.storage.backfill import BackfillService


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    backfill: BackfillService | None = None
    if config.auth_enabled:
        get_app_secret()
    if config.backfill.enabled:
        backfill = BackfillService(get_storage(), get_engine(), config.backfill, config.buffer)
        await backfill.start()
    yield
    if backfill is not None:
        await backfill.stop()
    shutdown_engine()


app = FastAPI(lifespan=lifespan)

cors_origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
if config.frontend_url:
    cors_origins.append(config.frontend_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

api = FastAPI()
api.include_router(accounts_router)
api.include_router(api_keys_router)
api.include_router(artifacts_router)
api.include_router(auth_router)
api.include_router(avatars_router)
api.include_router(collaborators_router)
api.include_router(files_router)
api.include_router(logs_router)
api.include_router(media_router)
api.include_router(orgs_router)
api.include_router(projects_router)
api.include_router(runs_router)
api.include_router(scalars_router)
api.include_router(users_router)

app.mount("/api/v1", api)


@api.exception_handler(404)
def not_found(_request: Request, _exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "Route not found"})


@api.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": "v1"}
