from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse
from fastapi.staticfiles import StaticFiles

from underfit_api.auth import get_app_secret
from underfit_api.buffer import log_buffer, scalar_buffer
from underfit_api.config import config
from underfit_api.db import get_engine, shutdown_engine
from underfit_api.routes.accounts import router as accounts_router
from underfit_api.routes.api_keys import router as api_keys_router
from underfit_api.routes.artifacts import router as artifacts_router
from underfit_api.routes.auth import router as auth_router
from underfit_api.routes.avatars import router as avatars_router
from underfit_api.routes.collaborators import router as collaborators_router
from underfit_api.routes.files import router as files_router
from underfit_api.routes.logs import router as logs_router
from underfit_api.routes.media import router as media_router
from underfit_api.routes.organizations import router as orgs_router
from underfit_api.routes.projects import router as projects_router
from underfit_api.routes.runs import router as runs_router
from underfit_api.routes.scalars import router as scalars_router
from underfit_api.routes.users import router as users_router
from underfit_api.storage import get_storage
from underfit_api.storage.backfill import BackfillService

logger = logging.getLogger(__name__)


async def _flush_loop() -> None:
    engine = get_engine()
    storage = get_storage()
    interval = config.buffer.flush_interval_ms / 1000
    while True:
        await asyncio.sleep(interval)
        try:
            with engine.begin() as conn:
                log_buffer.flush_stale(conn, storage)
                scalar_buffer.flush_stale(conn, storage)
        except Exception:
            logger.exception("Buffer flush error")


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    backfill: BackfillService | None = None
    if config.auth_enabled:
        get_app_secret()
    if config.backfill.enabled:
        backfill = BackfillService(get_storage(), get_engine(), config.backfill, config.buffer)
        await backfill.start()
    flush_task = asyncio.create_task(_flush_loop())
    yield
    flush_task.cancel()
    if backfill is not None:
        await backfill.stop()
    engine = get_engine()
    storage = get_storage()
    with engine.begin() as conn:
        log_buffer.flush_all(conn, storage)
        scalar_buffer.flush_all(conn, storage)
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


@api.exception_handler(HTTPException)
def http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
    content = exc.detail if isinstance(exc.detail, dict) else {"error": str(exc.detail)}
    return JSONResponse(status_code=exc.status_code, content=content)


@api.exception_handler(RequestValidationError)
def validation_exception_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": "Validation error"})


@api.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": "v1"}


if (_static_dir := Path(__file__).parent / config.static_dir).is_dir():
    _index_html = _static_dir / "index.html"
    app.mount("/assets", StaticFiles(directory=_static_dir / "assets"), name="static-assets")

    @app.get("/{path:path}")
    async def spa_fallback(path: str) -> FileResponse:
        if (file := _static_dir / path).is_file():
            return FileResponse(file)
        return FileResponse(_index_html)
