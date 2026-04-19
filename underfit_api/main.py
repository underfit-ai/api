from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fastapi import APIRouter, FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import RequestResponseEndpoint

from underfit_api import backfill
from underfit_api.auth import get_app_secret
from underfit_api.buffers import BadStartLineError, BadStepError
from underfit_api.buffers import logs as log_buffer
from underfit_api.buffers import scalars as scalar_buffer
from underfit_api.config import config
from underfit_api.db import build_engine, ensure_local_cache_schema
from underfit_api.dependencies import AppContext
from underfit_api.models import HealthResponse
from underfit_api.repositories import accounts as accounts_repo
from underfit_api.routes.account_avatars import router as account_avatars_router
from underfit_api.routes.accounts import router as accounts_router
from underfit_api.routes.api_keys import router as api_keys_router
from underfit_api.routes.artifacts import router as artifacts_router
from underfit_api.routes.auth import router as auth_router
from underfit_api.routes.logs import router as logs_router
from underfit_api.routes.media import router as media_router
from underfit_api.routes.organization_members import router as org_members_router
from underfit_api.routes.organizations import router as orgs_router
from underfit_api.routes.project_collaborators import router as project_collaborators_router
from underfit_api.routes.projects import router as projects_router
from underfit_api.routes.run_workers import router as workers_router
from underfit_api.routes.runs import router as runs_router
from underfit_api.routes.scalars import router as scalars_router
from underfit_api.routes.users import router as users_router
from underfit_api.storage import build_storage

logger = logging.getLogger(__name__)
BACKFILL_WRITE_ERROR = "API write endpoints are disabled while backfill is enabled"
WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}
BACKFILL_WRITE_ROUTE_WHITELIST = (
    re.compile(r"^/api/v1/accounts/[^/]+/projects/[^/]+/ui-state$"),
    re.compile(r"^/api/v1/accounts/[^/]+/projects/[^/]+/runs/[^/]+/ui-state$"),
)


async def _flush_loop(ctx: AppContext) -> None:
    interval = config.buffer.flush_interval_ms / 1000
    try:
        while True:
            await asyncio.sleep(interval)
            try:
                await asyncio.to_thread(scalar_buffer.compact, ctx.engine, ctx.storage)
                await asyncio.to_thread(log_buffer.compact, ctx.engine, ctx.storage)
            except Exception:
                logger.exception("Buffer flush error")
    except asyncio.CancelledError:
        return


def _validate_config() -> None:
    if config.backfill.enabled and config.auth_enabled:
        raise RuntimeError("Backfill mode requires auth_enabled = false")
    if config.auth_enabled:
        get_app_secret()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    _validate_config()
    engine = ensure_local_cache_schema() if config.backfill.enabled else build_engine()
    ctx = AppContext(engine=engine, storage=build_storage())
    app.state.ctx = ctx
    if not config.auth_enabled:
        with ctx.engine.begin() as conn:
            accounts_repo.get_or_create_local(conn)
    if config.backfill.enabled:
        with ctx.engine.begin() as conn:
            backfill.sync(ctx, conn)
    flush_task = None if config.backfill.enabled else asyncio.create_task(_flush_loop(ctx))
    yield
    if flush_task is not None:
        flush_task.cancel()
        with suppress(asyncio.CancelledError):
            await flush_task
    ctx.engine.dispose()


app = FastAPI(lifespan=lifespan)

cors_origins = ["http://localhost:5173", "http://127.0.0.1:5173"]
if config.frontend_url:
    cors_origins.append(config.frontend_url)

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.middleware("http")
async def backfill_middleware(request: Request, call_next: RequestResponseEndpoint) -> Response:
    if (
        config.backfill.enabled and request.url.path.startswith("/api/v1")
        and request.method in WRITE_METHODS
        and not any(pattern.fullmatch(request.url.path) for pattern in BACKFILL_WRITE_ROUTE_WHITELIST)
    ):
        return JSONResponse(status_code=409, content={"error": BACKFILL_WRITE_ERROR})
    return await call_next(request)


@app.exception_handler(404)
def not_found(_request: Request, _exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "Route not found"})


@app.exception_handler(HTTPException)
def http_exception_handler(_request: Request, exc: HTTPException) -> JSONResponse:
    content = exc.detail if isinstance(exc.detail, dict) else {"error": str(exc.detail)}
    return JSONResponse(status_code=exc.status_code, content=content)


@app.exception_handler(RequestValidationError)
def validation_exception_handler(_request: Request, exc: RequestValidationError) -> JSONResponse:
    return JSONResponse(status_code=400, content={"error": "Validation error"})


@app.exception_handler(BadStartLineError)
def bad_start_line_handler(_request: Request, exc: BadStartLineError) -> JSONResponse:
    return JSONResponse(status_code=409, content={"error": "Invalid startLine", "expectedStartLine": exc.expected})


@app.exception_handler(BadStepError)
def bad_step_handler(_request: Request, exc: BadStepError) -> JSONResponse:
    content = {"error": "Step must be strictly increasing", "lastStep": exc.last_step}
    return JSONResponse(status_code=409, content=content)


api_router = APIRouter(prefix="/api/v1")


@api_router.get("/health")
def health() -> HealthResponse:
    return HealthResponse()


api_router.include_router(accounts_router)
api_router.include_router(api_keys_router)
api_router.include_router(artifacts_router)
api_router.include_router(auth_router)
api_router.include_router(account_avatars_router)
api_router.include_router(org_members_router)
api_router.include_router(project_collaborators_router)
api_router.include_router(logs_router)
api_router.include_router(media_router)
api_router.include_router(orgs_router)
api_router.include_router(projects_router)
api_router.include_router(runs_router)
api_router.include_router(scalars_router)
api_router.include_router(users_router)
api_router.include_router(workers_router)

app.include_router(api_router)


if (_static_dir := Path(__file__).parent / config.static_dir).is_dir():
    _index_html = _static_dir / "index.html"
    app.mount("/assets", StaticFiles(directory=_static_dir / "assets"), name="static-assets")

    @app.get("/{path:path}")
    async def spa_fallback(path: str) -> FileResponse:
        if path.startswith("api/"):
            raise HTTPException(404, "Route not found")
        if (file := _static_dir / path).is_file():
            return FileResponse(file)
        return FileResponse(_index_html)
