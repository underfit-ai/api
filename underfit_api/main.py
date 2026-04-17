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
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import RequestResponseEndpoint

from underfit_api.auth import get_app_secret
from underfit_api.backfill import BackfillService
from underfit_api.buffer import LogBuffer, ScalarBuffer
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
from underfit_api.routes.files import router as files_router
from underfit_api.routes.logs import router as logs_router
from underfit_api.routes.media import router as media_router
from underfit_api.routes.organization_members import router as org_members_router
from underfit_api.routes.organizations import router as orgs_router
from underfit_api.routes.project_collaborators import router as project_collaborators_router
from underfit_api.routes.projects import router as projects_router
from underfit_api.routes.resolvers import AccountAliasRedirectError, ProjectAliasRedirectError
from underfit_api.routes.run_workers import router as workers_router
from underfit_api.routes.runs import router as runs_router
from underfit_api.routes.scalars import router as scalars_router
from underfit_api.routes.users import router as users_router
from underfit_api.storage import build_storage

logger = logging.getLogger(__name__)
BACKFILL_WRITE_ERROR = "API write endpoints are disabled while backfill is enabled"
WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


def _flush_once(ctx: AppContext) -> None:
    with ctx.engine.begin() as conn:
        ctx.log_buffer.persist_due(conn, ctx.storage)
        ctx.scalar_buffer.persist_due(conn, ctx.storage)
        ctx.log_buffer.flush_inactive(conn, ctx.storage)
        ctx.scalar_buffer.flush_inactive(conn, ctx.storage)


async def _flush_loop(ctx: AppContext) -> None:
    interval = config.buffer.flush_interval_ms / 1000
    try:
        while True:
            await asyncio.sleep(interval)
            try:
                await asyncio.to_thread(_flush_once, ctx)
            except Exception:
                logger.exception("Buffer flush error")
    except asyncio.CancelledError:
        return


def _validate_config() -> None:
    if config.backfill.enabled and config.auth_enabled:
        raise RuntimeError("Backfill mode requires auth_enabled = false")
    if config.auth_enabled:
        get_app_secret()


def _init_context(app: FastAPI) -> AppContext:
    if not (ctx := getattr(app.state, "ctx", None)):
        engine = ensure_local_cache_schema() if config.backfill.enabled else build_engine()
        ctx = AppContext(engine=engine, storage=build_storage(), log_buffer=LogBuffer(), scalar_buffer=ScalarBuffer())
    if not config.auth_enabled:
        with ctx.engine.begin() as conn:
            accounts_repo.get_or_create_local(conn)
    return ctx


async def _init_backfill(ctx: AppContext) -> BackfillService | None:
    if not config.backfill.enabled:
        return None
    backfill = BackfillService(ctx.storage, ctx.engine, config.backfill)
    await backfill.start()
    return backfill


def _shutdown_context(ctx: AppContext) -> None:
    with ctx.engine.begin() as conn:
        ctx.log_buffer.flush_all(conn, ctx.storage)
        ctx.scalar_buffer.flush_all(conn, ctx.storage)
    ctx.engine.dispose()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    _validate_config()
    ctx = _init_context(app)
    app.state.ctx = ctx
    backfill = await _init_backfill(ctx)
    flush_task = asyncio.create_task(_flush_loop(ctx))
    yield
    flush_task.cancel()
    with suppress(asyncio.CancelledError):
        await flush_task
    if backfill is not None:
        await backfill.stop()
    _shutdown_context(ctx)


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
async def block_api_writes_during_backfill(request: Request, call_next: RequestResponseEndpoint) -> Response:
    if (
        config.backfill.enabled
        and request.url.path.startswith("/api/v1")
        and request.method in WRITE_METHODS
        and not request.url.path.endswith("/ui-state")
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


def _redirect(request: Request, new_path: str) -> RedirectResponse:
    query = str(request.url.query)
    return RedirectResponse(url=new_path + (f"?{query}" if query else ""), status_code=307)


@app.exception_handler(AccountAliasRedirectError)
def account_alias_redirect_handler(request: Request, exc: AccountAliasRedirectError) -> RedirectResponse:
    return _redirect(request, re.sub(
        r"/(accounts|users|organizations)/[^/]+", rf"/\1/{exc.new_handle}", request.url.path, count=1,
    ))


@app.exception_handler(ProjectAliasRedirectError)
def project_alias_redirect_handler(request: Request, exc: ProjectAliasRedirectError) -> RedirectResponse:
    return _redirect(request, re.sub(
        r"/accounts/[^/]+/projects/[^/]+",
        f"/accounts/{exc.new_account_handle}/projects/{exc.new_project_name}",
        request.url.path, count=1,
    ))


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
api_router.include_router(files_router)
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
        if (file := _static_dir / path).is_file():
            return FileResponse(file)
        return FileResponse(_index_html)
