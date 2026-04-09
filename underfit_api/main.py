from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from pathlib import Path

from fastapi import FastAPI, HTTPException, Request
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles
from starlette.middleware.base import RequestResponseEndpoint

import underfit_api.db as db
import underfit_api.storage as storage_mod
from underfit_api.auth import get_app_secret
from underfit_api.buffer import log_buffer, scalar_buffer
from underfit_api.config import config
from underfit_api.models import HealthResponse
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
from underfit_api.schema import metadata
from underfit_api.storage.backfill import BackfillService

logger = logging.getLogger(__name__)
BACKFILL_WRITE_ERROR = "API write endpoints are disabled while backfill is enabled"
WRITE_METHODS = {"POST", "PUT", "PATCH", "DELETE"}


def _flush_once() -> None:
    with db.engine.begin() as conn:
        log_buffer.persist_due(conn, storage_mod.storage)
        scalar_buffer.persist_due(conn, storage_mod.storage)
        log_buffer.flush_inactive(conn, storage_mod.storage)
        scalar_buffer.flush_inactive(conn, storage_mod.storage)


async def _flush_loop() -> None:
    interval = config.buffer.flush_interval_ms / 1000
    try:
        while True:
            await asyncio.sleep(interval)
            try:
                await asyncio.to_thread(_flush_once)
            except Exception:
                logger.exception("Buffer flush error")
    except asyncio.CancelledError:
        return


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    metadata.create_all(db.engine)
    backfill: BackfillService | None = None
    if config.auth_enabled:
        get_app_secret()
    if config.backfill.enabled:
        backfill = BackfillService(storage_mod.storage, db.engine, config.backfill)
        await backfill.start()
    flush_task = asyncio.create_task(_flush_loop())
    yield
    flush_task.cancel()
    with suppress(asyncio.CancelledError):
        await flush_task
    if backfill is not None:
        await backfill.stop()
    with db.engine.begin() as conn:
        log_buffer.flush_all(conn, storage_mod.storage)
        scalar_buffer.flush_all(conn, storage_mod.storage)
    db.engine.dispose()


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

api = FastAPI()


@api.middleware("http")
async def block_api_writes_during_backfill(request: Request, call_next: RequestResponseEndpoint) -> Response:
    if config.backfill.enabled and request.method in WRITE_METHODS:
        return JSONResponse(status_code=409, content={"error": BACKFILL_WRITE_ERROR})
    return await call_next(request)


api.include_router(accounts_router)
api.include_router(api_keys_router)
api.include_router(artifacts_router)
api.include_router(auth_router)
api.include_router(account_avatars_router)
api.include_router(org_members_router)
api.include_router(project_collaborators_router)
api.include_router(files_router)
api.include_router(logs_router)
api.include_router(media_router)
api.include_router(orgs_router)
api.include_router(projects_router)
api.include_router(runs_router)
api.include_router(scalars_router)
api.include_router(users_router)
api.include_router(workers_router)

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


@api.exception_handler(AccountAliasRedirectError)
def account_alias_redirect_handler(request: Request, exc: AccountAliasRedirectError) -> RedirectResponse:
    new_path = re.sub(r"/(accounts|users|organizations)/[^/]+", rf"/\1/{exc.new_handle}", request.url.path, count=1)
    query = str(request.url.query)
    location = new_path + (f"?{query}" if query else "")
    return RedirectResponse(url=location, status_code=307)


@api.exception_handler(ProjectAliasRedirectError)
def project_alias_redirect_handler(request: Request, exc: ProjectAliasRedirectError) -> RedirectResponse:
    new_path = re.sub(
        r"/accounts/[^/]+/projects/[^/]+",
        f"/accounts/{exc.new_account_handle}/projects/{exc.new_project_name}",
        request.url.path,
        count=1,
    )
    query = str(request.url.query)
    location = new_path + (f"?{query}" if query else "")
    return RedirectResponse(url=location, status_code=307)


@api.get("/health")
def health() -> HealthResponse:
    return HealthResponse()


if (_static_dir := Path(__file__).parent / config.static_dir).is_dir():
    _index_html = _static_dir / "index.html"
    app.mount("/assets", StaticFiles(directory=_static_dir / "assets"), name="static-assets")

    @app.get("/{path:path}")
    async def spa_fallback(path: str) -> FileResponse:
        if (file := _static_dir / path).is_file():
            return FileResponse(file)
        return FileResponse(_index_html)
