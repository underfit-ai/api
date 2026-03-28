from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.auth import get_app_secret
from app.config import config
from app.db import shutdown_engine
from app.routes.accounts import router as accounts_router
from app.routes.api_keys import router as api_keys_router
from app.routes.auth import router as auth_router
from app.routes.users import router as users_router


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
    if config.auth_enabled:
        get_app_secret()
    yield
    shutdown_engine()


app = FastAPI(lifespan=lifespan)

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
api.include_router(auth_router)
api.include_router(users_router)

app.mount("/api/v1", api)


@api.exception_handler(404)
def not_found(_request: Request, _exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=404, content={"error": "Route not found"})


@api.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok", "version": "v1"}
