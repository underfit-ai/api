from __future__ import annotations

from collections.abc import Iterator
from typing import Annotated, NamedTuple, Optional
from uuid import UUID

from fastapi import Cookie, Depends, Header, HTTPException, Request
from sqlalchemy import Connection, Engine

from underfit_api.auth import hash_token
from underfit_api.buffer import BufferStore
from underfit_api.config import config
from underfit_api.models import User
from underfit_api.repositories import api_keys as api_keys_repo
from underfit_api.repositories import sessions as sessions_repo
from underfit_api.repositories import users as users_repo
from underfit_api.storage.types import Storage


class AppContext(NamedTuple):
    engine: Engine
    storage: Storage
    buffer: BufferStore


def get_ctx(request: Request) -> AppContext:
    return request.app.state.ctx


def get_conn(ctx: Annotated[AppContext, Depends(get_ctx)]) -> Iterator[Connection]:
    with ctx.engine.begin() as conn:
        yield conn


Ctx = Annotated[AppContext, Depends(get_ctx)]
Conn = Annotated[Connection, Depends(get_conn)]
AuthorizationHeader = Annotated[Optional[str], Header()]
SessionTokenCookie = Annotated[Optional[str], Cookie()]


def _authenticate(conn: Connection, authorization: str | None, session_token: str | None) -> User | None:
    if not config.auth_enabled:
        return users_repo.get_by_email(conn, "local@underfit.local")
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        if key := api_keys_repo.get_by_token_hash(conn, hash_token(token)):
            return users_repo.get_by_id(conn, key.user_id)
    if session_token and (session_user_id := sessions_repo.get_user_id_by_token_hash(conn, hash_token(session_token))):
        return users_repo.get_by_id(conn, session_user_id)
    return None


class AuthContext(NamedTuple):
    authorization: str | None
    session_token: str | None

    def maybe_user(self, conn: Connection) -> User | None:
        return _authenticate(conn, self.authorization, self.session_token)

    def require_user(self, conn: Connection) -> User:
        if not (user := self.maybe_user(conn)):
            raise HTTPException(401, "Unauthorized")
        return user


def get_auth(authorization: AuthorizationHeader = None, session_token: SessionTokenCookie = None) -> AuthContext:
    return AuthContext(authorization, session_token)


Auth = Annotated[AuthContext, Depends(get_auth)]


def get_current_user(conn: Conn, auth: Auth) -> User:
    return auth.require_user(conn)


def get_maybe_user(conn: Conn, auth: Auth) -> User | None:
    return auth.maybe_user(conn)


def get_current_worker(authorization: AuthorizationHeader = None) -> UUID:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Unauthorized")
    try:
        return UUID(authorization[7:])
    except ValueError:
        raise HTTPException(401, "Unauthorized") from None


RequireUser = Annotated[User, Depends(get_current_user)]
MaybeUser = Annotated[Optional[User], Depends(get_maybe_user)]
CurrentWorker = Annotated[UUID, Depends(get_current_worker)]
