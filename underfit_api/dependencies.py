from __future__ import annotations

from typing import Annotated, Optional
from uuid import UUID

from fastapi import Cookie, Depends, Header, HTTPException
from sqlalchemy import Connection

from underfit_api.auth import hash_token, verify_signed_token
from underfit_api.config import config
from underfit_api.db import get_conn
from underfit_api.models import User
from underfit_api.repositories import api_keys as api_keys_repo
from underfit_api.repositories import sessions as sessions_repo
from underfit_api.repositories import users as users_repo

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


def get_current_user(
    conn: Conn, authorization: AuthorizationHeader = None, session_token: SessionTokenCookie = None,
) -> User:
    if not (user := _authenticate(conn, authorization, session_token)):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return user


def get_maybe_user(
    conn: Conn, authorization: AuthorizationHeader = None, session_token: SessionTokenCookie = None,
) -> User | None:
    return _authenticate(conn, authorization, session_token)


def get_current_worker(authorization: AuthorizationHeader = None) -> UUID:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Unauthorized")
    try:
        if not config.auth_enabled:
            return UUID(authorization[7:])
        token = verify_signed_token(authorization[7:])
        if not token:
            raise HTTPException(401, "Unauthorized")
        return UUID(token["worker_id"])
    except (KeyError, ValueError, TypeError):
        raise HTTPException(401, "Unauthorized") from None


CurrentUser = Annotated[User, Depends(get_current_user)]
MaybeUser = Annotated[Optional[User], Depends(get_maybe_user)]
CurrentWorker = Annotated[UUID, Depends(get_current_worker)]
