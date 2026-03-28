from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Optional

from fastapi import Cookie, Depends, Header, HTTPException
from sqlalchemy import Connection

from app.auth import hash_token
from app.config import config
from app.db import get_conn
from app.models import User
from app.repositories import users as users_repo
from app.schema import api_keys, sessions

Conn = Annotated[Connection, Depends(get_conn)]
AuthorizationHeader = Annotated[Optional[str], Header()]
SessionTokenCookie = Annotated[Optional[str], Cookie()]


def _get_local_user(conn: Connection) -> User:
    existing = users_repo.get_by_email(conn, "local@underfit.local")
    if existing is not None:
        return existing
    return users_repo.create(conn, "local@underfit.local", "local", "Local User")


def _authenticate(conn: Connection, authorization: str | None, session_token: str | None) -> User | None:
    if not config.auth_enabled:
        return _get_local_user(conn)
    if authorization and authorization.startswith("Bearer "):
        token = authorization[7:]
        key = conn.execute(api_keys.select().where(api_keys.c.token_hash == hash_token(token))).first()
        if key is not None:
            return users_repo.get_by_id(conn, key.user_id)
    if session_token:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        session = conn.execute(
            sessions.select().where(sessions.c.token_hash == hash_token(session_token), sessions.c.expires_at > now),
        ).first()
        if session is not None:
            return users_repo.get_by_id(conn, session.user_id)
    return None


def get_current_user(
    conn: Conn,
    authorization: AuthorizationHeader = None,
    session_token: SessionTokenCookie = None,
) -> User:
    if not (user := _authenticate(conn, authorization, session_token)):
        raise HTTPException(status_code=401, detail="Unauthorized")
    return user


def get_maybe_user(
    conn: Conn,
    authorization: AuthorizationHeader = None,
    session_token: SessionTokenCookie = None,
) -> User | None:
    return _authenticate(conn, authorization, session_token)


CurrentUser = Annotated[User, Depends(get_current_user)]
MaybeUser = Annotated[Optional[User], Depends(get_maybe_user)]
