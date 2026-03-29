from __future__ import annotations

import os
from base64 import urlsafe_b64encode
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import Connection

from underfit_api.auth import hash_token
from underfit_api.models import Session
from underfit_api.schema import sessions

SESSION_TTL_DAYS = 30


def create(conn: Connection, user_id: UUID) -> Session:
    token = urlsafe_b64encode(os.urandom(32)).decode()
    prefix = token[:8]
    token_hash = hash_token(token)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    expires = now + timedelta(days=SESSION_TTL_DAYS)
    session_id = uuid4()
    conn.execute(sessions.insert().values(
        id=session_id,
        user_id=user_id,
        token_prefix=prefix,
        token_hash=token_hash,
        created_at=now,
        expires_at=expires,
    ))
    return Session(token=token, created_at=now, expires_at=expires)


def delete_by_token(conn: Connection, token: str) -> None:
    conn.execute(sessions.delete().where(sessions.c.token_hash == hash_token(token)))
