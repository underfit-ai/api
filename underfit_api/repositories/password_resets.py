from __future__ import annotations

import os
from base64 import urlsafe_b64encode
from datetime import timedelta
from uuid import UUID, uuid4

from sqlalchemy import Connection

from underfit_api.auth import hash_token
from underfit_api.helpers import utcnow
from underfit_api.schema import password_reset_tokens

RESET_TOKEN_TTL_MINUTES = 30


def create(conn: Connection, user_id: UUID) -> str:
    conn.execute(password_reset_tokens.delete().where(password_reset_tokens.c.user_id == user_id))
    token = urlsafe_b64encode(os.urandom(32)).decode()
    now = utcnow()
    conn.execute(password_reset_tokens.insert().values(
        id=uuid4(),
        user_id=user_id,
        token_hash=hash_token(token),
        created_at=now,
        expires_at=now + timedelta(minutes=RESET_TOKEN_TTL_MINUTES),
    ))
    return token


def get_user_id(conn: Connection, token: str) -> UUID | None:
    row = conn.execute(
        password_reset_tokens.select().where(
            password_reset_tokens.c.token_hash == hash_token(token),
            password_reset_tokens.c.expires_at > utcnow(),
        ),
    ).first()
    return row.user_id if row else None


def delete(conn: Connection, token: str) -> None:
    conn.execute(password_reset_tokens.delete().where(password_reset_tokens.c.token_hash == hash_token(token)))
