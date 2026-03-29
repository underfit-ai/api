from __future__ import annotations

from datetime import datetime, timezone
from uuid import UUID

from sqlalchemy import Connection

from underfit_api.auth import verify_password
from underfit_api.schema import user_auth


def verify(conn: Connection, user_id: UUID, password: str) -> bool:
    row = conn.execute(user_auth.select().where(user_auth.c.id == user_id)).first()
    return verify_password(
        password,
        row.password_hash,
        row.password_salt,
        row.password_iterations,
        row.password_digest,
    ) if row else False


def create(
    conn: Connection,
    user_id: UUID,
    password_hash: str,
    password_salt: str,
    password_iterations: int,
    password_digest: str,
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(user_auth.insert().values(
        id=user_id, password_hash=password_hash, password_salt=password_salt,
        password_iterations=password_iterations, password_digest=password_digest,
        created_at=now, updated_at=now,
    ))
