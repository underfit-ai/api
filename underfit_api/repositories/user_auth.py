from __future__ import annotations

from uuid import UUID

from sqlalchemy import Connection

from underfit_api.auth import verify_password
from underfit_api.helpers import utcnow
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


def get_password_hash_prefix(conn: Connection, user_id: UUID, length: int = 8) -> str | None:
    row = conn.execute(user_auth.select().where(user_auth.c.id == user_id)).first()
    return row.password_hash[:length] if row else None


def update_password(
    conn: Connection, user_id: UUID, password_hash: str, password_salt: str,
    password_iterations: int, password_digest: str,
) -> None:
    conn.execute(user_auth.update().where(user_auth.c.id == user_id).values(
        password_hash=password_hash, password_salt=password_salt,
        password_iterations=password_iterations, password_digest=password_digest,
        updated_at=utcnow(),
    ))


def create(
    conn: Connection, user_id: UUID, password_hash: str, password_salt: str,
    password_iterations: int, password_digest: str,
) -> None:
    now = utcnow()
    conn.execute(user_auth.insert().values(
        id=user_id, password_hash=password_hash, password_salt=password_salt,
        password_iterations=password_iterations, password_digest=password_digest,
        created_at=now, updated_at=now,
    ))
