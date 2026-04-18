from __future__ import annotations

from uuid import UUID

from sqlalchemy import Connection

from underfit_api.auth import PasswordHash, verify_password
from underfit_api.helpers import utcnow
from underfit_api.schema import user_auth


def verify(conn: Connection, user_id: UUID, password: str) -> bool:
    if not (row := conn.execute(user_auth.select().where(user_auth.c.id == user_id)).first()):
        return False
    return verify_password(password, PasswordHash(
        row.password_hash, row.password_salt, row.password_iterations, row.password_digest,
    ))


def get_password_hash_prefix(conn: Connection, user_id: UUID, length: int = 8) -> str | None:
    row = conn.execute(user_auth.select().where(user_auth.c.id == user_id)).first()
    return row.password_hash[:length] if row else None


def update_password(conn: Connection, user_id: UUID, pw: PasswordHash) -> None:
    conn.execute(user_auth.update().where(user_auth.c.id == user_id).values(
        password_hash=pw.hash, password_salt=pw.salt, password_iterations=pw.iterations, password_digest=pw.digest,
        updated_at=utcnow(),
    ))


def create(conn: Connection, user_id: UUID, pw: PasswordHash) -> None:
    now = utcnow()
    conn.execute(user_auth.insert().values(
        id=user_id, password_hash=pw.hash, password_salt=pw.salt,
        password_iterations=pw.iterations, password_digest=pw.digest,
        created_at=now, updated_at=now,
    ))
