from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Connection, Row

from underfit_api.helpers import utcnow
from underfit_api.schema import account_aliases


def create(conn: Connection, account_id: UUID, handle: str) -> None:
    conn.execute(account_aliases.insert().values(
        id=uuid4(), account_id=account_id, handle=handle, created_at=utcnow(),
    ))


def get_by_handle(conn: Connection, handle: str) -> Row | None:
    return conn.execute(
        account_aliases.select().where(account_aliases.c.handle == handle.lower()),
    ).first()


def handle_exists(conn: Connection, handle: str) -> bool:
    return get_by_handle(conn, handle) is not None
