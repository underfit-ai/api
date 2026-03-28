from __future__ import annotations

from sqlalchemy import Connection

from app.models import Account
from app.repositories import organizations as organizations_repo
from app.repositories import users as users_repo
from app.schema import accounts


def exists(conn: Connection, handle: str) -> bool:
    return conn.execute(accounts.select().where(accounts.c.handle == handle.lower())).first() is not None


def get_by_handle(conn: Connection, handle: str) -> Account | None:
    account = conn.execute(accounts.select().where(accounts.c.handle == handle.lower())).first()
    if account is None:
        return None
    if account.type == "ORGANIZATION":
        return organizations_repo.get_by_id(conn, account.id)
    return users_repo.get_by_id(conn, account.id)
