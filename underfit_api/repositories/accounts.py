from __future__ import annotations

from sqlalchemy import Connection

from underfit_api.models import Account
from underfit_api.repositories import organizations as organizations_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import accounts


def exists(conn: Connection, handle: str) -> bool:
    return get_by_handle(conn, handle) is not None


def get_by_handle(conn: Connection, handle: str) -> Account | None:
    if not (account := conn.execute(accounts.select().where(accounts.c.handle == handle.lower())).first()):
        return None
    if account.type == "ORGANIZATION":
        return organizations_repo.get_by_id(conn, account.id)
    return users_repo.get_by_id(conn, account.id)
