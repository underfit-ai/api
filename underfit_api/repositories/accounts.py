from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Connection, Row

from underfit_api.helpers import utcnow
from underfit_api.models import Account
from underfit_api.repositories import organizations as organizations_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import account_aliases, accounts


def exists(conn: Connection, handle: str) -> bool:
    return get_by_handle(conn, handle) is not None


def get_by_id(conn: Connection, account_id: UUID) -> Account | None:
    if not (account := conn.execute(accounts.select().where(accounts.c.id == account_id)).first()):
        return None
    if account.type == "ORGANIZATION":
        return organizations_repo.get_by_id(conn, account.id)
    return users_repo.get_by_id(conn, account.id)


def get_by_handle(conn: Connection, handle: str) -> Account | None:
    if not (account := conn.execute(accounts.select().where(accounts.c.handle == handle.lower())).first()):
        return None
    if account.type == "ORGANIZATION":
        return organizations_repo.get_by_id(conn, account.id)
    return users_repo.get_by_id(conn, account.id)


def rename(conn: Connection, account_id: UUID, new_handle: str) -> None:
    conn.execute(accounts.update().where(accounts.c.id == account_id).values(handle=new_handle))


def create_alias(conn: Connection, account_id: UUID, handle: str) -> None:
    conn.execute(account_aliases.insert().values(
        id=uuid4(), account_id=account_id, handle=handle, created_at=utcnow(),
    ))


def get_alias_by_handle(conn: Connection, handle: str) -> Row | None:
    return conn.execute(
        account_aliases.select().where(account_aliases.c.handle == handle.lower()),
    ).first()


def alias_handle_exists(conn: Connection, handle: str) -> bool:
    return get_alias_by_handle(conn, handle) is not None
