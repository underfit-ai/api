from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Connection, Row

from underfit_api.helpers import utcnow
from underfit_api.models import Account, User
from underfit_api.repositories import organizations as organizations_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import account_aliases, accounts

LOCAL_USER_EMAIL = "local@underfit.local"
LOCAL_USER_HANDLE = "local"


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
    return conn.execute(account_aliases.select().where(account_aliases.c.handle == handle.lower())).first()


def get_or_create_local(conn: Connection) -> User:
    if user := users_repo.get_by_email(conn, LOCAL_USER_EMAIL):
        return user
    user = users_repo.create(conn, LOCAL_USER_EMAIL, LOCAL_USER_HANDLE, "Local User")
    create_alias(conn, user.id, LOCAL_USER_HANDLE)
    return user
