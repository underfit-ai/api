from __future__ import annotations

from uuid import UUID

from sqlalchemy import Connection

from underfit_api.models import Account, User
from underfit_api.repositories import organizations as organizations_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import accounts

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


def get_or_create_local(conn: Connection) -> User:
    if user := users_repo.get_by_email(conn, LOCAL_USER_EMAIL):
        return user
    return users_repo.create(conn, LOCAL_USER_EMAIL, LOCAL_USER_HANDLE, "Local User")
