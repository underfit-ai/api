from __future__ import annotations

from typing import Any
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.models import Account, Organization, User
from underfit_api.repositories import users as users_repo
from underfit_api.schema import accounts, organizations, users

LOCAL_USER_EMAIL = "local@underfit.local"
LOCAL_USER_HANDLE = "local"

_query = sa.select(
    accounts.c.id, accounts.c.handle, accounts.c.type,
    sa.func.coalesce(users.c.name, organizations.c.name).label("name"),
    users.c.email, users.c.bio,
    sa.func.coalesce(users.c.created_at, organizations.c.created_at).label("created_at"),
    sa.func.coalesce(users.c.updated_at, organizations.c.updated_at).label("updated_at"),
).select_from(
    accounts
    .outerjoin(users, users.c.id == accounts.c.id)
    .outerjoin(organizations, organizations.c.id == accounts.c.id),
)


def _to_account(row: sa.Row[Any]) -> Account:
    return Organization.model_validate(row) if row.type == "ORGANIZATION" else User.model_validate(row)


def get_by_id(conn: Connection, account_id: UUID) -> Account | None:
    row = conn.execute(_query.where(accounts.c.id == account_id)).first()
    return _to_account(row) if row else None


def get_by_handle(conn: Connection, handle: str) -> Account | None:
    row = conn.execute(_query.where(accounts.c.handle == handle.lower())).first()
    return _to_account(row) if row else None


def rename(conn: Connection, account_id: UUID, new_handle: str) -> Account:
    conn.execute(accounts.update().where(accounts.c.id == account_id).values(handle=new_handle))
    result = get_by_id(conn, account_id)
    assert result is not None
    return result


def get_or_create_local(conn: Connection) -> User:
    if user := users_repo.get_by_email(conn, LOCAL_USER_EMAIL):
        return user
    return users_repo.create(conn, LOCAL_USER_EMAIL, LOCAL_USER_HANDLE, "Local User")
