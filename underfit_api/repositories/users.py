from __future__ import annotations

from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Body, User
from underfit_api.schema import accounts, users

SEARCH_LIMIT = 20

_base_query = users.join(accounts, users.c.id == accounts.c.id).select


class UserSettings(Body):
    name: str | None = None
    bio: str | None = None


def get_by_id(conn: Connection, user_id: UUID) -> User | None:
    row = conn.execute(_base_query().where(users.c.id == user_id)).first()
    return User.model_validate(row) if row else None


def get_by_handle(conn: Connection, handle: str) -> User | None:
    row = conn.execute(_base_query().where(accounts.c.handle == handle.lower())).first()
    return User.model_validate(row) if row else None


def get_by_email(conn: Connection, email: str) -> User | None:
    row = conn.execute(_base_query().where(users.c.email == email)).first()
    return User.model_validate(row) if row else None


def create(conn: Connection, email: str, handle: str, name: str) -> User:
    user_id = uuid4()
    now = utcnow()
    conn.execute(accounts.insert().values(id=user_id, handle=handle, type="USER"))
    conn.execute(users.insert().values(id=user_id, email=email, name=name, bio="", created_at=now, updated_at=now))
    result = get_by_id(conn, user_id)
    assert result is not None
    return result


def update_settings(conn: Connection, user_id: UUID, patch: UserSettings) -> User:
    if values := patch.model_dump(exclude_unset=True):
        conn.execute(users.update().where(users.c.id == user_id).values(updated_at=utcnow(), **values))
    result = get_by_id(conn, user_id)
    assert result is not None
    return result


def search(conn: Connection, query: str) -> list[User]:
    base = _base_query()
    if "@" in query:
        rows = conn.execute(
            base.where(users.c.email.istartswith(query)).order_by(users.c.email).limit(SEARCH_LIMIT),
        ).all()
        return [User.model_validate(row) for row in rows]

    name_rows = conn.execute(
        base.where(sa.func.lower(users.c.name) == query.lower()).order_by(accounts.c.handle).limit(SEARCH_LIMIT),
    ).all()
    seen_ids = {row.id for row in name_rows}
    handle_query = base.where(accounts.c.handle.istartswith(query))
    if seen_ids:
        handle_query = handle_query.where(~accounts.c.id.in_(seen_ids))
    handle_query = handle_query.order_by(sa.func.length(accounts.c.handle), accounts.c.handle).limit(SEARCH_LIMIT)
    handle_rows = conn.execute(handle_query).all()
    return [User.model_validate(row) for row in [*name_rows, *handle_rows][:SEARCH_LIMIT]]
