from __future__ import annotations

import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy import Connection

from app.models import User
from app.schema import accounts, collaborators, users

_join = collaborators.join(users, collaborators.c.user_id == users.c.id).join(
    accounts, users.c.id == accounts.c.id,
)
_columns = [
    users.c.id,
    accounts.c.handle,
    accounts.c.type,
    users.c.email,
    users.c.name,
    users.c.bio,
    users.c.created_at,
    users.c.updated_at,
]


def list_by_project(conn: Connection, project_id: uuid.UUID) -> list[User]:
    rows = conn.execute(
        sa.select(*_columns).select_from(_join).where(collaborators.c.project_id == project_id),
    ).all()
    return [User.model_validate(row) for row in rows]


def get(conn: Connection, project_id: uuid.UUID, user_id: uuid.UUID) -> bool:
    row = conn.execute(
        collaborators.select().where(
            collaborators.c.project_id == project_id, collaborators.c.user_id == user_id,
        ),
    ).first()
    return row is not None


def add(conn: Connection, project_id: uuid.UUID, user_id: uuid.UUID) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(collaborators.insert().values(
        id=uuid.uuid4(), project_id=project_id, user_id=user_id, created_at=now, updated_at=now,
    ))


def remove(conn: Connection, project_id: uuid.UUID, user_id: uuid.UUID) -> None:
    conn.execute(collaborators.delete().where(
        collaborators.c.project_id == project_id, collaborators.c.user_id == user_id,
    ))
