from __future__ import annotations

from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Collaborator, User
from underfit_api.schema import accounts, collaborators, users

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


def list_by_project(conn: Connection, project_id: UUID) -> list[User]:
    rows = conn.execute(
        sa.select(*_columns).select_from(_join).where(collaborators.c.project_id == project_id),
    ).all()
    return [User.model_validate(row) for row in rows]


def get(conn: Connection, project_id: UUID, user_id: UUID) -> bool:
    row = conn.execute(
        collaborators.select().where(
            collaborators.c.project_id == project_id, collaborators.c.user_id == user_id,
        ),
    ).first()
    return row is not None


def add(conn: Connection, project_id: UUID, user_id: UUID) -> Collaborator:
    now = utcnow()
    collab_id = uuid4()
    conn.execute(collaborators.insert().values(
        id=collab_id, project_id=project_id, user_id=user_id, created_at=now, updated_at=now,
    ))
    return Collaborator(id=collab_id, project_id=project_id, user_id=user_id, created_at=now, updated_at=now)


def remove(conn: Connection, project_id: UUID, user_id: UUID) -> None:
    conn.execute(collaborators.delete().where(
        collaborators.c.project_id == project_id, collaborators.c.user_id == user_id,
    ))
