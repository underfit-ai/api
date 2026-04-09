from __future__ import annotations

from uuid import UUID, uuid4

from sqlalchemy import Connection, select

from underfit_api.helpers import utcnow
from underfit_api.models import ProjectCollaborator
from underfit_api.schema import accounts, project_collaborators, users

_join = project_collaborators.join(users, project_collaborators.c.user_id == users.c.id).join(
    accounts, users.c.id == accounts.c.id,
)
_select = select(
    users,
    accounts.c.handle,
    accounts.c.type,
    project_collaborators.c.created_at.label("collaborator_created_at"),
    project_collaborators.c.updated_at.label("collaborator_updated_at"),
).select_from(_join)


def list_by_project(conn: Connection, project_id: UUID) -> list[ProjectCollaborator]:
    rows = conn.execute(_select.where(project_collaborators.c.project_id == project_id)).all()
    return [ProjectCollaborator.model_validate(row) for row in rows]


def get(conn: Connection, project_id: UUID, user_id: UUID) -> ProjectCollaborator | None:
    row = conn.execute(
        _select.where(project_collaborators.c.project_id == project_id, project_collaborators.c.user_id == user_id),
    ).first()
    return ProjectCollaborator.model_validate(row) if row else None


def add(conn: Connection, project_id: UUID, user_id: UUID) -> ProjectCollaborator:
    now = utcnow()
    conn.execute(project_collaborators.insert().values(
        id=uuid4(), project_id=project_id, user_id=user_id, created_at=now, updated_at=now,
    ))
    result = get(conn, project_id, user_id)
    assert result is not None
    return result


def remove(conn: Connection, project_id: UUID, user_id: UUID) -> None:
    conn.execute(project_collaborators.delete().where(
        project_collaborators.c.project_id == project_id, project_collaborators.c.user_id == user_id,
    ))
