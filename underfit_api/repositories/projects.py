from __future__ import annotations

from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection, Row, func

from underfit_api.helpers import utcnow
from underfit_api.models import Project
from underfit_api.schema import accounts, organization_members, project_aliases, project_collaborators, projects, runs

_join = projects.join(accounts, projects.c.account_id == accounts.c.id)
_columns = [
    projects.c.id,
    accounts.c.handle.label("owner"),
    projects.c.name,
    projects.c.description,
    projects.c.metadata,
    projects.c.visibility,
    projects.c.pending_transfer_to,
    projects.c.created_at,
    projects.c.updated_at,
]


def is_collaborator(user_id: UUID) -> sa.Exists:
    return sa.exists(sa.select(1).where(
        project_collaborators.c.project_id == projects.c.id,
        project_collaborators.c.user_id == user_id,
    ))


def is_org_admin(user_id: UUID) -> sa.Exists:
    return sa.exists(sa.select(1).where(
        organization_members.c.organization_id == projects.c.account_id,
        organization_members.c.user_id == user_id,
        organization_members.c.role == "ADMIN",
    ))


def get_by_id(conn: Connection, project_id: UUID) -> Project | None:
    row = conn.execute(sa.select(*_columns).select_from(_join).where(projects.c.id == project_id)).first()
    return Project.model_validate(row) if row else None


def get_by_account_and_name(conn: Connection, account_id: UUID, name: str) -> Project | None:
    row = conn.execute(
        sa.select(*_columns).select_from(_join).where(
            projects.c.account_id == account_id, projects.c.name == name.lower(),
        ),
    ).first()
    return Project.model_validate(row) if row else None


def list_visible_by_account(conn: Connection, account_id: UUID, viewer_id: UUID | None) -> list[Project]:
    visibility_filters: list[sa.ColumnElement[bool]] = [projects.c.visibility == "public"]
    if viewer_id is not None:
        visibility_filters.extend([
            projects.c.account_id == viewer_id,
            is_collaborator(viewer_id),
            is_org_admin(viewer_id),
        ])
    rows = conn.execute(
        sa.select(*_columns).select_from(_join)
        .where(projects.c.account_id == account_id, sa.or_(*visibility_filters))
        .order_by(projects.c.created_at.desc()),
    ).all()
    return [Project.model_validate(row) for row in rows]


def list_related_to_user(conn: Connection, user_id: UUID) -> list[Project]:
    run_count = func.count(runs.c.id).label("run_count")
    j = _join.outerjoin(runs, (runs.c.project_id == projects.c.id) & (runs.c.user_id == user_id))
    rows = conn.execute(
        sa.select(*_columns, run_count).select_from(j)
        .where(sa.or_(projects.c.account_id == user_id, is_collaborator(user_id), is_org_admin(user_id)))
        .group_by(projects.c.id)
        .order_by(run_count.desc(), projects.c.updated_at.desc()),
    ).all()
    return [Project.model_validate(row) for row in rows]


def create(
    conn: Connection, account_id: UUID, name: str, description: str | None, visibility: str,
    metadata: dict[str, object],
) -> Project:
    project_id = uuid4()
    now = utcnow()
    conn.execute(projects.insert().values(
        id=project_id, account_id=account_id, name=name, description=description,
        metadata=metadata, visibility=visibility, created_at=now, updated_at=now,
    ))
    result = get_by_id(conn, project_id)
    assert result is not None
    return result


def update(
    conn: Connection, project_id: UUID, description: str | None, visibility: str | None, metadata: dict[str, object],
) -> Project | None:
    values: dict[str, object] = {"updated_at": utcnow(), "metadata": metadata}
    if description is not None:
        values["description"] = description
    if visibility is not None:
        values["visibility"] = visibility
    conn.execute(projects.update().where(projects.c.id == project_id).values(**values))
    return get_by_id(conn, project_id)


def rename(conn: Connection, project_id: UUID, new_name: str) -> Project | None:
    conn.execute(projects.update().where(projects.c.id == project_id).values(name=new_name, updated_at=utcnow()))
    return get_by_id(conn, project_id)


def set_pending_transfer(conn: Connection, project_id: UUID, to_account_id: UUID | None) -> None:
    conn.execute(
        projects.update().where(projects.c.id == project_id)
        .values(pending_transfer_to=to_account_id, updated_at=utcnow()),
    )


def transfer(conn: Connection, project_id: UUID, new_account_id: UUID, new_name: str) -> Project | None:
    now = utcnow()
    conn.execute(
        projects.update().where(projects.c.id == project_id).values(
            account_id=new_account_id, name=new_name, pending_transfer_to=None, updated_at=now,
        ),
    )
    return get_by_id(conn, project_id)


def create_alias(conn: Connection, project_id: UUID, account_id: UUID, name: str) -> None:
    conn.execute(project_aliases.insert().values(
        id=uuid4(), project_id=project_id, account_id=account_id, name=name, created_at=utcnow(),
    ))


def get_alias_by_account_and_name(conn: Connection, account_id: UUID, name: str) -> Row | None:
    return conn.execute(
        project_aliases.select().where(
            project_aliases.c.account_id == account_id, project_aliases.c.name == name.lower(),
        ),
    ).first()
