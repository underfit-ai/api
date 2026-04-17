from __future__ import annotations

from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection, func

from underfit_api.helpers import utcnow
from underfit_api.models import Project
from underfit_api.schema import accounts, organization_members, project_collaborators, projects, runs

_join = projects.join(accounts, projects.c.account_id == accounts.c.id)
_columns = [
    projects.c.id,
    accounts.c.handle.label("owner"),
    accounts.c.id.label("account_id"),
    accounts.c.type.label("account_type"),
    projects.c.name,
    projects.c.storage_key,
    projects.c.description,
    projects.c.metadata,
    projects.c.ui_state,
    projects.c.baseline_run_id,
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
    row = conn.execute(sa.select(*_columns).select_from(_join).where(
        projects.c.account_id == account_id, projects.c.name == name.lower(),
    )).first()
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
    run_counts = sa.select(
        runs.c.project_id, func.count(runs.c.id).label("run_count"),
    ).where(runs.c.user_id == user_id).group_by(runs.c.project_id).subquery()
    run_count = func.coalesce(run_counts.c.run_count, 0).label("run_count")
    j = _join.outerjoin(run_counts, run_counts.c.project_id == projects.c.id)
    rows = conn.execute(
        sa.select(*_columns, run_count).select_from(j)
        .where(sa.or_(projects.c.account_id == user_id, is_collaborator(user_id), is_org_admin(user_id)))
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
        id=project_id, account_id=account_id, name=name, storage_key=str(project_id), description=description,
        metadata=metadata, ui_state={}, visibility=visibility, created_at=now, updated_at=now,
    ))
    result = get_by_id(conn, project_id)
    assert result is not None
    return result


def update(
    conn: Connection, project_id: UUID, *,
    description: str | None = None, visibility: str | None = None,
    metadata: dict[str, object] | None = None, ui_state: dict[str, object] | None = None,
) -> Project:
    values = {
        "updated_at": utcnow(), "description": description, "visibility": visibility,
        "metadata": metadata, "ui_state": ui_state,
    }
    values = {k: v for k, v in values.items() if v is not None}
    conn.execute(projects.update().where(projects.c.id == project_id).values(**values))
    result = get_by_id(conn, project_id)
    assert result is not None
    return result


def set_baseline_run(conn: Connection, project_id: UUID, run_id: UUID | None) -> None:
    conn.execute(projects.update().where(projects.c.id == project_id).values(
        baseline_project_id=project_id if run_id is not None else None, baseline_run_id=run_id, updated_at=utcnow(),
    ))


def rename(conn: Connection, project_id: UUID, new_name: str) -> Project:
    conn.execute(projects.update().where(projects.c.id == project_id).values(name=new_name, updated_at=utcnow()))
    result = get_by_id(conn, project_id)
    assert result is not None
    return result


def set_pending_transfer(conn: Connection, project_id: UUID, to_account_id: UUID | None) -> None:
    conn.execute(
        projects.update().where(projects.c.id == project_id)
        .values(pending_transfer_to=to_account_id, updated_at=utcnow()),
    )


def transfer(conn: Connection, project_id: UUID, new_account_id: UUID, new_name: str) -> Project:
    conn.execute(projects.update().where(projects.c.id == project_id).values(
        account_id=new_account_id, name=new_name, pending_transfer_to=None, updated_at=utcnow(),
    ))
    result = get_by_id(conn, project_id)
    assert result is not None
    return result


def delete(conn: Connection, project_id: UUID) -> None:
    conn.execute(projects.delete().where(projects.c.id == project_id))
