from __future__ import annotations

import uuid
from datetime import datetime, timezone

import sqlalchemy as sa
from sqlalchemy import Connection, func

from app.models import Project
from app.schema import accounts, projects, runs

_join = projects.join(accounts, projects.c.account_id == accounts.c.id)
_columns = [
    projects.c.id,
    accounts.c.handle.label("owner"),
    projects.c.name,
    projects.c.description,
    projects.c.visibility,
    projects.c.created_at,
    projects.c.updated_at,
]


def get_by_id(conn: Connection, project_id: uuid.UUID) -> Project | None:
    row = conn.execute(sa.select(*_columns).select_from(_join).where(projects.c.id == project_id)).first()
    return Project.model_validate(row) if row else None


def get_by_account_and_name(conn: Connection, account_id: uuid.UUID, name: str) -> Project | None:
    row = conn.execute(
        sa.select(*_columns).select_from(_join).where(
            projects.c.account_id == account_id, projects.c.name == name.lower(),
        ),
    ).first()
    return Project.model_validate(row) if row else None


def list_by_account(conn: Connection, account_id: uuid.UUID) -> list[Project]:
    rows = conn.execute(
        sa.select(*_columns).select_from(_join)
        .where(projects.c.account_id == account_id)
        .order_by(projects.c.created_at.desc()),
    ).all()
    return [Project.model_validate(row) for row in rows]


def list_by_user_run_count(conn: Connection, user_id: uuid.UUID) -> list[Project]:
    run_count = func.count(runs.c.id).label("run_count")
    j = _join.outerjoin(runs, (runs.c.project_id == projects.c.id) & (runs.c.user_id == user_id))
    rows = conn.execute(
        sa.select(*_columns, run_count).select_from(j).group_by(projects.c.id).order_by(run_count.desc()),
    ).all()
    return [Project.model_validate(row) for row in rows]


def create(
    conn: Connection, account_id: uuid.UUID, name: str, description: str | None, visibility: str,
) -> Project:
    project_id = uuid.uuid4()
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(projects.insert().values(
        id=project_id, account_id=account_id, name=name, description=description,
        visibility=visibility, created_at=now, updated_at=now,
    ))
    result = get_by_id(conn, project_id)
    assert result is not None
    return result


def update(
    conn: Connection, project_id: uuid.UUID, description: str | None, visibility: str | None,
) -> Project | None:
    values: dict[str, object] = {"updated_at": datetime.now(timezone.utc).replace(tzinfo=None)}
    if description is not None:
        values["description"] = description
    if visibility is not None:
        values["visibility"] = visibility
    conn.execute(projects.update().where(projects.c.id == project_id).values(**values))
    return get_by_id(conn, project_id)
