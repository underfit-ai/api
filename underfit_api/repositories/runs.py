from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.models import Body, Run
from underfit_api.repositories.projects import is_collaborator, is_org_admin
from underfit_api.schema import accounts, projects, run_workers, runs

_join = runs.join(projects, runs.c.project_id == projects.c.id).join(accounts, projects.c.account_id == accounts.c.id)
_user_handle = sa.select(accounts.c.handle).where(accounts.c.id == runs.c.user_id).correlate(runs).scalar_subquery()


class RunSettings(Body):
    ui_state: dict[str, object] | None = None
    is_pinned: bool | None = None


def _query() -> sa.Select[tuple[object, ...]]:
    cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
    is_active = sa.exists(sa.select(1).where(
        run_workers.c.run_id == runs.c.id,
        run_workers.c.last_heartbeat >= cutoff,
    )).label("is_active")
    is_baseline = sa.case((runs.c.id == projects.c.baseline_run_id, True), else_=False).label("is_baseline")
    return sa.select(
        runs.c.id,
        runs.c.project_id,
        accounts.c.handle.label("project_owner"),
        accounts.c.id.label("project_owner_id"),
        accounts.c.type.label("project_owner_type"),
        projects.c.name.label("project_name"),
        _user_handle.label("user"),
        runs.c.launch_id,
        runs.c.name,
        runs.c.storage_key,
        runs.c.terminal_state,
        is_active,
        runs.c.config,
        runs.c.metadata,
        runs.c.ui_state,
        runs.c.is_pinned,
        is_baseline,
        runs.c.summary,
        runs.c.created_at,
        runs.c.updated_at,
    ).select_from(_join)


def get_by_id(conn: Connection, run_id: UUID) -> Run | None:
    row = conn.execute(_query().where(runs.c.id == run_id)).first()
    return Run.model_validate(row) if row else None


def get_by_project_and_name(conn: Connection, project_id: UUID, name: str) -> Run | None:
    row = conn.execute(_query().where(runs.c.project_id == project_id, runs.c.name == name.lower())).first()
    return Run.model_validate(row) if row else None


def get_by_project_and_launch_id(conn: Connection, project_id: UUID, launch_id: str) -> Run | None:
    row = conn.execute(_query().where(runs.c.project_id == project_id, runs.c.launch_id == launch_id)).first()
    return Run.model_validate(row) if row else None


def has_active_worker(conn: Connection, run_id: UUID) -> bool:
    cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
    row = conn.execute(sa.select(1).where(
        run_workers.c.run_id == run_id, run_workers.c.last_heartbeat >= cutoff,
    )).first()
    return row is not None


def list_by_project(conn: Connection, project_id: UUID) -> list[Run]:
    is_baseline = sa.case((runs.c.id == projects.c.baseline_run_id, 1), else_=0)
    rows = conn.execute(_query().where(runs.c.project_id == project_id).order_by(
        is_baseline.desc(), runs.c.is_pinned.desc(), runs.c.created_at.desc(),
    )).all()
    return [Run.model_validate(row) for row in rows]


def list_visible_by_user(conn: Connection, user_id: UUID, viewer_id: UUID | None) -> list[Run]:
    visibility_filters: list[sa.ColumnElement[bool]] = [projects.c.visibility == "public"]
    if viewer_id is not None:
        visibility_filters.extend([
            projects.c.account_id == viewer_id,
            is_collaborator(viewer_id),
            is_org_admin(viewer_id),
        ])
    rows = conn.execute(
        _query().where(runs.c.user_id == user_id, sa.or_(*visibility_filters)).order_by(runs.c.created_at.desc()),
    ).all()
    return [Run.model_validate(row) for row in rows]


def create(
    conn: Connection, project_id: UUID, user_id: UUID, launch_id: str, name: str, config: dict[str, object] | None,
    metadata: dict[str, object],
) -> Run:
    run_id = uuid4()
    now = utcnow()
    conn.execute(runs.insert().values(
        id=run_id, project_id=project_id, user_id=user_id,
        launch_id=launch_id, name=name.lower(), storage_key=str(run_id),
        config=config, metadata=metadata, ui_state={}, is_pinned=False, summary={}, created_at=now, updated_at=now,
    ))
    result = get_by_id(conn, run_id)
    assert result is not None
    return result


def _update(conn: Connection, run_id: UUID, **values: object) -> Run:
    if values:
        conn.execute(runs.update().where(runs.c.id == run_id).values(updated_at=utcnow(), **values))
    result = get_by_id(conn, run_id)
    assert result is not None
    return result


def update_metadata(conn: Connection, run_id: UUID, metadata: dict[str, object]) -> Run:
    return _update(conn, run_id, metadata=metadata)


def update_terminal_state(conn: Connection, run_id: UUID, terminal_state: str) -> Run:
    return _update(conn, run_id, terminal_state=terminal_state)


def update_summary(conn: Connection, run_id: UUID, summary: dict[str, float]) -> Run:
    return _update(conn, run_id, summary=summary)


def update_settings(conn: Connection, run_id: UUID, patch: RunSettings) -> Run:
    return _update(conn, run_id, **patch.model_dump(exclude_unset=True))


def delete(conn: Connection, run_id: UUID) -> None:
    conn.execute(runs.delete().where(runs.c.id == run_id))
