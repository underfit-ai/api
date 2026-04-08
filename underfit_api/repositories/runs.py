from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.models import Run
from underfit_api.schema import accounts, projects, run_workers, runs

_join = runs.join(projects, runs.c.project_id == projects.c.id).join(accounts, projects.c.account_id == accounts.c.id)
_user_handle = sa.select(accounts.c.handle).where(accounts.c.id == runs.c.user_id).correlate(runs).scalar_subquery()


def _query() -> sa.Select[tuple[object, ...]]:
    cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
    is_active = sa.exists(sa.select(1).where(
        run_workers.c.run_id == runs.c.id,
        run_workers.c.last_heartbeat >= cutoff,
    )).label("is_active")
    return sa.select(
        runs.c.id,
        runs.c.project_id,
        accounts.c.handle.label("project_owner"),
        projects.c.name.label("project_name"),
        _user_handle.label("user"),
        runs.c.launch_id,
        runs.c.name,
        runs.c.storage_key,
        runs.c.terminal_state,
        is_active,
        runs.c.config,
        runs.c.created_at,
        runs.c.updated_at,
    ).select_from(_join)


def get_by_id(conn: Connection, pk: UUID) -> Run | None:
    row = conn.execute(_query().where(runs.c.id == pk)).first()
    return Run.model_validate(row) if row else None


def get_by_project_and_name(conn: Connection, project_id: UUID, name: str) -> Run | None:
    row = conn.execute(_query().where(runs.c.project_id == project_id, runs.c.name == name.lower())).first()
    return Run.model_validate(row) if row else None


def get_by_project_and_launch_id(conn: Connection, project_id: UUID, launch_id: str) -> Run | None:
    row = conn.execute(_query().where(runs.c.project_id == project_id, runs.c.launch_id == launch_id)).first()
    return Run.model_validate(row) if row else None


def has_active_worker(conn: Connection, pk: UUID) -> bool:
    cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
    row = conn.execute(sa.select(1).where(
        run_workers.c.run_id == pk, run_workers.c.last_heartbeat >= cutoff,
    )).first()
    return row is not None


def list_by_project(conn: Connection, project_id: UUID) -> list[Run]:
    rows = conn.execute(_query().where(runs.c.project_id == project_id).order_by(runs.c.created_at.desc())).all()
    return [Run.model_validate(row) for row in rows]


def list_by_user(conn: Connection, user_id: UUID) -> list[Run]:
    rows = conn.execute(_query().where(runs.c.user_id == user_id).order_by(runs.c.created_at.desc())).all()
    return [Run.model_validate(row) for row in rows]


def create(
    conn: Connection, project_id: UUID, user_id: UUID, launch_id: str, name: str, config: dict[str, object] | None,
) -> Run:
    pk = uuid4()
    now = utcnow()
    conn.execute(runs.insert().values(
        id=pk, project_id=project_id, user_id=user_id,
        launch_id=launch_id, name=name.lower(), storage_key=str(pk), config=config, created_at=now, updated_at=now,
    ))
    result = get_by_id(conn, pk)
    assert result is not None
    return result


def update(conn: Connection, pk: UUID, config: dict[str, object] | None, update_config: bool) -> Run | None:
    values: dict[str, object] = {"updated_at": utcnow()}
    if update_config:
        values["config"] = config
    conn.execute(runs.update().where(runs.c.id == pk).values(**values))
    return get_by_id(conn, pk)


def update_terminal_state(conn: Connection, pk: UUID, terminal_state: str) -> Run | None:
    conn.execute(runs.update().where(runs.c.id == pk).values(terminal_state=terminal_state, updated_at=utcnow()))
    return get_by_id(conn, pk)
