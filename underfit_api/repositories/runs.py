from __future__ import annotations

from collections.abc import Iterable
from datetime import timedelta
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.models import Run
from underfit_api.repositories.projects import is_collaborator, is_org_admin
from underfit_api.schema import accounts, projects, run_workers, runs

if TYPE_CHECKING:
    from underfit_api.buffer import ScalarPoint

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
        runs.c.metadata,
        runs.c.summary,
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
    pk = uuid4()
    now = utcnow()
    conn.execute(runs.insert().values(
        id=pk, project_id=project_id, user_id=user_id,
        launch_id=launch_id, name=name.lower(), storage_key=str(pk),
        config=config, metadata=metadata, summary={}, created_at=now, updated_at=now,
    ))
    result = get_by_id(conn, pk)
    assert result is not None
    return result


def update(conn: Connection, pk: UUID, metadata: dict[str, object] | None) -> Run | None:
    values: dict[str, object] = {"updated_at": utcnow()}
    if metadata is not None:
        values["metadata"] = metadata
    conn.execute(runs.update().where(runs.c.id == pk).values(**values))
    return get_by_id(conn, pk)


def update_terminal_state(conn: Connection, pk: UUID, terminal_state: str) -> Run | None:
    conn.execute(runs.update().where(runs.c.id == pk).values(terminal_state=terminal_state, updated_at=utcnow()))
    return get_by_id(conn, pk)


def delete(conn: Connection, pk: UUID) -> None:
    conn.execute(runs.delete().where(runs.c.id == pk))


def update_summary(conn: Connection, pk: UUID, points: Iterable[ScalarPoint]) -> None:
    row = conn.execute(sa.select(runs.c.summary).where(runs.c.id == pk).with_for_update()).first()
    if row is None:
        return
    summary: dict[str, dict[str, object]] = dict(row.summary or {})
    for point in points:
        ts = point.timestamp.isoformat() + "Z"
        for key, value in point.values.items():
            entry = summary.get(key)
            if entry is None or _is_newer(point.step, ts, entry):
                summary[key] = {"value": value, "step": point.step, "timestamp": ts}
    conn.execute(runs.update().where(runs.c.id == pk).values(summary=summary))


def _is_newer(step: int | None, ts: str, existing: dict[str, object]) -> bool:
    existing_step, existing_ts = existing["step"], existing["timestamp"]
    if step is not None and isinstance(existing_step, int):
        return step > existing_step
    return isinstance(existing_ts, str) and ts > existing_ts
