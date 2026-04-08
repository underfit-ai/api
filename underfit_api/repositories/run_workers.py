from __future__ import annotations

from datetime import timedelta
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.models import Worker
from underfit_api.schema import run_workers

_columns = [run_workers.c.id, run_workers.c.run_id, run_workers.c.worker_label,
            run_workers.c.last_heartbeat, run_workers.c.joined_at]


def create(conn: Connection, run_id: UUID, worker_label: str) -> Worker:
    row_id = uuid4()
    now = utcnow()
    conn.execute(run_workers.insert().values(
        id=row_id, run_id=run_id, worker_label=worker_label, last_heartbeat=now, joined_at=now,
    ))
    return Worker(id=row_id, run_id=run_id, worker_label=worker_label, last_heartbeat=now, joined_at=now)


def list_by_run(conn: Connection, run_id: UUID) -> list[Worker]:
    rows = conn.execute(
        sa.select(*_columns).where(run_workers.c.run_id == run_id).order_by(run_workers.c.joined_at),
    ).all()
    return [Worker.model_validate(r) for r in rows]


def get(conn: Connection, run_id: UUID, worker_label: str) -> Worker | None:
    row = conn.execute(
        sa.select(*_columns).where(run_workers.c.run_id == run_id, run_workers.c.worker_label == worker_label),
    ).first()
    return Worker.model_validate(row) if row else None


def get_by_id(conn: Connection, worker_id: UUID) -> Worker | None:
    row = conn.execute(sa.select(*_columns).where(run_workers.c.id == worker_id)).first()
    return Worker.model_validate(row) if row else None


def touch(conn: Connection, worker_id: UUID) -> bool:
    return conn.execute(
        run_workers.update().where(run_workers.c.id == worker_id).values(last_heartbeat=utcnow()),
    ).rowcount > 0


def get_inactive_ids(conn: Connection, worker_ids: set[UUID]) -> set[UUID]:
    if not worker_ids:
        return set()
    cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
    rows = conn.execute(
        sa.select(run_workers.c.id).where(run_workers.c.id.in_(worker_ids), run_workers.c.last_heartbeat < cutoff),
    ).all()
    return {row.id for row in rows}
