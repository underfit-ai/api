from __future__ import annotations

from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection

from underfit_api.helpers import utcnow
from underfit_api.models import Worker
from underfit_api.schema import run_workers, runs

_join = run_workers.join(runs, run_workers.c.run_id == runs.c.id)
_columns = [
    run_workers.c.id,
    run_workers.c.run_id,
    run_workers.c.worker_label,
    (run_workers.c.id == runs.c.primary_worker_id).label("is_primary"),
    run_workers.c.status,
    run_workers.c.joined_at,
]


def create(conn: Connection, run_id: UUID, worker_label: str, status: str, is_primary: bool) -> Worker:
    row_id = uuid4()
    now = utcnow()
    conn.execute(run_workers.insert().values(
        id=row_id, run_id=run_id, worker_label=worker_label, status=status, joined_at=now,
    ))
    if is_primary:
        conn.execute(runs.update().where(runs.c.id == run_id).values(primary_worker_id=row_id))
    return Worker(
        id=row_id, run_id=run_id, worker_label=worker_label, is_primary=is_primary, status=status, joined_at=now,
    )


def list_by_run(conn: Connection, run_id: UUID) -> list[Worker]:
    rows = conn.execute(
        sa.select(*_columns).select_from(_join)
        .where(run_workers.c.run_id == run_id)
        .order_by(run_workers.c.joined_at),
    ).all()
    return [Worker.model_validate(r) for r in rows]


def get(conn: Connection, run_id: UUID, worker_label: str) -> Worker | None:
    row = conn.execute(
        sa.select(*_columns).select_from(_join)
        .where(run_workers.c.run_id == run_id, run_workers.c.worker_label == worker_label),
    ).first()
    return Worker.model_validate(row) if row else None


def update_status(conn: Connection, run_id: UUID, worker_label: str, status: str) -> Worker | None:
    conn.execute(
        run_workers.update()
        .where(run_workers.c.run_id == run_id, run_workers.c.worker_label == worker_label)
        .values(status=status),
    )
    return get(conn, run_id, worker_label)
