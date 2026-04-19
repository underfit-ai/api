from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID, uuid4

import sqlalchemy as sa
from sqlalchemy import Connection
from sqlalchemy.engine import Row

from underfit_api.helpers import utcnow
from underfit_api.schema import scalar_segments


def get_end_line(conn: Connection, worker_id: UUID, resolution: int) -> int:
    row = conn.execute(
        scalar_segments.select()
        .where(scalar_segments.c.worker_id == worker_id, scalar_segments.c.resolution == resolution)
        .order_by(scalar_segments.c.end_line.desc()).limit(1),
    ).first()
    return row.end_line if row else 0


def upsert(
    conn: Connection, worker_id: UUID, resolution: int, start_line: int, end_line: int, end_step: int | None,
    start_at: datetime, end_at: datetime, storage_key: str,
) -> None:
    updated = conn.execute(scalar_segments.update().where(
        scalar_segments.c.worker_id == worker_id, scalar_segments.c.resolution == resolution,
        scalar_segments.c.start_line == start_line,
    ).values(end_line=end_line, end_step=end_step, end_at=end_at, storage_key=storage_key)).rowcount
    if updated == 0:
        conn.execute(scalar_segments.insert().values(
            id=uuid4(), worker_id=worker_id, resolution=resolution, start_line=start_line, end_line=end_line,
            end_step=end_step, start_at=start_at, end_at=end_at, storage_key=storage_key, created_at=utcnow(),
        ))


def list_by_resolution(conn: Connection, worker_id: UUID, resolution: int) -> Sequence[Row]:
    return conn.execute(
        scalar_segments.select()
        .where(scalar_segments.c.worker_id == worker_id, scalar_segments.c.resolution == resolution)
        .order_by(scalar_segments.c.start_line),
    ).all()


def get_last_step(conn: Connection, worker_id: UUID) -> int | None:
    return conn.execute(sa.select(sa.func.max(scalar_segments.c.end_step)).where(
        scalar_segments.c.worker_id == worker_id, scalar_segments.c.resolution == 1,
    )).scalar()


def get_last_timestamp(conn: Connection, worker_id: UUID) -> datetime | None:
    return conn.execute(sa.select(sa.func.max(scalar_segments.c.end_at)).where(
        scalar_segments.c.worker_id == worker_id, scalar_segments.c.resolution == 1,
    )).scalar()
