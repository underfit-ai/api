from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import Connection
from sqlalchemy.engine import Row

from underfit_api.helpers import utcnow
from underfit_api.schema import log_segments


def get_end_line(conn: Connection, worker_id: UUID) -> int:
    row = conn.execute(
        log_segments.select()
        .where(log_segments.c.worker_id == worker_id)
        .order_by(log_segments.c.end_line.desc())
        .limit(1),
    ).first()
    return row.end_line if row else 0


def upsert(
    conn: Connection, worker_id: UUID, start_line: int, end_line: int,
    start_at: datetime, end_at: datetime, storage_key: str,
) -> None:
    updated = conn.execute(log_segments.update().where(
        log_segments.c.worker_id == worker_id, log_segments.c.start_line == start_line,
    ).values(end_line=end_line, end_at=end_at, storage_key=storage_key)).rowcount
    if updated == 0:
        conn.execute(log_segments.insert().values(
            id=uuid4(), worker_id=worker_id, start_line=start_line, end_line=end_line,
            start_at=start_at, end_at=end_at, storage_key=storage_key, created_at=utcnow(),
        ))


def list_for_range(conn: Connection, worker_id: UUID, cursor: int, count: int) -> Sequence[Row]:
    return conn.execute(
        log_segments.select()
        .where(
            log_segments.c.worker_id == worker_id,
            log_segments.c.end_line > cursor,
            log_segments.c.start_line < cursor + count,
        )
        .order_by(log_segments.c.start_line),
    ).all()
