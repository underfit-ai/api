from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime, timezone
from uuid import UUID, uuid4

from sqlalchemy import Connection
from sqlalchemy.engine import Row

from underfit_api.schema import log_segments


def get_end_line(conn: Connection, run_id: UUID, worker_id: str) -> int:
    row = conn.execute(
        log_segments.select()
        .where(log_segments.c.run_id == run_id, log_segments.c.worker_id == worker_id)
        .order_by(log_segments.c.end_line.desc())
        .limit(1),
    ).first()
    return row.end_line if row else 0


def insert(
    conn: Connection,
    run_id: UUID,
    worker_id: str,
    start_line: int,
    end_line: int,
    start_at: datetime,
    end_at: datetime,
    byte_offset: int,
    byte_count: int,
    storage_key: str,
) -> None:
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    conn.execute(log_segments.insert().values(
        id=uuid4(),
        run_id=run_id,
        worker_id=worker_id,
        start_line=start_line,
        end_line=end_line,
        start_at=start_at,
        end_at=end_at,
        byte_offset=byte_offset,
        byte_count=byte_count,
        storage_key=storage_key,
        created_at=now,
    ))


def list_for_range(conn: Connection, run_id: UUID, worker_id: str, cursor: int, count: int) -> Sequence[Row]:
    return conn.execute(
        log_segments.select()
        .where(
            log_segments.c.run_id == run_id,
            log_segments.c.worker_id == worker_id,
            log_segments.c.end_line > cursor,
            log_segments.c.start_line < cursor + count,
        )
        .order_by(log_segments.c.start_line),
    ).all()
