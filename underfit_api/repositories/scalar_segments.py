from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy import Connection
from sqlalchemy.engine import Row

from underfit_api.helpers import utcnow
from underfit_api.schema import scalar_segments


def get_end_line(conn: Connection, run_worker_id: UUID, resolution: int) -> int:
    row = conn.execute(
        scalar_segments.select()
        .where(scalar_segments.c.run_worker_id == run_worker_id, scalar_segments.c.resolution == resolution)
        .order_by(scalar_segments.c.end_line.desc())
        .limit(1),
    ).first()
    return row.end_line if row else 0


def insert(
    conn: Connection,
    run_worker_id: UUID,
    resolution: int,
    start_line: int,
    end_line: int,
    start_at: datetime,
    end_at: datetime,
    byte_offset: int,
    byte_count: int,
    storage_key: str,
) -> None:
    conn.execute(scalar_segments.insert().values(
        id=uuid4(),
        run_worker_id=run_worker_id,
        resolution=resolution,
        start_line=start_line,
        end_line=end_line,
        start_at=start_at,
        end_at=end_at,
        byte_offset=byte_offset,
        byte_count=byte_count,
        storage_key=storage_key,
        created_at=utcnow(),
    ))


def list_by_resolution(conn: Connection, run_worker_id: UUID, resolution: int) -> Sequence[Row]:
    return conn.execute(
        scalar_segments.select()
        .where(scalar_segments.c.run_worker_id == run_worker_id, scalar_segments.c.resolution == resolution)
        .order_by(scalar_segments.c.start_line),
    ).all()
