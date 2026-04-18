from __future__ import annotations

import logging
from datetime import datetime, timedelta
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Connection, Engine
from sqlalchemy.exc import IntegrityError

from underfit_api.buffers import BadStartLineError
from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.models import LogEntry, LogLine, Worker
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.schema import log_chunks, run_workers
from underfit_api.storage import Storage

logger = logging.getLogger(__name__)


def clip(start_line: int, content: str, start_at: datetime, end_at: datetime, cursor: int, count: int) -> LogEntry:
    lines = content.splitlines()
    chunk_end = start_line + len(lines)
    sub_start = max(cursor, start_line)
    sub_end = min(cursor + count, chunk_end)
    clipped = lines[sub_start - start_line:sub_end - start_line]
    return LogEntry(
        start_line=sub_start, end_line=sub_end, content="\n".join(clipped), start_at=start_at, end_at=end_at,
    )


def end_line(conn: Connection, worker_id: UUID) -> int:
    staged = conn.execute(sa.select(sa.func.max(
        log_chunks.c.start_line + log_chunks.c.line_count,
    )).where(log_chunks.c.worker_id == worker_id)).scalar()
    return staged if staged is not None else log_seg_repo.get_end_line(conn, worker_id)


def append(conn: Connection, worker_id: UUID, start_line: int, lines: list[LogLine]) -> None:
    expected = end_line(conn, worker_id)
    if start_line != expected:
        raise BadStartLineError(expected)
    if not lines:
        return
    content = "".join(f"{line.content}\n" for line in lines)
    # Same savepoint pattern as scalars.append: the unique (worker_id, start_line) constraint
    # is the serialization point; IntegrityError means a concurrent writer won.
    try:
        with conn.begin_nested():
            conn.execute(log_chunks.insert().values(
                worker_id=worker_id, start_line=start_line, line_count=len(lines),
                byte_count=len(content.encode()), content=content,
                start_at=lines[0].timestamp, end_at=lines[-1].timestamp,
            ))
    except IntegrityError:
        raise BadStartLineError(end_line(conn, worker_id)) from None


def read_buffered(conn: Connection, worker_id: UUID, cursor: int, count: int) -> list[LogEntry]:
    rows = conn.execute(log_chunks.select().where(
        log_chunks.c.worker_id == worker_id,
        log_chunks.c.start_line < cursor + count,
        log_chunks.c.start_line + log_chunks.c.line_count > cursor,
    ).order_by(log_chunks.c.start_line)).all()
    return [clip(r.start_line, r.content, r.start_at, r.end_at, cursor, count) for r in rows]


# --- compaction ---

# Selected workers always drain fully; threshold vs heartbeat only decides whether to pick
# them up in this cycle, not how much to flush.
def _workers_to_flush(conn: Connection) -> list[Worker]:
    cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
    total_bytes = sa.func.sum(log_chunks.c.byte_count)
    rows = conn.execute(sa.select(*workers_repo.COLUMNS).select_from(
        workers_repo.JOIN.join(log_chunks, log_chunks.c.worker_id == run_workers.c.id),
    ).group_by(*workers_repo.COLUMNS).having(
        sa.or_(total_bytes >= config.buffer.log_segment_bytes, run_workers.c.last_heartbeat < cutoff),
    )).all()
    return [Worker.model_validate(r) for r in rows]


def compact(engine: Engine, storage: Storage) -> None:
    with engine.connect() as conn:
        workers = _workers_to_flush(conn)
    for worker in workers:
        try:
            with engine.begin() as conn:
                _compact_worker(conn, storage, worker)
        except Exception:
            logger.exception("Log compaction failed for worker %s", worker.id)


def _compact_worker(conn: Connection, storage: Storage, worker: Worker) -> None:
    rows = conn.execute(log_chunks.select().where(
        log_chunks.c.worker_id == worker.id,
    ).order_by(log_chunks.c.start_line)).all()
    if not rows:
        return
    start_line = rows[0].start_line
    seg_end = rows[-1].start_line + rows[-1].line_count
    storage_key = f"logs/{worker.worker_label}/segments/{start_line}.log"
    storage.write(f"{worker.run_storage_key}/{storage_key}", "".join(r.content for r in rows).encode())
    log_seg_repo.upsert(
        conn, worker.id, start_line=start_line, end_line=seg_end,
        start_at=rows[0].start_at, end_at=rows[-1].end_at, storage_key=storage_key,
    )
    conn.execute(log_chunks.delete().where(
        log_chunks.c.worker_id == worker.id, log_chunks.c.start_line < seg_end,
    ))
