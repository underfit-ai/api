from __future__ import annotations

import logging
from datetime import datetime, timedelta
from uuid import UUID

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy import Connection, Engine
from sqlalchemy.exc import IntegrityError

from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.models import LogEntry, Scalar, UTCDatetime, Worker
from underfit_api.repositories import log_segments as log_seg_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.schema import log_chunks, scalar_points
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)


def _scalar_resolutions() -> list[int]:
    return sorted({1, *config.buffer.scalar_resolutions})


def _scalar_max_resolution() -> int:
    return max(_scalar_resolutions())


def _log_storage_key(worker_label: str, start_line: int) -> str:
    return f"logs/{worker_label}/segments/{start_line}.log"


def _scalar_storage_key(worker_label: str, resolution: int, start_line: int) -> str:
    return f"scalars/{worker_label}/r{resolution}/{start_line}.jsonl"


def _read_downsampled(
    conn: Connection, worker_id: UUID, resolution: int, line_lte: int | None = None,
) -> list[Scalar]:
    bucket = (scalar_points.c.line - scalar_points.c.line.op("%")(resolution)).label("bucket")
    conditions = [scalar_points.c.worker_id == worker_id]
    if line_lte is not None:
        conditions.append(scalar_points.c.line <= line_lte)
    rows = conn.execute(sa.select(
        bucket,
        scalar_points.c.key,
        sa.func.avg(scalar_points.c.value).label("value"),
        sa.func.max(scalar_points.c.step).label("step"),
        sa.func.max(scalar_points.c.timestamp).label("timestamp"),
    ).where(*conditions).group_by(bucket, scalar_points.c.key).order_by(bucket)).all()
    grouped: dict[int, tuple[int, dict[str, float], datetime]] = {}
    for row in rows:
        step, values, timestamp = grouped.get(row.bucket, (row.step, {}, row.timestamp))
        values[row.key] = float(row.value)
        grouped[row.bucket] = (max(step, row.step), values, max(timestamp, row.timestamp))
    return [Scalar(step=step, values=values, timestamp=ts) for _, (step, values, ts) in sorted(grouped.items())]


def clip_log_lines(
    start_line: int, content: str, start_at: datetime, end_at: datetime, cursor: int, count: int,
) -> LogEntry:
    lines = content.splitlines()
    end_line = start_line + len(lines)
    sub_start = max(cursor, start_line)
    sub_end = min(cursor + count, end_line)
    clipped = lines[sub_start - start_line:sub_end - start_line]
    return LogEntry(
        start_line=sub_start, end_line=sub_end, content="\n".join(clipped),
        start_at=start_at, end_at=end_at,
    )


class BadStartLineError(Exception):
    def __init__(self, expected: int) -> None:
        self.expected = expected


class BadStepError(Exception):
    def __init__(self, last_step: int) -> None:
        self.last_step = last_step


class LogLine(BaseModel):
    timestamp: UTCDatetime
    content: str


class BufferStore:
    # --- scalars ---

    def scalar_end_line(self, conn: Connection, worker_id: UUID, resolution: int = 1) -> int:
        seg_end = scalar_seg_repo.get_end_line(conn, worker_id, resolution)
        staged = conn.execute(sa.select(sa.func.count(sa.distinct(scalar_points.c.line))).where(
            scalar_points.c.worker_id == worker_id,
        )).scalar() or 0
        return seg_end + (staged + resolution - 1) // resolution

    def _scalar_last_step(self, conn: Connection, worker_id: UUID) -> int | None:
        staged = conn.execute(sa.select(sa.func.max(scalar_points.c.step)).where(
            scalar_points.c.worker_id == worker_id,
        )).scalar()
        if staged is not None:
            return staged
        return scalar_seg_repo.get_last_step(conn, worker_id)

    def append_scalars(self, conn: Connection, worker_id: UUID, start_line: int, scalars: list[Scalar]) -> None:
        expected = self.scalar_end_line(conn, worker_id, 1)
        if start_line != expected:
            raise BadStartLineError(expected)
        last_step = self._scalar_last_step(conn, worker_id)
        for scalar in scalars:
            if last_step is not None and scalar.step <= last_step:
                raise BadStepError(last_step)
            last_step = scalar.step
        rows = [
            {"worker_id": worker_id, "line": start_line + i, "step": s.step,
             "key": k, "value": v, "timestamp": s.timestamp}
            for i, s in enumerate(scalars) for k, v in s.values.items()
        ]
        if not rows:
            return
        try:
            with conn.begin_nested():
                conn.execute(sa.insert(scalar_points), rows)
        except IntegrityError:
            raise BadStartLineError(self.scalar_end_line(conn, worker_id, 1)) from None

    def read_buffered_scalars(self, conn: Connection, worker_id: UUID, resolution: int) -> list[Scalar]:
        return _read_downsampled(conn, worker_id, resolution)

    # --- logs ---

    def log_end_line(self, conn: Connection, worker_id: UUID) -> int:
        staged = conn.execute(sa.select(sa.func.max(
            log_chunks.c.start_line + log_chunks.c.line_count,
        )).where(log_chunks.c.worker_id == worker_id)).scalar()
        if staged is not None:
            return staged
        return log_seg_repo.get_end_line(conn, worker_id)

    def append_logs(self, conn: Connection, worker_id: UUID, start_line: int, lines: list[LogLine]) -> None:
        expected = self.log_end_line(conn, worker_id)
        if start_line != expected:
            raise BadStartLineError(expected)
        if not lines:
            return
        content = "".join(f"{line.content}\n" for line in lines)
        try:
            with conn.begin_nested():
                conn.execute(log_chunks.insert().values(
                    worker_id=worker_id, start_line=start_line, line_count=len(lines),
                    byte_count=len(content.encode()), content=content,
                    start_at=lines[0].timestamp, end_at=lines[-1].timestamp,
                ))
        except IntegrityError:
            raise BadStartLineError(self.log_end_line(conn, worker_id)) from None

    def read_buffered_logs(self, conn: Connection, worker_id: UUID, cursor: int, count: int) -> list[LogEntry]:
        rows = conn.execute(log_chunks.select().where(
            log_chunks.c.worker_id == worker_id,
            log_chunks.c.start_line < cursor + count,
            log_chunks.c.start_line + log_chunks.c.line_count > cursor,
        ).order_by(log_chunks.c.start_line)).all()
        return [clip_log_lines(r.start_line, r.content, r.start_at, r.end_at, cursor, count) for r in rows]

    # --- compaction ---

    def compact(self, engine: Engine, storage: Storage, *, include_partial: bool = False) -> None:
        with engine.connect() as conn:
            worker_ids = self._workers_with_staging(conn)
        for worker_id in worker_ids:
            try:
                with engine.begin() as conn:
                    self._compact_worker(conn, storage, worker_id, include_partial=include_partial)
            except Exception:
                logger.exception("Compaction failed for worker %s", worker_id)

    def _workers_with_staging(self, conn: Connection) -> set[UUID]:
        scalar_q = sa.select(scalar_points.c.worker_id).distinct()
        log_q = sa.select(log_chunks.c.worker_id).distinct()
        return {r.worker_id for r in conn.execute(sa.union(scalar_q, log_q))}

    def _compact_worker(
        self, conn: Connection, storage: Storage, worker_id: UUID, *, include_partial: bool,
    ) -> None:
        worker = workers_repo.get_by_id(conn, worker_id)
        if worker is None:
            return
        cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
        partial = include_partial or worker.last_heartbeat < cutoff
        self._compact_scalars(conn, storage, worker, partial=partial)
        self._compact_logs(conn, storage, worker, partial=partial)

    def _compact_scalars(self, conn: Connection, storage: Storage, worker: Worker, *, partial: bool) -> None:
        base_at_1 = scalar_seg_repo.get_end_line(conn, worker.id, 1)
        max_line = conn.execute(sa.select(sa.func.max(scalar_points.c.line)).where(
            scalar_points.c.worker_id == worker.id,
        )).scalar()
        if max_line is None:
            return
        staged = max_line + 1 - base_at_1
        max_res = _scalar_max_resolution()
        take = staged if partial else (staged // max_res) * max_res
        if take == 0:
            return
        cutoff_line = base_at_1 + take - 1
        for resolution in _scalar_resolutions():
            base = scalar_seg_repo.get_end_line(conn, worker.id, resolution)
            downsampled = _read_downsampled(conn, worker.id, resolution, line_lte=cutoff_line)
            if not downsampled:
                continue
            storage_key = _scalar_storage_key(worker.worker_label, resolution, base)
            content = "".join(p.model_dump_json() + "\n" for p in downsampled)
            storage.write(f"{worker.run_storage_key}/{storage_key}", content.encode())
            scalar_seg_repo.upsert(
                conn, worker.id, resolution,
                start_line=base, end_line=base + len(downsampled), end_step=downsampled[-1].step,
                start_at=downsampled[0].timestamp, end_at=downsampled[-1].timestamp, storage_key=storage_key,
            )
        conn.execute(scalar_points.delete().where(
            scalar_points.c.worker_id == worker.id, scalar_points.c.line <= cutoff_line,
        ))

    def _compact_logs(self, conn: Connection, storage: Storage, worker: Worker, *, partial: bool) -> None:
        rows = conn.execute(log_chunks.select().where(
            log_chunks.c.worker_id == worker.id,
        ).order_by(log_chunks.c.start_line)).all()
        if not rows:
            return
        total_bytes = sum(r.byte_count for r in rows)
        if not partial and total_bytes < config.buffer.log_segment_bytes:
            return
        start_line = rows[0].start_line
        end_line = rows[-1].start_line + rows[-1].line_count
        storage_key = _log_storage_key(worker.worker_label, start_line)
        storage.write(f"{worker.run_storage_key}/{storage_key}", "".join(r.content for r in rows).encode())
        log_seg_repo.upsert(
            conn, worker.id, start_line=start_line, end_line=end_line,
            start_at=rows[0].start_at, end_at=rows[-1].end_at, storage_key=storage_key,
        )
        conn.execute(log_chunks.delete().where(
            log_chunks.c.worker_id == worker.id, log_chunks.c.start_line < end_line,
        ))
