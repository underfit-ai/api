from __future__ import annotations

import logging
import threading
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from datetime import datetime, timedelta
from uuid import UUID

import sqlalchemy as sa
from pydantic import BaseModel
from sqlalchemy import Connection, Engine
from sqlalchemy.engine import Row

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


def _group_scalar_rows(rows: Sequence[Row]) -> list[tuple[int, Scalar]]:
    grouped: dict[int, tuple[int, dict[str, float], datetime]] = {}
    for row in rows:
        step, values, timestamp = grouped.get(row.line, (row.step, {}, row.timestamp))
        values[row.key] = row.value
        grouped[row.line] = (step, values, max(timestamp, row.timestamp))
    return [
        (line, Scalar(step=step, values=values, timestamp=timestamp))
        for line, (step, values, timestamp) in sorted(grouped.items())
    ]


def _downsample(scalars: list[Scalar], resolution: int) -> list[Scalar]:
    if resolution == 1:
        return list(scalars)
    output: list[Scalar] = []
    for i in range(0, len(scalars), resolution):
        chunk = scalars[i:i + resolution]
        sums: dict[str, float] = {}
        counts: dict[str, int] = {}
        for s in chunk:
            for k, v in s.values.items():
                sums[k] = sums.get(k, 0.0) + v
                counts[k] = counts.get(k, 0) + 1
        averaged = {k: sums[k] / counts[k] for k in sums}
        output.append(Scalar(step=chunk[-1].step, values=averaged, timestamp=chunk[-1].timestamp))
    return output


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
    def __init__(self) -> None:
        self._locks: dict[UUID, threading.RLock] = {}
        self._lock_dict_lock = threading.Lock()

    @contextmanager
    def _worker_lock(self, worker_id: UUID) -> Iterator[None]:
        with self._lock_dict_lock:
            lock = self._locks.setdefault(worker_id, threading.RLock())
        with lock:
            yield

    # --- scalars ---

    def scalar_end_line(self, conn: Connection, worker_id: UUID, resolution: int = 1) -> int:
        seg_end = scalar_seg_repo.get_end_line(conn, worker_id, resolution)
        staged = self._staged_scalar_count(conn, worker_id)
        if staged == 0:
            return seg_end
        return seg_end + (staged + resolution - 1) // resolution

    def _staged_scalar_count(self, conn: Connection, worker_id: UUID) -> int:
        return conn.execute(sa.select(sa.func.count(sa.distinct(scalar_points.c.line))).where(
            scalar_points.c.worker_id == worker_id,
        )).scalar() or 0

    def _scalar_last_step(self, conn: Connection, worker_id: UUID) -> int | None:
        staged = conn.execute(sa.select(sa.func.max(scalar_points.c.step)).where(
            scalar_points.c.worker_id == worker_id,
        )).scalar()
        if staged is not None:
            return staged
        return scalar_seg_repo.get_last_step(conn, worker_id)

    def append_scalars(self, conn: Connection, worker_id: UUID, start_line: int, scalars: list[Scalar]) -> None:
        with self._worker_lock(worker_id):
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
            if rows:
                conn.execute(sa.insert(scalar_points), rows)

    def read_buffered_scalars(self, conn: Connection, worker_id: UUID, resolution: int) -> list[Scalar]:
        with self._worker_lock(worker_id):
            rows = conn.execute(sa.select(scalar_points).where(
                scalar_points.c.worker_id == worker_id,
            ).order_by(scalar_points.c.line)).all()
            scalars = [s for _, s in _group_scalar_rows(rows)]
            return _downsample(scalars, resolution)

    # --- logs ---

    def log_end_line(self, conn: Connection, worker_id: UUID) -> int:
        staged = conn.execute(sa.select(sa.func.max(
            log_chunks.c.start_line + log_chunks.c.line_count,
        )).where(log_chunks.c.worker_id == worker_id)).scalar()
        if staged is not None:
            return staged
        return log_seg_repo.get_end_line(conn, worker_id)

    def append_logs(self, conn: Connection, worker_id: UUID, start_line: int, lines: list[LogLine]) -> None:
        with self._worker_lock(worker_id):
            expected = self.log_end_line(conn, worker_id)
            if start_line != expected:
                raise BadStartLineError(expected)
            if not lines:
                return
            content = "".join(f"{line.content}\n" for line in lines)
            conn.execute(log_chunks.insert().values(
                worker_id=worker_id, start_line=start_line, line_count=len(lines),
                byte_count=len(content.encode()), content=content,
                start_at=lines[0].timestamp, end_at=lines[-1].timestamp,
            ))

    def read_buffered_logs(self, conn: Connection, worker_id: UUID, cursor: int, count: int) -> list[LogEntry]:
        with self._worker_lock(worker_id):
            rows = conn.execute(log_chunks.select().where(
                log_chunks.c.worker_id == worker_id,
                log_chunks.c.start_line < cursor + count,
                log_chunks.c.start_line + log_chunks.c.line_count > cursor,
            ).order_by(log_chunks.c.start_line)).all()
            entries: list[LogEntry] = []
            for row in rows:
                end_line = row.start_line + row.line_count
                sub_start = max(cursor, row.start_line)
                sub_end = min(cursor + count, end_line)
                lines = row.content.splitlines()
                clipped = lines[sub_start - row.start_line:sub_end - row.start_line]
                entries.append(LogEntry(
                    start_line=sub_start, end_line=sub_end, content="\n".join(clipped),
                    start_at=row.start_at, end_at=row.end_at,
                ))
            return entries

    # --- compaction ---

    def compact_due(self, engine: Engine, storage: Storage) -> None:
        self._run_compaction(engine, storage, force_partial=False)

    def flush_all(self, engine: Engine, storage: Storage) -> None:
        self._run_compaction(engine, storage, force_partial=True)

    def _run_compaction(self, engine: Engine, storage: Storage, *, force_partial: bool) -> None:
        with engine.connect() as conn:
            worker_ids = self._workers_with_staging(conn)
        for worker_id in worker_ids:
            try:
                with engine.begin() as conn:
                    self._compact_worker(conn, storage, worker_id, force_partial=force_partial)
            except Exception:
                logger.exception("Compaction failed for worker %s", worker_id)

    def _workers_with_staging(self, conn: Connection) -> set[UUID]:
        scalar_ids = {r.worker_id for r in conn.execute(sa.select(scalar_points.c.worker_id).distinct())}
        log_ids = {r.worker_id for r in conn.execute(sa.select(log_chunks.c.worker_id).distinct())}
        return scalar_ids | log_ids

    def _compact_worker(
        self, conn: Connection, storage: Storage, worker_id: UUID, *, force_partial: bool,
    ) -> None:
        with self._worker_lock(worker_id):
            worker = workers_repo.get_by_id(conn, worker_id)
            if worker is None:
                conn.execute(scalar_points.delete().where(scalar_points.c.worker_id == worker_id))
                conn.execute(log_chunks.delete().where(log_chunks.c.worker_id == worker_id))
                with self._lock_dict_lock:
                    self._locks.pop(worker_id, None)
                return
            cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
            partial = force_partial or worker.last_heartbeat < cutoff
            self._compact_scalars(conn, storage, worker, partial=partial)
            self._compact_logs(conn, storage, worker, partial=partial)

    def _compact_scalars(self, conn: Connection, storage: Storage, worker: Worker, *, partial: bool) -> None:
        rows = conn.execute(sa.select(scalar_points).where(
            scalar_points.c.worker_id == worker.id,
        ).order_by(scalar_points.c.line)).all()
        if not rows:
            return
        grouped = _group_scalar_rows(rows)
        max_res = _scalar_max_resolution()
        take = len(grouped) if partial else (len(grouped) // max_res) * max_res
        if take == 0:
            return
        chunk = grouped[:take]
        scalars = [s for _, s in chunk]
        last_line = chunk[-1][0]
        for resolution in _scalar_resolutions():
            base = scalar_seg_repo.get_end_line(conn, worker.id, resolution)
            downsampled = _downsample(scalars, resolution)
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
            scalar_points.c.worker_id == worker.id, scalar_points.c.line <= last_line,
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
