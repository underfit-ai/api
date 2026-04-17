from __future__ import annotations

import logging
from datetime import datetime, timedelta
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Connection, Engine
from sqlalchemy.exc import IntegrityError

from underfit_api.buffers import BadStartLineError, BadStepError
from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.models import Scalar
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.schema import run_workers, scalar_points
from underfit_api.storage.types import Storage

logger = logging.getLogger(__name__)


def resolutions() -> list[int]:
    return sorted({1, *config.buffer.scalar_resolutions})


def read_buffered(
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


# Compaction advances every resolution's segments together by a multiple of max(resolutions),
# so the staged tail always starts exactly at base_at_1 and staged_count / r gives the lines
# that a resolution-r segment would gain if flushed now.
def end_line(conn: Connection, worker_id: UUID, resolution: int = 1) -> int:
    seg_end = scalar_seg_repo.get_end_line(conn, worker_id, resolution)
    staged = conn.execute(sa.select(sa.func.count(sa.distinct(scalar_points.c.line))).where(
        scalar_points.c.worker_id == worker_id,
    )).scalar() or 0
    return seg_end + (staged + resolution - 1) // resolution


# Staged points always hold the most recent steps (compaction drains oldest-first), so we
# only fall back to segments when nothing is staged.
def _last_step(conn: Connection, worker_id: UUID) -> int | None:
    staged = conn.execute(sa.select(sa.func.max(scalar_points.c.step)).where(
        scalar_points.c.worker_id == worker_id,
    )).scalar()
    if staged is not None:
        return staged
    return scalar_seg_repo.get_last_step(conn, worker_id)


def append(conn: Connection, worker_id: UUID, start_line: int, scalars: list[Scalar]) -> None:
    expected = end_line(conn, worker_id, 1)
    if start_line != expected:
        raise BadStartLineError(expected)
    last_step = _last_step(conn, worker_id)
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
    # Savepoint lets us recover from a concurrent append that won the race on the same line
    # without aborting the outer transaction; the unique (worker_id, line, key) constraint
    # is what actually serializes writers.
    try:
        with conn.begin_nested():
            conn.execute(sa.insert(scalar_points), rows)
    except IntegrityError:
        raise BadStartLineError(end_line(conn, worker_id, 1)) from None


# --- compaction ---

# Flushes must take a multiple of max_res to keep resolution segments aligned with base_at_1,
# so we round the configured target down to the nearest multiple and clamp up to max_res.
def _flush_threshold() -> int:
    max_res = max(resolutions())
    return max(max_res, (config.buffer.scalar_segment_lines // max_res) * max_res)


def _workers_to_flush(conn: Connection, *, include_partial: bool) -> list[tuple[UUID, bool]]:
    cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
    staged = sa.func.count(sa.distinct(scalar_points.c.line))
    q = sa.select(scalar_points.c.worker_id, run_workers.c.last_heartbeat).select_from(
        scalar_points.join(run_workers, run_workers.c.id == scalar_points.c.worker_id),
    ).group_by(scalar_points.c.worker_id, run_workers.c.last_heartbeat)
    if not include_partial:
        q = q.having(sa.or_(staged >= _flush_threshold(), run_workers.c.last_heartbeat < cutoff))
    rows = conn.execute(q).all()
    return [(r.worker_id, include_partial or r.last_heartbeat < cutoff) for r in rows]


def compact(engine: Engine, storage: Storage, *, include_partial: bool = False) -> None:
    with engine.connect() as conn:
        targets = _workers_to_flush(conn, include_partial=include_partial)
    for worker_id, partial in targets:
        try:
            with engine.begin() as conn:
                _compact_worker(conn, storage, worker_id, partial=partial)
        except Exception:
            logger.exception("Scalar compaction failed for worker %s", worker_id)


def _compact_worker(conn: Connection, storage: Storage, worker_id: UUID, *, partial: bool) -> None:
    worker = workers_repo.get_by_id(conn, worker_id)
    if worker is None:
        return
    base_at_1 = scalar_seg_repo.get_end_line(conn, worker.id, 1)
    max_line = conn.execute(sa.select(sa.func.max(scalar_points.c.line)).where(
        scalar_points.c.worker_id == worker.id,
    )).scalar()
    if max_line is None:
        return
    staged = max_line + 1 - base_at_1
    max_res = max(resolutions())
    # Partial flush (shutdown / dead worker) drains everything and accepts that the final
    # bucket at higher resolutions may average fewer points; otherwise we round to max_res.
    take = staged if partial else (staged // max_res) * max_res
    if take == 0:
        return
    cutoff_line = base_at_1 + take - 1
    for resolution in resolutions():
        base = scalar_seg_repo.get_end_line(conn, worker.id, resolution)
        downsampled = read_buffered(conn, worker.id, resolution, line_lte=cutoff_line)
        if not downsampled:
            continue
        storage_key = f"scalars/{worker.worker_label}/r{resolution}/{base}.jsonl"
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
