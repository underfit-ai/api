from __future__ import annotations

import logging
from datetime import datetime, timedelta
from uuid import UUID

import sqlalchemy as sa
from sqlalchemy import Connection, Engine
from sqlalchemy.exc import IntegrityError

from underfit_api.buffers import BadStartLineError, BadStepError, BadTimestampError
from underfit_api.config import config
from underfit_api.helpers import utcnow
from underfit_api.models import Scalar, Worker
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import scalar_segments as scalar_seg_repo
from underfit_api.schema import run_workers, scalar_points, scalar_segments
from underfit_api.storage import Storage

logger = logging.getLogger(__name__)


def read_buffered(conn: Connection, worker_id: UUID, resolution: int, line_lte: int | None = None) -> list[Scalar]:
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
    grouped: dict[int, tuple[int | None, dict[str, float], datetime]] = {}
    for row in rows:
        prev = grouped.get(row.bucket)
        if prev is None:
            grouped[row.bucket] = (row.step, {row.key: float(row.value)}, row.timestamp)
        else:
            step, values, timestamp = prev
            values[row.key] = float(row.value)
            merged_step = row.step if step is None else step if row.step is None else max(step, row.step)
            grouped[row.bucket] = (merged_step, values, max(timestamp, row.timestamp))
    return [Scalar(step=step, values=values, timestamp=ts) for _, (step, values, ts) in sorted(grouped.items())]


# Mid-run compaction keeps every resolution aligned to seg_end(1), so staged_count / r is
# what resolution r would gain if flushed now. Terminal drains clear staging entirely.
def end_line(conn: Connection, worker_id: UUID, resolution: int = 1) -> int:
    seg_end = scalar_seg_repo.get_end_line(conn, worker_id, resolution)
    staged = conn.execute(sa.select(sa.func.count(sa.distinct(scalar_points.c.line))).where(
        scalar_points.c.worker_id == worker_id,
    )).scalar() or 0
    return seg_end + (staged + resolution - 1) // resolution


def total_line_counts(conn: Connection, worker_ids: list[UUID]) -> dict[int, int]:
    if not worker_ids:
        return {r: 0 for r in config.buffer.scalar_resolutions}
    seg_rows = conn.execute(sa.select(
        scalar_segments.c.worker_id, scalar_segments.c.resolution, sa.func.max(scalar_segments.c.end_line),
    ).where(scalar_segments.c.worker_id.in_(worker_ids)).group_by(
        scalar_segments.c.worker_id, scalar_segments.c.resolution,
    )).all()
    seg_end = {(r[0], r[1]): r[2] for r in seg_rows}
    staged_rows = conn.execute(sa.select(
        scalar_points.c.worker_id, sa.func.count(sa.distinct(scalar_points.c.line)),
    ).where(scalar_points.c.worker_id.in_(worker_ids)).group_by(scalar_points.c.worker_id)).all()
    staged = {r[0]: r[1] for r in staged_rows}
    return {
        res: sum(seg_end.get((wid, res), 0) + (staged.get(wid, 0) + res - 1) // res for wid in worker_ids)
        for res in config.buffer.scalar_resolutions
    }


def append(conn: Connection, worker_id: UUID, start_line: int, scalars: list[Scalar]) -> None:
    expected = end_line(conn, worker_id, 1)
    if start_line != expected:
        raise BadStartLineError(expected)
    # Staged points always hold the most recent steps (compaction drains oldest-first),
    # so we only fall back to segments when nothing is staged.
    last_step = conn.execute(sa.select(sa.func.max(scalar_points.c.step)).where(
        scalar_points.c.worker_id == worker_id,
    )).scalar()
    if last_step is None:
        last_step = scalar_seg_repo.get_last_step(conn, worker_id)
    last_ts = conn.execute(sa.select(sa.func.max(scalar_points.c.timestamp)).where(
        scalar_points.c.worker_id == worker_id,
    )).scalar()
    if last_ts is None:
        last_ts = scalar_seg_repo.get_last_timestamp(conn, worker_id)
    for scalar in scalars:
        if scalar.step is not None:
            if last_step is not None and scalar.step <= last_step:
                raise BadStepError(last_step)
            last_step = scalar.step
        if last_ts is not None and scalar.timestamp <= last_ts:
            raise BadTimestampError(last_ts)
        last_ts = scalar.timestamp
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

# Flushes must take a multiple of max_res to keep resolution segments aligned, so we round
# the configured target down to the nearest multiple. scalar_segment_lines >= max_res is
# enforced by config validation.
def _flush_threshold() -> int:
    max_res = max(config.buffer.scalar_resolutions)
    return (config.buffer.scalar_segment_lines // max_res) * max_res


def _workers_to_flush(conn: Connection) -> list[tuple[Worker, bool]]:
    cutoff = utcnow() - timedelta(seconds=config.buffer.worker_timeout_s)
    staged = sa.func.count(sa.distinct(scalar_points.c.line))
    rows = conn.execute(sa.select(*workers_repo.COLUMNS).select_from(
        workers_repo.JOIN.join(scalar_points, scalar_points.c.worker_id == run_workers.c.id),
    ).group_by(*workers_repo.COLUMNS).having(
        sa.or_(staged >= _flush_threshold(), run_workers.c.last_heartbeat < cutoff),
    )).all()
    workers = [Worker.model_validate(r) for r in rows]
    # Gate partial drain on the whole run being quiet, not just this worker — a briefly-stale
    # worker that later reconnects would otherwise produce misaligned higher-resolution segments.
    run_active = {w.run_id: runs_repo.has_active_worker(conn, w.run_id) for w in workers}
    return [(w, not run_active[w.run_id]) for w in workers]


def compact(engine: Engine, storage: Storage) -> None:
    with engine.connect() as conn:
        targets = _workers_to_flush(conn)
    for worker, partial in targets:
        try:
            with engine.begin() as conn:
                _compact_worker(conn, storage, worker, partial=partial)
        except Exception:
            logger.exception("Scalar compaction failed for worker %s", worker.id)


def _compact_worker(conn: Connection, storage: Storage, worker: Worker, *, partial: bool) -> None:
    base_at_1 = scalar_seg_repo.get_end_line(conn, worker.id, 1)
    max_line = conn.execute(sa.select(sa.func.max(scalar_points.c.line)).where(
        scalar_points.c.worker_id == worker.id,
    )).scalar()
    if max_line is None:
        return
    staged = max_line + 1 - base_at_1
    max_res = max(config.buffer.scalar_resolutions)
    # Mid-run we round to max_res to keep bucket boundaries aligned; terminal drains flush
    # the ragged tail and accept that the final bucket may average fewer points.
    take = staged if partial else (staged // max_res) * max_res
    if take == 0:
        return
    cutoff_line = base_at_1 + take - 1
    for resolution in config.buffer.scalar_resolutions:
        base = scalar_seg_repo.get_end_line(conn, worker.id, resolution)
        downsampled = read_buffered(conn, worker.id, resolution, line_lte=cutoff_line)
        if not downsampled:
            continue
        storage_key = f"scalars/{worker.worker_label}/r{resolution}/{base}.jsonl"
        content = "".join(p.model_dump_json() + "\n" for p in downsampled)
        storage.write(f"{worker.run_storage_key}/{storage_key}", content.encode())
        segment_steps = [p.step for p in downsampled if p.step is not None]
        scalar_seg_repo.upsert(
            conn, worker.id, resolution,
            start_line=base, end_line=base + len(downsampled),
            end_step=max(segment_steps) if segment_steps else None,
            start_at=downsampled[0].timestamp, end_at=downsampled[-1].timestamp, storage_key=storage_key,
        )
    conn.execute(scalar_points.delete().where(
        scalar_points.c.worker_id == worker.id, scalar_points.c.line <= cutoff_line,
    ))
