from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest
from sqlalchemy import Engine, select

from tests.conftest import CreateWorker
from underfit_api.buffers import BadStartLineError, BadStepError
from underfit_api.buffers import logs as log_buffer
from underfit_api.buffers import scalars as scalar_buffer
from underfit_api.config import config
from underfit_api.models import LogLine, Scalar, Worker
from underfit_api.schema import log_chunks, log_segments, run_workers, scalar_points, scalar_segments
from underfit_api.storage import Storage

T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _scalars(start: int, count: int, *, t: datetime = T0, keys: tuple[str, ...] = ("loss",)) -> list[Scalar]:
    return [Scalar(step=start + i, values={k: float(start + i) for k in keys}, timestamp=t + timedelta(seconds=i))
            for i in range(count)]


def _mark_stale(engine: Engine, worker: Worker) -> None:
    with engine.begin() as conn:
        conn.execute(run_workers.update().where(run_workers.c.id == worker.id).values(
            last_heartbeat=T0 - timedelta(seconds=config.buffer.worker_timeout_s + 1),
        ))


def test_log_compaction_writes_segment_and_clears_chunks(engine: Engine, storage: Storage, worker: Worker) -> None:
    config.buffer.log_segment_bytes = 1
    with engine.begin() as conn:
        log_buffer.append(conn, worker.id, 0, [LogLine(timestamp=T0, content="a")])
        log_buffer.append(conn, worker.id, 1, [LogLine(timestamp=T0, content="b")])
    log_buffer.compact(engine, storage)
    with engine.begin() as conn:
        segments = conn.execute(select(log_segments).where(log_segments.c.worker_id == worker.id)).all()
        assert conn.execute(select(log_chunks).where(log_chunks.c.worker_id == worker.id)).all() == []
    assert [(s.start_line, s.end_line) for s in segments] == [(0, 2)]
    assert storage.read(f"{worker.run_storage_key}/{segments[0].storage_key}").decode() == "a\nb\n"


def test_log_compaction_skips_below_threshold(engine: Engine, storage: Storage, worker: Worker) -> None:
    config.buffer.log_segment_bytes = 100
    with engine.begin() as conn:
        log_buffer.append(conn, worker.id, 0, [LogLine(timestamp=T0, content="x")])
    log_buffer.compact(engine, storage)
    with engine.begin() as conn:
        assert conn.execute(select(log_segments).where(log_segments.c.worker_id == worker.id)).all() == []
    _mark_stale(engine, worker)
    log_buffer.compact(engine, storage)
    with engine.begin() as conn:
        rows = conn.execute(select(log_segments).where(log_segments.c.worker_id == worker.id)).all()
    assert len(rows) == 1 and rows[0].end_line == 1


def test_scalar_ingest_validates_step_and_start_line(engine: Engine, worker: Worker) -> None:
    with engine.begin() as conn:
        scalar_buffer.append(conn, worker.id, 0, _scalars(0, 3))
        with pytest.raises(BadStartLineError) as exc:
            scalar_buffer.append(conn, worker.id, 0, _scalars(10, 1))
        assert exc.value.expected == 3
        with pytest.raises(BadStepError) as step_exc:
            scalar_buffer.append(conn, worker.id, 3, [Scalar(step=2, values={"loss": 0.0}, timestamp=T0)])
        assert step_exc.value.last_step == 2


def test_scalar_ingest_allows_stepless_points(engine: Engine, storage: Storage, worker: Worker) -> None:
    config.buffer.scalar_resolutions = [1, 10]
    config.buffer.scalar_segment_lines = 10
    stepless = [Scalar(values={"cpu": float(i)}, timestamp=T0 + timedelta(seconds=i)) for i in range(3)]
    with engine.begin() as conn:
        scalar_buffer.append(conn, worker.id, 0, stepless)
        scalar_buffer.append(conn, worker.id, 3, _scalars(5, 2))
        scalar_buffer.append(conn, worker.id, 5, [
            Scalar(values={"cpu": 9.0}, timestamp=T0 + timedelta(seconds=9)),
        ])
        with pytest.raises(BadStepError) as step_exc:
            scalar_buffer.append(conn, worker.id, 6, [Scalar(step=6, values={"loss": 0.0}, timestamp=T0)])
        assert step_exc.value.last_step == 6
    _mark_stale(engine, worker)
    scalar_buffer.compact(engine, storage)
    with engine.begin() as conn:
        rows = {r.resolution: r for r in conn.execute(
            select(scalar_segments).where(scalar_segments.c.worker_id == worker.id),
        ).all()}
    assert rows[1].end_step == 6 and rows[10].end_step == 6


def test_scalar_compaction_full_chunk_emits_all_resolutions(engine: Engine, storage: Storage, worker: Worker) -> None:
    config.buffer.scalar_resolutions = [1, 10]
    config.buffer.scalar_segment_lines = 20
    with engine.begin() as conn:
        scalar_buffer.append(conn, worker.id, 0, _scalars(0, 25, keys=("loss", "accuracy")))
    scalar_buffer.compact(engine, storage)
    with engine.begin() as conn:
        rows = {r.resolution: r for r in conn.execute(
            select(scalar_segments).where(scalar_segments.c.worker_id == worker.id),
        ).all()}
        staged = conn.execute(select(scalar_points.c.line).where(scalar_points.c.worker_id == worker.id)).all()
    assert rows[1].end_line == 20 and rows[10].end_line == 2
    assert {row.line for row in staged} == {20, 21, 22, 23, 24}
    r10 = [json.loads(line) for line in
           storage.read(f"{worker.run_storage_key}/{rows[10].storage_key}").decode().splitlines()]
    assert [p["values"] for p in r10] == [{"loss": 4.5, "accuracy": 4.5}, {"loss": 14.5, "accuracy": 14.5}]
    with engine.begin() as conn:
        scalar_buffer.append(conn, worker.id, 25, _scalars(25, 25))
    scalar_buffer.compact(engine, storage)
    with engine.begin() as conn:
        rows = conn.execute(select(scalar_segments).where(
            scalar_segments.c.worker_id == worker.id,
        ).order_by(scalar_segments.c.start_line)).all()
    latest = {r.resolution: r for r in rows}
    assert latest[1].end_line == 50 and latest[10].end_line == 5


def test_scalar_partial_flush_waits_for_entire_run_to_go_quiet(
    engine: Engine, storage: Storage, create_worker: CreateWorker,
) -> None:
    worker_a = create_worker("0")
    create_worker("1")
    with engine.begin() as conn:
        scalar_buffer.append(conn, worker_a.id, 0, _scalars(0, 7))
    _mark_stale(engine, worker_a)
    scalar_buffer.compact(engine, storage)
    with engine.begin() as conn:
        assert conn.execute(select(scalar_segments).where(scalar_segments.c.worker_id == worker_a.id)).all() == []


def test_scalar_compaction_skips_below_threshold(engine: Engine, storage: Storage, worker: Worker) -> None:
    config.buffer.scalar_resolutions = [1, 10]
    config.buffer.scalar_segment_lines = 100
    with engine.begin() as conn:
        scalar_buffer.append(conn, worker.id, 0, _scalars(0, 7))
    scalar_buffer.compact(engine, storage)
    with engine.begin() as conn:
        assert conn.execute(select(scalar_segments).where(scalar_segments.c.worker_id == worker.id)).all() == []
    _mark_stale(engine, worker)
    scalar_buffer.compact(engine, storage)
    with engine.begin() as conn:
        rows = {r.resolution: r for r in conn.execute(
            select(scalar_segments).where(scalar_segments.c.worker_id == worker.id),
        ).all()}
        assert conn.execute(select(scalar_points).where(scalar_points.c.worker_id == worker.id)).all() == []
    assert rows[1].end_line == 7 and rows[10].end_line == 1
    with engine.begin() as conn, pytest.raises(BadStepError) as step_exc:
        scalar_buffer.append(conn, worker.id, 7, [Scalar(step=6, values={"loss": 0.0}, timestamp=T0)])
    assert step_exc.value.last_step == 6
