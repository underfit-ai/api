from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import Engine, select

from underfit_api.buffer import BadStartLineError, BadStepError, BufferStore, LogLine
from underfit_api.config import FileStorageConfig, config
from underfit_api.models import Scalar
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import log_chunks, log_segments, run_workers, scalar_points, scalar_segments
from underfit_api.storage.file import FileStorage

T0 = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _create_worker(engine: Engine, worker_label: str = "0") -> UUID:
    with engine.begin() as conn:
        user = users_repo.create(conn, email="owner@example.com", handle="owner", name="Owner")
        project = projects_repo.create(conn, user.id, "underfit", None, "private", {})
        run = runs_repo.create(conn, project.id, user.id, "test-launch-id", "test-run", None, {})
        return workers_repo.create(conn, run.id, worker_label).id


def _run_storage_key(engine: Engine, worker_id: UUID) -> str:
    with engine.begin() as conn:
        worker = workers_repo.get_by_id(conn, worker_id)
        assert worker is not None
        run = runs_repo.get_by_id(conn, worker.run_id)
        assert run is not None
        return run.storage_key


def _scalars(start: int, count: int, *, t: datetime = T0) -> list[Scalar]:
    return [Scalar(step=start + i, values={"loss": float(start + i)}, timestamp=t + timedelta(seconds=i))
            for i in range(count)]


def test_log_ingest_validates_start_line_and_round_trips_buffered(engine: Engine) -> None:
    rwid = _create_worker(engine)
    buffer = BufferStore()
    with engine.begin() as conn:
        buffer.append_logs(conn, rwid, 0, [LogLine(timestamp=T0, content="a"), LogLine(timestamp=T0, content="b")])
        assert buffer.log_end_line(conn, rwid) == 2
        with pytest.raises(BadStartLineError) as exc:
            buffer.append_logs(conn, rwid, 1, [LogLine(timestamp=T0, content="late")])
        assert exc.value.expected == 2
        entries = buffer.read_buffered_logs(conn, rwid, cursor=0, count=10)
        assert [(e.start_line, e.end_line, e.content) for e in entries] == [(0, 2, "a\nb")]


def test_log_compaction_writes_segment_and_clears_chunks(engine: Engine, tmp_path: Path) -> None:
    rwid = _create_worker(engine)
    buffer = BufferStore()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    with engine.begin() as conn:
        buffer.append_logs(conn, rwid, 0, [LogLine(timestamp=T0, content="a")])
        buffer.append_logs(conn, rwid, 1, [LogLine(timestamp=T0, content="b")])
    buffer.compact(engine, storage, include_partial=True)
    with engine.begin() as conn:
        segments = conn.execute(select(log_segments).where(log_segments.c.worker_id == rwid)).all()
        assert conn.execute(select(log_chunks).where(log_chunks.c.worker_id == rwid)).all() == []
    assert [(s.start_line, s.end_line) for s in segments] == [(0, 2)]
    assert storage.read(f"{_run_storage_key(engine, rwid)}/{segments[0].storage_key}").decode() == "a\nb\n"


def test_log_compaction_skips_below_byte_threshold_until_partial(engine: Engine, tmp_path: Path) -> None:
    rwid = _create_worker(engine)
    buffer = BufferStore()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    config.buffer.log_segment_bytes = 100
    with engine.begin() as conn:
        buffer.append_logs(conn, rwid, 0, [LogLine(timestamp=T0, content="x")])
    buffer.compact(engine, storage)
    with engine.begin() as conn:
        assert conn.execute(select(log_segments).where(log_segments.c.worker_id == rwid)).all() == []
        conn.execute(run_workers.update().where(run_workers.c.id == rwid).values(
            last_heartbeat=T0 - timedelta(seconds=config.buffer.worker_timeout_s + 1),
        ))
    buffer.compact(engine, storage)
    with engine.begin() as conn:
        rows = conn.execute(select(log_segments).where(log_segments.c.worker_id == rwid)).all()
    assert len(rows) == 1 and rows[0].end_line == 1


def test_scalar_ingest_validates_step_and_start_line(engine: Engine) -> None:
    rwid = _create_worker(engine)
    buffer = BufferStore()
    with engine.begin() as conn:
        buffer.append_scalars(conn, rwid, 0, _scalars(0, 3))
        with pytest.raises(BadStartLineError) as exc:
            buffer.append_scalars(conn, rwid, 0, _scalars(10, 1))
        assert exc.value.expected == 3
        with pytest.raises(BadStepError) as step_exc:
            buffer.append_scalars(conn, rwid, 3, [Scalar(step=2, values={"loss": 0.0}, timestamp=T0)])
        assert step_exc.value.last_step == 2


def test_scalar_buffered_read_downsamples_live_tail(engine: Engine) -> None:
    rwid = _create_worker(engine)
    buffer = BufferStore()
    with engine.begin() as conn:
        buffer.append_scalars(conn, rwid, 0, _scalars(0, 10))
        r1 = buffer.read_buffered_scalars(conn, rwid, 1)
        r10 = buffer.read_buffered_scalars(conn, rwid, 10)
        assert len(r1) == 10
        assert [p.values["loss"] for p in r10] == [4.5]
        assert buffer.scalar_end_line(conn, rwid, 10) == 1


def test_scalar_compaction_full_chunk_emits_all_resolutions(engine: Engine, tmp_path: Path) -> None:
    rwid = _create_worker(engine)
    buffer = BufferStore()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    config.buffer.scalar_resolutions = [1, 10]
    with engine.begin() as conn:
        buffer.append_scalars(conn, rwid, 0, _scalars(0, 25))
    buffer.compact(engine, storage)
    with engine.begin() as conn:
        rows = {r.resolution: r for r in conn.execute(
            select(scalar_segments).where(scalar_segments.c.worker_id == rwid),
        ).all()}
        staged = conn.execute(select(scalar_points.c.line).where(scalar_points.c.worker_id == rwid)).all()
    assert rows[1].end_line == 20 and rows[10].end_line == 2
    assert {row.line for row in staged} == {20, 21, 22, 23, 24}


def test_scalar_partial_compaction_on_inactive_worker(engine: Engine, tmp_path: Path) -> None:
    rwid = _create_worker(engine)
    buffer = BufferStore()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    config.buffer.scalar_resolutions = [1, 10]
    with engine.begin() as conn:
        buffer.append_scalars(conn, rwid, 0, _scalars(0, 7))
        conn.execute(run_workers.update().where(run_workers.c.id == rwid).values(
            last_heartbeat=T0 - timedelta(seconds=config.buffer.worker_timeout_s + 1),
        ))
    buffer.compact(engine, storage)
    with engine.begin() as conn:
        rows = {r.resolution: r for r in conn.execute(
            select(scalar_segments).where(scalar_segments.c.worker_id == rwid),
        ).all()}
        assert conn.execute(select(scalar_points).where(scalar_points.c.worker_id == rwid)).all() == []
    assert rows[1].end_line == 7 and rows[10].end_line == 1
