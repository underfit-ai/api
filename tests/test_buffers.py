from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

import pytest
from sqlalchemy import Engine, select

import underfit_api.buffer as buffer_mod
from underfit_api.buffer import BadStartLineError, LogBuffer, LogLine, ScalarBuffer
from underfit_api.config import FileStorageConfig, config
from underfit_api.helpers import utcnow
from underfit_api.models import Scalar
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import run_workers as workers_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import log_segments, run_workers, scalar_segments
from underfit_api.storage.file import FileStorage


def _create_worker(engine: Engine, worker_label: str = "0") -> UUID:
    with engine.begin() as conn:
        user = users_repo.create(conn, email="owner@example.com", handle="owner", name="Owner")
        project = projects_repo.create(conn, user.id, "underfit", None, "private", {})
        run = runs_repo.create(conn, project.id, user.id, "test-launch-id", "test-run", None, {})
        worker = workers_repo.create(conn, run.id, worker_label)
        return worker.id


def _run_storage_key(engine: Engine, worker_id: UUID) -> str:
    with engine.begin() as conn:
        worker = workers_repo.get_by_id(conn, worker_id)
        assert worker is not None
        run = runs_repo.get_by_id(conn, worker.run_id)
        assert run is not None
        return run.storage_key


def test_log_buffer_slices_by_cursor(engine: Engine) -> None:
    rwid = _create_worker(engine, "worker-1")
    buffer = LogBuffer()
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with engine.begin() as conn:
        buffer.append(conn, rwid, 0, [
            LogLine(timestamp=t0, content="a"),
            LogLine(timestamp=t0 + timedelta(seconds=1), content="b"),
        ])
        assert buffer.get_end_line(conn, rwid) == 2
        assert [line.content for line in buffer.read_buffered(rwid, cursor=1, count=2)] == ["b"]

        with pytest.raises(BadStartLineError) as exc_info:
            buffer.append(conn, rwid, 1, [LogLine(timestamp=t0, content="late")])
        assert exc_info.value.expected == 2


def test_log_buffer_flushes_to_segment_and_tracks_byte_offsets(engine: Engine, tmp_path: Path) -> None:
    rwid = _create_worker(engine, "worker-1")
    buffer = LogBuffer()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with engine.begin() as conn:
        assert buffer.append(conn, rwid, 0, [LogLine(timestamp=t0, content="first")]) is None
        buffer.flush(conn, storage, rwid)
        assert buffer.append(conn, rwid, 1, [LogLine(timestamp=t0, content="second")]) is None
        buffer.flush(conn, storage, rwid)

        segments = conn.execute(
            select(log_segments)
            .where(log_segments.c.worker_id == rwid)
            .order_by(log_segments.c.start_line),
        ).all()

    assert [(s.start_line, s.end_line) for s in segments] == [(0, 1), (1, 2)]
    assert storage.read(f"{_run_storage_key(engine, rwid)}/{segments[0].storage_key}").decode() == "first\n"
    assert storage.read(f"{_run_storage_key(engine, rwid)}/{segments[1].storage_key}").decode() == "second\n"


def test_log_buffer_persists_due_then_flushes_inactive(
    engine: Engine, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    rwid = _create_worker(engine, "worker-1")
    buffer = LogBuffer()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    rows_query = select(log_segments).where(log_segments.c.worker_id == rwid).order_by(log_segments.c.start_line)

    with engine.begin() as conn:
        assert buffer.append(conn, rwid, 0, [LogLine(timestamp=t0, content="a")]) is None
        monkeypatch.setattr(buffer_mod, "utcnow", lambda: utcnow() + timedelta(days=1))
        buffer.persist_due(conn, storage)
        rows = conn.execute(rows_query).all()
        assert len(rows) == 1 and rows[0].end_line == 1
        assert buffer.buffer_start_line(rwid) == 0

        assert buffer.append(conn, rwid, 1, [LogLine(timestamp=t0, content="b")]) is None
        conn.execute(run_workers.update().where(run_workers.c.id == rwid).values(
            last_heartbeat=t0 - timedelta(seconds=config.buffer.worker_timeout_s + 1),
        ))
        buffer.flush_inactive(conn, storage)

        assert buffer.buffer_start_line(rwid) is None
        assert [(r.start_line, r.end_line) for r in conn.execute(rows_query).all()] == [(0, 2)]
        assert storage.read(f"{_run_storage_key(engine, rwid)}/{rows[0].storage_key}").decode() == "a\nb\n"


def test_log_buffer_flush_if_needed_uses_byte_threshold(engine: Engine, tmp_path: Path) -> None:
    rwid = _create_worker(engine, "worker-1")
    buffer = LogBuffer()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    config.buffer.max_segment_bytes = 5

    with engine.begin() as conn:
        assert buffer.append(conn, rwid, 0, [
            LogLine(timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc), content="abcd"),
        ]) is None
        buffer.flush_if_needed(conn, storage, rwid)
        segments = conn.execute(select(log_segments).where(log_segments.c.worker_id == rwid)).all()
    assert len(segments) == 1
    assert storage.read(f"{_run_storage_key(engine, rwid)}/{segments[0].storage_key}").decode() == "abcd\n"


def test_scalar_buffer_builds_resolution_tiers(engine: Engine) -> None:
    rwid = _create_worker(engine)
    buffer = ScalarBuffer()
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with engine.begin() as conn:
        points = [
            Scalar(step=i, values={"loss": float(i + 1)}, timestamp=t0 + timedelta(seconds=i))
            for i in range(10)
        ]
        assert buffer.append(conn, rwid, 0, points) is None

        r1 = buffer.read_buffered(rwid, 1)
        r10 = buffer.read_buffered(rwid, 10)
        assert len(r1) == 10 and len(r10) == 1
        assert r10[0].values["loss"] == 5.5
        assert buffer.resolution_line_count(conn, rwid, 10) == 1


def test_scalar_buffer_persists_all_resolutions_in_place(engine: Engine, tmp_path: Path) -> None:
    rwid = _create_worker(engine)
    buffer = ScalarBuffer()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    config.buffer.scalar_resolutions = [2]
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)
    points = [Scalar(step=i, values={"loss": float(i)}, timestamp=t0) for i in range(4)]
    rows_query = select(scalar_segments).where(scalar_segments.c.worker_id == rwid)
    with engine.begin() as conn:
        assert buffer.append(conn, rwid, 0, points[:2]) is None
        buffer.persist(conn, storage, rwid)
        r2_id = next(r.id for r in conn.execute(rows_query).all() if r.resolution == 2)
        assert buffer.append(conn, rwid, 2, points[2:]) is None
        buffer.persist(conn, storage, rwid)
        rows = {r.resolution: r for r in conn.execute(rows_query).all()}
        assert rows[1].end_line == 4 and rows[2].end_line == 2 and rows[2].id == r2_id


def test_scalar_flush_if_needed_keeps_partial_higher_tiers_until_explicit_flush(
    engine: Engine, tmp_path: Path,
) -> None:
    rwid = _create_worker(engine)
    buffer = ScalarBuffer()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    config.buffer.max_segment_bytes = 1
    config.buffer.scalar_resolutions = [2]

    with engine.begin() as conn:
        assert buffer.append(conn, rwid, 0, [
            Scalar(
                step=0,
                values={"loss": 1.0},
                timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
            ),
        ]) is None
        buffer.flush_if_needed(conn, storage, rwid)

        by_res = {
            row.resolution: row
            for row in conn.execute(
                select(scalar_segments).where(scalar_segments.c.worker_id == rwid),
            ).all()
        }
        assert 1 in by_res
        assert 2 not in by_res

        buffer.flush(conn, storage, rwid)
        by_res_after = {
            row.resolution: row
            for row in conn.execute(
                select(scalar_segments).where(scalar_segments.c.worker_id == rwid),
            ).all()
        }
        assert 2 in by_res_after
        buffer = ScalarBuffer()
        assert buffer.append(conn, rwid, 1, [
            Scalar(step=1, values={"loss": 2.0}, timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc)),
        ]) is None
        buffer.flush(conn, storage, rwid)
        assert conn.execute(select(scalar_segments).where(
            scalar_segments.c.worker_id == rwid, scalar_segments.c.resolution == 2,
        ).order_by(scalar_segments.c.start_line)).all()[-1].start_line == 1
