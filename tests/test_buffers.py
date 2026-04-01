from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID

from sqlalchemy import select

import underfit_api.db as db
from underfit_api.buffer import LogBuffer, LogLine, ScalarBuffer, ScalarPoint
from underfit_api.config import FileStorageConfig, config
from underfit_api.repositories import projects as projects_repo
from underfit_api.repositories import runs as runs_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import log_segments, scalar_segments
from underfit_api.storage.file import FileStorage


def _create_run_id() -> UUID:
    with db.engine.begin() as conn:
        user = users_repo.create(conn, email="owner@example.com", handle="owner", name="Owner")
        project = projects_repo.create(conn, user.id, "underfit", None, "private")
        run = runs_repo.create(conn, project.id, user.id, "running", None)
        assert run is not None
        return run.id


def test_log_buffer_expands_multiline_and_slices_by_cursor() -> None:
    run_id = _create_run_id()
    buffer = LogBuffer()
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with db.engine.begin() as conn:
        expected = buffer.append(conn, run_id, "worker-1", 0, [
            LogLine(timestamp=t0, content="a\nb"),
            LogLine(timestamp=t0 + timedelta(seconds=1), content="c"),
        ])
        assert expected is None
        assert buffer.get_end_line(conn, run_id, "worker-1") == 3
        assert [line.content for line in buffer.read_buffered(run_id, "worker-1", cursor=1, count=2)] == ["b", "c"]

        conflict = buffer.append(conn, run_id, "worker-1", 1, [LogLine(timestamp=t0, content="late")])
        assert conflict == 3


def test_log_buffer_flushes_to_segment_and_tracks_byte_offsets(tmp_path: Path) -> None:
    run_id = _create_run_id()
    buffer = LogBuffer()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with db.engine.begin() as conn:
        assert buffer.append(conn, run_id, "worker-1", 0, [LogLine(timestamp=t0, content="first")]) is None
        buffer.flush(conn, storage, run_id, "worker-1")
        assert buffer.append(conn, run_id, "worker-1", 1, [LogLine(timestamp=t0, content="second")]) is None
        buffer.flush(conn, storage, run_id, "worker-1")

        segments = conn.execute(
            select(log_segments)
            .where(log_segments.c.run_id == run_id, log_segments.c.worker_id == "worker-1")
            .order_by(log_segments.c.start_line),
        ).all()

    assert len(segments) == 2
    assert (segments[0].start_line, segments[0].end_line, segments[1].start_line, segments[1].end_line) == (
        0, 1, 1, 2,
    )
    assert (segments[0].byte_offset, segments[1].byte_offset) == (0, segments[0].byte_count)
    assert segments[0].storage_key == segments[1].storage_key
    assert storage.read(segments[0].storage_key).decode() == "first\nsecond\n"


def test_log_buffer_flush_if_needed_uses_byte_threshold(tmp_path: Path) -> None:
    run_id = _create_run_id()
    buffer = LogBuffer()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    original = config.buffer.max_segment_bytes
    config.buffer.max_segment_bytes = 5

    try:
        with db.engine.begin() as conn:
            assert buffer.append(conn, run_id, "worker-1", 0, [
                LogLine(timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc), content="abcd"),
            ]) is None
            buffer.flush_if_needed(conn, storage, run_id, "worker-1")
            segments = conn.execute(
                select(log_segments).where(log_segments.c.run_id == run_id, log_segments.c.worker_id == "worker-1"),
            ).all()
        assert len(segments) == 1 and segments[0].byte_count == 5
    finally:
        config.buffer.max_segment_bytes = original


def test_scalar_buffer_builds_resolution_tiers() -> None:
    run_id = _create_run_id()
    buffer = ScalarBuffer()
    t0 = datetime(2025, 1, 1, tzinfo=timezone.utc)

    with db.engine.begin() as conn:
        points = [
            ScalarPoint(step=i, values={"loss": float(i + 1)}, timestamp=t0 + timedelta(seconds=i))
            for i in range(10)
        ]
        assert buffer.append(conn, run_id, 0, points) is None

        r1 = buffer.read_buffered(run_id, 1)
        r2 = buffer.read_buffered(run_id, 2)
        assert len(r1) == 10 and len(r2) == 1
        assert r2[0].values["loss"] == 5.5
        assert buffer.tier_line_count(conn, run_id, 2) == 1


def test_scalar_flush_if_needed_keeps_partial_higher_tiers_until_explicit_flush(tmp_path: Path) -> None:
    run_id = _create_run_id()
    buffer = ScalarBuffer()
    storage = FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    original = config.buffer.max_segment_bytes
    config.buffer.max_segment_bytes = 1

    try:
        with db.engine.begin() as conn:
            assert buffer.append(conn, run_id, 0, [
                ScalarPoint(
                    step=0,
                    values={"loss": 1.0},
                    timestamp=datetime(2025, 1, 1, tzinfo=timezone.utc),
                ),
            ]) is None
            buffer.flush_if_needed(conn, storage, run_id)

            by_res = {
                row.resolution: row
                for row in conn.execute(
                    select(scalar_segments).where(scalar_segments.c.run_id == run_id),
                ).all()
            }
            assert 0 in by_res
            assert 1 in by_res
            assert 2 not in by_res

            buffer.flush(conn, storage, run_id)
            by_res_after = {
                row.resolution: row
                for row in conn.execute(
                    select(scalar_segments).where(scalar_segments.c.run_id == run_id),
                ).all()
            }
            assert 2 in by_res_after
    finally:
        config.buffer.max_segment_bytes = original
