from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

import underfit_api.db as db
from underfit_api.config import BackfillConfig, BufferConfig, FileStorageConfig, config
from underfit_api.schema import (
    accounts,
    artifacts,
    log_segments,
    media,
    projects,
    run_workers,
    runs,
    scalar_segments,
    users,
)
from underfit_api.storage.backfill import BackfillService
from underfit_api.storage.file import FileStorage


def _service(max_segment_bytes: int = 256 * 1024) -> tuple[BackfillService, FileStorage]:
    assert isinstance(config.storage, FileStorageConfig)
    storage = FileStorage(config.storage)
    service = BackfillService(
        storage,
        db.engine,
        BackfillConfig(enabled=True, scan_interval_ms=10, debounce_ms=1, realtime=False),
        BufferConfig(max_segment_bytes=max_segment_bytes, max_segment_age_ms=30_000, flush_interval_ms=1_000),
    )
    return service, storage


def _scan(service: BackfillService, storage: FileStorage) -> None:
    service._process_batch(set(storage.list_files("")))  # noqa: SLF001


def _write_json(storage: FileStorage, key: str, data: object) -> None:
    storage.write(key, json.dumps(data).encode())


def _write_text(storage: FileStorage, key: str, data: str) -> None:
    storage.write(key, data.encode())


_log_join = log_segments.join(run_workers, log_segments.c.worker_id == run_workers.c.id)
_scalar_join = scalar_segments.join(run_workers, scalar_segments.c.worker_id == run_workers.c.id)


def test_realtime_backfill_ingests_file_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = FileStorage(FileStorageConfig(base=str(tmp_path)))
    callbacks: list[Callable[[str], None]] = []

    monkeypatch.setattr(storage, "watch", callbacks.append)
    monkeypatch.setattr(storage, "stop_watching", lambda: None)

    service = BackfillService(
        storage,
        db.engine,
        BackfillConfig(enabled=True, scan_interval_ms=3_600_000, debounce_ms=5, realtime=True),
        BufferConfig(max_segment_bytes=256 * 1024, max_segment_age_ms=30_000, flush_interval_ms=1_000),
    )

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(service.start())
        assert callbacks

        run_id = uuid4()
        run_dir = Path(storage.base) / str(run_id)
        run_dir.mkdir(parents=True)
        (run_dir / "run.json").write_text(json.dumps({"project": "RT", "name": "rt-run"}))
        log_dir = run_dir / "logs"
        log_dir.mkdir()
        (log_dir / "worker-1.log").write_text("line1\nline2\n")

        callbacks[0](f"{run_id}/logs/worker-1.log")
        loop.run_until_complete(asyncio.sleep(0.05))

        with db.engine.begin() as conn:
            run_rows = conn.execute(select(runs).where(runs.c.id == run_id)).all()
            log_rows = conn.execute(
                select(log_segments).select_from(_log_join).where(run_workers.c.run_id == run_id),
            ).all()

        assert len(run_rows) == 1
        assert run_rows[0].name == "rt-run"
        assert len(log_rows) == 1
        assert log_rows[0].end_line == 2
    finally:
        loop.run_until_complete(service.stop())
        loop.close()


def test_backfill_ingests_run_logs_and_scalars() -> None:
    service, storage = _service()
    run_id = uuid4()
    line0 = {"step": 1, "values": {"loss": 0.8}, "timestamp": "2025-01-01T00:00:00Z"}
    line1 = {"step": 2, "values": {"loss": 0.6}, "timestamp": "2025-01-01T00:00:01Z"}
    log_content = "hello\nworld\n"
    scalar_content = f"{json.dumps(line0)}\n{json.dumps(line1)}\n"

    _write_json(storage, f"{run_id}/run.json", {
        "project": "Vision",
        "name": "Trial A",
        "status": "running",
        "config": {"lr": 0.01, "seed": 7},
    })
    _write_text(storage, f"{run_id}/logs/worker-1.log", log_content)
    _write_text(storage, f"{run_id}/scalars/0/raw.jsonl", scalar_content)

    _scan(service, storage)

    with db.engine.begin() as conn:
        account_rows = conn.execute(select(accounts)).all()
        user_rows = conn.execute(select(users)).all()
        project_rows = conn.execute(select(projects)).all()
        run_rows = conn.execute(select(runs)).all()
        log_rows = conn.execute(select(log_segments)).all()
        scalar_rows = conn.execute(select(scalar_segments)).all()

    assert len(account_rows) == len(user_rows) == len(project_rows) == len(run_rows) == 1
    assert (account_rows[0].handle, user_rows[0].email, project_rows[0].name) == (
        "local", "local@underfit.local", "Vision",
    )
    assert (run_rows[0].id, run_rows[0].name, run_rows[0].status, run_rows[0].config) == (
        run_id, "Trial A", "running", {"lr": 0.01, "seed": 7},
    )

    assert len(log_rows) == len(scalar_rows) == 1
    assert (log_rows[0].start_line, log_rows[0].end_line, log_rows[0].byte_offset, log_rows[0].byte_count) == (
        0, 2, 0, len(log_content.encode()),
    )

    scalar = scalar_rows[0]
    assert (scalar.resolution, scalar.start_line, scalar.end_line) == (0, 0, 2)
    assert (scalar.start_at.isoformat(), scalar.end_at.isoformat()) == (
        "2025-01-01T00:00:00", "2025-01-01T00:00:01",
    )
    assert (scalar.byte_offset, scalar.byte_count) == (0, len(scalar_content.encode()))


def test_backfill_appends_without_duplicate_segments() -> None:
    service, storage = _service()
    run_id = uuid4()
    scalar_a0 = {"step": 0, "values": {"loss": 1.0}, "timestamp": "2025-01-01T00:00:00Z"}
    scalar_a1 = {"step": 1, "values": {"loss": 0.8}, "timestamp": "2025-01-01T00:00:01Z"}
    scalar_a2 = {"step": 2, "values": {"loss": 0.6}, "timestamp": "2025-01-01T00:00:02Z"}

    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial B"})
    _write_text(storage, f"{run_id}/logs/worker-1.log", "l0\nl1\n")
    _write_text(storage, f"{run_id}/scalars/0/raw.jsonl", f"{json.dumps(scalar_a0)}\n{json.dumps(scalar_a1)}\n")
    _scan(service, storage)

    appended_log = "l0\nl1\nl2\n"
    appended_scalars = f"{json.dumps(scalar_a0)}\n{json.dumps(scalar_a1)}\n{json.dumps(scalar_a2)}\n"
    _write_text(storage, f"{run_id}/logs/worker-1.log", appended_log)
    _write_text(storage, f"{run_id}/scalars/0/raw.jsonl", appended_scalars)
    _scan(service, storage)

    with db.engine.begin() as conn:
        log_rows = conn.execute(
            select(log_segments).select_from(_log_join)
            .where(run_workers.c.run_id == run_id, run_workers.c.worker_label == "worker-1"),
        ).all()
        scalar_rows = conn.execute(
            select(scalar_segments).select_from(_scalar_join)
            .where(run_workers.c.run_id == run_id, scalar_segments.c.resolution == 0),
        ).all()

    assert len(log_rows) == len(scalar_rows) == 1
    assert (log_rows[0].start_line, log_rows[0].end_line, log_rows[0].byte_offset, log_rows[0].byte_count) == (
        0, 3, 0, len(appended_log.encode()),
    )
    assert (scalar_rows[0].start_line, scalar_rows[0].end_line) == (0, 3)
    assert (scalar_rows[0].byte_offset, scalar_rows[0].byte_count, scalar_rows[0].end_at.isoformat()) == (
        0, len(appended_scalars.encode()), "2025-01-01T00:00:02",
    )


def test_backfill_rebuilds_segments_after_truncation() -> None:
    service, storage = _service()
    run_id = uuid4()

    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial C"})
    _write_text(storage, f"{run_id}/logs/worker-1.log", "a\nb\nc\n")
    _write_text(
        storage,
        f"{run_id}/scalars/0/raw.jsonl",
        "{\"timestamp\":\"2025-01-01T00:00:00Z\"}\n{\"timestamp\":\"2025-01-01T00:00:01Z\"}\n",
    )
    _scan(service, storage)

    truncated_log = "x\n"
    truncated_scalars = "{\"timestamp\":\"2025-01-02T00:00:00Z\"}\n"
    _write_text(storage, f"{run_id}/logs/worker-1.log", truncated_log)
    _write_text(storage, f"{run_id}/scalars/0/raw.jsonl", truncated_scalars)
    _scan(service, storage)

    with db.engine.begin() as conn:
        log_rows = conn.execute(
            select(log_segments).select_from(_log_join)
            .where(run_workers.c.run_id == run_id, run_workers.c.worker_label == "worker-1"),
        ).all()
        scalar_rows = conn.execute(
            select(scalar_segments).select_from(_scalar_join)
            .where(run_workers.c.run_id == run_id, scalar_segments.c.resolution == 0),
        ).all()

    assert len(log_rows) == len(scalar_rows) == 1
    assert (log_rows[0].start_line, log_rows[0].end_line, log_rows[0].byte_offset, log_rows[0].byte_count) == (
        0, 1, 0, len(truncated_log.encode()),
    )
    assert (
        scalar_rows[0].start_line,
        scalar_rows[0].end_line,
        scalar_rows[0].byte_offset,
        scalar_rows[0].byte_count,
    ) == (0, 1, 0, len(truncated_scalars.encode()))


def test_backfill_skips_orphan_data_and_stops_scalar_at_invalid_json() -> None:
    service, storage = _service()
    orphan_run_id = uuid4()
    valid_run_id = uuid4()

    _write_text(storage, f"{orphan_run_id}/logs/worker-1.log", "orphaned\n")
    _write_text(storage, f"{orphan_run_id}/scalars/0/raw.jsonl", '{"timestamp":"2025-01-01T00:00:00Z"}\n')

    scalar_lines = (
        '{"step":0,"timestamp":"2025-01-01T00:00:00Z"}\n'
        '{"step":1,"timestamp":"2025-01-01T00:00:01Z"}\n'
        "{bad-json}\n"
        '{"step":2,"timestamp":"2025-01-01T00:00:02Z"}\n'
    )
    _write_json(storage, f"{valid_run_id}/run.json", {"project": "Vision", "name": "Trial D", "status": "finished"})
    _write_text(storage, f"{valid_run_id}/scalars/0/raw.jsonl", scalar_lines)

    _scan(service, storage)

    with db.engine.begin() as conn:
        run_rows = conn.execute(select(runs).order_by(runs.c.id)).all()
        log_rows = conn.execute(select(log_segments)).all()
        scalar_rows = conn.execute(select(scalar_segments)).all()

    assert [row.id for row in run_rows] == [valid_run_id]
    assert log_rows == []
    assert len(scalar_rows) == 1
    scalar = scalar_rows[0]
    assert (scalar.start_line, scalar.end_line, scalar.end_at.isoformat()) == (0, 2, "2025-01-01T00:00:01")


def test_backfill_skips_run_with_invalid_run_json_status() -> None:
    service, storage = _service()
    invalid_run_id = uuid4()
    valid_run_id = uuid4()

    _write_json(storage, f"{invalid_run_id}/run.json", {"project": "Vision", "name": "Bad Trial", "status": "oops"})
    _write_json(storage, f"{valid_run_id}/run.json", {"project": "Vision", "name": "Good Trial", "status": "finished"})
    _scan(service, storage)

    with db.engine.begin() as conn:
        run_rows = conn.execute(select(runs).order_by(runs.c.id)).all()

    assert [row.id for row in run_rows] == [valid_run_id]
    assert (run_rows[0].name, run_rows[0].status) == ("Good Trial", "finished")


def test_backfill_updates_artifact_and_media_records() -> None:
    service, storage = _service()
    run_id = uuid4()
    artifact_id = uuid4()
    media_id = uuid4()

    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial E"})
    _write_json(storage, f"{run_id}/artifacts/{artifact_id}/artifact.json", {
        "name": "dataset-v1",
        "type": "dataset",
        "step": 10,
        "metadata": {"format": "parquet"},
    })
    _write_json(storage, f"{run_id}/artifacts/{artifact_id}/manifest.json", {
        "files": ["a.bin", "b.bin"],
    })
    _write_text(storage, f"{run_id}/artifacts/{artifact_id}/files/a.bin", "a")

    _write_text(storage, f"{run_id}/media/{media_id}/0", "m0")
    _write_text(storage, f"{run_id}/media/{media_id}/1", "m1")
    _write_json(storage, f"{run_id}/media/{media_id}/media.json", {
        "key": "samples",
        "step": 7,
        "type": "image",
        "metadata": {"split": "val"},
    })

    _scan(service, storage)

    with db.engine.begin() as conn:
        artifact_row = conn.execute(select(artifacts).where(artifacts.c.id == artifact_id)).first()
        media_row = conn.execute(select(media).where(media.c.id == media_id)).first()

    assert artifact_row is not None and media_row is not None
    assert (artifact_row.run_id, artifact_row.name, artifact_row.step, artifact_row.metadata) == (
        run_id, "dataset-v1", 10, {"format": "parquet"},
    )
    assert artifact_row.finalized_at is None
    assert artifact_row.stored_size_bytes is None
    assert (media_row.run_id, media_row.key, media_row.step, media_row.type, media_row.count, media_row.metadata) == (
        run_id, "samples", 7, "image", 2, {"split": "val"},
    )

    _write_text(storage, f"{run_id}/artifacts/{artifact_id}/files/b.bin", "b")
    _write_text(storage, f"{run_id}/media/{media_id}/2", "m2")
    _scan(service, storage)

    with db.engine.begin() as conn:
        artifact_row = conn.execute(select(artifacts).where(artifacts.c.id == artifact_id)).first()
        media_row = conn.execute(select(media).where(media.c.id == media_id)).first()

    assert artifact_row is not None and media_row is not None
    assert media_row.count == 3
    assert artifact_row.stored_size_bytes == 2
    assert artifact_row.finalized_at is not None
