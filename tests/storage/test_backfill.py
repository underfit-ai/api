from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

from app.config import BackfillConfig, BufferConfig, config
from app.db import get_engine
from app.schema import accounts, artifacts, log_segments, media, projects, runs, scalar_segments, users
from app.storage.backfill import BackfillService
from app.storage.file import FileStorage


def _service(max_segment_bytes: int = 256 * 1024) -> tuple[BackfillService, FileStorage]:
    storage = FileStorage(config.storage.base)
    service = BackfillService(
        storage,
        get_engine(),
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


def test_realtime_backfill_ingests_file_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = FileStorage(str(tmp_path))
    callbacks: list[Callable[[str], None]] = []

    monkeypatch.setattr(storage, "watch", callbacks.append)
    monkeypatch.setattr(storage, "stop_watching", lambda: None)

    service = BackfillService(
        storage,
        get_engine(),
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

        with get_engine().begin() as conn:
            run_rows = conn.execute(select(runs).where(runs.c.id == run_id)).all()
            log_rows = conn.execute(select(log_segments).where(log_segments.c.run_id == run_id)).all()

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
    _write_text(storage, f"{run_id}/scalars/r0.jsonl", scalar_content)

    _scan(service, storage)

    with get_engine().begin() as conn:
        account_rows = conn.execute(select(accounts)).all()
        user_rows = conn.execute(select(users)).all()
        project_rows = conn.execute(select(projects)).all()
        run_rows = conn.execute(select(runs)).all()
        log_rows = conn.execute(select(log_segments)).all()
        scalar_rows = conn.execute(select(scalar_segments)).all()

    assert len(account_rows) == 1
    assert account_rows[0].handle == "local"
    assert len(user_rows) == 1
    assert user_rows[0].email == "local@underfit.local"
    assert len(project_rows) == 1
    assert project_rows[0].name == "Vision"
    assert len(run_rows) == 1
    assert run_rows[0].id == run_id
    assert run_rows[0].name == "Trial A"
    assert run_rows[0].status == "running"
    assert run_rows[0].config == {"lr": 0.01, "seed": 7}

    assert len(log_rows) == 1
    assert log_rows[0].run_id == run_id
    assert log_rows[0].worker_id == "worker-1"
    assert log_rows[0].start_line == 0
    assert log_rows[0].end_line == 2
    assert log_rows[0].byte_offset == 0
    assert log_rows[0].byte_count == len(log_content.encode())

    assert len(scalar_rows) == 1
    assert scalar_rows[0].run_id == run_id
    assert scalar_rows[0].resolution == 0
    assert scalar_rows[0].start_line == 0
    assert scalar_rows[0].end_line == 2
    assert scalar_rows[0].start_at.isoformat() == "2025-01-01T00:00:00"
    assert scalar_rows[0].end_at.isoformat() == "2025-01-01T00:00:01"
    assert scalar_rows[0].byte_offset == 0
    assert scalar_rows[0].byte_count == len(scalar_content.encode())


def test_backfill_appends_without_duplicate_segments() -> None:
    service, storage = _service()
    run_id = uuid4()
    scalar_a0 = {"step": 0, "values": {"loss": 1.0}, "timestamp": "2025-01-01T00:00:00Z"}
    scalar_a1 = {"step": 1, "values": {"loss": 0.8}, "timestamp": "2025-01-01T00:00:01Z"}
    scalar_a2 = {"step": 2, "values": {"loss": 0.6}, "timestamp": "2025-01-01T00:00:02Z"}

    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial B"})
    _write_text(storage, f"{run_id}/logs/worker-1.log", "l0\nl1\n")
    _write_text(storage, f"{run_id}/scalars/r0.jsonl", f"{json.dumps(scalar_a0)}\n{json.dumps(scalar_a1)}\n")
    _scan(service, storage)

    appended_log = "l0\nl1\nl2\n"
    appended_scalars = f"{json.dumps(scalar_a0)}\n{json.dumps(scalar_a1)}\n{json.dumps(scalar_a2)}\n"
    _write_text(storage, f"{run_id}/logs/worker-1.log", appended_log)
    _write_text(storage, f"{run_id}/scalars/r0.jsonl", appended_scalars)
    _scan(service, storage)

    with get_engine().begin() as conn:
        log_rows = conn.execute(
            select(log_segments).where(log_segments.c.run_id == run_id, log_segments.c.worker_id == "worker-1"),
        ).all()
        scalar_rows = conn.execute(
            select(scalar_segments).where(scalar_segments.c.run_id == run_id, scalar_segments.c.resolution == 0),
        ).all()

    assert len(log_rows) == 1
    assert log_rows[0].start_line == 0
    assert log_rows[0].end_line == 3
    assert log_rows[0].byte_offset == 0
    assert log_rows[0].byte_count == len(appended_log.encode())

    assert len(scalar_rows) == 1
    assert scalar_rows[0].start_line == 0
    assert scalar_rows[0].end_line == 3
    assert scalar_rows[0].byte_offset == 0
    assert scalar_rows[0].byte_count == len(appended_scalars.encode())
    assert scalar_rows[0].end_at.isoformat() == "2025-01-01T00:00:02"


def test_backfill_rebuilds_segments_after_truncation() -> None:
    service, storage = _service()
    run_id = uuid4()

    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial C"})
    _write_text(storage, f"{run_id}/logs/worker-1.log", "a\nb\nc\n")
    _write_text(
        storage,
        f"{run_id}/scalars/r0.jsonl",
        "{\"timestamp\":\"2025-01-01T00:00:00Z\"}\n{\"timestamp\":\"2025-01-01T00:00:01Z\"}\n",
    )
    _scan(service, storage)

    truncated_log = "x\n"
    truncated_scalars = "{\"timestamp\":\"2025-01-02T00:00:00Z\"}\n"
    _write_text(storage, f"{run_id}/logs/worker-1.log", truncated_log)
    _write_text(storage, f"{run_id}/scalars/r0.jsonl", truncated_scalars)
    _scan(service, storage)

    with get_engine().begin() as conn:
        log_rows = conn.execute(
            select(log_segments).where(log_segments.c.run_id == run_id, log_segments.c.worker_id == "worker-1"),
        ).all()
        scalar_rows = conn.execute(
            select(scalar_segments).where(scalar_segments.c.run_id == run_id, scalar_segments.c.resolution == 0),
        ).all()

    assert len(log_rows) == 1
    assert log_rows[0].start_line == 0
    assert log_rows[0].end_line == 1
    assert log_rows[0].byte_offset == 0
    assert log_rows[0].byte_count == len(truncated_log.encode())

    assert len(scalar_rows) == 1
    assert scalar_rows[0].start_line == 0
    assert scalar_rows[0].end_line == 1
    assert scalar_rows[0].byte_offset == 0
    assert scalar_rows[0].byte_count == len(truncated_scalars.encode())


def test_backfill_skips_orphan_data_and_stops_scalar_at_invalid_json() -> None:
    service, storage = _service()
    orphan_run_id = uuid4()
    valid_run_id = uuid4()

    _write_text(storage, f"{orphan_run_id}/logs/worker-1.log", "orphaned\n")
    _write_text(storage, f"{orphan_run_id}/scalars/r0.jsonl", '{"timestamp":"2025-01-01T00:00:00Z"}\n')

    scalar_lines = (
        '{"step":0,"timestamp":"2025-01-01T00:00:00Z"}\n'
        '{"step":1,"timestamp":"2025-01-01T00:00:01Z"}\n'
        "{bad-json}\n"
        '{"step":2,"timestamp":"2025-01-01T00:00:02Z"}\n'
    )
    _write_json(storage, f"{valid_run_id}/run.json", {"project": "Vision", "name": "Trial D", "status": "finished"})
    _write_text(storage, f"{valid_run_id}/scalars/r0.jsonl", scalar_lines)

    _scan(service, storage)

    with get_engine().begin() as conn:
        run_rows = conn.execute(select(runs).order_by(runs.c.id)).all()
        log_rows = conn.execute(select(log_segments)).all()
        scalar_rows = conn.execute(select(scalar_segments).order_by(scalar_segments.c.run_id)).all()

    assert [row.id for row in run_rows] == [valid_run_id]
    assert log_rows == []
    assert len(scalar_rows) == 1
    assert scalar_rows[0].run_id == valid_run_id
    assert scalar_rows[0].start_line == 0
    assert scalar_rows[0].end_line == 2
    assert scalar_rows[0].end_at.isoformat() == "2025-01-01T00:00:01"


def test_backfill_skips_run_with_invalid_run_json_status() -> None:
    service, storage = _service()
    invalid_run_id = uuid4()
    valid_run_id = uuid4()

    _write_json(storage, f"{invalid_run_id}/run.json", {"project": "Vision", "name": "Bad Trial", "status": "oops"})
    _write_json(storage, f"{valid_run_id}/run.json", {"project": "Vision", "name": "Good Trial", "status": "finished"})
    _scan(service, storage)

    with get_engine().begin() as conn:
        run_rows = conn.execute(select(runs).order_by(runs.c.id)).all()

    assert [row.id for row in run_rows] == [valid_run_id]
    assert run_rows[0].name == "Good Trial"
    assert run_rows[0].status == "finished"


def test_backfill_updates_artifact_and_media_records() -> None:
    service, storage = _service()
    run_id = uuid4()
    artifact_id = uuid4()
    media_id = uuid4()

    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial E"})
    _write_json(storage, f"{run_id}/artifacts/{artifact_id}/manifest.json", {
        "name": "dataset-v1",
        "type": "dataset",
        "step": 10,
        "metadata": {"format": "parquet"},
        "files": [{"path": "a.bin"}, {"path": "b.bin"}],
    })
    _write_text(storage, f"{run_id}/artifacts/{artifact_id}/files/0", "a")

    _write_text(storage, f"{run_id}/media/{media_id}/0", "m0")
    _write_text(storage, f"{run_id}/media/{media_id}/1", "m1")
    _write_json(storage, f"{run_id}/media/{media_id}/metadata.json", {
        "key": "samples",
        "step": 7,
        "type": "image",
        "metadata": {"split": "val"},
    })

    _scan(service, storage)

    with get_engine().begin() as conn:
        artifact_row = conn.execute(select(artifacts).where(artifacts.c.id == artifact_id)).first()
        media_row = conn.execute(select(media).where(media.c.id == media_id)).first()

    assert artifact_row is not None
    assert artifact_row.run_id == run_id
    assert artifact_row.name == "dataset-v1"
    assert artifact_row.status == "open"
    assert artifact_row.declared_file_count == 2
    assert artifact_row.uploaded_file_count == 1

    assert media_row is not None
    assert media_row.run_id == run_id
    assert media_row.key == "samples"
    assert media_row.step == 7
    assert media_row.type == "image"
    assert media_row.count == 2
    assert media_row.metadata == {"split": "val"}

    _write_text(storage, f"{run_id}/artifacts/{artifact_id}/files/1", "b")
    _write_text(storage, f"{run_id}/media/{media_id}/2", "m2")
    _scan(service, storage)

    with get_engine().begin() as conn:
        artifact_row = conn.execute(select(artifacts).where(artifacts.c.id == artifact_id)).first()
        media_row = conn.execute(select(media).where(media.c.id == media_id)).first()

    assert artifact_row is not None
    assert artifact_row.status == "finalized"
    assert artifact_row.uploaded_file_count == 2
    assert artifact_row.finalized_at is not None

    assert media_row is not None
    assert media_row.count == 3
