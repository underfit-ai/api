from __future__ import annotations

import asyncio
import json
from collections.abc import Callable
from pathlib import Path
from uuid import uuid4

import pytest
from sqlalchemy import select

import underfit_api.db as db
from underfit_api.config import BackfillConfig, FileStorageConfig, config
from underfit_api.schema import (
    accounts,
    artifacts,
    log_segments,
    media,
    project_aliases,
    projects,
    run_workers,
    runs,
    scalar_segments,
    users,
)
from underfit_api.storage.backfill import BackfillService
from underfit_api.storage.file import FileStorage


def _service() -> tuple[BackfillService, FileStorage]:
    assert isinstance(config.storage, FileStorageConfig)
    storage = FileStorage(config.storage)
    service = BackfillService(storage, db.engine, BackfillConfig(enabled=True, scan_interval_ms=10, debounce_ms=1))
    return service, storage


def _scan(service: BackfillService, storage: FileStorage) -> None:
    service._process_batch(set(storage.list_files("")))  # noqa: SLF001


def _write_json(storage: FileStorage, key: str, data: object) -> None:
    storage.write(key, json.dumps(data).encode())


def _write_text(storage: FileStorage, key: str, data: str) -> None:
    storage.write(key, data.encode())


def test_realtime_backfill_ingests_file_changes(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    storage = FileStorage(FileStorageConfig(base=str(tmp_path)))
    callbacks: list[Callable[[str], None]] = []
    monkeypatch.setattr(storage, "watch", callbacks.append)
    monkeypatch.setattr(storage, "stop_watching", lambda: None)
    service = BackfillService(
        storage,
        db.engine,
        BackfillConfig(enabled=True, scan_interval_ms=3_600_000, debounce_ms=5, realtime=True),
    )

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(service.start())
        run_id = uuid4()
        run_dir = Path(storage.base) / str(run_id)
        (run_dir / "logs" / "worker-1" / "segments").mkdir(parents=True)
        (run_dir / "run.json").write_text(json.dumps({"project": "RT", "name": "rt-run"}))
        (run_dir / "logs" / "worker-1" / "segments" / "0.log").write_text("line1\nline2\n")
        callbacks[0](f"{run_id}/logs/worker-1/segments/0.log")
        loop.run_until_complete(asyncio.sleep(0.05))
        with db.engine.begin() as conn:
            run_row = conn.execute(select(runs).where(runs.c.id == run_id)).first()
            log_row = conn.execute(select(log_segments)).first()
        assert run_row is not None and run_row.name == "rt-run"
        assert log_row is not None and (log_row.start_line, log_row.end_line) == (0, 2)
    finally:
        loop.run_until_complete(service.stop())
        loop.close()


def test_backfill_ingests_segment_files() -> None:
    service, storage = _service()
    run_id = uuid4()
    _write_json(storage, f"{run_id}/run.json", {
        "project": "Vision",
        "name": "Trial A",
        "config": {"lr": 0.01, "seed": 7},
        "metadata": {"summary": {"loss": 0.6}},
    })
    _write_text(storage, f"{run_id}/logs/worker-1/segments/0.log", "hello\nworld\n")
    _write_text(storage, f"{run_id}/scalars/0/r1/0.jsonl", (
        '{"step":1,"values":{"loss":0.8},"timestamp":"2025-01-01T00:00:00Z"}\n'
        '{"step":2,"values":{"loss":0.6},"timestamp":"2025-01-01T00:00:01Z"}\n'
    ))
    _scan(service, storage)

    with db.engine.begin() as conn:
        assert len(conn.execute(select(accounts)).all()) == 1
        assert len(conn.execute(select(users)).all()) == 1
        assert len(conn.execute(select(projects)).all()) == 1
        project_row = conn.execute(select(projects)).first()
        alias_row = conn.execute(select(project_aliases)).first()
        run_row = conn.execute(select(runs)).first()
        log_row = conn.execute(select(log_segments)).first()
        scalar_row = conn.execute(select(scalar_segments)).first()
        worker_row = conn.execute(select(run_workers)).first()

    assert project_row is not None and project_row.name == "vision"
    assert alias_row is not None and alias_row.name == "vision"
    assert run_row is not None
    assert run_row.id == run_id and run_row.name == "trial a" and run_row.storage_key == str(run_id)
    assert run_row.terminal_state is None and run_row.config == {"lr": 0.01, "seed": 7}
    assert run_row.metadata == {"summary": {"loss": 0.6}}
    assert worker_row is not None and worker_row.worker_label == "worker-1"
    assert log_row is not None and (log_row.start_line, log_row.end_line) == (0, 2)
    assert scalar_row is not None and (scalar_row.resolution, scalar_row.start_line, scalar_row.end_line) == (1, 0, 2)
    assert scalar_row.end_at.isoformat() == "2025-01-01T00:00:01"


def test_backfill_stops_scalar_segment_at_invalid_json() -> None:
    service, storage = _service()
    run_id = uuid4()
    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial B", "terminal_state": "finished"})
    _write_text(storage, f"{run_id}/scalars/0/r1/0.jsonl", (
        '{"step":0,"timestamp":"2025-01-01T00:00:00Z"}\n'
        '{"step":1,"timestamp":"2025-01-01T00:00:01Z"}\n'
        "{bad-json}\n"
    ))
    _scan(service, storage)

    with db.engine.begin() as conn:
        scalar_row = conn.execute(select(scalar_segments)).first()
    assert scalar_row is not None and (scalar_row.start_line, scalar_row.end_line, scalar_row.end_at.isoformat()) == (
        0, 2, "2025-01-01T00:00:01",
    )


def test_backfill_updates_artifact_and_media_records() -> None:
    service, storage = _service()
    run_id = uuid4()
    artifact_id = uuid4()
    media_id = uuid4()

    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial C"})
    _write_json(storage, f"{run_id}/artifacts/{artifact_id}/artifact.json", {
        "name": "dataset-v1",
        "type": "dataset",
        "step": 10,
        "metadata": {"format": "parquet"},
    })
    _write_json(storage, f"{run_id}/artifacts/{artifact_id}/manifest.json", {"files": ["a.bin", "b.bin"]})
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
    assert artifact_row.finalized_at is None
    assert media_row.count == 2

    _write_text(storage, f"{run_id}/artifacts/{artifact_id}/files/b.bin", "b")
    _write_text(storage, f"{run_id}/media/{media_id}/2", "m2")
    _scan(service, storage)

    with db.engine.begin() as conn:
        artifact_row = conn.execute(select(artifacts).where(artifacts.c.id == artifact_id)).first()
        media_row = conn.execute(select(media).where(media.c.id == media_id)).first()
    assert artifact_row is not None and media_row is not None
    assert artifact_row.finalized_at is not None and artifact_row.stored_size_bytes == 2
    assert media_row.count == 3
