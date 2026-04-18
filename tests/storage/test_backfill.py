from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
from uuid import uuid4

import boto3
import pytest
from moto import mock_aws
from sqlalchemy import Engine, select

from underfit_api import backfill
from underfit_api.config import FileStorageConfig, S3StorageConfig
from underfit_api.dependencies import AppContext
from underfit_api.repositories import organizations as organizations_repo
from underfit_api.repositories import users as users_repo
from underfit_api.schema import artifacts, log_segments, media, projects, run_workers, runs, scalar_segments
from underfit_api.storage.file import FileStorage
from underfit_api.storage.s3 import S3Storage
from underfit_api.storage.types import Storage


@pytest.fixture(params=["file", "s3"], ids=["file", "s3"])
def storage(request: pytest.FixtureRequest, tmp_path: Path) -> Iterator[Storage]:
    if request.param == "file":
        yield FileStorage(FileStorageConfig(base=str(tmp_path / "storage")))
    else:
        with mock_aws():
            boto3.client("s3", region_name="us-east-1").create_bucket(Bucket="test-bucket")
            yield S3Storage(S3StorageConfig(bucket="test-bucket", prefix="pfx", region="us-east-1"))


def _sync(engine: Engine, storage: Storage) -> None:
    ctx = AppContext(engine=engine, storage=storage)
    with engine.begin() as conn:
        backfill.sync(ctx, conn)
        for row in conn.execute(select(runs.c.id)).all():
            backfill.refresh_run(ctx, conn, row.id)


def _write_json(storage: Storage, key: str, data: object) -> None:
    storage.write(key, json.dumps(data).encode())


def _write_text(storage: Storage, key: str, data: str) -> None:
    storage.write(key, data.encode())


def test_backfill_ingests_segment_files(storage: Storage, engine: Engine) -> None:
    run_id = uuid4()
    _write_json(storage, f"{run_id}/run.json", {
        "project": "Vision", "name": "Trial A", "config": {"lr": 0.01, "seed": 7},
        "metadata": {"summary": {"loss": 0.6}},
    })
    _write_text(storage, f"{run_id}/logs/worker-1/segments/0.log", "hello\nworld\n")
    _write_text(storage, f"{run_id}/scalars/0/r1/0.jsonl", (
        '{"step":1,"values":{"loss":0.8},"timestamp":"2025-01-01T00:00:00Z"}\n'
        '{"step":2,"values":{"loss":0.6},"timestamp":"2025-01-01T00:00:01Z"}\n'
    ))
    _sync(engine, storage)

    with engine.begin() as conn:
        project_row = conn.execute(select(projects)).first()
        run_row = conn.execute(select(runs).where(runs.c.id == run_id)).first()
        log_worker_row = conn.execute(select(run_workers).where(
            run_workers.c.run_id == run_id, run_workers.c.worker_label == "worker-1",
        )).first()
        scalar_worker_row = conn.execute(select(run_workers).where(
            run_workers.c.run_id == run_id, run_workers.c.worker_label == "0",
        )).first()
        assert log_worker_row is not None and scalar_worker_row is not None
        log_row = conn.execute(select(log_segments).where(log_segments.c.worker_id == log_worker_row.id)).first()
        scalar_row = conn.execute(select(scalar_segments).where(
            scalar_segments.c.worker_id == scalar_worker_row.id,
        )).first()

    assert project_row is not None and project_row.name == "vision"
    assert run_row is not None
    assert run_row.name == "trial a" and run_row.storage_key == str(run_id)
    assert run_row.terminal_state is None and run_row.config == {"lr": 0.01, "seed": 7}
    assert run_row.metadata == {"summary": {"loss": 0.6}}
    assert log_row is not None and (log_row.start_line, log_row.end_line) == (0, 2)
    assert scalar_row is not None and (scalar_row.resolution, scalar_row.start_line, scalar_row.end_line) == (1, 0, 2)
    assert scalar_row.end_at.isoformat() == "2025-01-01T00:00:01"
    assert run_row.summary == {}

    _write_json(storage, f"{run_id}/run.json", {
        "project": "Vision", "name": "Trial A", "summary": {"loss": 0.1, "best": 1.0},
    })
    _write_text(storage, f"{run_id}/scalars/0/r1/2.jsonl", (
        '{"step":3,"values":{"loss":0.4,"acc":0.9},"timestamp":"2025-01-01T00:00:02Z"}\n'
    ))
    _sync(engine, storage)
    with engine.begin() as conn:
        log_segment_count = conn.execute(select(log_segments)).all()
        rescanned_run = conn.execute(select(runs).where(runs.c.id == run_id)).first()
    assert len(log_segment_count) == 1
    assert rescanned_run is not None and rescanned_run.summary == {"loss": 0.1, "best": 1.0}


def test_backfill_stops_scalar_segment_at_invalid_json(storage: Storage, engine: Engine) -> None:
    run_id = uuid4()
    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial B", "terminal_state": "finished"})
    _write_text(storage, f"{run_id}/scalars/0/r1/0.jsonl", (
        '{"step":0,"values":{"loss":0.9},"timestamp":"2025-01-01T00:00:00Z"}\n'
        '{"step":1,"values":{"loss":0.8},"timestamp":"2025-01-01T00:00:01Z"}\n'
        "{bad-json}\n"
    ))
    _sync(engine, storage)

    with engine.begin() as conn:
        scalar_row = conn.execute(select(scalar_segments)).first()
    assert scalar_row is not None and (scalar_row.start_line, scalar_row.end_line, scalar_row.end_at.isoformat()) == (
        0, 2, "2025-01-01T00:00:01",
    )


def test_backfill_rejects_runs_attributed_to_org_handle(storage: Storage, engine: Engine) -> None:
    run_id = uuid4()
    with engine.begin() as conn:
        organizations_repo.create(conn, "core", "Core")

    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "user": "core", "name": "Trial Org"})
    _write_text(storage, f"{run_id}/logs/worker-1/segments/0.log", "hello\n")
    _sync(engine, storage)

    with engine.begin() as conn:
        assert conn.execute(select(runs).where(runs.c.id == run_id)).first() is None
        assert conn.execute(select(run_workers).where(run_workers.c.run_id == run_id)).first() is None
        assert conn.execute(select(log_segments)).first() is None
        assert conn.execute(select(projects)).first() is None


def test_backfill_updates_artifact_and_media_records(storage: Storage, engine: Engine) -> None:
    run_id = uuid4()
    artifact_id = uuid4()
    with engine.begin() as conn:
        user = users_repo.create(conn, "sam@example.com", "sam", "Sam")

    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial C"})
    _write_json(storage, f"{run_id}/artifacts/{artifact_id}/artifact.json", {"name": "base", "metadata": {"tag": "v1"}})
    _write_json(storage, f"{run_id}/artifacts/{artifact_id}/manifest.json", {"files": ["a.bin", "b.bin"]})
    _write_text(storage, f"{run_id}/artifacts/{artifact_id}/files/a.bin", "a")
    _write_text(storage, f"{run_id}/media/image/samples_7_0.png", "m0")
    _write_text(storage, f"{run_id}/media/image/samples_7_2.png", "m2")
    _write_text(storage, f"{run_id}/media/bad-type/samples_7_1.png", "m1")
    _sync(engine, storage)

    with engine.begin() as conn:
        artifact_row = conn.execute(select(artifacts).where(artifacts.c.id == artifact_id)).first()
        media_rows = conn.execute(select(media).order_by(media.c.index)).all()
    assert artifact_row is not None
    assert artifact_row.finalized_at is None
    assert [(r.index, r.storage_key) for r in media_rows] == [
        (0, "media/image/samples_7_0.png"), (2, "media/image/samples_7_2.png"),
    ]

    _write_json(storage, f"{run_id}/run.json", {"project": "NLP", "user": "Sam", "name": "Trial D"})
    _write_json(
        storage, f"{run_id}/artifacts/{artifact_id}/artifact.json", {"step": 3, "name": "best", "type": "model"},
    )
    _write_text(storage, f"{run_id}/artifacts/{artifact_id}/files/b.bin", "b")
    _write_text(storage, f"{run_id}/media/image/samples_7_1.png", "m1")
    _sync(engine, storage)

    with engine.begin() as conn:
        run_row = conn.execute(select(runs).where(runs.c.id == run_id)).first()
        artifact_row = conn.execute(select(artifacts).where(artifacts.c.id == artifact_id)).first()
        media_rows = conn.execute(select(media).order_by(media.c.index)).all()
        project_row = (
            conn.execute(select(projects).where(projects.c.id == run_row.project_id)).first() if run_row else None
        )
    assert run_row is not None and artifact_row is not None and project_row is not None
    assert run_row.user_id == user.id and project_row.name == "nlp" and run_row.name == "trial d"
    assert artifact_row.project_id == run_row.project_id
    assert (artifact_row.step, artifact_row.name, artifact_row.type) == (3, "best", "model")
    assert artifact_row.finalized_at is not None and artifact_row.stored_size_bytes == 2
    assert [r.index for r in media_rows] == [0, 1, 2]


def test_backfill_deletes_runs_only_when_directory_gone(storage: Storage, engine: Engine) -> None:
    run_id = uuid4()
    _write_json(storage, f"{run_id}/run.json", {"project": "Vision", "name": "Trial D"})
    _write_text(storage, f"{run_id}/logs/worker-1/segments/0.log", "hello\n")
    _sync(engine, storage)
    with engine.begin() as conn:
        assert conn.execute(select(runs).where(runs.c.id == run_id)).first() is not None

    _write_text(storage, f"{run_id}/run.json", "{bad-json}")
    _sync(engine, storage)
    with engine.begin() as conn:
        assert conn.execute(select(runs).where(runs.c.id == run_id)).first() is not None

    storage.delete(f"{run_id}/run.json")
    _sync(engine, storage)
    with engine.begin() as conn:
        assert conn.execute(select(runs).where(runs.c.id == run_id)).first() is not None

    for key in storage.list_files(str(run_id)):
        storage.delete(key)
    _sync(engine, storage)
    with engine.begin() as conn:
        assert conn.execute(select(runs).where(runs.c.id == run_id)).first() is None


def test_backfill_reads_ui_state_file(storage: Storage, engine: Engine) -> None:
    run_id = uuid4()
    _write_json(storage, f"{run_id}/run.json", {"project": "vision", "name": "run-a"})
    _write_json(storage, ".ui-state.json", {
        "runs": {str(run_id): {"uiState": {"layout": "grid"}, "isPinned": True}},
        "projects": {"local/vision": {"uiState": {"charts": "all"}, "baselineRunId": str(run_id)}},
    })
    _sync(engine, storage)

    with engine.begin() as conn:
        run_row = conn.execute(select(runs).where(runs.c.id == run_id)).first()
        project_row = conn.execute(select(projects).where(projects.c.name == "vision")).first()
    assert run_row is not None and run_row.ui_state == {"layout": "grid"} and run_row.is_pinned is True
    assert project_row is not None
    assert project_row.ui_state == {"charts": "all"} and project_row.baseline_run_id == run_id

    run_b = uuid4()
    _write_json(storage, f"{run_b}/run.json", {"project": "vision", "name": "run-b"})
    _write_json(storage, ".ui-state.json", {"projects": {"local/vision": {"baselineRunId": str(run_b)}}})
    _sync(engine, storage)
    with engine.begin() as conn:
        project_row = conn.execute(select(projects).where(projects.c.name == "vision")).first()
    assert project_row is not None and project_row.baseline_run_id == run_b
