from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.conftest import Headers
from underfit_api.buffers import logs as log_buffer
from underfit_api.schema import run_workers
from underfit_api.storage import Storage

INGEST = "/api/v1/ingest/logs"
LOGS = "/api/v1/accounts/owner/projects/underfit/runs/r/logs/0"


def test_log_ingest_and_read(
    client: TestClient, owner_headers: Headers, worker_headers: Headers, engine: Engine, storage: Storage,
) -> None:
    lines = [
        {"timestamp": "2025-01-01T00:00:00+00:00", "content": "hello"},
        {"timestamp": "2025-01-01T00:00:01+00:00", "content": "world"},
    ]
    tail = {"start_line": 2, "lines": [{"timestamp": "2025-01-01T00:00:02+00:00", "content": "tail"}]}
    newline_payload = {"start_line": 0, "lines": [{"timestamp": "2025-01-01T00:00:00+00:00", "content": "a\nb"}]}

    payload = {"start_line": 0, "lines": lines}
    assert client.post(INGEST, json=payload).status_code == 401
    assert client.post(INGEST, headers=worker_headers, json=newline_payload).status_code == 400
    assert client.post(INGEST, headers=worker_headers, json=payload).json()["nextStartLine"] == 2
    assert client.post(INGEST, headers=worker_headers, json=tail).json()["nextStartLine"] == 3
    assert client.post(INGEST, headers=worker_headers, json=payload).status_code == 409

    first = client.get(LOGS, headers=owner_headers, params={"count": 2}).json()
    assert first["entries"][0]["content"] == "hello\nworld"
    assert (first["nextCursor"], first["hasMore"]) == (2, True)
    assert client.get(LOGS, headers=owner_headers, params={"cursor": 2}).json()["entries"][0]["content"] == "tail"

    with engine.begin() as conn:
        conn.execute(run_workers.update().values(last_heartbeat=datetime(2020, 1, 1, tzinfo=timezone.utc)))
    log_buffer.compact(engine, storage)
    page = client.get(LOGS, headers=owner_headers, params={"cursor": 1, "count": 1}).json()
    assert page["entries"][0]["content"] == "world"
    assert (page["nextCursor"], page["hasMore"]) == (2, True)
