from __future__ import annotations

from datetime import datetime, timezone

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.conftest import Headers
from underfit_api.buffers import scalars as scalar_buffer
from underfit_api.schema import run_workers
from underfit_api.storage import Storage

INGEST = "/api/v1/ingest/scalars"
SCALARS = "/api/v1/accounts/owner/projects/underfit/runs/r/scalars"


def test_scalar_ingest_and_read(
    client: TestClient, owner_headers: Headers, worker_headers: Headers, engine: Engine, storage: Storage,
) -> None:
    points = [
        {"step": i, "values": {"loss": round(1.0 - i * 0.01, 4)}, "timestamp": f"2025-01-01T00:00:{i:02d}+00:00"}
        for i in range(20)
    ]
    payload = {"start_line": 0, "scalars": points}
    assert client.post(INGEST, json=payload).status_code == 401
    assert client.post(INGEST, headers=worker_headers, json=payload).json()["nextStartLine"] == 20
    duplicate = client.post(INGEST, headers=worker_headers, json=payload)
    assert duplicate.status_code == 409
    assert duplicate.json() == {"error": "Invalid startLine", "expectedStartLine": 20}

    stale = {"step": 5, "values": {"loss": 9.9}, "timestamp": "2025-01-01T00:01:00+00:00"}
    reject = client.post(INGEST, headers=worker_headers, json={"start_line": 20, "scalars": [stale]})
    assert reject.status_code == 409
    assert reject.json() == {"error": "Step must be strictly increasing", "lastStep": 19}

    assert client.get(SCALARS, headers=owner_headers, params={"resolution": 1, "targetPoints": 10}).status_code == 400

    full = client.get(SCALARS, headers=owner_headers).json()
    assert full["resolution"] == 1
    assert full["series"]["loss"]["axis"] == 0
    assert len(full["axes"][0]["steps"]) == 20
    assert len(full["series"]["loss"]["values"]) == 20
    reduced = client.get(SCALARS, headers=owner_headers, params={"targetPoints": 5}).json()
    assert reduced["resolution"] == 10
    assert len(reduced["axes"][0]["steps"]) == 2
    assert client.get(SCALARS, headers=owner_headers, params={"targetPoints": 1}).json()["resolution"] == 100

    with engine.begin() as conn:
        conn.execute(run_workers.update().values(last_heartbeat=datetime(2020, 1, 1, tzinfo=timezone.utc)))
    scalar_buffer.compact(engine, storage)
    assert len(client.get(SCALARS, headers=owner_headers).json()["axes"][0]["steps"]) == 20
    from_storage = client.get(SCALARS, headers=owner_headers, params={"resolution": 10}).json()
    assert from_storage["resolution"] == 10
    assert from_storage["axes"][0]["steps"][-1] == 19
