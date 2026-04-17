from __future__ import annotations

from fastapi.testclient import TestClient
from sqlalchemy import Engine

from tests.conftest import Headers
from underfit_api import buffer
from underfit_api.storage.types import Storage


def test_write_and_read_scalars(client: TestClient, owner_headers: Headers, worker_headers: Headers) -> None:
    scalars_url = "/api/v1/accounts/owner/projects/underfit/runs/r/scalars"
    points = [
        {"step": i, "values": {"loss": round(1.0 - i * 0.01, 4)}, "timestamp": f"2025-01-01T00:00:{i:02d}+00:00"}
        for i in range(20)
    ]
    scalars = {"start_line": 0, "scalars": points}
    assert client.post("/api/v1/ingest/scalars", headers=worker_headers, json=scalars).json()["nextStartLine"] == 20
    stale = {"step": 5, "values": {"loss": 9.9}, "timestamp": "2025-01-01T00:01:00+00:00"}
    reject = client.post("/api/v1/ingest/scalars", headers=worker_headers, json={"start_line": 20, "scalars": [stale]})
    assert reject.status_code == 409 and reject.json()["lastStep"] == 19

    full = client.get(scalars_url, headers=owner_headers)
    assert full.status_code == 200
    assert full.json()["resolution"] == 1
    assert full.json()["pointCount"] == 20

    reduced = client.get(scalars_url, headers=owner_headers, params={"targetPoints": 5})
    assert reduced.status_code == 200
    assert reduced.json()["resolution"] == 10
    assert reduced.json()["pointCount"] == 2
    reduced_again = client.get(scalars_url, headers=owner_headers, params={"targetPoints": 1}).json()
    assert reduced_again["resolution"] == 100
    assert reduced_again["pointCount"] == 1


def test_scalar_ingest_validation(client: TestClient, owner_headers: Headers, worker_headers: Headers) -> None:
    scalars_url = "/api/v1/accounts/owner/projects/underfit/runs/r/scalars"
    payload = {
        "start_line": 0,
        "scalars": [{"step": 1, "values": {"loss": 0.1}, "timestamp": "2025-01-01T00:00:00+00:00"}],
    }

    assert client.post("/api/v1/ingest/scalars", json=payload).status_code == 401
    assert client.post("/api/v1/ingest/scalars", headers=worker_headers, json=payload).status_code == 200
    duplicate = {
        "start_line": 0,
        "scalars": [{"step": 2, "values": {"loss": 0.2}, "timestamp": "2025-01-01T00:00:01+00:00"}],
    }
    assert client.post("/api/v1/ingest/scalars", headers=worker_headers, json=duplicate).status_code == 409

    invalid_query = client.get(scalars_url, headers=owner_headers, params={"resolution": 1, "targetPoints": 10})
    assert invalid_query.status_code == 400


def test_read_scalars_from_storage(
    client: TestClient, owner_headers: Headers, worker_headers: Headers, engine: Engine, storage: Storage,
) -> None:
    scalars_url = "/api/v1/accounts/owner/projects/underfit/runs/r/scalars"
    points = [
        {"step": i, "values": {"loss": round(1.0 - i * 0.01, 4)}, "timestamp": f"2025-01-01T00:00:{i:02d}+00:00"}
        for i in range(20)
    ]
    assert client.post(
        "/api/v1/ingest/scalars", headers=worker_headers, json={"start_line": 0, "scalars": points},
    ).status_code == 200
    buffer.compact(engine, storage, include_partial=True)
    assert client.get(scalars_url, headers=owner_headers).json()["pointCount"] == 20
    reduced = client.get(scalars_url, headers=owner_headers, params={"resolution": 10}).json()
    assert reduced["resolution"] == 10
    assert reduced["pointCount"] == 2
    assert reduced["points"][-1]["step"] == 19
