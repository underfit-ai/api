from __future__ import annotations

from fastapi.testclient import TestClient

import underfit_api.db as db
import underfit_api.storage as storage_mod
from tests.conftest import Headers
from underfit_api.buffer import scalar_buffer

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"


def _setup_scalars(client: TestClient, headers: Headers) -> tuple[Headers, str]:
    client.post("/api/v1/accounts/owner/projects", headers=headers, json={"name": "underfit", "visibility": "private"})
    run = client.post(LAUNCH, headers=headers, json={"runName": "r", "launchId": "1"}).json()
    return {"Authorization": f"Bearer {run['workerToken']}"}, "/api/v1/accounts/owner/projects/underfit/runs/r/scalars"


def test_write_and_read_scalars_with_auto_resolution(client: TestClient, owner_headers: Headers) -> None:
    headers, scalars_url = _setup_scalars(client, owner_headers)
    points = [
        {"step": i, "values": {"loss": round(1.0 - i * 0.01, 4)}, "timestamp": f"2025-01-01T00:00:{i:02d}+00:00"}
        for i in range(20)
    ]
    scalars = {"start_line": 0, "scalars": points}
    assert client.post("/api/v1/ingest/scalars", headers=headers, json=scalars).json()["nextStartLine"] == 20
    stale = {"step": 5, "values": {"loss": 9.9}, "timestamp": "2025-01-01T00:01:00+00:00"}
    reject = client.post("/api/v1/ingest/scalars", headers=headers, json={"start_line": 20, "scalars": [stale]})
    assert reject.status_code == 409 and reject.json()["lastStep"] == 19

    full = client.get(scalars_url, headers=owner_headers)
    assert full.status_code == 200
    assert full.json()["resolution"] == 1
    assert full.json()["pointCount"] == 20
    assert len(full.json()["points"]) == 20

    reduced = client.get(scalars_url, headers=owner_headers, params={"targetPoints": 5})
    assert reduced.status_code == 200
    assert reduced.json()["resolution"] == 10
    assert reduced.json()["pointCount"] == 2
    assert len(reduced.json()["points"]) == 2
    reduced_again = client.get(scalars_url, headers=owner_headers, params={"targetPoints": 1}).json()
    assert reduced_again["resolution"] == 10
    assert len(reduced_again["points"]) == 2


def test_scalar_ingest_and_query_validation(client: TestClient, owner_headers: Headers) -> None:
    headers, scalars_url = _setup_scalars(client, owner_headers)
    payload = {
        "start_line": 0,
        "scalars": [{"step": 1, "values": {"loss": 0.1}, "timestamp": "2025-01-01T00:00:00+00:00"}],
    }

    assert client.post("/api/v1/ingest/scalars", json=payload).status_code == 401
    assert client.post("/api/v1/ingest/scalars", headers=headers, json=payload).status_code == 200
    duplicate = {
        "start_line": 0,
        "scalars": [{"step": 2, "values": {"loss": 0.2}, "timestamp": "2025-01-01T00:00:01+00:00"}],
    }
    assert client.post("/api/v1/ingest/scalars", headers=headers, json=duplicate).status_code == 409

    invalid_query = client.get(scalars_url, headers=owner_headers, params={"resolution": 1, "targetPoints": 10})
    assert invalid_query.status_code == 400
    missing_resolution = client.get(scalars_url, headers=owner_headers, params={"resolution": 10})
    assert missing_resolution.status_code == 404


def test_read_scalars_from_persisted_segments(client: TestClient, owner_headers: Headers) -> None:
    headers, scalars_url = _setup_scalars(client, owner_headers)
    points = [
        {"step": i, "values": {"loss": round(1.0 - i * 0.01, 4)}, "timestamp": f"2025-01-01T00:00:{i:02d}+00:00"}
        for i in range(20)
    ]
    assert client.post(
        "/api/v1/ingest/scalars", headers=headers, json={"start_line": 0, "scalars": points},
    ).status_code == 200
    with db.engine.begin() as conn:
        scalar_buffer.flush_all(conn, storage_mod.storage)
    assert client.get(scalars_url, headers=owner_headers).json()["pointCount"] == 20
    reduced = client.get(scalars_url, headers=owner_headers, params={"targetPoints": 5}).json()
    assert reduced["resolution"] == 10
    assert 0 < reduced["pointCount"] < 20
    assert reduced["points"][-1]["step"] == 19
