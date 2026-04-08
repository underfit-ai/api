from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import Headers

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

    full = client.get(scalars_url, headers=owner_headers)
    assert full.status_code == 200
    assert len(full.json()) == 20

    reduced = client.get(scalars_url, headers=owner_headers, params={"maxPoints": 2})
    assert reduced.status_code == 200
    assert len(reduced.json()) == 2


def test_scalars_validate_cursor_inputs(client: TestClient, owner_headers: Headers) -> None:
    headers, scalars_url = _setup_scalars(client, owner_headers)

    payload = {
        "start_line": 0,
        "scalars": [{"step": 1, "values": {"loss": 0.1}, "timestamp": "2025-01-01T00:00:00+00:00"}],
    }
    assert client.post("/api/v1/ingest/scalars", headers=headers, json=payload).status_code == 200
    duplicate = {
        "start_line": 0,
        "scalars": [{"step": 2, "values": {"loss": 0.2}, "timestamp": "2025-01-01T00:00:01+00:00"}],
    }
    assert client.post("/api/v1/ingest/scalars", headers=headers, json=duplicate).status_code == 409

    invalid_query = client.get(scalars_url, headers=owner_headers, params={"resolution": 1, "maxPoints": 10})
    assert invalid_query.status_code == 400
    missing_resolution = client.get(scalars_url, headers=owner_headers, params={"resolution": 10})
    assert missing_resolution.status_code == 404


def test_scalars_require_worker_token(client: TestClient) -> None:
    payload = {
        "start_line": 0,
        "scalars": [{"step": 1, "values": {"loss": 0.1}, "timestamp": "2025-01-01T00:00:00+00:00"}],
    }
    assert client.post("/api/v1/ingest/scalars", json=payload).status_code == 401
