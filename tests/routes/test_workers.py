from __future__ import annotations

from datetime import timedelta
from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from tests.conftest import CreateProject, Headers
from underfit_api.auth import create_signed_token, get_app_secret
from underfit_api.config import config
from underfit_api.main import app

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"


def test_heartbeat_and_terminal_state(
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(handle="owner", name="underfit")
    run = client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1"}).json()
    worker_headers = {"Authorization": f"Bearer {run['workerToken']}"}

    assert client.post("/api/v1/workers/heartbeat", headers=worker_headers).status_code == 200

    resp = client.put("/api/v1/runs/terminal-state", headers=worker_headers, json={"terminalState": "failed"})
    assert resp.status_code == 200 and resp.json()["terminalState"] == "failed"


def test_any_worker_can_set_terminal_state(
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(handle="owner", name="underfit")
    client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1", "workerLabel": "0"})
    second = client.post(LAUNCH, headers=owner_headers, json={"runName": "r", "launchId": "1", "workerLabel": "1"})
    worker_headers = {"Authorization": f"Bearer {second.json()['workerToken']}"}

    resp = client.put("/api/v1/runs/terminal-state", headers=worker_headers, json={"terminalState": "finished"})
    assert resp.status_code == 200 and resp.json()["terminalState"] == "finished"


def test_worker_auth_without_app_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("UNDERFIT_APP_SECRET", raising=False)
    get_app_secret.cache_clear()
    config.auth_enabled = False
    with TestClient(app) as client:
        project = client.post("/api/v1/accounts/local/projects", json={"name": "underfit", "visibility": "private"})
        assert project.status_code == 200
        run = client.post(
            "/api/v1/accounts/local/projects/underfit/runs/launch", json={"runName": "r", "launchId": "1"},
        ).json()
        UUID(run["workerToken"])
        worker_headers = {"Authorization": f"Bearer {run['workerToken']}"}
        assert client.post("/api/v1/workers/heartbeat", headers=worker_headers).status_code == 200
        assert client.put(
            "/api/v1/runs/terminal-state", headers=worker_headers, json={"terminalState": "finished"},
        ).status_code == 200


@pytest.mark.parametrize(("method", "url", "body"), [
    ("post", "/api/v1/workers/heartbeat", None),
    ("put", "/api/v1/runs/terminal-state", {"terminalState": "finished"}),
    ("put", "/api/v1/runs/summary", {"summary": {}}),
])
def test_worker_endpoints_reject_invalid_tokens(client: TestClient, method: str, url: str, body: object) -> None:
    unknown = create_signed_token({"worker_id": str(UUID(int=0))}, timedelta(minutes=1))
    for token in ["bogus", unknown]:
        response = getattr(client, method)(url, headers={"Authorization": f"Bearer {token}"}, json=body)
        assert response.status_code == 401
