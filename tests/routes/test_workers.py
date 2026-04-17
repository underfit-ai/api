from __future__ import annotations

from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from tests.conftest import Headers
from underfit_api.auth import get_app_secret
from underfit_api.config import config
from underfit_api.main import app

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"


def test_heartbeat_and_terminal_state(client: TestClient, worker_headers: Headers) -> None:
    assert client.post("/api/v1/workers/heartbeat", headers=worker_headers).status_code == 200

    resp = client.put("/api/v1/runs/terminal-state", headers=worker_headers, json={"terminalState": "failed"})
    assert resp.status_code == 200 and resp.json()["terminalState"] == "failed"


def test_worker_auth_without_app_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("UNDERFIT_APP_SECRET", raising=False)
    get_app_secret.cache_clear()
    config.auth_enabled = False
    with TestClient(app) as client:
        projects_url = "/api/v1/accounts/local/projects"
        assert client.post(projects_url, json={"name": "underfit", "visibility": "private"}).status_code == 200
        run = client.post(f"{projects_url}/underfit/runs/launch", json={"runName": "r", "launchId": "1"}).json()
        UUID(run["workerToken"])
        worker_headers = {"Authorization": f"Bearer {run['workerToken']}"}
        payload = {"terminalState": "finished"}
        assert client.post("/api/v1/workers/heartbeat", headers=worker_headers).status_code == 200
        assert client.put("/api/v1/runs/terminal-state", headers=worker_headers, json=payload).status_code == 200


@pytest.mark.parametrize(("method", "url", "body"), [
    ("post", "/api/v1/workers/heartbeat", None),
    ("put", "/api/v1/runs/terminal-state", {"terminalState": "finished"}),
    ("put", "/api/v1/runs/summary", {"summary": {}}),
])
def test_worker_endpoints_reject_invalid_tokens(client: TestClient, method: str, url: str, body: object) -> None:
    for token in ["bogus", str(UUID(int=0))]:
        response = getattr(client, method)(url, headers={"Authorization": f"Bearer {token}"}, json=body)
        assert response.status_code == 401
