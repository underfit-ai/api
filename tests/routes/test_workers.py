from __future__ import annotations

from uuid import UUID

import pytest
from fastapi.testclient import TestClient

from tests.conftest import Headers
from underfit_api.auth import get_app_secret
from underfit_api.config import config
from underfit_api.main import app


def test_heartbeat_and_terminal_state(client: TestClient, worker_headers: Headers) -> None:
    assert client.post("/api/v1/workers/heartbeat", headers=worker_headers).status_code == 200

    resp = client.put("/api/v1/runs/terminal-state", headers=worker_headers, json={"terminalState": "failed"})
    assert resp.status_code == 200 and resp.json()["terminalState"] == "failed"

    for token in ["bogus", str(UUID(int=0))]:
        bad = {"Authorization": f"Bearer {token}"}
        assert client.post("/api/v1/workers/heartbeat", headers=bad).status_code == 401
        terminal = client.put("/api/v1/runs/terminal-state", headers=bad, json={"terminalState": "finished"})
        assert terminal.status_code == 401
        assert client.put("/api/v1/runs/summary", headers=bad, json={"summary": {}}).status_code == 401


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


