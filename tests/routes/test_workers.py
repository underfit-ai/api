from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateProject, Headers

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
