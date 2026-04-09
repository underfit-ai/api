from __future__ import annotations

from datetime import timedelta

import pytest
from fastapi.testclient import TestClient

import underfit_api.db as db
from tests.conftest import AddCollaborator, CreateProject, Headers
from underfit_api.auth import verify_signed_token
from underfit_api.helpers import utcnow
from underfit_api.schema import run_workers

LAUNCH = "/api/v1/accounts/owner/projects/underfit/runs/launch"
RUNS = "/api/v1/accounts/owner/projects/underfit/runs"


def _launch(client: TestClient, headers: Headers, **overrides: object) -> dict[str, object]:
    body: dict[str, object] = {"runName": "my-run", "launchId": "abc-123", "workerLabel": "0"}
    body.update(overrides)
    return client.post(LAUNCH, headers=headers, json=body).json()


def test_launch_lifecycle(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")

    run = _launch(client, owner_headers, config={"lr": 0.001}, metadata={"summary": {"loss": 0.5}})
    assert run["isActive"] is True
    assert run["terminalState"] is None
    assert run["config"] == {"lr": 0.001}
    assert run["metadata"] == {"summary": {"loss": 0.5}}
    assert run["launchId"] == "abc-123"
    assert run["name"] == "my-run"
    assert run["workerToken"] is not None

    name = run["name"]
    assert isinstance(name, str)
    fetched = client.get(f"{RUNS}/{name.upper()}", headers=owner_headers)
    assert fetched.status_code == 200
    assert fetched.json()["id"] == run["id"]

    updated = client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={"metadata": {"summary": {"loss": 0.25}}})
    assert updated.status_code == 200
    assert updated.json()["config"] == {"lr": 0.001}
    assert updated.json()["metadata"] == {"summary": {"loss": 0.25}}

    preserved = client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={})
    assert preserved.status_code == 200
    assert preserved.json()["metadata"] == {"summary": {"loss": 0.25}}

    cleared = client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={"metadata": {}})
    assert cleared.status_code == 200
    assert cleared.json()["metadata"] == {}

    with db.engine.begin() as conn:
        conn.execute(run_workers.update().values(last_heartbeat=utcnow() - timedelta(seconds=16)))
    assert client.get(f"{RUNS}/{run['name']}", headers=owner_headers).json()["isActive"] is False

    assert len(client.get(RUNS, headers=owner_headers).json()) == 1
    assert client.get("/api/v1/users/owner/runs", headers=owner_headers).json()[0]["id"] == run["id"]


def test_launch_join(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")
    run = _launch(client, owner_headers)
    joined = client.post(LAUNCH, headers=owner_headers, json={
        "runName": "ignored", "launchId": "abc-123", "workerLabel": "1",
    })
    assert joined.status_code == 200
    assert joined.json()["id"] == run["id"]
    assert joined.json()["name"] == "my-run"
    assert joined.json()["workerToken"] is not None
    assert joined.json()["workerToken"] != run["workerToken"]

    workers = client.get(f"{RUNS}/{run['name']}/workers", headers=owner_headers).json()
    assert {w["workerLabel"] for w in workers} == {"0", "1"}


def test_launch_idempotent_retry(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")
    first = _launch(client, owner_headers)
    retry = _launch(client, owner_headers)
    first_token = verify_signed_token(str(first["workerToken"]))
    retry_token = verify_signed_token(str(retry["workerToken"]))
    assert first_token is not None and retry_token is not None
    assert first_token["worker_id"] == retry_token["worker_id"]


def test_launch_rejects_stale_run(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")
    _launch(client, owner_headers)
    with db.engine.begin() as conn:
        conn.execute(run_workers.update().values(last_heartbeat=utcnow() - timedelta(seconds=16)))
    body = {"runName": "new", "launchId": "abc-123", "workerLabel": "1"}
    resp = client.post(LAUNCH, headers=owner_headers, json=body)
    assert resp.status_code == 409


@pytest.mark.parametrize(("auth", "payload", "status"), [
    (False, {"runName": "r", "launchId": "1"}, 401),
    (True, {"runName": "r", "launchId": "1", "config": {"blob": "x" * 70000}}, 400),
])
def test_launch_validation(
    auth: bool, payload: dict[str, object], status: int,
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(handle="owner", name="underfit")
    headers = owner_headers if auth else None
    assert client.post(LAUNCH, headers=headers, json=payload).status_code == status


def test_launch_duplicate_name(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")
    _launch(client, owner_headers, runName="baseline", launchId="id-1")
    resp = client.post(LAUNCH, headers=owner_headers, json={"runName": "baseline", "launchId": "id-2"})
    assert resp.status_code == 409


def test_launch_access_controls(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    create_project: CreateProject, add_collaborator: AddCollaborator,
) -> None:
    create_project(handle="owner", name="underfit")
    body = {"runName": "r", "launchId": "1"}
    assert client.post(LAUNCH, headers=outsider_headers, json=body).status_code == 403
    add_collaborator(handle="owner", project_name="underfit", user_handle="outsider")
    assert client.post(LAUNCH, headers=outsider_headers, json=body).status_code == 200
