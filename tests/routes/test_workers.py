from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, CreateRun, CreateUser, Headers
from underfit_api.auth import verify_signed_token
from underfit_api.models import Run

RUNS = "/api/v1/accounts/owner/projects/underfit/runs"


def _workers_url(run: Run) -> str:
    return f"{RUNS}/{run.name}/workers"


def test_primary_worker_created_with_run(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    resp = client.get(_workers_url(run), headers=owner_headers)
    assert resp.status_code == 200
    workers = resp.json()
    assert len(workers) == 1
    assert workers[0]["workerLabel"] == "0"
    assert workers[0]["isPrimary"] is True
    assert workers[0]["status"] == "running"


def test_custom_primary_worker_id(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    create_run(handle="owner", project_name="underfit", user_handle="owner")
    run = client.post(RUNS, headers=owner_headers, json={"status": "running", "worker_label": "rank-0"}).json()
    workers = client.get(f"{RUNS}/{run['name']}/workers", headers=owner_headers).json()
    token = verify_signed_token(run["workerToken"])
    assert token is not None and token["worker_id"] == workers[0]["id"]
    assert len(workers) == 1
    assert workers[0]["workerLabel"] == "rank-0"
    assert workers[0]["isPrimary"] is True


def test_add_and_list_workers(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    url = _workers_url(run)
    resp = client.post(url, headers=owner_headers, json={"workerLabel": "1", "status": "running"})
    assert resp.status_code == 200
    token = verify_signed_token(resp.json()["workerToken"])
    assert token is not None and token["worker_id"] == resp.json()["id"]
    assert resp.json()["workerLabel"] == "1"
    assert resp.json()["isPrimary"] is False

    resp = client.post(url, headers=owner_headers, json={"workerLabel": "2"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"

    workers = client.get(url, headers=owner_headers).json()
    assert len(workers) == 3
    assert {w["workerLabel"] for w in workers} == {"0", "1", "2"}


def test_duplicate_worker_rejected(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    client.post(_workers_url(run), headers=owner_headers, json={"workerLabel": "1"})
    assert client.post(_workers_url(run), headers=owner_headers, json={"workerLabel": "1"}).status_code == 409


def test_update_worker_status(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    client.post(_workers_url(run), headers=owner_headers, json={"workerLabel": "1", "status": "running"})
    resp = client.put(f"{_workers_url(run)}/1", headers=owner_headers, json={"status": "finished"})
    assert resp.status_code == 200 and resp.json()["status"] == "finished"
    assert client.put(f"{_workers_url(run)}/2", headers=owner_headers, json={"status": "finished"}).status_code == 404


def test_worker_access_controls(
    client: TestClient, outsider_headers: Headers, create_user: CreateUser,
    create_run: CreateRun, add_collaborator: AddCollaborator,
) -> None:
    create_user(email="owner@example.com", handle="owner", name="Owner")
    run = create_run(handle="owner", project_name="underfit", user_handle="owner")
    assert client.post(_workers_url(run), headers=outsider_headers, json={"workerLabel": "1"}).status_code == 403
    add_collaborator(handle="owner", project_name="underfit", user_handle="outsider")
    assert client.post(_workers_url(run), headers=outsider_headers, json={"workerLabel": "1"}).status_code == 200
