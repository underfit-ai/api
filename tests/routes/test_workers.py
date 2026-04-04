from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, CreateRun, Headers

RUNS = "/api/v1/accounts/owner/projects/underfit/runs"


def _workers_url(run: dict[str, object]) -> str:
    return f"{RUNS}/{run['name']}/workers"


def test_primary_worker_created_with_run(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(owner_headers)
    resp = client.get(_workers_url(run), headers=owner_headers)
    assert resp.status_code == 200
    workers = resp.json()
    assert len(workers) == 1
    assert workers[0]["workerLabel"] == "0"
    assert workers[0]["isPrimary"] is True
    assert workers[0]["status"] == "running"


def test_custom_primary_worker_id(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    create_run(owner_headers)
    run = client.post(RUNS, headers=owner_headers, json={"status": "running", "worker_label": "rank-0"}).json()
    workers = client.get(_workers_url(run), headers=owner_headers).json()
    assert len(workers) == 1
    assert workers[0]["workerLabel"] == "rank-0"
    assert workers[0]["isPrimary"] is True


def test_add_and_list_workers(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(owner_headers)
    url = _workers_url(run)
    resp = client.post(url, headers=owner_headers, json={"workerLabel": "1", "status": "running"})
    assert resp.status_code == 200
    assert resp.json()["workerLabel"] == "1"
    assert resp.json()["isPrimary"] is False

    resp = client.post(url, headers=owner_headers, json={"workerLabel": "2"})
    assert resp.status_code == 200
    assert resp.json()["status"] == "queued"

    workers = client.get(url, headers=owner_headers).json()
    assert len(workers) == 3
    assert {w["workerLabel"] for w in workers} == {"0", "1", "2"}


def test_duplicate_worker_rejected(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(owner_headers)
    url = _workers_url(run)
    client.post(url, headers=owner_headers, json={"workerLabel": "1"})
    assert client.post(url, headers=owner_headers, json={"workerLabel": "1"}).status_code == 409


def test_update_worker_status(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(owner_headers)
    url = _workers_url(run)
    client.post(url, headers=owner_headers, json={"workerLabel": "1", "status": "running"})
    resp = client.put(f"{url}/1", headers=owner_headers, json={"status": "finished"})
    assert resp.status_code == 200 and resp.json()["status"] == "finished"
    assert client.put(f"{url}/99", headers=owner_headers, json={"status": "finished"}).status_code == 404


def test_worker_access_controls(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    create_run: CreateRun, add_collaborator: AddCollaborator,
) -> None:
    run = create_run(owner_headers)
    url = _workers_url(run)
    assert client.post(url, headers=outsider_headers, json={"workerLabel": "1"}).status_code == 403
    add_collaborator(owner_headers)
    assert client.post(url, headers=outsider_headers, json={"workerLabel": "1"}).status_code == 200
