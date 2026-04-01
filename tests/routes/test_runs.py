from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, CreateProject, Headers

RUNS = "/api/v1/accounts/owner/projects/underfit/runs"


def test_run_lifecycle_and_listing(
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(owner_headers)

    created = client.post(RUNS, headers=owner_headers, json={"status": "running", "config": {"lr": 0.001}})
    assert created.status_code == 200
    run = created.json()
    assert run["status"] == "running"
    assert run["config"] == {"lr": 0.001}

    fetched = client.get(f"{RUNS}/{run['name'].upper()}", headers=owner_headers)
    assert fetched.status_code == 200
    assert fetched.json()["id"] == run["id"]

    updated = client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={"config": {"lr": 0.0005}})
    assert updated.status_code == 200
    assert updated.json()["status"] == "running"
    assert updated.json()["config"] == {"lr": 0.0005}

    project_runs = client.get(RUNS, headers=owner_headers)
    assert project_runs.status_code == 200
    assert len(project_runs.json()) == 1

    user_runs = client.get("/api/v1/users/owner/runs", headers=owner_headers)
    assert user_runs.status_code == 200
    assert user_runs.json()[0]["id"] == run["id"]


def test_run_create_validation(
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(owner_headers)

    cases = [
        ({}, {"status": "running"}, 401),
        (owner_headers, {"status": "unknown"}, 400),
        (owner_headers, {"status": "running", "config": {"blob": "x" * 70000}}, 400),
    ]
    for headers, payload, status in cases:
        resp = client.post(RUNS, headers=headers or None, json=payload)
        assert resp.status_code == status

    created = client.post(RUNS, headers=owner_headers, json={"status": "running"})
    assert created.status_code == 200
    run = created.json()

    updated = client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={"status": "unknown"})
    assert updated.status_code == 400


def test_run_access_controls(
    client: TestClient,
    create_project: CreateProject,
    owner_headers: Headers,
    outsider_headers: Headers,
    add_collaborator: AddCollaborator,
) -> None:
    create_project(owner_headers)
    forbidden_create = client.post(RUNS, headers=outsider_headers, json={"status": "running"})
    assert forbidden_create.status_code == 403
    run = client.post(RUNS, headers=owner_headers, json={"status": "running"}).json()
    forbidden_update = client.put(f"{RUNS}/{run['name']}", headers=outsider_headers, json={"status": "finished"})
    assert forbidden_update.status_code == 403

    add_collaborator(owner_headers)

    allowed_create = client.post(RUNS, headers=outsider_headers, json={"status": "running"})
    assert allowed_create.status_code == 200
    allowed_update = client.put(f"{RUNS}/{run['name']}", headers=outsider_headers, json={"status": "finished"})
    assert allowed_update.status_code == 200
    assert allowed_update.json()["status"] == "finished"
