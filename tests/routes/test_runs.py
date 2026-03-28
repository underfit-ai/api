from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, CreateProject, OutsiderHeaders, OwnerHeaders


def test_run_lifecycle_and_listing(
    client: TestClient, owner_headers: OwnerHeaders, create_project: CreateProject,
) -> None:
    create_project(owner_headers)

    created = client.post(
        "/api/v1/accounts/owner/projects/underfit/runs",
        headers=owner_headers,
        json={"status": "running", "config": {"lr": 0.001}},
    )
    assert created.status_code == 200
    run = created.json()
    assert run["status"] == "running"
    assert run["config"] == {"lr": 0.001}

    fetched = client.get(f"/api/v1/accounts/owner/projects/underfit/runs/{run['name'].upper()}", headers=owner_headers)
    assert fetched.status_code == 200
    assert fetched.json()["id"] == run["id"]

    updated = client.put(
        f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}",
        headers=owner_headers,
        json={"config": {"lr": 0.0005}},
    )
    assert updated.status_code == 200
    assert updated.json()["status"] == "running"
    assert updated.json()["config"] == {"lr": 0.0005}

    project_runs = client.get("/api/v1/accounts/owner/projects/underfit/runs", headers=owner_headers)
    assert project_runs.status_code == 200
    assert len(project_runs.json()) == 1

    user_runs = client.get("/api/v1/users/owner/runs", headers=owner_headers)
    assert user_runs.status_code == 200
    assert user_runs.json()[0]["id"] == run["id"]


def test_run_create_validation(
    client: TestClient, owner_headers: OwnerHeaders, create_project: CreateProject,
) -> None:
    create_project(owner_headers)

    unauthorized = client.post("/api/v1/accounts/owner/projects/underfit/runs", json={"status": "running"})
    assert unauthorized.status_code == 401

    bad_status = client.post(
        "/api/v1/accounts/owner/projects/underfit/runs",
        headers=owner_headers,
        json={"status": "unknown"},
    )
    assert bad_status.status_code == 400

    too_large = client.post(
        "/api/v1/accounts/owner/projects/underfit/runs",
        headers=owner_headers,
        json={"status": "running", "config": {"blob": "x" * 70000}},
    )
    assert too_large.status_code == 400


def test_run_update_rejects_invalid_status(
    client: TestClient, owner_headers: OwnerHeaders, create_project: CreateProject,
) -> None:
    create_project(owner_headers)
    created = client.post(
        "/api/v1/accounts/owner/projects/underfit/runs",
        headers=owner_headers,
        json={"status": "running"},
    )
    assert created.status_code == 200
    run = created.json()

    updated = client.put(
        f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}",
        headers=owner_headers,
        json={"status": "unknown"},
    )
    assert updated.status_code == 400


def test_run_update_requires_project_access(
    client: TestClient,
    owner_headers: OwnerHeaders,
    create_project: CreateProject,
    outsider_headers: OutsiderHeaders,
    add_collaborator: AddCollaborator,
) -> None:
    create_project(owner_headers)
    created = client.post(
        "/api/v1/accounts/owner/projects/underfit/runs",
        headers=owner_headers,
        json={"status": "running"},
    )
    assert created.status_code == 200
    run = created.json()
    forbidden = client.put(
        f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}",
        headers=outsider_headers,
        json={"status": "finished"},
    )
    assert forbidden.status_code == 403

    add_collaborator(owner_headers)

    allowed = client.put(
        f"/api/v1/accounts/owner/projects/underfit/runs/{run['name']}",
        headers=outsider_headers,
        json={"status": "finished"},
    )
    assert allowed.status_code == 200
    assert allowed.json()["status"] == "finished"


def test_run_create_requires_project_access(
    client: TestClient,
    create_project: CreateProject,
    owner_headers: OwnerHeaders,
    outsider_headers: OutsiderHeaders,
    add_collaborator: AddCollaborator,
) -> None:
    create_project(owner_headers)

    forbidden = client.post(
        "/api/v1/accounts/owner/projects/underfit/runs",
        headers=outsider_headers,
        json={"status": "running"},
    )
    assert forbidden.status_code == 403

    add_collaborator(owner_headers)

    allowed = client.post(
        "/api/v1/accounts/owner/projects/underfit/runs",
        headers=outsider_headers,
        json={"status": "running"},
    )
    assert allowed.status_code == 200
