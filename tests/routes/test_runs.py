from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.conftest import AddCollaborator, CreateProject, CreateRun, Headers
from underfit_api.repositories import runs as runs_repo

RUNS = "/api/v1/accounts/owner/projects/underfit/runs"


def test_run_lifecycle(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")

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


@pytest.mark.parametrize(("auth", "payload", "status"), [
    (False, {"status": "running"}, 401),
    (True, {"status": "unknown"}, 400),
    (True, {"status": "running", "config": {"blob": "x" * 70000}}, 400),
])
def test_run_create_validation(
    auth: bool, payload: dict[str, object], status: int,
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(handle="owner", name="underfit")
    headers = owner_headers if auth else None
    assert client.post(RUNS, headers=headers, json=payload).status_code == status


def test_run_update_validation(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")
    created = client.post(RUNS, headers=owner_headers, json={"status": "running"})
    assert created.status_code == 200
    run = created.json()
    assert client.put(f"{RUNS}/{run['name']}", headers=owner_headers, json={"status": "unknown"}).status_code == 400


def test_duplicate_run_names(client: TestClient, owner_headers: Headers, create_run: CreateRun) -> None:
    run = create_run(handle="owner", project_name="underfit", user_handle="owner", name="BASELINE")
    assert run.name == "baseline"

    duplicate = client.post(RUNS, headers=owner_headers, json={"name": "BASELINE"})
    assert duplicate.status_code == 409


def test_run_create_name_generation(
    client: TestClient, owner_headers: Headers, create_project: CreateProject, monkeypatch: pytest.MonkeyPatch,
) -> None:
    create_project(handle="owner", name="underfit")
    monkeypatch.setattr(runs_repo, "_adjectives", ["brave"])
    monkeypatch.setattr(runs_repo, "_nouns", ["otter"])

    first = client.post(RUNS, headers=owner_headers, json={})
    second = client.post(RUNS, headers=owner_headers, json={})

    assert first.status_code == 200 and first.json()["name"] == "brave-otter"
    assert second.status_code == 200 and second.json()["name"] == "brave-otter-2"


def test_run_access_controls(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    create_project: CreateProject, add_collaborator: AddCollaborator,
) -> None:
    create_project(handle="owner", name="underfit")
    assert client.post(RUNS, headers=outsider_headers, json={"status": "running"}).status_code == 403
    run = client.post(RUNS, headers=owner_headers, json={"status": "running"}).json()
    assert client.put(f"{RUNS}/{run['name']}", headers=outsider_headers, json={"status": "finished"}).status_code == 403
    add_collaborator(handle="owner", project_name="underfit", user_handle="outsider")
    assert client.post(RUNS, headers=outsider_headers, json={"status": "running"}).status_code == 200

    allowed_update = client.put(f"{RUNS}/{run['name']}", headers=outsider_headers, json={"status": "finished"})
    assert allowed_update.status_code == 200
    assert allowed_update.json()["status"] == "finished"
