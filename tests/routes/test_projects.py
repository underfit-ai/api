from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from tests.conftest import CreateProject, Headers

BASE = "/api/v1/accounts/owner/projects"


def test_project_lifecycle(client: TestClient, owner_headers: Headers) -> None:
    create_payload = {"name": "Underfit", "description": "Tracking", "visibility": "private"}
    created = client.post(BASE, headers=owner_headers, json=create_payload)
    assert created.status_code == 200
    assert created.json()["name"] == "underfit"

    fetched = client.get(f"{BASE}/UNDERFIT", headers=owner_headers)
    assert fetched.status_code == 200 and fetched.json()["description"] == "Tracking"

    listed = client.get(BASE, headers=owner_headers)
    assert listed.status_code == 200 and [p["name"] for p in listed.json()] == ["underfit"]

    update_payload = {"description": "Updated", "visibility": "public"}
    updated = client.put(f"{BASE}/underfit", headers=owner_headers, json=update_payload)
    assert updated.status_code == 200
    assert (updated.json()["description"], updated.json()["visibility"]) == ("Updated", "public")


@pytest.mark.parametrize(("url", "payload", "status", "existing"), [
    ("/api/v1/accounts/missing/projects", {"name": "underfit", "visibility": "private"}, 404, False),
    (BASE, {"name": "underfit", "visibility": "internal"}, 400, False),
    (BASE, {"name": "underfit", "visibility": "private"}, 200, False),
    (BASE, {"name": "UNDERFIT", "visibility": "private"}, 409, True),
])
def test_project_creation_validation(
    url: str,
    payload: dict[str, str],
    status: int,
    existing: bool,
    client: TestClient,
    owner_headers: Headers,
    create_project: CreateProject,
) -> None:
    if existing:
        create_project(owner_headers)
    assert client.post(url, headers=owner_headers, json=payload).status_code == status


@pytest.mark.parametrize(("method", "url", "payload"), [
    ("put", f"{BASE}/underfit", {"description": "hacked"}),
    ("post", f"{BASE}/underfit/rename", {"name": "hacked"}),
    ("post", BASE, {"name": "proj", "description": "x", "visibility": "private"}),
])
def test_project_admin_permissions(
    method: str,
    url: str,
    payload: dict[str, str],
    client: TestClient,
    owner_headers: Headers,
    outsider_headers: Headers,
    create_project: CreateProject,
) -> None:
    create_project(owner_headers)
    assert getattr(client, method)(url, headers=outsider_headers, json=payload).status_code == 403

def test_rename_project(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(owner_headers)

    renamed = client.post(f"{BASE}/underfit/rename", headers=owner_headers, json={"name": "new-project"})
    assert renamed.status_code == 200
    assert renamed.json()["name"] == "new-project"

    fetched = client.get("/api/v1/accounts/owner/projects/new-project", headers=owner_headers)
    assert fetched.status_code == 200 and fetched.json()["name"] == "new-project"

    response = client.get("/api/v1/accounts/owner/projects/underfit", headers=owner_headers, follow_redirects=False)
    assert response.status_code == 307
    assert "/projects/new-project" in response.headers["location"]


def test_rename_project_conflicts(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(owner_headers, name="project1")
    create_project(owner_headers, name="project2")
    assert client.post(f"{BASE}/project1/rename", headers=owner_headers, json={"name": "project2"}).status_code == 409

    create_project(owner_headers, name="original")
    create_project(owner_headers, name="other")
    client.post(f"{BASE}/original/rename", headers=owner_headers, json={"name": "renamed"})
    assert client.post(f"{BASE}/other/rename", headers=owner_headers, json={"name": "original"}).status_code == 409
    assert client.post(BASE, headers=owner_headers, json={"name": "original"}).status_code == 409
