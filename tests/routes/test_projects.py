from __future__ import annotations

from fastapi.testclient import TestClient

from tests.conftest import CreateProject, Headers

BASE = "/api/v1/accounts/owner/projects"


def test_project_crud_and_listing(client: TestClient, owner_headers: Headers) -> None:
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


def test_project_creation_validation(client: TestClient, owner_headers: Headers) -> None:
    cases = [
        ("/api/v1/accounts/missing/projects", {"name": "underfit", "visibility": "private"}, 404),
        (BASE, {"name": "underfit", "visibility": "internal"}, 400),
        (BASE, {"name": "underfit", "visibility": "private"}, 200),
        (BASE, {"name": "UNDERFIT", "visibility": "private"}, 409),
    ]
    for url, payload, status in cases:
        assert client.post(url, headers=owner_headers, json=payload).status_code == status


def test_project_admin_permissions(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(owner_headers)
    ops = [
        ("put", f"{BASE}/underfit", {"description": "hacked"}),
        ("post", f"{BASE}/underfit/rename", {"name": "hacked"}),
        ("post", BASE, {"name": "proj", "description": "x", "visibility": "private"}),
    ]
    for method, url, payload in ops:
        assert getattr(client, method)(url, headers=outsider_headers, json=payload).status_code == 403
    assert client.post(
        BASE,
        headers=owner_headers,
        json={"name": "proj", "description": "x", "visibility": "private"},
    ).status_code == 200


def test_rename_project(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(owner_headers)

    renamed = client.post(f"{BASE}/underfit/rename", headers=owner_headers, json={"name": "new-project"})
    assert renamed.status_code == 200
    assert renamed.json()["name"] == "new-project"

    fetched = client.get("/api/v1/accounts/owner/projects/new-project", headers=owner_headers)
    assert fetched.status_code == 200 and fetched.json()["name"] == "new-project"

    response = client.get(
        "/api/v1/accounts/owner/projects/underfit", headers=owner_headers, follow_redirects=False,
    )
    assert response.status_code == 307
    assert "/projects/new-project" in response.headers["location"]


def test_rename_project_conflicts(
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(owner_headers, name="project-a")
    create_project(owner_headers, name="project-b")
    conflict_existing = client.post(f"{BASE}/project-a/rename", headers=owner_headers, json={"name": "project-b"})
    assert conflict_existing.status_code == 409

    create_project(owner_headers, name="original")
    create_project(owner_headers, name="other")
    client.post(f"{BASE}/original/rename", headers=owner_headers, json={"name": "renamed"})
    conflict_alias = client.post(f"{BASE}/other/rename", headers=owner_headers, json={"name": "original"})
    assert conflict_alias.status_code == 409


def test_cannot_create_project_with_old_alias_name(
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(owner_headers, name="original")
    client.post(f"{BASE}/original/rename", headers=owner_headers, json={"name": "renamed"})

    conflict = client.post(BASE, headers=owner_headers, json={"name": "original", "visibility": "private"})
    assert conflict.status_code == 409
