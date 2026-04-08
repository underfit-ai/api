from __future__ import annotations

from typing import cast

import pytest
from fastapi.testclient import TestClient
from httpx import Response

from tests.conftest import CreateOrg, CreateOrgMember, CreateProject, CreateUser, Headers

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


def test_list_projects(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    create_project: CreateProject, create_org: CreateOrg, create_org_member: CreateOrgMember,
) -> None:
    create_project(handle="owner", name="private")
    create_project(handle="owner", name="shared")
    create_project(handle="owner", name="public", visibility="public")
    create_project(handle="outsider", name="outsider-private")
    added = client.put("/api/v1/accounts/owner/projects/shared/collaborators/outsider", headers=owner_headers)
    assert added.status_code == 200

    def _project_names(projects: Response) -> set[str]:
        return {cast(str, project["name"]) for project in projects.json()}

    owner_projects = client.get(BASE, headers=owner_headers)
    assert owner_projects.status_code == 200 and _project_names(owner_projects) == {"private", "public", "shared"}
    outsider_projects = client.get(BASE, headers=outsider_headers)
    assert outsider_projects.status_code == 200 and _project_names(outsider_projects) == {"public", "shared"}
    public_projects = client.get(BASE)
    assert public_projects.status_code == 200 and _project_names(public_projects) == {"public"}
    my_projects = client.get("/api/v1/me/projects", headers=outsider_headers)
    assert my_projects.status_code == 200 and _project_names(my_projects) == {"outsider-private", "shared"}

    org = create_org(owner_headers)
    admin_headers = create_org_member(org["id"], "admin@example.com", "admin", "Admin", role="ADMIN")
    create_project(handle="core", name="secret")
    org_projects = client.get("/api/v1/accounts/core/projects", headers=admin_headers)
    assert org_projects.status_code == 200 and _project_names(org_projects) == {"secret"}
    admin_projects = client.get("/api/v1/me/projects", headers=admin_headers)
    assert admin_projects.status_code == 200 and _project_names(admin_projects) == {"secret"}


def test_get_project_access_controls(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers, create_project: CreateProject,
) -> None:
    create_project(handle="owner", name="private")
    create_project(handle="owner", name="shared")
    create_project(handle="owner", name="public", visibility="public")

    assert client.get(f"{BASE}/public").status_code == 200
    assert client.get(f"{BASE}/private").status_code == 401
    assert client.get(f"{BASE}/private", headers=outsider_headers).status_code == 403
    assert client.get(f"{BASE}/shared", headers=outsider_headers).status_code == 403

    added = client.put("/api/v1/accounts/owner/projects/shared/collaborators/outsider", headers=owner_headers)
    assert added.status_code == 200
    assert client.get(f"{BASE}/shared", headers=outsider_headers).status_code == 200


def test_org_admin_can_access_project(
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
    create_org: CreateOrg, create_org_member: CreateOrgMember,
) -> None:
    org = create_org(owner_headers)
    admin_headers = create_org_member(org["id"], "admin@example.com", "admin", "Admin", role="ADMIN")
    member_headers = create_org_member(org["id"], "member@example.com", "member", "Member")
    create_project(handle="core", name="secret")
    launch_url = "/api/v1/accounts/core/projects/secret/runs/launch"
    launch_payload = {"runName": "r", "launchId": "1"}

    assert client.get("/api/v1/accounts/core/projects/secret", headers=member_headers).status_code == 403
    assert client.get("/api/v1/accounts/core/projects/secret", headers=admin_headers).status_code == 200
    assert client.post(launch_url, headers=member_headers, json=launch_payload).status_code == 403
    assert client.post(launch_url, headers=admin_headers, json=launch_payload).status_code == 200


@pytest.mark.parametrize(("url", "payload", "status", "existing"), [
    ("/api/v1/accounts/missing/projects", {"name": "underfit", "visibility": "private"}, 404, False),
    (BASE, {"name": "underfit", "visibility": "internal"}, 400, False),
    (BASE, {"name": "underfit", "visibility": "private"}, 200, False),
    (BASE, {"name": "UNDERFIT", "visibility": "private"}, 409, True),
])
def test_project_creation_validation(
    url: str, payload: dict[str, str], status: int, existing: bool,
    client: TestClient, owner_headers: Headers, create_project: CreateProject,
) -> None:
    if existing:
        create_project(handle="owner", name="underfit")
    assert client.post(url, headers=owner_headers, json=payload).status_code == status


@pytest.mark.parametrize(("method", "url", "payload"), [
    ("put", f"{BASE}/underfit", {"description": "hacked"}),
    ("post", f"{BASE}/underfit/rename", {"name": "hacked"}),
    ("post", BASE, {"name": "proj", "description": "x", "visibility": "private"}),
])
def test_project_admin_permissions(
    method: str, url: str, payload: dict[str, str],
    client: TestClient, outsider_headers: Headers, create_project: CreateProject, create_user: CreateUser,
) -> None:
    create_user(email="owner@example.com", handle="owner", name="Owner")
    create_project(handle="owner", name="underfit")
    assert getattr(client, method)(url, headers=outsider_headers, json=payload).status_code == 403


def test_rename_project(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="underfit")

    renamed = client.post(f"{BASE}/underfit/rename", headers=owner_headers, json={"name": "new-project"})
    assert renamed.status_code == 200
    assert renamed.json()["name"] == "new-project"

    fetched = client.get("/api/v1/accounts/owner/projects/new-project", headers=owner_headers)
    assert fetched.status_code == 200 and fetched.json()["name"] == "new-project"

    response = client.get("/api/v1/accounts/owner/projects/underfit", headers=owner_headers, follow_redirects=False)
    assert response.status_code == 307
    assert "/projects/new-project" in response.headers["location"]


def test_rename_project_conflicts(client: TestClient, owner_headers: Headers, create_project: CreateProject) -> None:
    create_project(handle="owner", name="project1")
    create_project(handle="owner", name="project2")
    assert client.post(f"{BASE}/project1/rename", headers=owner_headers, json={"name": "project2"}).status_code == 409

    create_project(handle="owner", name="original")
    create_project(handle="owner", name="other")
    client.post(f"{BASE}/original/rename", headers=owner_headers, json={"name": "renamed"})
    assert client.post(f"{BASE}/other/rename", headers=owner_headers, json={"name": "original"}).status_code == 409
    assert client.post(BASE, headers=owner_headers, json={"name": "original"}).status_code == 409
