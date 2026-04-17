from __future__ import annotations

import json
from typing import cast

import pytest
from fastapi.testclient import TestClient
from httpx import Response
from sqlalchemy import Engine

import underfit_api.routes.projects as projects_route
from tests.conftest import CreateOrg, CreateOrgMember, CreateProject, CreateUser, Headers
from underfit_api.config import config
from underfit_api.repositories import projects as projects_repo
from underfit_api.storage.types import Storage

BASE = "/api/v1/accounts/owner/projects"


def test_project_lifecycle(client: TestClient, owner_headers: Headers) -> None:
    create_payload = {
        "name": "Underfit",
        "description": "Tracking",
        "metadata": {"charts": {"default": "loss"}},
        "visibility": "private",
    }
    created = client.post(BASE, headers=owner_headers, json=create_payload)
    assert created.status_code == 200
    assert created.json()["name"] == "underfit"
    assert created.json()["metadata"] == {"charts": {"default": "loss"}}
    assert created.json()["uiState"] == {} and created.json()["baselineRunId"] is None

    fetched = client.get(f"{BASE}/UNDERFIT", headers=owner_headers)
    assert fetched.status_code == 200 and fetched.json()["description"] == "Tracking"

    listed = client.get(BASE, headers=owner_headers)
    assert listed.status_code == 200 and [p["name"] for p in listed.json()] == ["underfit"]

    update_payload = {"description": "Updated", "metadata": {"charts": {"default": "accuracy"}}, "visibility": "public"}
    updated = client.put(f"{BASE}/underfit", headers=owner_headers, json=update_payload)
    assert updated.status_code == 200
    assert (updated.json()["description"], updated.json()["visibility"]) == ("Updated", "public")
    assert updated.json()["metadata"] == {"charts": {"default": "accuracy"}}

    preserved = client.put(f"{BASE}/underfit", headers=owner_headers, json={"description": "Retitled"})
    assert preserved.status_code == 200
    assert preserved.json()["description"] == "Retitled"
    assert preserved.json()["metadata"] == {"charts": {"default": "accuracy"}}

    cleared = client.put(f"{BASE}/underfit", headers=owner_headers, json={"metadata": {}})
    assert cleared.status_code == 200
    assert cleared.json()["metadata"] == {}


def test_update_project_ui_state(
    client: TestClient, owner_headers: Headers, create_project: CreateProject, storage: Storage,
) -> None:
    create_project(handle="owner", name="underfit")
    ui_url = f"{BASE}/underfit/ui-state"
    resp = client.put(ui_url, headers=owner_headers, json={"uiState": {"charts": "all"}})
    assert resp.json()["uiState"] == {"charts": "all"}

    config.backfill.enabled = True
    resp = client.put(ui_url, headers=owner_headers, json={"uiState": {"charts": "loss"}})
    assert resp.status_code == 200
    assert client.put(f"{BASE}/underfit", headers=owner_headers, json={"description": "x"}).status_code == 409

    assert json.loads(storage.read(".projects/owner/underfit/ui.json")) == {
        "uiState": {"charts": "loss"},
    }


def test_project_visibility_and_listing(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers,
    create_project: CreateProject, create_org: CreateOrg, create_org_member: CreateOrgMember,
) -> None:
    create_project(handle="owner", name="private")
    create_project(handle="owner", name="shared")
    create_project(handle="owner", name="public", visibility="public")
    create_project(handle="outsider", name="outsider-private")

    def _project_names(projects: Response) -> set[str]:
        return {cast(str, project["name"]) for project in projects.json()}

    assert client.get(f"{BASE}/public").status_code == 200
    assert client.get(f"{BASE}/private").status_code == 401
    assert client.get(f"{BASE}/private", headers=outsider_headers).status_code == 403
    assert client.get(f"{BASE}/shared", headers=outsider_headers).status_code == 403
    owner_projects = client.get(BASE, headers=owner_headers)
    assert owner_projects.status_code == 200 and _project_names(owner_projects) == {"private", "public", "shared"}
    outsider_projects = client.get(BASE, headers=outsider_headers)
    assert outsider_projects.status_code == 200 and _project_names(outsider_projects) == {"public"}
    public_projects = client.get(BASE)
    assert public_projects.status_code == 200 and _project_names(public_projects) == {"public"}
    added = client.put("/api/v1/accounts/owner/projects/shared/collaborators/outsider", headers=owner_headers)
    assert added.status_code == 200
    assert client.get(f"{BASE}/shared", headers=outsider_headers).status_code == 200
    assert _project_names(client.get(BASE, headers=outsider_headers)) == {"public", "shared"}
    launched = client.post("/api/v1/accounts/owner/projects/shared/runs/launch", headers=outsider_headers, json={
        "runName": "r", "launchId": "1",
    })
    assert launched.status_code == 200
    my_projects = client.get("/api/v1/me/projects", headers=outsider_headers)
    assert my_projects.status_code == 200
    assert [cast(str, project["name"]) for project in my_projects.json()] == ["shared", "outsider-private"]

    org = create_org(owner_headers)
    admin_headers = create_org_member(org["id"], "admin@example.com", "admin", "Admin", role="ADMIN")
    member_headers = create_org_member(org["id"], "member@example.com", "member", "Member")
    create_project(handle="core", name="secret")
    create_project(handle="core", name="open", visibility="public")
    launch_url = "/api/v1/accounts/core/projects/secret/runs/launch"
    assert client.get("/api/v1/accounts/core/projects/secret", headers=member_headers).status_code == 403
    assert client.get("/api/v1/accounts/core/projects/secret", headers=admin_headers).status_code == 200
    assert client.get("/api/v1/accounts/core/projects/secret", headers=outsider_headers).status_code == 403
    assert client.get("/api/v1/accounts/core/projects/open", headers=outsider_headers).status_code == 200
    assert client.get("/api/v1/accounts/core/projects/open").status_code == 200
    assert client.post(launch_url, headers=member_headers, json={"runName": "r", "launchId": "1"}).status_code == 403
    assert client.post(launch_url, headers=admin_headers, json={"runName": "r", "launchId": "1"}).status_code == 200
    org_projects = client.get("/api/v1/accounts/core/projects", headers=admin_headers)
    assert org_projects.status_code == 200 and _project_names(org_projects) == {"secret", "open"}
    admin_projects = client.get("/api/v1/me/projects", headers=admin_headers)
    assert admin_projects.status_code == 200 and _project_names(admin_projects) == {"secret", "open"}


@pytest.mark.parametrize(("url", "payload", "status", "existing"), [
    ("/api/v1/accounts/missing/projects", {"name": "underfit", "visibility": "private"}, 404, False),
    (BASE, {"name": "underfit", "visibility": "internal"}, 400, False),
    (BASE, {"name": "UNDERFIT", "visibility": "private"}, 409, True),
])
def test_project_creation_rejects_invalid_requests(
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


def test_delete_project(
    client: TestClient, owner_headers: Headers, outsider_headers: Headers, create_project: CreateProject,
    create_org: CreateOrg, create_org_member: CreateOrgMember,
    engine: Engine, storage: Storage, monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = create_project(handle="owner", name="underfit")
    run = client.post("/api/v1/accounts/owner/projects/underfit/runs/launch", headers=owner_headers, json={
        "runName": "my-run", "launchId": "delete-project",
    }).json()
    storage.write(f"{project.id}/artifacts/project.txt", b"project")
    storage.write(f"{run['id']}/files/run.txt", b"run")
    delete_prefix = projects_route.delete_prefix

    def _delete_prefix(s: Storage, prefix: str) -> None:
        with engine.begin() as conn:
            assert projects_repo.get_by_id(conn, project.id) is None
        delete_prefix(s, prefix)

    monkeypatch.setattr(projects_route, "delete_prefix", _delete_prefix)
    assert client.delete(f"{BASE}/underfit", headers=outsider_headers).status_code == 403
    assert client.delete(f"{BASE}/underfit", headers=owner_headers).status_code == 200
    assert not storage.exists(f"{project.id}/artifacts/project.txt")
    assert not storage.exists(f"{run['id']}/files/run.txt")

    org = create_org(owner_headers)
    admin_headers = create_org_member(org["id"], "admin@example.com", "admin", "Admin", role="ADMIN")
    member_headers = create_org_member(org["id"], "member@example.com", "member", "Member")
    create_project(handle="core", name="secret")
    assert client.delete("/api/v1/accounts/core/projects/secret", headers=member_headers).status_code == 403
    assert client.delete("/api/v1/accounts/core/projects/secret", headers=admin_headers).status_code == 200
